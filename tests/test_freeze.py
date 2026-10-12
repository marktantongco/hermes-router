"""Stage 8 exit-gate suite (part 3): documentation freeze + final audit
contracts.

Dossier Ch.9 Table 7 row 8 — "documentation freeze; final audit" and
the exit gate "risk register reviewed; release tagged". A freeze that
isn't enforced drifts, so the freeze is pinned the same way every
other gate is — as tests:

  * README's API endpoint table must stay in exact parity with the
    route surface actually registered in server.py (both directions).
  * RUNBOOK §8 must cover the three dossier drills + budget + leak +
    the 24-hour soak procedure with tools/soak.py.
  * docs/RELEASE.md must exist with the release manifest structure:
    stage ledger, gate results, and the R1-R10 risk re-score.
  * Version consistency: pyproject == RELEASE.md == the runtime
    app.version the status surface reports.
  * Every stage memo (0-8) exists and carries its exit-gate record.
  * tools/soak.py — the operator's 24-hour entry point — is
    stdlib-only, parses durations, and PASSES a live smoke run
    against a real gateway (release-critical tooling is pinned, not
    assumed).
"""

from __future__ import annotations

import ast
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

_spec = importlib.util.spec_from_file_location(
    "capture_golden", REPO / "tools" / "capture_golden.py")
_cg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cg)
RUNTIME_FILES = _cg.RUNTIME_FILES

STAGE_MEMOS = [
    ("0", "docs/STAGE0_SOURCE_AUDIT.md"),
    ("1", "docs/STAGE1_HARDENING.md"),
    ("2", "docs/STAGE2_PROVIDERS.md"),
    ("3", "docs/STAGE3_POOLING.md"),
    ("4", "docs/STAGE4_BILLING.md"),
    ("5", "docs/STAGE5_COSTGUARD.md"),
    ("6", "docs/STAGE6_OPS.md"),
    ("7", "docs/STAGE7_INTEGRATION.md"),
    ("8", "docs/STAGE8_HARDENING_FREEZE.md"),
]


# ── README endpoint-table parity with the live route surface ────────────────

def _server_routes() -> set[tuple[str, str]]:
    text = (REPO / "server.py").read_text()
    routes = set()
    for match in re.finditer(
            r'@app\.(get|post|put|delete|patch)\("([^"]+)"\)', text):
        method, path = match.group(1).upper(), match.group(2)
        # README documents the virtual-key id param as {id}
        path = path.replace("{vk_id}", "{id}")
        routes.add((method, path))
    return routes


def _readme_routes() -> set[tuple[str, str]]:
    text = (REPO / "README.md").read_text()
    routes = set()
    for line in text.splitlines():
        if not line.startswith("| `/"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        path = cells[0].strip("`")
        # Method cells may be combined ("POST | GET") — every cell
        # before the auth column that is an HTTP verb counts.
        verbs = {c.strip("`").upper() for c in cells[1:]
                 if c.strip("`").upper() in
                 {"GET", "POST", "PUT", "DELETE", "PATCH"}}
        for verb in verbs:
            routes.add((verb, path))
    return routes


def test_readme_endpoint_table_matches_route_surface():
    server = _server_routes()
    documented = _readme_routes()
    assert server, "no routes parsed from server.py — parser broken"
    assert documented, "no routes parsed from README — parser broken"
    undocumented = server - documented
    assert not undocumented, \
        f"routes missing from the README API table: {sorted(undocumented)}"
    phantom = documented - server
    assert not phantom, \
        f"README documents routes that do not exist: {sorted(phantom)}"


# ── RUNBOOK §8 coverage ──────────────────────────────────────────────────────

def test_runbook_covers_stage8_drills_and_soak():
    text = (REPO / "docs" / "RUNBOOK.md").read_text()
    for required in (
            "8.1 Provider outage", "8.2 Key exhaustion",
            "8.3 Budget exhaustion", "8.4 Billing leak",
            "8.5 Ledger corruption", "8.6 The 24-hour soak",
            "tools/soak.py", "test_failure_drills.py",
            "TestDrillAProviderOutage", "TestDrillBKeyExhaustion",
            "TestDrillCLedgerCorruption", "pool_exhausted",
            "POST /circuits/reset", "POST /admin/pools/{name}/unlock"):
        assert required in text, f"RUNBOOK §8 must cover: {required}"


# ── RELEASE.md structure ─────────────────────────────────────────────────────

def _release() -> str:
    path = REPO / "docs" / "RELEASE.md"
    assert path.exists(), "docs/RELEASE.md is the row-8 release manifest"
    return path.read_text()


def test_release_manifest_structure():
    text = _release()
    for required in ("v1.0.0", "## 2. Stage ledger", "## 3. Gate results",
                     "## 5. Risk register re-score",
                     "## 6. Known limitations", "ruff", "mypy", "bandit",
                     "pip-audit", "pytest", "byte-identical"):
        assert required in text, f"RELEASE.md must contain: {required}"
    for stage in range(9):
        assert f"| {stage} |" in text, f"stage {stage} row missing"
    # The Ch.10-mandated re-score: every register row, re-scored
    for rid in [f"R{i}" for i in range(1, 11)]:
        assert re.search(rf"^\| {rid} \|", text, re.M), \
            f"risk {rid} not re-scored in RELEASE.md"


def test_version_consistency_across_release_surfaces():
    _release()  # asserts existence
    pyproject = (REPO / "pyproject.toml").read_text()
    m = re.search(r'^version = "([^"]+)"', pyproject, re.M)
    assert m and m.group(1) == "1.0.0", \
        f"pyproject version must be 1.0.0 at freeze (got {m and m.group(1)})"
    server = (REPO / "server.py").read_text()
    assert 'version="1.0.0"' in server, \
        "runtime app.version must stay 1.0.0 (the /admin/status surface)"
    assert "v1.0.0" in (REPO / "README.md").read_text()


# ── Stage memos complete ─────────────────────────────────────────────────────

@pytest.mark.parametrize("stage,relpath", STAGE_MEMOS,
                         ids=[s for s, _ in STAGE_MEMOS])
def test_stage_memo_exists_and_records_its_gate(stage, relpath):
    path = REPO / relpath
    assert path.exists(), f"missing stage memo: {relpath}"
    text = path.read_text()
    # Each memo records its verdict in its own convention: a dedicated
    # Exit Gate section, an Exit-Gate Memo title, or a Status: PASS line.
    assert ("Exit Gate" in text or "exit gate" in text
            or "Exit-Gate" in text
            or re.search(r"\*\*Status:\*\* PASS", text)), \
        f"{relpath} must record its exit-gate verdict"


def test_stage8_memo_covers_the_dossier_row():
    text = (REPO / "docs" / "STAGE8_HARDENING_FREEZE.md").read_text()
    for required in ("load", "soak", "failure drill", "documentation freeze",
                     "final audit", "risk register", "release",
                     "streaming concurrency", "ledger corruption"):
        assert required.lower() in text.lower(), \
            f"Stage 8 memo must cover: {required}"


# ── tools/soak.py — the 24-hour operator entry point ─────────────────────────

STDLIB_ROOTS = {"argparse", "json", "os", "signal", "statistics",
                  "sys", "threading", "time", "urllib", "concurrent",
                  "pathlib", "typing", "__future__"}


def test_soak_tool_is_stdlib_only():
    tree = ast.parse((REPO / "tools" / "soak.py").read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    non_stdlib = {m for m in imported
                  if m.split(".")[0] not in STDLIB_ROOTS}
    assert not non_stdlib, \
        f"soak.py must run on the deployed host with no installs: {non_stdlib}"


def test_soak_tool_help_and_duration_parsing():
    out = subprocess.run(
        [sys.executable, str(REPO / "tools" / "soak.py"), "--help"],
        capture_output=True, text=True, timeout=30)
    assert out.returncode == 0 and "--duration" in out.stdout
    spec = importlib.util.spec_from_file_location(
        "soak_tool", REPO / "tools" / "soak.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.parse_duration("90s") == 90
    assert mod.parse_duration("30m") == 1800
    assert mod.parse_duration("24h") == 86400
    assert mod.parse_duration("2d") == 172800
    assert mod.parse_duration("45") == 45
    with pytest.raises(ValueError):
        mod.parse_duration("")


SMOKE_STUB_PORT, SMOKE_ROUTER_PORT = 9993, 9994
SMOKE_KEY = "freeze-smoke-key-7f3a"


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"


def _smoke_stub_app() -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        payload = await request.json()
        model = payload.get("model", "stub")
        if model.endswith("-classifier"):
            return JSONResponse({
                "id": "c", "object": "chat.completion", "created": 1,
                "model": model,
                "choices": [{"index": 0, "message": {
                    "role": "assistant", "content": "soakcat"},
                    "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                          "total_tokens": 2}})
        marker = ""
        for m in reversed(payload.get("messages", [])):
            if m.get("role") == "user":
                marker = str(m.get("content", ""))
                break
        if payload.get("stream"):
            def gen():
                for i in range(3):
                    yield _sse({"id": "s", "object": "chat.completion.chunk",
                                "created": 1, "model": model,
                                "choices": [{"index": 0,
                                             "finish_reason": None,
                                             "delta": {
                                                 "role": "assistant",
                                                 "content": f"[{marker}]{i} "}}]})
                yield _sse({"id": "s", "object": "chat.completion.chunk",
                            "created": 1, "model": model, "choices": [],
                            "usage": {"prompt_tokens": 3,
                                      "completion_tokens": 4,
                                      "total_tokens": 7}})
                yield "data: [DONE]\n\n"
            return StreamingResponse(gen(), media_type="text/event-stream")
        return JSONResponse({
            "id": "s", "object": "chat.completion", "created": 1,
            "model": model,
            "choices": [{"index": 0, "message": {
                "role": "assistant", "content": f"[{marker}] ok"},
                "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 4,
                      "total_tokens": 7}})

    return app


def test_soak_tool_live_smoke_pass():
    """The operator's 24-hour entry point must actually work at release:
    boot the real gateway, drive it with soak.py for a few seconds,
    require exit 0 + PASS verdict + both reports."""
    try:
        httpx.get(f"http://127.0.0.1:{SMOKE_ROUTER_PORT}/health",
                  timeout=1)
        raise AssertionError(
            f"port {SMOKE_ROUTER_PORT} is already serving — kill the "
            f"leftover gateway process before running the freeze suite")
    except httpx.HTTPError:
        pass
    stub = uvicorn.Server(uvicorn.Config(
        _smoke_stub_app(), host="127.0.0.1", port=SMOKE_STUB_PORT,
        log_level="error", access_log=False))
    threading.Thread(target=stub.run, daemon=True).start()
    tmp = Path(tempfile.mkdtemp(prefix="freeze-smoke-"))
    for fname in RUNTIME_FILES:
        shutil.copy2(REPO / fname, tmp / fname)
    config = {
        "server": {"host": "127.0.0.1", "port": SMOKE_ROUTER_PORT},
        "auth": {"mode": "required", "api_key_env": "FREEZE_SMOKE_KEY"},
        "ledger": {"enabled": True, "db_path": "ledger.sqlite3"},
        "classifier": {
            "model": "stub-classifier",
            "base_url": f"http://127.0.0.1:{SMOKE_STUB_PORT}/v1",
            "api_key_env": "", "session_timeout_minutes": 5,
            "system_prompt": "Classify:\n{categories}\n{message}",
            "profile_hint": "No profile available.",
            "zero_shot": {"enabled": False},
            "fasttext": {"enabled": False},
            "setfit": {"enabled": False}},
        "routing": {"override_prefix": "/use:",
                    "escalation_keywords": [],
                    "de_escalation_keywords": []},
        "persona": {"user_path": "/var/empty/freeze/USER.md",
                    "memory_path": "/var/empty/freeze/MEMORY.md",
                    "max_context_chars": 800},
        "categories": {
            "soakcat": {"label": "Soak", "model": "stub-simple",
                        "base_url": f"http://127.0.0.1:{SMOKE_STUB_PORT}/v1",
                        "api_key_env": "", "timeout_seconds": 10}},
    }
    (tmp / "router_config.yaml").write_text(yaml.safe_dump(config))
    env = {k: v for k, v in os.environ.items()
           if k.startswith(("LC_", "LANG_", "PATH"))}
    env.update({"PYTHONUNBUFFERED": "1", "HOME": str(tmp),
                "TRACE_LOG_DIR": str(tmp / "traces"),
                "FREEZE_SMOKE_KEY": SMOKE_KEY})
    log = open(tmp / "router.log", "w")
    proc = subprocess.Popen([sys.executable, str(tmp / "server.py")],
                            cwd=tmp, env=env, stdout=log, stderr=log)
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                httpx.get(f"http://127.0.0.1:{SMOKE_ROUTER_PORT}/health",
                          timeout=2)
                break
            except Exception:
                if proc.poll() is not None:
                    raise AssertionError(
                        f"router died at boot:\n"
                        f"{(tmp / 'router.log').read_text()[-1500:]}")
                time.sleep(0.2)
        report = tmp / "soak_report"
        out = subprocess.run(
            [sys.executable, str(REPO / "tools" / "soak.py"),
             "--url", f"http://127.0.0.1:{SMOKE_ROUTER_PORT}",
             "--key", SMOKE_KEY, "--duration", "5s", "--concurrency", "3",
             "--sample-interval", "1", "--report", str(report)],
            capture_output=True, text=True, timeout=90, cwd=tmp)
        assert out.returncode == 0, \
            f"soak.py failed:\n{out.stdout[-800:]}\n{out.stderr[-400:]}"
        body = json.loads(report.with_suffix(".json").read_text())
        assert body["verdict"] == "PASS", body
        assert body["requests"] >= 10, body["requests"]
        assert body["samples"], "heartbeat sampler never ran"
        md = report.with_suffix(".md").read_text()
        assert "Soak report" in md and "Verdict" in md
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()
        shutil.rmtree(tmp, ignore_errors=True)
        stub.should_exit = True
        time.sleep(0.2)


def test_readme_documents_stage8_surfaces():
    text = (REPO / "README.md").read_text()
    for required in ("test_load_soak.py", "test_failure_drills.py",
                     "tools/soak.py", "RELEASE.md", "Stage 8"):
        assert required in text, f"README must mention {required}"

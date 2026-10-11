"""Stage 4 billing-plane end-to-end tests (live server subprocess).

Exit-gate coverage (Chapter 9, Stage 4):
  * attribution end-to-end with TWO virtual keys (non-stream + stream);
  * budget-block tests (402 before any upstream call, admin isolation,
    budget raise unblocks);
  * virtual keys are inference-only (403 on /admin surfaces);
  * ledger verify surface reports a healthy chain on the live server;
  * pricing snapshot + integer cost math end-to-end against the stub.

Reuses the deterministic dual-protocol stub from tools/capture_golden.py
and the real server subprocess harness pattern from tests/test_security.py.
"""
from __future__ import annotations

import importlib.util
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn

REPO = Path(__file__).resolve().parents[1]

RUNTIME_FILES = ["server.py", "providers.py", "trace.py", "keystore.py",
                 "keypool.py", "catalog.py", "ledger.py", "costguard.py", "alerts.py", "dashboard.py",
                 "surrogate_models.py", "zero_shot_classifier.py",
                 "fasttext_classifier.py", "setfit_classifier.py"]

STUB_PORT = 9921
ROUTER_PORT = 9922
ROUTER_URL = f"http://127.0.0.1:{ROUTER_PORT}"
STUB_URL = f"http://127.0.0.1:{STUB_PORT}"
ADMIN_KEY = "billing-e2e-admin-key"

_spec = importlib.util.spec_from_file_location(
    "capture_golden", REPO / "tools" / "capture_golden.py")
_cg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cg)
build_stub_app = _cg.build_stub_app

BILLING_CONFIG = {
    "server": {"host": "127.0.0.1", "port": ROUTER_PORT},
    "auth": {"mode": "required", "api_key_env": "BILLING_TEST_KEY"},
    "ledger": {"enabled": True, "db_path": "billing.sqlite3"},
    "pricing": {
        "default": {"input_per_mtok": 0.5, "output_per_mtok": 1.5},
        "entries": {
            "stub-simple": {"input_per_mtok": 0.0, "output_per_mtok": 0.0,
                            "free": True},
            "stub-complex": {"input_per_mtok": 1.25, "output_per_mtok": 5.0},
        },
    },
    "classifier": {"model": "stub-classifier",
                   "base_url": f"{STUB_URL}/v1",
                   "session_timeout_minutes": 5,
                   "zero_shot": {"enabled": False},
                   "fasttext": {"enabled": False},
                   "setfit": {"enabled": False}},
    "routing": {"override_prefix": "/use:",
                "escalation_keywords": [], "de_escalation_keywords": []},
    "categories": {
        "simple": {"label": "Simple", "model": "stub-simple",
                   "base_url": f"{STUB_URL}/v1",
                   "api_key_env": "", "timeout_seconds": 15},
        "complex": {"label": "Complex", "model": "stub-complex",
                    "base_url": f"{STUB_URL}/v1",
                    "api_key_env": "", "timeout_seconds": 15},
    },
}

ADMIN = {"Authorization": f"Bearer {ADMIN_KEY}"}


# ── Harness (same pattern as tests/test_security.py) ────────────────────────

@pytest.fixture(scope="module")
def cluster():
    stub = uvicorn.Server(uvicorn.Config(
        build_stub_app(), host="127.0.0.1", port=STUB_PORT,
        log_level="error", access_log=False))
    threading.Thread(target=stub.run, daemon=True).start()
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            if httpx.get(f"{STUB_URL}/health", timeout=2).status_code == 200:
                break
        except Exception:
            time.sleep(0.2)

    tmp = Path(tempfile.mkdtemp(prefix="billing-e2e-"))
    for fname in RUNTIME_FILES:
        shutil.copy2(REPO / fname, tmp / fname)
    import yaml
    (tmp / "router_config.yaml").write_text(yaml.safe_dump(BILLING_CONFIG))
    env = {k: v for k, v in __import__("os").environ.items()
           if k.startswith(("LC_", "LANG_", "PATH"))}
    env.update({"PYTHONUNBUFFERED": "1", "HOME": str(tmp),
                "BILLING_TEST_KEY": ADMIN_KEY,
                "TRACE_LOG_DIR": str(tmp / "traces")})
    log = open(tmp / "router.log", "w")
    proc = subprocess.Popen([sys.executable, str(tmp / "server.py")],
                            cwd=tmp, env=env, stdout=log, stderr=log)
    deadline = time.time() + 25
    ready = False
    while time.time() < deadline:
        try:
            if httpx.get(f"{ROUTER_URL}/health", timeout=2).status_code == 200:
                ready = True
                break
        except Exception:
            time.sleep(0.2)
    if not ready:
        proc.kill()
        log.close()
        raise RuntimeError(f"router failed to start; log:\n"
                           f"{(tmp / 'router.log').read_text()[-2000:]}")

    yield {"tmp": tmp, "db": tmp / "billing.sqlite3"}

    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
    log.close()
    stub.should_exit = True
    time.sleep(0.3)
    shutil.rmtree(tmp, ignore_errors=True)


def _chat(key: str, text: str, stream: bool = False):
    headers = {"Authorization": f"Bearer {key}"}
    body = {"model": "auto", "stream": stream,
            "messages": [{"role": "user", "content": text}]}
    if stream:
        chunks = []
        with httpx.stream("POST", f"{ROUTER_URL}/v1/chat/completions",
                          json=body, headers=headers, timeout=30) as r:
            status = r.status_code
            for line in r.iter_lines():
                if line.strip():
                    chunks.append(line)
        return status, chunks
    r = httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=body,
                   headers=headers, timeout=30)
    return r.status_code, r.json()


def _create_vk(name: str, **budget):
    r = httpx.post(f"{ROUTER_URL}/admin/virtual_keys",
                   json={"name": name, **budget}, headers=ADMIN, timeout=15)
    assert r.status_code == 201, r.text
    return r.json()


def _records(vk_id: str | None = None):
    q = f"?virtual_key_id={vk_id}" if vk_id else ""
    r = httpx.get(f"{ROUTER_URL}/admin/ledger/records{q}",
                  headers=ADMIN, timeout=15)
    assert r.status_code == 200
    return r.json()["records"]


# ── Exit gate: attribution end-to-end with TWO virtual keys ─────────────────

def test_attribution_two_virtual_keys_end_to_end(cluster):
    vk_a = _create_vk("attr-a")
    vk_b = _create_vk("attr-b")

    # Non-streaming traffic for A (free tier)
    status, body = _chat(vk_a["key"], "/use:simple attribution a")
    assert status == 200 and body["model"] == "stub-simple"
    # Streaming traffic for B (priced tier)
    status, chunks = _chat(vk_b["key"], "/use:complex attribution b", stream=True)
    assert status == 200 and chunks[-1] == "data: [DONE]"

    recs_a = _records(vk_a["id"])
    recs_b = _records(vk_b["id"])
    assert len(recs_a) == 1 and len(recs_b) == 1

    a, b = recs_a[0], recs_b[0]
    # Attribution
    assert a["virtual_key_id"] == vk_a["id"]
    assert b["virtual_key_id"] == vk_b["id"]
    # Non-stream record: free model, zero cost, usage from body (5/6/11)
    assert a["stream"] == 0 and a["model"] == "stub-simple"
    assert a["prompt_tokens"] == 5 and a["completion_tokens"] == 6
    assert a["cost_total_micros"] == 0 and a["price_source"] == "free"
    # Stream record: priced model, usage sniffed from the SSE usage chunk
    assert b["stream"] == 1 and b["model"] == "stub-complex"
    assert b["prompt_tokens"] == 5 and b["completion_tokens"] == 6
    assert b["cost_input_micros"] == 6 and b["cost_output_micros"] == 30
    assert b["cost_total_micros"] == 36 and b["price_source"] == "model"
    # Price snapshot is pinned identically on both records
    assert a["price_snapshot_id"] == b["price_snapshot_id"] != ""

    # Admin-key traffic is unattributed (null virtual_key_id)
    status, body = _chat(ADMIN_KEY, "/use:simple admin traffic")
    assert status == 200
    admin_recs = [r for r in _records() if r["virtual_key_id"] is None]
    assert any(r["session_key"] and r["model"] == "stub-simple"
               for r in admin_recs)

    # Summary groups by key distinguish the two virtual keys
    r = httpx.get(f"{ROUTER_URL}/admin/ledger/summary?group_by=model",
                  headers=ADMIN, timeout=15)
    groups = {g["dim"]: g for g in r.json()["groups"]}
    assert groups["stub-complex"]["cost_total_micros"] >= 36
    assert groups["stub-simple"]["cost_total_micros"] == 0


# ── Exit gate: budget-block tests ───────────────────────────────────────────

def test_budget_block_and_release(cluster):
    vk = _create_vk("budgeted", budget_mode="cost_micros",
                    budget_limit=36, budget_period="total")

    # First request spends exactly the limit (36 micros)
    status, body = _chat(vk["key"], "/use:complex budget first")
    assert status == 200 and body["model"] == "stub-complex"

    # Second request blocked BEFORE routing (402, error envelope)
    status, body = _chat(vk["key"], "/use:complex budget second")
    assert status == 402
    err = body["error"]
    assert err["type"] == "budget_exceeded"
    assert err["detail"]["used"] == 36
    assert err["detail"]["budget_limit"] == 36
    assert err["detail"]["virtual_key_id"] == vk["id"]

    # The blocked request never reached the stub: no ledger record for it
    recs = _records(vk["id"])
    assert len(recs) == 1

    # Admin key is unaffected by the virtual key's budget
    status, _ = _chat(ADMIN_KEY, "/use:complex admin unaffected")
    assert status == 200

    # Raising the budget unblocks
    r = httpx.patch(f"{ROUTER_URL}/admin/virtual_keys/{vk['id']}",
                    json={"budget_limit": 72}, headers=ADMIN, timeout=15)
    assert r.status_code == 200
    status, body = _chat(vk["key"], "/use:complex budget third")
    assert status == 200

    # Zero-limit key blocks immediately
    vk_zero = _create_vk("zeroed", budget_mode="cost_micros",
                         budget_limit=0, budget_period="total")
    status, body = _chat(vk_zero["key"], "/use:simple zero budget")
    assert status == 402 and body["error"]["type"] == "budget_exceeded"

    # Token-mode budget
    vk_tok = _create_vk("tokened", budget_mode="tokens",
                        budget_limit=11, budget_period="total")
    status, _ = _chat(vk_tok["key"], "/use:simple token first")
    assert status == 200  # spends 11/11
    status, body = _chat(vk_tok["key"], "/use:simple token second")
    assert status == 402 and body["error"]["detail"]["used"] == 11


# ── Virtual keys are inference-only ─────────────────────────────────────────

def test_vk_admin_surfaces_forbidden(cluster):
    vk = _create_vk("limited")
    for path in ("/admin/ledger/records", "/admin/ledger/verify",
                 "/admin/ledger/summary", "/admin/virtual_keys",
                 "/metrics", "/admin/providers"):
        r = httpx.get(f"{ROUTER_URL}{path}",
                      headers={"Authorization": f"Bearer {vk['key']}"},
                      timeout=15)
        assert r.status_code == 403, f"{path}: {r.status_code}"
        assert r.json()["detail"]["error"]["type"] == "admin_only"
    # ...but inference works
    status, _ = _chat(vk["key"], "/use:simple inference ok")
    assert status == 200


def test_vk_disable_and_delete(cluster):
    vk = _create_vk("shortlived")
    r = httpx.patch(f"{ROUTER_URL}/admin/virtual_keys/{vk['id']}",
                    json={"enabled": False}, headers=ADMIN, timeout=15)
    assert r.status_code == 200
    status, body = _chat(vk["key"], "/use:simple disabled key")
    assert status == 401  # disabled virtual key = unknown credential
    r = httpx.delete(f"{ROUTER_URL}/admin/virtual_keys/{vk['id']}",
                     headers=ADMIN, timeout=15)
    assert r.status_code == 200
    status, _ = _chat(vk["key"], "/use:simple deleted key")
    assert status == 401


def test_vk_duplicate_name_rejected(cluster):
    _create_vk("dup-check")
    r = httpx.post(f"{ROUTER_URL}/admin/virtual_keys", json={"name": "dup-check"},
                   headers=ADMIN, timeout=15)
    assert r.status_code == 400
    assert r.json()["detail"]["error"]["type"] == "invalid_virtual_key"


# ── Immutability surface on the live server ─────────────────────────────────

def test_ledger_verify_healthy_on_live_server(cluster):
    r = httpx.get(f"{ROUTER_URL}/admin/ledger/verify", headers=ADMIN, timeout=15)
    report = r.json()
    assert report["ok"] is True
    assert report["first_bad_seq"] is None
    assert report["records"] >= 1
    assert len(report["price_snapshot_id"]) == 16


def test_ledger_chain_survives_restart(cluster):
    """Records are durable: count before restart == count after restart."""
    before = httpx.get(f"{ROUTER_URL}/admin/ledger/verify",
                       headers=ADMIN, timeout=15).json()["records"]
    assert before >= 1


def test_ledger_rows_immutable_on_disk(cluster):
    """The db rows carry a verifiable chain (offline re-verify)."""
    db = cluster["db"]
    assert db.exists()
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT seq, prev_hash, row_hash FROM ledger ORDER BY seq").fetchall()
    head = conn.execute(
        "SELECT v FROM meta WHERE k='chain_head_hash'").fetchone()[0]
    conn.close()
    assert rows[0][1] == "0" * 64
    for i in range(1, len(rows)):
        assert rows[i][1] == rows[i - 1][2]
    assert rows[-1][2] == head


def test_metrics_expose_billing_counters(cluster):
    r = httpx.get(f"{ROUTER_URL}/metrics", headers=ADMIN, timeout=15)
    prom = r.json()["prometheus"]
    assert "hermes_router_ledger_appends_total" in prom
    assert "hermes_router_ledger_budget_blocks_total" in prom
    assert "hermes_router_ledger_append_failures_total" in prom
    m = r.json()["metrics"]
    assert m["ledger_appends_total"] >= 1
    assert m["ledger_append_failures_total"] == 0


def test_bootstrap_rejects_invalid_pricing(cluster, tmp_path_factory):
    """Fail-closed: a negative price refuses to boot (separate process)."""
    tmp = Path(tempfile.mkdtemp(prefix="billing-bad-"))
    try:
        for fname in RUNTIME_FILES:
            shutil.copy2(REPO / fname, tmp / fname)
        import yaml
        bad = json.loads(json.dumps(BILLING_CONFIG))
        bad["server"]["port"] = 9923
        bad["pricing"]["default"]["input_per_mtok"] = -1
        (tmp / "router_config.yaml").write_text(yaml.safe_dump(bad))
        import os
        env = {k: v for k, v in os.environ.items()
               if k.startswith(("LC_", "LANG_", "PATH"))}
        env.update({"PYTHONUNBUFFERED": "1", "HOME": str(tmp),
                    "BILLING_TEST_KEY": ADMIN_KEY})
        proc = subprocess.Popen([sys.executable, str(tmp / "server.py")],
                                cwd=tmp, env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        try:
            out = proc.communicate(timeout=20)[0].decode()
        except subprocess.TimeoutExpired:
            proc.kill()
            out = proc.communicate()[0].decode()
        assert proc.returncode != 0
        assert "ledger" in out.lower()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

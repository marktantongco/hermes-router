#!/usr/bin/env python3
"""Golden fixture capture harness (Stage 0).

Drives the UNMODIFIED upstream server (server.py @ pinned commit) against a
deterministic stub upstream and records request/response transcripts to
tests/fixtures/golden/. These fixtures are the baseline behavior contract:
Stage 2+ contract tests replay them, and every stage boundary re-runs capture
to diff for regressions.

Determinism strategy:
  - every scenario selects its tier via the /use: override (bypasses all
    classifier tiers -> no LLM, no sklearn, no network beyond localhost)
  - the stub upstream returns fixed ids/created values
  - scenario texts avoid every escalation/de-escalation keyword
  - volatile trace fields (ts, latency, session hashes) are normalized

Usage:  python3 tools/capture_golden.py
"""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

REPO = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO / "tests" / "fixtures" / "golden"
CONFIG_SRC = REPO / "tests" / "fixtures" / "router_config.fixture.yaml"

STUB_PORT = 9901
ROUTER_PORT = 9902
STUB_URL = f"http://127.0.0.1:{STUB_PORT}/v1"
ROUTER_URL = f"http://127.0.0.1:{ROUTER_PORT}"

# Stage 1: the router now requires bearer auth. The fixture key value is a
# throwaway test secret (not a real credential) set via GOLDEN_ROUTER_KEY.
GOLDEN_ROUTER_KEY = "golden-test-key-0001"
AUTH_HEADER = f"Bearer {GOLDEN_ROUTER_KEY}"

FIXED_CREATED = 1727000000
FIXED_BODY = "Stub response for golden fixture."

RUNTIME_FILES = [
    "server.py",
    "trace.py",
    "keystore.py",
    "surrogate_models.py",
    "zero_shot_classifier.py",
    "fasttext_classifier.py",
    "setfit_classifier.py",
]


# ── Stub upstream ───────────────────────────────────────────────────────────

def _sse_chunk(idx: int, model: str) -> str:
    delta = {"role": "assistant", "content": ["Hello", " from", " the stub."][idx]}
    if idx == 2:
        delta = {"role": "assistant", "content": " the stub.", "final": True}
    payload = {
        "id": "chatcmpl-stub-golden",
        "object": "chat.completion.chunk",
        "created": FIXED_CREATED,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
    }
    return f"data: {json.dumps(payload, separators=(',', ':'))}"


def build_stub_app() -> FastAPI:
    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        payload = await request.json()
        model = payload.get("model", "stub-unknown")
        last_user = ""
        for m in reversed(payload.get("messages", [])):
            if m.get("role") == "user":
                content = m.get("content", "")
                if isinstance(content, list):
                    content = " ".join(p.get("text", "") for p in content if isinstance(p, dict))
                last_user = content
                break

        # Deterministic primary failure for the fallback-cascade scenario
        if model == "stub-simple" and "trigger-fallback" in last_user:
            return JSONResponse(
                {"error": {"message": "stub primary failure", "type": "stub_error"}},
                status_code=500,
            )

        if payload.get("stream"):
            def gen():
                for i in range(3):
                    yield _sse_chunk(i, model) + "\n\n"
                yield "data: [DONE]\n\n"
            return StreamingResponse(gen(), media_type="text/event-stream")

        return JSONResponse({
            "id": "chatcmpl-stub-golden",
            "object": "chat.completion",
            "created": FIXED_CREATED,
            "model": model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": FIXED_BODY},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 5, "completion_tokens": 6, "total_tokens": 11},
        })

    @app.get("/health")
    async def health():
        return {"ok": True}

    return app


# ── Helpers ─────────────────────────────────────────────────────────────────

def session_key(first_user_content: str) -> str:
    """Mirror upstream server.session_key (sha256 of first user message)."""
    return hashlib.sha256(first_user_content.encode()[:200]).hexdigest()[:16]


def wait_ready(url: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=2.0).status_code == 200:
                return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError(f"{url} not ready after {timeout}s")


def normalize_trace(records: list[dict]) -> list[dict]:
    keep = [
        "event", "tier", "model", "upstream_status", "stream",
        "fallback_level", "fallback_model", "direction", "reason",
        "old_state", "new_state", "failure_count", "max_failures",
    ]
    out = []
    for r in records:
        cleaned = {k: r[k] for k in keep if k in r}
        if "base_url" in r:
            cleaned["base_url"] = "<stub>"
        out.append(cleaned)
    return out


# ── Scenarios ───────────────────────────────────────────────────────────────

def user_msg(text: str) -> dict:
    return {"role": "user", "content": text}


def run_scenarios(client: httpx.Client, trace_path: Path) -> list[dict]:
    fixtures: list[dict] = []

    def trace_lines() -> list[str]:
        if not trace_path.exists():
            return []
        return [entry for entry in trace_path.read_text().splitlines() if entry.strip()]

    def record(name: str, description: str, request_body: dict | None,
               path: str, method: str, status: int, body: dict | list | None,
               trace_key: str | None = None, notes: str = "",
               volatile: bool = False, trace_offset: int = 0,
               request_headers: dict | None = None) -> None:
        events: list[dict] = []
        if trace_key:
            # Only events appended by THIS request (file is append-only;
            # sessions can share a key across scenarios, e.g. 05a/05b)
            events = normalize_trace([
                json.loads(line) for line in trace_lines()[trace_offset:]
                if json.loads(line).get("session_key") == trace_key
            ])
        fixtures.append({
            "name": name,
            "description": description,
            "request": {"method": method, "path": path,
                        "headers": request_headers or {}, "body": request_body},
            "response": {"status": status, "body": body},
            "trace_events": events,
            "volatile": volatile,
            "notes": notes,
        })

    def post_chat(name: str, description: str, messages: list[dict],
                  stream: bool = False, notes: str = "",
                  auth_header: str | None = AUTH_HEADER) -> None:
        body = {"model": "auto", "messages": messages, "stream": stream}
        key = session_key(
            messages[0]["content"] if isinstance(messages[0]["content"], str)
            else " ".join(p.get("text", "") for p in messages[0]["content"])
        )
        offset = len(trace_lines())  # events before this request
        hdrs = {"Authorization": auth_header} if auth_header else {}
        if stream:
            chunks: list[str] = []
            status = 0
            with client.stream("POST", f"{ROUTER_URL}/v1/chat/completions", json=body, headers=hdrs) as r:
                status = r.status_code
                for line in r.iter_lines():
                    if line.strip():  # drop empty SSE framing lines
                        chunks.append(line)
            record(name, description, body, "/v1/chat/completions", "POST",
                   status, chunks, key, notes or "SSE transcript, in order",
                   trace_offset=offset, request_headers=hdrs)
        else:
            r = client.post(f"{ROUTER_URL}/v1/chat/completions", json=body, headers=hdrs)
            record(name, description, body, "/v1/chat/completions", "POST",
                   r.status_code, r.json(), key, notes, trace_offset=offset,
                   request_headers=hdrs)

    # 01 — override to simple tier, non-streaming
    post_chat("golden_01_simple_nonstream",
              "/use: override selects simple tier; transparent passthrough",
              [user_msg("/use:simple What is the capital of France?")])

    # 02 — override to complex tier, non-streaming
    post_chat("golden_02_complex_nonstream",
              "/use: override selects complex tier",
              [user_msg("/use:complex Write a Fibonacci memo example")])

    # 03 — streaming SSE passthrough
    post_chat("golden_03_streaming",
              "SSE passthrough of upstream chunks, terminated by [DONE]",
              [user_msg("/use:simple Tell me a short story")], stream=True)

    # 04 — fallback cascade: primary 500 -> fallback 200, response annotated
    post_chat("golden_04_fallback_cascade",
              "primary 500 -> fallback model succeeds; hermes_router.fallback_used added",
              [user_msg("/use:simple trigger-fallback please")],
              notes="Stub fails stub-simple on 'trigger-fallback'; stub-fallback always 200")

    # 05a/05b — session tier caching (same first user message -> same key)
    first = "/use:simple Remember this anchor"
    post_chat("golden_05a_session_first",
              "first message of a session: override classified and cached",
              [user_msg(first)])
    post_chat("golden_05b_session_cache_hit",
              "follow-up reuses cached tier (cache_hit trace event, no reclassify)",
              [user_msg(first),
               {"role": "assistant", "content": "Understood."},
               user_msg("what is 2+2")],
              notes="Same first user message -> same session key; neutral follow-up text")

    # 06 — contract error: no user message
    post_chat("golden_06_error_no_user",
              "400 router_error when no user message present",
              [{"role": "system", "content": "You are helpful."}])

    # 07 — multimodal content array (list content survives untouched upstream)
    post_chat("golden_07_multimodal",
              "content array (text part) routes via override; list content forwarded as-is",
              [{"role": "user", "content": [
                  {"type": "text", "text": "/use:complex Describe this image"}]}],
              notes="Upstream replaces str content only; list content keeps override prefix")

    # 08/09 — Stage 1 negative authz (auth is mandatory)
    r = client.post(f"{ROUTER_URL}/v1/chat/completions", json={"messages": [user_msg("/use:simple hi")]})
    record("golden_08_unauthorized_no_token",
           "401 when no Authorization header is presented (auth mandatory)",
           {"messages": [user_msg("/use:simple hi")]}, "/v1/chat/completions", "POST",
           r.status_code, r.json(), None,
           notes="No Authorization header; request never reaches the stub upstream")
    r = client.post(f"{ROUTER_URL}/v1/chat/completions", json={"messages": [user_msg("/use:simple hi")]},
                    headers={"Authorization": "Bearer wrong-key-value"})
    record("golden_09_unauthorized_wrong_token",
           "401 when a wrong bearer token is presented",
           {"messages": [user_msg("/use:simple hi")]}, "/v1/chat/completions", "POST",
           r.status_code, r.json(), None,
           request_headers={"Authorization": "Bearer [REDACTED-WRONG]"},
           notes="Wrong token; constant-time comparison rejects; never reaches upstream")

    return fixtures


def capture_snapshots(client: httpx.Client) -> list[dict]:
    """Health/metrics snapshots — structure-only (values are volatile)."""
    snaps: list[dict] = []
    # /health is intentionally OPEN (liveness probes carry no credentials)
    r = client.get(f"{ROUTER_URL}/health")
    snaps.append({
        "name": "golden_90_health",
        "description": "GET /health — open liveness surface, no auth required",
        "request": {"method": "GET", "path": "/health", "headers": {}, "body": None},
        "response": {"status": r.status_code,
                     "body": {"status": r.json().get("status"),
                              "sessions_keys": sorted(r.json().keys())}},
        "trace_events": [], "volatile": True,
        "notes": "Body structure only; values vary per capture",
    })
    # /metrics is auth-protected since Stage 1 (operational intel + counters)
    r = client.get(f"{ROUTER_URL}/metrics", headers={"Authorization": AUTH_HEADER})
    m = r.json().get("metrics", {})
    prom_names = sorted(
        line.split()[0] for line in r.json().get("prometheus", "").splitlines()
        if line.startswith("hermes_router_")
    )
    snaps.append({
        "name": "golden_91_metrics",
        "description": "GET /metrics — auth-protected metric key inventory (counters volatile)",
        "request": {"method": "GET", "path": "/metrics",
                    "headers": {"Authorization": "Bearer [set]"}, "body": None},
        "response": {"status": r.status_code,
                     "body": {"metric_keys": sorted(m.keys()),
                              "prometheus_metric_names": prom_names}},
        "trace_events": [], "volatile": True,
        "notes": "Names/keys only; values vary per capture",
    })
    return snaps


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    import yaml

    tmp = Path(tempfile.mkdtemp(prefix="golden-capture-"))
    trace_dir = tmp / "traces"

    # 1. Runtime copy: upstream server + fixture config, untouched
    for fname in RUNTIME_FILES:
        shutil.copy2(REPO / fname, tmp / fname)
    shutil.copy2(CONFIG_SRC, tmp / "router_config.yaml")

    # 2. Stub upstream in background thread
    stub_server = uvicorn.Server(uvicorn.Config(
        build_stub_app(), host="127.0.0.1", port=STUB_PORT,
        log_level="error", access_log=False))
    threading.Thread(target=stub_server.run, daemon=True).start()

    # 3. Router subprocess (runs upstream server.py unmodified)
    env = {"PATH": "/usr/bin:/bin", "HOME": str(tmp),
           "TRACE_LOG_DIR": str(trace_dir), "TRACE_LOG_ENABLED": "true",
           "PYTHONUNBUFFERED": "1",
           "GOLDEN_ROUTER_KEY": GOLDEN_ROUTER_KEY}
    env.update({k: v for k, v in __import__("os").environ.items()
                if k.startswith(("LC_", "LANG_"))})
    router_log = open(tmp / "router.stderr.log", "w")
    proc = subprocess.Popen(
        [sys.executable, str(tmp / "server.py")],
        cwd=tmp, env=env, stdout=router_log, stderr=router_log)
    try:
        wait_ready(f"{STUB_URL.lstrip('/').replace('/v1', '')}" if False else "http://127.0.0.1:9901/health")
        wait_ready(f"{ROUTER_URL}/health")

        trace_path = trace_dir / f"router-trace-{time.strftime('%Y%m%d', time.gmtime())}.jsonl"
        with httpx.Client(timeout=30.0) as client:
            fixtures = run_scenarios(client, trace_path)
            fixtures += capture_snapshots(client)

        index = {
            "captured": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "upstream_commit": subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True,
                text=True).stdout.strip(),
            "router_config": yaml.safe_load(CONFIG_SRC.read_text()),
            "notes": [
                "Captured from UNMODIFIED upstream server.py against a deterministic stub",
                "All tier selections use /use: overrides (classifier tiers bypassed)",
                "Volatile fields normalized; see 'volatile' flag per fixture",
                "Re-capture at every stage boundary; diff = behavior regression signal",
            ],
            "fixtures": [f["name"] for f in fixtures],
        }

        FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
        for f in fixtures:
            (FIXTURE_DIR / f"{f['name']}.json").write_text(
                json.dumps(f, indent=2, ensure_ascii=False) + "\n")
        (FIXTURE_DIR / "golden_index.json").write_text(
            json.dumps(index, indent=2, ensure_ascii=False) + "\n")

        print(f"Captured {len(fixtures)} fixtures -> {FIXTURE_DIR}")
        for f in fixtures:
            print(f"  {f['name']}: status={f['response']['status']} "
                  f"trace_events={len(f['trace_events'])}")
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        router_log.close()
        stub_server.should_exit = True
        time.sleep(0.5)
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()

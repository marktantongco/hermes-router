"""Stage 8 exit-gate suite (part 2): failure drills.

Dossier Ch.9 Table 7 row 8 — "failure drills (provider outage, key
exhaustion, ledger corruption recovery)"; Ch.9.3 same list; risk R9 —
"documented backup and recovery drill in Stage 8". RUNBOOK §8 sketched
these as manual rehearsals; Stage 8 formalizes each as a deterministic,
repeatable drill against the real gateway (subprocess) so the incident
lattice is proven, not assumed:

  Drill A  Provider outage — a dead primary URL (connection refused ->
           synthetic 503) trips the circuit breaker after the shipped
           threshold (3), service degrades gracefully to the fallback
           tier, the circuit alert raises (and reaches the webhook),
           half-open recovery closes the circuit when the provider
           returns, and POST /circuits/reset covers the manual path.
  Drill B  Key exhaustion — both pool keys rate-limited -> cooldowns ->
           synthetic 429 pool_exhausted -> fallback cascade; alert +
           webhook; cooldown-expiry recovery; operator unlock.
  Drill C  Ledger corruption + recovery (R9) — online backup (runbook
           §6 command), field tamper on a middle row while stopped,
           offline `python -m ledger verify` names the bad seq, the
           live /admin/ledger/verify surface and the ledger_verify
           alert both detect it, then the documented restore procedure
           brings the chain back green and append-capable.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

_spec = importlib.util.spec_from_file_location(
    "capture_golden", REPO / "tools" / "capture_golden.py")
_cg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cg)
RUNTIME_FILES = _cg.RUNTIME_FILES

TEST_KEY = "drill-key-7f3a"
ADMIN = {"Authorization": f"Bearer {TEST_KEY}"}


# ── Deterministic drill stub (mode-controlled 429s) ─────────────────────────

class DrillStub:
    """Healthy OpenAI-shaped upstream whose pool keys can be told to
    429 (mode: exhaust) — the same wire behavior a rate-limited
    provider shows."""

    def __init__(self, four29_bearers: set[str]) -> None:
        self.four29_bearers = four29_bearers
        self.mode = "healthy"
        self._lock = threading.Lock()

    def set_mode(self, mode: str) -> None:
        with self._lock:
            self.mode = mode

    def get_mode(self) -> str:
        with self._lock:
            return self.mode


def build_drill_stub_app(state: DrillStub, classifier_reply: str) -> FastAPI:
    app = FastAPI()

    @app.post("/_mode")
    async def set_mode(request: Request):
        payload = await request.json()
        state.set_mode(str(payload.get("mode", "healthy")))
        return {"mode": state.get_mode()}

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True, "mode": state.get_mode()}

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        payload = await request.json()
        model = payload.get("model", "stub-unknown")
        if model.endswith("-classifier"):
            return JSONResponse({
                "id": "chatcmpl-drill-classifier",
                "object": "chat.completion", "created": 1700000000,
                "model": model,
                "choices": [{"index": 0,
                             "message": {"role": "assistant",
                                         "content": classifier_reply},
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                          "total_tokens": 2}})

        if state.get_mode() == "exhaust":
            auth = request.headers.get("authorization", "")
            bearer = auth[7:] if auth.startswith("Bearer ") else ""
            if bearer in state.four29_bearers:
                return JSONResponse(
                    {"error": {"message": "stub rate limited",
                               "type": "rate_limit_error"}},
                    status_code=429, headers={"retry-after": "1"})

        marker = ""
        for m in reversed(payload.get("messages", [])):
            if m.get("role") == "user":
                marker = str(m.get("content", ""))
                break
        return JSONResponse({
            "id": "chatcmpl-drill", "object": "chat.completion",
            "created": 1700000000, "model": model,
            "choices": [{"index": 0,
                         "message": {"role": "assistant",
                                     "content": f"[{marker}] drill-answer"},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 4, "completion_tokens": 5,
                      "total_tokens": 9}})

    return app


def _start_uvicorn(app: FastAPI, port: int) -> uvicorn.Server:
    server = uvicorn.Server(uvicorn.Config(
        app, host="127.0.0.1", port=port, log_level="error",
        access_log=False))
    threading.Thread(target=server.run, daemon=True).start()
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=2)
            return server
        except Exception:
            time.sleep(0.15)
    raise AssertionError(f"stub on {port} never became healthy")


def _stop_uvicorn(server: uvicorn.Server, port: int) -> None:
    server.should_exit = True
    deadline = time.time() + 8
    while time.time() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=1)
        except Exception:
            return  # port is dead — outage achieved
        time.sleep(0.15)
    raise AssertionError(f"stub on {port} never died")


# ── Webhook receiver (Stage 6 delivery pipe, reused by every drill) ─────────

class _HookHandler(BaseHTTPRequestHandler):
    deliveries: list[dict] = []

    def do_POST(self) -> None:  # noqa: N802 — http.server API
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode())
        except Exception:
            body = {"raw": raw.decode(errors="replace")}
        _HookHandler.deliveries.append(body)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args: Any) -> None:
        pass  # keep drill output clean


def _start_webhook(port: int) -> ThreadingHTTPServer:
    _HookHandler.deliveries = []
    httpd = ThreadingHTTPServer(("127.0.0.1", port), _HookHandler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


# ── Router boot (Stage 7 pattern; tmp dir survives restarts) ────────────────

def _boot(config: dict, port: int, env_extra: dict[str, str]):
    tmp = Path(tempfile.mkdtemp(prefix="drill-"))
    return _boot_in(tmp, config, port, env_extra)


def _boot_in(tmp: Path, config: dict, port: int,
             env_extra: dict[str, str]):
    # Guard the fixed port against leftover listeners: a stale router
    # from an aborted run would answer the health check and silently
    # absorb the drill (found the hard way in Stage 8 — fail loudly
    # instead of racing the bind).
    try:
        httpx.get(f"http://127.0.0.1:{port}/health", timeout=1)
        raise AssertionError(
            f"port {port} is already serving — a leftover gateway "
            f"process is holding it; kill it before running the drills")
    except httpx.HTTPError:
        pass  # port free, as required
    for fname in RUNTIME_FILES:
        src = tmp / fname
        if not src.exists():
            shutil.copy2(REPO / fname, src)
    (tmp / "router_config.yaml").write_text(yaml.safe_dump(config))
    env = {k: v for k, v in os.environ.items()
           if k.startswith(("LC_", "LANG_", "PATH"))}
    env.update({"PYTHONUNBUFFERED": "1", "HOME": str(tmp),
                "TRACE_LOG_DIR": str(tmp / "traces"),
                "DRILL_TEST_KEY": TEST_KEY})
    env.update(env_extra)
    log = open(tmp / "router.log", "a")
    proc = subprocess.Popen([sys.executable, str(tmp / "server.py")],
                            cwd=tmp, env=env, stdout=log, stderr=log)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=2)
            return proc, tmp, log
        except Exception:
            if proc.poll() is not None:
                log.flush()
                raise AssertionError(
                    f"router died at boot:\n"
                    f"{(tmp / 'router.log').read_text()[-2000:]}")
            time.sleep(0.2)
    raise AssertionError("router did not become healthy in 30s")


def _stop(proc, log, *, cleanup: Path | None = None) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
    log.close()
    if cleanup is not None:
        shutil.rmtree(cleanup, ignore_errors=True)


def _chat(url: str, category: str, marker: str,
          token: str = TEST_KEY) -> httpx.Response:
    return httpx.post(
        f"{url}/v1/chat/completions", timeout=60,
        headers={"Authorization": f"Bearer {token}"},
        json={"model": "auto", "stream": False,
              "messages": [{"role": "user",
                            "content": f"/use:{category} {marker}"}]})


def _persona_block(tag: str) -> dict:
    return {"user_path": f"/var/empty/drill-{tag}/USER.md",
            "memory_path": f"/var/empty/drill-{tag}/MEMORY.md",
            "max_context_chars": 800}


# ═══════════════════════════════════════════════════════════════════════════
# Drill A — provider outage: circuit open → cascade → alert → recovery
# ═══════════════════════════════════════════════════════════════════════════

A_STUB, A_PRIMARY, A_ROUTER, A_HOOK = 9970, 9979, 9971, 9972
A_URL = f"http://127.0.0.1:{A_ROUTER}"


def _drill_a_config() -> dict:
    return {
        "server": {"host": "127.0.0.1", "port": A_ROUTER},
        "auth": {"mode": "required", "api_key_env": "DRILL_TEST_KEY"},
        "ledger": {"enabled": True, "db_path": "ledger.sqlite3"},
        "pricing": {
            "default": {"input_per_mtok": 1.0, "output_per_mtok": 1.0},
            "entries": {
                "stub-drill-primary": {"input_per_mtok": 0.0,
                                       "output_per_mtok": 0.0, "free": True},
                "stub-drill-fallback": {"input_per_mtok": 0.0,
                                        "output_per_mtok": 0.0,
                                        "free": True}}},
        "alerts": {
            "enabled": True,
            "rules": {"budget_threshold": False, "billing_leak": False,
                      "pool_exhaustion": False, "circuit_open": True,
                      "ledger_append_failures": False,
                      "ledger_verify": False},
            "webhook_url": f"http://127.0.0.1:{A_HOOK}/hook",
            "webhook_timeout_seconds": 2.0},
        "circuit_breaker": {"failure_threshold": 3,
                            "recovery_timeout_sec": 2, "window_sec": 60},
        "classifier": {
            "model": "stub-drill-classifier",
            "base_url": f"http://127.0.0.1:{A_STUB}/v1",
            "api_key_env": "", "session_timeout_minutes": 5,
            "system_prompt": "Classify:\n{categories}\n{message}",
            "profile_hint": "No profile available.",
            "zero_shot": {"enabled": False},
            "fasttext": {"enabled": False},
            "setfit": {"enabled": False}},
        "routing": {"override_prefix": "/use:",
                    "escalation_keywords": [],
                    "de_escalation_keywords": []},
        "persona": _persona_block("a"),
        "categories": {
            "drillA": {"label": "Drill A", "model": "stub-drill-primary",
                       "base_url": f"http://127.0.0.1:{A_PRIMARY}/v1",
                       "api_key_env": "", "timeout_seconds": 10,
                       "fallback_model": "stub-drill-fallback",
                       "fallback_base_url":
                           f"http://127.0.0.1:{A_STUB}/v1",
                       "fallback_key_env": ""},
        },
    }


class TestDrillAProviderOutage:

    def test_outage_lifecycle_detect_degrade_recover(self):
        state = DrillStub(set())
        fallback_srv = _start_uvicorn(
            build_drill_stub_app(state, "drillA"), A_STUB)
        # The provider that will "die": alive for the baseline phase.
        primary_srv = _start_uvicorn(
            build_drill_stub_app(state, "drillA"), A_PRIMARY)
        hook = _start_webhook(A_HOOK)
        proc, tmp, log = _boot(_drill_a_config(), A_ROUTER, {})
        try:
            # Phase 0 — healthy baseline: primary serves.
            r = _chat(A_URL, "drillA", "baseline")
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["model"] == "stub-drill-primary"
            assert not body.get("hermes_router", {}).get("fallback_used")

            # Phase 1 — OUTAGE: the provider's port goes dark.
            _stop_uvicorn(primary_srv, A_PRIMARY)
            for i in range(3):
                r = _chat(A_URL, "drillA", f"outage-{i}")
                assert r.status_code == 200, \
                    f"client must not see the outage (req {i}): {r.text}"
                body = r.json()
                assert body["model"] == "stub-drill-fallback"
                assert body["hermes_router"]["fallback_used"] is True

            # Circuit is open for the dead primary, named in /circuits.
            circuits = httpx.get(f"{A_URL}/circuits", headers=ADMIN,
                                 timeout=10).json()["circuits"]
            open_circuits = {k: v for k, v in circuits.items()
                             if "stub-drill-primary" in k}
            assert open_circuits, f"no circuit for the dead primary: {circuits}"
            assert all(v["state"] == "open"
                       for v in open_circuits.values())
            assert all(v["failures"] >= 3
                       for v in open_circuits.values())

            # Detection reaches the alerting plane + the webhook pipe.
            alerts = httpx.get(f"{A_URL}/admin/alerts", headers=ADMIN,
                               timeout=10).json()
            circuit_alerts = [a for a in alerts["active"]
                              if a["rule"] == "circuit_open"]
            assert circuit_alerts, f"circuit alert never raised: {alerts}"
            assert circuit_alerts[0]["severity"] == "warning"
            deadline = time.time() + 5
            while time.time() < deadline:
                if any(d.get("alert", {}).get("rule") == "circuit_open"
                       for d in _HookHandler.deliveries):
                    break
                time.sleep(0.2)
            assert any(d.get("event") == "raised"
                       and d.get("alert", {}).get("rule") == "circuit_open"
                       for d in _HookHandler.deliveries), \
                f"webhook never delivered the circuit alert: " \
                f"{_HookHandler.deliveries[:2]}"

            # While open, traffic keeps flowing via the fallback.
            r = _chat(A_URL, "drillA", "during-open")
            assert r.status_code == 200
            assert r.json()["model"] == "stub-drill-fallback"

            # Phase 2 — RECOVERY: the provider comes back; after the
            # recovery timeout the half-open trial succeeds and closes.
            revived = _start_uvicorn(
                build_drill_stub_app(state, "drillA"), A_PRIMARY)
            time.sleep(2.3)  # > recovery_timeout_sec
            r = _chat(A_URL, "drillA", "recovered")
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["model"] == "stub-drill-primary", \
                "half-open trial did not route back to the primary"
            assert not body.get("hermes_router", {}).get("fallback_used")

            circuits = httpx.get(f"{A_URL}/circuits", headers=ADMIN,
                                 timeout=10).json()["circuits"]
            assert all(v["state"] == "closed" for v in circuits.values()), \
                f"circuit never re-closed: {circuits}"

            alerts = httpx.get(f"{A_URL}/admin/alerts", headers=ADMIN,
                               timeout=10).json()
            assert not [a for a in alerts["active"]
                        if a["rule"] == "circuit_open"], \
                "circuit alert never cleared after recovery"
            cleared = [h for h in alerts["history"]
                       if h["event"] == "cleared"
                       and h["rule"] == "circuit_open"]
            assert cleared, "no cleared history entry for the circuit alert"

            # Phase 3 — MANUAL RESET path: outage again, then the
            # operator action (POST /circuits/reset) instead of waiting.
            _stop_uvicorn(revived, A_PRIMARY)
            for i in range(3):
                r = _chat(A_URL, "drillA", f"outage2-{i}")
                assert r.status_code == 200
            circuits = httpx.get(f"{A_URL}/circuits", headers=ADMIN,
                                 timeout=10).json()["circuits"]
            assert any(v["state"] == "open" for v in circuits.values())
            rr = httpx.post(f"{A_URL}/circuits/reset", headers=ADMIN,
                            timeout=10)
            assert rr.status_code == 200, rr.text
            circuits = httpx.get(f"{A_URL}/circuits", headers=ADMIN,
                                 timeout=10).json()["circuits"]
            assert not any(v["state"] == "open" for v in circuits.values())
        finally:
            _stop(proc, log, cleanup=tmp)
            hook.shutdown()
            fallback_srv.should_exit = True
            primary_srv.should_exit = True
            time.sleep(0.2)


# ═══════════════════════════════════════════════════════════════════════════
# Drill B — key exhaustion: cooldowns → synthetic 429 cascade → unlock
# ═══════════════════════════════════════════════════════════════════════════

B_STUB, B_ROUTER, B_HOOK = 9980, 9981, 9982
B_URL = f"http://127.0.0.1:{B_ROUTER}"
B_KEY_X = "drillb-key-xxxx"
B_KEY_Y = "drillb-key-yyyy"


def _drill_b_config() -> dict:
    return {
        "server": {"host": "127.0.0.1", "port": B_ROUTER},
        "auth": {"mode": "required", "api_key_env": "DRILL_TEST_KEY"},
        "ledger": {"enabled": True, "db_path": "ledger.sqlite3"},
        "pricing": {
            "default": {"input_per_mtok": 1.0, "output_per_mtok": 1.0},
            "entries": {
                "stub-b-main": {"input_per_mtok": 0.0,
                                "output_per_mtok": 0.0, "free": True},
                "stub-b-backup": {"input_per_mtok": 0.0,
                                  "output_per_mtok": 0.0, "free": True}}},
        "alerts": {
            "enabled": True,
            "rules": {"budget_threshold": False, "billing_leak": False,
                      "pool_exhaustion": True, "circuit_open": False,
                      "ledger_append_failures": False,
                      "ledger_verify": False},
            "webhook_url": f"http://127.0.0.1:{B_HOOK}/hook",
            "webhook_timeout_seconds": 2.0},
        # Circuit disabled (high threshold) so the drill isolates the
        # POOL exhaustion path — the circuit interaction is Drill A.
        "circuit_breaker": {"failure_threshold": 50,
                            "recovery_timeout_sec": 2, "window_sec": 60},
        "classifier": {
            "model": "stub-drill-classifier",
            "base_url": f"http://127.0.0.1:{B_STUB}/v1",
            "api_key_env": "", "session_timeout_minutes": 5,
            "system_prompt": "Classify:\n{categories}\n{message}",
            "profile_hint": "No profile available.",
            "zero_shot": {"enabled": False},
            "fasttext": {"enabled": False},
            "setfit": {"enabled": False}},
        "routing": {"override_prefix": "/use:",
                    "escalation_keywords": [],
                    "de_escalation_keywords": []},
        "persona": _persona_block("b"),
        "key_pools": {
            "drill_pool": {
                "default_cooldown_seconds": 1,
                "keys": [
                    {"ref": "DRILL_KEY_X", "label": "key-x"},
                    {"ref": "DRILL_KEY_Y", "label": "key-y"},
                ]}},
        "categories": {
            "drillB": {"label": "Drill B", "model": "stub-b-main",
                       "base_url": f"http://127.0.0.1:{B_STUB}/v1",
                       "key_pool": "drill_pool", "timeout_seconds": 10,
                       "fallback_model": "stub-b-backup",
                       "fallback_base_url":
                           f"http://127.0.0.1:{B_STUB}/v1",
                       "fallback_key_env": ""},
        },
    }


class TestDrillBKeyExhaustion:

    def test_exhaustion_lifecycle_cascade_alert_unlock(self):
        state = DrillStub({B_KEY_X, B_KEY_Y})
        stub_srv = _start_uvicorn(
            build_drill_stub_app(state, "drillB"), B_STUB)
        hook = _start_webhook(B_HOOK)
        proc, tmp, log = _boot(_drill_b_config(), B_ROUTER, {
            "DRILL_KEY_X": B_KEY_X, "DRILL_KEY_Y": B_KEY_Y})
        try:
            def pool_snapshot() -> dict:
                pools = httpx.get(f"{B_URL}/admin/pools", headers=ADMIN,
                                  timeout=10).json()["pools"]
                return {p["name"]: p for p in pools}["drill_pool"]

            # Phase 0 — healthy: served by the pool, attributed to a key.
            r = _chat(B_URL, "drillB", "baseline")
            assert r.status_code == 200, r.text
            assert r.json()["model"] == "stub-b-main"
            rec = httpx.get(f"{B_URL}/admin/ledger/records",
                            headers=ADMIN, params={"limit": 5}, timeout=10)
            rows = [row for row in rec.json()["records"]
                    if row["model"] == "stub-b-main"]
            assert rows and rows[0]["key_id"].startswith("drill_pool/"), \
                f"pool key attribution missing: {rows[:1]}"
            assert pool_snapshot()["keys_available"] == 2

            # Phase 1 — both keys rate-limited. The first request cools
            # both keys (raw 429s cascade to the fallback); the second
            # finds no available lease and engages the synthetic-429
            # pool_exhausted path. The client stays 200 throughout.
            httpx.post(f"http://127.0.0.1:{B_STUB}/_mode",
                       json={"mode": "exhaust"}, timeout=5)
            r = _chat(B_URL, "drillB", "exhausted-1")
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["model"] == "stub-b-backup"
            assert body["hermes_router"]["fallback_used"] is True
            r = _chat(B_URL, "drillB", "exhausted-2")
            assert r.status_code == 200, r.text
            assert r.json()["model"] == "stub-b-backup"

            snap = pool_snapshot()
            assert snap["keys_available"] == 0
            per_key = {k["label"]: k for k in snap["keys"]}
            assert per_key["key-x"]["consecutive_429"] >= 1
            assert per_key["key-y"]["consecutive_429"] >= 1

            m = httpx.get(f"{B_URL}/metrics", headers=ADMIN,
                          timeout=10).json()["metrics"]
            assert m.get("pool_exhausted_total", 0) >= 1
            assert m.get("pool_429_total", 0) >= 2

            # Detection: critical pool alert + webhook delivery.
            alerts = httpx.get(f"{B_URL}/admin/alerts", headers=ADMIN,
                               timeout=10).json()
            pool_alerts = [a for a in alerts["active"]
                           if a["rule"] == "pool_exhaustion"]
            assert pool_alerts and pool_alerts[0]["severity"] == "critical"
            deadline = time.time() + 5
            while time.time() < deadline:
                if any(d.get("alert", {}).get("rule") == "pool_exhaustion"
                       for d in _HookHandler.deliveries):
                    break
                time.sleep(0.2)
            assert any(d.get("event") == "raised" and d.get("alert", {})
                       .get("rule") == "pool_exhaustion"
                       for d in _HookHandler.deliveries), \
                "webhook never delivered the pool alert"

            # Phase 2 — natural recovery: retry-after lapses, the pool
            # refills, service returns to the primary tier.
            httpx.post(f"http://127.0.0.1:{B_STUB}/_mode",
                       json={"mode": "healthy"}, timeout=5)
            time.sleep(1.3)
            assert pool_snapshot()["keys_available"] == 2
            r = _chat(B_URL, "drillB", "recovered")
            assert r.status_code == 200
            assert r.json()["model"] == "stub-b-main"

            # Phase 3 — OPERATOR RECOVERY: exhaust again, then unlock
            # (no waiting for the cooldown to lapse).
            httpx.post(f"http://127.0.0.1:{B_STUB}/_mode",
                       json={"mode": "exhaust"}, timeout=5)
            r = _chat(B_URL, "drillB", "exhausted-again")
            assert r.status_code == 200
            assert pool_snapshot()["keys_available"] == 0
            rr = httpx.post(f"{B_URL}/admin/pools/drill_pool/unlock",
                            headers=ADMIN, timeout=10)
            assert rr.status_code == 200, rr.text
            assert rr.json()["keys_reset"] == 2
            assert pool_snapshot()["keys_available"] == 2

            # The alert cleared once the pool refilled.
            httpx.post(f"http://127.0.0.1:{B_STUB}/_mode",
                       json={"mode": "healthy"}, timeout=5)
            alerts = httpx.get(f"{B_URL}/admin/alerts", headers=ADMIN,
                               timeout=10).json()
            assert not [a for a in alerts["active"]
                        if a["rule"] == "pool_exhaustion"], \
                "pool alert never cleared after recovery"
        finally:
            _stop(proc, log, cleanup=tmp)
            hook.shutdown()
            stub_srv.should_exit = True
            time.sleep(0.2)


# ═══════════════════════════════════════════════════════════════════════════
# Drill C — ledger corruption + the R9 backup/restore procedure
# ═══════════════════════════════════════════════════════════════════════════

C_STUB, C_ROUTER, C_HOOK = 9990, 9991, 9992
C_URL = f"http://127.0.0.1:{C_ROUTER}"


def _drill_c_config() -> dict:
    return {
        "server": {"host": "127.0.0.1", "port": C_ROUTER},
        "auth": {"mode": "required", "api_key_env": "DRILL_TEST_KEY"},
        "ledger": {"enabled": True, "db_path": "ledger.sqlite3"},
        "pricing": {
            "default": {"input_per_mtok": 1.0, "output_per_mtok": 1.0},
            "entries": {
                "stub-c-main": {"input_per_mtok": 0.0,
                                "output_per_mtok": 0.0, "free": True}}},
        "alerts": {
            "enabled": True,
            "rules": {"budget_threshold": False, "billing_leak": False,
                      "pool_exhaustion": False, "circuit_open": False,
                      "ledger_append_failures": False,
                      "ledger_verify": True},
            "webhook_url": f"http://127.0.0.1:{C_HOOK}/hook",
            "webhook_timeout_seconds": 2.0},
        "classifier": {
            "model": "stub-drill-classifier",
            "base_url": f"http://127.0.0.1:{C_STUB}/v1",
            "api_key_env": "", "session_timeout_minutes": 5,
            "system_prompt": "Classify:\n{categories}\n{message}",
            "profile_hint": "No profile available.",
            "zero_shot": {"enabled": False},
            "fasttext": {"enabled": False},
            "setfit": {"enabled": False}},
        "routing": {"override_prefix": "/use:",
                    "escalation_keywords": [],
                    "de_escalation_keywords": []},
        "persona": _persona_block("c"),
        "categories": {
            "drillC": {"label": "Drill C", "model": "stub-c-main",
                       "base_url": f"http://127.0.0.1:{C_STUB}/v1",
                       "api_key_env": "", "timeout_seconds": 10},
        },
    }


def _ledger_total() -> int:
    r = httpx.get(f"{C_URL}/admin/ledger/records", headers=ADMIN,
                  params={"limit": 1}, timeout=10)
    assert r.status_code == 200, r.text
    return r.json()["total"]


class TestDrillCLedgerCorruption:
    """R9: "documented backup and recovery drill in Stage 8". The full
    runbook §6/§7 procedure, executed end-to-end: online backup ->
    stop -> tamper -> offline verify names the row -> live detection +
    alert -> restore -> chain green and appending again."""

    def test_corruption_detect_alert_restore(self):
        state = DrillStub(set())
        stub_srv = _start_uvicorn(
            build_drill_stub_app(state, "drillC"), C_STUB)
        hook = _start_webhook(C_HOOK)
        proc, tmp, log = _boot(_drill_c_config(), C_ROUTER, {})
        db = tmp / "ledger.sqlite3"
        backup = tmp / "ledger-backup.sqlite3"
        try:
            # Generate traffic; total is exact (Stage 8 billing fix).
            for i in range(5):
                r = _chat(C_URL, "drillC", f"c-{i}")
                assert r.status_code == 200, r.text
            assert _ledger_total() == 5

            # Runbook §6 online backup (sqlite3 .backup equivalent).
            src = sqlite3.connect(db)
            dst = sqlite3.connect(backup)
            src.backup(dst)
            dst.close()
            src.close()
            assert backup.exists() and backup.stat().st_size > 0

            # Stop writes (runbook §7 step 1): gateway down.
            proc.terminate()
            proc.wait(timeout=8)

            # Tamper with a middle row's cost (field tamper class).
            con = sqlite3.connect(db)
            con.execute("UPDATE ledger SET cost_total_micros = cost_total_micros + 1 "
                        "WHERE seq = 3")
            con.commit()
            con.close()

            # Offline verify (§7 step 2): exit 1, names the first bad seq.
            out = subprocess.run(
                [sys.executable, "-m", "ledger", "verify",
                 "--db", "ledger.sqlite3"],
                cwd=tmp, capture_output=True, text=True, timeout=30)
            assert out.returncode == 1, \
                f"offline verify should fail: {out.stdout} {out.stderr}"
            report = json.loads(out.stdout)
            assert report["ok"] is False
            assert report["first_bad_seq"] == 3, report

            # Live detection: the gateway boots (append-only plane does
            # not refuse on a broken history) and both the verify
            # endpoint and the ledger_verify alert see the damage.
            proc, tmp, log = _boot_in(tmp, _drill_c_config(), C_ROUTER, {})
            r = httpx.get(f"{C_URL}/admin/ledger/verify", headers=ADMIN,
                          timeout=30)
            assert r.status_code == 200, r.text
            assert r.json()["ok"] is False
            assert r.json()["first_bad_seq"] == 3

            httpx.get(f"{C_URL}/admin/status", headers=ADMIN, timeout=10)
            alerts = httpx.get(f"{C_URL}/admin/alerts", headers=ADMIN,
                               timeout=10).json()
            ledger_alerts = [a for a in alerts["active"]
                             if a["rule"] == "ledger_verify"]
            assert ledger_alerts, \
                f"ledger_verify alert never raised: {alerts}"
            assert ledger_alerts[0]["severity"] == "critical"
            deadline = time.time() + 5
            while time.time() < deadline:
                if any(d.get("alert", {}).get("rule") == "ledger_verify"
                       for d in _HookHandler.deliveries):
                    break
                time.sleep(0.2)
            assert any(d.get("alert", {}).get("rule") == "ledger_verify"
                       for d in _HookHandler.deliveries), \
                "webhook never delivered the ledger alert"

            # Recovery (§6 restore + §7 step 3): stop, restore the
            # backup, start — chain green, history intact, appending.
            proc.terminate()
            proc.wait(timeout=8)
            for suffix in ("-wal", "-shm"):
                sidecar = tmp / f"ledger.sqlite3{suffix}"
                if sidecar.exists():
                    sidecar.unlink()
            shutil.copy2(backup, db)
            proc, tmp, log = _boot_in(tmp, _drill_c_config(), C_ROUTER, {})

            r = httpx.get(f"{C_URL}/admin/ledger/verify", headers=ADMIN,
                          timeout=30)
            assert r.status_code == 200, r.text
            assert r.json()["ok"] is True, r.json()
            assert _ledger_total() == 5, "restore lost rows"

            r = _chat(C_URL, "drillC", "after-restore")
            assert r.status_code == 200, r.text
            assert _ledger_total() == 6, "chain not appending after restore"
            r = httpx.get(f"{C_URL}/admin/ledger/verify", headers=ADMIN,
                          timeout=30)
            assert r.json()["ok"] is True, "chain broke on post-restore append"

            # Offline CLI agrees on the restored chain.
            out = subprocess.run(
                [sys.executable, "-m", "ledger", "verify",
                 "--db", "ledger.sqlite3"],
                cwd=tmp, capture_output=True, text=True, timeout=30)
            assert out.returncode == 0, out.stdout + out.stderr
            assert json.loads(out.stdout)["ok"] is True
        finally:
            _stop(proc, log, cleanup=tmp)
            hook.shutdown()
            stub_srv.should_exit = True
            time.sleep(0.2)

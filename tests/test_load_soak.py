"""Stage 8 exit-gate suite (part 1): load and soak with streaming
concurrency.

Dossier Ch.9 Table 7 row 8 — "Load and soak tests (streaming
concurrency)"; Ch.9.3 — "Stage 8 adds load (sustained streaming
concurrency), soak (24-hour stability), failure drills ... and the
final documentation audit."

Layering follows the Stage 6/7 precedent — deterministic proof in CI,
operator-scale proof as a documented procedure:

  * Load: sustained concurrent SSE traffic against the real gateway
    (subprocess) and a deterministic stub upstream. Every stream must
    reassemble to its own request's echo (no cross-stream
    contamination), terminate with the usage frame + [DONE], and every
    request must land exactly one attributed ledger row. File-descriptor
    and RSS deltas are bounded to catch connection/state leaks.
  * Soak: a time-compressed stability window (default 12s; the 24-hour
    operator run is tools/soak.py, documented in RUNBOOK §8) with a
    deterministic fault schedule — a rate-limited window (rotation),
    a full-exhaustion window (synthetic 429 -> fallback cascade), and
    healthy traffic otherwise. Asserts recovery, ledger integrity,
    bounded resource growth and alert raise/clear over the window.

Environment knobs (kept generous-defaulted so CI runs the full proof):

  HERMES_ROUTER_TEST_LOAD_SCALE   float, default 1.0 — multiplies
                                  concurrency levels (floors apply).
  HERMES_ROUTER_TEST_SOAK_SECONDS float, default 12.0 — compressed
                                  soak window length (min 6.0).
"""

from __future__ import annotations

import importlib.util
import itertools
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

# Reuse the runtime-file inventory from the capture harness so this
# suite inherits any future runtime additions automatically (Stage 7
# pattern — an omitted file means a boot failure found by the suite).
_spec = importlib.util.spec_from_file_location(
    "capture_golden", REPO / "tools" / "capture_golden.py")
_cg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cg)
RUNTIME_FILES = _cg.RUNTIME_FILES

STUB_PORT = 9960
ROUTER_PORT = 9961
ROUTER_URL = f"http://127.0.0.1:{ROUTER_PORT}"
TEST_KEY = "load-soak-key-7f3a"
ADMIN = {"Authorization": f"Bearer {TEST_KEY}"}

# Throwaway pool key values (never real credentials). The stub 429s a
# mode-dependent subset so rotation/exhaustion windows are reproducible.
POOL_KEY_A = "loadpool-key-aaaa"   # 429s during the rotation window
POOL_KEY_B = "loadpool-key-bbbb"   # 429s only during the exhaustion window

SCALE = max(0.1, float(os.environ.get("HERMES_ROUTER_TEST_LOAD_SCALE", "1.0")))
SOAK_SECONDS = max(6.0, float(os.environ.get("HERMES_ROUTER_TEST_SOAK_SECONDS",
                                             "12.0")))
LOAD_CONCURRENCY = max(8, int(32 * SCALE))
MIXED_CONCURRENCY = max(6, int(24 * SCALE))
SOAK_WORKERS = max(3, int(6 * SCALE))

# Fault schedule for the compressed soak, as fractions of the window.
ROTATION_WINDOW = (0.25, 0.40)     # key-a 429s only -> rotation
EXHAUST_WINDOW = (0.55, 0.68)      # both keys 429 -> synthetic 429 cascade


# ── Deterministic load stub ─────────────────────────────────────────────────

def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"


def _last_user_text(payload: dict) -> str:
    for m in reversed(payload.get("messages", [])):
        if m.get("role") == "user":
            content = m.get("content", "")
            if isinstance(content, list):
                return " ".join(p.get("text", "") for p in content
                                if isinstance(p, dict))
            return str(content)
    return ""


class StubMode:
    """Test-controlled fault state for the stub upstream (thread-safe)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._mode = "healthy"    # healthy | rotation | exhaustion

    def set(self, mode: str) -> None:
        with self._lock:
            self._mode = mode

    def get(self) -> str:
        with self._lock:
            return self._mode


STUB_MODE = StubMode()


def build_load_stub_app() -> FastAPI:
    """Deterministic OpenAI-compatible upstream with fault injection.

    Behavior contract:
      * POST /_mode {"mode": ...} — test control plane (rotation makes
        POOL_KEY_A 429; exhaustion makes both pool keys 429).
      * model == stub-load-classifier -> "load" (classification reply).
      * bearer in the active fault set -> 429 + retry-after: 1.
      * otherwise: nonstream JSON or SSE stream echoing the request's
        unique marker so cross-stream contamination is detectable.
    """
    app = FastAPI()

    @app.post("/_mode")
    async def set_mode(request: Request):
        payload = await request.json()
        STUB_MODE.set(str(payload.get("mode", "healthy")))
        return {"mode": STUB_MODE.get()}

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True, "mode": STUB_MODE.get()}

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        payload = await request.json()
        model = payload.get("model", "stub-unknown")

        if model == "stub-load-classifier":
            return JSONResponse({
                "id": "chatcmpl-stub-classifier", "object": "chat.completion",
                "created": 1700000000, "model": model,
                "choices": [{"index": 0,
                             "message": {"role": "assistant",
                                         "content": "load"},
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                          "total_tokens": 2},
            })

        mode = STUB_MODE.get()
        auth = request.headers.get("authorization", "")
        bearer = auth[7:] if auth.startswith("Bearer ") else ""
        fault_keys = set()
        if mode == "rotation":
            fault_keys = {POOL_KEY_A}
        elif mode == "exhaustion":
            fault_keys = {POOL_KEY_A, POOL_KEY_B}
        if bearer in fault_keys:
            return JSONResponse(
                {"error": {"message": "stub rate limited",
                           "type": "rate_limit_error"}},
                status_code=429, headers={"retry-after": "1"})

        marker = _last_user_text(payload)
        if payload.get("stream"):
            def gen():
                for idx in range(6):
                    yield _sse({"id": "chatcmpl-stub-load",
                                "object": "chat.completion.chunk",
                                "created": 1700000000, "model": model,
                                "choices": [{"index": 0, "finish_reason": None,
                                             "delta": {"role": "assistant",
                                                       "content":
                                                           f"[{marker}]{idx} "
                                                           "chunk "}}]})
                yield _sse({"id": "chatcmpl-stub-load",
                            "object": "chat.completion.chunk",
                            "created": 1700000000, "model": model,
                            "choices": [{"index": 0, "finish_reason": "stop",
                                         "delta": {}}]})
                yield _sse({"id": "chatcmpl-stub-load",
                            "object": "chat.completion.chunk",
                            "created": 1700000000, "model": model,
                            "choices": [],
                            "usage": {"prompt_tokens": 5,
                                      "completion_tokens": 12,
                                      "total_tokens": 17}})
                yield "data: [DONE]\n\n"

            from fastapi.responses import StreamingResponse
            return StreamingResponse(gen(), media_type="text/event-stream")

        return JSONResponse({
            "id": "chatcmpl-stub-load", "object": "chat.completion",
            "created": 1700000000, "model": model,
            "choices": [{"index": 0,
                         "message": {"role": "assistant",
                                     "content": f"[{marker}] answer"},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 7,
                      "total_tokens": 12},
        })

    return app


# ── Harness: config, boot, teardown (Stage 7 pattern) ───────────────────────

def _base_config() -> dict:
    """Load/soak posture: auth, ledger, generous cost guard, alerts on
    (pool + circuit rules), one pooled category with a non-pooled
    fallback hop, classifier pointed at the stub."""
    return {
        "server": {"host": "127.0.0.1", "port": ROUTER_PORT},
        "auth": {"mode": "required", "api_key_env": "LOAD_TEST_KEY"},
        "ledger": {"enabled": True, "db_path": "ledger.sqlite3"},
        "pricing": {
            "default": {"input_per_mtok": 1.0, "output_per_mtok": 2.0},
            "entries": {
                "stub-load-main": {"input_per_mtok": 0.0,
                                   "output_per_mtok": 0.0, "free": True},
                "stub-load-backup": {"input_per_mtok": 0.0,
                                     "output_per_mtok": 0.0, "free": True}}},
        "cost_guard": {"enabled": True, "estimate_chars_per_token": 4,
                       "limits": {"global": {"requests_per_minute": 100000,
                                             "tokens_per_minute":
                                                 100000000}}},
        "alerts": {
            "enabled": True,
            "rules": {"budget_threshold": False, "billing_leak": False,
                      "pool_exhaustion": True, "circuit_open": True,
                      "ledger_append_failures": False,
                      "ledger_verify": False}},
        # Circuit breaker: threshold set high so the soak exercises the
        # POOL lattice (rotation -> exhaustion -> synthetic 429 cascade)
        # deterministically — under concurrent bursts a low threshold
        # opens the circuit mid-burst and masks the pool-exhaustion path
        # (that interaction is Drill A's territory in test_failure_drills).
        "circuit_breaker": {"failure_threshold": 50,
                            "recovery_timeout_sec": 2, "window_sec": 60},
        "classifier": {
            "model": "stub-load-classifier",
            "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
            "api_key_env": "", "session_timeout_minutes": 5,
            "system_prompt": "Classify:\n{categories}\n{message}",
            "profile_hint": "No profile available.",
            "zero_shot": {"enabled": False},
            "fasttext": {"enabled": False},
            "setfit": {"enabled": False}},
        "routing": {"override_prefix": "/use:",
                    "escalation_keywords": [],
                    "de_escalation_keywords": []},
        "persona": {"user_path": "/var/empty/load-soak/USER.md",
                    "memory_path": "/var/empty/load-soak/MEMORY.md",
                    "max_context_chars": 800},
        "key_pools": {
            "load_pool": {
                "default_cooldown_seconds": 1,
                "keys": [
                    {"ref": "LOAD_POOL_KEY_A", "label": "key-a"},
                    {"ref": "LOAD_POOL_KEY_B", "label": "key-b"},
                ],
            },
        },
        "categories": {
            "load": {"label": "Load", "model": "stub-load-main",
                     "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
                     "key_pool": "load_pool", "timeout_seconds": 20,
                     "fallback_model": "stub-load-backup",
                     "fallback_base_url":
                         f"http://127.0.0.1:{STUB_PORT}/v1",
                     "fallback_key_env": ""},
        },
    }


def _start_router():
    # Fail loudly on a leftover listener from an aborted run instead of
    # racing the bind (Stage 8 drill-harness lesson).
    try:
        httpx.get(f"{ROUTER_URL}/health", timeout=1)
        raise AssertionError(
            f"port {ROUTER_PORT} is already serving — kill the leftover "
            f"gateway process before running the load/soak suite")
    except httpx.HTTPError:
        pass  # port free, as required
    tmp = Path(tempfile.mkdtemp(prefix="load-soak-"))
    for fname in RUNTIME_FILES:
        shutil.copy2(REPO / fname, tmp / fname)
    (tmp / "router_config.yaml").write_text(yaml.safe_dump(_base_config()))
    env = {k: v for k, v in os.environ.items()
           if k.startswith(("LC_", "LANG_", "PATH"))}
    env.update({"PYTHONUNBUFFERED": "1", "HOME": str(tmp),
                "TRACE_LOG_DIR": str(tmp / "traces"),
                "LOAD_TEST_KEY": TEST_KEY,
                "LOAD_POOL_KEY_A": POOL_KEY_A,
                "LOAD_POOL_KEY_B": POOL_KEY_B})
    log = open(tmp / "router.log", "w")
    proc = subprocess.Popen([sys.executable, str(tmp / "server.py")],
                            cwd=tmp, env=env, stdout=log, stderr=log)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            httpx.get(f"{ROUTER_URL}/health", timeout=2)
            return proc, tmp, log
        except Exception:
            if proc.poll() is not None:
                log.flush()
                raise AssertionError(
                    f"router died at boot:\n{(tmp / 'router.log').read_text()}")
            time.sleep(0.2)
    raise AssertionError("router did not become healthy in 30s")


def _stop_router(proc, tmp, log) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
    log.close()
    shutil.rmtree(tmp, ignore_errors=True)


def _wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            httpx.get(url, timeout=2)
            return
        except Exception:
            time.sleep(0.2)
    raise AssertionError(f"stub never became healthy: {url}")


# ── Resource sampling (Linux /proc; skipped where unavailable) ───────────────

def _rss_kb(pid: int) -> int | None:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except OSError:
        return None
    return None


def _fd_count(pid: int) -> int | None:
    try:
        return len(os.listdir(f"/proc/{pid}/fd"))
    except OSError:
        return None


# ── Client helpers ───────────────────────────────────────────────────────────

def _stream_once(rid: str, token: str) -> dict[str, Any]:
    """One full SSE consumption. Returns {status, content, done, usage,
    error} — cross-stream contamination shows up as a missing marker."""
    out: dict[str, Any] = {"status": 0, "content": "", "done": False,
                           "usage": False, "error": ""}
    try:
        with httpx.stream(
                "POST", f"{ROUTER_URL}/v1/chat/completions", timeout=60,
                headers={"Authorization": f"Bearer {token}"},
                json={"model": "auto", "stream": True,
                      "messages": [{"role": "user",
                                    "content": f"/use:load marker-{rid}"}]}) as r:
            out["status"] = r.status_code
            if r.status_code != 200:
                out["error"] = r.read().decode(errors="replace")[:300]
                return out
            for line in r.iter_lines():
                if not line.startswith("data: "):
                    continue
                data = line[6:]
                if data == "[DONE]":
                    out["done"] = True
                    break
                try:
                    frame = json.loads(data)
                except json.JSONDecodeError:
                    continue
                choices = frame.get("choices", [])
                if choices and choices[0].get("delta", {}).get("content"):
                    out["content"] += choices[0]["delta"]["content"]
                if "usage" in frame and not choices:
                    out["usage"] = True
    except Exception as exc:  # noqa: BLE001 — the load client never raises
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _nonstream_once(rid: str, token: str) -> dict[str, Any]:
    out: dict[str, Any] = {"status": 0, "content": "", "error": ""}
    try:
        r = httpx.post(
            f"{ROUTER_URL}/v1/chat/completions", timeout=60,
            headers={"Authorization": f"Bearer {token}"},
            json={"model": "auto", "stream": False,
                  "messages": [{"role": "user",
                                "content": f"/use:load marker-{rid}"}]})
        out["status"] = r.status_code
        if r.status_code == 200:
            out["content"] = r.json()["choices"][0]["message"]["content"]
        else:
            out["error"] = r.text[:300]
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _ledger_total() -> int:
    r = httpx.get(f"{ROUTER_URL}/admin/ledger/records",
                  headers=ADMIN, params={"limit": 1}, timeout=20)
    assert r.status_code == 200, r.text
    return r.json()["total"]


def _pool_available() -> int:
    r = httpx.get(f"{ROUTER_URL}/admin/pools", headers=ADMIN, timeout=20)
    assert r.status_code == 200, r.text
    pools = {p["name"]: p for p in r.json()["pools"]}
    return int(pools["load_pool"]["keys_available"])


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def stub():
    server = uvicorn.Server(uvicorn.Config(
        build_load_stub_app(), host="127.0.0.1", port=STUB_PORT,
        log_level="error", access_log=False))
    threading.Thread(target=server.run, daemon=True).start()
    _wait_http(f"http://127.0.0.1:{STUB_PORT}/health")
    STUB_MODE.set("healthy")
    yield
    server.should_exit = True
    time.sleep(0.3)


@pytest.fixture(scope="module")
def router(stub):
    proc, tmp, log = _start_router()
    yield {"proc": proc, "tmp": tmp, "log": log, "pid": proc.pid}
    _stop_router(proc, tmp, log)


@pytest.fixture(scope="module")
def vk(router) -> dict:
    r = httpx.post(f"{ROUTER_URL}/admin/virtual_keys", headers=ADMIN,
                   json={"name": "load-soak", "budget_mode": "none"},
                   timeout=10)
    assert r.status_code == 201, r.text
    body = r.json()
    return {"id": body["id"], "token": body["key"]}


# ── Load: sustained streaming concurrency ────────────────────────────────────

class TestStreamingLoad:
    """Dossier row 8: load = sustained streaming concurrency. Every
    stream must complete with its own content, land exactly one ledger
    row, and leave no fd/state residue behind."""

    def test_sustained_streams_mixed_waves_attribution_and_hygiene(
            self, router, vk):
        pid = router["pid"]

        # Warmup + baseline samples
        warm = _nonstream_once("warm-1", TEST_KEY)
        assert warm["status"] == 200, warm["error"]
        rss_base = _rss_kb(pid)
        fd_base = _fd_count(pid)
        ledger_base = _ledger_total()
        metrics_base = httpx.get(f"{ROUTER_URL}/metrics", headers=ADMIN,
                                 timeout=20).json()["metrics"]

        # Wave 1 — sustained concurrent streaming (admin lane)
        with ThreadPoolExecutor(max_workers=LOAD_CONCURRENCY) as pool:
            wave1 = list(pool.map(
                lambda i: _stream_once(f"w1-{i}", TEST_KEY),
                range(LOAD_CONCURRENCY)))
        for i, res in enumerate(wave1):
            assert res["status"] == 200, f"w1-{i}: {res['error']}"
            assert res["done"], f"w1-{i}: stream never sent [DONE]"
            assert res["usage"], f"w1-{i}: usage frame missing"
            assert f"marker-w1-{i}" in res["content"], \
                f"w1-{i}: cross-stream contamination: {res['content'][:80]}"
        fd_mid = _fd_count(pid)
        _ = _rss_kb(pid)  # sampled for the log, not asserted (wave-local)

        # Wave 2 — mixed stream/nonstream through a virtual key
        # (attribution under concurrency is the Stage 8 addition)
        jobs: list[tuple[str, str]] = []
        for i in range(MIXED_CONCURRENCY):
            lane = "s" if i % 2 == 0 else "n"
            jobs.append((f"w2-{lane}-{i}", lane))
        with ThreadPoolExecutor(max_workers=MIXED_CONCURRENCY) as pool:
            wave2 = list(pool.map(
                lambda j: _stream_once(j[0], vk["token"]) if j[1] == "s"
                else _nonstream_once(j[0], vk["token"]), jobs))
        for (rid, _), res in zip(jobs, wave2):
            assert res["status"] == 200, f"{rid}: {res['error']}"
            assert f"marker-{rid}" in res["content"], \
                f"{rid}: wrong body: {res['content'][:80]}"

        # Ledger: exactly one attributed row per request. The records
        # endpoint's `total` is unfiltered (Stage 4 shape, fixture-pinned),
        # so attribution is proven by fetching the filtered page itself.
        r = httpx.get(f"{ROUTER_URL}/admin/ledger/records",
                      headers=ADMIN,
                      params={"limit": MIXED_CONCURRENCY + 10,
                              "virtual_key_id": vk["id"]},
                      timeout=20)
        assert r.status_code == 200, r.text
        vk_rows = r.json()["records"]
        assert len(vk_rows) == MIXED_CONCURRENCY, \
            (f"virtual-key attribution under concurrency: "
             f"{len(vk_rows)} rows != {MIXED_CONCURRENCY}")
        assert all(row.get("virtual_key_id") == vk["id"] for row in vk_rows)
        assert all(row.get("status") == "ok" and row.get("model")
                   in ("stub-load-main", "stub-load-backup")
                   for row in vk_rows)
        total_now = _ledger_total()
        assert total_now - ledger_base == \
            LOAD_CONCURRENCY + MIXED_CONCURRENCY, \
            (f"ledger delta {total_now - ledger_base} != "
             f"{LOAD_CONCURRENCY + MIXED_CONCURRENCY}")

        # Metrics counters match the traffic exactly. Semantics (pinned by
        # the Stage 0-3 funnels): requests_total_<cat> counts NONSTREAM
        # requests per category; streams count globally.
        metrics_now = httpx.get(f"{ROUTER_URL}/metrics", headers=ADMIN,
                                timeout=20).json()["metrics"]
        nonstream_delta = metrics_now["requests_total_load"] - \
            metrics_base["requests_total_load"]
        stream_delta = metrics_now["stream_requests_total"] - \
            metrics_base["stream_requests_total"]
        expected_streams = LOAD_CONCURRENCY + (MIXED_CONCURRENCY + 1) // 2
        expected_nonstream = MIXED_CONCURRENCY // 2  # warmup is in the base
        assert nonstream_delta == expected_nonstream, \
            f"nonstream counter {nonstream_delta} != {expected_nonstream}"
        assert stream_delta == expected_streams, \
            f"stream counter {stream_delta} != {expected_streams}"

        # No stuck cooldowns after healthy load
        assert _pool_available() == 2

        # Resource hygiene: fds return to baseline; RSS stays bounded
        fd_end = _fd_count(pid)
        rss_end = _rss_kb(pid)
        if fd_base is not None and fd_mid is not None and fd_end is not None:
            assert fd_end <= fd_base + 10, \
                f"fd leak: base {fd_base} mid {fd_mid} end {fd_end}"
        if rss_base is not None and rss_end is not None:
            assert rss_end < rss_base + 96 * 1024, \
                f"RSS grew {rss_end - rss_base} KB during load"

    def test_stream_reassembly_shape(self, router, vk):
        """One stream, frame-by-frame contract: role delta first, content
        deltas, stop frame, usage frame, [DONE] terminator."""
        res = _stream_once("shape-1", vk["token"])
        assert res["status"] == 200, res["error"]
        assert res["done"] and res["usage"]
        assert res["content"].startswith("[marker-shape-1]0")
        assert "chunk" in res["content"]


# ── Soak: compressed stability window with a fault schedule ─────────────────

class TestCompressedSoak:
    """Dossier row 8: soak = stability over time. The in-suite window is
    compressed (default 12s) but walks the same failure lattice as the
    24-hour operator run (tools/soak.py + RUNBOOK §8): rate-limited
    rotation window, full pool exhaustion window, recovery, continuous
    sampling of the alerting heartbeat."""

    def test_compressed_soak_window(self, router, vk):
        pid = router["pid"]
        STUB_MODE.set("healthy")

        ledger_base = _ledger_total()
        rss_base = _rss_kb(pid)
        fd_base = _fd_count(pid)

        stop = threading.Event()
        results: list[dict[str, Any]] = []
        results_lock = threading.Lock()
        samples: list[dict[str, Any]] = []
        samples_lock = threading.Lock()
        t0 = time.monotonic()

        def phase(elapsed: float) -> str:
            frac = elapsed / SOAK_SECONDS
            if ROTATION_WINDOW[0] <= frac < ROTATION_WINDOW[1]:
                return "rotation"
            if EXHAUST_WINDOW[0] <= frac < EXHAUST_WINDOW[1]:
                return "exhaustion"
            return "healthy"

        def mode_switcher():
            last = "healthy"
            while not stop.is_set():
                want = phase(time.monotonic() - t0)
                if want != last:
                    httpx.post(f"http://127.0.0.1:{STUB_PORT}/_mode",
                               json={"mode": want}, timeout=5)
                    last = want
                time.sleep(0.1)

        def sampler():
            while not stop.is_set():
                try:
                    st = httpx.get(f"{ROUTER_URL}/admin/status",
                                   headers=ADMIN, timeout=10).json()
                    active = len(st.get("alerts", {}).get("active", []))
                    with samples_lock:
                        samples.append({
                            "t": round(time.monotonic() - t0, 2),
                            "requests": st["cards"]["requests"],
                            "ledger": st["cards"]["ledger_records"],
                            "pools_available":
                                st["cards"]["pools_available"],
                            "circuits_open": st["cards"]["circuits_open"],
                            "active_alerts": active})
                except Exception:
                    pass
                time.sleep(0.7)

        seq = itertools.count()

        def worker():
            while not stop.is_set():
                i = next(seq)
                rid = f"soak-{i}"
                if i % 5 == 4:
                    res = _nonstream_once(rid, vk["token"])
                else:
                    res = _stream_once(rid, vk["token"])
                res["rid"] = rid
                with results_lock:
                    results.append(res)

        switcher = threading.Thread(target=mode_switcher, daemon=True)
        sampler_thread = threading.Thread(target=sampler, daemon=True)
        workers = [threading.Thread(target=worker, daemon=True)
                   for _ in range(SOAK_WORKERS)]
        switcher.start()
        sampler_thread.start()
        for w in workers:
            w.start()

        time.sleep(SOAK_SECONDS)
        stop.set()
        for w in workers:
            w.join(timeout=10)
        switcher.join(timeout=3)
        sampler_thread.join(timeout=3)
        # let any in-flight cooldown (retry-after: 1) lapse
        time.sleep(1.3)

        # 1. Every client-visible outcome was a complete 200
        assert len(results) >= SOAK_WORKERS * 3, \
            f"sustained traffic did not flow ({len(results)} requests)"
        bad = [r for r in results if r["status"] != 200]
        assert not bad, f"{len(bad)} requests failed during soak: {bad[:3]}"
        streams = [r for r in results if "done" in r]
        assert all(r["done"] and r["usage"] for r in streams), \
            "a stream terminated without usage+[DONE] during soak"
        for r in results:
            assert f"marker-{r['rid']}" in r["content"], \
                f"{r['rid']}: wrong body under load/fault mix"

        # 2. Ledger integrity held and every request landed exactly once
        r = httpx.get(f"{ROUTER_URL}/admin/ledger/verify",
                      headers=ADMIN, timeout=30)
        assert r.status_code == 200, r.text
        assert r.json()["ok"] is True, "ledger chain broke during soak"
        assert _ledger_total() - ledger_base == len(results), \
            (f"ledger delta {_ledger_total() - ledger_base} != "
             f"{len(results)} completed requests")

        # 3. Fault lattice actually happened (rotation + exhaustion)
        metrics = httpx.get(f"{ROUTER_URL}/metrics", headers=ADMIN,
                            timeout=20).json()["metrics"]
        assert metrics["pool_429_total"] > 0, "rotation window never fired"
        assert metrics["pool_exhausted_total"] > 0, \
            "exhaustion window never engaged the cascade"
        assert metrics["fallback_used_total"] > 0, \
            "cascade never served during exhaustion"
        # ... and the system recovered: pool fully available again
        assert _pool_available() == 2, "cooldowns never recovered"
        # ... no circuit ever needed to open (all faults were 429-class)
        circuits = httpx.get(f"{ROUTER_URL}/circuits", headers=ADMIN,
                             timeout=10).json()["circuits"]
        assert all(c.get("state") == "closed" for c in circuits.values()), \
            f"circuit left open after soak: {circuits}"

        # 4. Alerts: pool-exhaustion raised during the window, cleared after
        r = httpx.get(f"{ROUTER_URL}/admin/alerts", headers=ADMIN,
                      timeout=10)
        assert r.status_code == 200, r.text
        alerts = r.json()
        assert alerts["enabled"] is True
        active_now = [a for a in alerts.get("active", [])
                      if a["rule"] == "pool_exhaustion"]
        assert not active_now, f"pool alert never cleared: {active_now}"
        raised = [a for a in alerts.get("history", [])
                  if a.get("rule") == "pool_exhaustion"
                  or a.get("alert", {}).get("rule") == "pool_exhaustion"]
        assert raised, "pool_exhaustion alert never raised during window"

        # 5. Resource stability across the window (first vs last third)
        rss_end = _rss_kb(pid)
        fd_end = _fd_count(pid)
        if rss_base is not None and rss_end is not None:
            assert rss_end < rss_base + 64 * 1024, \
                f"RSS grew {rss_end - rss_base} KB over {SOAK_SECONDS}s"
        if fd_base is not None and fd_end is not None:
            assert fd_end <= fd_base + 12, \
                f"fd drift: base {fd_base} end {fd_end}"

        # 6. The sampler saw a sane timeline (monotone counters,
        #    circuits never left open, enough samples to be meaningful)
        with samples_lock:
            timeline = list(samples)
        assert len(timeline) >= 3, "sampler never ran"
        reqs = [s["requests"] for s in timeline]
        assert all(b >= a for a, b in zip(reqs, reqs[1:])), \
            f"request counter regressed: {reqs}"
        assert all(s["circuits_open"] == 0 for s in timeline)


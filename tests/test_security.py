"""Stage 1 negative-authorization and security-posture tests.

Exit gate (Chapter 9, Stage 1): negative authz tests pass; no plaintext
secrets on disk; auth fail-closed behavior verified end-to-end against the
real server process (subprocess harness, no mocks of the auth path).
"""
from __future__ import annotations

import importlib.util
import json
import os
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

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))  # for `import server` (redaction unit test)

STUB_PORT = 9911
ROUTER_PORT = 9912
ROUTER_URL = f"http://127.0.0.1:{ROUTER_PORT}"
TEST_KEY = "security-test-key-7f3a"
RUNTIME_FILES = ["server.py", "providers.py", "trace.py", "keystore.py",
                 "keypool.py", "catalog.py",
                 "surrogate_models.py", "zero_shot_classifier.py",
                 "fasttext_classifier.py", "setfit_classifier.py"]

# Reuse the deterministic stub upstream from the capture harness
_spec = importlib.util.spec_from_file_location(
    "capture_golden", REPO / "tools" / "capture_golden.py")
_cg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cg)
build_stub_app = _cg.build_stub_app


# ── Harness ─────────────────────────────────────────────────────────────────

def _base_env(extra: dict) -> dict:
    env = {k: v for k, v in os.environ.items()
           if k.startswith(("LC_", "LANG_", "PATH"))}
    env.update({"PYTHONUNBUFFERED": "1", "HOME": "/tmp", **extra})
    return env


@pytest.fixture(scope="module")
def stub():
    server = uvicorn.Server(uvicorn.Config(
        build_stub_app(), host="127.0.0.1", port=STUB_PORT,
        log_level="error", access_log=False))
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{STUB_PORT}/health", timeout=2).status_code == 200:
                break
        except Exception:
            time.sleep(0.2)
    yield
    server.should_exit = True
    time.sleep(0.3)


def start_router(config: dict, env_extra: dict | None = None):
    """Copy runtime files + write config, start server.py. Returns (proc, tmp)."""
    tmp = Path(tempfile.mkdtemp(prefix="sec-test-"))
    for fname in RUNTIME_FILES:
        shutil.copy2(REPO / fname, tmp / fname)
    import yaml
    (tmp / "router_config.yaml").write_text(yaml.safe_dump(config))
    env = _base_env({"TRACE_LOG_DIR": str(tmp / "traces"), **(env_extra or {})})
    log = open(tmp / "router.log", "w")
    proc = subprocess.Popen([sys.executable, str(tmp / "server.py")],
                            cwd=tmp, env=env, stdout=log, stderr=log)
    return proc, tmp, log


def stop_router(proc, tmp, log):
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
    log.close()
    shutil.rmtree(tmp, ignore_errors=True)


def wait_ready(timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"{ROUTER_URL}/health", timeout=2).status_code == 200:
                return True
        except Exception:
            time.sleep(0.2)
    return False


AUTHED_CONFIG = {
    "server": {"host": "127.0.0.1", "port": ROUTER_PORT},
    "auth": {"mode": "required", "api_key_env": "SEC_TEST_KEY"},
    "classifier": {"model": "stub-classifier",
                   "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
                   "session_timeout_minutes": 5,
                   "zero_shot": {"enabled": False},
                   "fasttext": {"enabled": False},
                   "setfit": {"enabled": False}},
    "routing": {"override_prefix": "/use:",
                "escalation_keywords": [], "de_escalation_keywords": []},
    "categories": {
        "simple": {"label": "Simple", "model": "stub-simple",
                   "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
                   "api_key_env": "", "timeout_seconds": 15},
        "complex": {"label": "Complex", "model": "stub-complex",
                    "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
                    "api_key_env": "", "timeout_seconds": 15},
    },
}


# ── Negative authorization (live server) ────────────────────────────────────

def _err_type(r) -> str:
    """401 detail is wrapped by FastAPI: {"detail": {"error": {...}}}."""
    body = r.json()
    err = body.get("error", {}) or (body.get("detail", {}) or {}).get("error", {})
    return err.get("type", "")


def test_negative_authz_matrix(stub):
    """401 without/wrong token on protected surfaces; /health stays open; valid key passes."""
    proc, tmp, log = start_router(AUTHED_CONFIG, {"SEC_TEST_KEY": TEST_KEY})
    try:
        assert wait_ready(), "router did not become ready"

        # /health: intentionally open (liveness probes carry no credentials)
        assert httpx.get(f"{ROUTER_URL}/health", timeout=5).status_code == 200

        body = {"messages": [{"role": "user", "content": "/use:simple ping"}]}
        # no token
        r = httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=body, timeout=10)
        assert r.status_code == 401 and _err_type(r) == "auth_error"
        # wrong token
        r = httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=body, timeout=10,
                       headers={"Authorization": "Bearer not-the-key"})
        assert r.status_code == 401
        # malformed header
        r = httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=body, timeout=10,
                       headers={"Authorization": TEST_KEY})  # missing Bearer prefix
        assert r.status_code == 401
        # valid token -> full round trip to stub
        r = httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=body, timeout=15,
                       headers={"Authorization": f"Bearer {TEST_KEY}"})
        assert r.status_code == 200
        assert r.json()["choices"][0]["message"]["content"]

        # /metrics protected
        assert httpx.get(f"{ROUTER_URL}/metrics", timeout=5).status_code == 401
        assert httpx.get(f"{ROUTER_URL}/metrics", timeout=5,
                         headers={"Authorization": f"Bearer {TEST_KEY}"}).status_code == 200

        # admin surfaces protected
        for path in ("/admin/config", "/admin/sessions", "/reload", "/circuits"):
            method = "post" if path == "/reload" else "get"
            r = getattr(httpx, method)(f"{ROUTER_URL}{path}", timeout=5)
            assert r.status_code == 401, f"{path} not protected"
    finally:
        stop_router(proc, tmp, log)


def test_server_refuses_authless_start(stub):
    """auth.mode=required (default) with no configured key -> process exits."""
    cfg = json.loads(json.dumps(AUTHED_CONFIG))
    del cfg["auth"]
    proc, tmp, log = start_router(cfg)
    try:
        time.sleep(4)
        assert proc.poll() is not None, "authless server must exit (fail closed)"
        assert proc.returncode != 0
        assert not wait_ready(timeout=1.0)
    finally:
        stop_router(proc, tmp, log)


def test_server_refuses_empty_key_value(stub):
    """Configured-but-unresolvable key -> fail closed (Bug #22 class, at startup)."""
    proc, tmp, log = start_router(AUTHED_CONFIG, {"SEC_TEST_KEY": ""})
    try:
        time.sleep(4)
        assert proc.poll() is not None, "empty-key server must exit (fail closed)"
        assert proc.returncode != 0
    finally:
        stop_router(proc, tmp, log)


def test_explicit_disabled_mode_still_starts_but_flags(stub):
    """Opt-out requires explicit auth.mode: disabled (documented loud warning)."""
    cfg = json.loads(json.dumps(AUTHED_CONFIG))
    cfg["auth"] = {"mode": "disabled", "api_key_env": ""}
    proc, tmp, log = start_router(cfg)
    try:
        assert wait_ready(), "explicitly disabled auth should still boot"
        r = httpx.post(f"{ROUTER_URL}/v1/chat/completions", timeout=10,
                       json={"messages": [{"role": "user", "content": "/use:simple ping"}]})
        assert r.status_code == 200  # anonymous allowed ONLY under explicit opt-out
    finally:
        stop_router(proc, tmp, log)


# ── CORS lockdown (live server) ─────────────────────────────────────────────

def _cors_headers(stub, cors_env: str | None, origin: str = "https://evil.example") -> dict:
    cfg = json.loads(json.dumps(AUTHED_CONFIG))
    env = {"SEC_TEST_KEY": TEST_KEY}
    if cors_env is not None:
        env["CORS_ORIGINS"] = cors_env
    proc, tmp, log = start_router(cfg, env)
    try:
        assert wait_ready()
        r = httpx.get(f"{ROUTER_URL}/health", timeout=5,
                      headers={"Origin": origin})
        return {k.lower(): v for k, v in r.headers.items()}
    finally:
        stop_router(proc, tmp, log)


def test_cors_default_disabled(stub):
    h = _cors_headers(stub, None)
    assert "access-control-allow-origin" not in h, "CORS must be off by default"


def test_cors_wildcard_has_no_credentials(stub):
    h = _cors_headers(stub, "*")
    assert h.get("access-control-allow-origin") == "*"
    assert h.get("access-control-allow-credentials", "false").lower() != "true"


def test_cors_explicit_allowlist_keeps_credentials(stub):
    h = _cors_headers(stub, "https://ops.example", origin="https://ops.example")
    assert h.get("access-control-allow-origin") == "https://ops.example"
    assert h.get("access-control-allow-credentials", "").lower() == "true"


def test_cors_allowlist_blocks_unknown_origin(stub):
    h = _cors_headers(stub, "https://ops.example", origin="https://evil.example")
    assert "access-control-allow-origin" not in h, "non-allowlisted origin must get no ACAO"


# ── Encrypted keystore (in-process unit tests) ──────────────────────────────

@pytest.fixture()
def keystore_env(tmp_path, monkeypatch):
    from cryptography.fernet import Fernet
    store = tmp_path / "keys.enc"
    monkeypatch.setenv("ROUTER_KEYSTORE_KEY", Fernet.generate_key().decode())
    monkeypatch.setenv("ROUTER_KEYSTORE_FILE", str(store))
    return store


def test_keystore_roundtrip_and_at_rest_encryption(keystore_env):
    import keystore
    keystore.put_alias("router-test", "super-secret-value")
    assert keystore.resolve_key("ks:router-test") == "super-secret-value"
    assert keystore.list_aliases() == ["router-test"]
    # No plaintext secret anywhere in the store file bytes
    raw = keystore_env.read_bytes()
    assert b"super-secret-value" not in raw
    assert raw.startswith(b"gAAAA")  # Fernet token envelope


def test_keystore_wrong_master_fails_closed(keystore_env):
    import keystore
    from cryptography.fernet import Fernet
    keystore.put_alias("router-test", "super-secret-value")
    monkey_master = Fernet.generate_key().decode()
    os.environ["ROUTER_KEYSTORE_KEY"] = monkey_master  # rotate to wrong key
    keystone_cache_reset = getattr(keystore, "_cache", None)
    keystore._cache = None  # invalidate in-memory cache
    try:
        with pytest.raises(keystore.KeystoreError):
            keystore.resolve_key("ks:router-test")
    finally:
        os.environ.pop("ROUTER_KEYSTORE_KEY", None)
        keystore._cache = keystone_cache_reset


def test_keystore_missing_alias_fails_closed(keystore_env):
    import keystore
    with pytest.raises(keystore.KeystoreError):
        keystore.resolve_key("ks:does-not-exist")


def test_keystore_cli(keystore_env):
    r = subprocess.run([sys.executable, "-m", "keystore", "put", "cli-alias", "cli-secret"],
                       capture_output=True, text=True, cwd=REPO,
                       env=_base_env({k: v for k, v in os.environ.items()
                                      if k.startswith("ROUTER_KEYSTORE")}))
    assert r.returncode == 0, r.stderr
    r = subprocess.run([sys.executable, "-m", "keystore", "list"],
                       capture_output=True, text=True, cwd=REPO,
                       env=_base_env({k: v for k, v in os.environ.items()
                                      if k.startswith("ROUTER_KEYSTORE")}))
    assert "cli-alias" in r.stdout


def test_auth_key_resolved_from_keystore_e2e(stub, keystore_env):
    """Full loop: admin key lives only in the encrypted store, never in env."""
    import keystore
    keystore.put_alias("router-admin", TEST_KEY)
    cfg = json.loads(json.dumps(AUTHED_CONFIG))
    cfg["auth"]["api_key_env"] = "ks:router-admin"
    proc, tmp, log = start_router(cfg, {"ROUTER_KEYSTORE_KEY": os.environ["ROUTER_KEYSTORE_KEY"],
                                        "ROUTER_KEYSTORE_FILE": os.environ["ROUTER_KEYSTORE_FILE"]})
    try:
        assert wait_ready()
        r = httpx.post(f"{ROUTER_URL}/v1/chat/completions", timeout=15,
                       json={"messages": [{"role": "user", "content": "/use:simple ping"}]},
                       headers={"Authorization": f"Bearer {TEST_KEY}"})
        assert r.status_code == 200
    finally:
        stop_router(proc, tmp, log)


# ── Log redaction (unit) ────────────────────────────────────────────────────

def test_log_redaction_scrubs_tokens_and_keys():
    import logging
    import server  # module-level import attaches the filter to root handler
    flt = server.RedactFilter()
    rec = logging.LogRecord("t", logging.INFO, "f", 1,
                            "upstream call with Authorization: Bearer abc123.def-456 sent", (), None)
    assert flt.filter(rec) is True
    assert "abc123.def-456" not in rec.getMessage()
    assert "[REDACTED]" in rec.getMessage()

    rec2 = logging.LogRecord("t", logging.INFO, "f", 1,
                             "key sk-a1b2c3d4e5f6g7h8 leaked", (), None)
    flt.filter(rec2)
    assert "sk-a1b2c3d4e5f6g7h8" not in rec2.getMessage()
    assert "[REDACTED-KEY]" in rec2.getMessage()

    # Normal messages pass through untouched
    rec3 = logging.LogRecord("t", logging.INFO, "f", 1,
                             "Routing session abc -> simple", (), None)
    msg_before = rec3.getMessage()
    flt.filter(rec3)
    assert rec3.getMessage() == msg_before

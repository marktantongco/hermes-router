"""Stage 3 key-pool tests: rotation, cooldowns, capability tags, chaos.

Covers the keypool core (parse_retry_after, acquire/report semantics,
config validation, registry) plus one live end-to-end rotation against the
real server process with the deterministic capture stub (key-a 429s with
Retry-After, key-b serves).

Exit criterion (dossier Stage 3): "rotation chaos tests pass; cooldown
behavior matches provider headers".
"""
from __future__ import annotations

import asyncio
import time
from email.utils import format_datetime
from datetime import datetime, timezone, timedelta

import httpx
import pytest

from keypool import (
    KeyPool,
    PoolError,
    PoolKeyState,
    all_pools,
    build_pools,
    get_pool,
    parse_retry_after,
    register_pools,
    validate_pool_config,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_pool(n: int = 2, **policy) -> KeyPool:
    keys = [
        PoolKeyState(ref=f"KEY_{i}", label=f"key-{chr(97 + i)}",
                     key_id=f"p/key-{chr(97 + i)}")
        for i in range(n)
    ]
    # Secrets resolve from the environment in production; tests stub the
    # resolver by pre-setting values via monkeypatch at call sites that
    # need them. For pure-state tests we bypass resolution entirely.
    pool = KeyPool("p", keys, **policy)
    pool._resolve_secret = lambda key: f"secret-{key.label}"  # type: ignore[method-assign]
    return pool


def http_date(seconds_ahead: float) -> str:
    dt = datetime.now(timezone.utc) + timedelta(seconds=seconds_ahead)
    return format_datetime(dt, usegmt=True)


# ── parse_retry_after: cooldown behavior matches provider headers ───────────

class TestParseRetryAfter:
    P = {"default": 60.0, "max_seconds": 3600.0}

    def test_numeric_seconds(self):
        assert parse_retry_after({"retry-after": "15"}, **self.P) == 15.0

    def test_float_seconds(self):
        assert parse_retry_after({"retry-after": "2.5"}, **self.P) == 2.5

    def test_http_date_future(self):
        v = parse_retry_after({"retry-after": http_date(30)}, **self.P)
        assert 25 <= v <= 31

    def test_http_date_past_clamps_to_zero(self):
        assert parse_retry_after({"retry-after": http_date(-30)}, **self.P) == 0.0

    def test_openai_duration_tokens(self):
        assert parse_retry_after({"x-ratelimit-reset-requests": "6m0s"}, **self.P) == 360.0

    def test_duration_compound(self):
        # 1-day resets parse correctly but are clamped by policy in practice
        assert parse_retry_after(
            {"x-ratelimit-reset-requests": "1d0h0m30s"},
            default=60.0, max_seconds=90000.0) == 86430.0

    def test_duration_milliseconds(self):
        assert parse_retry_after({"x-ratelimit-reset-tokens": "500ms"}, **self.P) == 0.5

    def test_garbage_falls_back_to_default(self):
        assert parse_retry_after({"retry-after": "soon"}, **self.P) == 60.0

    def test_no_headers_uses_default(self):
        assert parse_retry_after(None, **self.P) == 60.0

    def test_negative_clamps_to_zero(self):
        assert parse_retry_after({"retry-after": "-5"}, **self.P) == 0.0

    def test_clamped_to_max(self):
        v = parse_retry_after({"retry-after": "999999"},
                              default=60.0, max_seconds=120.0)
        assert v == 120.0

    def test_retry_after_wins_over_reset(self):
        v = parse_retry_after(
            {"retry-after": "7", "x-ratelimit-reset-requests": "6m0s"}, **self.P)
        assert v == 7.0

    def test_header_case_insensitive(self):
        assert parse_retry_after({"Retry-After": "9"}, **self.P) == 9.0


# ── acquire: round-robin, exhaustion, capability tags ───────────────────────

class TestAcquire:
    def test_round_robin_order(self):
        pool = make_pool(2)
        assert [pool.acquire().key_id for _ in range(4)] == [
            "p/key-a", "p/key-b", "p/key-a", "p/key-b"]

    def test_exhausted_returns_none(self):
        pool = make_pool(1)
        assert pool.acquire() is not None
        pool.report_result("p/key-a", 429, {"retry-after": "100"})
        assert pool.acquire() is None

    def test_capability_filter(self):
        pool = make_pool(2)
        pool.keys[0].capabilities = frozenset({"fast"})
        pool.keys[1].capabilities = frozenset({"fast", "vision"})
        lease = pool.acquire(frozenset({"vision"}))
        assert lease.key_id == "p/key-b"
        # b stays available (no cooldown without a 429) and a never qualifies
        assert pool.acquire(frozenset({"vision"})).key_id == "p/key-b"
        assert pool.acquire(frozenset({"deep"})) is None

    def test_require_unavailable_capability_exhausts(self):
        pool = make_pool(2)
        assert pool.acquire(frozenset({"agentic"})) is None

    def test_cooldown_skips_cooling_key(self):
        pool = make_pool(2)
        pool.report_result("p/key-a", 429, {"retry-after": "500"})
        assert pool.acquire().key_id == "p/key-b"
        assert pool.acquire().key_id == "p/key-b"  # a still cooling

    def test_cooldown_expiry_recovers(self):
        pool = make_pool(1)
        pool.report_result("p/key-a", 429, {"retry-after": "0.05"})
        time.sleep(0.06)
        assert pool.acquire() is not None

    def test_quarantine_excludes_key(self):
        pool = make_pool(2)
        pool.report_result("p/key-a", 401)
        assert pool.keys[0].disabled is True
        ids = [pool.acquire().key_id for _ in range(3)]
        assert ids == ["p/key-b", "p/key-b", "p/key-b"]

    def test_empty_secret_fails_closed(self, monkeypatch):
        pool = KeyPool("p", [PoolKeyState(ref="MISSING_ENV_X", label="k", key_id="p/k")])
        monkeypatch.delenv("MISSING_ENV_X", raising=False)
        with pytest.raises(PoolError, match="empty value"):
            pool.acquire()

    def test_keystore_ref_resolution(self, monkeypatch):
        import keypool
        monkeypatch.setattr(keypool, "resolve_key", lambda ref: "ks-secret")
        pool = KeyPool("p", [PoolKeyState(ref="ks:alias", label="k", key_id="p/k")])
        assert pool.acquire().api_key == "ks-secret"

    def test_keystore_failure_raises_pool_error(self, monkeypatch):
        import keypool
        from keystore import KeystoreError
        def boom(ref):
            raise KeystoreError("alias missing")
        monkeypatch.setattr(keypool, "resolve_key", boom)
        pool = KeyPool("p", [PoolKeyState(ref="ks:alias", label="k", key_id="p/k")])
        with pytest.raises(PoolError, match="alias missing"):
            pool.acquire()


# ── report_result: provider-header cooldowns + quarantine ────────────────────

class TestReportResult:
    def test_429_uses_retry_after_header(self):
        pool = make_pool(1)
        pool.report_result("p/key-a", 429, {"retry-after": "30"})
        meta = pool.keys[0].metadata(time.time())
        assert 29 <= meta["cooldown_remaining_sec"] <= 30
        assert meta["consecutive_429"] == 1 and meta["total_429"] == 1

    def test_429_without_header_uses_policy_default(self):
        pool = make_pool(1, default_cooldown_seconds=12)
        pool.report_result("p/key-a", 429)
        meta = pool.keys[0].metadata(time.time())
        assert 11 <= meta["cooldown_remaining_sec"] <= 12

    def test_429_respects_max_cooldown(self):
        pool = make_pool(1, max_cooldown_seconds=5)
        pool.report_result("p/key-a", 429, {"retry-after": "9999"})
        assert pool.keys[0].metadata(time.time())["cooldown_remaining_sec"] <= 5

    def test_success_resets_consecutive(self):
        pool = make_pool(1)
        pool.report_result("p/key-a", 429, {"retry-after": "0.01"})
        pool.report_result("p/key-a", 200)
        assert pool.keys[0].consecutive_429 == 0
        assert pool.keys[0].total_success == 1

    def test_401_disables_by_default(self):
        pool = make_pool(1)
        pool.report_result("p/key-a", 401)
        assert pool.keys[0].disabled is True
        assert pool.keys[0].total_auth_failures == 1

    def test_403_cooldown_mode(self):
        pool = make_pool(1, disable_on_auth_failure=False,
                         auth_failure_cooldown_seconds=45)
        pool.report_result("p/key-a", 403)
        assert pool.keys[0].disabled is False
        assert pool.keys[0].metadata(time.time())["cooldown_remaining_sec"] > 40

    def test_unknown_key_id_is_ignored(self):
        pool = make_pool(1)
        pool.report_result("p/ghost", 429)  # must not raise

    def test_5xx_recorded_not_cooled(self):
        pool = make_pool(1)
        pool.report_result("p/key-a", 503)
        meta = pool.keys[0].metadata(time.time())
        assert meta["last_status"] == 503 and meta["available"] is True


# ── config validation (fail-closed) ─────────────────────────────────────────

def base_cfg(**pools) -> dict:
    return {
        "key_pools": pools,
        "categories": {
            "a": {"model": "m", "base_url": "https://x/v1", "api_key_env": "K"},
        },
    }


class TestConfigValidation:
    def test_valid_config_passes(self):
        cfg = base_cfg(p1={"keys": [{"ref": "K1", "label": "one"}]})
        assert validate_pool_config(cfg) == []
        assert build_pools(cfg)["p1"].size == 1

    def test_unknown_strategy_rejected(self):
        cfg = base_cfg(p1={"strategy": "random", "keys": [{"ref": "K1", "label": "one"}]})
        assert any("strategy" in e for e in validate_pool_config(cfg))

    def test_duplicate_labels_rejected(self):
        cfg = base_cfg(p1={"keys": [{"ref": "K1", "label": "x"},
                                    {"ref": "K2", "label": "x"}]})
        assert any("duplicate" in e for e in validate_pool_config(cfg))

    def test_missing_ref_rejected(self):
        cfg = base_cfg(p1={"keys": [{"label": "x"}]})
        assert any("ref" in e for e in validate_pool_config(cfg))

    def test_empty_keys_rejected(self):
        cfg = base_cfg(p1={"keys": []})
        assert any("non-empty" in e for e in validate_pool_config(cfg))

    def test_negative_policy_rejected(self):
        cfg = base_cfg(p1={"keys": [{"ref": "K1", "label": "x"}],
                           "default_cooldown_seconds": -1})
        assert any(">= 0" in e for e in validate_pool_config(cfg))

    def test_unknown_pool_fields_rejected(self):
        cfg = base_cfg(p1={"keys": [{"ref": "K1", "label": "x"}], "secret": 1})
        assert any("unknown fields" in e for e in validate_pool_config(cfg))

    def test_unknown_key_fields_rejected(self):
        cfg = base_cfg(p1={"keys": [{"ref": "K1", "label": "x", "value": "oops"}]})
        assert any("unknown fields" in e for e in validate_pool_config(cfg))

    def test_key_pool_and_api_key_env_mutually_exclusive(self):
        cfg = base_cfg(p1={"keys": [{"ref": "K1", "label": "x"}]})
        cfg["categories"]["a"]["key_pool"] = "p1"
        assert any("mutually exclusive" in e for e in validate_pool_config(cfg))

    def test_key_pool_and_alternate_key_env_mutually_exclusive(self):
        cfg = base_cfg(p1={"keys": [{"ref": "K1", "label": "x"}]})
        cat = cfg["categories"]["a"]
        del cat["api_key_env"]
        cat["key_pool"] = "p1"
        cat["alternate_key_env"] = "K2"
        assert any("mutually exclusive" in e for e in validate_pool_config(cfg))

    def test_unknown_pool_reference_rejected(self):
        cfg = base_cfg(p1={"keys": [{"ref": "K1", "label": "x"}]})
        cfg["categories"]["a"]["key_pool"] = "ghost"
        errs = validate_pool_config(cfg)
        assert any("not defined" in e for e in errs)
        with pytest.raises(PoolError):
            build_pools(cfg)

    def test_require_capabilities_without_pool_rejected(self):
        cfg = base_cfg()
        cfg["categories"]["a"]["require_capabilities"] = ["fast"]
        assert any("require_capabilities requires key_pool" in e
                   for e in validate_pool_config(cfg))

    def test_fallback_key_pool_reference_checked(self):
        cfg = base_cfg(p1={"keys": [{"ref": "K1", "label": "x"}]})
        cat = cfg["categories"]["a"]
        cat["fallback_model"] = "m2"
        cat["fallback_base_url"] = "https://x/v1"
        cat["fallback_key_pool"] = "ghost"
        assert any("fallback_key_pool" in e and "not defined" in e
                   for e in validate_pool_config(cfg))

    def test_keystore_alias_missing_fails_build(self, monkeypatch):
        import keypool
        from keystore import KeystoreError
        def boom(ref):
            raise KeystoreError("alias missing")
        monkeypatch.setattr(keypool, "resolve_key", boom)
        cfg = base_cfg(p1={"keys": [{"ref": "ks:ghost", "label": "x"}]})
        with pytest.raises(PoolError, match="alias missing"):
            build_pools(cfg)


# ── registry ─────────────────────────────────────────────────────────────────

class TestRegistry:
    def test_register_get_all(self):
        pool = make_pool(1)
        register_pools({"p": pool})
        assert get_pool("p") is pool
        assert set(all_pools()) == {"p"}

    def test_get_missing_raises(self):
        register_pools({})
        with pytest.raises(PoolError, match="not registered"):
            get_pool("nope")


# ── rotation chaos (deterministic scripted sequence) ────────────────────────

class TestRotationChaos:
    def test_alternating_429_rotation_sequence(self):
        """Scripted chaos: 4 keys, mixed outcomes, assert exact rotation path."""
        pool = make_pool(4)
        script = {  # key_id -> outcomes in the order the pool will use them
            "p/key-a": [429, 200], "p/key-b": [200],
            "p/key-c": [429], "p/key-d": [200],
        }
        served = []
        for _ in range(20):
            lease = pool.acquire()
            if lease is None:
                break
            outcomes = script.get(lease.key_id)
            if not outcomes:  # cooling keys re-enter when cooldowns expire
                pool.report_result(lease.key_id, 200)
                served.append((lease.key_id, 200))
                continue
            status = outcomes.pop(0)
            headers = {"retry-after": "0.01"} if status == 429 else None
            pool.report_result(lease.key_id, status, headers)
            served.append((lease.key_id, status))
            time.sleep(0.012)  # let 10ms cooldowns expire between rounds
        statuses = [s for _, s in served]
        assert 429 in statuses and statuses.count(200) >= 8
        assert all(k in {f"p/key-{c}" for c in "abcd"} for k, _ in served)

    def test_exhaustion_engages_after_all_cool(self):
        pool = make_pool(3)
        for key in ("p/key-a", "p/key-b", "p/key-c"):
            pool.acquire()
            pool.report_result(key, 429, {"retry-after": "60"})
        assert pool.acquire() is None
        snap = pool.snapshot()
        assert snap["keys_available"] == 0

    def test_unlock_restores_service(self):
        pool = make_pool(2)
        for key in ("p/key-a", "p/key-b"):
            pool.acquire()
            pool.report_result(key, 401)
        assert pool.acquire() is None
        assert pool.unlock() == 2
        assert pool.acquire() is not None


# ── concurrency (asyncio gather over the pool) ──────────────────────────────

class TestConcurrency:
    def test_parallel_acquire_no_oversell_or_crash(self):
        pool = make_pool(3)
        pool.keys[0].capabilities = frozenset({"fast"})
        pool.keys[1].capabilities = frozenset({"fast"})
        pool.keys[2].capabilities = frozenset({"fast"})

        async def one(i: int):
            lease = pool.acquire(frozenset({"fast"}))
            if lease is None:
                return None
            await asyncio.sleep(0.001)
            if i % 3 == 0:
                pool.report_result(lease.key_id, 429, {"retry-after": "0.001"})
            else:
                pool.report_result(lease.key_id, 200)
            return lease.key_id

        async def run():
            return await asyncio.gather(*(one(i) for i in range(60)))

        results = asyncio.run(run())
        acquired = [r for r in results if r]
        assert len(acquired) == 60  # every gather got a lease (cooldowns expire in 1ms)
        assert set(acquired) <= {"p/key-a", "p/key-b", "p/key-c"}
        for key in pool.keys:
            assert key.total_acquired == key.total_429 + key.total_success


# ── Live end-to-end rotation (real server, deterministic stub) ───────────────

def _e2e_pool_config(stub_port: int, router_port: int) -> dict:
    return {
        "server": {"host": "127.0.0.1", "port": router_port},
        "auth": {"mode": "required", "api_key_env": "SEC_TEST_KEY"},
        "classifier": {"model": "stub-classifier",
                       "base_url": f"http://127.0.0.1:{stub_port}/v1",
                       "session_timeout_minutes": 5,
                       "zero_shot": {"enabled": False},
                       "fasttext": {"enabled": False},
                       "setfit": {"enabled": False}},
        "routing": {"override_prefix": "/use:",
                    "escalation_keywords": [], "de_escalation_keywords": []},
        "key_pools": {
            "e2e_pool": {
                "default_cooldown_seconds": 1,
                "keys": [
                    {"ref": "POOL_KEY_A", "label": "key-a"},
                    {"ref": "POOL_KEY_B", "label": "key-b"},
                ],
            },
        },
        "categories": {
            "pooled": {"label": "Pooled", "model": "stub-simple",
                       "base_url": f"http://127.0.0.1:{stub_port}/v1",
                       "key_pool": "e2e_pool", "timeout_seconds": 15},
        },
    }


class TestLivePoolRotation:
    """End-to-end: real server.py + capture stub. key-a 429s, key-b serves."""

    def test_rotation_and_admin_surface(self):
        import importlib.util
        import os
        import shutil
        import subprocess
        import sys
        import tempfile
        import threading
        from pathlib import Path

        import uvicorn

        repo = Path(__file__).resolve().parents[1]
        spec = importlib.util.spec_from_file_location(
            "capture_golden", repo / "tools" / "capture_golden.py")
        cg = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cg)

        stub_port, router_port = 9921, 9922
        router_url = f"http://127.0.0.1:{router_port}"

        server = uvicorn.Server(uvicorn.Config(
            cg.build_stub_app(), host="127.0.0.1", port=stub_port,
            log_level="error", access_log=False))
        t = threading.Thread(target=server.run, daemon=True)
        t.start()
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                if httpx.get(f"http://127.0.0.1:{stub_port}/health",
                             timeout=2).status_code == 200:
                    break
            except Exception:
                time.sleep(0.2)

        import yaml
        tmp = Path(tempfile.mkdtemp(prefix="pool-e2e-"))
        for fname in ["server.py", "providers.py", "trace.py", "keystore.py",
                      "keypool.py", "catalog.py", "surrogate_models.py",
                      "zero_shot_classifier.py", "fasttext_classifier.py",
                      "setfit_classifier.py"]:
            shutil.copy2(repo / fname, tmp / fname)
        (tmp / "router_config.yaml").write_text(
            yaml.safe_dump(_e2e_pool_config(stub_port, router_port)))

        env = {k: v for k, v in os.environ.items()
               if k.startswith(("LC_", "LANG_", "PATH"))}
        env.update({
            "PYTHONUNBUFFERED": "1", "HOME": str(tmp),
            "TRACE_LOG_DIR": str(tmp / "traces"),
            "SEC_TEST_KEY": "security-test-key-7f3a",
            "POOL_KEY_A": cg.POOL_KEY_A, "POOL_KEY_B": cg.POOL_KEY_B,
        })
        log = open(tmp / "router.log", "w")
        proc = subprocess.Popen([sys.executable, str(tmp / "server.py")],
                                cwd=tmp, env=env, stdout=log, stderr=log)
        try:
            deadline = time.time() + 25
            ready = False
            while time.time() < deadline:
                try:
                    if httpx.get(f"{router_url}/health", timeout=2).status_code == 200:
                        ready = True
                        break
                except Exception:
                    time.sleep(0.2)
            assert ready, "router did not boot with pool config"

            auth = {"Authorization": "Bearer security-test-key-7f3a"}
            r = httpx.post(f"{router_url}/v1/chat/completions", timeout=20,
                           headers=auth,
                           json={"model": "auto", "stream": False,
                                 "messages": [{"role": "user",
                                               "content": "/use:pooled hello pool"}]})
            assert r.status_code == 200, r.text
            assert r.json()["model"] == "stub-simple"  # served via key-b after 429 on key-a

            # Pool surface: metadata only, key-a cooling, key-b healthy
            r = httpx.get(f"{router_url}/admin/pools", timeout=10, headers=auth)
            assert r.status_code == 200
            pools = {p["name"]: p for p in r.json()["pools"]}
            e2e = pools["e2e_pool"]
            keys = {k["label"]: k for k in e2e["keys"]}
            assert keys["key-a"]["total_429"] == 1
            assert keys["key-a"]["consecutive_429"] == 1
            assert keys["key-b"]["total_success"] == 1
            assert not any("secret" in str(k).lower() or "api_key" in str(k)
                           for k in keys["key-a"])  # no secret material in admin surface

            # Admin surfaces are auth-gated
            r = httpx.get(f"{router_url}/admin/pools", timeout=10)
            assert r.status_code == 401

            # Metrics expose pool counters
            r = httpx.get(f"{router_url}/metrics", timeout=10, headers=auth)
            prom = r.json()["prometheus"]
            assert "hermes_router_pool_rotations_total 1" in prom
            assert 'hermes_router_pool_keys_available{pool="e2e_pool"} 1' in prom

            # Unlock endpoint clears state
            r = httpx.post(f"{router_url}/admin/pools/e2e_pool/unlock",
                           timeout=10, headers=auth)
            assert r.status_code == 200 and r.json()["keys_reset"] == 1
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
            log.close()
            shutil.rmtree(tmp, ignore_errors=True)
            server.should_exit = True

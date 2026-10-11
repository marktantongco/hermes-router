"""Stage 6 contracts for the operations dashboard (dashboard.py + endpoints).

Part A pins the pure renderer: total rendering (any subset of ctx keys),
the stable section-id contract, XSS escaping, pagination, the zero-
external-assets rule (no scripts, no CDN, inline CSS only).

Part B pins the live surfaces on a real server process: the auth matrix
on /dashboard (401 / 403 virtual key / 200 admin), the JSON variant,
/admin/status, /admin/alerts (raise -> webhook -> no double-fire),
the billing-leak path, /admin/alerts/test, fail-soft webhook delivery
on a 500 receiver, and hot-reload swap/clear semantics for the engine.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx
import importlib.util
import pytest
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from dashboard import (  # noqa: E402
    DASHBOARD_SECTIONS,
    PAGE_SIZE,
    page_count,
    render_dashboard,
)

# Reuse the deterministic stub upstream from the capture harness
_spec = importlib.util.spec_from_file_location(
    "capture_golden", REPO / "tools" / "capture_golden.py")
_cg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cg)
build_stub_app = _cg.build_stub_app

STUB_PORT = 9930
ROUTER_PORT = 9931
WEBHOOK_PORT = 9932
ROUTER_URL = f"http://127.0.0.1:{ROUTER_PORT}"
DASH = f"{ROUTER_URL}/dashboard"
STATUS = f"{ROUTER_URL}/admin/status"
ALERTS = f"{ROUTER_URL}/admin/alerts"
TEST_KEY = "dashboard-test-key-4b1c"
AUTH = {"Authorization": f"Bearer {TEST_KEY}"}


# ── Part A: pure renderer ───────────────────────────────────────────────────

def _full_ctx() -> dict:
    return {
        "title": "Hermes Router — Operations Dashboard",
        "generated_at": "2026-10-11T00:00:00.000Z",
        "uptime_seconds": 3725,
        "version": "1.0.0",
        "features": {"billing": True, "cost_guard": True, "alerts": True,
                     "pools": True, "catalog": True},
        "server": {"host": "127.0.0.1", "port": ROUTER_PORT},
        "cards": {"requests": 12, "errors": 0, "sessions": 2,
                  "circuits_open": 0, "pools_available": 3,
                  "ledger_records": 12, "budget_blocks": 1,
                  "cost_guard_blocks": 0},
        "alerts": {"enabled": True, "active": [
            {"id": "pool:free_tier", "rule": "pool_exhaustion",
             "severity": "critical", "title": "Key pool exhausted",
             "detail": {"pool": "free_tier", "size": 2},
             "first_seen": "2026-10-11T00:00:00Z",
             "last_seen": "2026-10-11T00:00:00Z", "state": "active"}],
            "raised_now": [], "cleared_now": [], "history": [],
            "config": {}, "stats": {}},
        "spend": {
            "by_model": [{"dim": "stub-complex", "requests": 2,
                          "prompt_tokens": 10, "completion_tokens": 12,
                          "total_tokens": 22, "cost_total_micros": 72}],
            "by_key": [{"dim": "none", "requests": 2, "prompt_tokens": 10,
                        "completion_tokens": 12, "total_tokens": 22,
                        "cost_total_micros": 72}],
            "by_day": [{"dim": "2026-10-11", "requests": 2,
                        "prompt_tokens": 10, "completion_tokens": 12,
                        "total_tokens": 22, "cost_total_micros": 72}]},
        "budgets": [
            {"id": "vk-aaaa1111", "name": "subscriber-a", "enabled": True,
             "budget_mode": "none", "budget_limit": 0,
             "budget_period": "total", "used": 0, "blocked": False,
             "used_ratio": None},
            {"id": "vk-bbbb2222", "name": "subscriber-b", "enabled": True,
             "budget_mode": "tokens", "budget_limit": 100,
             "budget_period": "day", "used": 90, "blocked": False,
             "used_ratio": 0.9},
            {"id": "vk-cccc3333", "name": "subscriber-c", "enabled": True,
             "budget_mode": "cost_micros", "budget_limit": 36,
             "budget_period": "total", "used": 36, "blocked": True,
             "used_ratio": 1.0}],
        "ledger": {"enabled": True, "total": 60, "page": 1, "pages": 3,
                   "page_size": PAGE_SIZE, "vk_filter": None,
                   "rows": [{"seq": 60, "ts": "2026-10-11T00:00:01.000Z",
                             "record_type": "usage", "status": "ok",
                             "model": "stub-complex", "provider": "openai",
                             "virtual_key_id": "vk-bbbb2222",
                             "prompt_tokens": 5, "completion_tokens": 6,
                             "cost_total_micros": 36, "fallback_level": 0,
                             "stream": 0}]},
        "pools": [{"name": "free_tier", "strategy": "round_robin", "size": 2,
                   "keys_available": 0, "policy": {}, "keys": []}],
        "circuits": [{"key": "stub__complex", "state": "open",
                      "failures": 3}],
        "cost_guard": {"enabled": True,
                       "watchdog": {"billing_visibility": "ok",
                                    "events_total": 1,
                                    "completions_total": 5,
                                    "unbilled_completions": 0},
                       "reconciliation": {"estimates_total": 5,
                                          "reconciled_total": 5,
                                          "unreconciled_total": 0,
                                          "drift_tokens_total": 4},
                       "limiters": [{"name": "global", "scope": "global",
                                     "dimension": "requests", "limit": 100,
                                     "admits": 5, "blocks": 0}]},
        "catalog": {"provider_count": 2, "source": "bundled",
                    "refreshed_at": "never", "providers": [
                        {"name": "groq", "confidence": "high",
                         "limits": {"requests_per_minute": 30,
                                    "tokens_per_minute": 7500,
                                    "requests_per_day": 1440}}]},
        "query": {"refresh": None},
    }


def test_renders_all_stable_section_ids():
    html_out = render_dashboard(_full_ctx())
    assert html_out.startswith("<!doctype html>")
    for sid in DASHBOARD_SECTIONS:
        assert f'id="{sid}"' in html_out, sid


def test_zero_external_assets_contract():
    html_out = render_dashboard(_full_ctx())
    assert "<script" not in html_out          # no JavaScript, period
    assert "src=" not in html_out             # no images/iframes/scripts
    assert "http://" not in html_out.replace(
        f"http://127.0.0.1:{ROUTER_PORT}", "")  # no absolute remote refs
    assert "@import" not in html_out          # no remote CSS
    assert "<style>" in html_out              # inline CSS only


def test_xss_escaping_everywhere():
    ctx = _full_ctx()
    hostile = '<script>alert("xss")</script>'
    ctx["budgets"] = [{"id": "vk-x", "name": hostile, "enabled": True,
                       "budget_mode": "tokens", "budget_limit": 10,
                       "budget_period": "day", "used": 9, "blocked": False,
                       "used_ratio": 0.9}]
    ctx["spend"]["by_model"] = [{"dim": hostile, "requests": 1,
                                 "prompt_tokens": 1, "completion_tokens": 1,
                                 "total_tokens": 2, "cost_total_micros": 1}]
    ctx["ledger"]["rows"] = [dict(_full_ctx()["ledger"]["rows"][0],
                                  model=hostile)]
    ctx["ledger"]["vk_filter"] = hostile
    ctx["alerts"]["active"] = [dict(_full_ctx()["alerts"]["active"][0],
                                    title=hostile)]
    html_out = render_dashboard(ctx)
    assert "<script>alert" not in html_out
    assert "&lt;script&gt;" in html_out          # escaped form present
    assert html_out.count("&lt;script&gt;") >= 4  # budgets+spend+ledger+alert


def test_total_rendering_with_empty_ctx():
    """A dashboard that 500s is worse than one that shows blanks."""
    html_out = render_dashboard({})
    for sid in DASHBOARD_SECTIONS:
        assert f'id="{sid}"' in html_out
    assert "disabled" in html_out or "—" in html_out


def test_budget_table_modes_and_bar_classes():
    html_out = render_dashboard(_full_ctx())
    assert "unlimited" in html_out                      # mode none row
    assert "subscriber-c" in html_out
    assert 'class="bar crit"' in html_out               # exhausted -> crit
    assert 'class="bar warn"' in html_out               # 90% -> warn
    assert "100.0%" in html_out


def test_ledger_disabled_message():
    ctx = _full_ctx()
    ctx["ledger"] = {"enabled": False}
    assert "Billing plane disabled" in render_dashboard(ctx)


def test_ledger_pagination_links():
    ctx = _full_ctx()
    ctx["ledger"].update({"page": 1, "pages": 3})
    page1 = render_dashboard(ctx)
    assert "older" in page1 and "newer" not in page1
    ctx["ledger"].update({"page": 2})
    page2 = render_dashboard(ctx)
    assert "older" in page2 and "newer" in page2
    ctx["ledger"].update({"page": 3})
    page3 = render_dashboard(ctx)
    assert "older" not in page3 and "newer" in page3
    assert "Page 3 / 3" in page3
    assert "60" in page3  # total records shown


def test_vk_filter_link_roundtrip():
    ctx = _full_ctx()
    ctx["ledger"]["vk_filter"] = "vk-abc"
    html_out = render_dashboard(ctx)
    assert "clear" in html_out
    assert "vk-abc" in html_out


def test_alerts_panel_states():
    ctx = _full_ctx()
    assert 'class="alert critical"' in render_dashboard(ctx)
    ctx["alerts"] = {"enabled": True, "active": []}
    assert "No active alerts" in render_dashboard(ctx)
    ctx["alerts"] = {"enabled": False}
    assert "Alerting disabled" in render_dashboard(ctx)


def test_refresh_meta_tag():
    ctx = _full_ctx()
    assert 'http-equiv="refresh"' not in render_dashboard(ctx)
    ctx["query"] = {"refresh": 30}
    html_out = render_dashboard(ctx)
    assert 'http-equiv="refresh" content="30"' in html_out


def test_costguard_visibility_badge():
    ctx = _full_ctx()
    assert "billing visibility: ok" in render_dashboard(ctx)
    ctx["cost_guard"]["watchdog"]["billing_visibility"] = "leaking"
    html_out = render_dashboard(ctx)
    assert "billing visibility: leaking" in html_out


def test_page_count_math():
    assert page_count(0) == 1
    assert page_count(1) == 1
    assert page_count(PAGE_SIZE) == 1
    assert page_count(PAGE_SIZE + 1) == 2
    assert page_count(3 * PAGE_SIZE) == 3


def test_money_and_uptime_formatting():
    html_out = render_dashboard(_full_ctx())
    assert "$0.000072" in html_out       # 72 micros -> USD display
    assert "1h 2m" in html_out           # 3725s uptime


# ── Part B: live surfaces ───────────────────────────────────────────────────

@pytest.fixture(scope="module")
def stub():
    server = uvicorn.Server(uvicorn.Config(
        build_stub_app(), host="127.0.0.1", port=STUB_PORT,
        log_level="error", access_log=False))
    threading.Thread(target=server.run, daemon=True).start()
    _wait_http(f"http://127.0.0.1:{STUB_PORT}/health")
    yield
    server.should_exit = True
    time.sleep(0.3)


@pytest.fixture(scope="module")
def webhook_receiver():
    """Records every POST; /hook -> 200, /fail -> 500 (fail-soft target)."""
    events: list[dict] = []

    app = FastAPI()

    @app.post("/hook")
    async def hook(request: Request):
        events.append({"path": "/hook", "body": await request.json()})
        return {"ok": True}

    @app.post("/fail")
    async def fail_hook(request: Request):
        events.append({"path": "/fail", "body": await request.json()})
        return JSONResponse(status_code=500, content={"refused": True})

    server = uvicorn.Server(uvicorn.Config(
        app, host="127.0.0.1", port=WEBHOOK_PORT,
        log_level="error", access_log=False))
    threading.Thread(target=server.run, daemon=True).start()
    _wait_http(f"http://127.0.0.1:{WEBHOOK_PORT}/docs")
    yield events
    server.should_exit = True
    time.sleep(0.3)


def _wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            httpx.get(url, timeout=2)
            return
        except Exception:
            time.sleep(0.2)


def _base_config(webhook: str) -> dict:
    return {
        "server": {"host": "127.0.0.1", "port": ROUTER_PORT},
        "auth": {"mode": "required", "api_key_env": "SEC_TEST_KEY"},
        "ledger": {"enabled": True, "db_path": "ledger.sqlite3"},
        "pricing": {
            "default": {"input_per_mtok": 0.5, "output_per_mtok": 1.5},
            "entries": {
                "stub-simple": {"input_per_mtok": 0.0,
                                "output_per_mtok": 0.0, "free": True},
                "stub-complex": {"input_per_mtok": 1.25,
                                 "output_per_mtok": 5.0}}},
        "cost_guard": {
            "enabled": True, "estimate_chars_per_token": 4,
            "watchdog": {"statuses": [401, 402]}},
        "alerts": {
            "enabled": True,
            "rules": {"budget_threshold": True, "billing_leak": True,
                      "pool_exhaustion": False, "circuit_open": False,
                      "ledger_append_failures": True,
                      "ledger_verify": False},
            "webhook_url": webhook,
            "webhook_timeout_seconds": 2.0},
        "classifier": {"model": "stub-classifier",
                       "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
                       "session_timeout_minutes": 5,
                       "zero_shot": {"enabled": False},
                       "fasttext": {"enabled": False},
                       "setfit": {"enabled": False}},
        "routing": {"override_prefix": "/use:",
                    "escalation_keywords": [],
                    "de_escalation_keywords": []},
        "categories": {
            "simple": {"label": "Simple", "model": "stub-simple",
                       "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
                       "api_key_env": "", "timeout_seconds": 15},
            "complex": {"label": "Complex", "model": "stub-complex",
                        "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
                        "api_key_env": "", "timeout_seconds": 15}},
    }


def _start_router(config: dict):
    tmp = Path(tempfile.mkdtemp(prefix="dash-e2e-"))
    for fname in ["server.py", "providers.py", "trace.py", "keystore.py",
                  "keypool.py", "catalog.py", "ledger.py", "costguard.py",
                  "alerts.py", "dashboard.py", "surrogate_models.py",
                  "zero_shot_classifier.py", "fasttext_classifier.py",
                  "setfit_classifier.py"]:
        shutil.copy2(REPO / fname, tmp / fname)
    (tmp / "router_config.yaml").write_text(yaml.safe_dump(config))
    env = {k: v for k, v in os.environ.items()
           if k.startswith(("LC_", "LANG_", "PATH"))}
    env.update({"PYTHONUNBUFFERED": "1", "HOME": str(tmp),
                "TRACE_LOG_DIR": str(tmp / "traces"),
                "SEC_TEST_KEY": TEST_KEY})
    log = open(tmp / "router.log", "w")
    proc = subprocess.Popen([sys.executable, str(tmp / "server.py")],
                            cwd=tmp, env=env, stdout=log, stderr=log)
    return proc, tmp, log


def _stop_router(proc, tmp, log):
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
    log.close()
    shutil.rmtree(tmp, ignore_errors=True)


def _chat(prompt: str, headers: dict, stream: bool = False):
    return httpx.post(
        f"{ROUTER_URL}/v1/chat/completions", timeout=15,
        headers=headers,
        json={"model": "auto", "stream": stream,
              "messages": [{"role": "user", "content": prompt}]})


def _create_vk(name: str, **budget) -> dict:
    body = {"name": name}
    body.update(budget)
    r = httpx.post(f"{ROUTER_URL}/admin/virtual_keys", json=body,
                   headers=AUTH, timeout=10)
    assert r.status_code == 201, r.text
    return r.json()


def _wait_webhook(events: list[dict], predicate, timeout: float = 6.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for event in events:
            if predicate(event):
                return event
        time.sleep(0.2)
    return None


def test_dashboard_live_surfaces(stub, webhook_receiver):
    """End-to-end: auth matrix, render contract, alert lifecycle,
    webhook delivery + fail-soft, metrics, hot-reload swap/clear."""
    hook_url = f"http://127.0.0.1:{WEBHOOK_PORT}/hook"
    fail_url = f"http://127.0.0.1:{WEBHOOK_PORT}/fail"
    config = _base_config(hook_url)
    proc, tmp, log = _start_router(config)
    try:
        deadline = time.time() + 25
        ready = False
        while time.time() < deadline and not ready:
            try:
                ready = httpx.get(f"{ROUTER_URL}/health",
                                  timeout=2).status_code == 200
            except Exception:
                time.sleep(0.2)
        assert ready, "router did not boot"

        # ── auth matrix on /dashboard (an admin surface) ──
        assert httpx.get(DASH, timeout=10).status_code == 401
        assert httpx.get(
            DASH, headers={"Authorization": "Bearer wrong"},
            timeout=10).status_code == 401
        vk_row = _create_vk("dash-nobudget")
        r = httpx.get(DASH, headers={"Authorization": f"Bearer {vk_row['key']}"},
                      timeout=10)
        assert r.status_code == 403  # virtual keys: inference only

        # ── admin render contract ──
        r = httpx.get(DASH, headers=AUTH, timeout=10)
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        for sid in DASHBOARD_SECTIONS:
            assert f'id="{sid}"' in r.text, sid
        assert "<script" not in r.text

        # ── JSON variant mirrors the ctx contract ──
        r = httpx.get(f"{DASH}?format=json", headers=AUTH, timeout=10)
        assert r.status_code == 200
        ctx = r.json()
        for key in ("title", "generated_at", "uptime_seconds", "version",
                    "features", "cards", "alerts", "spend", "budgets",
                    "ledger", "pools", "circuits", "cost_guard", "catalog"):
            assert key in ctx, key
        assert ctx["features"]["alerts"] is True
        assert ctx["ledger"]["enabled"] is True

        # ── light status surface ──
        r = httpx.get(STATUS, headers=AUTH, timeout=10)
        assert r.status_code == 200
        status = r.json()
        for key in ("cards", "features", "alerts", "pools", "circuits",
                    "uptime_seconds"):
            assert key in status, key
        assert "spend" not in status  # deliberately light

        # ── alerts: initially quiet, engine enabled ──
        r = httpx.get(ALERTS, headers=AUTH, timeout=10)
        assert r.status_code == 200
        assert r.json()["enabled"] is True
        assert r.json()["active"] == []

        # ── budget exhaustion -> critical alert + raised webhook ──
        vk_c = _create_vk("dash-budget", budget_mode="cost_micros",
                          budget_limit=36, budget_period="total")
        assert _chat("/use:complex dash budget one",
                     {"Authorization": f"Bearer {vk_c['key']}"}).status_code == 200
        blocked = _chat("/use:complex dash budget two",
                        {"Authorization": f"Bearer {vk_c['key']}"})
        assert blocked.status_code == 402  # Stage 4 gate, pre-routing
        r = httpx.get(ALERTS, headers=AUTH, timeout=10)
        active_ids = [a["id"] for a in r.json()["active"]]
        assert f"budget:{vk_c['id']}" in active_ids
        budget_alert = next(a for a in r.json()["active"]
                            if a["id"] == f"budget:{vk_c['id']}")
        assert budget_alert["severity"] == "critical"
        event = _wait_webhook(
            webhook_receiver,
            lambda e: e["path"] == "/hook" and e["body"]["event"] == "raised"
            and e["body"]["alert"]["id"] == f"budget:{vk_c['id']}")
        assert event is not None, "raised webhook not delivered"

        # ── no double-fire while the alert stays active ──
        seen = len(webhook_receiver)
        httpx.get(ALERTS, headers=AUTH, timeout=10)
        time.sleep(1.0)
        assert len(webhook_receiver) == seen

        # ── billing-leak watchdog -> leak alert (admin traffic, 402 stub) ──
        _chat("/use:complex billing-leak probe", AUTH)
        r = httpx.get(ALERTS, headers=AUTH, timeout=10)
        active_ids = [a["id"] for a in r.json()["active"]]
        assert "billing_leak" in active_ids
        assert _wait_webhook(
            webhook_receiver,
            lambda e: e["path"] == "/hook" and e["body"]["event"] == "raised"
            and e["body"]["alert"]["id"] == "billing_leak") is not None

        # ── metrics surface carries the alert family ──
        r = httpx.get(f"{ROUTER_URL}/metrics", headers=AUTH, timeout=10)
        assert "hermes_router_alerts_active" in r.json()["prometheus"]
        assert r.json()["metrics"]["alerts_active"] >= 2

        # ── webhook pipe check ──
        r = httpx.post(f"{ROUTER_URL}/admin/alerts/test", headers=AUTH,
                       timeout=10)
        assert r.status_code == 200
        assert r.json()["delivered"] is True
        assert _wait_webhook(
            webhook_receiver,
            lambda e: e["body"]["event"] == "test") is not None

        # ── hot-reload to a failing webhook: fail-soft + fresh state ──
        config["alerts"]["webhook_url"] = fail_url
        (tmp / "router_config.yaml").write_text(yaml.safe_dump(config))
        r = httpx.post(f"{ROUTER_URL}/reload", headers=AUTH, timeout=10)
        assert r.status_code == 200
        vk_d = _create_vk("dash-budget-2", budget_mode="cost_micros",
                          budget_limit=36, budget_period="total")
        _chat("/use:complex dash budget three",
              {"Authorization": f"Bearer {vk_d['key']}"})
        _chat("/use:complex dash budget four",
              {"Authorization": f"Bearer {vk_d['key']}"})
        r = httpx.get(ALERTS, headers=AUTH, timeout=10)
        active_ids = [a["id"] for a in r.json()["active"]]
        assert f"budget:{vk_d['id']}" in active_ids
        assert _wait_webhook(
            webhook_receiver,
            lambda e: e["path"] == "/fail"
            and e["body"]["alert"]["id"] == f"budget:{vk_d['id']}"
        ) is not None, "fail-soft target should still RECEIVE the attempt"
        r = httpx.post(f"{ROUTER_URL}/admin/alerts/test", headers=AUTH,
                       timeout=10)
        assert r.json()["delivered"] is False  # 500 receiver
        assert r.json()["status_code"] == 500
        r = httpx.get(f"{ROUTER_URL}/metrics", headers=AUTH, timeout=10)
        assert r.json()["metrics"]["alerts_webhook_failures_total"] >= 1

        # ── hot-reload with alerts removed: engine clears cleanly ──
        del config["alerts"]
        (tmp / "router_config.yaml").write_text(yaml.safe_dump(config))
        r = httpx.post(f"{ROUTER_URL}/reload", headers=AUTH, timeout=10)
        assert r.status_code == 200
        r = httpx.get(ALERTS, headers=AUTH, timeout=10)
        assert r.json() == {"enabled": False}
        r = httpx.get(f"{ROUTER_URL}/metrics", headers=AUTH, timeout=10)
        assert "hermes_router_alerts_active" not in r.json()["prometheus"]
        # dashboard keeps rendering with alerting off
        r = httpx.get(DASH, headers=AUTH, timeout=10)
        assert r.status_code == 200 and 'id="alerts"' in r.text
        assert "Alerting disabled" in r.text
    finally:
        _stop_router(proc, tmp, log)

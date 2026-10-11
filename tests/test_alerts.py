"""Stage 6 unit contracts for the alerting engine (alerts.py).

Pure-logic coverage: config validation matrix (fail-closed), every rule's
boundary math, state transitions (raise / severity-change / clear), the
bounded history ring, counters, and the webhook payload shape. The live
HTTP surfaces (endpoints, webhook delivery, hot-reload) are covered by
tests/test_dashboard.py's live-process section.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from alerts import (  # noqa: E402
    RULE_NAMES,
    AlertEngine,
    AlertsError,
    build_alerts,
    webhook_payload,
)


def _engine(**overrides) -> AlertEngine:
    """Engine with all rules ON except where overridden."""
    rules = {name: True for name in RULE_NAMES}
    rules.update(overrides.pop("rules", {}))
    params = dict(
        budget_warn=0.8, budget_critical=1.0, leak_window=900.0,
        rules=rules, webhook_url="", webhook_timeout=3.0,
        history_limit=100)
    params.update(overrides)
    return AlertEngine(**params)


# ── Config validation (fail-closed) ─────────────────────────────────────────

def test_absent_section_disables_engine():
    assert build_alerts({}) is None
    assert build_alerts({"alerts": None}) is None


def test_enabled_false_disables_engine():
    assert build_alerts({"alerts": {"enabled": False}}) is None


def test_non_mapping_section_rejected():
    with pytest.raises(AlertsError):
        build_alerts({"alerts": ["nope"]})


def test_unknown_top_level_key_rejected():
    with pytest.raises(AlertsError, match="unknown keys"):
        build_alerts({"alerts": {"nonsense": 1}})


def test_enabled_must_be_boolean():
    with pytest.raises(AlertsError, match="enabled"):
        build_alerts({"alerts": {"enabled": "true"}})


@pytest.mark.parametrize("value", [0, -0.1, 1.5, "high", None, True])
def test_budget_warn_threshold_bounds(value):
    with pytest.raises(AlertsError):
        build_alerts({"alerts": {"budget_warn_threshold": value}})


@pytest.mark.parametrize("value", [0.5, 0.8, 0.95, 1.0])
def test_budget_warn_threshold_valid(value):
    engine = build_alerts({"alerts": {"budget_warn_threshold": value}})
    assert engine is not None and engine.budget_warn == value


@pytest.mark.parametrize("value", [0, -0.1, 1.001, "high", None, True])
def test_budget_critical_threshold_bounds(value):
    with pytest.raises(AlertsError):
        build_alerts({"alerts": {"budget_critical_threshold": value}})


def test_critical_below_warn_rejected():
    with pytest.raises(AlertsError, match=">= budget_warn"):
        build_alerts({"alerts": {"budget_warn_threshold": 0.9,
                                 "budget_critical_threshold": 0.8}})


@pytest.mark.parametrize("key", ["billing_leak_window_seconds",
                                 "webhook_timeout_seconds"])
def test_positive_numeric_fields(key):
    with pytest.raises(AlertsError):
        build_alerts({"alerts": {key: 0}})
    with pytest.raises(AlertsError):
        build_alerts({"alerts": {key: "soon"}})


def test_unknown_rule_name_rejected():
    with pytest.raises(AlertsError, match="unknown rule"):
        build_alerts({"alerts": {"rules": {"made_up_rule": True}}})


def test_rule_value_must_be_boolean():
    with pytest.raises(AlertsError, match="must be a boolean"):
        build_alerts({"alerts": {"rules": {"pool_exhaustion": "yes"}}})


def test_rules_must_be_mapping():
    with pytest.raises(AlertsError, match="mapping"):
        build_alerts({"alerts": {"rules": ["pool_exhaustion"]}})


@pytest.mark.parametrize("url", ["ftp://hook.example/x", "file:///etc/passwd",
                                 "gopher://x", "javascript:alert(1)"])
def test_webhook_url_scheme_rejected(url):
    with pytest.raises(AlertsError, match="http"):
        build_alerts({"alerts": {"webhook_url": url}})


@pytest.mark.parametrize("url", ["", "http://127.0.0.1:9944/hook",
                                 "https://hooks.example.com/x"])
def test_webhook_url_scheme_valid(url):
    engine = build_alerts({"alerts": {"webhook_url": url}})
    assert engine is not None and engine.webhook_url == url


@pytest.mark.parametrize("value", [True, 0, 10001, "many", 1.5])
def test_history_limit_bounds(value):
    with pytest.raises(AlertsError):
        build_alerts({"alerts": {"history_limit": value}})


def test_defaults_materialized():
    """Empty section -> engine with defaults; ledger_verify OFF by default."""
    engine = build_alerts({"alerts": {}})
    assert engine is not None
    assert engine.budget_warn == 0.8
    assert engine.budget_critical == 1.0
    assert engine.leak_window == 900.0
    assert engine.webhook_timeout == 3.0
    for name in RULE_NAMES:
        assert engine.rules[name] is (name != "ledger_verify"), name


def test_explicit_rule_overrides_persist():
    engine = build_alerts({"alerts": {"rules": {"ledger_verify": True,
                                                "circuit_open": False}}})
    assert engine is not None
    assert engine.rules["ledger_verify"] is True
    assert engine.rules["circuit_open"] is False
    assert engine.rules["budget_threshold"] is True


# ── Rule: budget_threshold ──────────────────────────────────────────────────

def _vk(id="vk-1", name="sub", mode="tokens", limit=100, used=0,
        blocked=False, period="day"):
    return {"id": id, "name": name, "mode": mode, "limit": limit,
            "period": period, "used": used, "blocked": blocked}


def test_budget_below_threshold_quiet():
    result = _engine().evaluate({"budgets": [_vk(used=50)]})
    assert result["active"] == []


def test_budget_at_warn_threshold_raises_warning():
    result = _engine().evaluate({"budgets": [_vk(used=80)]})
    assert len(result["active"]) == 1
    alert = result["active"][0]
    assert alert["severity"] == "warning"
    assert alert["id"] == "budget:vk-1"
    assert alert["detail"]["used_ratio"] == 0.8


def test_budget_at_critical_threshold_raises_critical():
    result = _engine().evaluate({"budgets": [_vk(used=100)]})
    assert result["active"][0]["severity"] == "critical"


def test_budget_blocked_is_critical_even_below_ratio():
    result = _engine().evaluate({"budgets": [_vk(used=10, blocked=True)]})
    assert result["active"][0]["severity"] == "critical"


def test_budget_zero_limit_with_mode_is_critical():
    result = _engine().evaluate({"budgets": [_vk(mode="cost_micros",
                                                 limit=0, used=0)]})
    assert len(result["active"]) == 1
    assert result["active"][0]["severity"] == "critical"
    assert result["active"][0]["detail"]["used_ratio"] is None


def test_budget_mode_none_skipped():
    result = _engine().evaluate({"budgets": [_vk(mode="none", used=999)]})
    assert result["active"] == []


def test_budget_multiple_keys_independent_alerts():
    budgets = [_vk(id="vk-a", used=90), _vk(id="vk-b", used=10),
               _vk(id="vk-c", used=100)]
    result = _engine().evaluate({"budgets": budgets})
    assert [a["id"] for a in result["active"]] == \
        ["budget:vk-a", "budget:vk-c"]


# ── Rule: pool_exhaustion ───────────────────────────────────────────────────

def test_pool_exhausted_raises_critical():
    surfaces = {"pools": [{"name": "free_tier", "size": 3, "available": 0}]}
    result = _engine().evaluate(surfaces)
    assert result["active"][0]["id"] == "pool:free_tier"
    assert result["active"][0]["severity"] == "critical"


def test_pool_partially_available_quiet():
    surfaces = {"pools": [{"name": "free_tier", "size": 3, "available": 1}]}
    assert _engine().evaluate(surfaces)["active"] == []


def test_empty_pool_not_an_alert():
    surfaces = {"pools": [{"name": "empty", "size": 0, "available": 0}]}
    assert _engine().evaluate(surfaces)["active"] == []


# ── Rule: circuit_open ──────────────────────────────────────────────────────

def test_open_circuit_raises_warning():
    surfaces = {"circuits": [
        {"key": "a", "state": "open", "failures": 3},
        {"key": "b", "state": "closed", "failures": 0}]}
    result = _engine().evaluate(surfaces)
    assert len(result["active"]) == 1
    assert result["active"][0]["id"] == "circuit:a"
    assert result["active"][0]["severity"] == "warning"
    assert result["active"][0]["detail"]["failures"] == 3


# ── Rule: billing_leak ──────────────────────────────────────────────────────

def _leak_event(seconds_ago: float, status=402, model="stub-complex"):
    return {"ts": time.strftime("%Y-%m-%dT%H:%M:%S",
                                time.gmtime(time.time() - seconds_ago)) + "Z",
            "status": status, "model": model, "key_id": None}


def test_recent_leak_event_raises_critical():
    surfaces = {"billing_leak_events": [_leak_event(60)]}
    result = _engine().evaluate(surfaces)
    assert result["active"][0]["id"] == "billing_leak"
    detail = result["active"][0]["detail"]
    assert detail["events_in_window"] == 1
    assert detail["by_status"] == {"402": 1}
    assert detail["models"] == ["stub-complex"]


def test_stale_leak_event_outside_window_ignored():
    surfaces = {"billing_leak_events": [_leak_event(901)]}
    assert _engine().evaluate(surfaces)["active"] == []


def test_leak_window_boundary_inclusive():
    # Watchdog stamps are second-resolution (strftime truncates), so an
    # event at 899s parses to now-ts in [899, 900) — inside the 900s
    # window; 901s lands outside it.
    surfaces = {"billing_leak_events": [_leak_event(899)]}
    assert len(_engine().evaluate(surfaces)["active"]) == 1
    surfaces = {"billing_leak_events": [_leak_event(901)]}
    assert _engine().evaluate(surfaces)["active"] == []


def test_malformed_leak_timestamps_ignored():
    surfaces = {"billing_leak_events": [
        {"ts": "not-a-date", "status": 402},
        {"ts": 12345, "status": 402},
        {"ts": "2026-01-01T00:00:00+00:00", "status": 402},  # no Z suffix
    ]}
    assert _engine().evaluate(surfaces)["active"] == []


def test_leak_events_grouped_into_one_alert():
    surfaces = {"billing_leak_events": [_leak_event(10, 402),
                                        _leak_event(20, 401),
                                        _leak_event(30, 402)]}
    result = _engine().evaluate(surfaces)
    assert len(result["active"]) == 1
    assert result["active"][0]["detail"]["by_status"] == {"401": 1, "402": 2}


# ── Rule: ledger_append_failures ────────────────────────────────────────────

def test_append_failures_raise_critical():
    surfaces = {"ledger_append_failures": 2}
    result = _engine().evaluate(surfaces)
    assert result["active"][0]["id"] == "ledger_append_failures"
    assert result["active"][0]["detail"]["failures"] == 2


def test_zero_append_failures_quiet():
    assert _engine().evaluate({"ledger_append_failures": 0})["active"] == []


# ── Rule: ledger_verify (opt-in) ────────────────────────────────────────────

def test_verify_ok_quiet():
    surfaces = {"ledger_verify": {"ok": True, "records": 10}}
    assert _engine().evaluate(surfaces)["active"] == []


def test_verify_failure_raises_critical():
    surfaces = {"ledger_verify": {"ok": False, "records": 9,
                                  "first_bad_seq": 4,
                                  "reason": "prev_hash linkage break"}}
    result = _engine().evaluate(surfaces)
    alert = result["active"][0]
    assert alert["id"] == "ledger_verify"
    assert alert["detail"]["first_bad_seq"] == 4
    assert "tamper" in alert["detail"]["note"]


def test_verify_absent_quiet():
    assert _engine().evaluate({"ledger_verify": None})["active"] == []


# ── Transitions, history, stats ─────────────────────────────────────────────

def test_raise_persist_clear_lifecycle():
    engine = _engine()
    surfaces = {"pools": [{"name": "p", "size": 2, "available": 0}]}
    first = engine.evaluate(surfaces)
    assert len(first["raised_now"]) == 1
    assert first["raised_now"][0]["first_seen"] == first["raised_now"][0]["last_seen"]
    assert first["stats"]["alerts_raised_total"] == 1

    second = engine.evaluate(surfaces)
    assert second["raised_now"] == []
    assert len(second["active"]) == 1
    assert second["active"][0]["first_seen"] == first["active"][0]["first_seen"]
    assert second["stats"]["alerts_raised_total"] == 1  # no double count

    recovered = {"pools": [{"name": "p", "size": 2, "available": 2}]}
    third = engine.evaluate(recovered)
    assert third["active"] == []
    assert len(third["cleared_now"]) == 1
    assert third["cleared_now"][0]["state"] == "cleared"
    assert third["stats"]["alerts_cleared_total"] == 1


def test_severity_change_recorded_in_history():
    engine = _engine()
    surfaces = {"budgets": [_vk(used=80)]}
    engine.evaluate(surfaces)
    worse = engine.evaluate({"budgets": [_vk(used=100)]})
    assert worse["raised_now"] == []          # same alert id, not a new raise
    assert worse["active"][0]["severity"] == "critical"
    events = [h["event"] for h in worse["history"]]
    assert "severity_change" in events


def test_history_bounded_by_limit():
    engine = _engine(history_limit=3)
    for i in range(5):
        engine.evaluate({"pools": [{"name": f"p{i}", "size": 1,
                                    "available": 0}]})
    assert len(engine._history if hasattr(engine, "_history")
               else engine.evaluate({})["history"]) <= 3
    # and via the public surface: last evaluation reports the bounded ring
    result = engine.evaluate({})
    assert len(result["history"]) <= 3


def test_disabled_rule_never_fires():
    engine = _engine(rules={"pool_exhaustion": False})
    surfaces = {"pools": [{"name": "p", "size": 2, "available": 0}]}
    assert engine.evaluate(surfaces)["active"] == []


def test_reenabling_rule_after_rebuild_is_fresh_state():
    """A rebuilt engine (hot reload) starts with empty state."""
    first = _engine()
    first.evaluate({"pools": [{"name": "p", "size": 1, "available": 0}]})
    assert first.stats()["alerts_active"] == 1
    second = build_alerts({"alerts": {}})
    assert second is not None and second.stats()["alerts_active"] == 0


def test_stats_and_webhook_counters():
    engine = _engine()
    assert engine.stats()["alerts_webhook_sent_total"] == 0
    engine.note_webhook_result(True)
    engine.note_webhook_result(True)
    engine.note_webhook_result(False)
    stats = engine.stats()
    assert stats["alerts_webhook_sent_total"] == 2
    assert stats["alerts_webhook_failures_total"] == 1


def test_config_echo_is_secrets_free():
    engine = build_alerts({"alerts": {"webhook_url":
                                      "https://hooks.example/x?token=s3cret"}})
    assert engine is not None
    echo = engine.describe()
    assert echo["webhook_configured"] is True
    assert "s3cret" not in str(echo)  # presence flag only, never the URL


# ── Webhook payload ─────────────────────────────────────────────────────────

def test_webhook_payload_shape_without_alert():
    payload = webhook_payload("test")
    assert payload["event"] == "test"
    assert payload["gateway"] == "hermes-router"
    assert payload["ts"].endswith("Z")
    assert "alert" not in payload


def test_webhook_payload_shape_with_alert():
    alert = {"id": "pool:p", "rule": "pool_exhaustion",
             "severity": "critical", "title": "Key pool exhausted",
             "detail": {"pool": "p"}, "first_seen": "2026-01-01T00:00:00Z",
             "last_seen": "2026-01-01T00:00:00Z"}
    payload = webhook_payload("raised", alert)
    assert payload["alert"]["id"] == "pool:p"
    assert payload["alert"]["severity"] == "critical"

"""Alerting engine for the Hermes Router-Proxy (Stage 6 operations plane).

Purpose
-------
Rule-based alert evaluation over the gateway's existing surfaces.
Evaluation is **pull-model and pure**: the server assembles a plain
"surfaces" snapshot (virtual-key budgets, pool availability, circuit
states, watchdog billing-leak events, ledger counters) and hands it to
``AlertEngine.evaluate()`` — no background thread, no timers, no I/O.
Every evaluation is deterministic for a given snapshot, which is what
makes the engine unit-testable and the golden fixtures stable.

Rules (each independently toggleable, fail-closed config):

  budget_threshold       warning when a virtual-key budget is >= the
                         warn threshold used (default 80%), critical at
                         >= the critical threshold (default 100% — the
                         key is at/over its blocking budget).
  pool_exhaustion        critical when a configured pool has keys but
                         none currently available (all cooling down or
                         quarantined).
  circuit_open           warning per open circuit breaker.
  billing_leak           critical when the cost-guard watchdog observed
                         a billing signal (401/402 by default) within
                         the leak window (default 15 min).
  ledger_append_failures critical when ledger appends have failed since
                         boot (billing visibility loss).
  ledger_verify          critical when the ledger hash chain fails
                         verification (tamper evidence). OFF by default:
                         verification is a full-chain O(n) scan.

State transitions are tracked so webhooks fire exactly once per
raise/clear (delivery itself lives in server.py — this module stays
I/O-free). History is a bounded ring of transition events.

Like every stage module: absent section = engine off; unknown keys and
invalid values refuse to start (fail-closed at boot, reload, dispatch).
"""

from __future__ import annotations

import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

__all__ = [
    "AlertsError",
    "RULE_NAMES",
    "AlertEngine",
    "build_alerts",
    "webhook_payload",
]

RULE_NAMES = (
    "budget_threshold",
    "pool_exhaustion",
    "circuit_open",
    "billing_leak",
    "ledger_append_failures",
    "ledger_verify",
)

SEVERITY_ORDER = {"info": 0, "warning": 1, "critical": 2}
_HISTORY_LIMIT_DEFAULT = 100
_WEBHOOK_TIMEOUT_DEFAULT = 3.0
_BILLING_LEAK_WINDOW_DEFAULT = 900.0
_BUDGET_WARN_DEFAULT = 0.8
_BUDGET_CRITICAL_DEFAULT = 1.0

# Known top-level config keys (fail-closed: anything else refuses boot).
_KNOWN_FIELDS = {
    "enabled", "budget_warn_threshold", "budget_critical_threshold",
    "billing_leak_window_seconds", "rules", "webhook_url",
    "webhook_timeout_seconds", "history_limit",
}


class AlertsError(RuntimeError):
    """Invalid alerts configuration (boot/refuse, reload/reject)."""


def _now_iso(now: float) -> str:
    return datetime.fromtimestamp(now, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S") + "Z"


def _parse_iso(value: Any) -> float | None:
    """Parse the watchdog's second-resolution UTC stamps ('...Z')."""
    if not isinstance(value, str) or not value.endswith("Z"):
        return None
    try:
        dt = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return None
    return dt.timestamp()


def _num(value: Any, label: str, *, minimum: float = 0.0,
         exclusive: bool = False, maximum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AlertsError(f"alerts.{label} must be a number, got {value!r}")
    if exclusive and value <= minimum:
        raise AlertsError(f"alerts.{label} must be > {minimum}, got {value!r}")
    if not exclusive and value < minimum:
        raise AlertsError(f"alerts.{label} must be >= {minimum}, got {value!r}")
    if maximum is not None and value > maximum:
        raise AlertsError(f"alerts.{label} must be <= {maximum}, got {value!r}")
    return float(value)


class AlertEngine:
    """Stateful evaluator over precomputed surface snapshots."""

    def __init__(self, *, budget_warn: float, budget_critical: float,
                 leak_window: float, rules: dict[str, bool],
                 webhook_url: str, webhook_timeout: float,
                 history_limit: int) -> None:
        self.budget_warn = budget_warn
        self.budget_critical = budget_critical
        self.leak_window = leak_window
        self.rules = rules
        self.webhook_url = webhook_url
        self.webhook_timeout = webhook_timeout
        self.history_limit = history_limit
        # id -> active alert record (dict, secrets-free by construction)
        self._active: dict[str, dict[str, Any]] = {}
        self._history: deque[dict[str, Any]] = deque(maxlen=history_limit)
        self._raised_total = 0
        self._cleared_total = 0
        self._webhook_sent_total = 0
        self._webhook_failures_total = 0

    # ── evaluation ──────────────────────────────────────────────────────
    def evaluate(self, surfaces: dict[str, Any],
                 now: float | None = None) -> dict[str, Any]:
        """Evaluate every enabled rule against ``surfaces``.

        Returns the full snapshot: active alerts, this round's
        transitions (for webhook delivery), bounded history, config echo
        and counters. Safe to call concurrently; state mutations are
        atomic under the GIL and single-threaded in the event loop.
        """
        now = time.time() if now is None else now
        found: dict[str, dict[str, Any]] = {}
        for rule in RULE_NAMES:
            if not self.rules.get(rule, True):
                continue
            handler = getattr(self, f"_rule_{rule}")
            for alert in handler(surfaces, now):
                found[alert["id"]] = alert

        raised_now: list[dict[str, Any]] = []
        cleared_now: list[dict[str, Any]] = []
        for alert_id, alert in found.items():
            prev = self._active.get(alert_id)
            if prev is None:
                alert["first_seen"] = _now_iso(now)
                alert["last_seen"] = _now_iso(now)
                alert["state"] = "active"
                raised_now.append(alert)
                self._raised_total += 1
                self._history.append(
                    {"ts": _now_iso(now), "event": "raised", "id": alert_id,
                     "rule": alert["rule"], "severity": alert["severity"]})
            else:
                alert["first_seen"] = prev["first_seen"]
                alert["last_seen"] = _now_iso(now)
                alert["state"] = "active"
                if alert["severity"] != prev["severity"]:
                    self._history.append({
                        "ts": _now_iso(now), "event": "severity_change",
                        "id": alert_id, "rule": alert["rule"],
                        "severity": alert["severity"],
                        "previous": prev["severity"]})
        for alert_id, prev in list(self._active.items()):
            if alert_id not in found:
                prev["state"] = "cleared"
                prev["last_seen"] = _now_iso(now)
                cleared_now.append(prev)
                self._cleared_total += 1
                self._history.append(
                    {"ts": _now_iso(now), "event": "cleared", "id": alert_id,
                     "rule": prev["rule"], "severity": prev["severity"]})
        self._active = {
            aid: found[aid] for aid in found} if found else {}
        # keep cleared records out of _active but visible via history
        return {
            "enabled": True,
            "active": [self._active[aid] for aid in sorted(self._active)],
            "raised_now": raised_now,
            "cleared_now": cleared_now,
            "history": list(self._history),
            "config": self.describe(),
            "stats": self.stats(),
        }

    # ── rules (pure; each returns candidate alerts) ─────────────────────
    def _rule_budget_threshold(self, surfaces: dict[str, Any],
                               now: float) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for vk in surfaces.get("budgets", []):
            mode = vk.get("mode", "none")
            if mode == "none":
                continue
            limit = vk.get("limit", 0)
            used = vk.get("used", 0)
            blocked = bool(vk.get("blocked"))
            if limit > 0:
                ratio = used / limit
            else:
                # mode set with limit 0 = zero allowance = always critical
                ratio = float("inf")
            if blocked or ratio >= self.budget_critical:
                severity = "critical"
            elif ratio >= self.budget_warn:
                severity = "warning"
            else:
                continue
            out.append({
                "id": f"budget:{vk.get('id', '?')}",
                "rule": "budget_threshold",
                "severity": severity,
                "title": "Virtual-key budget exhausted"
                         if severity == "critical"
                         else "Virtual-key budget near limit",
                "detail": {
                    "virtual_key_id": vk.get("id"),
                    "name": vk.get("name"),
                    "mode": mode,
                    "period": vk.get("period"),
                    "used": used,
                    "limit": limit,
                    "used_ratio": (None if ratio == float("inf")
                                   else round(ratio, 4)),
                },
            })
        return out

    def _rule_pool_exhaustion(self, surfaces: dict[str, Any],
                              now: float) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for pool in surfaces.get("pools", []):
            if pool.get("size", 0) > 0 and pool.get("available", 0) == 0:
                out.append({
                    "id": f"pool:{pool.get('name', '?')}",
                    "rule": "pool_exhaustion",
                    "severity": "critical",
                    "title": "Key pool exhausted",
                    "detail": {
                        "pool": pool.get("name"),
                        "size": pool.get("size"),
                        "note": "every key is cooling down or quarantined",
                    },
                })
        return out

    def _rule_circuit_open(self, surfaces: dict[str, Any],
                           now: float) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for circuit in surfaces.get("circuits", []):
            if circuit.get("state") == "open":
                out.append({
                    "id": f"circuit:{circuit.get('key', '?')}",
                    "rule": "circuit_open",
                    "severity": "warning",
                    "title": "Circuit breaker open",
                    "detail": {
                        "circuit": circuit.get("key"),
                        "failures": circuit.get("failures"),
                    },
                })
        return out

    def _rule_billing_leak(self, surfaces: dict[str, Any],
                           now: float) -> list[dict[str, Any]]:
        events = surfaces.get("billing_leak_events", [])
        recent = []
        for event in events:
            ts = _parse_iso(event.get("ts"))
            if ts is not None and now - ts <= self.leak_window:
                recent.append(event)
        if not recent:
            return []
        by_status: dict[str, int] = {}
        for event in recent:
            status = str(event.get("status"))
            by_status[status] = by_status.get(status, 0) + 1
        models = sorted({str(e.get("model")) for e in recent
                         if e.get("model")})
        return [{
            "id": "billing_leak",
            "rule": "billing_leak",
            "severity": "critical",
            "title": "Billing-leak signals observed",
            "detail": {
                "events_in_window": len(recent),
                "window_seconds": self.leak_window,
                "by_status": by_status,
                "models": models,
                "note": "upstream returned a billing status (401/402); "
                        "check provider credentials and paid-tier exposure",
            },
        }]

    def _rule_ledger_append_failures(self, surfaces: dict[str, Any],
                                     now: float) -> list[dict[str, Any]]:
        failures = surfaces.get("ledger_append_failures", 0)
        if not failures:
            return []
        return [{
            "id": "ledger_append_failures",
            "rule": "ledger_append_failures",
            "severity": "critical",
            "title": "Ledger append failures since boot",
            "detail": {
                "failures": failures,
                "note": "billing visibility is degraded; completions may "
                        "be unrecorded — see runbook backup/verify drill",
            },
        }]

    def _rule_ledger_verify(self, surfaces: dict[str, Any],
                            now: float) -> list[dict[str, Any]]:
        verdict = surfaces.get("ledger_verify")
        if not verdict:
            return []
        if verdict.get("ok", True):
            return []
        return [{
            "id": "ledger_verify",
            "rule": "ledger_verify",
            "severity": "critical",
            "title": "Ledger hash-chain verification FAILED",
            "detail": {
                "records": verdict.get("records"),
                "first_bad_seq": verdict.get("first_bad_seq"),
                "reason": verdict.get("reason"),
                "note": "tamper evidence tripped — freeze writes and "
                        "follow the ledger recovery runbook",
            },
        }]

    # ── surfaces for the outside world ─────────────────────────────────
    def describe(self) -> dict[str, Any]:
        return {
            "budget_warn_threshold": self.budget_warn,
            "budget_critical_threshold": self.budget_critical,
            "billing_leak_window_seconds": self.leak_window,
            "rules": dict(self.rules),
            "webhook_configured": bool(self.webhook_url),
            "webhook_timeout_seconds": self.webhook_timeout,
            "history_limit": self.history_limit,
        }

    def stats(self) -> dict[str, int]:
        return {
            "alerts_active": len(self._active),
            "alerts_raised_total": self._raised_total,
            "alerts_cleared_total": self._cleared_total,
            "alerts_webhook_sent_total": self._webhook_sent_total,
            "alerts_webhook_failures_total": self._webhook_failures_total,
        }

    def note_webhook_result(self, sent: bool) -> None:
        """Called by the server after a delivery attempt (fail-soft)."""
        if sent:
            self._webhook_sent_total += 1
        else:
            self._webhook_failures_total += 1


def webhook_payload(event: str, alert: dict[str, Any] | None = None,
                    now: float | None = None) -> dict[str, Any]:
    """Build the JSON body for a webhook delivery (raised/cleared/test)."""
    now = time.time() if now is None else now
    body: dict[str, Any] = {
        "event": event,
        "gateway": "hermes-router",
        "ts": _now_iso(now),
    }
    if alert is not None:
        body["alert"] = {
            "id": alert.get("id"),
            "rule": alert.get("rule"),
            "severity": alert.get("severity"),
            "title": alert.get("title"),
            "detail": alert.get("detail"),
            "first_seen": alert.get("first_seen"),
            "last_seen": alert.get("last_seen"),
        }
    return body


# ── Config validation + construction (fail-closed) ─────────────────────────

def build_alerts(cfg: dict[str, Any]) -> AlertEngine | None:
    """Build from ``cfg["alerts"]``. Missing/empty/disabled → None."""
    section = cfg.get("alerts")
    if section is None:
        return None
    if not isinstance(section, dict):
        raise AlertsError("alerts section must be a mapping")
    unknown = set(section) - _KNOWN_FIELDS
    if unknown:
        raise AlertsError(
            f"alerts: unknown keys {sorted(unknown)} "
            f"(known: {sorted(_KNOWN_FIELDS)})")
    enabled = section.get("enabled", True)
    if not isinstance(enabled, bool):
        raise AlertsError(f"alerts.enabled must be a boolean, got {enabled!r}")
    if not enabled:
        return None

    budget_warn = _num(section.get("budget_warn_threshold",
                                   _BUDGET_WARN_DEFAULT),
                       "budget_warn_threshold", minimum=0, exclusive=True,
                       maximum=1)
    budget_critical = _num(
        section.get("budget_critical_threshold", _BUDGET_CRITICAL_DEFAULT),
        "budget_critical_threshold", minimum=0, exclusive=True, maximum=1)
    if budget_critical < budget_warn:
        raise AlertsError(
            "alerts.budget_critical_threshold must be >= "
            "budget_warn_threshold")
    leak_window = _num(
        section.get("billing_leak_window_seconds",
                    _BILLING_LEAK_WINDOW_DEFAULT),
        "billing_leak_window_seconds", minimum=0, exclusive=True)
    webhook_timeout = _num(
        section.get("webhook_timeout_seconds", _WEBHOOK_TIMEOUT_DEFAULT),
        "webhook_timeout_seconds", minimum=0, exclusive=True)

    raw_rules = section.get("rules", {})
    if not isinstance(raw_rules, dict):
        raise AlertsError("alerts.rules must be a mapping of rule → boolean")
    # Materialize every rule with its default (ledger_verify defaults OFF —
    # verification is an O(n) full-chain scan per evaluation).
    rules: dict[str, bool] = {name: (name != "ledger_verify")
                              for name in RULE_NAMES}
    for name, value in raw_rules.items():
        if name not in RULE_NAMES:
            raise AlertsError(
                f"alerts.rules: unknown rule {name!r} "
                f"(known: {list(RULE_NAMES)})")
        if not isinstance(value, bool):
            raise AlertsError(
                f"alerts.rules.{name} must be a boolean, got {value!r}")
        rules[name] = value

    webhook_url = section.get("webhook_url", "")
    if not isinstance(webhook_url, str):
        raise AlertsError(
            f"alerts.webhook_url must be a string, got {webhook_url!r}")
    webhook_url = webhook_url.strip()
    if webhook_url and not webhook_url.startswith(("http://", "https://")):
        raise AlertsError(
            "alerts.webhook_url must be an http(s) URL "
            f"(got {webhook_url[:32]!r})")

    history_limit = section.get("history_limit", _HISTORY_LIMIT_DEFAULT)
    if isinstance(history_limit, bool) or not isinstance(history_limit, int) \
            or not 1 <= history_limit <= 10000:
        raise AlertsError(
            f"alerts.history_limit must be an int in 1..10000, "
            f"got {history_limit!r}")

    return AlertEngine(
        budget_warn=budget_warn,
        budget_critical=budget_critical,
        leak_window=leak_window,
        rules=rules,
        webhook_url=webhook_url,
        webhook_timeout=webhook_timeout,
        history_limit=history_limit,
    )

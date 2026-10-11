"""Cost guard for the Hermes Router-Proxy (Stage 5).

Three cooperating mechanisms between the Stage 4 budget gate and upstream
dispatch:

* **Sliding-window limiters** (RPM / TPM / RPD generalized as
  ``requests``/``tokens`` over an arbitrary ``window_seconds``) in three
  scopes: ``principal`` (per virtual key; the admin gateway key is exempt,
  mirroring the Stage 4 budget gate), ``pool`` (per key-pool free-tier
  ceiling, enforced at the pooled dispatch door) and ``global`` (every
  request, admin included).
* **Estimate-to-actual reconciliation** — requests are admitted against a
  prompt-token estimate (chars / ``estimate_chars_per_token``); when the
  completion's real usage arrives at the ledger-append choke point, the
  estimate is reconciled to the actual total via a signed correction event
  on the principal/global token windows. Drift (|actual - estimate|) is
  accumulated for the reconciliation report (risk R6).
* **Billing-leak watchdog** — upstream 401/402 responses (configurable)
  are recorded as leak events: Stage 3 pooling owns 429 (cooldown) and
  401/403 (quarantine), but a 402 "payment required" from a provider is a
  *billing* signal nobody else owns — it means the flat-cost lane is
  degrading (risk R2). The watchdog also cross-checks successful
  completions against ledger appends (billing visibility).

**Pacer** — a token-bucket dispatch smoother (``max_burst`` immediate
admissions, then ``requests_per_second`` steady) so the gateway holds
under configured ceilings instead of bursting into provider 429s.

Fail-closed: invalid ``cost_guard`` configuration refuses startup /
reload (``CostGuardError``). A missing section disables the guard with
zero behavior change (pre-Stage-5 configs and fixtures stay identical).

Concurrency: one guard-wide lock serializes preflight / pool-gate /
reconcile check-and-admit pairs; the pacer and watchdog carry their own
locks. All operations are in-memory and O(window events) — no I/O under
any lock.

License: MIT
"""

from __future__ import annotations

import math
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque

__all__ = [
    "CostGuardError", "LimiterRule", "AdmissionTicket", "SlidingWindow",
    "Limiter", "Pacer", "BillingWatchdog", "CostGuard", "build_cost_guard",
    "SCOPES", "DIMENSIONS", "slugify",
]

# ── Constants ───────────────────────────────────────────────────────────────

SCOPES = ("principal", "pool", "global")
DIMENSIONS = ("requests", "tokens")
DEFAULT_WATCHDOG_STATUSES = (401, 402)
DEFAULT_CHARS_PER_TOKEN = 4
_WATCHDOG_EVENT_LIMIT = 128


class CostGuardError(RuntimeError):
    """Raised on invalid cost_guard configuration (fail closed)."""


def slugify(name: str) -> str:
    """Metric-safe identifier fragment for a limiter name."""
    return re.sub(r"[^0-9a-zA-Z_]", "_", name)


# ── Sliding window ──────────────────────────────────────────────────────────

@dataclass
class LimiterRule:
    """One configured limiter (immutable after validation)."""

    name: str
    scope: str            # one of SCOPES
    dimension: str        # "requests" | "tokens"
    limit: int            # max admitted weight inside the window
    window_seconds: float


class SlidingWindow:
    """True sliding window over (timestamp, weight) events.

    Weights are ints (request counts / token counts); reconciliation appends
    signed corrections (actual - estimate), so the in-window sum is exact
    for principal/global token limiters.
    """

    __slots__ = ("_events",)

    def __init__(self) -> None:
        self._events: Deque[tuple[float, int]] = deque()

    def prune(self, now: float, window: float) -> None:
        horizon = now - window
        while self._events and self._events[0][0] <= horizon:
            self._events.popleft()

    def sum(self) -> int:
        return sum(w for _, w in self._events)

    def event_count(self) -> int:
        return len(self._events)

    def append(self, ts: float, weight: int) -> None:
        self._events.append((ts, weight))

    def retry_after(self, now: float, weight: int, limit: int,
                    window: float) -> float:
        """Seconds until enough weight expires for ``weight`` to fit.

        Walks the events oldest-first simulating progressive expiry; if the
        window fully draining still cannot fit the weight (the request alone
        exceeds the limit), returns the full window as the drain horizon.
        """
        remaining = self.sum()
        if remaining + weight <= limit:
            return 0.0
        for ts, w in self._events:
            remaining -= w
            if remaining + weight <= limit:
                return max(0.0, (ts + window) - now)
        return float(window)


class Limiter:
    """A rule plus per-identity sliding windows and counters."""

    def __init__(self, rule: LimiterRule):
        self.rule = rule
        self._windows: dict[str, SlidingWindow] = {}
        self.admits = 0
        self.blocks = 0

    def _window(self, identity: str) -> SlidingWindow:
        win = self._windows.get(identity)
        if win is None:
            win = SlidingWindow()
            self._windows[identity] = win
        return win

    def used(self, identity: str, now: float) -> int:
        win = self._windows.get(identity)
        if win is None:
            return 0
        win.prune(now, self.rule.window_seconds)
        return win.sum()

    def check(self, identity: str, weight: int, now: float) -> dict[str, Any]:
        """Verdict without mutation: {allowed, used, limit, retry_after...}."""
        win = self._window(identity)
        win.prune(now, self.rule.window_seconds)
        used = win.sum()
        allowed = used + weight <= self.rule.limit
        retry = 0.0 if allowed else win.retry_after(
            now, weight, self.rule.limit, self.rule.window_seconds)
        return {
            "allowed": allowed,
            "limiter": self.rule.name,
            "scope": self.rule.scope,
            "identity": identity,
            "dimension": self.rule.dimension,
            "limit": self.rule.limit,
            "used": used,
            "window_seconds": self.rule.window_seconds,
            "retry_after_seconds": round(retry, 3),
        }

    def admit(self, identity: str, weight: int, now: float) -> None:
        self._window(identity).append(now, weight)
        self.admits += 1

    def correct(self, identity: str, delta: int, now: float) -> None:
        """Reconciliation correction event (signed; no admit counter)."""
        if delta:
            self._window(identity).append(now, delta)

    def identities(self, now: float) -> dict[str, dict[str, int]]:
        """In-window snapshot per identity (pruned first)."""
        out: dict[str, dict[str, int]] = {}
        for ident, win in self._windows.items():
            win.prune(now, self.rule.window_seconds)
            out[ident] = {"used": win.sum(), "events": win.event_count()}
        return out


# ── Pacer (token bucket) ────────────────────────────────────────────────────

class Pacer:
    """Global dispatch smoother: burst now, then hold requests_per_second."""

    def __init__(self, requests_per_second: float, max_burst: int):
        self.requests_per_second = float(requests_per_second)
        self.max_burst = int(max_burst)
        self._tokens = float(max_burst)
        self._last = time.monotonic()
        self._lock = threading.Lock()
        self.waits_total = 0
        self.last_wait_seconds = 0.0

    def acquire(self, now: float | None = None) -> float:
        """Reserve one dispatch slot; returns seconds the caller must wait."""
        now = time.monotonic() if now is None else now
        with self._lock:
            elapsed = max(0.0, now - self._last)
            self._tokens = min(
                float(self.max_burst), self._tokens + elapsed * self.requests_per_second)
            self._last = now
            self._tokens -= 1.0
            wait = 0.0 if self._tokens >= 0 else -self._tokens / self.requests_per_second
            if wait > 0:
                self.waits_total += 1
            self.last_wait_seconds = wait
            return wait

    def describe(self) -> dict[str, Any]:
        with self._lock:
            return {
                "enabled": True,
                "requests_per_second": self.requests_per_second,
                "max_burst": self.max_burst,
                "pace_waits_total": self.waits_total,
                "last_wait_seconds": round(self.last_wait_seconds, 3),
            }


# ── Billing-leak watchdog ───────────────────────────────────────────────────

@dataclass
class WatchdogEvent:
    ts: str
    status: int
    model: str
    key_id: str | None

    def as_dict(self) -> dict[str, Any]:
        return {"ts": self.ts, "status": self.status,
                "model": self.model, "key_id": self.key_id}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"


class BillingWatchdog:
    """Detects billing-plane degradation instead of preventing it.

    Fires on configured upstream statuses (default 401/402 — credential
    death and provider-side payment/quota signals) and cross-checks
    completions against ledger appends for visibility loss.
    """

    def __init__(self, statuses: tuple[int, ...] = DEFAULT_WATCHDOG_STATUSES):
        self.statuses = frozenset(statuses)
        self._lock = threading.Lock()
        self._events: Deque[WatchdogEvent] = deque(maxlen=_WATCHDOG_EVENT_LIMIT)
        self._by_status: dict[int, int] = {}
        self.events_total = 0
        self.completions_total = 0
        self.usage_missing_total = 0

    def observe(self, status: int, model: str,
                key_id: str | None) -> bool:
        """Record one upstream observation; True when it fires the watchdog."""
        if status not in self.statuses:
            return False
        with self._lock:
            self.events_total += 1
            self._by_status[status] = self._by_status.get(status, 0) + 1
            self._events.append(
                WatchdogEvent(_now_iso(), status, model, key_id))
        return True

    def note_completion(self, has_usage: bool) -> None:
        with self._lock:
            self.completions_total += 1
            if not has_usage:
                self.usage_missing_total += 1

    def report(self, *, ledger_enabled: bool,
               ledger_appends: int) -> dict[str, Any]:
        with self._lock:
            events = [e.as_dict() for e in self._events]
            by_status = {str(k): v for k, v in sorted(self._by_status.items())}
            completions = self.completions_total
            usage_missing = self.usage_missing_total
        if not ledger_enabled:
            visibility, unbilled = "disabled", 0
        else:
            unbilled = max(0, completions - ledger_appends)
            visibility = "ok" if unbilled == 0 else "leaking"
        return {
            "statuses": sorted(self.statuses),
            "events_total": self.events_total,
            "by_status": by_status,
            "events": events,
            "completions_total": completions,
            "usage_missing_total": usage_missing,
            "billing_visibility": visibility,
            "unbilled_completions": unbilled,
        }


# ── Admission tickets (estimate-to-actual reconciliation) ──────────────────

@dataclass
class AdmissionTicket:
    principal_id: str
    estimated_tokens: int
    admitted_at: float
    status: str = "open"  # open | reconciled | abandoned
    actual_tokens: int | None = None
    is_admin: bool = False  # principal-scope windows never touched (mirrors preflight)

    def as_dict(self) -> dict[str, Any]:
        return {
            "principal_id": self.principal_id,
            "estimated_tokens": self.estimated_tokens,
            "status": self.status,
            "actual_tokens": self.actual_tokens,
        }


# ── Facade ──────────────────────────────────────────────────────────────────

@dataclass
class _GuardStats:
    estimates_total: int = 0
    estimated_tokens_total: int = 0
    reconciled_total: int = 0
    actual_tokens_total: int = 0
    unreconciled_total: int = 0
    drift_tokens_total: int = 0


@dataclass
class CostGuard:
    limiters: list[Limiter] = field(default_factory=list)
    pacer: Pacer | None = None
    watchdog: BillingWatchdog = field(default_factory=BillingWatchdog)
    chars_per_token: float = DEFAULT_CHARS_PER_TOKEN
    _stats: _GuardStats = field(default_factory=_GuardStats, repr=False)

    def __post_init__(self) -> None:
        self._lock = threading.Lock()

    # ── estimation ──────────────────────────────────────────────────────
    def estimate_tokens(self, payload: dict[str, Any]) -> int:
        """Prompt-token estimate: total message text chars / cpt, min 1."""
        chars = 0
        for m in payload.get("messages") or []:
            if not isinstance(m, dict):
                continue
            content = m.get("content")
            if isinstance(content, str):
                chars += len(content)
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and isinstance(part.get("text"), str):
                        chars += len(part["text"])
        return max(1, math.ceil(chars / self.chars_per_token))

    # ── preflight: principal + global scopes, pre-routing ───────────────
    def preflight(self, payload: dict[str, Any], *, principal_id: str,
                  is_admin: bool,
                  now: float | None = None) -> tuple[AdmissionTicket | None,
                                                     dict[str, Any] | None]:
        """Admission control before any routing/upstream contact.

        Returns (ticket, None) when admitted, or (None, block) when a
        limiter refuses. Principal scope gates virtual-key principals only
        (the operator key is limiter-exempt by design, mirroring Stage 4
        budgets); global scope gates every principal.
        """
        now = time.monotonic() if now is None else now
        est = self.estimate_tokens(payload)
        with self._lock:
            admits: list[tuple[Limiter, str, int]] = []
            for limiter in self.limiters:
                if limiter.rule.scope == "pool":
                    continue  # pool door is per dispatch attempt
                if limiter.rule.scope == "principal" and is_admin:
                    continue
                identity = (principal_id
                            if limiter.rule.scope == "principal" else "global")
                weight = 1 if limiter.rule.dimension == "requests" else est
                verdict = limiter.check(identity, weight, now)
                if not verdict["allowed"]:
                    limiter.blocks += 1
                    return None, verdict
                admits.append((limiter, identity, weight))
            for limiter, identity, weight in admits:
                limiter.admit(identity, weight, now)
            self._stats.estimates_total += 1
            self._stats.estimated_tokens_total += est
        return AdmissionTicket(principal_id, est, now, is_admin=is_admin), None

    # ── pool door: per pooled dispatch attempt ──────────────────────────
    def pool_gate(self, pool_name: str, payload: dict[str, Any],
                  now: float | None = None) -> dict[str, Any] | None:
        """Pool-scope ceilings at the pooled dispatch door.

        None == attempt allowed (and admitted); a block dict means the
        caller must skip this attempt like an exhausted pool. Atomic
        like preflight: pool windows are touched only when every pool
        limiter admits. The pool windows keep the prompt estimate (each
        upstream attempt genuinely consumed prompt tokens); only
        principal/global windows reconcile.
        """
        now = time.monotonic() if now is None else now
        est = self.estimate_tokens(payload)
        with self._lock:
            admits: list[tuple[Limiter, int]] = []
            for limiter in self.limiters:
                if limiter.rule.scope != "pool":
                    continue
                weight = 1 if limiter.rule.dimension == "requests" else est
                verdict = limiter.check(pool_name, weight, now)
                if not verdict["allowed"]:
                    limiter.blocks += 1
                    return verdict
                admits.append((limiter, weight))
            for limiter, weight in admits:
                limiter.admit(pool_name, weight, now)
        return None

    # ── reconciliation ──────────────────────────────────────────────────
    def reconcile(self, ticket: AdmissionTicket,
                  usage: dict[str, Any] | None,
                  now: float | None = None) -> None:
        """Swap the estimate for the actual usage on token windows."""
        if ticket.status != "open" or usage is None:
            return
        now = time.monotonic() if now is None else now
        prompt = int(usage.get("prompt_tokens", 0) or 0)
        completion = int(usage.get("completion_tokens", 0) or 0)
        actual = int(usage.get("total_tokens", prompt + completion) or 0)
        if actual <= 0:
            return
        delta = actual - ticket.estimated_tokens
        with self._lock:
            for limiter in self.limiters:
                if limiter.rule.dimension != "tokens":
                    continue
                if limiter.rule.scope == "principal":
                    # principal-scope windows are admin-exempt at preflight;
                    # corrections must stay symmetric with admissions or the
                    # window would accumulate bare deltas (no estimate).
                    if not ticket.is_admin:
                        limiter.correct(ticket.principal_id, delta, now)
                elif limiter.rule.scope == "global":
                    limiter.correct("global", delta, now)
                # pool scope: estimates are kept (documented, conservative)
            ticket.status = "reconciled"
            ticket.actual_tokens = actual
            self._stats.reconciled_total += 1
            self._stats.actual_tokens_total += actual
            self._stats.drift_tokens_total += abs(delta)

    def abandon(self, ticket: AdmissionTicket) -> None:
        """Request ended without usage (failed / interrupted / never routed)."""
        if ticket.status != "open":
            return
        ticket.status = "abandoned"
        with self._lock:
            self._stats.unreconciled_total += 1

    # ── watchdog / pacer passthroughs ───────────────────────────────────
    @property
    def watchdog_statuses(self) -> frozenset[int]:
        return self.watchdog.statuses

    def observe_upstream(self, status: int, model: str,
                         key_id: str | None) -> bool:
        return self.watchdog.observe(status, model, key_id)

    def note_completion(self, has_usage: bool) -> None:
        self.watchdog.note_completion(has_usage)

    def pace(self) -> float:
        """Pacer slot reservation; 0.0 when the pacer is off."""
        return self.pacer.acquire() if self.pacer is not None else 0.0

    # ── reporting ───────────────────────────────────────────────────────
    def stats(self) -> dict[str, int]:
        """Flat counters for the metrics surface (guard = single source)."""
        with self._lock:
            s = self._stats
            out = {
                "cost_guard_estimates_total": s.estimates_total,
                "cost_guard_reconciled_total": s.reconciled_total,
                "cost_guard_unreconciled_total": s.unreconciled_total,
                "cost_guard_estimate_drift_tokens_total": s.drift_tokens_total,
            }
        out["cost_guard_blocks_total"] = sum(lm.blocks for lm in self.limiters)
        out["cost_guard_pace_waits_total"] = (
            self.pacer.waits_total if self.pacer is not None else 0)
        wd = self.watchdog
        out["cost_guard_watchdog_events_total"] = wd.events_total
        out["cost_guard_completions_total"] = wd.completions_total
        out["cost_guard_usage_missing_total"] = wd.usage_missing_total
        return out

    def describe(self, *, ledger_enabled: bool,
                 ledger_appends: int) -> dict[str, Any]:
        """Full admin snapshot (config + counters + in-window loads)."""
        now = time.monotonic()
        with self._lock:
            s = self._stats
            stats_snapshot = {
                "estimates_total": s.estimates_total,
                "estimated_tokens_total": s.estimated_tokens_total,
                "reconciled_total": s.reconciled_total,
                "actual_tokens_total": s.actual_tokens_total,
                "unreconciled_total": s.unreconciled_total,
                "drift_tokens_total": s.drift_tokens_total,
            }
            limiters = [
                {
                    "name": lm.rule.name,
                    "scope": lm.rule.scope,
                    "dimension": lm.rule.dimension,
                    "limit": lm.rule.limit,
                    "window_seconds": lm.rule.window_seconds,
                    "admits": lm.admits,
                    "blocks": lm.blocks,
                    "identities": lm.identities(now),
                }
                for lm in self.limiters
            ]
        return {
            "enabled": True,
            "estimate_chars_per_token": self.chars_per_token,
            "limiters": limiters,
            "pacer": (self.pacer.describe() if self.pacer is not None
                      else {"enabled": False}),
            "watchdog": self.watchdog.report(
                ledger_enabled=ledger_enabled, ledger_appends=ledger_appends),
            "reconciliation": stats_snapshot,
        }


# ── Config validation + construction (fail-closed) ──────────────────────────

def _num(value: Any, label: str, *, minimum: float = 0.0,
         exclusive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CostGuardError(f"cost_guard.{label} must be a number, got {value!r}")
    if exclusive and value <= minimum:
        raise CostGuardError(f"cost_guard.{label} must be > {minimum}, got {value!r}")
    if not exclusive and value < minimum:
        raise CostGuardError(f"cost_guard.{label} must be >= {minimum}, got {value!r}")
    return float(value)


def build_cost_guard(cfg: dict[str, Any]) -> CostGuard | None:
    """Build from ``cfg["cost_guard"]``. Missing/empty/disabled → None."""
    section = cfg.get("cost_guard")
    if section is None:
        return None
    if not isinstance(section, dict):
        raise CostGuardError("cost_guard section must be a mapping")
    enabled = section.get("enabled", True)
    if not isinstance(enabled, bool):
        raise CostGuardError(
            f"cost_guard.enabled must be a boolean, got {enabled!r}")
    if not enabled:
        return None

    guard = CostGuard()

    cpt = section.get("estimate_chars_per_token", DEFAULT_CHARS_PER_TOKEN)
    guard.chars_per_token = _num(
        cpt, "estimate_chars_per_token", minimum=0, exclusive=True)

    # Limiters
    raw_limiters = section.get("limiters", [])
    if not isinstance(raw_limiters, list):
        raise CostGuardError("cost_guard.limiters must be a list")
    seen_names: set[str] = set()
    for i, raw in enumerate(raw_limiters):
        if not isinstance(raw, dict):
            raise CostGuardError(f"cost_guard.limiters[{i}] must be a mapping")
        name = raw.get("name")
        if not isinstance(name, str) or not name.strip():
            raise CostGuardError(f"cost_guard.limiters[{i}].name must be a "
                                 f"non-empty string")
        if name in seen_names:
            raise CostGuardError(f"cost_guard limiter name duplicated: {name!r}")
        seen_names.add(name)
        scope = raw.get("scope")
        if scope not in SCOPES:
            raise CostGuardError(
                f"cost_guard.limiters[{i}].scope must be one of "
                f"{list(SCOPES)}, got {scope!r}")
        has_requests = "requests" in raw
        has_tokens = "tokens" in raw
        if has_requests == has_tokens:  # both or neither
            raise CostGuardError(
                f"cost_guard.limiters[{i}] ({name!r}) must set exactly one "
                f"of 'requests' or 'tokens'")
        if has_requests:
            dimension = "requests"
            limit = _num(raw["requests"], f"limiters[{i}].requests",
                         minimum=1, exclusive=False)
            if limit != int(limit):
                raise CostGuardError(
                    f"cost_guard.limiters[{i}].requests must be an integer")
        else:
            dimension = "tokens"
            limit = _num(raw["tokens"], f"limiters[{i}].tokens",
                         minimum=1, exclusive=False)
            if limit != int(limit):
                raise CostGuardError(
                    f"cost_guard.limiters[{i}].tokens must be an integer")
        window = _num(raw.get("window_seconds"),
                      f"limiters[{i}].window_seconds", minimum=0, exclusive=True)
        guard.limiters.append(Limiter(LimiterRule(
            name=name, scope=scope, dimension=dimension,
            limit=int(limit), window_seconds=window)))

    # Pacer (absent sub-section = off)
    raw_pacer = section.get("pacer")
    if raw_pacer is not None:
        if not isinstance(raw_pacer, dict):
            raise CostGuardError("cost_guard.pacer must be a mapping")
        rate = _num(raw_pacer.get("requests_per_second"),
                    "pacer.requests_per_second", minimum=0, exclusive=True)
        burst = _num(raw_pacer.get("max_burst"), "pacer.max_burst",
                     minimum=1, exclusive=False)
        if burst != int(burst):
            raise CostGuardError("cost_guard.pacer.max_burst must be an integer")
        guard.pacer = Pacer(rate, int(burst))

    # Watchdog (absent = default statuses; empty list rejected)
    raw_wd = section.get("watchdog")
    if raw_wd is None:
        statuses: tuple[int, ...] = DEFAULT_WATCHDOG_STATUSES
    else:
        if not isinstance(raw_wd, dict):
            raise CostGuardError("cost_guard.watchdog must be a mapping")
        raw_statuses = raw_wd.get("statuses", list(DEFAULT_WATCHDOG_STATUSES))
        if not isinstance(raw_statuses, list) or not raw_statuses:
            raise CostGuardError(
                "cost_guard.watchdog.statuses must be a non-empty list")
        parsed: list[int] = []
        for s in raw_statuses:
            if isinstance(s, bool) or not isinstance(s, int) \
                    or not 400 <= s <= 599:
                raise CostGuardError(
                    f"cost_guard.watchdog.statuses entries must be HTTP "
                    f"status ints in 400..599, got {s!r}")
            parsed.append(s)
        statuses = tuple(parsed)
    guard.watchdog = BillingWatchdog(statuses)
    return guard

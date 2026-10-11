"""Stage 5 cost-guard tests: limiters, pacer, watchdog, reconciliation.

Exit gate (Chapter 9, Stage 5):
  * limiter tests at boundary values (unit + e2e)
  * watchdog fires on injected 402/401 (e2e against the deterministic stub)
  * pacer holds configured ceilings (unit with injected clocks)

Unit tests are offline. The e2e block drives the real server process
against the capture harness's deterministic stub upstream (same harness
pattern as test_security / test_billing).
"""
from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))  # for `import costguard`

from costguard import (  # noqa: E402
    AdmissionTicket,
    BillingWatchdog,
    CostGuard,
    CostGuardError,
    Limiter,
    LimiterRule,
    Pacer,
    SlidingWindow,
    build_cost_guard,
)

# Reuse the deterministic stub upstream from the capture harness
_spec = importlib.util.spec_from_file_location(
    "capture_golden", REPO / "tools" / "capture_golden.py")
_cgmod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cgmod)
build_stub_app = _cgmod.build_stub_app


# ═══════════════════════════════ Unit: sliding window ═══════════════════════

class TestSlidingWindow:
    def test_prune_evicts_at_and_before_horizon(self):
        w = SlidingWindow()
        w.append(9.9, 1)     # older than horizon -> evicted
        w.append(10.0, 2)    # exactly at horizon (<=) -> evicted
        w.append(10.5, 4)    # inside the window -> kept
        w.append(20.0, 8)
        w.prune(30.0, 20.0)          # horizon = 10.0
        assert w.sum() == 12
        assert w.event_count() == 2

    def test_sum_over_signed_corrections(self):
        w = SlidingWindow()
        w.append(1.0, 8)
        w.append(2.0, 3)    # reconcile correction +3
        w.append(3.0, -5)   # reconcile correction -5
        assert w.sum() == 6

    def test_retry_after_progressive_expiry(self):
        w = SlidingWindow()
        w.append(100.0, 5)
        w.append(110.0, 5)
        # limit 10, new weight 6 -> BOTH events must expire (5+6 and
        # 0+6 checks walk the deque oldest-first)
        r = w.retry_after(now=115.0, weight=6, limit=10, window=30.0)
        assert r == pytest.approx(25.0)  # 110 + 30 - 115

    def test_retry_after_full_window_when_weight_exceeds_limit(self):
        w = SlidingWindow()
        w.append(100.0, 5)
        # weight 20 alone > limit 10 -> whole window is the drain horizon
        assert w.retry_after(105.0, 20, 10, 60.0) == 60.0

    def test_retry_after_zero_when_it_fits(self):
        w = SlidingWindow()
        w.append(100.0, 5)
        assert w.retry_after(101.0, 5, 10, 60.0) == 0.0


# ═══════════════════════════════ Unit: limiter boundary ═════════════════════

class TestLimiterBoundary:
    def _lim(self, limit=2, dimension="requests", window=60.0):
        return Limiter(LimiterRule(
            name="t", scope="principal", dimension=dimension,
            limit=limit, window_seconds=window))

    def test_admit_at_exact_boundary_allowed(self):
        lm = self._lim(limit=2)
        assert lm.check("p1", 1, now=0.0)["allowed"] is True
        lm.admit("p1", 1, now=0.0)
        v = lm.check("p1", 1, now=0.1)
        assert v["allowed"] is True      # 1 + 1 == limit 2
        assert v["used"] == 1
        lm.admit("p1", 1, now=0.1)
        assert lm.check("p1", 1, now=0.2)["allowed"] is False  # 2 + 1 > 2

    def test_block_verdict_shape(self):
        lm = self._lim(limit=2)
        lm.admit("p1", 1, now=0.0)
        lm.admit("p1", 1, now=0.0)
        v = lm.check("p1", 1, now=0.5)
        assert v["allowed"] is False
        assert v["limiter"] == "t" and v["scope"] == "principal"
        assert v["identity"] == "p1" and v["dimension"] == "requests"
        assert v["used"] == 2 and v["limit"] == 2
        assert v["window_seconds"] == 60.0
        assert v["retry_after_seconds"] == pytest.approx(59.5, abs=0.2)

    def test_window_expiry_frees_capacity(self):
        lm = self._lim(limit=1, window=10.0)
        lm.admit("p1", 1, now=0.0)
        assert lm.check("p1", 1, now=5.0)["allowed"] is False
        assert lm.check("p1", 1, now=9.9)["allowed"] is False
        # at exactly window elapsed the event hits the horizon (<=) and goes
        assert lm.check("p1", 1, now=10.0)["allowed"] is True

    def test_identities_are_isolated(self):
        lm = self._lim(limit=1)
        lm.admit("a", 1, now=0.0)
        assert lm.check("a", 1, now=0.1)["allowed"] is False
        assert lm.check("b", 1, now=0.1)["allowed"] is True

    def test_correction_moves_used_exact(self):
        lm = self._lim(limit=10, dimension="tokens")
        lm.admit("p", 8, now=0.0)
        lm.correct("p", 3, now=1.0)   # actual 11 vs est 8
        assert lm.used("p", now=2.0) == 11
        lm.correct("p", -5, now=3.0)  # over-estimate correction
        assert lm.used("p", now=4.0) == 6

    def test_used_zero_for_unknown_identity(self):
        assert self._lim().used("nobody", now=0.0) == 0


# ═══════════════════════════════ Unit: pacer ceilings ═══════════════════════

class TestPacer:
    def test_burst_admitted_with_zero_wait(self):
        p = Pacer(requests_per_second=10.0, max_burst=5)
        t0 = 1000.0
        waits = [p.acquire(now=t0 + i * 0.001) for i in range(5)]
        assert waits == [0.0] * 5

    def test_steady_rate_enforced_after_burst(self):
        p = Pacer(requests_per_second=2.0, max_burst=1)
        t0 = 1000.0
        assert p.acquire(now=t0) == 0.0
        w = p.acquire(now=t0 + 0.01)   # burst consumed -> wait ~0.5s
        assert w == pytest.approx(0.495, abs=0.01)
        # continuous acquires hold the steady ceiling (~1/rate apart)
        w2 = p.acquire(now=t0 + 0.01 + 0.5)
        assert w2 == pytest.approx(0.5, abs=0.05)
        # a full idle period (>= 1/rate after the last acquire) restores burst
        assert p.acquire(now=t0 + 0.01 + 0.5 + 1.0) == 0.0

    def test_refill_capped_at_max_burst(self):
        p = Pacer(requests_per_second=10.0, max_burst=2)
        p.acquire(now=0.0)
        p.acquire(now=0.0)
        p.acquire(now=0.0)             # burst consumed, one waiting
        # long idle: refill caps at 2 tokens
        assert p.acquire(now=100.0) == 0.0
        assert p.acquire(now=100.0) == 0.0
        assert p.acquire(now=100.0) > 0.0

    def test_describe_shape(self):
        p = Pacer(requests_per_second=5.0, max_burst=3)
        d = p.describe()
        assert d == {"enabled": True, "requests_per_second": 5.0,
                     "max_burst": 3, "pace_waits_total": 0,
                     "last_wait_seconds": 0.0}


# ═══════════════════════════════ Unit: watchdog ═════════════════════════════

class TestBillingWatchdog:
    def test_fires_on_configured_statuses_only(self):
        wd = BillingWatchdog(statuses=(401, 402))
        assert wd.observe(402, "m1", "pool/key-a") is True
        assert wd.observe(401, "m1", None) is True
        assert wd.observe(200, "m1", None) is False
        assert wd.observe(429, "m1", None) is False   # Stage 3 owns 429
        assert wd.observe(500, "m1", None) is False

    def test_report_counters_and_events(self):
        wd = BillingWatchdog()
        wd.observe(402, "stub-complex", None)
        wd.observe(401, "stub-simple", "rotate/key-a")
        r = wd.report(ledger_enabled=False, ledger_appends=0)
        assert r["events_total"] == 2
        assert r["by_status"] == {"401": 1, "402": 1}
        assert [e["status"] for e in r["events"]] == [402, 401]
        assert r["events"][1]["key_id"] == "rotate/key-a"
        assert r["billing_visibility"] == "disabled"  # ledger off

    def test_billing_visibility_ok_and_leaking(self):
        wd = BillingWatchdog()
        wd.note_completion(True)
        wd.note_completion(True)
        assert wd.report(ledger_enabled=True, ledger_appends=2)[
            "billing_visibility"] == "ok"
        assert wd.report(ledger_enabled=True, ledger_appends=1)[
            "billing_visibility"] == "leaking"
        assert wd.report(ledger_enabled=True, ledger_appends=1)[
            "unbilled_completions"] == 1

    def test_usage_missing_counter(self):
        wd = BillingWatchdog()
        wd.note_completion(True)
        wd.note_completion(False)
        assert wd.report(ledger_enabled=False, ledger_appends=0)[
            "usage_missing_total"] == 1

    def test_event_ring_buffer_is_bounded(self):
        wd = BillingWatchdog()
        for i in range(200):
            wd.observe(402, f"m{i}", None)
        r = wd.report(ledger_enabled=False, ledger_appends=0)
        assert r["events_total"] == 200       # total is unbounded
        assert len(r["events"]) == 128        # ring is bounded


# ═══════════════════════════════ Unit: estimate / preflight / reconcile ═════

def _guard_cfg(**over: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "cost_guard": {
            "enabled": True,
            "estimate_chars_per_token": 4,
            "limiters": [],
        }
    }
    cfg["cost_guard"].update(over)
    return cfg


class TestEstimation:
    def test_estimate_from_string_content(self):
        g = build_cost_guard(_guard_cfg()) or CostGuard()
        msg = {"messages": [{"role": "user", "content": "x" * 34}]}
        assert g.estimate_tokens(msg) == 9   # ceil(34/4)

    def test_estimate_from_multimodal_list_content(self):
        g = build_cost_guard(_guard_cfg()) or CostGuard()
        msg = {"messages": [{"role": "user", "content": [
            {"type": "text", "text": "x" * 20},
            {"type": "image_url", "image_url": {"url": "http://x"}},
        ]}]}
        assert g.estimate_tokens(msg) == 5   # only text parts count

    def test_estimate_minimum_one(self):
        g = build_cost_guard(_guard_cfg()) or CostGuard()
        assert g.estimate_tokens({"messages": []}) == 1
        assert g.estimate_tokens({}) == 1

    def test_estimate_counts_all_messages(self):
        g = build_cost_guard(_guard_cfg()) or CostGuard()
        msg = {"messages": [
            {"role": "system", "content": "y" * 8},
            {"role": "user", "content": "x" * 8},
        ]}
        assert g.estimate_tokens(msg) == 4


class TestPreflight:
    CFG = {
        "cost_guard": {
            "enabled": True,
            "estimate_chars_per_token": 4,
            "limiters": [
                {"name": "r2", "scope": "principal", "requests": 2,
                 "window_seconds": 60},
                {"name": "pool1", "scope": "pool", "requests": 1,
                 "window_seconds": 60},
                {"name": "tok19", "scope": "principal", "tokens": 19,
                 "window_seconds": 60},
                {"name": "glob", "scope": "global", "requests": 5,
                 "window_seconds": 60},
            ],
        }
    }

    def _guard(self) -> CostGuard:
        g = build_cost_guard(self.CFG)
        assert g is not None
        return g

    def test_principal_scope_skips_admin(self):
        g = self._guard()
        payload = {"messages": [{"role": "user", "content": "hi"}]}
        for i in range(5):
            t, block = g.preflight(payload, principal_id="admin",
                                   is_admin=True, now=float(i))
            assert block is None and t is not None
        # the global limiter DID gate admin (limit 5)
        t, block = g.preflight(payload, principal_id="admin",
                               is_admin=True, now=6.0)
        assert t is None
        assert block["limiter"] == "glob"

    def test_principal_scope_gates_virtual_key(self):
        g = self._guard()
        payload = {"messages": [{"role": "user", "content": "hi"}]}
        assert g.preflight(payload, principal_id="vk1", is_admin=False,
                           now=0.0)[1] is None
        assert g.preflight(payload, principal_id="vk1", is_admin=False,
                           now=0.1)[1] is None
        t, block = g.preflight(payload, principal_id="vk1", is_admin=False,
                               now=0.2)
        assert t is None
        assert block["limiter"] == "r2"
        assert block["used"] == 2 and block["limit"] == 2

    def test_pool_scope_not_checked_in_preflight(self):
        g = self._guard()
        payload = {"messages": [{"role": "user", "content": "hi"}]}
        # exhaust the pool window directly: preflight stays unaffected
        assert g.pool_gate("p", payload, now=0.0) is None
        assert g.pool_gate("p", payload, now=0.1) is not None
        assert g.preflight(payload, principal_id="vk2", is_admin=False,
                           now=0.2)[1] is None

    def test_preflight_admission_is_all_or_nothing(self):
        g = self._guard()
        payload = {"messages": [{"role": "user", "content": "x" * 80}]}
        # est = 20 > token limit 19 -> token limiter blocks. The requests
        # limiter passed BEFORE it -- verify it did NOT admit anything.
        t, block = g.preflight(payload, principal_id="vk3", is_admin=False,
                               now=0.0)
        assert t is None and block["limiter"] == "tok19"
        r2 = next(lm for lm in g.limiters if lm.rule.name == "r2")
        assert r2.used("vk3", now=0.5) == 0      # no partial admission

    def test_block_verdict_carries_identity(self):
        g = self._guard()
        payload = {"messages": [{"role": "user", "content": "hi"}]}
        g.preflight(payload, principal_id="vk4", is_admin=False, now=0.0)
        g.preflight(payload, principal_id="vk4", is_admin=False, now=0.1)
        block = g.preflight(payload, principal_id="vk4", is_admin=False,
                            now=0.2)[1]
        assert block["identity"] == "vk4"
        assert block["scope"] == "principal"
        assert block["dimension"] == "requests"


class TestPoolGate:
    def test_admit_then_block_per_pool_identity(self):
        g = build_cost_guard({
            "cost_guard": {"limiters": [
                {"name": "pool-1", "scope": "pool", "requests": 1,
                 "window_seconds": 60}]}})
        assert g is not None
        payload = {"messages": [{"role": "user", "content": "hi"}]}
        assert g.pool_gate("pool-a", payload, now=0.0) is None
        assert g.pool_gate("pool-b", payload, now=0.1) is None   # isolated
        b = g.pool_gate("pool-a", payload, now=0.2)
        assert b is not None and b["limiter"] == "pool-1"
        assert b["identity"] == "pool-a"

    def test_pool_gate_is_atomic_across_limiters(self):
        g = build_cost_guard({
            "cost_guard": {"limiters": [
                {"name": "pa", "scope": "pool", "requests": 5,
                 "window_seconds": 60},
                {"name": "pb", "scope": "pool", "requests": 1,
                 "window_seconds": 60}]}})
        assert g is not None
        payload = {"messages": [{"role": "user", "content": "hi"}]}
        assert g.pool_gate("p", payload, now=0.0) is None          # both admit
        b = g.pool_gate("p", payload, now=0.1)
        assert b is not None and b["limiter"] == "pb"              # pb blocks
        pa = next(lm for lm in g.limiters if lm.rule.name == "pa")
        assert pa.used("p", now=0.2) == 1   # pa did NOT admit on the blocked attempt


class TestReconciliation:
    def _guard(self) -> CostGuard:
        g = build_cost_guard({
            "cost_guard": {"estimate_chars_per_token": 4, "limiters": [
                {"name": "pt", "scope": "principal", "tokens": 19,
                 "window_seconds": 60},
                {"name": "gt", "scope": "global", "tokens": 1000,
                 "window_seconds": 60},
                {"name": "poolt", "scope": "pool", "tokens": 100,
                 "window_seconds": 60}]}})
        assert g is not None
        return g

    def test_estimate_swapped_for_actual_on_token_windows(self):
        g = self._guard()
        payload = {"messages": [{"role": "user", "content": "x" * 34}]}  # est 9
        ticket, block = g.preflight(payload, principal_id="vk", is_admin=False,
                                    now=0.0)
        assert block is None and ticket is not None
        g.pool_gate("p", payload, now=0.0)
        g.reconcile(ticket, {"prompt_tokens": 5, "completion_tokens": 6,
                             "total_tokens": 11}, now=1.0)
        pt = next(lm for lm in g.limiters if lm.rule.name == "pt")
        gt = next(lm for lm in g.limiters if lm.rule.name == "gt")
        poolt = next(lm for lm in g.limiters if lm.rule.name == "poolt")
        assert pt.used("vk", now=2.0) == 11        # 9 est + 2 correction
        assert gt.used("global", now=2.0) == 11
        assert poolt.used("p", now=2.0) == 9       # pool keeps the estimate
        assert ticket.status == "reconciled"
        assert ticket.actual_tokens == 11

    def test_reconcile_is_idempotent_per_ticket(self):
        g = self._guard()
        payload = {"messages": [{"role": "user", "content": "x" * 34}]}  # est 9
        ticket, _ = g.preflight(payload, principal_id="vk", is_admin=False,
                                now=0.0)
        g.reconcile(ticket, {"total_tokens": 11}, now=1.0)
        g.reconcile(ticket, {"total_tokens": 50}, now=2.0)   # second is a no-op
        pt = next(lm for lm in g.limiters if lm.rule.name == "pt")
        assert pt.used("vk", now=3.0) == 11

    def test_admin_reconcile_skips_principal_windows(self):
        # regression: admin tickets are principal-scope-exempt at preflight;
        # their corrections must not seed bare deltas into principal windows
        g = self._guard()
        payload = {"messages": [{"role": "user", "content": "x" * 34}]}  # est 9
        ticket, _ = g.preflight(payload, principal_id="admin", is_admin=True,
                                now=0.0)
        g.reconcile(ticket, {"total_tokens": 11}, now=1.0)
        pt = next(lm for lm in g.limiters if lm.rule.name == "pt")
        gt = next(lm for lm in g.limiters if lm.rule.name == "gt")
        assert "admin" not in pt.identities(now=2.0)   # never touched
        assert gt.used("global", now=2.0) == 11        # global still correct

    def test_reconcile_without_usage_is_noop(self):
        g = self._guard()
        ticket = AdmissionTicket("vk", 9, 0.0)
        g.reconcile(ticket, None, now=1.0)
        assert ticket.status == "open"

    def test_reconcile_ignores_zero_actual(self):
        g = self._guard()
        ticket = AdmissionTicket("vk", 9, 0.0)
        g.reconcile(ticket, {"total_tokens": 0}, now=1.0)
        assert ticket.status == "open"

    def test_abandon_counts_unreconciled_and_keeps_estimate(self):
        g = self._guard()
        payload = {"messages": [{"role": "user", "content": "x" * 34}]}
        ticket, _ = g.preflight(payload, principal_id="vk", is_admin=False,
                                now=0.0)
        g.abandon(ticket)
        g.abandon(ticket)                    # idempotent
        assert ticket.status == "abandoned"
        s = g.stats()
        assert s["cost_guard_unreconciled_total"] == 1
        pt = next(lm for lm in g.limiters if lm.rule.name == "pt")
        assert pt.used("vk", now=1.0) == 9   # conservative: estimate stays


# ═══════════════════════════════ Unit: config validation ════════════════════

class TestBuildCostGuard:
    def test_missing_section_disables(self):
        assert build_cost_guard({}) is None

    def test_explicit_disable(self):
        assert build_cost_guard({"cost_guard": {"enabled": False}}) is None

    def test_enabled_must_be_boolean(self):
        with pytest.raises(CostGuardError, match="enabled"):
            build_cost_guard({"cost_guard": {"enabled": "false"}})

    def test_section_must_be_mapping(self):
        with pytest.raises(CostGuardError, match="mapping"):
            build_cost_guard({"cost_guard": [1, 2]})

    def test_limiter_name_required_and_unique(self):
        base = {"cost_guard": {"limiters": [
            {"scope": "principal", "requests": 2, "window_seconds": 60}]}}
        with pytest.raises(CostGuardError, match="name"):
            build_cost_guard(base)
        dup = {"cost_guard": {"limiters": [
            {"name": "a", "scope": "principal", "requests": 2,
             "window_seconds": 60},
            {"name": "a", "scope": "global", "requests": 2,
             "window_seconds": 60}]}}
        with pytest.raises(CostGuardError, match="duplicated"):
            build_cost_guard(dup)

    def test_scope_must_be_known(self):
        with pytest.raises(CostGuardError, match="scope"):
            build_cost_guard({"cost_guard": {"limiters": [
                {"name": "a", "scope": "universe", "requests": 2,
                 "window_seconds": 60}]}})

    def test_exactly_one_dimension(self):
        with pytest.raises(CostGuardError, match="exactly one"):
            build_cost_guard({"cost_guard": {"limiters": [
                {"name": "a", "scope": "principal", "requests": 2,
                 "tokens": 3, "window_seconds": 60}]}})
        with pytest.raises(CostGuardError, match="exactly one"):
            build_cost_guard({"cost_guard": {"limiters": [
                {"name": "a", "scope": "principal",
                 "window_seconds": 60}]}})

    def test_limits_must_be_positive_integers(self):
        for bad in (0, -1, 2.5, "3", True):
            with pytest.raises(CostGuardError):
                build_cost_guard({"cost_guard": {"limiters": [
                    {"name": "a", "scope": "principal", "requests": bad,
                     "window_seconds": 60}]}})

    def test_window_must_be_positive(self):
        for bad in (0, -5, "x"):
            with pytest.raises(CostGuardError, match="window"):
                build_cost_guard({"cost_guard": {"limiters": [
                    {"name": "a", "scope": "principal", "requests": 2,
                     "window_seconds": bad}]}})

    def test_chars_per_token_positive_number(self):
        with pytest.raises(CostGuardError, match="chars_per_token"):
            build_cost_guard({"cost_guard": {"estimate_chars_per_token": 0}})
        with pytest.raises(CostGuardError, match="chars_per_token"):
            build_cost_guard({"cost_guard": {"estimate_chars_per_token": "4"}})

    def test_pacer_validation(self):
        with pytest.raises(CostGuardError, match="requests_per_second"):
            build_cost_guard({"cost_guard": {"pacer": {"max_burst": 5}}})
        with pytest.raises(CostGuardError, match="max_burst"):
            build_cost_guard({"cost_guard": {"pacer": {
                "requests_per_second": 5, "max_burst": 0}}})
        with pytest.raises(CostGuardError, match="max_burst"):
            build_cost_guard({"cost_guard": {"pacer": {
                "requests_per_second": 5, "max_burst": 2.5}}})

    def test_watchdog_status_validation(self):
        with pytest.raises(CostGuardError, match="statuses"):
            build_cost_guard({"cost_guard": {"watchdog": {"statuses": []}}})
        for bad in (200, 600, "402", True, 40.5):
            with pytest.raises(CostGuardError, match="statuses"):
                build_cost_guard({"cost_guard": {
                    "watchdog": {"statuses": [bad]}}})

    def test_full_config_builds(self):
        g = build_cost_guard({
            "cost_guard": {
                "estimate_chars_per_token": 4,
                "limiters": [
                    {"name": "rpm", "scope": "principal", "requests": 60,
                     "window_seconds": 60},
                    {"name": "tpm", "scope": "principal", "tokens": 100000,
                     "window_seconds": 60},
                    {"name": "rpd", "scope": "global", "requests": 1000,
                     "window_seconds": 86400},
                ],
                "pacer": {"requests_per_second": 10, "max_burst": 20},
                "watchdog": {"statuses": [402]},
            }})
        assert g is not None
        assert [lm.rule.name for lm in g.limiters] == ["rpm", "tpm", "rpd"]
        assert g.pacer is not None and g.pacer.max_burst == 20
        assert g.watchdog_statuses == frozenset({402})
        d = g.describe(ledger_enabled=True, ledger_appends=0)
        assert d["enabled"] is True
        assert len(d["limiters"]) == 3
        assert d["pacer"]["enabled"] is True
        assert d["watchdog"]["statuses"] == [402]

    def test_stats_shape(self):
        g = build_cost_guard({"cost_guard": {"limiters": [
            {"name": "a", "scope": "principal", "requests": 1,
             "window_seconds": 60}]}})
        assert g is not None
        s = g.stats()
        for key in ("cost_guard_estimates_total", "cost_guard_blocks_total",
                    "cost_guard_reconciled_total",
                    "cost_guard_unreconciled_total",
                    "cost_guard_pace_waits_total",
                    "cost_guard_watchdog_events_total",
                    "cost_guard_completions_total",
                    "cost_guard_usage_missing_total",
                    "cost_guard_estimate_drift_tokens_total"):
            assert key in s and s[key] == 0


# ═══════════════════════════════ E2E harness ═══════════════════════════════

STUB_PORT = 9931
ROUTER_PORT = 9932
ROUTER_URL = f"http://127.0.0.1:{ROUTER_PORT}"
STUB_URL = f"http://127.0.0.1:{STUB_PORT}"
ADMIN_KEY = "costguard-e2e-admin-key"
RUNTIME_FILES = ["server.py", "providers.py", "trace.py", "keystore.py",
                 "keypool.py", "catalog.py", "ledger.py", "costguard.py", "alerts.py", "dashboard.py",
                 "surrogate_models.py", "zero_shot_classifier.py",
                 "fasttext_classifier.py", "setfit_classifier.py"]


def _base_env(extra: dict) -> dict:
    env = {k: v for k, v in os.environ.items()
           if k.startswith(("LC_", "LANG_", "PATH"))}
    env.update({"PYTHONUNBUFFERED": "1", "HOME": "/tmp", **extra})
    return env


def start_router(config: dict, env_extra: dict | None = None,
                 port: int = ROUTER_PORT):
    tmp = Path(tempfile.mkdtemp(prefix="cg-test-"))
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


def wait_ready(url: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=2.0).status_code == 200:
                return True
        except Exception:
            time.sleep(0.2)
    return False


def write_config(tmp: Path, config: dict) -> None:
    import yaml
    (tmp / "router_config.yaml").write_text(yaml.safe_dump(config))


CATEGORIES = {
    "simple": {"label": "Simple", "model": "stub-simple",
               "base_url": f"{STUB_URL}/v1", "api_key_env": "",
               "fallback_model": "stub-fallback",
               "fallback_base_url": f"{STUB_URL}/v1",
               "fallback_key_env": "", "timeout_seconds": 20},
    "complex": {"label": "Complex", "model": "stub-complex",
                "base_url": f"{STUB_URL}/v1", "api_key_env": "",
                "timeout_seconds": 20},
    "pooled_guard": {"label": "Guarded pool", "model": "stub-simple",
                     "base_url": f"{STUB_URL}/v1",
                     "key_pool": "guard_pool", "timeout_seconds": 20},
}


def e2e_config(cost_guard: dict | None, port: int = ROUTER_PORT) -> dict:
    cfg: dict[str, Any] = {
        "server": {"host": "127.0.0.1", "port": port},
        "auth": {"mode": "required", "api_key_env": "CG_TEST_KEY"},
        "ledger": {"enabled": True, "db_path": "cg.sqlite3"},
        "pricing": {
            "default": {"input_per_mtok": 0.5, "output_per_mtok": 1.5},
            "entries": {
                "stub-simple": {"input_per_mtok": 0.0,
                                "output_per_mtok": 0.0, "free": True},
                "stub-complex": {"input_per_mtok": 1.25,
                                 "output_per_mtok": 5.0},
            },
        },
        "classifier": {"model": "stub-classifier",
                       "base_url": f"{STUB_URL}/v1",
                       "session_timeout_minutes": 5,
                       "zero_shot": {"enabled": False},
                       "fasttext": {"enabled": False},
                       "setfit": {"enabled": False}},
        "routing": {"override_prefix": "/use:", "escalation_keywords": [],
                    "de_escalation_keywords": []},
        "key_pools": {
            "guard_pool": {"default_cooldown_seconds": 1,
                           "keys": [{"ref": "CG_TEST_POOL_KEY",
                                     "label": "key-g"}]},
        },
        "categories": CATEGORIES,
    }
    if cost_guard is not None:
        cfg["cost_guard"] = cost_guard
    return cfg


GUARD_CONFIG = {
    "enabled": True,
    "estimate_chars_per_token": 4,
    "limiters": [
        {"name": "vk-two-requests", "scope": "principal", "requests": 2,
         "window_seconds": 3600},
        {"name": "guard-pool-one-request", "scope": "pool", "requests": 1,
         "window_seconds": 3600},
        {"name": "vk-token-budget", "scope": "principal", "tokens": 19,
         "window_seconds": 3600},
        {"name": "global-requests", "scope": "global", "requests": 1000,
         "window_seconds": 60},
    ],
    "pacer": {"requests_per_second": 100, "max_burst": 50},
    "watchdog": {"statuses": [401, 402]},
}


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
            if httpx.get(f"{STUB_URL}/health", timeout=2.0).status_code == 200:
                break
        except Exception:
            time.sleep(0.2)
    yield
    server.should_exit = True
    time.sleep(0.3)


@pytest.fixture(scope="module")
def router(stub):
    proc, tmp, log = start_router(
        e2e_config(GUARD_CONFIG),
        env_extra={"CG_TEST_KEY": ADMIN_KEY, "CG_TEST_POOL_KEY": "cgpool-key"})
    assert wait_ready(f"{ROUTER_URL}/health"), (tmp / "router.log").read_text()
    yield proc, tmp
    stop_router(proc, tmp, log)


def _chat(body: dict, token: str = ADMIN_KEY) -> httpx.Response:
    return httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=body,
                      headers={"Authorization": f"Bearer {token}"},
                      timeout=30.0)


def _make_vk(name: str) -> dict:
    r = httpx.post(f"{ROUTER_URL}/admin/virtual_keys", json={"name": name},
                   headers={"Authorization": f"Bearer {ADMIN_KEY}"},
                   timeout=10.0)
    assert r.status_code == 201, r.text
    return r.json()


def _costguard() -> dict:
    r = httpx.get(f"{ROUTER_URL}/admin/costguard",
                  headers={"Authorization": f"Bearer {ADMIN_KEY}"},
                  timeout=10.0)
    assert r.status_code == 200, r.text
    return r.json()


# ═══════════════════════════════ E2E: boot posture ══════════════════════════

def test_e2e_invalid_config_refuses_boot(stub):
    bad = e2e_config({"enabled": True, "limiters": [
        {"name": "broken", "scope": "principal", "requests": 0,
         "window_seconds": 60}]})
    proc, tmp, log = start_router(bad, env_extra={"CG_TEST_KEY": ADMIN_KEY})
    try:
        proc.wait(timeout=20)
        assert proc.returncode not in (0, None)
        log_text = (tmp / "router.log").read_text()
        assert "Refusing to start" in log_text
        assert "cost_guard" in log_text
    finally:
        stop_router(proc, tmp, log)


def test_e2e_admin_endpoint_is_admin_only(router):
    vk = _make_vk("cg-forbidden-probe")
    r = httpx.get(f"{ROUTER_URL}/admin/costguard",
                  headers={"Authorization": f"Bearer {vk['key']}"},
                  timeout=10.0)
    assert r.status_code == 403
    assert r.json()["detail"]["error"]["type"] == "admin_only"


def test_e2e_guard_reports_enabled(router):
    snap = _costguard()
    assert snap["enabled"] is True
    assert snap["estimate_chars_per_token"] == 4
    assert [lm["name"] for lm in snap["limiters"]] == [
        "vk-two-requests", "guard-pool-one-request",
        "vk-token-budget", "global-requests"]
    assert snap["pacer"]["enabled"] is True
    assert snap["watchdog"]["statuses"] == [401, 402]


# ═══════════════════════════════ E2E: limiter boundary ══════════════════════

def test_e2e_request_limiter_boundary_virtual_key(router):
    vk = _make_vk("cg-boundary-requests")
    hdr = {"Authorization": f"Bearer {vk['key']}"}
    def msgs(n):
        return {"model": "auto", "stream": False, "messages": [
            {"role": "user", "content": f"/use:simple boundary request {n}"}]}
    r1 = httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=msgs(1),
                    headers=hdr, timeout=30.0)
    r2 = httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=msgs(2),
                    headers=hdr, timeout=30.0)
    assert r1.status_code == 200 and r2.status_code == 200
    r3 = httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=msgs(3),
                    headers=hdr, timeout=30.0)
    assert r3.status_code == 429
    err = r3.json()["error"]
    assert err["type"] == "cost_guard_rate_limited"
    d = err["detail"]
    assert d["limiter"] == "vk-two-requests"
    assert d["scope"] == "principal"
    assert d["dimension"] == "requests"
    assert d["used"] == 2 and d["limit"] == 2
    assert d["retry_after_seconds"] > 0
    # blocked requests never reach a provider: no upstream call happened
    snap = _costguard()
    r2lim = next(lm for lm in snap["limiters"]
                 if lm["name"] == "vk-two-requests")
    assert r2lim["blocks"] >= 1


def test_e2e_admin_exempt_from_principal_limiters(router):
    # the operator key is limiter-exempt (mirrors the Stage 4 budget gate)
    for i in range(3):
        r = _chat({"model": "auto", "stream": False, "messages": [
            {"role": "user", "content": f"/use:simple admin exempt check {i}"}]})
        assert r.status_code == 200


def test_e2e_token_reconciliation_boundary(router):
    vk = _make_vk("cg-boundary-tokens")
    hdr = {"Authorization": f"Bearer {vk['key']}"}
    body = {"model": "auto", "stream": False, "messages": [
        {"role": "user", "content": "/use:simple reconcile token budget"}]}
    r1 = httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=body,
                    headers=hdr, timeout=30.0)
    assert r1.status_code == 200
    r2 = httpx.post(f"{ROUTER_URL}/v1/chat/completions", json=body,
                    headers=hdr, timeout=30.0)
    assert r2.status_code == 429
    d = r2.json()["error"]["detail"]
    assert d["limiter"] == "vk-token-budget"
    assert d["dimension"] == "tokens"
    # THE reconciliation proof: estimates alone (9+9=18) fit the limit 19;
    # the reconciled window (11) + new estimate (9) = 20 does not.
    assert d["used"] == 11
    assert d["limit"] == 19
    snap = _costguard()
    assert snap["reconciliation"]["reconciled_total"] >= 1
    assert snap["reconciliation"]["drift_tokens_total"] >= 2


def test_e2e_pool_gate_boundary(router):
    body1 = {"model": "auto", "stream": False, "messages": [
        {"role": "user", "content": "/use:pooled_guard pool gate first"}]}
    body2 = {"model": "auto", "stream": False, "messages": [
        {"role": "user", "content": "/use:pooled_guard pool gate second"}]}
    r1 = _chat(body1)
    assert r1.status_code == 200
    r2 = _chat(body2)
    # synthetic 429 at the dispatch door + no fallback tier -> 502 envelope
    assert r2.status_code == 502
    err = r2.json()["error"]
    assert err["type"] == "upstream_error"
    assert err["status_code"] == 429
    assert "cost guard limiter" in err["message"]
    assert "guard-pool-one-request" in err["message"]
    snap = _costguard()
    pool_lim = next(lm for lm in snap["limiters"]
                    if lm["name"] == "guard-pool-one-request")
    assert pool_lim["identities"]["guard_pool"]["used"] == 1
    assert pool_lim["blocks"] >= 1


def test_e2e_budget_block_precedes_limiter_block(stub):
    # Stage 4 gate runs first: a spent budget reports 402, not 429
    cfg = e2e_config({
        "enabled": True,
        "limiters": [{"name": "vk-one", "scope": "principal", "requests": 1,
                      "window_seconds": 3600}]}, port=9934)
    proc, tmp, log = start_router(
        cfg, env_extra={"CG_TEST_KEY": ADMIN_KEY, "CG_TEST_POOL_KEY": "k"},
        port=9934)
    try:
        assert wait_ready("http://127.0.0.1:9934/health")
        url = "http://127.0.0.1:9934"
        vk = httpx.post(f"{url}/admin/virtual_keys", json={
            "name": "precedence", "budget_mode": "cost_micros",
            "budget_limit": 36, "budget_period": "total"},
            headers={"Authorization": f"Bearer {ADMIN_KEY}"},
            timeout=10.0).json()
        hdr = {"Authorization": f"Bearer {vk['key']}"}
        body = {"model": "auto", "stream": False, "messages": [
            {"role": "user", "content": "/use:complex spend the budget"}]}
        r1 = httpx.post(f"{url}/v1/chat/completions", json=body,
                        headers=hdr, timeout=30.0)
        assert r1.status_code == 200        # spends exactly 36 micros
        r2 = httpx.post(f"{url}/v1/chat/completions", json=body,
                        headers=hdr, timeout=30.0)
        assert r2.status_code == 402        # budget, NOT limiter
        assert r2.json()["error"]["type"] == "budget_exceeded"
    finally:
        stop_router(proc, tmp, log)


# ═══════════════════════════════ E2E: watchdog injection ════════════════════

def test_e2e_watchdog_fires_on_injected_402(router):
    before = _costguard()["watchdog"]["events_total"]
    r = _chat({"model": "auto", "stream": False, "messages": [
        {"role": "user", "content": "/use:complex trigger billing-leak"}]})
    # terminal 402 on complex (no fallback) -> 502 envelope
    assert r.status_code == 502
    assert "402" in r.json()["error"]["message"]
    wd = _costguard()["watchdog"]
    assert wd["events_total"] == before + 1
    assert wd["by_status"].get("402", 0) >= 1
    ev = [e for e in wd["events"] if e["status"] == 402][-1]
    assert ev["model"] == "stub-complex"
    assert "ts" in ev and "key_id" in ev


def test_e2e_watchdog_billing_visibility_ok(router):
    snap = _costguard()
    wd = snap["watchdog"]
    assert wd["completions_total"] >= 1
    assert wd["billing_visibility"] == "ok"
    assert wd["unbilled_completions"] == 0


def test_e2e_unreconciled_includes_abandoned_tickets(router):
    snap = _costguard()
    # the injected-402 request and pool-gate-blocked request both abandoned
    assert snap["reconciliation"]["unreconciled_total"] >= 2


# ═══════════════════════════════ E2E: pacer ceiling ═════════════════════════

def test_e2e_pacer_holds_ceiling_and_still_serves(stub):
    cfg = e2e_config({
        "enabled": True,
        "limiters": [],
        "pacer": {"requests_per_second": 4.0, "max_burst": 1}}, port=9936)
    proc, tmp, log = start_router(
        cfg, env_extra={"CG_TEST_KEY": ADMIN_KEY, "CG_TEST_POOL_KEY": "k"},
        port=9936)
    try:
        assert wait_ready("http://127.0.0.1:9936/health")
        url = "http://127.0.0.1:9936"
        hdr = {"Authorization": f"Bearer {ADMIN_KEY}"}
        t0 = time.monotonic()
        codes = []
        for i in range(3):
            r = httpx.post(f"{url}/v1/chat/completions", json={
                "model": "auto", "stream": False, "messages": [
                    {"role": "user",
                     "content": f"/use:simple paced request {i}"}]},
                headers=hdr, timeout=30.0)
            codes.append(r.status_code)
        elapsed = time.monotonic() - t0
        assert codes == [200, 200, 200]    # waits happen, requests still serve
        # 3 requests at burst 1 + 4 rps: ~2 gaps of 0.25s each
        assert elapsed >= 0.4, "pacer did not hold the ceiling"
        snap = httpx.get(f"{url}/admin/costguard", headers=hdr,
                         timeout=10.0).json()
        assert snap["pacer"]["pace_waits_total"] >= 2
        assert snap["pacer"]["last_wait_seconds"] >= 0.0
    finally:
        stop_router(proc, tmp, log)


# ═══════════════════════════════ E2E: metrics surface ═══════════════════════

def test_e2e_metrics_expose_cost_guard_counters(router):
    r = httpx.get(f"{ROUTER_URL}/metrics",
                  headers={"Authorization": f"Bearer {ADMIN_KEY}"},
                  timeout=10.0)
    assert r.status_code == 200
    body = r.json()
    prom = body["prometheus"]
    for name in ("cost_guard_blocks_total",
                 "cost_guard_pace_waits_total",
                 "cost_guard_watchdog_events_total",
                 "cost_guard_completions_total",
                 "cost_guard_estimates_total",
                 "cost_guard_reconciled_total",
                 "cost_guard_unreconciled_total",
                 "cost_guard_estimate_drift_tokens_total"):
        assert f"hermes_router_{name}" in prom
        assert name in body["metrics"]
    # blocks were exercised by the boundary tests above
    assert body["metrics"]["cost_guard_blocks_total"] >= 3
    assert body["metrics"]["cost_guard_watchdog_events_total"] >= 1


# ═══════════════════════════════ E2E: hot reload ════════════════════════════

def test_e2e_reload_swap_and_fail_closed(stub):
    cfg_a = e2e_config({"enabled": True, "limiters": [
        {"name": "probe", "scope": "principal", "requests": 1,
         "window_seconds": 3600}]}, port=9938)
    proc, tmp, log = start_router(
        cfg_a, env_extra={"CG_TEST_KEY": ADMIN_KEY, "CG_TEST_POOL_KEY": "k"},
        port=9938)
    try:
        assert wait_ready("http://127.0.0.1:9938/health")
        url = "http://127.0.0.1:9938"
        hdr = {"Authorization": f"Bearer {ADMIN_KEY}"}
        vk = httpx.post(f"{url}/admin/virtual_keys",
                        json={"name": "reload-probe"},
                        headers=hdr, timeout=10.0).json()
        vhdr = {"Authorization": f"Bearer {vk['key']}"}
        body = {"model": "auto", "stream": False, "messages": [
            {"role": "user", "content": "/use:simple reload probe one"}]}
        assert httpx.post(f"{url}/v1/chat/completions", json=body,
                          headers=vhdr, timeout=30.0).status_code == 200
        blocked = httpx.post(f"{url}/v1/chat/completions", json=body,
                             headers=vhdr, timeout=30.0)
        assert blocked.status_code == 429
        assert blocked.json()["error"]["detail"]["limiter"] == "probe"

        # invalid candidate config -> 400, previous guard stays live
        write_config(tmp, e2e_config({"enabled": True, "limiters": [
            {"name": "bad", "scope": "principal", "requests": 0,
             "window_seconds": 60}]}))
        rr = httpx.post(f"{url}/reload", headers=hdr, timeout=10.0)
        assert rr.status_code == 400
        assert "cost_guard" in rr.text
        still = httpx.post(f"{url}/v1/chat/completions", json=body,
                           headers=vhdr, timeout=30.0)
        assert still.status_code == 429      # guard A intact

        # valid swap -> fresh windows under the new guard
        write_config(tmp, e2e_config({"enabled": True, "limiters": [
            {"name": "probe2", "scope": "principal", "requests": 100,
             "window_seconds": 3600}]}))
        assert httpx.post(f"{url}/reload", headers=hdr,
                          timeout=10.0).status_code == 200
        ok = httpx.post(f"{url}/v1/chat/completions", json=body,
                        headers=vhdr, timeout=30.0)
        assert ok.status_code == 200
        snap = httpx.get(f"{url}/admin/costguard", headers=hdr,
                         timeout=10.0).json()
        assert [lm["name"] for lm in snap["limiters"]] == ["probe2"]

        # absent section -> guard cleared
        write_config(tmp, e2e_config(None))
        assert httpx.post(f"{url}/reload", headers=hdr,
                          timeout=10.0).status_code == 200
        assert httpx.get(f"{url}/admin/costguard", headers=hdr,
                         timeout=10.0).json() == {"enabled": False}
        final = httpx.post(f"{url}/v1/chat/completions", json=body,
                           headers=vhdr, timeout=30.0)
        assert final.status_code == 200
        prom = httpx.get(f"{url}/metrics", headers=hdr,
                         timeout=10.0).json()["prometheus"]
        assert "hermes_router_cost_guard_blocks_total" not in prom
    finally:
        stop_router(proc, tmp, log)

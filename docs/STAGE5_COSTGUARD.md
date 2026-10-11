# Stage 5 — Cost Guard (Exit-Gate Memo)

**Status:** PASS · **Date:** 2026-10-11 · **Branch:** `stage-0-baseline` (staged commits)
**Scope authority:** approved D1+D2 (v1 = Stages 0–5), dossier Ch.9 stage table (Table 7, cost guard):

> *Stage 5 — Cost guard: generalized sliding-window limiters (RPM/TPM/RPD)
> with estimate-to-actual reconciliation; billing-leak watchdog; pacer.
> Exit gate: limiter tests at boundary values; watchdog fires on injected
> 402/401; pacer holds configured ceilings.*

Adapted patterns (dossier Ch.7, best-implementation matrix): free-tier hard
limiters from `mgorb/gemini-rate-proxy` (ADAPT — sliding windows +
estimate-to-actual reconciliation, generalized from single-provider to
scope-per-identity), billing-leak watchdog and free-cap pacer from
`slamdunktiger/zero-cost-hermes-augmentation` (ADAPT — detection concept,
reimplemented gateway-native). Zero code taken from either; both are
pattern-level adaptations of MIT-licensed repos.

**Session note:** the previous session collapsed mid-Stage-5 with the module
written and partially integrated. This session completed the integration
from the recovered workspace (module untouched except two hardening fixes),
then tests, fixtures, and gates.

---

## 1. What Was Built

### 1.1 `costguard.py` (new, ~660 LOC, stdlib-only, carried over + hardened)

Three cooperating mechanisms, all in-memory, all O(window events), no I/O
under any lock:

**Sliding-window limiters** — `LimiterRule(name, scope, dimension, limit,
window_seconds)` generalized as requests/tokens over an arbitrary window
(true sliding window over `(ts, weight)` events, not fixed buckets — RPM,
TPM and RPD are just window_seconds 60/60/86400). Three scopes:

| Scope | Identity | Enforced at | Exemptions |
|---|---|---|---|
| `principal` | virtual-key id | preflight (pre-routing) | admin gateway key (mirrors the Stage 4 budget gate) |
| `pool` | key-pool name | pooled dispatch door | none (a blocked attempt = pool exhaustion: synthetic 429, cascade advances) |
| `global` | `"global"` | preflight | none — admin included |

`retry_after()` walks the deque oldest-first simulating progressive expiry,
so a blocked request gets an honest wait hint (and the full-window drain
horizon when a single request exceeds the limit by itself).

**Estimate-to-actual reconciliation** — requests are admitted against a
prompt-token estimate (`chars / estimate_chars_per_token`, min 1, str and
multimodal list content). When the completion's real usage lands at the
`_ledger_append` choke point, `reconcile()` swaps the estimate for the
actual total via a **signed correction event** (`actual - estimate`) on the
principal/global token windows — windows hold exact usage, not estimates.
Tickets that end without usage (failed / interrupted / never routed) are
`abandon()`ed and counted as unreconciled. Pool windows deliberately keep
the estimate (each upstream attempt genuinely consumed prompt tokens —
conservative direction). Drift (`|actual - estimate|`) accumulates for the
R6 reconciliation report. `AdmissionTicket` carries `is_admin` so
principal-scope corrections stay symmetric with admissions.

**Billing-leak watchdog** — upstream 401/402 (configurable, 400–599) are
recorded as leak events with model + key attribution. Stage 3 pooling owns
429 (cooldown) and 401/403 (quarantine); a 402 "payment required" is a
*billing* signal nobody else owns — it means the flat-cost lane is degrading
(risk R2). The report also cross-checks completions against ledger appends:
`billing_visibility: ok | leaking | disabled`, `unbilled_completions` (risk
R9's early-warning surface). Event ring is bounded (128) with unbounded
totals.

**Pacer** — global token-bucket dispatch smoother (`max_burst` immediate
admits, then `requests_per_second` steady) so the gateway holds under
configured ceilings instead of bursting into provider 429s.

**Fail-closed config** — `build_cost_guard()` validates everything: boolean
`enabled`, positive `estimate_chars_per_token`, unique non-empty limiter
names, known scopes, exactly one of `requests`/`tokens`, positive-integer
limits, positive windows, pacer rate/burst, watchdog statuses in 400–599.
A missing section → guard off with zero behavior change (pre-Stage-5
configs stay valid); an invalid section → `CostGuardError` → refused
startup/reload.

### 1.2 Dispatch integration (server.py, surgical)

- **Preflight + pacer at the entry point** (`chat_completions`): after the
  Stage 4 budget gate (precedence preserved: spent budget = 402, not 429),
  before any routing. A block returns `429 cost_guard_rate_limited` with
  the full verdict (limiter, scope, identity, dimension, limit, used,
  window, retry hint). Rejected requests never reach a provider — no trace
  events, no upstream contact (fixture-pinned).
- **Pool gate at both dispatch doors**: the nonstream pooled lane
  (`call_model`) returns a synthetic 429 that engages the existing
  fallback cascade; the streaming lane mirrors pool-exhaustion semantics
  per tier attempt (`continue` to the next tier). Both carry trace_route
  429 events + tier 429 metrics.
- **Watchdog observation on all four upstream lanes**: nonstream pooled
  (per key attempt), nonstream single-key (final outcome incl. the
  alternate-key retry), streaming initial response, and streaming 503-retry
  responses.
- **Reconciliation at the single choke point**: `_ledger_append` — every
  completion (7 call sites: 3 nonstream, 4 stream) passes the admission
  ticket; usage present → reconcile, usage None → abandon. Terminal
  failures (`_proxy_error` paths, early contract errors, all-tiers
  exhausted) abandon explicitly. A crash between preflight and the choke
  point abandons via the entry-point exception handler.
- **Boot/reload lifecycle**: `_stage5_bootstrap` (validate-then-commit,
  all-or-nothing like Stages 3–4) wired into startup, hot-reload, and
  main-before-bind. An invalid candidate config leaves the previous guard
  live (reload answers 400); an absent section clears the guard.
- **Surfaces**: `GET /admin/costguard` (admin-only; full describe:
  limiters, per-identity in-window loads, admits/blocks, pacer, watchdog
  events, reconciliation counters, billing visibility) and nine
  `cost_guard_*` counters on `/metrics` (JSON dict + Prometheus lines; the
  guard is the single source; the section is absent when the guard is
  disabled).

### 1.3 Fixes made to the carried-over module

1. **`pool_gate` atomicity** — pool windows are now touched only when every
   pool-scope limiter admits (was: sequential admit-then-check, which
   could leave an admit behind a later block).
2. **`enabled` type check** — a string `"false"` no longer truthy-enables
   the guard; non-boolean `enabled` refuses config.
3. **Admin reconciliation symmetry** (found by the fixture diff): admin
   tickets are principal-scope-exempt at preflight, but `reconcile()` still
   wrote their bare correction deltas into principal token windows —
   seeding an `admin` identity with no matching estimates. Fixed:
   `AdmissionTicket.is_admin`; principal-scope corrections skip admin
   tickets (global still corrected — global gates everyone). Regression-
   tested in unit + pinned by fixture 93 (principal identities are exactly
   the VK_* labels).

---

## 2. Boundary Arithmetic (why the fixtures prove the mechanisms)

The fixture config picks limits so each block **is** the proof:

| Scenario | Limiter | Arithmetic |
|---|---|---|
| 21b/21c/21d (VK_D) | `vk-two-requests` (principal, requests, 2) | two admits (2/2), third blocked — exact boundary |
| 22b/22c (VK_E) | `vk-token-budget` (principal, tokens, 19) | req1: est 9 admitted, reconciled 9→11 (+2). req2: 11 + est 9 = 20 > 19 **blocked** — but estimate-only 9+9=18 would fit: `used=11` in the 429 detail is the reconciliation proof |
| 23a/23b (guard_pool) | `guard-pool-one-request` (pool, requests, 1) | first pooled dispatch admitted (served by key-g), second synthetic-429 at the door; no fallback tier → 502 envelope naming the limiter |
| 24a (admin) | watchdog (402) | stub injects 402 on "billing-leak"; terminal on the no-fallback complex tier → 502 envelope; watchdog records model + key |
| 93 | snapshot | admits/blocks/identities/loads all deterministic for the pinned scenario order (global 24 admits; unreconciled 3 = no-user 400 + pool block + 402) |

Pacer fixture discipline: burst 50 >> capture traffic → `pace_waits_total`
pins at 0 (deterministic); the pacer's ceiling math is unit-tested with
injected clocks (burst free, steady ~1/rate, refill cap) and e2e-tested on
a dedicated server (rps 4 / burst 1: three sequential requests all serve
200, elapsed ≥ 0.4s, `pace_waits_total` ≥ 2).

## 3. Verification (all gates green)

- **ruff** 0 findings (rule set E4/E7/E9/F, ratchet intact) · **mypy** 0
  issues (8 files now — `costguard.py` added to the CI list) · **bandit**
  clean on the 8-file runtime surface · **pip-audit** clean (closure
  unchanged — costguard is stdlib-only).
- **pytest 340 passed** (was 259): +66 in `tests/test_costguard.py`
  (sliding-window/limiter boundary math, pacer ceilings, watchdog
  semantics incl. ring bound and billing visibility, estimation over str
  and multimodal content, preflight all-or-nothing admission, pool-gate
  atomicity, reconcile idempotence and admin symmetry, ~15-case
  fail-closed config matrix, plus live-server e2e: boot refusal, admin
  gating, request/token/pool boundaries, budget-precedence, injected-402
  watchdog, pacer ceiling, metrics surface, hot-reload swap/fail-closed)
  and +15 fixture contract tests/updates.
- **Golden fixtures re-captured: 43 fixtures** (was 33). Diff review
  against Stage 4: **29 of 33 byte-identical** — every behavior fixture
  01–20 (including the Stage 4 billing scenarios) unchanged with the guard
  enabled; intended deltas only: `14` (new `pooled_guard` category in the
  provider-resolution map), `91` (nine `cost_guard_*` metric names +
  pooled_guard category lines + guard_pool gauge), `92` (guard_pool entry:
  key-g 1 acquired / 1 success), `index` (timestamp, HEAD, cost_guard
  config, Stage 5 notes). New: 21a–d, 22a–c, 23a–b, 24a, 93.
- **Security sweep**: no plaintext tokens, no pool key material, no raw vk
  ids in any fixture (VK_D/VK_E labels; `retry_after` scrubbed to
  `[computed]`; watchdog `ts` scrubbed).

## 4. Design Decisions

- **Budget gate precedence** (402 before 429): a spent budget is the
  operator's own signal and answers first; limiter blocks are the second
  line. Pinned by fixture 19c (unchanged) and the e2e precedence test.
- **Admin exemption symmetry**: the operator key skips principal-scope
  limiters AND their corrections (a gateway operator must not be locked
  out of their own gateway by subscriber-shaped limits); global scope
  still gates admin — a free-tier ceiling doesn't care who asked.
- **Pool estimates are kept, not reconciled**: every upstream attempt
  genuinely consumed prompt tokens on that key, so the conservative
  direction is the correct one. Principal/global windows reconcile because
  the request (not the attempt) is the billing unit.
- **Watchdog observes, never heals autonomously**: auto-heal to a free
  path (the source repo's cron design) is deferred to Stage 6 operations
  (alerting) per the dossier's cost-guard plane split — the guard surfaces
  degradation (`by_status`, `billing_visibility`); acting on it stays an
  operator decision.
- **In-memory windows by design**: limiters protect *this process's*
  dispatch rate; durable truth stays in the Stage 4 ledger. Restart clears
  windows (a fresh process has dispatched nothing) — consistent with
  circuit breakers and pool cooldowns.

## 5. Exit Gate Verdict

| Criterion (dossier Ch.9) | Evidence |
|---|---|
| Limiter tests at boundary values | unit boundary suite + e2e 429s at exact limits (21d, 22c, 23b) + fixture contracts |
| Watchdog fires on injected 402/401 | e2e + fixture 24a + 93 (`by_status {"402": 1}`) |
| Pacer holds configured ceilings | unit ceiling math + e2e rps-4/burst-1 (all served, ≥2 waits, elapsed ≥ 0.4s) |
| Stage gate additions (Ch.11.2) | limiter boundary + watchdog-injection tests: both suites in `tests/test_costguard.py` |
| No behavior change when disabled | 29/33 fixtures byte-identical with the guard *enabled and admitting*; guard-off covered by reload e2e + `{"enabled": false}` |

**Stage 5 exit gate: PASS.** All five stage planes (ingress hardening,
providers, pooling+catalog, billing, cost guard) are now live under one
config, one revertible commit range per stage. Remaining v1 scope per D2:
Stage 6 (operations & UX) — dashboard, alerting, compose profile, runbook.

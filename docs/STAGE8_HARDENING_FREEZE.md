# Stage 8 — Hardening & Freeze (Exit-Gate Memo)

**Date**: 2026-10-12 · **Scope source**: dossier Ch.9 Table 7 row 8 +
Ch.9.3 stage-specific gate additions + risk R9's "documented backup and
recovery drill in Stage 8" + Ch.10's "Stage 8 re-scores [the risk
register] before release".

**Row 8 definition** — *Key activities*: load and soak tests (streaming
concurrency); failure drills (provider outage, key exhaustion, ledger
corruption recovery); documentation freeze; final audit. *Exit gate*:
all suites green; risk register reviewed; release tagged.

---

## 1. What Was Built

### 1.1 The defect the load gate was designed to catch (server.py fix)

The load suite's first sustained-concurrency run failed its own ledger
invariant: **996 completed requests, 967 ledger rows, zero append
failures** — 29 cost records were never even attempted. The cause,
isolated causally (a client-side linger experiment moved the loss to
exactly zero): when a streaming client disconnects at the final
`[DONE]` frame, uvicorn cancels the response generator at its last
`yield` *before* the post-loop `_ledger_append` runs. Billing
completeness was racing socket teardown, and losing ~3% of stream cost
records under sustained concurrency.

The fix is minimal and structural: both stream pump sites in
`route_request_stream` (the main lane and the 503-retry lane) now track
a `ledger_written` flag and append an `interrupted` cost record in a
`finally` guard when the generator exits at a yield without having
written one — cancellation, `GeneratorExit`, and mid-stream client
vanishing all land a record. The cost guard's admission ticket is
reconciled/abandoned by the same choke point, so estimate-to-actual
reconciliation also closes for those requests now (previously they
counted as unreconciled).

Evidence: `tests/test_load_soak.py::TestStreamingLoad` asserts exact
ledger equality (one row per request) across 32-way concurrent
streaming with immediate-close clients, plus exact virtual-key
attribution under concurrency.

### 1.2 `tests/test_load_soak.py` (new) — load + compressed soak

* **Load** (`TestStreamingLoad`): a 32-way concurrent streaming wave
  (unique marker per stream; every stream must reassemble *its own*
  content — cross-stream contamination is detectable), then a 24-way
  mixed stream/nonstream wave through a virtual key. Asserts: all
  200s, usage frame + `[DONE]` on every stream, exact ledger row
  count, exact VK-attributed row count, per-category metric deltas,
  pool availability recovery, file-descriptor hygiene (fd count back
  to baseline + ≤10) and RSS bounded (< +96 MB) — the resource-leak
  checks the dossier's "sustained streaming concurrency" language
  implies.
* **Soak** (`TestCompressedSoak`): a deterministic fault lattice over a
  compressed window (default 12 s, `HERMES_ROUTER_TEST_SOAK_SECONDS`):
  rotation phase (one pool key 429s → cooldown rotation), exhaustion
  phase (both keys 429 → synthetic `pool_exhausted` 429 → fallback
  cascade), recovery, all sampled by the alerting heartbeat
  (`/admin/status`) every 0.7 s. Asserts zero client-visible failures,
  exact ledger equality, `pool_exhausted`/`pool_429`/`fallback_used`
  counters all engaged, pool fully recovered, circuits never left
  open, pool-exhaustion alert raised *and* cleared, monotone counters,
  bounded RSS/fd drift, and a green `ledger/verify` at the end.
* **Harness hardening** (lesson from the drills, applied here too):
  `_start_router` refuses to boot against an occupied port instead of
  racing a leftover listener.

The 24-hour operator run is `tools/soak.py` (below) + RUNBOOK §8 —
same layering precedent as the Stage 6 compose gate and the Stage 7
live-Ollama lane: deterministic proof in CI, operator-scale proof as a
documented procedure.

### 1.3 `tests/test_failure_drills.py` (new) — the three dossier drills

* **Drill A — provider outage**: primary URL dies (connection refused →
  synthetic 503); the shipped circuit threshold (3) trips; every client
  request still 200 via the fallback cascade; `/circuits` shows open;
  the `circuit_open` alert raises and reaches the webhook; the provider
  revives and after `recovery_timeout_sec` the half-open trial closes
  the circuit and clears the alert; the manual `POST /circuits/reset`
  path is drilled separately.
* **Drill B — key exhaustion**: both pool keys rate-limited → raw 429s
  cascade (request 1), then `pool_exhausted` synthetic 429 cascade
  (request 2, `acquire_all` empty); `/admin/pools` shows 0 available
  with per-key `consecutive_429`; critical alert + webhook; natural
  cooldown-expiry recovery; operator `POST /admin/pools/{name}/unlock`
  (2 keys reset immediately); alert cleared after recovery.
* **Drill C — ledger corruption + recovery (R9)**: the full runbook §6/§7
  procedure end-to-end — online `sqlite3 .backup`, stop, tamper
  `cost_total_micros` on middle row seq 3, offline
  `python -m ledger verify --db` exits 1 naming `first_bad_seq: 3`,
  restart (the append-only plane boots; detection is via
  `/admin/ledger/verify` + the `ledger_verify` critical alert +
  webhook), then the documented restore (stop → restore → remove WAL
  sidecars → start) returns the chain to green with history intact and
  appending (seq continuity), confirmed by both the endpoint and the
  offline CLI.

### 1.4 `tools/soak.py` (new, stdlib-only) — the 24-hour operator driver

Sustained mixed stream/nonstream traffic with unique markers and full
stream reassembly, heartbeat sampling (`/admin/status` +
alerting timeline), graceful SIGINT/SIGTERM (report still written),
JSON + Markdown reports (status histogram, p50/p95/p99, error rate,
timeline table), preflight auth check, and a PASS/FAIL verdict
(zero 5xx + `--max-error-rate`, default 0). No third-party imports —
it runs on the deployed host. Validated live in-suite (see §4).

### 1.5 Documentation freeze

* **RUNBOOK §8** rewritten: each drill as (automated test → what it
  proves) + (manual rehearsal procedure with exact commands); new §8.6
  documents the 24-hour soak procedure with `tools/soak.py`, its pass
  criteria and cadence.
* **README**: new "Release v1.0.0" section (freeze notice, gate
  summary, evidence pointers) + operator pointers to the drills/soak.
* **docs/RELEASE.md** (new): the release manifest — version, date,
  stage ledger, gate results, the risk register re-score (Ch.10
  requires Stage 8 to re-score before release), known limitations,
  open owner decision (root LICENSE, carried from Stage 0).
* **pyproject.toml**: project version 0.1.0 → 1.0.0, matching the
  runtime `app.version` the status surface has reported since Stage 6.
* **`tests/test_freeze.py`** (new): the freeze contracts — README
  endpoint-table parity with the actual route surface, RUNBOOK §8
  coverage, RELEASE.md structure (gate table + R1–R10 re-score rows),
  version consistency (pyproject == RELEASE.md == server
  `app.version`), stage-memo presence 0–8, `tools/soak.py`
  stdlib-only + live smoke against a real gateway (the operator tool
  is release-critical, so its PASS path is pinned, not assumed).

## 2. The Exit Gate, Layered

| Gate | Evidence |
|---|---|
| All suites green | §4 gate table (ruff / mypy / bandit / pip-audit / pytest) |
| Load (sustained streaming concurrency) | `TestStreamingLoad` — exact billing under 32-way streams |
| Soak (24-hour stability) | `TestCompressedSoak` (deterministic lattice) + `tools/soak.py` (operator procedure, live-smoked) |
| Failure drills (outage / exhaustion / ledger recovery) | `test_failure_drills.py` — three full lifecycles |
| Final documentation audit | `test_freeze.py` — docs-as-test contracts |
| Risk register reviewed | RELEASE.md §5 — R1–R10 re-scored |
| Release tagged | annotated tag `v1.0.0` on the stage-8 commit |

## 3. Design Decisions

* **Fix, don't waive.** The stream-billing loss was found by the Stage
  8 gate itself. The dossier's rule is explicit: "A stage with any red
  check does not exit; failures loop back to implementation, never
  forward." The fix is behavior-preserving on every pinned surface
  (all 46 golden fixtures re-verified byte-identical after it) and
  closes a billing-completeness hole that every prior stage's
  sequential harness was structurally unable to see.
* **Soak fault-lattice uses a high circuit threshold** (50) so the
  pool-exhaustion path is exercised deterministically; under concurrent
  429 bursts a low circuit threshold opens the circuit mid-burst and
  masks the pool path (measured, not assumed — the first soak run
  showed `pool_exhausted_total == 0` with 165 fallbacks). Circuit
  behavior is Drill A's dedicated territory.
* **Drill B fires two requests** to reach the synthetic-429 path: the
  pooled nonstream funnel returns the raw second-key 429 (which
  cascades) and only reports `pool_exhausted` when `acquire_all`
  finds no lease — pinned as the documented two-step exhaustion shape.
* **Fixed test ports are pre-checked as free** before any boot. An
  orphaned listener on a drill port silently absorbs the drill (the
  health check races the failed bind); the harness now fails loudly
  instead. This cost twenty minutes of archaeology once; it should
  never cost it again.
* **`tools/soak.py` is stdlib-only** like the other operator-facing
  modules (`alerts.py`, `dashboard.py`, the plugin): the 24-hour run
  happens on the deployed host, where installing test dependencies is
  exactly what you don't want to do.

## 4. Gate Evidence

| Gate | Result |
|---|---|
| ruff 0.16.8 (`E4/E7/E9/F`) | 0 findings |
| mypy 2.3.1 (11 runtime files) | 0 errors |
| bandit 1.9.4 (runtime surface) | 0 issues |
| pip-audit (32-dist pinned closure) | clean |
| pytest | see RELEASE.md (all passed; new: load/soak + drills + freeze) |
| golden fixture re-capture | 46/46 behavior fixtures byte-identical; index metadata + Stage 8 note only |

## 5. Exit Gate Verdict

**PASS** — all suites green; the three dossier failure drills plus
load and compressed soak are test-pinned; the 24-hour soak and manual
drill rehearsals are documented procedures with a validated driver;
the risk register is re-scored in RELEASE.md; the release is tagged
`v1.0.0`. One owner decision remains open by explicit deferral (root
LICENSE file, Stage 0) — recorded in RELEASE.md §6, not blocking the
tag by prior owner instruction.

# RELEASE.md — hermes-router v1.0.0

**Release**: v1.0.0 (Stage 8 freeze) · **Date**: 2026-10-12
**Base**: fork of kjames2001/hermes-router-proxy @ `0a9560e`
(MIT — upstream file-header claims; root LICENSE resolved at publication, §6)
**Scope**: the approved D2 v1 plan (Stages 0–8), delivered as nine
revertible stage commits on `stage-0-baseline`, tagged `v1.0.0`.

This is the release manifest the dossier's Ch.9 row-8 exit gate
requires: all suites green, risk register re-scored (Ch.10), release
tagged. The stage-by-stage engineering record lives in
`docs/STAGE{0..8}_*.md`; operations live in `docs/RUNBOOK.md`.

---

## 1. What this release is

A hardened, self-hosted LLM gateway for the Hermes agent: mandatory
auth and an encrypted key store (Stage 1); an OpenAI/Anthropic provider
abstraction (Stage 2); key pooling with 429-header-aware cooldown
rotation plus a seven-tier free-tier catalog (Stage 3); an immutable
hash-chained cost ledger with virtual keys and blocking budgets
(Stage 4); sliding-window cost guard with estimate-to-actual
reconciliation and a billing-leak watchdog (Stage 5); alerting, a
zero-asset admin dashboard and a hardened compose profile (Stage 6);
Hermes onboarding (`/v1/models` + the optional model-provider plugin)
(Stage 7); and the Stage 8 hardening: load/soak with streaming
concurrency, the three failure drills, documentation freeze and this
final audit.

## 2. Stage ledger

| Stage | Commit | Scope |
|---|---|---|
| 0 | `694f82d` | baseline: source audit, scaffold, golden fixture suite |
| 1 | `1677141` | security hardening (auth, keystore, CORS, redaction) |
| 2 | `b1a64c6` | provider abstraction (openai + anthropic adapters) |
| 3 | `4f3c19f` | key pooling + free-tier catalog |
| 4 | `306ca11` | immutable cost ledger, virtual keys, budgets |
| 5 | `0c6cddb` | cost guard (limiters, reconciliation, watchdog) |
| 6 | `bee8169` | operations (alerting, dashboard, compose, runbook) |
| 7 | `c06a2f6` | Hermes integration (/v1/models, plugin, guide) |
| 8 | (this commit) | hardening & freeze (load/soak, drills, docs freeze, audit) |

## 3. Gate results (final audit, 2026-10-12)

| Gate | Result |
|---|---|
| ruff 0.16.8 (`E4/E7/E9/F`) | 0 findings |
| mypy 2.3.1 (11 runtime files) | 0 errors |
| bandit 1.9.4 (runtime surface) | 0 issues |
| pip-audit (`requirements-audit.txt`, 32-dist closure) | clean |
| pytest (full suite) | 491 passed, 4 skipped (live-Ollama lane) |
| golden fixture re-capture | 46/46 behavior fixtures byte-identical vs Stage 7 |

Stage 8 additions to the suite: `tests/test_load_soak.py` (load +
compressed soak), `tests/test_failure_drills.py` (the three dossier
drills), `tests/test_freeze.py` (documentation-freeze contracts +
`tools/soak.py` live smoke).

## 4. The Stage 8 hardening findings (worth knowing at release)

1. **Stream billing completeness** — the load gate found that a
   streaming client disconnecting at the final `[DONE]` frame could
   cancel the response generator before its cost record was written
   (~3% of stream records lost under sustained concurrency). Fixed
   with a `finally`-guarded append on both stream pump lanes; the
   golden surface is unchanged (all 46 fixtures byte-identical), and
   the load suite now asserts exact one-row-per-request billing under
   32-way concurrent streams.
2. **Concurrent 429 bursts mask pool exhaustion behind the circuit
   breaker** (threshold 3) — measured, documented, and the soak /
   drills now deliberately isolate the two mechanisms (high circuit
   threshold for the pool lattice; Drill A owns the circuit
   lifecycle).
3. **Harness hygiene** — fixed test ports are now pre-checked as free
   before boot; a leftover listener previously absorbed a drill
   silently.

## 5. Risk register re-score (dossier Ch.10, mandatory at Stage 8)

| ID | Risk (dossier) | L×I at plan | L×I at v1.0.0 | Basis for the re-score |
|---|---|---|---|---|
| R1 | Unverified base hides routing defects | M×H | **L×H** | Stage 0 audit + 46 golden fixtures + 483-test suite + 8 stage-boundary regression diffs |
| R2 | Free-tier ceilings change | H×M | **M×M** | Catalog + refresh job + watchdog live (S3/S5); terms not re-validated since the audit — operator refresh procedure in RUNBOOK §4 |
| R3 | Hermes plugin contract drift | M×M | **L×M** | Plugin adapts 5 observed registry shapes, fail-soft; pinned contract documented (S7) |
| R4 | AGPL contamination of billing | M×H | **L×H** | Clean-room protocol followed; license review gate passed at Stage 4; no AGPL code in tree |
| R5 | Provider key theft | L×H | **L×H** | Stage 1 posture intact; bandit + pip-audit green at release; keystore at-rest encryption |
| R6 | Token estimation drift | M×M | **L×M** | Estimate-to-actual reconciliation (S5) + Stage 8 closes the disconnect-completeness gap; drift counter observable on /metrics |
| R7 | Upstream single-maintainer stall | H×M | **L×M** | Fork owned by this project; base pinned at 0a9560e; all consumed components vendored |
| R8 | Streaming (SSE) regressions | M×M | **L×M** | Golden streaming fixtures pinned through 8 re-captures; Stage 8 adds concurrent-stream integrity checks |
| R9 | Ledger corruption / data loss | L×H | **L×H** | Tamper-evidence tests (S4) + the Stage 8 backup/restore drill executed end-to-end (RUNBOOK §6/§7/§8 validated) |
| R10 | Scope creep delays billing core | M×M | **L×L** | Stages 0–8 delivered at gates in order; INVESTIGATE items remain parked at the approval gate |

**Register verdict**: no risk regressed; four reduced in likelihood;
none requires action before the tag. R2 remains the standing
operational watch-item (quarterly catalog/terms review, RUNBOOK §4).

## 6. Known limitations & open owner decision

1. **24-hour soak** is an operator procedure (`tools/soak.py`,
   RUNBOOK §8.6). At publication a **real 60-minute release soak** was
   executed against a production-shaped deployment of the frozen
   runtime — **PASS**: 211,287 requests at concurrency 24 (70%
   streaming), 100% HTTP 200, zero incomplete bodies, 212,896-record
   HMAC ledger verified intact, bounded RSS/fds; evidence in
   `docs/soak/`. The full 24-hour run is launched per §8.6 as the
   standing release/quarterly procedure (the in-suite compressed
   window remains the deterministic proof of the failure lattice).
2. **Docker compose deploy** is validated by a 14-test structural
   contract; the deploy-time step is documented in the runbook (docker
   is unavailable in the CI sandbox).
3. **Live local Ollama lane** (4 tests) auto-skips without a local
   runtime; the manual procedure is HERMES_INTEGRATION §8/§10.
4. **Root LICENSE file**: **RESOLVED at publication** — the root
   `LICENSE` (MIT, dual attribution: upstream James Huang @ `0a9560e`;
   this fork) now makes explicit the grant every file header has claimed
   since Stage 0. MIT is the only license consistent with the upstream
   header claims and the R4 anti-contamination posture; no relicense
   occurred. This closes the Stage-0 owner decision and clears the
   redistribution precondition.
5. **`/admin/ledger/records` `total`** is the unfiltered count (Stage 4
   shape, fixture-pinned); filtered pagination is available via the
   returned `records` page. Noted for a future minor release.
6. **Resource assertions** (fd/RSS) read `/proc` and skip gracefully
   where absent (non-Linux).

## 7. Operator entry points

* Deploy + first boot: RUNBOOK §2–§3 · routine ops: §4 · backup: §6
* Incident: ledger tamper §7 · drills rehearsal: §8 (automated tests +
  manual procedures) · 24-hour soak: §8.6
* Hermes onboarding: `docs/HERMES_INTEGRATION.md` + the
  `plugins/hermes-router-provider/` package
* Evidence: this manifest, `docs/STAGE{0..8}_*.md`, and the gate
  outputs recorded in each stage memo

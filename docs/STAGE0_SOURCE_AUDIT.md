# Stage 0 — Source Audit Memo (Baseline and Truth-Setting)

**Project:** Unified Hermes Gateway (free-tier any-model routing + operator-side billing plane)
**Base:** `kjames2001/hermes-router-proxy` (D1, approved 2026-09-22)
**Upstream pin:** `0a9560e23d42d0faf6f89afd0fa947ad5b3aad88` (2026-09-16, "feat: add 4-tier classification pipeline")
**Working branch:** `stage-0-baseline` (clean clone; upstream untouched, all scaffold is additive)
**Mandate:** Dossier Chapter 9, Stage 0 — first-hand source audit, commit pin, CI/lint/type/test scaffold, verified gap list, golden fixtures. No behavioral modifications to upstream code.

---

## 1. Provenance and Integrity

The base repository was cloned at depth 50 from `https://github.com/kjames2001/hermes-router-proxy.git` into `gateway/base`. The cloned HEAD (`0a9560e`) matches the audited commit exactly — its committer timestamp (2026-09-16 20:25:47 +0200 = 18:25:47 UTC) is byte-identical to the `pushed_at` recorded in the audit snapshot, so the forensic scorecard scored precisely this code state. The freshness re-validation gate (2026-09-22) confirmed the repo live, un-archived, un-renamed, with no disqualifying change. All Stage 0 work lives on the `stage-0-baseline` branch; `origin` still points upstream for re-sync, and no upstream file is modified — the full scaffold is additive (verified by `git diff main --stat` showing only new files).

## 2. First-Hand Architecture Map (21 files, ~5,300 LOC)

The system is a deliberately compact FastAPI service with four real layers:

| Layer | Files | Verified facts |
|---|---|---|
| **Core proxy** | `server.py` (2,321 LOC) | Single-file app: `/v1/chat/completions` (sync + SSE), `/health`, `/metrics` (JSON + Prometheus text), `/reload` (hot config swap with validation), `/circuits`, `/admin/sessions`, `/classifier/report`, `/admin/config`. Session key = SHA-256 of first user message (16 hex chars); 500-session LRU-evicted cache; per-`base_url+model` circuit breakers (open → synthetic 503). |
| **Audit trail** | `trace.py` (241 LOC) | Thread-locked JSONL appender with size rotation (10 MB × 5), date-stamped files, 7 event types (`classify`, `cache_hit`, `deviation`, `route`, `circuit`, `key_rotation`, `stream_error`), user-message previews truncated to 120 chars. This is the Stage 4 billing-ledger seed. |
| **Classification pipeline** | `fasttext_classifier.py`, `setfit_classifier.py`, `zero_shot_classifier.py`, `surrogate_models.py` (+4 train/fit scripts) | 5-tier cheapest-first cascade (fastText → SetFit → zero-shot → sklearn surrogate → LLM call), each optional and failure-tolerant ("falls back to first category on any failure"). All tiers bypassable via `/use:<category>` override and keyword override lists. |
| **Deployment** | `Dockerfile`, `docker-compose.yaml`, `install.sh`, `router-config/systemd/` | Python 3.13-slim multi-stage build, healthchecks, compose mounts `/root/.hermes/.env` and a user `router_config.yaml` — confirms the Hermes-agent integration surface (`~/.hermes`) documented in the dossier. |

Routing behavior verified first-hand at the code level: 3-tier fallback cascade (primary → fallback1 → fallback2) with per-attempt trace events; 429-triggered key rotation to `alternate_key_env`; 503/transport retries with backoff on the sync path; transparent SSE proxying on the stream path (clean cut on inner-stream failure, delegated retry, per-session 3-strike fallback skip). The `/use:` override is detected only at message start, stripped for string content, and left intact for multimodal (list) content — captured in fixtures.

## 3. Dossier Claims — First-Hand Verification Record

| Dossier claim (Ch. 5.2 / 9) | Verdict | Evidence |
|---|---|---|
| "No test suite visible" | **REFINED** | `test_trace.py` exists: 30+ pytest tests, all 27 pass — but it covers `trace.py` only. The 2,321-line core (`server.py`) has **zero** tests. Gap stands in substance: the routing/metering core is untested. |
| "OpenAI-shape-only ingress" | **CONFIRMED** | Single ingress route; no Anthropic-native or other protocol adapters. |
| "No per-user attribution or budgets" | **CONFIRMED** | No caller identity concept anywhere; `session_key` is content-derived, not credential-derived; no budget logic. Stage 4 scope confirmed necessary. |
| "Permissive CORS" | **CONFIRMED** | `CORS_ORIGINS` defaults to `*` **with** `allow_credentials=True`. |
| "Optional endpoint authentication" | **CONFIRMED, sharper** | Auth is env-var-keyed and correct where configured (`hmac.compare_digest`, empty-env fail-closed per "Bug #22 fix") — but the shipped `router_config.yaml` ships `api_key_env: ''`, i.e. **auth disabled by default**. Admin endpoints (`/reload`, `/admin/config`, `/admin/sessions`) inherit this. |
| "Docker Compose + systemd production shape" | **CONFIRMED** | Verified compose file, Dockerfile healthcheck, systemd unit. |
| MIT license | **CORRECTED** | **No root `LICENSE` file exists** and README carries no license statement; MIT is claimed only inside `trace.py`/`server.py` file headers ("License: MIT"). Legal posture is MIT-by-header-claim, not MIT-by-license-file. Action deferred to owner decision (§6). |

## 4. Security Posture Summary (Stage 1 Input)

The fail-closed instinct is present (auth rejects when configured-but-empty; classifiers fail open only to the *cheapest local* path, never to an unconfigured upstream), but defaults are developer-laptop defaults: no auth, CORS `*` + credentials, metrics/admin surfaces unauthenticated, and upstream keys read from env without any at-rest store. One hygiene defect: `router_config.yaml` is **tracked in git despite being listed in `.gitignore`** (it predates the ignore rule); it contains no literal secrets (only `api_key_env` *references*), but it does expose the author's real endpoint topology and will keep absorbing local edits into commits. Stage 1 scope: `git rm --cached` it, default auth on, bind localhost, lock CORS, redact secrets in logs, pip-audit/bandit gates.

## 5. Golden Fixture Suite (Committed Baseline Contract)

`tools/capture_golden.py` runs the **unmodified** upstream server against a deterministic stub upstream (fixed ids, fixed bodies, scripted failure) and records 10 transcripts to `tests/fixtures/golden/`: override routing (simple/complex), SSE passthrough ([DONE]-terminated), the full fallback cascade (primary 500 → fallback 200 → `hermes_router.fallback_used: true` with paired trace events), session tier caching (`cache_hit` without reclassify), the 400 no-user-message error contract, multimodal list-content passthrough, and structure-only `/health` + `/metrics` snapshots. Every scenario selects its tier via `/use:` overrides, so capture is fully deterministic with zero model dependencies; volatile fields (timestamps, latencies, counters) are normalized and flagged. `tests/test_golden_fixtures.py` (10 tests) validates the committed transcripts offline — this is the regression diff oracle that every later stage boundary re-runs capture against.

## 6. Gate Results and Debt Ratchet Inventory

All three gates are **green on untouched upstream code** (config-calibrated; the alternative — auto-fixing upstream style — was rejected because Stage 0 forbids behavioral diffs):

| Gate | Command | Result |
|---|---|---|
| Lint | `ruff check .` | 0 errors (rule set: E4/E7/E9/F + per-file ratchet ignores) |
| Types | `mypy server.py trace.py` | 0 errors (2 documented per-module overrides) |
| Tests | `pytest` | 37 passed (27 upstream trace + 10 fixture contracts) |

Extended rule set debt (not yet enabled, 42 findings: BLE001×16, I001×7, EXE001×4, RUF100×3, UP017×2, PLW1508×2, S110×2, SIM/TRY/B005/C401/RUF059×5) is catalogued for the ratchet. Specific ratchet items: **R-L1** unused numpy imports (`fasttext_classifier.py`, `setfit_classifier.py`); **R-L2** 8 lambda assignments (`server.py`); **R-L3** 6 ambiguous variable names (train scripts); **R-L4** 2 E741 + 2 E402 (`test_trace.py` — frozen, upstream-owned); **R-M1** 3 typing quirks (`server.py:112,717,1316`); **R-M2** Optional-model typing gap (`surrogate_models.py:61`). Each stage that touches a file must clear its items and tighten the corresponding ignore.

**Owner decision recorded (not executed):** add a root `LICENSE` file. The file headers claim MIT and the dossier scored the repo MIT; formalizing it (MIT, © James Huang) protects the fork's redistribution rights and is one commit — but license text is a legal act, so it is deferred to the owner's explicit instruction rather than added during Stage 0.

## 7. Exit-Gate Checklist (Chapter 9, Stage 0)

- [x] Full source audit memo (this document)
- [x] Upstream commit pinned (`0a9560e`), provenance recorded, branch isolated
- [x] CI scaffold committed (`.github/workflows/ci.yml` runs the identical local gate chain)
- [x] ruff/mypy/pytest green on untouched code (37/37/0/0)
- [x] Verified gap list recorded (§3, §4) with stage mapping intact
- [x] Golden request/response fixture suite committed (10 fixtures + index with provenance)
- [x] No upstream file modified (additive-only diff)

**Stage 0 verdict: PASS.** Next: Stage 1 — Security hardening (localhost default bind, mandatory bearer auth, encrypted key store, secrets redaction, CORS lockdown, pip-audit + bandit gates), after which the fixture suite is re-captured to diff the posture change.

# Stage 4 — Immutable Cost Ledger (Exit-Gate Memo)

**Status:** PASS · **Date:** 2026-10-11 · **Branch:** `stage-0-baseline` (staged commits)
**Scope authority:** approved D1+D2 (v1 = Stages 0–5), dossier Ch.9 stage table (Table 7, billing plane):

> *Stage 4 — Billing plane: SQLite WAL ledger, virtual keys, subscriber
> attribution, blocking budgets, price-catalog snapshots (clean-room per
> R4). Exit gate: ledger immutability tests, budget-block tests,
> attribution end-to-end with two virtual keys.*

Conflict rule **R4** honored: the polyrouter billing chapter was used as a
requirements source at spec level only — zero code, schema dumps, or
templates taken from any AGPL-licensed repository. Everything here is
first-party.

---

## 1. What Was Built

### 1.1 `ledger.py` (new, ~700 LOC, zero new dependencies)

**Hash-chained append-only cost ledger** on SQLite (WAL mode):

```
row_hash = HMAC-SHA256(secret, canonical_json(record))   # secret optional
        or  SHA-256(canonical_json(record))              # no secret set
```

Every record embeds `seq` + `prev_hash`, so `Ledger.verify()` (and the
offline `python -m ledger verify` CLI) detects all three tamper classes:
field editing (row hash mismatch), middle-row deletion (prev_hash linkage
break), and tail truncation (chain-head mismatch against `meta`). The
optional HMAC secret — env var name or `ks:<alias>`, resolved through the
Stage 1 keystore — upgrades corruption-evidence into tamper-proofing
against anyone without the secret. Appends are serialized; nothing ever
UPDATEs or DELETEs a ledger row.

**Integer-only money math** (no float anywhere in the cost path): prices
are stored as integer nano-USD per million tokens (`price_*_nano`),
costs accumulate as integer micro-USD (`cost_*_micros`). 5 tokens at
$1.25/Mtok = exactly 6 micros (truncated), forever reproducible.

**Price catalog with content-hashed snapshot ids**: `build_price_catalog()`
validates every entry fail-closed (negative prices, missing fields, and
non-numeric garbage refuse startup), tags each entry with its source
(`explicit` config / `default` fallback / `free`), and derives a stable
`snapshot_id` from the catalog content — every ledger record pins the exact
snapshot it was priced against, so later price edits never rewrite history.

**Virtual key store**: `vk-<32 hex>` tokens, plaintext shown exactly once
at creation, only the SHA-256 stored at rest. Budgets: `none | tokens |
cost_micros` × `day | month | total` windows. `evaluate_budget()` computes
in-window spend from the ledger itself — the same immutable rows, not a
second mutable counter that could drift from the truth.

### 1.2 Dispatch integration (server.py, surgical)

- **Principal-aware auth**: `verify_auth(request, require_admin=False)`.
  Admin key → full access; virtual key → inference only. Every `/admin/*`,
  `/metrics`, and `/reload` call site passes `require_admin=True`; virtual
  keys get `403 admin_only` (13 call sites migrated, plus the `/reload`
  path). `/health` stays intentionally open.
- **402 budget gate, pre-routing**: `chat_completions` resolves the
  principal, and a virtual key at/over budget is refused with
  `402 budget_exceeded` *before* any upstream contact (fixture 19c pins
  `trace_events == []` — the stub is provably never called).
- **Usage capture at all five completion sites** — three non-streaming
  response bodies plus both stream pumps. Streams run through
  `_UsageSniffer`, which wraps the Stage 2 `StreamTranslator` and sniffs
  only the *input* SSE lines (usage frames ride `data: {...}` on the input
  side for both protocols), leaving the translated output byte-exact —
  the Stage 2 fixtures stay identical.
- **7 new admin endpoints**: `POST/GET /admin/virtual_keys`,
  `PATCH/DELETE /admin/virtual_keys/{id}`, `GET /admin/ledger/verify`,
  `GET /admin/ledger/records` (per-subscriber filter), and
  `GET /admin/ledger/summary` (`group_by=model|key|session|day`).
- **Billing metrics**: `ledger_appends_total`,
  `ledger_append_failures_total`, `ledger_budget_blocks_total`.
- **Fail-closed `_stage4_bootstrap`** wired into startup, hot-reload, and
  `main()` — bad pricing config or an unresolvable HMAC ref refuses to
  start (all-or-nothing on reload: previous plane stays live on error).
- **Ledger failures never break proxying**: a failed append is counted and
  logged; the request still completes.

### 1.3 Attribution contract

Ledger records carry `provider` (Stage 2), `key_id`/`from_key_id` (Stage 3
pool seeds), and now `virtual_key_id` + `tier` + `fallback_level` — with
**identical numbering on both dispatch paths** (primary=0, fallback1=1,
fallback2=2). Admin-key traffic is recorded with `virtual_key_id: null`;
subscriber traffic is attributed end-to-end.

---

## 2. Golden Fixture Diff (regression oracle) — CLEAN

Re-captured after the stage: **33 fixtures** (was 22 files / 21 scenarios).
Diff vs Stage 3:

- All 14 behavior fixtures + admin snapshots 90/92: **byte-identical**.
- `golden_03_streaming` / `golden_17_pool_stream_rotation`: intended delta
  only — the stub upstream now emits a final pre-`[DONE]` usage chunk
  (OpenAI `include_usage` convention) so the sniffer has deterministic
  material to capture; translated client output unchanged.
- `golden_91_metrics`: intended delta only — the three billing counters.
- New Stage 4 contracts (pinned in `tests/test_golden_fixtures.py`):
  - **18a/18b** — virtual key creation (201, token redacted in fixture,
    plaintext shown once by contract).
  - **18e/18f** — *attribution end-to-end with two virtual keys* (the
    dossier exit gate): VK_A free-tier non-stream record
    (`cost_total_micros: 0`, `price_source: free`) and VK_B priced SSE
    record (`tier: complex`, `stream: 1`, exact integer costs 6+30=36
    micros, pinned price nanos, chain hashes scrubbed to `[hash]`);
    admin traffic stays unattributed (`total > count`).
  - **19a–19d** — blocking budgets: create with `cost_micros`/36/`total`,
    last-allowed request 200, next request `402 budget_exceeded`
    (`used: 36 == limit: 36`, no trace events — pre-routing proof),
    listing shows in-window budget state with no secret material.
  - **20** — virtual key on `/admin/ledger/records` → `403 admin_only`
    with the vk bearer redacted in the captured request headers.
  - **Security sweep** — no `vk-` plaintext token anywhere in the suite.

---

## 3. Verification (all gates green)

| Gate | Result |
|------|--------|
| ruff 0.16.8 (`E4,E7,E9,F`) | 0 findings, no new ignores |
| mypy 2.3.1 (7 files incl. ledger) | 0 errors |
| bandit (runtime surface, 7 files) | no issues (nosec notes on the B104 bind-guards and B608 SQL-identifier guards only — identifiers come from closed constant sets, never user input) |
| pip-audit (32-dist closure) | no known vulnerabilities |
| pytest | **259 passed** (was 196): +36 ledger unit, +10 billing e2e, +13 fixture/schema |

Test highlights (dossier exit criteria mapped):

- *Ledger immutability tests* — all three tamper classes detected
  (field edit / middle-row delete / tail truncation) plus meta-head
  forgery; HMAC mode; **on-disk immutability** (direct `UPDATE` via sqlite3
  is caught by the live server's `/admin/ledger/verify`); **restart
  durability** (chain survives a server restart); CLI verify parity.
- *Budget-block tests* — cost-mode and token-mode windows; zero-limit keys
  block immediately; budget raise via `PATCH` unblocks; the admin key is
  unaffected by a subscriber's budget; blocked requests leave no ledger
  record (they never reached an upstream).
- *Attribution end-to-end with two virtual keys* — live `server.py`
  subprocess cluster: VK_A (non-stream, free tier) and VK_B (stream,
  priced) both attributed in `/admin/ledger/records?virtual_key_id=`;
  integer costs pinned exactly; duplicate names rejected; disable + delete
  lifecycle; metrics expose the billing counters.
- *Fail-closed bootstrap* — invalid pricing config refuses to boot.

## 4. Incidents Fixed During the Stage

1. **Session interruption + reconstruction**: the original Stage 4 run was
   interrupted at ~90% (environment collapse after the full-suite green
   run, before fixture re-capture). The workspace was rebuilt in a fresh
   environment by replaying the recorded tool-call history — the
   reconstructed tree matched the recorded gate state exactly (same test
   count, same findings) before the remaining work was completed. No code
   was re-derived by hand.
2. **Stream ledger attribution was inconsistent** (caught by the fixture
   diff review, the oracle doing its job): primary stream records carried
   `tier: null` (the stream path never injected the category into the
   attempt config) and `fallback_level` was numbered `attempt_idx + 1`
   (primary=1) — disagreeing with the non-stream path's primary=0 and
   mislabeling fallbacks whenever a 3-strike skip started the attempt list
   at fallback1. Fixed: the stream path now injects `model_cfg["tier"]`,
   and the four ledger append sites use the attempt *label* (primary=0,
   fallback1=1, fallback2=2). The five `trace_route` sites were
   deliberately left at their Stage 3 semantics — trace numbering is a
   Stage 3 committed contract, no fixture pins a stream fallback_level,
   and changing it is out of the Stage 4 mandate.
3. **Pending gate findings on the new test files** (written after the last
   recorded ruff run): one E402 (late `importlib` import) and one F401
   (unused `DEFAULT_DB` import) — both fixed.
4. **pytest security floor**: pip-audit flagged PYSEC-2026-1845 in pytest
   9.0.2; the closure was upgraded (9.1.1) and the `dev` floor raised to
   `pytest>=9.0.3`, keeping the "zero known vulnerabilities in the
   dependency closure" posture blocking in CI.

## 5. Design Decisions of Record

- **Append-only, never UPDATE**: the ledger is evidence, not a ledger-plus-
  corrections pair. Corrections would be new rows in a future stage;
  history is immutable by construction.
- **HMAC optional, chain always**: without a secret the chain still proves
  *corruption* (bit rot, partial writes); with a secret it proves
  *authorship*. Operators choose via `hmac_key_env`.
- **Prices snapshotted per record**: `price_snapshot_id` + full nano prices
  in every row — reproducible billing archaeology even after catalog
  changes; `price_source` distinguishes explicit/default/free provenance.
- **Budgets derive from the ledger, not a shadow counter**: `spent_for_key`
  sums the immutable records in-window. There is no second number that can
  disagree with the billed truth.
- **Gate ordering**: auth → budget → route. A budget-exhausted subscriber
  costs the operator nothing (no upstream call, no ledger row, no trace
  event beyond the block counter).
- **Sniffer on the input side only**: usage extraction never touches the
  translated client stream — Stage 2's byte-exact passthrough guarantee is
  preserved by construction (fixtures re-prove it).
- **Virtual keys are inference-only**: the admin key remains the single
  operations credential; subscriber tokens can never read config, pools,
  ledger, or metrics (403 by role, not by absence of a feature).

## 6. Exit-Gate Checklist

- [x] Ledger immutability tests (three tamper classes + HMAC + on-disk
      mutation + restart durability + CLI parity)
- [x] Budget-block tests (cost + token modes, windows, raise-to-unblock,
      zero-limit, admin unaffected, no upstream contact)
- [x] Attribution end-to-end with two virtual keys (live-process e2e +
      pinned fixtures 18a–18f)
- [x] Golden fixture re-capture diffed: prior behavior byte-identical
      except the two intended deltas (usage chunk, billing metrics)
- [x] Clean-room per R4 (spec-level requirements only, first-party code)
- [x] Fail-closed bootstrap (startup / hot-reload / main), all-or-nothing
      reload
- [x] No plaintext secrets in fixtures, admin surfaces, or logs (token
      redaction pinned by contract test)
- [x] All quality gates green; no new lint debt; CI carries the 7-file
      mypy/bandit surface and ledger.py in the pytest path

**Verdict: Stage 4 PASS.** Next stage per approved D2: **Stage 5** — the
dossier's remaining v1 scope (observability hardening + the deferred D3
INVESTIGATE items at the gate).

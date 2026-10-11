# Stage 3 — Key Pooling & Free-Tier Catalog (Exit-Gate Memo)

**Status:** PASS · **Date:** 2026-09-24 · **Branch:** `stage-0-baseline` (staged commits)
**Scope authority:** approved D1+D2 (v1 = Stages 0–5), dossier Ch.9 stage table:

> *Stage 3 — Pooling and free-tier catalog: merge keypool pooling (429
> header-aware cooldown, capability tags); ingest free-tier catalog with
> limits and refresh job. Exit gate: rotation chaos tests pass; catalog
> covers the seven free tiers; cooldown behavior matches provider headers.*

Source contributions adapted (both MIT, both scored in the best-implementation
matrix): the **pooling core** from the runner-up `piyush-tyagi-13/llm-keypool`
(keypool-style rotation "almost verbatim" per the dossier) and the **catalog
concept** from `DevvGwardo/free-llm-router` (auto-detected models/limits for
seven free tiers). Clean-room adaptation onto the Stage 2 provider interface —
no code copied from AGPL sources.

---

## 1. What Was Built

### 1.1 `keypool.py` (new, ~500 LOC, zero new dependencies)

Key-level state lives here; endpoint-level state stays in the circuit
breaker. The two layers are orthogonal and compose:

| Concern | Owner | Trigger | Effect |
|---------|-------|---------|--------|
| Key exhaustion | **KeyPool** | HTTP 429 on one key | that key cools down; next key serves |
| Key death | **KeyPool** | HTTP 401/403 | key quarantined until reload/unlock |
| Endpoint overload | circuit breaker | 429/5xx per base_url+model | endpoint skipped for recovery_timeout |

Core surface:

- `KeyPool.acquire(require)` — lazy round-robin over *available* keys
  (not cooling, not quarantined, capabilities ⊇ required). Returns `None`
  only when exhausted; raises `PoolError` (fail-closed) when a secret
  cannot be resolved.
- `KeyPool.acquire_all(require)` — ordered snapshot of available leases
  (streaming path builds its attempt list up front).
- `KeyPool.report_result(key_id, status, headers)` — feeds outcomes back:
  429 → cooldown from the provider's own headers; 401/403 → quarantine
  (`disable_on_auth_failure: false` switches to a policy cooldown);
  2xx → success counters + consecutive-429 reset; 5xx → `last_status`
  only (circuit breaker's domain).
- `parse_retry_after(headers, default, max_seconds)` — cooldown behavior
  matches provider headers, in priority order: `Retry-After` (delta-seconds
  or RFC-7231 HTTP-date; a past date means *retry now* per RFC),
  `x-ratelimit-reset-requests` / `x-ratelimit-reset-tokens` (OpenAI-style
  duration tokens `500ms`, `6m0s`, `1d0h0m30s`), then the policy default.
  Everything clamps to `[0, max_cooldown_seconds]` — provider garbage can
  never disable a key for good.
- `unlock()` + `POST /admin/pools/{name}/unlock` — operator recovery.
- Secrets: resolved at acquire time via the Stage 1 resolver (env or
  `ks:<alias>`), never logged, never in any admin/metrics surface.

Config (`key_pools:` top-level section, validated fail-closed):

- pool policy: `strategy` (round_robin only in v1), `default_cooldown_seconds`
  (60), `max_cooldown_seconds` (3600), `disable_on_auth_failure` (true),
  `auth_failure_cooldown_seconds` (900)
- key entries: `ref` (env/keystore), `label`, `capabilities` — duplicate
  labels rejected
- category/fallback references: `key_pool` (primary), `fallback_key_pool`,
  `fallback2_key_pool`, each mutually exclusive with the single-key
  `*key_env` field; `require_capabilities` only meaningful with a pool
- unknown pool references, malformed shapes and negative numbers refuse to
  start (and refuse hot reload) — same posture as Stage 1/2 gates

### 1.2 Dispatch integration (server.py)

- `call_model` — pooled branch: request translation happens once
  (key-independent), only auth headers rotate; **one attempt per key
  maximum** (every 429 cools its key, so the pool makes forward progress
  by construction); pool exhaustion returns a synthetic 429
  (`error.type: pool_exhausted`) that engages the existing route-level
  fallback cascade; the 503-retry policy is factored into
  `_post_with_503_retry` and shared by both branches (behavior-identical,
  fixture-pinned). The single-key branch keeps the pre-Stage-3 behavior
  byte-for-byte.
- `route_request_stream` — pooled tiers acquire their candidate list up
  front; 429 cools the used key and rotates within the tier; 401/403
  quarantine and rotate; pre-flight exhaustion skips to the next tier.
  Fallbacks carry their own `key_pool` / `require_capabilities`.
- Attribution: successful pooled responses carry the serving key id
  internally (`_hermes_key_id`); `trace_route` events gain an additive
  `key_id` field and `key_rotation` events an additive `from_key_id` —
  **stable identifiers reserved as the Stage 4 ledger's per-key column**.
- Lifecycle: `_stage3_bootstrap()` builds catalog + pools with an explicit
  validate-then-commit split — a rejected hot reload leaves the previous
  pools/catalog live (all-or-nothing). Wired into startup, `POST /reload`,
  and the `__main__` pre-flight.
- Pooled primary categories may omit `api_key_env` entirely; every
  `api_key_env` access hardened to `.get()` semantics (one real boot bug
  found and fixed during fixture capture — see §4).

### 1.3 `catalog.py` + `catalog/free_tiers.yaml` (new)

Data-driven provider facts, deliberately dumb (no billing decisions here):

- **Bundled snapshot covers the seven audited free tiers**: Groq, Cerebras,
  Mistral, Google Gemini (OpenAI-compatible branch), OpenRouter, SambaNova,
  Nous Research portal — each with base_url, adapter name (validated
  against the live provider registry), capability tags, documented limits
  (`limits.confidence`: vendor-docs | community | unverified — the Nous
  portal entry is explicitly `unverified`), signup URL and operator notes.
  Every number is a point-in-time snapshot with a `source`; Stage 5
  limiters must treat catalog values as overridable defaults.
- `catalog_ref: <name>` on a category fills only ABSENT fields
  (base_url, provider, first model); explicit config always wins; unknown
  refs fail closed.
- `CatalogStore` refresh lifecycle: file re-read or remote snapshot pull.
  Remote pulls require explicit `catalog.allow_remote: true` (refused
  otherwise), are size-capped (1 MiB), follow no redirects, and swap
  atomically — a failing refresh keeps the last-good document live and is
  counted (`catalog_refresh_failures_total`).
- Background refresh job (`catalog.refresh_minutes` > 0) scheduled by the
  bootstrap when the event loop exists; defensive catch keeps the job
  alive; `POST /admin/catalog/refresh` for operator-triggered refresh.

### 1.4 New operational surfaces (all auth-gated except /health)

| Surface | Purpose |
|---------|---------|
| `GET /admin/pools` | per-pool/per-key state: availability, cooldown remaining, counters, quarantine — metadata only, never secrets |
| `POST /admin/pools/{name}/unlock` | clear quarantine + cooldowns after credential repair |
| `GET /admin/catalog` | catalog summary (providers, limits, confidence) |
| `POST /admin/catalog/refresh` | force refresh; last-good retained on failure |
| `/metrics` additions | `pool_requests_total`, `pool_429_total`, `pool_rotations_total`, `pool_auth_failures_total`, `pool_exhausted_total` counters + `hermes_router_pool_keys_available{pool=...}` gauges |

---

## 2. Golden Fixture Diff (regression oracle) — CLEAN

Re-captured after the stage: **21 fixtures** (was 17). Diff vs Stage 2:

- All 14 behavior fixtures + `/health` snapshot: **byte-identical** — the
  `_post_with_503_retry` extraction and pool branch added zero drift to
  single-key openai/anthropic paths.
- `golden_14_admin_providers`: intended delta only — the three new fixture
  categories (`pooled`, `pooled_exhaust`, `pooled_stream`) appear in
  per-category provider resolution.
- `golden_91_metrics`: intended delta only — pool counters/gauges and the
  per-category series for the new fixture categories.
- New Stage 3 contracts (pinned in `tests/test_golden_fixtures.py`):
  - **15_pool_rotation** — key-a 429 (stub `retry-after: 1`) → key-b
    serves; trace `[key_rotation(from_key_id=rotate_pool/key-a), route(key_id=rotate_pool/key-b)]`.
  - **16_pool_exhausted** — both keys 429 → synthetic 429 → cascade →
    stub-fallback 200 with `fallback_used=true`; trace `[kr(c), kr(d),
    route(429), route(200)]`.
  - **17_pool_stream_rotation** — streaming: key-e 429 trips the circuit
    (3rd consecutive failure for stub-simple after scenario 16, since 15's
    success reset the counter — upstream semantics, deterministic),
    key-f streams and re-closes the circuit in-request; trace
    `[circuit closed→open, kr(e), circuit open→closed, route(200, key-f)]`.
  - **92_admin_pools** — per-key counters after 15–17 (deterministic);
    cooldown/availability fields excluded as volatile; asserts no secret
    material in the admin surface.
- Capture harness: stub upstream discriminates pooled keys by bearer value
  (deterministic 429 subset with `retry-after: 1`); three disjoint pools
  keep every scenario's RR pointer fresh; trace `record()` now includes
  session-less events (`key_rotation`, `circuit`) inside the offset slice.

---

## 3. Verification (all gates green)

| Gate | Result |
|------|--------|
| ruff 0.16.8 (`E4,E7,E9,F`) | 0 findings, no new ignores |
| mypy 2.3.1 (6 files incl. keypool, catalog) | 0 errors (2 targeted `attr-defined` ignores for the pooled-key attribution attribute) |
| bandit (runtime surface, 6 files) | no issues |
| pip-audit (29-dist closure) | no known vulnerabilities |
| pytest | **196 passed** (was 108): +41 keypool, +21 catalog, +26 fixture/schema |

Test highlights (dossier exit criteria mapped):

- *Rotation chaos tests pass* — scripted 4-key chaos (mixed outcomes,
  cooldown expiry windows), exhaustion→None→unlock recovery, and a
  60-task `asyncio.gather` concurrency test asserting no oversell
  (`total_acquired == total_429 + total_success` per key) and no crashes.
- *Catalog covers the seven free tiers* — schema validation suite
  (version, required fields, adapter registry cross-check, confidence
  rules) plus the seven-name membership contract.
- *Cooldown behavior matches provider headers* — 12 `parse_retry_after`
  cases: numeric/float seconds, HTTP-date future/past (past → 0 per RFC),
  duration tokens incl. compound and milliseconds, garbage → default,
  clamping, priority and case-insensitivity.
- Live end-to-end (real `server.py` subprocess + deterministic stub):
  rotation serves via key-b, `/admin/pools` counters correct and
  secret-free, `/metrics` pool lines present, unlock endpoint resets
  exactly the cooling keys, admin surfaces 401 without bearer.

## 4. Incidents Fixed During the Stage

1. **Pooled primary categories crashed streaming** (`KeyError:
   'api_key_env'`): pooled primaries legitimately omit `api_key_env`, but
   the streaming path indexed it unconditionally before the pool branch.
   Fixed with `.get()` at all four access sites (streaming attempt cfg,
   `call_model` single-key branch, both fallback cfg builders in
   `route_request`); caught by fixture capture before any release.
2. **Validator early-return bug**: `_check_ref` returned before checking
   `fallback_key_pool` references when the category itself had no primary
   pool — fallback pool refs are now checked unconditionally.
3. **RFC 7231 semantics**: a past `Retry-After` HTTP-date originally fell
   through to the policy default (60s penalty); corrected to *retry now*
   (0) — the unit test caught the design flaw.
4. Toolchain re-install after workspace reset (ruff 0.16.8 / mypy 2.3.1 —
   identical versions to the Stage 2 close; ratchet not loosened).

## 5. Design Decisions of Record

- **Pool vs circuit orthogonality**: key-level cooldown must not
  double-punish an endpoint already tripped by the circuit breaker — both
  layers run, and 5xx outcomes deliberately leave pool cooldowns untouched.
- **Lazy acquire (non-stream) vs snapshot (stream)**: streaming needs the
  full attempt list before entering the pump loop; the difference is
  documented and covered by fixtures on both paths.
- **One attempt per key per request**: guarantees forward progress without
  unbounded retries; cross-request recovery comes from cooldown expiry, not
  in-request spinning.
- **Quarantine on 401/403**: a dead credential never earns another request
  (default); operators recover via reload or the unlock endpoint.
- **Catalog is data, not authority**: `confidence` markers + `source` +
  refresh job; the unverified Nous portal entry is honest about endpoint
  uncertainty rather than inventing quota numbers.
- **Remote pulls opt-in**: `allow_remote: false` default, 1 MiB cap, no
  redirects — the catalog cannot become an SSRF or supply-chain vector
  without an explicit operator decision.

## 6. Exit-Gate Checklist

- [x] Rotation chaos tests pass (scripted + concurrency + live e2e)
- [x] Catalog covers the seven free tiers (contract test)
- [x] Cooldown behavior matches provider headers (12-case suite)
- [x] Golden fixture re-capture diffed: openai/anthropic behavior
      byte-identical; only intended deltas
- [x] Capability tags route (unit + fixture `require_capabilities` path)
- [x] Fail-closed at boot, hot reload, and dispatch (unknown pools/refs)
- [x] Secrets never surface in admin/metrics/traces (fixture-pinned)
- [x] Stable key ids plumbed to trace events (Stage 4 ledger seed)
- [x] All quality gates green; no new lint debt; CI updated to 6-file
      mypy/bandit surface

**Verdict: Stage 3 PASS.** Next stage per approved D2: **Stage 4 — the
immutable cost ledger** (hash-chained per-request cost records keyed by the
provider/key/session identifiers this stage stabilized; the strategic-moat
stage flagged "don't rush it" at the approval gate).

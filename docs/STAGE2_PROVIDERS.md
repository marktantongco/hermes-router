# Stage 2 — Provider Abstraction (Exit-Gate Memo)

**Status:** PASS · **Date:** 2026-09-23 · **Branch:** `stage-0-baseline` (staged commits)
**Scope authority:** approved D1+D2 (v1 = Stages 0–5), dossier Ch.9 stage table:

> *Stage 2 — Provider abstraction: internal Provider interface; port
> neostryder-style registry and Anthropic translation as adapters; contract
> tests per adapter (mocked). Exit gate: all adapters pass contract tests;
> base behavior regression-diff clean.*

Conflict resolution C1 (dossier Ch.7) is the binding design constraint:
**one OpenAI-compatible external surface; protocol translation is an
upstream concern, never an ingress concern.**

---

## 1. What Was Built

### 1.1 `providers.py` (new, ~700 LOC, zero new dependencies)

The internal Provider interface and its first two adapters:

| Component | Role |
|-----------|------|
| `ProviderAdapter` (ABC) | `build_url` / `build_headers` / `build_request` / `translate_request` / `translate_response` / `open_stream_translator` |
| `BuiltRequest` | fully provider-native upstream request (url, json, headers) |
| `StreamTranslator` | stateful SSE line transformer; base class = byte-exact passthrough |
| `OpenAICompatAdapter` (`openai`) | passthrough to any `/v1/chat/completions` upstream — behavior-identical to the pre-Stage-2 dispatch path |
| `AnthropicAdapter` (`anthropic`) | full OpenAI ↔ Anthropic Messages translation incl. SSE synthesis |
| `PROVIDER_REGISTRY` + `PROVIDER_ALIASES` | registry with capability metadata; aliases `ollama`, `lmstudio`, `vllm`, `llamacpp`, `openai_compat`, `openai-compatible` → `openai` |
| `get_provider` / `provider_name` | fail-closed resolution (unknown → `AdapterError`); never-raise name helper for traces |
| `validate_provider_config` | startup/hot-reload gate: every configured name must resolve |

**Anthropic translation contract** (pinned by tests, see §3):

- system messages hoist to top-level `system` (multiple join with `\n\n`)
- `tool` role messages merge into ONE user message of `tool_result` blocks
- assistant `tool_calls` → `tool_use` content blocks (arguments parsed JSON)
- `max_tokens` mandatory: request value → category `max_tokens_default` → 4096
- images: data URIs → base64 source; http(s) → URL source; other schemes dropped
- `tool_choice`: `auto`/`required`/named; `"none"` drops tools entirely
- unsupported OpenAI fields (`n`, `logprobs`, `response_format`, `seed`,
  penalties, `reasoning_effort`) dropped with a debug log (reasoning_effort
  remains D3-gated)
- error envelopes (`type: "error"`) pass through untouched
- stop_reason map: `end_turn|stop_sequence→stop`, `max_tokens→length`,
  `tool_use→tool_calls`, `refusal→content_filter`
- SSE synthesis: Anthropic events → OpenAI `chat.completion.chunk` frames
  (role-convention first chunk, text deltas, tool_use +
  `input_json_delta` accumulation, finish chunk from `message_delta`,
  optional include_usage-style usage chunk, `data: [DONE]` terminator;
  ping/comments/non-JSON swallowed; `error` event → error frame + `[DONE]`)

Determinism hook: `HERMES_ROUTER_FIXED_EPOCH` pins adapter-synthesized
`created` timestamps (test/capture only; unset in production).

### 1.2 `server.py` integration (surgical, +110 lines net)

| Site | Change |
|------|--------|
| `call_model` | adapter resolution (fail-closed synthetic 502 on unknown provider) → `build_request` (url/payload/headers) → alt-key rotation via `build_headers` → 200-bodies translated, untranslatable → synthetic 502 so the cascade engages |
| `route_request_stream` | same assembly; both SSE pump sites (main + 503-retry) now feed lines through a per-request `StreamTranslator` |
| `route_request` | fallback configs carry `fallback_provider` / `fallback2_provider` (unset → inherit primary); success bodies annotated; traces carry provider |
| circuit breaker | failure tuple extended to `(502, 503, 504, 529)` — 529 = Anthropic overloaded; zero effect on OpenAI upstreams |
| `_startup` + `__main__` | `validate_provider_config` — refuse boot on unknown names |
| `POST /reload` | rejects hot-reload with 400 if any provider name fails to resolve (atomic swap preserved) |
| `GET /admin/providers` (new) | auth-protected registry introspection: capability metadata, aliases, per-category resolution |
| `_annotate_provider` | `hermes_router.provider` annotation **only for non-openai producers** — default-provider responses stay byte-identical (golden contract preserved) |

Classifier lane (`_call_classifier_raw`) intentionally stays OpenAI-only:
it is internal infrastructure, not a provider surface.

### 1.3 `trace.py` (additive)

`trace_route(..., provider: str | None = None)` — optional field appended
when present. Every dispatch site now passes the producing adapter's name,
giving Stage 4's ledger its stable provider identifier at the audit layer.

## 2. Golden Fixture Diff (regression oracle) — CLEAN

Re-captured with the dual-protocol stub: **17 fixtures** (11 → 17; new:
10 anthropic nonstream, 11 anthropic stream synthesis, 12 cross-protocol
cascade anthropic→openai, 13 tool_call synthesis, 14 `/admin/providers`
registry snapshot).

Reviewed deltas in pre-existing fixtures — every one intended:

| Fixture | Delta | Verdict |
|---------|-------|---------|
| 01–05b, 07 (trace events) | route events gain `"provider": "openai"` | intended (attribution) |
| 04 (cascade) | both route events gain provider; bodies unchanged | intended |
| 06, 08, 09 | no change (no route events emitted) | clean |
| 91 (metrics) | + `requests_total_anthropic` + 2 prometheus series | intended (new category exists) |
| all openai response bodies | byte-identical | **contract preserved** |

`RUNTIME_FILES` in `tools/capture_golden.py` and `tests/test_security.py`
gained `providers.py` (subprocess harnesses copy the runtime closure; the
omission surfaced as 6 boot failures in the security suite — fixed).

## 3. Verification (all gates green)

| Gate | Result |
|------|--------|
| `ruff check .` | 0 findings (ruff 0.16.8) |
| `mypy server.py trace.py providers.py keystore.py` | 0 errors (mypy 2.3.1) |
| `bandit -r server.py providers.py keystore.py trace.py` | clean (3 documented `nosec B104` Stage-1 literals) |
| `pip-audit -r requirements-audit.txt --no-deps` | no known vulnerabilities |
| `pytest` | **108 passed** (Stage 1: 66 → +32 adapter contract units, +5 Stage 2 fixture contracts, +5 schema entries) |

New test coverage:

- `tests/test_providers.py` (32): registry fail-closed resolution, aliases,
  config validation; openai byte-exact framing identity; anthropic request
  (system hoist, tool merge, images, tools/tool_choice, max_tokens
  precedence, field drops), response (shape, tool_calls, stop map, error
  passthrough), SSE (happy path, tool streaming, noise swallowing, error
  termination, data-only dispatch robustness)
- `tests/test_golden_fixtures.py` (+5): per-fixture behavior contracts for
  scenarios 10–14, including the cross-protocol cascade and the
  "openai producer stays unannotated" rule

Tool-chain note: this stage ran under ruff 0.16.8 / mypy 2.3.1 (workspace
reset re-installed current versions; Stage 0/1 used ruff 0.6-era / mypy
1.x). Gates pass under the newer compilers — the ratchet did not loosen.

## 4. Ratchet Status

- R-L1..R-L4 (upstream per-file ignores): untouched, still catalogued
- R-M1/R-M2: retired in Stage 1; no new mypy overrides introduced
- New code: zero findings under the enforced rule set; no new ignores added
- Extended-rule debt in touched files: no increase (new module is clean)

## 5. Design Decisions of Record

1. **Flat module** (`providers.py`) over a package — matches the codebase
   idiom (server.py / trace.py / keystore.py) and the subprocess-harness
   runtime-file model.
2. **Translation inside `call_model`** (response rebuild via
   `httpx.Response(200, json=...)`) so `route_request`'s consumer contract
   (`status_code` + `.json()`) is untouched — the cascade required zero
   structural edits.
3. **Malformed 200 bodies now cascade (502) instead of raising a raw 500**
   — strictly better failure semantics; not fixture-pinned (stub always
   returns valid JSON), documented here.
4. **Provider annotation only on non-default producers** keeps every openai
   golden body byte-identical while giving operators/Stage-4 ledger
   visibility for non-default routes.
5. **`fallback_provider` defaults to inheriting the primary's provider**;
   cross-protocol fallbacks are explicit config (fixture 12 pins the
   explicit form).
6. **Anthropic `base_url` is the host root** (adapter appends
   `/v1/messages`), mirroring the Anthropic SDK convention.

## 6. Exit-Gate Checklist

- [x] Internal Provider interface defined and documented
- [x] neostryder-style registry (capability metadata + aliases) implemented
- [x] Anthropic translation adapter (request/response/SSE/tool calls)
- [x] Contract tests per adapter, mocked (32 unit tests)
- [x] All adapters pass contract tests
- [x] Base behavior regression-diff clean (openai bodies byte-identical;
      all fixture deltas intended and catalogued in §2)
- [x] Fail-closed validation at boot and hot-reload
- [x] Provider attribution in audit trace (Stage 4 seed)
- [x] All five gates green (ruff / mypy / bandit / pip-audit / pytest)
- [x] No new lint/type/security debt introduced

**Verdict: Stage 2 exit gate PASS.** Next: Stage 3 — pooling and free-tier
catalog (keypool 429-header-aware cooldown rotation, capability tags,
free-tier catalog ingestion with refresh job).

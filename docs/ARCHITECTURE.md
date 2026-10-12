# Architecture — hermes-router v1.0.0

This document defines the system element by element: what each piece is,
what it is for, and how it behaves — the reference behind the diagrams,
the [README](../README.md), the [RUNBOOK](RUNBOOK.md) and the
[wiki walkthrough](WALKTHROUGH.md).

* [Big-picture schematic](#1-big-picture) — planes and rails
* [Request lifecycle](#2-request-lifecycle) — the six phases, step by step
* [Element reference](#3-element-reference) — purpose and function of every element
* [The layered build](#4-the-layered-build) — how nine stages compose the release
* [Data and state](#5-data-and-state) — what lives where on disk
* [Failure model](#6-failure-model) — what happens when things break

---

## 1. Big picture

![Architecture schematic](assets/architecture.png)

The gateway is a **vertical pipeline with two cross-cutting rails**. A
request flows top to bottom through six planes — clients, security edge,
routing core, policy gates, dispatch, upstream providers — while the
billing plane records what every completion cost and the operations
plane watches all of it. Each plane can only be reached by passing the
one above it, which is what makes the security and cost guarantees
mechanical rather than aspirational.

The design rule across all planes is **fail-closed**: an ambiguous or
missing configuration refuses to boot, an unauthenticated call is
rejected before routing, an over-budget key is rejected before any
upstream spend, and an unverifiable ledger is treated as corrupted.

## 2. Request lifecycle

![Request lifecycle flowchart](assets/request_lifecycle.png)

One `POST /v1/chat/completions` request passes through six phases.
The gates are listed in order; the first gate that fails terminates the
request with its documented error and nothing downstream executes.

| Phase | Gate | Failure mode | Purpose |
|---|---|---|---|
| 1. Arrival & auth | bearer token check | `401` no/wrong token, `403` virtual key on admin surface | Establish *who* is calling before anything is spent |
| 2. Policy gates | budget gate → cost-guard limiters → pacer | `402 budget_exceeded`, `429 cost_guard_rate_limited` | Block or shape *spend the request is allowed to create* — before any upstream call |
| 3. Routing decision | session cache → 5-tier classifier → deviation / override | (never fails — falls back to cheapest local tier) | Decide *which model* serves the message, spending as little as possible deciding |
| 4. Upstream dispatch | circuit breaker → pool lease → adapter call → cascade | synthetic `503` / `429` stepping to the next chain entry | Make *one upstream call* succeed, isolating failures per endpoint and per key |
| 5. Completion & billing | finally-guarded ledger append | append failure → alert (billing visibility loss) | Convert the completion into *one immutable cost record* — exactly one, always |
| 6. Response & observation | — | — | Deliver the response and make the request *observable* (metrics, traces, sessions, alerts) |

Two properties of this pipeline are pinned by tests rather than
promised by comments: **exactly one ledger row per completion**
(`tests/test_load_soak.py` asserts it under 32-way concurrent
streaming, including clients that disconnect mid-stream), and **README
route parity in both directions** (`tests/test_freeze.py`).

## 3. Element reference

Every element below exists in the tree; the file names are the runtime
modules (`server.py`, `providers.py`, `keypool.py`, `ledger.py`,
`costguard.py`, `alerts.py`, `dashboard.py`, `keystore.py`,
`catalog.py`, `trace.py`).

### 3.1 Security edge (Stage 1 — `server.py`, `keystore.py`)

* **Mandatory authentication** — *purpose:* only key-holders may talk
  to the gateway. *function:* every route requires a bearer token that
  resolves (env name or `ks:` keystore alias) at boot; a configured but
  unresolvable key refuses startup. `auth.mode: disabled` exists for
  local development only and must be written explicitly.
* **Virtual keys** — *purpose:* let subscribers (people, apps, agents)
  use the gateway without holding the master key. *function:* issued
  via `POST /admin/virtual_keys`, carry their own budgets, are
  forbidden from `/admin/*` and `/metrics` (403), and stamp their id
  onto every ledger record they cause.
* **Encrypted keystore** (`keystore.py`) — *purpose:* provider keys
  should not sit in plaintext env files. *function:* Fernet-encrypted
  store (`0600`) managed by `python -m keystore`; the master key is
  env-only and never written by the gateway; `ks:<alias>` references
  resolve at call time.
* **Log redaction** — *purpose:* logs must not leak credentials.
  *function:* the RedactFilter rewrites bearer/vk/sk/rk/fk-looking
  tokens before any line is emitted.
* **CORS + bind lockdown** — *purpose:* reduce the attack surface.
  *function:* CORS off by default (same-origin), explicit allow-lists
  required, a wildcard origin drops credentials; the server binds
  `127.0.0.1` unless explicitly configured otherwise (the Dockerfile
  sets `ROUTER_HOST=0.0.0.0` and exposure is controlled by the port
  mapping).

### 3.2 Routing core (audited upstream heritage — `server.py`)

* **Session cache** — *purpose:* classify a conversation once, not per
  message. *function:* the session key is the first 16 hex chars of
  `sha256(first user message)`; entries carry the chosen tier and a
  timestamp; TTL from `classifier.session_timeout_minutes` (default
  5). Cache hits skip classification entirely.
* **5-tier classifier** — *purpose:* pick the category (chat, code,
  devops, research, …) as cheaply as confidence allows.
  *function:* tiers are tried cheapest-first and each yields at its
  confidence threshold — fastText (bag-of-words, <0.01 ms), SetFit
  (fine-tuned sentence transformer), zero-shot embeddings (cosine
  similarity against per-category descriptions), the ML surrogate
  (TF-IDF/embedding pipeline trained on traces), and finally the LLM
  flash classifier. Optional tiers are disabled per config; the chain
  degrades to the cheapest local tier, never to an unconfigured
  upstream.
* **Keyword deviation detection** — *purpose:* follow-up messages can
  change intent without starting a new session. *function:* a
  sub-millisecond scan for escalation / de-escalation keywords
  ("debug this" escalates; "thanks!" de-escalates) re-routes the
  session when the intent genuinely shifts.
* **`/use:` override** — *purpose:* the human always wins.
  *function:* a message starting `/use:<category>` forces the category
  for that message.
* **Persona context** — *purpose:* personalized answers.
  *function:* optional USER.md / MEMORY.md excerpts, length-capped by
  `max_context_chars`, prepended to classification and dispatch.

### 3.3 Policy gates (Stages 4–5 — `ledger.py` budgets, `costguard.py`)

* **Budget gate** — *purpose:* a subscriber cannot exceed their
  allocation. *function:* before any upstream call, the virtual key's
  in-window spend (day / month / total) is compared to its limit;
  `>=` means `402 budget_exceeded` and zero upstream cost. Budget
  state derives from ledger records, so it is exactly as trustworthy
  as the ledger.
* **Cost-guard limiters** — *purpose:* rate and token ceilings per
  scope, enforced even for the admin key at the global scope.
  *function:* true sliding windows in three scopes — `principal` (per
  virtual key; the admin key is exempt, mirroring the budget gate),
  `pool` (enforced at the pooled dispatch door; blocked = cascade
  step with a synthetic 429), `global` — each limiter setting exactly
  one dimension (`requests` or `tokens`). Tokens are estimated on
  admission (chars / `estimate_chars_per_token`) and **reconciled to
  the upstream-reported total** after completion, so windows hold
  exact usage.
* **Pacer** — *purpose:* smooth bursts so free-tier ceilings are not
  tripped by concurrency spikes. *function:* token bucket — up to
  `max_burst` immediate admits, then `requests_per_second` steady.
* **Billing-leak watchdog** — *purpose:* an upstream 401/402 means
  someone billed us invisibly or a key is bad. *function:* records
  such events; `/admin/costguard` cross-checks completions against
  ledger appends and reports `billing_visibility: ok | leaking`.

### 3.4 Dispatch plane (Stages 2–3 — `providers.py`, `keypool.py`)

* **Provider adapters** — *purpose:* speak two wire protocols, route
  to any vendor. *function:* the internal Provider interface with two
  implementations — `openai` (any `/v1/chat/completions` endpoint:
  OpenAI, Ollama, vLLM, LM Studio, llama.cpp, Groq, DeepSeek, …) and
  `anthropic` (Messages API, host-root base URL, full request/
  response translation including streaming SSE synthesis and tool
  calls). Adapter names are validated at boot; unknown names refuse
  to start (fail-closed).
* **Key pools** (`keypool.py`) — *purpose:* treat N of your own keys
  as one fleet. *function:* round-robin leasing with capability-tag
  filtering (`require_capabilities`); on raw 429 the used key cools
  down for the duration the provider's own headers announce
  (`Retry-After` seconds / HTTP-date, `x-ratelimit-reset`), clamped
  by policy; on 401/403 the key is quarantined until fixed or
  operator-unlocked (`POST /admin/pools/{name}/unlock`). Key ids
  (`<pool>/<label>`) seed the ledger's key dimension.
* **Circuit breakers** — *purpose:* stop hammering a dead endpoint.
  *function:* per-endpoint counters over a window; at
  `failure_threshold` (default 3) the circuit opens, calls fail fast
  with synthetic 503, and the cascade advances; after
  `recovery_timeout_sec` a half-open trial closes or re-opens the
  circuit. `GET /circuits` lists state, `POST /circuits/reset`
  force-closes.
* **Fallback cascade** — *purpose:* one bad provider is a step, not
  an outage. *function:* each category carries primary → fallback →
  fallback2 (each with its own model / base URL / key source /
  provider protocol — cross-protocol fallback is allowed). Any
  failure (transport → synthetic 503; 429 after pool exhaustion →
  synthetic 429) advances to the next entry; the client sees an error
  only when the entire chain is exhausted.

### 3.5 Billing plane (Stage 4 — `ledger.py`)

* **Immutable cost ledger** — *purpose:* provable accounting.
  *function:* SQLite in WAL mode; every successful completion appends
  exactly one record — model, provider, key id, virtual-key
  attribution, token counts, integer micro-USD costs, and the exact
  price snapshot used. Each record chains the previous record's hash
  (SHA-256; HMAC-SHA256 when `hmac_key_env` is set, making the chain
  tamper-proof, not just tamper-evident). Appends are finally-guarded
  on both stream pump lanes — a client disconnecting at the terminal
  `[DONE]` frame still lands an interrupted record (the Stage 8 fix;
  previously ~3% of concurrent stream records were lost).
* **Price snapshots** — *purpose:* costs must be reproducible.
  *function:* `pricing` entries (USD per million tokens, integer-exact
  nano-USD internally) are snapshot-hashed; the snapshot id is stored
  on every record, so a later price change never rewrites history.
* **Virtual keys + budgets** — *purpose:* per-subscriber spend
  control. *function:* create / list / patch / revoke over the admin
  API; `budget_mode: none | tokens | cost_micros` ×
  `budget_period: day | month | total`; the token is printed exactly
  once at creation.
* **Verify surfaces** — *purpose:* prove integrity on demand.
  *function:* `GET /admin/ledger/verify` recomputes the full chain;
  `python -m ledger verify` does it offline (exit 1 +
  `first_bad_seq` on corruption) — the R9 backup/restore drill
  rehearses both.

### 3.6 Operations plane (Stages 6+8 — `dashboard.py`, `alerts.py`)

* **Zero-asset dashboard** — *purpose:* operational sight without
  third-party baggage. *function:* `GET /dashboard` (admin key)
  renders status cards, spend by model/key/day, virtual-key budgets,
  a paginated ledger browser, pool and circuit state — from zero
  external assets, so it works air-gapped. `?format=json` for
  machine consumption.
* **Alerting rules** — *purpose:* the gateway tells you before you
  find out. *function:* budget_threshold (warn ≥80%, critical at
  100% or blocked), pool_exhaustion, circuit_open, billing_leak,
  ledger_append_failures, and opt-in ledger_verify (O(n) chain
  scan). Evaluation is pull-model — every `/dashboard`,
  `/admin/status` or `/admin/alerts` call evaluates all rules, so
  your uptime checker's polling cadence drives alerting.
* **Webhook** — *purpose:* alerts should reach you. *function:*
  `raised` / `cleared` / `test` events POSTed once per transition;
  delivery is fail-soft (never breaks a request; failures counted in
  `/metrics`).
* **Metrics + traces** — *purpose:* numbers for graphs, decisions
  for humans. *function:* Prometheus text at `/metrics`; one JSONL
  line per routing decision in `traces/` (classification, cache hit,
  deviation, fallback, breaker transitions), input-capped and
  size-capped.

### 3.7 Hermes integration (Stage 7)

* **`GET /v1/models`** — *purpose:* BYOM clients can discover what
  they may call. *function:* every routable upstream model (primary +
  fallbacks, all categories), deduplicated, with tier metadata.
* **Model-provider plugin** (`plugins/hermes-router-provider/`) —
  *purpose:* onboard a Hermes agent without touching its core.
  *function:* a self-contained provider package that adapts the
  observed registry shapes, fail-soft against contract drift; see
  [HERMES_INTEGRATION.md](HERMES_INTEGRATION.md).

### 3.8 Hardening (Stage 8)

* **Load gate** — 32-way concurrent streaming with unique markers,
  asserting exact one-row-per-request billing, exact attribution, and
  bounded fds/RSS. This gate found and closed the stream-disconnect
  billing gap.
* **Compressed soak** — a deterministic fault lattice (rotation →
  exhaustion → synthetic-429 cascade → recovery) in a time-compressed
  window, asserting ledger green, counters engaged, alerts raised and
  cleared.
* **Failure drills** — the three dossier lifecycles as automated
  end-to-end tests plus RUNBOOK §8 manual procedures: provider outage
  (circuit → cascade → alert → half-open recovery → reset), key
  exhaustion (cooldowns → synthetic 429 → alert → unlock), ledger
  corruption (backup → tamper → detect → restore).
* **Docs freeze** — README endpoint table ↔ live routes parity both
  directions, RUNBOOK coverage, RELEASE structure, version
  consistency, memo existence: all pinned as tests, because a freeze
  that isn't enforced drifts.
* **`tools/soak.py`** — the operator's 24-hour entry point
  (stdlib-only, RUNBOOK §8.6): sustained mixed stream/non-stream
  traffic with unique markers and full stream reassembly, heartbeat
  timeline sampling, graceful SIGINT/SIGTERM, JSON+Markdown reports,
  PASS verdict = zero 5xx + zero incomplete bodies + error rate
  within tolerance.

## 4. The layered build

![Layered build](assets/layers.png)

The release was assembled in nine gated stages (memo per stage in
`docs/STAGE{0..8}_*.md`, stage ledger in [RELEASE.md](RELEASE.md) §2):

| Stage | Layer | Why it sits there |
|---|---|---|
| 0 | Audit + golden fixtures | Truth about the inherited code before changing it — 46 byte-stable transcripts still pin the surface |
| 1 | Security hardening | Auth must wrap everything above it |
| 2 | Provider abstraction | Dispatch needs one interface before pooling or billing can mean anything |
| 3 | Key pooling + catalog | Rotation exploits the abstraction; the catalog knows the free-tier ceilings |
| 4 | Immutable ledger | Budgets and reconciliation need a source of truth for spend |
| 5 | Cost guard | Admission control needs the ledger to reconcile against |
| 6 | Operations | Alerting watches all planes; the dashboard reads the ledger |
| 7 | Hermes integration | Discovery + plugin ride on the finished surface |
| 8 | Hardening + freeze | Load, soak, drills, docs-as-tests, tag |

## 5. Data and state

| Path | Contents | Lifetime |
|---|---|---|
| `router_config.yaml` | the live configuration (env names, never secrets) | edited by the operator; hot-reload via `POST /reload` |
| `.router/ledger.sqlite3` | the cost ledger (WAL) | survives redeploys — back it up (RUNBOOK §6) |
| `.router/keystore.*` | encrypted provider keys | survives redeploys; master key stays in env |
| `.router/traces.jsonl` | routing decision traces | size-capped rotation |
| `.router/fasttext/`, `.router/setfit/`, `.router/surrogate/` | optional classifier artifacts | produced by the training scripts, read at boot if enabled |

Everything above is relative to the gateway working directory, which
is why the compose profile mounts exactly one state volume.

## 6. Failure model

| Failure | System behavior | Observable at |
|---|---|---|
| Wrong / missing token | `401` before routing | request log |
| Virtual key on admin surface | `403` | request log |
| Budget exhausted | `402` before any upstream spend | `/admin/virtual_keys`, alert budget_threshold |
| Rate / token window trip | `429 cost_guard_rate_limited` with the limiter verdict in detail | `/admin/costguard` |
| Provider endpoint down | circuit opens after threshold; cascade advances; synthetic 503 only if the chain is exhausted | `/circuits`, alert circuit_open |
| All pool keys cooling | synthetic 429, cascade advances | `/admin/pools`, alert pool_exhaustion |
| Upstream 401/402 | watchdog event, pool quarantine for the key | `/admin/costguard`, alert billing_leak |
| Ledger append failure | request still succeeds; visibility loss counted and alerted | `/admin/costguard`, alert ledger_append_failures |
| Ledger tampered | verify fails with `first_bad_seq`; boot-time detection alerts | `/admin/ledger/verify`, alert ledger_verify (opt-in), offline CLI |
| Client disconnects mid-stream | stream ends; an interrupted cost record still lands | ledger record `interrupted` flag |

The invariant across all rows: **failures degrade capability, never
accounting**. Whatever happens, the ledger tells the truth about what
was spent, or alerts you that it cannot.

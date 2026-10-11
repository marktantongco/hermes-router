# Hermes Agent Integration Guide — hermes-router gateway

Audience: the person wiring a Hermes agent (NousResearch's open-source
`hermes-agent`) or any BYOM OpenAI-compatible client to a self-hosted
hermes-router gateway. This is the Stage 7 deliverable for the
dossier's "custom_providers onboarding guide" scope item. It assumes
the posture built in Stages 0–6: mandatory bearer auth, provider
adapters, key pooling, the virtual-key billing plane, the cost guard
and the operations dashboard.

Conventions: `ROUTER` is the gateway base URL (default
`http://127.0.0.1:8766`), `$ADMIN` is the gateway admin key, and
`~/.hermes/` is the agent's config root. The gateway side of every
example uses the example config shipped in `router_config.example.yaml`.

---

## 1. The two integration paths

The gateway is consumed through one of two verified styles (dossier
Ch. 3.4). Both keep the agent un-forked — all routing, pooling,
metering and budget intelligence lives in the gateway process.

| Path | Footprint | What you get | When to use |
|---|---|---|---|
| **A — `custom_providers`** (default) | two lines of `~/.hermes/config.yaml` | full routing, pooling, budgets, ledger attribution | always; start here |
| **B — model-provider plugin** (optional) | `~/.hermes/plugins/hermes-router-provider/` + env | Path A **plus** native model discovery (`GET /v1/models`) in the agent's model picker | when you want the agent to see every routable model, not one hard-coded name |

Path A is a pure config-level integration — the agent treats the
gateway as just another OpenAI-compatible provider, which is exactly
the integration surface the base repo was selected for (dossier
Ch. 5.2). Path B is the optional plugin package shipped at
`plugins/hermes-router-provider/`; it wraps the same wire surface and
adds discovery, pinned to the verified registry contract (Ch. 3.2).

## 2. Gateway prerequisites

Complete these once, on the gateway host, before touching the agent.

```bash
# 1) A config with at least one category (copy the example and edit)
cp router_config.example.yaml router_config.yaml
#    - categories.<name>.model / base_url        -> your upstream models
#    - auth.api_key_env: ROUTER_PROXY_API_KEY    (auth is mandatory)

# 2) The admin key, resolvable in the gateway's environment
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
echo "ROUTER_PROXY_API_KEY=<that-value>" >> ~/.hermes/.env   # or your env file

# 3) Boot (compose profile, systemd unit, or bare uvicorn) and verify
curl -fsS $ROUTER/health
curl -fsS -H "Authorization: Bearer $ADMIN" $ROUTER/admin/providers | jq .
```

The `/admin/providers` output confirms every category resolves to a
provider adapter (fail-closed at boot: an unknown provider name
refuses startup). If you plan to attribute the agent's traffic to a
subscriber identity with a blocking budget — recommended, see §5 —
create the virtual key now and use it in place of the admin key
everywhere below.

## 3. Path A — `custom_providers` (the two-line footprint)

In `~/.hermes/config.yaml`, add the gateway as a custom provider and
point the default model at it:

```yaml
model:
  default: deepseek-v4-pro        # any id from GET /v1/models (see §4)
  provider: auto-router

custom_providers:
- name: auto-router
  base_url: http://127.0.0.1:8766/v1     # the gateway, /v1 root
  model: deepseek-v4-pro                 # must match one of the gateway's models
  api_key: ${HERMES_ROUTER_API_KEY}      # or the agent's own env interpolation
```

Then restart the Hermes gateway (`hermes gateway restart`) and verify
end-to-end from the agent host:

```bash
# The exact wire calls the agent now makes:
curl -fsS -H "Authorization: Bearer $HERMES_ROUTER_API_KEY" \
  $ROUTER/v1/models | jq '.data[].id'

curl -fsS -H "Authorization: Bearer $HERMES_ROUTER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model": "deepseek-v4-pro",
       "messages": [{"role": "user", "content": "hello"}]}' \
  $ROUTER/v1/chat/completions | jq '.choices[0].message.content'
```

How requests behave from here:

* **Routing is content-based, not name-based.** The classifier (or
  `/use:<category>` overrides, or the surrogate) picks the category per
  session; the request's `model` field is advisory — the gateway
  rewrites it to the selected category's upstream model. Picking
  "deepseek-v4-flash" vs "deepseek-v4-pro" in the agent does not pin
  the tier; the message content does.
* **Streaming and tool calls pass through unchanged** (OpenAI-protocol
  upstreams byte-exact; Anthropic-protocol tiers translate, including
  SSE synthesis and tool-call round trips — Stage 2 adapters).
* **Failover is delegated.** If the gateway is unreachable, the
  agent's own `fallback_providers` chain engages; if an upstream model
  is unhealthy, the gateway's internal cascade (fallback → fallback2,
  circuit breakers, pool rotation) engages first. Configure the
  agent-side fallback chain only for gateway-host outages.

## 4. Model naming rules (`GET /v1/models`)

`GET /v1/models` (bearer auth; admin key **or** virtual key) lists
every upstream model the router can reach across all categories —
primary, fallback and fallback2 hops — deduplicated and sorted:

```json
{"object": "list", "data": [
  {"id": "deepseek-v4-flash", "object": "model", "created": 1760000000,
   "owned_by": "openai",
   "hermes_router": {"categories": ["chat"], "roles": ["primary"]}},
  {"id": "deepseek-v4-pro", "object": "model", "created": 1760000000,
   "owned_by": "openai",
   "hermes_router": {"categories": ["code"], "roles": ["primary"]}}]}
```

Any listed `id` is a valid `custom_providers.model` value. The
namespaced `hermes_router` block (categories that route to the model,
roles it plays) is metadata for pickers and plugins; standard OpenAI
clients ignore it. Model ids appear in every completion's `model` echo
regardless, so this surface exposes nothing new. The list refreshes on
hot reload (`POST /reload`) — no restart needed after category edits.

## 5. Attribution and budgets: use a virtual key

The admin gateway key is all-powerful (admin surfaces + inference).
For the agent, prefer a **virtual key** (Stage 4): inference-only,
ledger-attributed per subscriber, and enforceably capped.

```bash
curl -X POST $ROUTER/admin/virtual_keys \
  -H "Authorization: Bearer $ADMIN" -H "Content-Type: application/json" \
  -d '{"name": "hermes-agent",
       "budget_mode": "cost_micros", "budget_limit": 5000000,
       "budget_period": "month"}'
# -> {"virtual_key": {"id": "vk-...", "token": "vk-..."}}   (token shown once)
```

Set that `vk-...` token as the agent's `api_key` (Path A) or
`HERMES_ROUTER_API_KEY` (Path B). Behavior changes vs the admin key:
402 `budget_exceeded` when the budget is spent (before any upstream
call), 429 `cost_guard_rate_limited` when a limiter window is full,
403 on all `/admin/*` and `/metrics` surfaces. Spend and attribution
land in the ledger (`/admin/ledger/records?virtual_key_id=...`) and on
the dashboard (`/dashboard?vk=<id>`).

## 6. Path B — the optional model-provider plugin

Install only if you want native model discovery. The package
(`plugins/hermes-router-provider/`) is stdlib-only and fail-soft by
design (risk R3 — upstream contract drift must never break agent
boot). Full install, config and verification steps live in the
package README; the short form:

```bash
mkdir -p ~/.hermes/plugins
cp -r plugins/hermes-router-provider ~/.hermes/plugins/
echo "HERMES_ROUTER_API_KEY=vk-..." >> ~/.hermes/.env
hermes gateway restart
```

Registration logs one line naming the registry shape it bound
(`register_provider`, positional variant, `register_model_provider`,
providers-mapping, or return-value fallback) and the discovered model
count. `list_models()` / `model_details()` surface the same entries as
`GET /v1/models`; `create_client()` returns an OpenAI SDK client
pointed at the gateway when the SDK is importable, else the
`{base_url, api_key}` connection dict.

## 7. Streaming and tool-calling contract

What the gateway preserves on the wire (pinned by the Stage 7 e2e
suite, `tests/test_hermes_e2e.py`):

* **Tool definitions** (`tools`, `tool_choice`) forwarded upstream
  as-is on OpenAI-protocol tiers; translated to Anthropic
  `tools`/`tool_choice` on Anthropic-protocol tiers.
* **Assistant `tool_calls`** returned in the OpenAI shape regardless
  of the upstream protocol (Anthropic `tool_use` blocks are
  synthesized back); `tool_call_id` round trips unchanged.
* **`role: "tool"` result messages** forwarded (OpenAI) or merged into
  `tool_result` blocks (Anthropic).
* **Streaming**: SSE frames forwarded byte-exact on OpenAI tiers and
  synthesized (`chat.completion.chunk` + `data: [DONE]`) on Anthropic
  tiers. Mid-stream upstream failures close the SSE cleanly so the
  agent's `HERMES_STREAM_RETRIES` reconnect logic engages with a fresh
  request — the delegated-retry contract from the base repo.

One Anthropic-tier caveat: request fields with no Anthropic equivalent
(`n`, `logprobs`, `response_format`, `seed`, ...) are dropped with a
debug log, and `max_tokens` defaults to the category's
`max_tokens_default` (else 4096) when the request omits it.

## 8. Local Ollama lane (free local inference)

The mission's zero-marginal-cost lane: a category pointing at a local
Ollama (or vLLM / LM Studio / llama.cpp — any OpenAI-compatible
runtime) needs no API key and records zero-cost ledger rows when
priced `free: true`:

```yaml
categories:
  chat:
    label: "Chat & Trivia"
    model: qwen2.5:3b            # any `ollama pull`-ed model
    base_url: http://localhost:11434/v1
    api_key_env: ""              # Ollama needs no key
    timeout_seconds: 120

pricing:
  entries:
    qwen2.5:3b: {input_per_mtok: 0.0, output_per_mtok: 0.0, free: true}
```

Verify the lane exactly as the Stage 7 exit gate does:

```bash
# upstream reachable?
curl -fsS http://localhost:11434/v1/models | jq '.models[].id'   # or .data[].id
# through the gateway (auth still mandatory — Ollama's openness ends at it):
curl -fsS -H "Authorization: Bearer $HERMES_ROUTER_API_KEY" \
  $ROUTER/v1/models | jq '.data[].id'
curl -fsS -H "Authorization: Bearer $HERMES_ROUTER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model": "qwen2.5:3b", "stream": true,
       "messages": [{"role": "user", "content": "hello"}]}' \
  $ROUTER/v1/chat/completions
```

The e2e suite automates this against a live local Ollama when one is
reachable (§10); in CI it runs against a faithful Ollama-shaped mock.

## 9. Troubleshooting

| Symptom | Meaning | Fix |
|---|---|---|
| 401 `auth_error` from the gateway | No/wrong bearer token, or the gateway's key env is empty | Send `Authorization: Bearer vk-...`; check `auth.api_key_env` resolves on the gateway host |
| Agent reports the model "does not exist" | `custom_providers.model` is not a gateway model id | List valid ids: `GET /v1/models`; edit config; the field is advisory for routing but must be a real id |
| 402 `budget_exceeded` | Virtual key spent its budget | Raise via `PATCH /admin/virtual_keys/{id}` or wait for the period to roll |
| 429 `cost_guard_rate_limited` | A cost-guard window (RPM/TPM/RPD) is full | Check `/admin/costguard`; tune limiter windows or the pacer |
| 429 with `Retry-After` upstream semantics | Free-tier pool cooldown rotation | Normal pooling behavior; check `/admin/pools` |
| Streaming stops early, agent retries | Upstream died mid-stream; gateway closed SSE cleanly | Expected delegated-retry contract; check circuit state (`/circuits`) and the runbook's failure drills |
| Plugin logs "no known provider-registry API" | Agent version changed its registry shape | Plugin still returns the provider (return-value lane); re-pin the plugin commit per the R3 policy |
| `/v1/models` empty `data` | No categories configured | Add categories; hot-reload |

## 10. Verification checklist (exit gate)

The dossier's Stage 7 exit gate — *end-to-end: Hermes → gateway → mock
and live local Ollama; tool calls preserved; streaming intact* — is
verified in two layers, mirroring the Stage 6 compose-gate pattern:

1. **In CI (deterministic, always):** `tests/test_hermes_e2e.py` boots
   the real gateway against an Ollama-shaped mock upstream and drives
   a Hermes-shaped client through the full agent flow — discovery
   (`/v1/models`), tool-call round trips (non-stream and stream),
   tool-result follow-ups, the plugin registration + discovery path,
   and the auth/budget/limiter edges. Tool-call preservation and SSE
   integrity are asserted byte-level.
2. **On an Ollama host (live, opt-in):** with Ollama at
   `HERMES_E2E_OLLAMA_URL` (default `http://127.0.0.1:11434/v1`), the
   same suite runs the discovery/chat/stream flows against the real
   local runtime (auto-skipped when unreachable; the manual §8
   commands are the operator-side equivalent).

Run it:

```bash
python -m pytest tests/test_hermes_e2e.py -v
# live lane, on a host with Ollama:
HERMES_E2E_OLLAMA_URL=http://localhost:11434/v1 python -m pytest tests/test_hermes_e2e.py -v
```

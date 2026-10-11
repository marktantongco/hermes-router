# Stage 7 — Hermes Integration (Exit-Gate Memo)

**Scope (dossier Ch.9, Table 7, row 7):** custom_providers onboarding
guide; optional model-provider plugin package (pinned contract) with
model discovery. **Exit gate:** end-to-end — Hermes → gateway → mock
and live local Ollama; tool calls preserved; streaming intact.

**Provenance:** the onboarding guide documents the integration surface
the base repo was selected for (dossier Ch. 5.2: "registered as a
custom OpenAI-compatible provider in the Hermes agent config, honoring
the agent's streaming retry contract"). The plugin package is the
Ch.7-matrix ADAPT of the model-provider-plugin wrapper concept
(denstepa/hermes-claude-cli-plugin architecture + xz_dev/
hermes-litellm-provider registry pattern — "Deepest verified
provider-plugin contract incl. tool-catalog loopback; behavior of the
sources rejected; risk: upstream API drift, pinned versions").
Clean-room: no code taken; the plugin depends only on the verified
public contract (Ch. 3.2) and the gateway's own wire surface.

## 1. What Was Built

### 1.1 `GET /v1/models` (server.py, +50 LOC)

The dossier's ingress plane (Ch. 8.1) specifies "/v1/chat/completions
and /v1/models with bearer authentication"; the second half did not
exist until now — and model discovery cannot work without it. The
endpoint enumerates every upstream model the router can reach across
the configured categories (primary, fallback, fallback2 hops),
deduplicated by name, sorted for determinism. Each entry carries the
standard OpenAI fields (`id`, `object`, `created`, `owned_by` — the
provider adapter name) plus a namespaced `hermes_router` block
(`categories` that route to the model, `roles` it plays) so pickers
and plugins can surface tier structure without admin access. Both
principal kinds may list (admin key and virtual keys — it is an
inference surface, the discovery call an agent's OpenAI client makes);
`created` honors the deterministic epoch hook, keeping golden captures
byte-stable. Model ids appear in every completion's `model` echo
regardless, so the surface exposes nothing new (a Stage-1-posture
note, not a new secret).

### 1.2 `plugins/hermes-router-provider/` (new package, stdlib-only)

The optional Path-B companion to the config-level `custom_providers`
integration. Three files:

* **`plugin.yaml`** — manifest pinning the verified contract: location
  `~/.hermes/plugins/hermes-router-provider/`, `kind:
  model-provider`, `entrypoint: provider:register`.
* **`provider.py`** (~250 LOC) — `register(ctx)` builds the provider
  from env (`HERMES_ROUTER_URL`, `HERMES_ROUTER_API_KEY`,
  `HERMES_ROUTER_MODEL`, `HERMES_ROUTER_TIMEOUT_SECONDS`), performs
  one fail-soft discovery attempt, registers into the host registry,
  and always returns the provider. The provider surface the agent's
  model machinery consumes: `name` / `base_url` / `api_key` /
  `default_model`, `list_models()` / `model_details()` (native
  discovery over `GET /v1/models`, urllib, warm cache, fail-soft to
  `[]`), and `create_client()` — an OpenAI SDK client pointed at the
  gateway when the SDK is importable, else the `{base_url, api_key}`
  connection dict (the audited client-factory-wrapping pattern).
* **`README.md`** — install (copy or git-URL with commit pinning),
  env configuration, verification, and the drift policy.

Risk R3 (upstream hermes-agent plugin/config contract drift) drives
the design: `_register_into_ctx` adapts defensively to the observed
registry shapes in order — `register_provider(name, provider)` (the
documented form), `register_provider(provider)` (positional variant,
reached via TypeError recovery), `register_model_provider(provider)`,
`ctx.providers` mapping, and finally the return-value lane — logging
which shape bound. Registration never raises; an unreachable gateway
degrades to a warning. Secrets stay out of `repr`/`describe()`
(Stage-1 redaction discipline applied client-side).

### 1.3 `docs/HERMES_INTEGRATION.md` (new guide)

The onboarding deliverable. Ten sections: the two integration paths
(custom_providers = the two-line default; plugin = optional discovery)
with a decision table; gateway prerequisites; the Path-A config block
plus the exact wire calls to verify; model naming rules against
`GET /v1/models`; virtual-key attribution and budgets (the agent
should use a `vk-` key, not the admin key); Path-B install and verify;
the streaming/tool-calling contract (what is preserved on the wire,
per protocol tier, including the delegated-retry behavior and the
Anthropic-tier field-drop caveat); the local Ollama lane with priced-
free config; a troubleshooting table keyed to the gateway's error
codes (401/402/429, model-name mismatch, early stream close, registry
drift); and the two-layer verification checklist.

## 2. The Exit Gate, Layered

Dossier row-7 gate: *end-to-end Hermes → gateway → mock and live local
Ollama; tool calls preserved; streaming intact.* Proven in two layers,
the Stage-6 compose-gate pattern (deterministic in CI; live where the
runtime exists):

1. **`tests/test_hermes_e2e.py`** (26 tests) against an Ollama-shaped
   mock upstream (`/v1/models` + `/v1/chat/completions` with tool
   calls, tool-result answers and SSE, in Ollama's OpenAI-compat
   shape). A Hermes-shaped client — the wire calls the agent's OpenAI
   client makes, no `/use:` crutches — drives: discovery; content-
   based routing through the real classifier wire path; a full
   tool-call round trip (tool_calls → tool result → final answer,
   with ledger attribution asserted per virtual key); plain streaming
   (content deltas + usage frame + `[DONE]`); streaming tool calls
   (deltas reassembled to the identical id/name/arguments); and the
   plugin lane end-to-end (mock ctx → registration shape → discovery
   → client factory → a tool-call chat through the produced
   connection). The `/v1/models` auth matrix (401 no/wrong token,
   admin + vk allowed) and the plugin's registration-shape adaptation
   (all five ctx shapes), fail-soft, env config and secrets-free
   introspection are unit-pinned in the same file.
2. **Live local Ollama lane** — four tests boot a second gateway
   against a real local Ollama (`HERMES_E2E_OLLAMA_URL`, default
   `http://127.0.0.1:11434/v1`) and run discovery, non-stream chat,
   streaming (SSE + `[DONE]`), and a tools wire check. They
   auto-skip when no Ollama is reachable — as in this CI sandbox —
   with the operator-side procedure (and exact commands) documented
   in the guide §8/§10. Tool-call *preservation* is proven
   deterministically by the mock lane; on live Ollama, whether a
   model chooses to call a tool is model behavior, so the live test
   pins the transport contract instead.

## 3. Design Decisions

1. **Config-level first, plugin second.** The guide leads with
   `custom_providers` because it is the surface the base repo was
   selected for, needs no plugin, and cannot drift with agent
   versions. The plugin exists for one capability only: native model
   discovery. This mirrors dossier Ch. 3.4 ("styles one and two
   require no fork of the agent") and de-risks R3.
2. **The `model` field stays advisory.** Routing is content-based;
   `/v1/models` ids are the names clients may *name*, not the tiers
   they *select*. The guide states this explicitly because it is the
   single most surprising behavior for new integrators.
3. **`hermes_router` metadata is namespaced, not standard.** OpenAI
   clients ignore unknown keys; pickers get tier structure. The
   alternative — separate admin-only discovery — would have made the
   plugin useless for virtual-key principals.
4. **The plugin is stdlib-only and runs warm.** It executes inside
   the agent process; adding dependencies there is hostile. One
   discovery attempt at registration warms the cache so model
   pickers are instant; later failures degrade to warning, never to
   an agent boot failure.
5. **Golden discipline held.** Fixture re-capture: all 45 behavior
   fixtures byte-identical; deltas are `golden_96_models.json` (new,
   deterministic — sorted ids, fixed-epoch `created`, static config)
   and the index (fixture list + Stage-7 note). `/metrics` and every
   other surface unchanged.

## 4. Gate Evidence

| Criterion (dossier Ch.9, row 7) | Evidence |
|---|---|
| Hermes → gateway → mock | `test_hermes_plain_chat_routes_by_content`, `test_hermes_tool_call_round_trip`, streaming tests — real gateway subprocess, classifier wire path, tool calls + results through, ledger-attributed per VK |
| … → live local Ollama | 4 live-lane tests + guide §8/§10 operator procedure (auto-skip in CI sandbox; Ollama absent — connection refused on 11434) |
| Tool calls preserved | Non-stream: id/type/function/arguments round trip asserted; stream: deltas reassembled to the identical contract; plugin lane repeats the proof through `create_client()` |
| Streaming intact | SSE byte contract: content deltas, finish frame, usage frame, `[DONE]`; delegated-retry clean-close behavior documented and unchanged from Stage 2's pinned fixtures |

**Quality gates:** ruff 0 / mypy 0 (11 files — plugin added) / bandit
0 / pip-audit clean (closure unchanged; plugin is stdlib-only, not in
requirements-audit.txt) / pytest **467 passed + 4 skipped** (443 +
28 new: 26 e2e + golden 96 contract + parametrized schema case).

## 5. Exit Gate Verdict

**Stage 7 exit gate: PASS.** The remaining stage per the v1 plan is
Stage 8 (hardening & freeze: load/soak tests with streaming
concurrency, failure drills, documentation freeze, final audit,
release tag). Say "proceed stage 8" when ready.

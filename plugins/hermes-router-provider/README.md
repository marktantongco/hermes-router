# hermes-router-provider (Hermes model-provider plugin)

Optional Stage 7 companion for the [hermes-router
gateway](../../README.md): routes the Hermes agent through a
self-hosted gateway — content-based tier routing, free-tier key
pooling, virtual-key budgets, immutable cost ledger — **with native
model discovery** over the gateway's OpenAI-compatible `GET /v1/models`
surface.

The config-level `custom_providers` entry (see
[docs/HERMES_INTEGRATION.md](../../docs/HERMES_INTEGRATION.md)) needs
**no plugin at all**. Install this plugin only if you want the agent's
model picker to list the gateway's routable models (with tier
structure) instead of one hard-coded name.

## Install

```bash
# From a hermes-router checkout (pin the commit — risk R3 policy):
mkdir -p ~/.hermes/plugins
cp -r plugins/hermes-router-provider ~/.hermes/plugins/

# Or via the agent's git-URL install (commit-pinned):
#   hermes plugin install <git-url-to-this-repo>#<commit> --name hermes-router-provider
```

Then restart the Hermes gateway and confirm registration in its logs:

```
hermes-router-provider: registered via register_provider (N models discovered)
```

## Configure

Environment (the agent loads `~/.hermes/.env`):

| Variable | Default | Purpose |
|---|---|---|
| `HERMES_ROUTER_URL` | `http://127.0.0.1:8766/v1` | Gateway OpenAI root |
| `HERMES_ROUTER_API_KEY` | — | Bearer token. **Use a `vk-` virtual key** (attribution + blocking budgets) rather than the admin gateway key |
| `HERMES_ROUTER_MODEL` | first discovered | Preferred model id |
| `HERMES_ROUTER_TIMEOUT_SECONDS` | `5.0` | Discovery timeout |

Create a virtual key on the gateway first (admin key required):

```bash
curl -X POST http://127.0.0.1:8766/admin/virtual_keys \
  -H "Authorization: Bearer $ROUTER_PROXY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "hermes-agent", "budget_mode": "cost_micros",
       "budget_limit": 5000000, "budget_period": "month"}'
# -> {"virtual_key": {"id": "...", "token": "vk-..."}}  (token shown once)
echo "HERMES_ROUTER_API_KEY=vk-..." >> ~/.hermes/.env
```

## Verify

```bash
# Discovery through the plugin (same wire call it makes):
curl -H "Authorization: Bearer $HERMES_ROUTER_API_KEY" \
  http://127.0.0.1:8766/v1/models
```

In Python (the surface the agent consumes):

```python
from plugins.hermes_router_provider import provider as plugin  # or import from the installed dir
p = plugin.build_provider()
p.list_models()          # ['deepseek-v4-flash', 'deepseek-v4-pro', ...]
p.model_details()        # full entries incl. hermes_router.categories/roles
p.create_client()        # OpenAI SDK client when importable, else connection dict
```

## Pinned contract & drift policy

The plugin depends on two surfaces only (dossier Ch. 3.2, VERIFIED):

1. The agent's `register(ctx)` / `ctx.register_provider(...)` plugin
   contract — adapted defensively to the observed call shapes
   (`register_provider(name, provider)`, `register_provider(provider)`,
   `register_model_provider(provider)`, `ctx.providers` mapping, and
   the return-value fallback). **Registration never raises.**
2. The gateway's OpenAI-compatible wire surface
   (`POST /v1/chat/completions`, `GET /v1/models`, bearer auth).

If a future agent version changes its registry API, the plugin logs
which fallback shape it used; you will see model discovery still
working (it only touches the gateway). Re-pin the plugin commit when
upgrading the agent, per the risk-R3 policy in
`docs/HERMES_INTEGRATION.md`.

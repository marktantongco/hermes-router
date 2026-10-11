"""Hermes model-provider plugin: hermes-router gateway adapter (Stage 7).

Optional companion to the config-level ``custom_providers`` onboarding
(docs/HERMES_INTEGRATION.md). Adds what a bare custom_providers entry
cannot: native model discovery against the gateway's OpenAI-compatible
``GET /v1/models`` surface, so the agent's model picker shows the
gateway's routable models (with tier structure) instead of a single
hard-coded name.

Pinned contract (dossier Ch. 3.2, VERIFIED):
  * ``register(ctx)`` is the plugin entrypoint (plugin.yaml
    ``kind: model-provider``, ``entrypoint: provider:register``).
  * ``ctx.register_provider(...)`` publishes into the agent's provider
    registry. The audited call shapes are adapted defensively, in order:
    ``register_provider(name, provider)``, ``register_provider(provider)``,
    ``register_model_provider(provider)``, and a registry mapping
    ``ctx.providers[name] = provider``. Whichever the running agent
    exposes is used; none matching logs a warning and still returns the
    provider (some hosts consume the return value).
  * The provider surface the agent's model machinery consumes:
    ``name`` / ``base_url`` / ``api_key`` / ``default_model`` attributes,
    ``list_models()`` discovery, and ``create_client()`` wrapping the
    OpenAI client factory (SDK when importable, connection dict
    otherwise — the audited Claude-CLI-bridge pattern).

Design constraints (risk R3 — upstream contract drift):
  * FAIL-SOFT EVERYWHERE. A plugin must never break agent boot: an
    unreachable gateway, a missing env key, or an unrecognized registry
    API degrades to a warning, never an exception.
  * STDLIB ONLY. The plugin runs inside the agent process; it must not
    add dependencies to the host environment.
  * SECRETS STAY OUT OF LOGS. The api key never appears in ``repr``/``str``
    (Stage 1 redaction discipline, applied client-side).

Configuration (environment; the agent loads ~/.hermes/.env):
  HERMES_ROUTER_URL    gateway OpenAI root, default http://127.0.0.1:8766/v1
  HERMES_ROUTER_API_KEY  bearer token; a ``vk-`` virtual key is recommended
                       (per-subscriber attribution + blocking budgets,
                       Stage 4) over the all-powerful admin gateway key
  HERMES_ROUTER_MODEL  optional preferred model id (validated against
                       discovery when available)
  HERMES_ROUTER_TIMEOUT_SECONDS  discovery timeout, default 5.0
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

log = logging.getLogger("hermes.plugins.hermes-router-provider")

__all__ = ["HermesRouterProvider", "register", "build_provider"]

PROVIDER_NAME = "hermes-router"
DEFAULT_BASE_URL = "http://127.0.0.1:8766/v1"
DEFAULT_TIMEOUT_SECONDS = 5.0


class HermesRouterProvider:
    """Model-provider facade over one hermes-router gateway.

    Attributes the agent's model machinery reads directly; ``list_models``
    and ``model_details`` implement discovery; ``create_client`` wraps the
    OpenAI client factory pointed at the gateway.
    """

    def __init__(self, base_url: str, api_key: str = "",
                 default_model: str = "",
                 timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        self.name = PROVIDER_NAME
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.default_model = default_model
        self.timeout_seconds = timeout_seconds
        self._discovered_at: float = 0.0
        self._models_cache: list[dict[str, Any]] = []
        self.last_error: str = ""

    # ── Introspection (secrets-free) ────────────────────────────────────
    def __repr__(self) -> str:
        return (f"<HermesRouterProvider base_url={self.base_url!r} "
                f"api_key={'[set]' if self.api_key else '[unset]'} "
                f"default_model={self.default_model!r}>")

    def describe(self) -> dict[str, Any]:
        """Operator-facing summary (safe to log — no secrets)."""
        return {
            "name": self.name,
            "base_url": self.base_url,
            "api_key": "[set]" if self.api_key else "[unset]",
            "default_model": self.default_model,
            "models": [m.get("id") for m in self._models_cache],
            "last_error": self.last_error,
        }

    # ── Model discovery (GET /v1/models) ───────────────────────────────
    def _fetch_models(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}/models"
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(  # nosec B310 - operator-configured
                    req, timeout=self.timeout_seconds) as resp:
                # Operator-configured loopback/LAN gateway URL, not user input.
                payload = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, ValueError) as exc:
            self.last_error = f"model discovery failed: {exc}"
            log.warning("%s: %s (gateway unreachable is non-fatal)",
                        PROVIDER_NAME, self.last_error)
            return []
        self.last_error = ""
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            self.last_error = "model discovery: unexpected payload shape"
            log.warning("%s: %s", PROVIDER_NAME, self.last_error)
            return []
        entries = [e for e in data
                   if isinstance(e, dict) and isinstance(e.get("id"), str)]
        self._models_cache = entries
        return entries

    def model_details(self, refresh: bool = False) -> list[dict[str, Any]]:
        """Full discovery entries (OpenAI fields + hermes_router metadata)."""
        if refresh or not self._models_cache:
            self._fetch_models()
        return list(self._models_cache)

    def list_models(self) -> list[str]:
        """Model ids the gateway can route (native discovery contract).

        Empty list on any failure — fail-soft by design (risk R3): the
        agent keeps booting and retries on the next call.
        """
        return [m["id"] for m in self.model_details()]

    # ── Client factory (the audited wrapping pattern) ──────────────────
    def create_client(self) -> Any:
        """Return an OpenAI client pointed at the gateway when the SDK is
        importable, else a connection dict the host can build from.

        The dict keeps the plugin stdlib-only while still serving hosts
        that construct their own OpenAI client (the ``custom_providers``
        lane does exactly this with ``base_url`` + ``api_key``).
        """
        connection = {"base_url": self.base_url, "api_key": self.api_key}
        try:
            from openai import OpenAI  # type: ignore[import-not-found]
        except ImportError:
            return connection
        try:
            return OpenAI(base_url=self.base_url, api_key=self.api_key or "sk-noauth")
        except Exception as exc:  # client construction is host-dependent
            log.warning("%s: OpenAI client construction failed (%s); "
                        "returning connection dict", PROVIDER_NAME, exc)
            return connection

    # ── Resolution helper ──────────────────────────────────────────────
    def resolve_default_model(self) -> str:
        """Configured model, else first discovered id, else empty string.

        The gateway routes by request content, not by this field — but a
        sane default keeps agent-side model pickers truthful.
        """
        if self.default_model:
            return self.default_model
        models = self.list_models()
        return models[0] if models else ""


def build_provider(environ: dict[str, str] | None = None) -> HermesRouterProvider:
    """Construct the provider from the environment (pure; test-friendly)."""
    env = environ if environ is not None else dict(os.environ)
    raw_timeout = env.get("HERMES_ROUTER_TIMEOUT_SECONDS", "").strip()
    try:
        timeout = float(raw_timeout) if raw_timeout else DEFAULT_TIMEOUT_SECONDS
    except ValueError:
        log.warning("%s: ignoring non-numeric "
                    "HERMES_ROUTER_TIMEOUT_SECONDS=%r", PROVIDER_NAME, raw_timeout)
        timeout = DEFAULT_TIMEOUT_SECONDS
    return HermesRouterProvider(
        base_url=(env.get("HERMES_ROUTER_URL") or DEFAULT_BASE_URL).strip(),
        api_key=env.get("HERMES_ROUTER_API_KEY", "").strip(),
        default_model=env.get("HERMES_ROUTER_MODEL", "").strip(),
        timeout_seconds=timeout,
    )


# ── Registration (defensive adaptation, in observed-preference order) ────────
def _register_into_ctx(ctx: Any, provider: HermesRouterProvider) -> str:
    """Publish ``provider`` through whichever registry API ``ctx`` exposes.

    Returns a short tag naming the shape that succeeded ("register_provider",
    "register_provider(positional)", "register_model_provider",
    "providers-mapping", "return-value"). Never raises: unknown hosts log
    a warning and rely on the return value.
    """
    name = provider.name
    reg = getattr(ctx, "register_provider", None)
    if callable(reg):
        try:
            reg(name, provider)
            return "register_provider"
        except TypeError:
            # Single-positional shape observed in some agent versions.
            reg(provider)
            return "register_provider(positional)"
    reg_model = getattr(ctx, "register_model_provider", None)
    if callable(reg_model):
        reg_model(provider)
        return "register_model_provider"
    providers = getattr(ctx, "providers", None)
    if isinstance(providers, dict):
        providers[name] = provider
        return "providers-mapping"
    log.warning("%s: host ctx exposes no known provider-registry API; "
                "returning provider for hosts that consume the return value",
                name)
    return "return-value"


def register(ctx: Any) -> HermesRouterProvider:
    """Plugin entrypoint (plugin.yaml ``entrypoint: provider:register``).

    Builds the provider from the environment, performs one fail-soft
    discovery attempt so ``model_details`` is warm at boot, registers
    into the host registry, and always returns the provider.
    """
    provider = build_provider()
    try:
        provider.model_details(refresh=True)  # warm cache; failures non-fatal
        if provider.last_error:
            log.info("%s: discovery deferred (gateway not reachable yet)",
                     provider.name)
        shape = _register_into_ctx(ctx, provider)
        log.info("%s: registered via %s (%d models discovered)",
                 provider.name, shape, len(provider.list_models()))
    except Exception as exc:  # absolute fail-soft: boot must survive
        log.warning("%s: registration degraded: %s", provider.name, exc)
    return provider

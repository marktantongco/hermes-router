"""Stage 7 exit-gate suite: Hermes agent -> gateway -> (mock + live) upstream.

Dossier Ch.9 Table 7 row 7 — "End-to-end: Hermes -> gateway -> mock and
live local Ollama; tool calls preserved; streaming intact."

Layers (mirroring the Stage 6 compose-gate pattern: deterministic proof
in CI, live proof where the runtime exists):

  Part 1  GET /v1/models ingress contract — auth matrix (401 no/wrong
          token; admin + virtual key allowed), OpenAI list shape,
          dedupe, per-model tier metadata.
  Part 2  Plugin unit contracts — the pinned register(ctx) shapes
          (dossier Ch. 3.2), defensive adaptation, fail-soft discovery,
          secrets-free repr, env configuration, manifest contract.
  Part 3  Full e2e against the Ollama-shaped mock upstream — a
          Hermes-shaped client (the wire calls the agent's OpenAI
          client makes) drives discovery, content-based routing,
          tool-call round trips (non-stream AND stream), tool-result
          follow-ups, ledger attribution via a virtual key, and the
          plugin lane end-to-end.
  Part 4  Live local Ollama lane — auto-skipped unless
          HERMES_E2E_OLLAMA_URL (default http://127.0.0.1:11434/v1) is
          reachable; boots a second gateway against the real runtime
          and runs discovery + chat + streaming. The operator-side
          procedure is docs/HERMES_INTEGRATION.md §8/§10.
  Part 5  Documentation contract — the onboarding guide and the plugin
          package must exist and cover the required surfaces.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

# Reuse the runtime-file inventory from the capture harness so this
# suite inherits any future runtime additions automatically.
_spec = importlib.util.spec_from_file_location(
    "capture_golden", REPO / "tools" / "capture_golden.py")
_cg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cg)
RUNTIME_FILES = _cg.RUNTIME_FILES

# Plugin package under test (client-side; NOT a gateway runtime file).
_pspec = importlib.util.spec_from_file_location(
    "hermes_router_provider_plugin",
    REPO / "plugins" / "hermes-router-provider" / "provider.py")
_plugin = importlib.util.module_from_spec(_pspec)
_pspec.loader.exec_module(_plugin)

STUB_PORT = 9950
ROUTER_PORT = 9951
OLLAMA_GW_PORT = 9952
ROUTER_URL = f"http://127.0.0.1:{ROUTER_PORT}"
OLLAMA_GW_URL = f"http://127.0.0.1:{OLLAMA_GW_PORT}"
TEST_KEY = "hermes-e2e-key-7f3a"
ADMIN = {"Authorization": f"Bearer {TEST_KEY}"}
OLLAMA_BASE = os.environ.get("HERMES_E2E_OLLAMA_URL",
                             "http://127.0.0.1:11434/v1").rstrip("/")

TOOLS = [{
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get current weather for a city",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    },
}]


# ── Ollama-shaped mock upstream ──────────────────────────────────────────────

def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"


def build_ollama_stub_app() -> FastAPI:
    """Deterministic OpenAI-compatible upstream in the shape of Ollama's
    compatibility layer (/v1/models + /v1/chat/completions).

    Behavior contract (what the gateway forwards, what the agent sees):
      * classifier wire calls (model == stub-classifier) -> "simple"
      * last user text mentions "weather" + tools present -> tool_calls
      * a role:"tool" message is present -> final answer echoing it
      * anything else -> plain content answer
      * stream=true -> SSE frames (tool-call deltas or content deltas),
        a usage chunk, then data: [DONE]
    """
    app = FastAPI()

    @app.get("/v1/models")
    async def models() -> dict:
        return {"object": "list", "data": [
            {"id": m, "object": "model", "created": 1700000000,
             "owned_by": "ollama-stub"}
            for m in ("stub-simple", "stub-complex", "stub-fallback")
        ]}

    def _last_user_text(payload: dict) -> str:
        for m in reversed(payload.get("messages", [])):
            if m.get("role") == "user":
                content = m.get("content", "")
                if isinstance(content, list):
                    return " ".join(p.get("text", "") for p in content
                                    if isinstance(p, dict))
                return str(content)
        return ""

    def _has_tool_result(payload: dict) -> bool:
        return any(m.get("role") == "tool"
                   for m in payload.get("messages", []))

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        payload = await request.json()
        model = payload.get("model", "stub-unknown")

        # The gateway's classifier wire call (classification prompt).
        if model == "stub-classifier":
            return JSONResponse({
                "id": "chatcmpl-stub-classifier",
                "object": "chat.completion", "created": 1700000000,
                "model": model,
                "choices": [{"index": 0,
                             "message": {"role": "assistant",
                                         "content": "simple"},
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                          "total_tokens": 2},
            })

        wants_tool = (payload.get("tools")
                      and "weather" in _last_user_text(payload).lower()
                      and not _has_tool_result(payload))

        if payload.get("stream"):
            def gen():
                if wants_tool:
                    yield _sse({"id": "chatcmpl-stub-stream",
                                "object": "chat.completion.chunk",
                                "created": 1700000000, "model": model,
                                "choices": [{"index": 0, "finish_reason": None,
                                             "delta": {"role": "assistant",
                                                       "tool_calls": [{
                                                           "index": 0,
                                                           "id": "call_stub_weather_1",
                                                           "type": "function",
                                                           "function": {
                                                               "name": "get_weather",
                                                               "arguments": ""}}]}}]})
                    yield _sse({"id": "chatcmpl-stub-stream",
                                "object": "chat.completion.chunk",
                                "created": 1700000000, "model": model,
                                "choices": [{"index": 0, "finish_reason": None,
                                             "delta": {"tool_calls": [{
                                                 "index": 0,
                                                 "function": {
                                                     "arguments": "{\"city\": \"Manila\"}"}}]}}]})
                    yield _sse({"id": "chatcmpl-stub-stream",
                                "object": "chat.completion.chunk",
                                "created": 1700000000, "model": model,
                                "choices": [{"index": 0,
                                             "finish_reason": "tool_calls",
                                             "delta": {}}]})
                else:
                    for text in ("The", " weather", " report."):
                        yield _sse({"id": "chatcmpl-stub-stream",
                                    "object": "chat.completion.chunk",
                                    "created": 1700000000, "model": model,
                                    "choices": [{"index": 0,
                                                 "finish_reason": None,
                                                 "delta": {"content": text}}]})
                    yield _sse({"id": "chatcmpl-stub-stream",
                                "object": "chat.completion.chunk",
                                "created": 1700000000, "model": model,
                                "choices": [{"index": 0,
                                             "finish_reason": "stop",
                                             "delta": {}}]})
                yield _sse({"id": "chatcmpl-stub-stream",
                            "object": "chat.completion.chunk",
                            "created": 1700000000, "model": model,
                            "choices": [],
                            "usage": {"prompt_tokens": 9,
                                      "completion_tokens": 7,
                                      "total_tokens": 16}})
                yield "data: [DONE]\n\n"
            return StreamingResponse(gen(), media_type="text/event-stream")

        if wants_tool:
            return JSONResponse({
                "id": "chatcmpl-stub-toolcall",
                "object": "chat.completion", "created": 1700000000,
                "model": model,
                "choices": [{"index": 0,
                             "message": {"role": "assistant",
                                         "content": None,
                                         "tool_calls": [{
                                             "id": "call_stub_weather_1",
                                             "type": "function",
                                             "function": {
                                                 "name": "get_weather",
                                                 "arguments": json.dumps(
                                                     {"city": "Manila"})}}]},
                             "finish_reason": "tool_calls"}],
                "usage": {"prompt_tokens": 9, "completion_tokens": 7,
                          "total_tokens": 16},
            })

        if _has_tool_result(payload):
            tool_texts = [str(m.get("content", ""))
                          for m in payload["messages"]
                          if m.get("role") == "tool"]
            return JSONResponse({
                "id": "chatcmpl-stub-final",
                "object": "chat.completion", "created": 1700000000,
                "model": model,
                "choices": [{"index": 0,
                             "message": {"role": "assistant",
                                         "content":
                                             f"Tool says: {tool_texts[0]}"},
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 8,
                          "total_tokens": 20},
            })

        return JSONResponse({
            "id": "chatcmpl-stub-plain",
            "object": "chat.completion", "created": 1700000000,
            "model": model,
            "choices": [{"index": 0,
                         "message": {"role": "assistant",
                                     "content": "Hello from the stub."},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 6,
                      "total_tokens": 11},
        })

    return app


# ── Harness: config, boot, teardown ─────────────────────────────────────────

def _base_config(port: int, chat_base_url: str, chat_model: str) -> dict:
    """Gateway config: auth mandatory, ledger on, cost guard on, two
    categories. The classifier points at the stub so content-based
    routing (what a real Hermes agent relies on — no /use: overrides)
    is exercised end-to-end."""
    return {
        "server": {"host": "127.0.0.1", "port": port},
        "auth": {"mode": "required", "api_key_env": "E2E_TEST_KEY"},
        "ledger": {"enabled": True, "db_path": "ledger.sqlite3"},
        "pricing": {
            "default": {"input_per_mtok": 0.5, "output_per_mtok": 1.5},
            "entries": {
                "stub-simple": {"input_per_mtok": 0.0,
                                "output_per_mtok": 0.0, "free": True},
                "stub-complex": {"input_per_mtok": 1.25,
                                 "output_per_mtok": 5.0},
                "stub-fallback": {"input_per_mtok": 0.0,
                                  "output_per_mtok": 0.0, "free": True}}},
        "cost_guard": {"enabled": True, "estimate_chars_per_token": 4,
                       "watchdog": {"statuses": [401, 402]}},
        "classifier": {
            "model": "stub-classifier",
            "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
            "api_key_env": "",
            "session_timeout_minutes": 5,
            "system_prompt": (
                "Classify the user's message into exactly one of these "
                "categories:\n{categories}\n\nReply with ONLY the category "
                "name, nothing else.\n\n\"{message}\" →"),
            "profile_hint": "No profile available.",
            "zero_shot": {"enabled": False},
            "fasttext": {"enabled": False},
            "setfit": {"enabled": False}},
        "routing": {"override_prefix": "/use:",
                    "escalation_keywords": [],
                    "de_escalation_keywords": []},
        # persona files intentionally absent: the classifier's profile
        # hint degrades to "No profile available." (the lazy-extraction
        # path) without any extra upstream calls
        "persona": {"user_path": "/var/empty/hermes-e2e/USER.md",
                    "memory_path": "/var/empty/hermes-e2e/MEMORY.md",
                    "max_context_chars": 800},
        "categories": {
            "simple": {"label": "Simple", "model": "stub-simple",
                       "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
                       "api_key_env": "", "timeout_seconds": 15,
                       "fallback_model": "stub-fallback",
                       "fallback_base_url":
                           f"http://127.0.0.1:{STUB_PORT}/v1",
                       "fallback_key_env": ""},
            "complex": {"label": "Complex", "model": "stub-complex",
                        "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
                        "api_key_env": "", "timeout_seconds": 15},
        },
    }


def _start_router(config: dict, port: int):
    tmp = Path(tempfile.mkdtemp(prefix="hermes-e2e-"))
    for fname in RUNTIME_FILES:
        shutil.copy2(REPO / fname, tmp / fname)
    (tmp / "router_config.yaml").write_text(yaml.safe_dump(config))
    env = {k: v for k, v in os.environ.items()
           if k.startswith(("LC_", "LANG_", "PATH"))}
    env.update({"PYTHONUNBUFFERED": "1", "HOME": str(tmp),
                "TRACE_LOG_DIR": str(tmp / "traces"),
                "E2E_TEST_KEY": TEST_KEY})
    log = open(tmp / "router.log", "w")
    proc = subprocess.Popen([sys.executable, str(tmp / "server.py")],
                            cwd=tmp, env=env, stdout=log, stderr=log)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=2)
            return proc, tmp, log
        except Exception:
            if proc.poll() is not None:
                log.flush()
                raise AssertionError(
                    f"router died at boot:\n{(tmp / 'router.log').read_text()}")
            time.sleep(0.2)
    raise AssertionError("router did not become healthy in 30s")


def _stop_router(proc, tmp, log) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
    log.close()
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="module")
def stub():
    server = uvicorn.Server(uvicorn.Config(
        build_ollama_stub_app(), host="127.0.0.1", port=STUB_PORT,
        log_level="error", access_log=False))
    threading.Thread(target=server.run, daemon=True).start()
    _wait_http(f"http://127.0.0.1:{STUB_PORT}/v1/models")
    yield
    server.should_exit = True
    time.sleep(0.3)


def _wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            httpx.get(url, timeout=2)
            return
        except Exception:
            time.sleep(0.2)


@pytest.fixture(scope="module")
def router(stub):
    proc, tmp, log = _start_router(_base_config(ROUTER_PORT, "", ""),
                                   ROUTER_PORT)
    yield ROUTER_URL
    _stop_router(proc, tmp, log)


@pytest.fixture(scope="module")
def vk(router) -> dict:
    """A virtual key minted exactly the way the onboarding guide does —
    the agent's lane (attribution + budgets) rather than the admin key."""
    r = httpx.post(f"{ROUTER_URL}/admin/virtual_keys", headers=ADMIN,
                   json={"name": "hermes-agent",
                         "budget_mode": "cost_micros",
                         "budget_limit": 10_000_000,
                         "budget_period": "month"}, timeout=10)
    assert r.status_code == 201, r.text
    body = r.json()
    return {"id": body["id"], "token": body["key"]}


# ── Hermes-shaped client (the wire calls the agent's OpenAI client makes) ───

def _sse_events(resp) -> tuple[list[str], list[dict | None]]:
    """Parse an SSE transcript: (raw non-empty lines, parsed events with
    None for the [DONE] sentinel)."""
    lines = [ln for ln in resp.text.splitlines() if ln.strip()]
    events: list[dict | None] = []
    for ln in lines:
        assert ln.startswith("data: "), f"non-SSE line: {ln!r}"
        body = ln[len("data: "):]
        events.append(None if body == "[DONE]" else json.loads(body))
    return lines, events


def hermes_models(token: str, base: str = ROUTER_URL) -> dict:
    r = httpx.get(f"{base}/v1/models",
                  headers={"Authorization": f"Bearer {token}"}, timeout=10)
    assert r.status_code == 200, r.text
    return r.json()


def hermes_chat(token: str, messages: list[dict], *, tools: list | None = None,
                stream: bool = False, model: str = "stub-simple",
                base: str = ROUTER_URL):
    payload: dict[str, Any] = {"model": model, "messages": messages,
                               "stream": stream}
    if tools:
        payload["tools"] = tools
    return httpx.post(f"{base}/v1/chat/completions",
                      headers={"Authorization": f"Bearer {token}"},
                      json=payload, timeout=120)


# ── Part 1: GET /v1/models ingress contract ─────────────────────────────────

def test_models_rejects_missing_and_wrong_tokens(router):
    assert httpx.get(f"{ROUTER_URL}/v1/models", timeout=10).status_code == 401
    r = httpx.get(f"{ROUTER_URL}/v1/models",
                  headers={"Authorization": "Bearer wrong-key"}, timeout=10)
    assert r.status_code == 401


def test_models_openai_shape_and_dedupe(router):
    body = hermes_models(TEST_KEY)
    assert body["object"] == "list"
    ids = [m["id"] for m in body["data"]]
    assert ids == sorted(ids), "deterministic ordering (sorted)"
    assert len(ids) == len(set(ids)), "deduplicated by model id"
    # primary + fallback hops across the two categories
    assert set(ids) == {"stub-simple", "stub-complex", "stub-fallback"}
    for m in body["data"]:
        assert m["object"] == "model"
        assert isinstance(m["created"], int)
        assert m["owned_by"] in ("openai", "anthropic")
        assert m["hermes_router"]["categories"]
        assert m["hermes_router"]["roles"]


def test_models_tier_metadata(router):
    body = hermes_models(TEST_KEY)
    by_id = {m["id"]: m for m in body["data"]}
    assert by_id["stub-simple"]["hermes_router"] == {
        "categories": ["simple"], "roles": ["primary"]}
    assert by_id["stub-complex"]["hermes_router"] == {
        "categories": ["complex"], "roles": ["primary"]}
    assert by_id["stub-fallback"]["hermes_router"] == {
        "categories": ["simple"], "roles": ["fallback"]}


def test_models_allowed_for_virtual_keys(router, vk):
    """Inference surface: the discovery call an agent's client makes."""
    body = hermes_models(vk["token"])
    assert {m["id"] for m in body["data"]} == {
        "stub-simple", "stub-complex", "stub-fallback"}


# ── Part 2: plugin unit contracts ───────────────────────────────────────────

class TwoArgCtx:
    """Documented shape: ctx.register_provider(name, provider)."""

    def __init__(self) -> None:
        self.registered: list[tuple[str, Any]] = []

    def register_provider(self, name: str, provider: Any) -> None:
        self.registered.append((name, provider))


class PositionalCtx:
    """Observed single-positional variant (risk R3 adaptation)."""

    def __init__(self) -> None:
        self.registered: list[Any] = []

    def register_provider(self, provider: Any) -> None:
        self.registered.append(provider)


class ModelProviderNameCtx:
    """Alternate registry method name."""

    def __init__(self) -> None:
        self.registered: list[Any] = []

    def register_model_provider(self, provider: Any) -> None:
        self.registered.append(provider)


class MappingCtx:
    """Registry-as-mapping host."""

    def __init__(self) -> None:
        self.providers: dict[str, Any] = {}


class BareCtx:
    """No registry API at all — the return-value lane."""


def _plugin_env(base_url: str = "", api_key: str = "vk-test-token",
                model: str = "") -> dict[str, str]:
    env = {"HERMES_ROUTER_API_KEY": api_key}
    if base_url:
        env["HERMES_ROUTER_URL"] = base_url
    if model:
        env["HERMES_ROUTER_MODEL"] = model
    return env


def test_plugin_registers_via_documented_two_arg_shape():
    ctx = TwoArgCtx()
    provider = _plugin.register(ctx)
    assert ctx.registered and ctx.registered[0][0] == "hermes-router"
    assert ctx.registered[0][1] is provider


def test_plugin_adapts_to_positional_shape():
    ctx = PositionalCtx()
    provider = _plugin.register(ctx)
    assert ctx.registered == [provider]


def test_plugin_adapts_to_register_model_provider_shape():
    ctx = ModelProviderNameCtx()
    provider = _plugin.register(ctx)
    assert ctx.registered == [provider]


def test_plugin_adapts_to_providers_mapping_shape():
    ctx = MappingCtx()
    provider = _plugin.register(ctx)
    assert ctx.providers["hermes-router"] is provider


def test_plugin_never_raises_without_registry_api():
    provider = _plugin.register(BareCtx())  # must not raise
    assert provider.name == "hermes-router"


def test_plugin_env_configuration():
    provider = _plugin.build_provider(
        {"HERMES_ROUTER_URL": "http://gw.example:9000/v1",
         "HERMES_ROUTER_API_KEY": "vk-xyz", "HERMES_ROUTER_MODEL": "m1",
         "HERMES_ROUTER_TIMEOUT_SECONDS": "2.5"})
    assert provider.base_url == "http://gw.example:9000/v1"
    assert provider.api_key == "vk-xyz"
    assert provider.default_model == "m1"
    assert provider.timeout_seconds == 2.5
    # garbage timeout falls back to the default, never raises
    provider = _plugin.build_provider(
        {"HERMES_ROUTER_TIMEOUT_SECONDS": "not-a-number"})
    assert provider.timeout_seconds == _plugin.DEFAULT_TIMEOUT_SECONDS


def test_plugin_secrets_free_introspection():
    provider = _plugin.build_provider(_plugin_env(api_key="vk-super-secret"))
    assert "vk-super-secret" not in repr(provider)
    assert "vk-super-secret" not in str(provider.describe())
    assert provider.describe()["api_key"] == "[set]"


def test_plugin_fail_soft_when_gateway_unreachable():
    provider = _plugin.HermesRouterProvider(
        base_url="http://127.0.0.1:59999/v1",  # nothing listens here
        api_key="vk-x", timeout_seconds=0.5)
    assert provider.list_models() == []          # fail-soft, no raise
    assert provider.last_error
    assert provider.resolve_default_model() == ""


def test_plugin_discovery_parses_gateway_payload(router, vk):
    """Against the LIVE gateway: the discovery the agent's picker gets."""
    provider = _plugin.HermesRouterProvider(
        base_url=f"{ROUTER_URL}/v1", api_key=vk["token"],
        timeout_seconds=5.0)
    ids = provider.list_models()
    assert set(ids) == {"stub-simple", "stub-complex", "stub-fallback"}
    details = provider.model_details()
    by_id = {m["id"]: m for m in details}
    assert by_id["stub-simple"]["hermes_router"]["roles"] == ["primary"]
    # a second call is served warm without a new fetch (cache path)
    assert provider.list_models() == ids


def test_plugin_manifest_contract():
    manifest = yaml.safe_load(
        (REPO / "plugins" / "hermes-router-provider" / "plugin.yaml")
        .read_text())
    assert manifest["kind"] == "model-provider"
    assert manifest["name"] == "hermes-router-provider"
    module, _, fn = manifest["entrypoint"].partition(":")
    assert (REPO / "plugins" / "hermes-router-provider"
            / f"{module}.py").exists()
    assert hasattr(_plugin, fn), "entrypoint function must exist"


# ── Part 3: full e2e — Hermes-shaped client through the gateway ─────────────

def test_hermes_plain_chat_routes_by_content(router, vk):
    """No /use: overrides — the agent relies on content-based routing
    exactly like a stock custom_providers integration."""
    r = hermes_chat(vk["token"],
                    [{"role": "user", "content": "hello there"}])
    assert r.status_code == 200, r.text
    body = r.json()
    # the classifier stub answers "simple" -> stub-simple tier upstream
    assert body["model"] == "stub-simple"
    assert body["choices"][0]["message"]["content"] == "Hello from the stub."


def test_hermes_tool_call_round_trip(router, vk):
    """The exit-gate core: tools preserved through the gateway, the
    tool result follow-up answered, and the traffic ledger-attributed."""
    messages = [{"role": "user",
                 "content": "What is the weather in Manila?"}]

    # 1) assistant requests a tool call
    r = hermes_chat(vk["token"], messages, tools=TOOLS)
    assert r.status_code == 200, r.text
    msg = r.json()["choices"][0]["message"]
    assert r.json()["choices"][0]["finish_reason"] == "tool_calls"
    assert msg["tool_calls"], "tool_calls must survive the gateway hop"
    call = msg["tool_calls"][0]
    assert call["type"] == "function"
    assert call["function"]["name"] == "get_weather"
    assert json.loads(call["function"]["arguments"]) == {"city": "Manila"}
    assert call["id"], "tool_call_id must round trip"

    # 2) the agent executes the tool and posts the result back
    messages += [
        {"role": "assistant", "content": None, "tool_calls": [call]},
        {"role": "tool", "tool_call_id": call["id"],
         "content": "31C, sunny"},
    ]
    r = hermes_chat(vk["token"], messages, tools=TOOLS)
    assert r.status_code == 200, r.text
    content = r.json()["choices"][0]["message"]["content"]
    assert "31C, sunny" in content, "tool result must reach the upstream"

    # 3) attribution: the virtual key's lane landed in the ledger
    r = httpx.get(f"{ROUTER_URL}/admin/ledger/records",
                  params={"virtual_key_id": vk["id"]},
                  headers=ADMIN, timeout=10)
    assert r.status_code == 200
    records = r.json()["records"]
    assert records, "virtual-key traffic must be ledger-attributed"
    assert all(rec["virtual_key_id"] == vk["id"] for rec in records)


def test_hermes_streaming_intact_plain(router, vk):
    r = hermes_chat(vk["token"],
                    [{"role": "user", "content": "tell me a story"}],
                    stream=True)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    lines, events = _sse_events(r)
    assert lines, "SSE transcript must not be empty"
    assert events[-1] is None, "stream must terminate with [DONE]"
    content = "".join(
        e["choices"][0]["delta"].get("content", "")
        for e in events if e is not None and e.get("choices"))
    assert content == "The weather report."
    finishes = [e["choices"][0]["finish_reason"] for e in events
                if e is not None and e.get("choices")]
    assert "stop" in finishes
    # usage frame present (the ledger's streaming sniffer feed)
    usage_frames = [e for e in events
                    if e is not None and e.get("usage") and not e.get("choices")]
    assert usage_frames and usage_frames[0]["usage"]["total_tokens"] == 16


def test_hermes_streaming_tool_calls_preserved(router, vk):
    """Tool-call deltas reassembled from the stream == the non-stream
    contract (id, name, arguments identical)."""
    r = hermes_chat(vk["token"],
                    [{"role": "user",
                      "content": "What is the weather in Manila?"}],
                    tools=TOOLS, stream=True)
    assert r.status_code == 200
    lines, events = _sse_events(r)
    assert events[-1] is None, "[DONE] terminator"

    tool_id, name, args = "", "", ""
    for e in events:
        if e is None or not e.get("choices"):
            continue
        delta = e["choices"][0].get("delta", {})
        for tc in delta.get("tool_calls", []) or []:
            if tc.get("id"):
                tool_id = tc["id"]
            fn = tc.get("function", {})
            name += fn.get("name", "")
            args += fn.get("arguments", "")
    assert tool_id == "call_stub_weather_1"
    assert name == "get_weather"
    assert json.loads(args) == {"city": "Manila"}
    finishes = [e["choices"][0]["finish_reason"] for e in events
                if e is not None and e.get("choices")]
    assert "tool_calls" in finishes


def test_plugin_lane_end_to_end(router, vk):
    """The Path-B lane: mock agent ctx (documented contract) -> plugin
    registration -> discovery -> client factory -> a full tool-call chat
    through the produced connection — proving the plugin delivers the
    same wire behavior as the custom_providers lane."""
    ctx = TwoArgCtx()
    env = {"HERMES_ROUTER_URL": f"{ROUTER_URL}/v1",
           "HERMES_ROUTER_API_KEY": vk["token"],
           "HERMES_ROUTER_TIMEOUT_SECONDS": "5"}
    provider = _plugin.build_provider(env)
    assert provider.model_details(refresh=True), "discovery against live gw"

    # registration through the documented shape
    shape = _plugin._register_into_ctx(ctx, provider)
    assert shape == "register_provider"
    assert ctx.registered[0][1] is provider

    # client factory: SDK client when importable, connection dict otherwise
    client = provider.create_client()
    if isinstance(client, dict):
        assert client["base_url"] == f"{ROUTER_URL}/v1"
        assert client["api_key"] == vk["token"]
        base, key = client["base_url"], client["api_key"]
    else:  # openai.OpenAI instance
        base = str(getattr(client, "base_url", "")).rstrip("/")
        key = client.api_key
        assert base.endswith(f":{ROUTER_PORT}/v1")

    # full tool-call round trip through the produced connection
    r = httpx.post(f"{base}/chat/completions",
                   headers={"Authorization": f"Bearer {key}"},
                   json={"model": provider.resolve_default_model(),
                         "messages": [{"role": "user",
                                       "content":
                                           "What is the weather in Manila?"}],
                         "tools": TOOLS}, timeout=30)
    assert r.status_code == 200, r.text
    call = r.json()["choices"][0]["message"]["tool_calls"][0]
    assert call["function"]["name"] == "get_weather"


# ── Part 4: live local Ollama lane (auto-skip when unreachable) ──────────────

def _ollama_models(base: str) -> list[str] | None:
    try:
        r = httpx.get(f"{base}/models", timeout=3.0)
        if r.status_code == 200:
            data = r.json().get("data", [])
            return [m["id"] for m in data if isinstance(m, dict) and m.get("id")]
    except Exception:
        return None
    return None


_OLLAMA_IDS = _ollama_models(OLLAMA_BASE) if OLLAMA_BASE else None
needs_ollama = pytest.mark.skipif(
    not _OLLAMA_IDS,
    reason=f"no live local Ollama at {OLLAMA_BASE} "
           "(set HERMES_E2E_OLLAMA_URL; CI proves the lane against the "
           "Ollama-shaped mock — see docs/HERMES_INTEGRATION.md §10)")


@pytest.fixture(scope="module")
def ollama_router():
    if not _OLLAMA_IDS:
        pytest.skip("no live local Ollama")
    cfg = {
        "server": {"host": "127.0.0.1", "port": OLLAMA_GW_PORT},
        "auth": {"mode": "required", "api_key_env": "E2E_TEST_KEY"},
        "ledger": {"enabled": True, "db_path": "ledger.sqlite3"},
        "pricing": {"default": {"input_per_mtok": 0.0,
                                "output_per_mtok": 0.0,
                                "free": True}},
        "cost_guard": {"enabled": True, "estimate_chars_per_token": 4,
                       "watchdog": {"statuses": [401, 402]}},
        "classifier": {
            "model": "stub-classifier",
            "base_url": f"http://127.0.0.1:{STUB_PORT}/v1",
            "api_key_env": "", "session_timeout_minutes": 5,
            "system_prompt": (
                "Classify the user's message into exactly one of these "
                "categories:\n{categories}\n\nReply with ONLY the category "
                "name, nothing else.\n\n\"{message}\" →"),
            "profile_hint": "No profile available.",
            "zero_shot": {"enabled": False},
            "fasttext": {"enabled": False},
            "setfit": {"enabled": False}},
        "routing": {"override_prefix": "/use:",
                    "escalation_keywords": [],
                    "de_escalation_keywords": []},
        "persona": {"user_path": "/var/empty/hermes-e2e/USER.md",
                    "memory_path": "/var/empty/hermes-e2e/MEMORY.md",
                    "max_context_chars": 800},
        "categories": {
            "chat": {"label": "Local", "model": _OLLAMA_IDS[0],
                     "base_url": OLLAMA_BASE,
                     "api_key_env": "", "timeout_seconds": 120}},
    }
    proc, tmp, log = _start_router(cfg, OLLAMA_GW_PORT)
    yield OLLAMA_GW_URL
    _stop_router(proc, tmp, log)


@needs_ollama
def test_live_ollama_discovery_through_gateway(ollama_router):
    body = hermes_models(TEST_KEY, base=ollama_router)
    ids = {m["id"] for m in body["data"]}
    assert _OLLAMA_IDS[0] in ids, "gateway must list the live local model"


@needs_ollama
def test_live_ollama_nonstream_chat(ollama_router):
    r = hermes_chat(TEST_KEY, [{"role": "user", "content": "Say OK."}],
                    model=_OLLAMA_IDS[0], base=ollama_router)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["model"] == _OLLAMA_IDS[0]
    msg = body["choices"][0]["message"]
    assert isinstance(msg.get("content"), str) or msg.get("tool_calls")


@needs_ollama
def test_live_ollama_streaming_chat(ollama_router):
    r = hermes_chat(TEST_KEY, [{"role": "user", "content": "Say OK."}],
                    model=_OLLAMA_IDS[0], stream=True, base=ollama_router)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    lines, events = _sse_events(r)
    assert events[-1] is None, "stream must terminate with [DONE]"
    assert any(e is not None for e in events), "at least one frame"


@needs_ollama
def test_live_ollama_tools_wire_preserved(ollama_router):
    """Tools are forwarded verbatim (OpenAI-protocol passthrough); whether
    the local model chooses to call one is model behavior — the strict
    preservation proof is the mock lane above."""
    r = hermes_chat(TEST_KEY,
                    [{"role": "user", "content": "What is the weather?"}],
                    tools=TOOLS, model=_OLLAMA_IDS[0], base=ollama_router)
    assert r.status_code == 200, r.text
    msg = r.json()["choices"][0]["message"]
    assert isinstance(msg.get("content"), str) or msg.get("tool_calls")


# ── Part 5: documentation + packaging contract ───────────────────────────────

def test_integration_guide_covers_required_sections():
    guide = REPO / "docs" / "HERMES_INTEGRATION.md"
    assert guide.exists(), "docs/HERMES_INTEGRATION.md is the Stage 7 deliverable"
    text = guide.read_text()
    for required in ("custom_providers", "plugin", "streaming", "tool",
                     "Ollama", "virtual key", "Troubleshooting",
                     "/v1/models", "verification"):
        assert required.lower() in text.lower(), f"guide must cover: {required}"


def test_plugin_package_files_present():
    pkg = REPO / "plugins" / "hermes-router-provider"
    for fname in ("plugin.yaml", "provider.py", "README.md"):
        assert (pkg / fname).exists(), f"plugin package missing {fname}"


def test_readme_documents_stage7_surfaces():
    text = (REPO / "README.md").read_text()
    assert "/v1/models" in text, "README endpoint table must list /v1/models"
    assert "HERMES_INTEGRATION" in text, "README must point at the guide"

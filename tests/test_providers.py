#!/usr/bin/env python3
"""Stage 2 provider abstraction — per-adapter contract tests (mocked).

Pins the translation contracts documented in providers.py:
  - registry resolution is fail-closed (unknown provider never dispatches)
  - openai adapter stays byte-compatible with the pre-Stage-2 behavior
  - anthropic adapter request/response/SSE translation edge cases
"""
from __future__ import annotations

import os
import json

# Pin deterministic timestamps BEFORE providers is imported (it reads the
# env at module import; pytest may import server transitively elsewhere).
os.environ.setdefault("HERMES_ROUTER_FIXED_EPOCH", "1727000000")

import pytest  # noqa: E402

from providers import (  # noqa: E402
    PROVIDER_REGISTRY,
    AdapterError,
    AnthropicAdapter,
    AnthropicStreamTranslator,
    OpenAICompatAdapter,
    get_provider,
    provider_name,
    validate_provider_config,
)

EPOCH = 1727000000


# ── Registry ────────────────────────────────────────────────────────────────

class TestRegistry:
    def test_known_providers_resolve(self):
        assert get_provider({}).name == "openai"          # default
        assert get_provider({"provider": "openai"}).name == "openai"
        assert get_provider({"provider": "anthropic"}).name == "anthropic"

    def test_aliases_resolve_to_openai(self):
        for alias in ("ollama", "lmstudio", "vllm", "llamacpp",
                      "openai_compat", "openai-compatible"):
            assert get_provider({"provider": alias}).name == "openai", alias

    def test_unknown_provider_fails_closed(self):
        with pytest.raises(AdapterError):
            get_provider({"provider": "definitely-not-a-provider"})

    def test_unknown_provider_name_never_raises(self):
        assert provider_name({"provider": "definitely-not-a-provider"}) \
            == "definitely-not-a-provider"

    def test_registry_metadata_complete(self):
        for name, adapter in PROVIDER_REGISTRY.items():
            meta = adapter.metadata()
            assert meta["protocol"], name
            assert isinstance(meta["streaming"], bool), name
            assert isinstance(meta["tools"], bool), name
            assert meta["auth"], name

    def test_validate_config_accepts_valid(self):
        cfg = {"categories": {
            "a": {"provider": "anthropic", "fallback_provider": "openai"},
            "b": {"model": "m"},                                # default ok
            "c": {"provider": "ollama"},                        # alias ok
        }}
        assert validate_provider_config(cfg) == []

    def test_validate_config_flags_unknown(self):
        cfg = {"categories": {
            "a": {"provider": "bogus"},
            "b": {"fallback_provider": "bogus2"},
            "c": {"fallback2_provider": "bogus3"},
        }}
        errors = validate_provider_config(cfg)
        assert len(errors) == 3
        assert all("unknown" in e for e in errors)

    def test_validate_config_ignores_empty(self):
        assert validate_provider_config({"categories": {"a": {"provider": ""}}}) == []


# ── OpenAI-compatible adapter (passthrough contract) ────────────────────────

class TestOpenAICompatAdapter:
    def setup_method(self):
        self.adapter = OpenAICompatAdapter()

    def test_build_url(self):
        assert self.adapter.build_url({"base_url": "http://x:1/v1/"}) \
            == "http://x:1/v1/chat/completions"

    def test_build_headers_bearer_and_anon(self):
        assert self.adapter.build_headers("sk-test") == {
            "Content-Type": "application/json",
            "Authorization": "Bearer sk-test",
        }
        assert self.adapter.build_headers("") == {"Content-Type": "application/json"}

    def test_build_request_swaps_model_only(self):
        payload = {"model": "auto", "messages": [{"role": "user", "content": "hi"}],
                   "temperature": 0.5}
        cfg = {"base_url": "http://x/v1", "model": "stub-simple"}
        built = self.adapter.build_request(cfg, payload, "")
        assert built.json == {**payload, "model": "stub-simple"}
        assert built.url == "http://x/v1/chat/completions"

    def test_translate_response_identity(self):
        body = {"id": "x", "choices": []}
        assert self.adapter.translate_response(body, {}, {}) is body

    def test_stream_translator_byte_exact_passthrough(self):
        tr = self.adapter.open_stream_translator({}, {})
        assert tr.feed("data: {\"a\":1}") == [b'data: {"a":1}\n']
        assert tr.feed("") == [b"\n"]
        assert tr.feed("data: [DONE]") == [b"data: [DONE]\n"]


# ── Anthropic adapter: request translation ──────────────────────────────────

def _anth_cfg() -> dict:
    return {"base_url": "https://api.anthropic.com", "model": "claude-stub"}


class TestAnthropicRequest:
    def setup_method(self):
        self.adapter = AnthropicAdapter()

    def test_build_url_and_headers(self):
        assert self.adapter.build_url({"base_url": "https://api.anthropic.com/"}) \
            == "https://api.anthropic.com/v1/messages"
        headers = self.adapter.build_headers("k")
        assert headers["x-api-key"] == "k"
        assert headers["anthropic-version"] == "2023-06-01"
        assert "Authorization" not in headers
        assert "x-api-key" not in self.adapter.build_headers("")

    def test_system_hoisted_and_messages_mapped(self):
        payload = {"messages": [
            {"role": "system", "content": "be terse"},
            {"role": "user", "content": "hello"},
        ]}
        out = self.adapter.translate_request(payload, _anth_cfg())
        assert out["system"] == "be terse"
        assert out["messages"] == [
            {"role": "user", "content": [{"type": "text", "text": "hello"}]},
        ]
        assert out["model"] == "claude-stub"

    def test_multiple_system_messages_joined(self):
        payload = {"messages": [
            {"role": "system", "content": "one"},
            {"role": "user", "content": "q"},
            {"role": "system", "content": "two"},
        ]}
        out = self.adapter.translate_request(payload, _anth_cfg())
        assert out["system"] == "one\n\ntwo"

    def test_max_tokens_precedence(self):
        base = {"messages": [{"role": "user", "content": "x"}]}
        assert self.adapter.translate_request(dict(base, max_tokens=99), _anth_cfg())["max_tokens"] == 99
        assert self.adapter.translate_request(base, dict(_anth_cfg(), max_tokens_default=77))["max_tokens"] == 77
        assert self.adapter.translate_request(base, _anth_cfg())["max_tokens"] == 4096

    def test_sampling_and_stop_translation(self):
        payload = {"messages": [{"role": "user", "content": "x"}],
                   "temperature": 0.2, "top_p": 0.9, "stop": ["END", "STOP"]}
        out = self.adapter.translate_request(payload, _anth_cfg())
        assert out["temperature"] == 0.2 and out["top_p"] == 0.9
        assert out["stop_sequences"] == ["END", "STOP"]
        payload["stop"] = "END"
        assert self.adapter.translate_request(payload, _anth_cfg())["stop_sequences"] == ["END"]

    def test_stream_flag_passthrough(self):
        out = self.adapter.translate_request(
            {"messages": [{"role": "user", "content": "x"}], "stream": True}, _anth_cfg())
        assert out["stream"] is True

    def test_tools_and_tool_choice_variants(self):
        tools = [{"type": "function", "function": {
            "name": "get_weather", "description": "w",
            "parameters": {"type": "object", "properties": {}}}}]
        base = {"messages": [{"role": "user", "content": "x"}], "tools": tools}

        out = self.adapter.translate_request(dict(base, tool_choice="auto"), _anth_cfg())
        assert out["tools"][0]["name"] == "get_weather"
        assert out["tools"][0]["input_schema"] == {"type": "object", "properties": {}}
        assert out["tool_choice"] == {"type": "auto"}

        assert self.adapter.translate_request(
            dict(base, tool_choice="required"), _anth_cfg())["tool_choice"] == {"type": "any"}
        assert self.adapter.translate_request(
            dict(base, tool_choice={"type": "function", "function": {"name": "get_weather"}}),
            _anth_cfg())["tool_choice"] == {"type": "tool", "name": "get_weather"}

        out = self.adapter.translate_request(dict(base, tool_choice="none"), _anth_cfg())
        assert "tools" not in out and "tool_choice" not in out

    def test_images_translated(self):
        payload = {"messages": [{"role": "user", "content": [
            {"type": "text", "text": "look"},
            {"type": "image_url", "image_url": {"url":
                "data:image/png;base64,AAAA"}},
            {"type": "image_url", "image_url": {"url": "https://x/y.png"}},
            {"type": "image_url", "image_url": {"url": "ftp://bad"}},
        ]}]}
        blocks = self.adapter.translate_request(payload, _anth_cfg())["messages"][0]["content"]
        assert blocks[0] == {"type": "text", "text": "look"}
        assert blocks[1] == {"type": "image", "source": {
            "type": "base64", "media_type": "image/png", "data": "AAAA"}}
        assert blocks[2] == {"type": "image", "source": {
            "type": "url", "url": "https://x/y.png"}}
        assert len(blocks) == 3  # ftp image dropped

    def test_assistant_tool_calls_become_tool_use(self):
        payload = {"messages": [
            {"role": "user", "content": "weather?"},
            {"role": "assistant", "content": "", "tool_calls": [{
                "id": "call_1", "type": "function",
                "function": {"name": "get_weather",
                             "arguments": "{\"city\": \"Paris\"}"}}]},
            {"role": "tool", "tool_call_id": "call_1", "content": "20C"},
        ]}
        out = self.adapter.translate_request(payload, _anth_cfg())
        msgs = out["messages"]
        assert msgs[1]["role"] == "assistant"
        assert msgs[1]["content"] == [{
            "type": "tool_use", "id": "call_1", "name": "get_weather",
            "input": {"city": "Paris"}}]
        # consecutive tool messages merge into ONE user message
        assert msgs[2] == {"role": "user", "content": [{
            "type": "tool_result", "tool_use_id": "call_1",
            "content": [{"type": "text", "text": "20C"}]}]}
        assert len(msgs) == 3

    def test_unsupported_fields_dropped(self):
        payload = {"messages": [{"role": "user", "content": "x"}],
                   "logprobs": True, "seed": 1, "response_format": {}}
        out = self.adapter.translate_request(payload, _anth_cfg())
        for field in ("logprobs", "seed", "response_format"):
            assert field not in out


# ── Anthropic adapter: response translation ─────────────────────────────────

class TestAnthropicResponse:
    def setup_method(self):
        self.adapter = AnthropicAdapter()

    def test_text_response_shape(self):
        body = {
            "id": "msg_1", "type": "message", "role": "assistant",
            "model": "claude-stub",
            "content": [{"type": "text", "text": "hello "},
                        {"type": "text", "text": "world"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 7, "output_tokens": 6},
        }
        out = self.adapter.translate_response(body, {}, _anth_cfg())
        assert out["id"] == "msg_1"
        assert out["object"] == "chat.completion"
        assert out["created"] == EPOCH
        assert out["model"] == "claude-stub"
        assert out["choices"][0]["message"] == {"role": "assistant", "content": "hello world"}
        assert out["choices"][0]["finish_reason"] == "stop"
        assert out["usage"] == {"prompt_tokens": 7, "completion_tokens": 6,
                                "total_tokens": 13}

    def test_tool_use_becomes_tool_calls(self):
        body = {
            "id": "msg_2", "role": "assistant", "model": "claude-stub",
            "content": [
                {"type": "text", "text": "Checking."},
                {"type": "tool_use", "id": "toolu_1", "name": "get_weather",
                 "input": {"city": "Paris"}}],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 12, "output_tokens": 9},
        }
        out = self.adapter.translate_response(body, {}, _anth_cfg())
        msg = out["choices"][0]["message"]
        assert msg["content"] == "Checking."
        assert msg["tool_calls"] == [{
            "id": "toolu_1", "type": "function",
            "function": {"name": "get_weather", "arguments": '{"city": "Paris"}'}}]
        assert out["choices"][0]["finish_reason"] == "tool_calls"

    def test_stop_reason_mapping(self):
        body = {"id": "m", "role": "assistant", "model": "x",
                "content": [], "stop_reason": "max_tokens", "usage": {}}
        out = self.adapter.translate_response(body, {}, _anth_cfg())
        assert out["choices"][0]["finish_reason"] == "length"
        body["stop_reason"] = "refusal"
        assert self.adapter.translate_response(body, {}, _anth_cfg()) \
            ["choices"][0]["finish_reason"] == "content_filter"

    def test_error_envelope_passthrough(self):
        err = {"type": "error", "error": {"type": "api_error", "message": "boom"}}
        assert self.adapter.translate_response(err, {}, _anth_cfg()) is err


# ── Anthropic SSE synthesis ─────────────────────────────────────────────────

def _line(event: str, data: dict) -> str:
    return f"data: {json.dumps(data)}" if not event else (
        f"event: {event}\ndata: {json.dumps(data)}")


def _feed_event(tr: AnthropicStreamTranslator, event: str, data: dict) -> list[bytes]:
    chunks: list[bytes] = []
    if event:
        chunks += tr.feed(f"event: {event}")
    chunks += tr.feed(f"data: {json.dumps(data)}")
    chunks += tr.feed("")
    return chunks


class TestAnthropicStreamTranslator:
    def _translator(self) -> AnthropicStreamTranslator:
        return AnthropicStreamTranslator(default_model="claude-stub")

    def test_happy_path_synthesis(self):
        tr = self._translator()
        out: list[bytes] = []
        out += _feed_event(tr, "message_start", {
            "type": "message_start", "message": {
                "id": "msg_9", "role": "assistant", "model": "claude-stub",
                "usage": {"input_tokens": 7}}})
        out += _feed_event(tr, "content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "text", "text": ""}})
        for part in ("Hel", "lo"):
            out += _feed_event(tr, "content_block_delta", {
                "type": "content_block_delta", "index": 0,
                "delta": {"type": "text_delta", "text": part}})
        out += _feed_event(tr, "message_delta", {
            "type": "message_delta", "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 6}})
        out += _feed_event(tr, "message_stop", {"type": "message_stop"})

        texts = [c.decode() for c in out]
        # first chunk: role convention
        first = json.loads(texts[0].removeprefix("data: ").strip())
        assert first["object"] == "chat.completion.chunk"
        assert first["choices"][0]["delta"] == {"role": "assistant", "content": ""}
        assert first["id"] == "msg_9" and first["created"] == EPOCH
        # text deltas
        deltas = [json.loads(t.removeprefix("data: ").strip()) for t in texts[1:3]]
        assert deltas[0]["choices"][0]["delta"] == {"content": "Hel"}
        assert deltas[1]["choices"][0]["delta"] == {"content": "lo"}
        # finish chunk
        finish = json.loads(texts[3].removeprefix("data: ").strip())
        assert finish["choices"][0]["finish_reason"] == "stop"
        assert finish["choices"][0]["delta"] == {}
        # usage chunk (include_usage convention)
        usage = json.loads(texts[4].removeprefix("data: ").strip())
        assert usage["choices"] == []
        assert usage["usage"] == {"prompt_tokens": 7, "completion_tokens": 6,
                                  "total_tokens": 13}
        # terminator
        assert texts[5] == "data: [DONE]\n\n"
        assert len(texts) == 6

    def test_tool_use_streaming(self):
        tr = self._translator()
        out: list[bytes] = []
        out += _feed_event(tr, "message_start", {
            "type": "message_start", "message": {"id": "m", "model": "c"}})
        out += _feed_event(tr, "content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "t1",
                              "name": "get_weather"}})
        out += _feed_event(tr, "content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{\"city\":"}})
        out += _feed_event(tr, "content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": " \"Paris\"}"}})
        out += _feed_event(tr, "message_delta", {
            "type": "message_delta", "delta": {"stop_reason": "tool_use"}})
        out += _feed_event(tr, "message_stop", {"type": "message_stop"})

        texts = [c.decode() for c in out]
        start = json.loads(texts[1].removeprefix("data: ").strip())
        assert start["choices"][0]["delta"]["tool_calls"][0]["id"] == "t1"
        assert start["choices"][0]["delta"]["tool_calls"][0]["function"]["name"] \
            == "get_weather"
        args = "".join(
            json.loads(t.removeprefix("data: ").strip())["choices"][0]["delta"]
            ["tool_calls"][0]["function"]["arguments"] for t in texts[2:4])
        assert args == '{"city": "Paris"}'
        finish = json.loads(texts[4].removeprefix("data: ").strip())
        assert finish["choices"][0]["finish_reason"] == "tool_calls"
        assert texts[-1] == "data: [DONE]\n\n"

    def test_noise_swallowed(self):
        tr = self._translator()
        assert tr.feed("") == []
        assert tr.feed(": keep-alive comment") == []
        assert tr.feed("event: ping") == []
        assert tr.feed("data: {\"type\": \"ping\"}") == []
        assert tr.feed("data: not-json") == []
        assert tr.feed("GET / HTTP/1.1") == []

    def test_error_event_terminates(self):
        tr = self._translator()
        chunks = _feed_event(tr, "error", {
            "type": "error", "error": {"type": "overloaded_error",
                                       "message": "overloaded"}})
        texts = [c.decode() for c in chunks]
        err = json.loads(texts[0].removeprefix("data: ").strip())
        assert err["error"]["type"] == "overloaded_error"
        assert texts[-1] == "data: [DONE]\n\n"

    def test_data_only_dispatch_without_event_lines(self):
        """Robustness: some proxies emit data lines without event lines."""
        tr = self._translator()
        out = tr.feed("data: " + json.dumps({
            "type": "message_start",
            "message": {"id": "m1", "model": "claude-stub"}}))
        assert out, "message_start should synthesize the role chunk"
        role = json.loads(out[0].decode().removeprefix("data: ").strip())
        assert role["choices"][0]["delta"]["role"] == "assistant"

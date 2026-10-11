#!/usr/bin/env python3
"""Provider abstraction layer (Stage 2).

One internal Provider interface sits behind the single OpenAI-compatible
external surface (conflict resolution C1): protocol translation is an
upstream concern, never an ingress concern.

Adapters:
  openai     — passthrough to any /v1/chat/completions upstream (default).
               Covers OpenAI, Ollama, LM Studio, vLLM, llama.cpp and every
               other OpenAI-compatible runtime (aliased registry names).
  anthropic  — full OpenAI <-> Anthropic Messages translation, including
               SSE synthesis (Anthropic events -> OpenAI chat.completion.chunk
               stream), tool-call round trips and image content blocks.

Registry:
  ``provider: <name>`` on a category (or fallback_provider / fallback2_provider)
  selects the adapter. Unknown names fail closed at startup (and on hot
  reload); request-time resolution failure returns a synthetic 502 so the
  fallback cascade can engage.

Determinism hook:
  HERMES_ROUTER_FIXED_EPOCH pins the synthesized ``created`` timestamps used
  by adapters (test/golden-capture only; unset in production).
"""
from __future__ import annotations

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("hermes-router.providers")

# ── Deterministic epoch hook (tests / golden capture only) ──────────────────
_FIXED_EPOCH: int | None = None
_raw_epoch = os.environ.get("HERMES_ROUTER_FIXED_EPOCH", "").strip()
if _raw_epoch:
    try:
        _FIXED_EPOCH = int(_raw_epoch)
    except ValueError:
        log.warning("Ignoring non-integer HERMES_ROUTER_FIXED_EPOCH=%r", _raw_epoch)


def now_epoch() -> int:
    """Wall-clock seconds, or the pinned epoch when capturing fixtures."""
    return _FIXED_EPOCH if _FIXED_EPOCH is not None else int(time.time())


class AdapterError(RuntimeError):
    """Raised for unknown provider names or unrecoverable translation input."""


@dataclass
class BuiltRequest:
    """A fully provider-native upstream request, ready to POST."""

    url: str
    json: dict[str, Any]
    headers: dict[str, str]


# ── Streaming ───────────────────────────────────────────────────────────────
class StreamTranslator:
    """Stateful SSE line transformer.

    ``feed()`` consumes one line of the upstream SSE stream (as yielded by
    ``httpx`` ``aiter_lines`` — including empty framing lines) and returns the
    fully-framed byte chunks to forward to the client. The base implementation
    is byte-exact passthrough: non-empty lines keep their original framing
    (``line\\n``), empty lines pass through as ``\\n``.
    """

    def feed(self, line: str) -> list[bytes]:
        if line:
            return [f"{line}\n".encode()]
        return [b"\n"]


# ── Adapter interface ───────────────────────────────────────────────────────
class ProviderAdapter(ABC):
    """Internal provider interface (conflict resolution C1).

    One OpenAI-compatible external surface exists at the gateway; everything
    below this interface is provider-native. Implementations translate the
    OpenAI chat-completions shape to the upstream protocol and back, for both
    JSON and SSE flows.
    """

    name: str = ""
    METADATA: dict[str, Any] = {}

    def metadata(self) -> dict[str, Any]:
        """Registry capability metadata (admin/ops surface)."""
        return dict(self.METADATA)

    @abstractmethod
    def build_url(self, model_cfg: dict[str, Any]) -> str:
        """Full upstream URL for a configured category/fallback entry."""

    @abstractmethod
    def build_headers(self, api_key: str) -> dict[str, str]:
        """Provider-native headers for one attempt (auth included)."""

    def build_request(
        self,
        model_cfg: dict[str, Any],
        request_payload: dict[str, Any],
        api_key: str,
    ) -> BuiltRequest:
        """Translate + assemble the complete upstream request."""
        return BuiltRequest(
            url=self.build_url(model_cfg),
            json=self.translate_request(request_payload, model_cfg),
            headers=self.build_headers(api_key),
        )

    def translate_request(
        self,
        payload: dict[str, Any],
        model_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        """OpenAI payload -> provider-native payload. Default: model swap."""
        return {**payload, "model": model_cfg["model"]}

    @abstractmethod
    def translate_response(
        self,
        upstream_json: dict[str, Any],
        request_payload: dict[str, Any],
        model_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        """Provider-native JSON response -> OpenAI chat.completion shape."""

    def open_stream_translator(
        self,
        request_payload: dict[str, Any],
        model_cfg: dict[str, Any],
    ) -> StreamTranslator:
        """A fresh per-request stream translator (identity by default)."""
        return StreamTranslator()


# ── OpenAI-compatible adapter (default) ─────────────────────────────────────
class OpenAICompatAdapter(ProviderAdapter):
    """Passthrough to any OpenAI-compatible /v1/chat/completions upstream.

    Behavior-identical to the pre-Stage-2 dispatch path: same URL shape, same
    Bearer auth, same payload, byte-identical SSE passthrough. The golden
    fixture suite pins this contract.
    """

    name = "openai"
    METADATA: dict[str, Any] = {
        "protocol": "openai-chat-completions",
        "streaming": True,
        "tools": True,
        "auth": "bearer",
        "description": "Any /v1/chat/completions upstream (OpenAI, Ollama, "
                       "LM Studio, vLLM, llama.cpp, aggregators).",
        "aliases": ["openai_compat", "openai-compatible", "ollama",
                    "lmstudio", "vllm", "llamacpp"],
    }

    def build_url(self, model_cfg: dict[str, Any]) -> str:
        return f"{model_cfg['base_url'].rstrip('/')}/chat/completions"

    def build_headers(self, api_key: str) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    def translate_response(
        self,
        upstream_json: dict[str, Any],
        request_payload: dict[str, Any],
        model_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        return upstream_json


# ── Anthropic Messages adapter ──────────────────────────────────────────────
_ANTHROPIC_VERSION = "2023-06-01"
_ANTHROPIC_DEFAULT_MAX_TOKENS = 4096

_STOP_REASON_MAP = {
    "end_turn": "stop",
    "stop_sequence": "stop",
    "max_tokens": "length",
    "tool_use": "tool_calls",
    "refusal": "content_filter",
}

# OpenAI request fields with no Anthropic equivalent in this adapter's
# contract; dropped (with a debug log) rather than sent upstream.
_ANTHROPIC_DROPPED_FIELDS = (
    "n", "logprobs", "top_logprobs", "response_format", "seed",
    "frequency_penalty", "presence_penalty", "reasoning_effort", "user",
)


def _text_of(content: Any) -> str:
    """Extract plain text from an OpenAI message content (str or list)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                parts.append(str(part.get("text", "")))
        return "".join(parts)
    return ""


def _anthropic_image_block(url: str) -> dict[str, Any] | None:
    """Translate an OpenAI image_url to an Anthropic image content block."""
    if url.startswith("data:"):
        header, _, data = url.partition(",")
        media_type = header.removeprefix("data:").split(";", 1)[0].strip()
        if not data:
            return None
        return {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type or "image/png",
                "data": data,
            },
        }
    if url.startswith(("http://", "https://")):
        return {"type": "image", "source": {"type": "url", "url": url}}
    return None


def _user_content_blocks(content: Any) -> list[dict[str, Any]]:
    """Translate one OpenAI user message content to Anthropic blocks."""
    if isinstance(content, str):
        return [{"type": "text", "text": content}] if content else []
    blocks: list[dict[str, Any]] = []
    if not isinstance(content, list):
        return blocks
    for part in content:
        if not isinstance(part, dict):
            continue
        ptype = part.get("type")
        if ptype == "text":
            text = str(part.get("text", ""))
            if text:
                blocks.append({"type": "text", "text": text})
        elif ptype == "image_url":
            url = (part.get("image_url") or {}).get("url", "")
            if isinstance(url, str):
                image = _anthropic_image_block(url)
                if image is not None:
                    blocks.append(image)
                else:
                    log.debug("Dropped unsupported image_url: %r", url[:60])
    return blocks


def _tool_result_blocks(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Translate a run of consecutive OpenAI ``tool`` role messages."""
    blocks = []
    for msg in messages:
        text = _text_of(msg.get("content"))
        blocks.append({
            "type": "tool_result",
            "tool_use_id": str(msg.get("tool_call_id", "")),
            "content": [{"type": "text", "text": text}] if text else [],
        })
    return blocks


class AnthropicAdapter(ProviderAdapter):
    """OpenAI <-> Anthropic /v1/messages translation.

    Contract highlights (pinned by tests/test_providers.py):
      - system messages hoist to the top-level ``system`` parameter
      - ``tool`` role messages merge into one user message of tool_result
        blocks (Anthropic requires them adjacent to the assistant tool_use)
      - assistant ``tool_calls`` become tool_use content blocks
      - ``max_tokens`` is mandatory: request value, else category option
        ``max_tokens_default``, else 4096
      - images: data URIs -> base64 source, http(s) -> URL source
      - tool_choice: auto/required/named; ``none`` drops tools entirely
      - SSE: Anthropic events synthesize OpenAI chat.completion.chunk frames
        terminated by ``data: [DONE]``
    """

    name = "anthropic"
    METADATA: dict[str, Any] = {
        "protocol": "anthropic-messages",
        "streaming": True,
        "tools": True,
        "auth": "x-api-key",
        "description": "Anthropic Messages API with native-schema translation "
                       "and SSE synthesis.",
        "aliases": [],
    }

    def build_url(self, model_cfg: dict[str, Any]) -> str:
        return f"{model_cfg['base_url'].rstrip('/')}/v1/messages"

    def build_headers(self, api_key: str) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "anthropic-version": _ANTHROPIC_VERSION,
        }
        if api_key:
            headers["x-api-key"] = api_key
        return headers

    # ── Request translation ─────────────────────────────────────────────
    def translate_request(
        self,
        payload: dict[str, Any],
        model_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        out: dict[str, Any] = {
            "model": model_cfg["model"],
            "max_tokens": int(
                payload.get("max_tokens")
                or model_cfg.get("max_tokens_default")
                or _ANTHROPIC_DEFAULT_MAX_TOKENS
            ),
        }

        system_parts = [
            _text_of(m.get("content"))
            for m in payload.get("messages", [])
            if m.get("role") == "system"
        ]
        system = "\n\n".join(s for s in system_parts if s)
        if system:
            out["system"] = system

        messages: list[dict[str, Any]] = []
        pending_tool_results: list[dict[str, Any]] = []

        def _flush_tool_results() -> None:
            nonlocal pending_tool_results
            if pending_tool_results:
                messages.append({
                    "role": "user",
                    "content": _tool_result_blocks(pending_tool_results),
                })
                pending_tool_results = []

        for msg in payload.get("messages", []):
            role = msg.get("role")
            if role == "system":
                continue  # hoisted to top-level system
            if role == "tool":
                pending_tool_results.append(msg)
                continue
            _flush_tool_results()
            if role == "user":
                blocks = _user_content_blocks(msg.get("content"))
                if blocks:
                    messages.append({"role": "user", "content": blocks})
            elif role == "assistant":
                asst_blocks: list[dict[str, Any]] = []
                text = _text_of(msg.get("content"))
                if text:
                    asst_blocks.append({"type": "text", "text": text})
                for call in msg.get("tool_calls") or []:
                    fn = call.get("function") or {}
                    try:
                        args = json.loads(fn.get("arguments") or "{}")
                    except (json.JSONDecodeError, TypeError):
                        args = {}
                    asst_blocks.append({
                        "type": "tool_use",
                        "id": str(call.get("id", "")),
                        "name": str(fn.get("name", "")),
                        "input": args if isinstance(args, dict) else {},
                    })
                if asst_blocks:
                    messages.append({"role": "assistant", "content": asst_blocks})
        _flush_tool_results()
        out["messages"] = messages

        if "temperature" in payload:
            out["temperature"] = payload["temperature"]
        if "top_p" in payload:
            out["top_p"] = payload["top_p"]
        if "stop" in payload:
            stop = payload["stop"]
            out["stop_sequences"] = [stop] if isinstance(stop, str) else list(stop)

        if payload.get("stream"):
            out["stream"] = True

        if payload.get("tools"):
            out["tools"] = [
                {
                    "name": str(tool.get("function", {}).get("name", "")),
                    "description": str(tool.get("function", {}).get("description", "")),
                    "input_schema": tool.get("function", {}).get("parameters")
                                    or {"type": "object", "properties": {}},
                }
                for tool in payload["tools"]
                if isinstance(tool, dict) and tool.get("type") == "function"
            ]
            choice = payload.get("tool_choice", "auto")
            if choice == "auto":
                out["tool_choice"] = {"type": "auto"}
            elif choice == "required":
                out["tool_choice"] = {"type": "any"}
            elif isinstance(choice, dict) and choice.get("type") == "function":
                out["tool_choice"] = {
                    "type": "tool",
                    "name": str(choice.get("function", {}).get("name", "")),
                }
            elif choice == "none":
                # Anthropic has no "never use tools" toggle — drop them.
                out.pop("tools", None)
                out.pop("tool_choice", None)

        dropped = [f for f in _ANTHROPIC_DROPPED_FIELDS if f in payload]
        if dropped:
            log.debug("Anthropic adapter dropping unsupported fields: %s", dropped)
        return out

    # ── Response translation ────────────────────────────────────────────
    def translate_response(
        self,
        upstream_json: dict[str, Any],
        request_payload: dict[str, Any],
        model_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        if upstream_json.get("type") == "error":
            # Anthropic error envelope — pass through untouched; callers key
            # fallback decisions off HTTP status, which is already non-200.
            return upstream_json

        text_parts: list[str] = []
        tool_calls: list[dict[str, Any]] = []
        for idx, block in enumerate(upstream_json.get("content") or []):
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype == "text":
                text_parts.append(str(block.get("text", "")))
            elif btype == "tool_use":
                tool_calls.append({
                    "id": str(block.get("id", f"call_{idx}")),
                    "type": "function",
                    "function": {
                        "name": str(block.get("name", "")),
                        "arguments": json.dumps(block.get("input") or {}),
                    },
                })
        message: dict[str, Any] = {"role": "assistant", "content": "".join(text_parts)}
        if tool_calls:
            message["tool_calls"] = tool_calls

        usage_src = upstream_json.get("usage") or {}
        prompt_tokens = int(usage_src.get("input_tokens", 0))
        completion_tokens = int(usage_src.get("output_tokens", 0))
        return {
            "id": str(upstream_json.get("id", f"chatcmpl-anthropic-{now_epoch()}")),
            "object": "chat.completion",
            "created": now_epoch(),
            "model": str(upstream_json.get("model", model_cfg.get("model", ""))),
            "choices": [{
                "index": 0,
                "message": message,
                "finish_reason": _STOP_REASON_MAP.get(
                    upstream_json.get("stop_reason", "end_turn"), "stop"),
            }],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
            },
        }

    # ── Streaming synthesis ─────────────────────────────────────────────
    def open_stream_translator(
        self,
        request_payload: dict[str, Any],
        model_cfg: dict[str, Any],
    ) -> StreamTranslator:
        return AnthropicStreamTranslator(default_model=str(model_cfg.get("model", "")))


class AnthropicStreamTranslator(StreamTranslator):
    """Anthropic SSE events -> OpenAI chat.completion.chunk synthesis."""

    def __init__(self, default_model: str = "") -> None:
        self._default_model = default_model
        self._pending_event = ""
        self._id = ""
        self._model = default_model
        self._created = now_epoch()
        self._finish_reason: str | None = None
        self._usage: dict[str, int] | None = None
        # tool_use block index -> {"id": ..., "name": ...}
        self._tool_blocks: dict[int, dict[str, str]] = {}
        self._block_index = 0

    # ── chunk emitters ──────────────────────────────────────────────────
    def _chunk(self, delta: dict[str, Any], finish: str | None = None) -> bytes:
        payload = {
            "id": self._id or f"chatcmpl-anthropic-{self._created}",
            "object": "chat.completion.chunk",
            "created": self._created,
            "model": self._model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        }
        return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n".encode()

    def _done(self) -> bytes:
        return b"data: [DONE]\n\n"

    def _error_chunks(self, err: dict[str, Any]) -> list[bytes]:
        body = json.dumps({"error": err}, separators=(",", ":"))
        return [f"data: {body}\n\n".encode(), self._done()]

    # ── event handlers ──────────────────────────────────────────────────
    def _on_message_start(self, data: dict[str, Any]) -> list[bytes]:
        message = data.get("message") or {}
        self._id = str(message.get("id", self._id))
        self._model = str(message.get("model", self._default_model))
        usage = message.get("usage") or {}
        if "input_tokens" in usage:
            self._usage = {"prompt_tokens": int(usage["input_tokens"]),
                           "completion_tokens": 0}
        # OpenAI convention: first chunk carries the role.
        return [self._chunk({"role": "assistant", "content": ""})]

    def _on_content_block_start(self, data: dict[str, Any]) -> list[bytes]:
        block = data.get("content_block") or {}
        self._block_index = int(data.get("index", 0))
        if block.get("type") == "tool_use":
            self._tool_blocks[self._block_index] = {
                "id": str(block.get("id", "")),
                "name": str(block.get("name", "")),
            }
            return [self._chunk({"tool_calls": [{
                "index": self._block_index,
                "id": str(block.get("id", "")),
                "type": "function",
                "function": {"name": str(block.get("name", "")), "arguments": ""},
            }]})]
        return []

    def _on_content_block_delta(self, data: dict[str, Any]) -> list[bytes]:
        delta = data.get("delta") or {}
        dtype = delta.get("type")
        if dtype == "text_delta":
            text = str(delta.get("text", ""))
            if text:
                return [self._chunk({"content": text})]
            return []
        if dtype == "input_json_delta":
            partial = str(delta.get("partial_json", ""))
            if partial:
                return [self._chunk({"tool_calls": [{
                    "index": int(data.get("index", self._block_index)),
                    "function": {"arguments": partial},
                }]})]
            return []
        return []

    def _on_message_delta(self, data: dict[str, Any]) -> list[bytes]:
        delta = data.get("delta") or {}
        if delta.get("stop_reason"):
            self._finish_reason = _STOP_REASON_MAP.get(
                str(delta["stop_reason"]), "stop")
        usage = data.get("usage") or {}
        if "output_tokens" in usage:
            if self._usage is None:
                self._usage = {"prompt_tokens": 0, "completion_tokens": 0}
            self._usage["completion_tokens"] = int(usage["output_tokens"])
        return []

    def _on_message_stop(self, data: dict[str, Any]) -> list[bytes]:
        chunks = [self._chunk({}, finish=self._finish_reason or "stop")]
        if self._usage is not None:
            total = self._usage["prompt_tokens"] + self._usage["completion_tokens"]
            chunks.append(f"data: {json.dumps({
                'id': self._id or f'chatcmpl-anthropic-{self._created}',
                'object': 'chat.completion.chunk',
                'created': self._created,
                'model': self._model,
                'choices': [],
                'usage': {**self._usage, 'total_tokens': total},
            }, separators=(',', ':'))}\n\n".encode())
        chunks.append(self._done())
        return chunks

    # ── feed ────────────────────────────────────────────────────────────
    def feed(self, line: str) -> list[bytes]:
        if not line:
            return []  # SSE framing — synthesized chunks carry their own
        if line.startswith(":"):
            return []  # SSE comment (keep-alive)
        if line.startswith("event:"):
            self._pending_event = line.removeprefix("event:").strip()
            return []
        if not line.startswith("data:"):
            return []  # unknown protocol line — swallow, never forward

        raw = line.removeprefix("data:").strip()
        if raw == "[DONE]":  # defensive: upstream already in OpenAI shape
            return [self._done()]
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.debug("Anthropic stream: non-JSON data line dropped: %r", raw[:80])
            return []
        if not isinstance(data, dict):
            return []

        etype = str(data.get("type") or self._pending_event or "")
        self._pending_event = ""
        if etype == "message_start":
            return self._on_message_start(data)
        if etype == "content_block_start":
            return self._on_content_block_start(data)
        if etype == "content_block_delta":
            return self._on_content_block_delta(data)
        if etype == "content_block_stop":
            return []
        if etype == "message_delta":
            return self._on_message_delta(data)
        if etype == "message_stop":
            return self._on_message_stop(data)
        if etype == "ping":
            return []
        if etype == "error":
            err = data.get("error") or {"message": "upstream stream error",
                                        "type": "upstream_error"}
            return self._error_chunks(err if isinstance(err, dict) else {"message": str(err)})
        return []


# ── Registry ────────────────────────────────────────────────────────────────
PROVIDER_REGISTRY: dict[str, ProviderAdapter] = {
    "openai": OpenAICompatAdapter(),
    "anthropic": AnthropicAdapter(),
}

# Convenience names that all resolve to the OpenAI-compatible adapter —
# every entry is a real OpenAI-compatible runtime, not a distinct protocol.
PROVIDER_ALIASES: dict[str, str] = {
    "openai_compat": "openai",
    "openai-compatible": "openai",
    "ollama": "openai",
    "lmstudio": "openai",
    "vllm": "openai",
    "llamacpp": "openai",
}


def resolve_provider_name(raw: str) -> str:
    """Canonical registry key for a configured provider name."""
    name = str(raw or "openai").strip().lower()
    return PROVIDER_ALIASES.get(name, name)


def get_provider(model_cfg: dict[str, Any]) -> ProviderAdapter:
    """Resolve the adapter for a category/fallback config dict. Fail-closed."""
    canonical = resolve_provider_name(str((model_cfg or {}).get("provider") or "openai"))
    adapter = PROVIDER_REGISTRY.get(canonical)
    if adapter is None:
        known = sorted(set(PROVIDER_REGISTRY) | set(PROVIDER_ALIASES))
        raise AdapterError(
            f"unknown provider {canonical!r} (known providers: {known})")
    return adapter


def provider_name(model_cfg: dict[str, Any]) -> str:
    """Best-effort provider name for trace attribution (never raises)."""
    try:
        return get_provider(model_cfg).name
    except AdapterError:
        return str((model_cfg or {}).get("provider") or "openai")


def validate_provider_config(cfg: dict[str, Any]) -> list[str]:
    """Startup/hot-reload gate: every configured provider name must resolve."""
    errors: list[str] = []
    for cat_name, cat in (cfg.get("categories") or {}).items():
        if not isinstance(cat, dict):
            continue
        for key in ("provider", "fallback_provider", "fallback2_provider"):
            raw = str(cat.get(key) or "").strip().lower()
            if raw and resolve_provider_name(raw) not in PROVIDER_REGISTRY:
                errors.append(
                    f"category '{cat_name}': unknown {key} {raw!r} "
                    f"(known: {sorted(PROVIDER_REGISTRY)})")
    return errors

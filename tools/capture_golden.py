#!/usr/bin/env python3
"""Golden fixture capture harness (Stage 0).

Drives the UNMODIFIED upstream server (server.py @ pinned commit) against a
deterministic stub upstream and records request/response transcripts to
tests/fixtures/golden/. These fixtures are the baseline behavior contract:
Stage 2+ contract tests replay them, and every stage boundary re-runs capture
to diff for regressions.

Determinism strategy:
  - every scenario selects its tier via the /use: override (bypasses all
    classifier tiers -> no LLM, no sklearn, no network beyond localhost)
  - the stub upstream returns fixed ids/created values
  - scenario texts avoid every escalation/de-escalation keyword
  - volatile trace fields (ts, latency, session hashes) are normalized

Stage 2: the stub speaks BOTH protocols — OpenAI /v1/chat/completions and
Anthropic /v1/messages — and scenarios 10-14 exercise the anthropic adapter
text / streaming / cross-protocol fallback / tool-call paths plus the
/admin/providers registry snapshot. HERMES_ROUTER_FIXED_EPOCH pins the
timestamps the adapter synthesizes.

Stage 5: the stub injects a deterministic billing-leak (402) on
"billing-leak" prompts in both protocols, and scenarios 21-24 pin the cost
guard — principal request/token limiter boundaries (VK_D/VK_E), pool-scope
ceilings (guard_pool) and the watchdog — plus the /admin/costguard
snapshot (93).

Usage:  python3 tools/capture_golden.py
"""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

REPO = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO / "tests" / "fixtures" / "golden"
CONFIG_SRC = REPO / "tests" / "fixtures" / "router_config.fixture.yaml"

STUB_PORT = 9901
ROUTER_PORT = 9902
STUB_URL = f"http://127.0.0.1:{STUB_PORT}/v1"
ROUTER_URL = f"http://127.0.0.1:{ROUTER_PORT}"

# Stage 1: the router now requires bearer auth. The fixture key value is a
# throwaway test secret (not a real credential) set via GOLDEN_ROUTER_KEY.
GOLDEN_ROUTER_KEY = "golden-test-key-0001"
AUTH_HEADER = f"Bearer {GOLDEN_ROUTER_KEY}"

FIXED_CREATED = 1727000000
FIXED_BODY = "Stub response for golden fixture."

# Stage 3: pool scenarios use dedicated throwaway key values; the stub
# rate-limits a deterministic subset so rotation is reproducible.
POOL_KEY_A = "poolkey-aaaa"   # rotate_pool key-a: always 429 (retry-after: 1)
POOL_KEY_B = "poolkey-bbbb"   # rotate_pool key-b: healthy
POOL_KEY_C = "poolkey-cccc"   # exhaust_pool key-c: always 429
POOL_KEY_D = "poolkey-dddd"   # exhaust_pool key-d: always 429
POOL_KEY_E = "poolkey-eeee"   # stream_pool key-e: always 429
POOL_KEY_F = "poolkey-ffff"   # stream_pool key-f: healthy
POOL_KEY_G = "poolkey-gggg"   # guard_pool key-g: healthy (Stage 5)
POOL_429_KEYS = {POOL_KEY_A, POOL_KEY_C, POOL_KEY_D, POOL_KEY_E}

RUNTIME_FILES = [
    "server.py",
    "providers.py",
    "trace.py",
    "keystore.py",
    "keypool.py",
    "catalog.py",
    "ledger.py",
    "costguard.py",
    "surrogate_models.py",
    "zero_shot_classifier.py",
    "fasttext_classifier.py",
    "setfit_classifier.py",
]


# ── Stub upstream ───────────────────────────────────────────────────────────

def _sse_chunk(idx: int, model: str) -> str:
    delta = {"role": "assistant", "content": ["Hello", " from", " the stub."][idx]}
    if idx == 2:
        delta = {"role": "assistant", "content": " the stub.", "final": True}
    payload = {
        "id": "chatcmpl-stub-golden",
        "object": "chat.completion.chunk",
        "created": FIXED_CREATED,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
    }
    return f"data: {json.dumps(payload, separators=(',', ':'))}"


def _sse_usage_chunk(model: str) -> str:
    """Final pre-[DONE] usage chunk (Stage 4: feeds the ledger sniffer).

    Mirrors OpenAI's stream_options.include_usage frame shape (empty
    choices + usage). Emitted unconditionally so streaming usage capture
    is pinned deterministically.
    """
    payload = {
        "id": "chatcmpl-stub-golden",
        "object": "chat.completion.chunk",
        "created": FIXED_CREATED,
        "model": model,
        "choices": [],
        "usage": {"prompt_tokens": 5, "completion_tokens": 6, "total_tokens": 11},
    }
    return f"data: {json.dumps(payload, separators=(',', ':'))}"


def _anth_event(name: str, data: dict) -> str:
    """One deterministic Anthropic SSE frame (event + data + blank line)."""
    return (f"event: {name}\n"
            f"data: {json.dumps(data, separators=(',', ':'))}\n\n")


def _last_user_text_anthropic(messages: list) -> str:
    for m in reversed(messages or []):
        if m.get("role") == "user":
            content = m.get("content", "")
            if isinstance(content, list):
                content = " ".join(
                    p.get("text", "") for p in content
                    if isinstance(p, dict) and p.get("type") == "text")
            return str(content)
    return ""


def build_stub_app() -> FastAPI:
    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        payload = await request.json()
        model = payload.get("model", "stub-unknown")

        # Stage 3: deterministic per-key behavior for pool scenarios
        auth = request.headers.get("authorization", "")
        if auth.startswith("Bearer ") and auth[7:] in POOL_429_KEYS:
            return JSONResponse(
                {"error": {"message": "stub rate limited", "type": "rate_limit_error"}},
                status_code=429,
                headers={"retry-after": "1"},
            )

        last_user = ""
        for m in reversed(payload.get("messages", [])):
            if m.get("role") == "user":
                content = m.get("content", "")
                if isinstance(content, list):
                    content = " ".join(p.get("text", "") for p in content if isinstance(p, dict))
                last_user = content
                break

        # Deterministic primary failure for the fallback-cascade scenario
        if model == "stub-simple" and "trigger-fallback" in last_user:
            return JSONResponse(
                {"error": {"message": "stub primary failure", "type": "stub_error"}},
                status_code=500,
            )

        # Stage 5: deterministic billing-leak trigger for the watchdog
        # scenario (upstream payment wall — a billing signal nobody owns)
        if "billing-leak" in last_user:
            return JSONResponse(
                {"error": {"message": "stub payment required",
                           "type": "payment_required_error"}},
                status_code=402,
            )

        if payload.get("stream"):
            def gen():
                for i in range(3):
                    yield _sse_chunk(i, model) + "\n\n"
                yield _sse_usage_chunk(model) + "\n\n"
                yield "data: [DONE]\n\n"
            return StreamingResponse(gen(), media_type="text/event-stream")

        return JSONResponse({
            "id": "chatcmpl-stub-golden",
            "object": "chat.completion",
            "created": FIXED_CREATED,
            "model": model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": FIXED_BODY},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 5, "completion_tokens": 6, "total_tokens": 11},
        })

    @app.post("/v1/messages")
    async def anthropic_messages(request: Request):
        """Anthropic-protocol stub (Stage 2) — deterministic, no auth."""
        payload = await request.json()
        model = payload.get("model", "stub-anthropic-unknown")
        text = _last_user_text_anthropic(payload.get("messages", []))

        # Deterministic primary failure for the cross-protocol cascade
        if "trigger-fallback" in text:
            return JSONResponse(
                {"type": "error",
                 "error": {"type": "api_error", "message": "stub anthropic failure"}},
                status_code=500,
            )

        # Stage 5: deterministic billing-leak trigger (anthropic shape)
        if "billing-leak" in text:
            return JSONResponse(
                {"type": "error",
                 "error": {"type": "permission_error",
                           "message": "stub anthropic payment required"}},
                status_code=402,
            )

        # Tool-use round trip: anthropic tool_use block -> OpenAI tool_calls
        if "weather" in text:
            return JSONResponse({
                "id": "msg_stub_tool", "type": "message", "role": "assistant",
                "model": model,
                "content": [
                    {"type": "text", "text": "Checking the weather."},
                    {"type": "tool_use", "id": "toolu_stub_01",
                     "name": "get_weather", "input": {"city": "Paris"}},
                ],
                "stop_reason": "tool_use", "stop_sequence": None,
                "usage": {"input_tokens": 12, "output_tokens": 9},
            })

        if payload.get("stream"):
            def gen_anth():
                yield _anth_event("message_start", {
                    "type": "message_start",
                    "message": {"id": "msg_stub_anthropic", "type": "message",
                                "role": "assistant", "model": model, "content": [],
                                "usage": {"input_tokens": 7}},
                })
                yield _anth_event("content_block_start", {
                    "type": "content_block_start", "index": 0,
                    "content_block": {"type": "text", "text": ""},
                })
                for part in ("Stub", " response", " for golden fixture."):
                    yield _anth_event("content_block_delta", {
                        "type": "content_block_delta", "index": 0,
                        "delta": {"type": "text_delta", "text": part},
                    })
                yield _anth_event("content_block_stop", {
                    "type": "content_block_stop", "index": 0})
                yield _anth_event("message_delta", {
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                    "usage": {"output_tokens": 6},
                })
                yield _anth_event("message_stop", {"type": "message_stop"})
            return StreamingResponse(gen_anth(), media_type="text/event-stream")

        return JSONResponse({
            "id": "msg_stub_anthropic", "type": "message", "role": "assistant",
            "model": model,
            "content": [{"type": "text", "text": FIXED_BODY}],
            "stop_reason": "end_turn", "stop_sequence": None,
            "usage": {"input_tokens": 7, "output_tokens": 6},
        })

    @app.get("/health")
    async def health():
        return {"ok": True}

    return app


# ── Helpers ─────────────────────────────────────────────────────────────────

def session_key(first_user_content: str) -> str:
    """Mirror upstream server.session_key (sha256 of first user message)."""
    return hashlib.sha256(first_user_content.encode()[:200]).hexdigest()[:16]


def wait_ready(url: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=2.0).status_code == 200:
                return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError(f"{url} not ready after {timeout}s")


def normalize_trace(records: list[dict]) -> list[dict]:
    keep = [
        "event", "tier", "model", "upstream_status", "stream",
        "fallback_level", "fallback_model", "direction", "reason",
        "old_state", "new_state", "failure_count", "max_failures",
        "provider", "key_id", "from_key_id",
    ]
    out = []
    for r in records:
        cleaned = {k: r[k] for k in keep if k in r}
        if "base_url" in r:
            cleaned["base_url"] = "<stub>"
        out.append(cleaned)
    return out


# ── Scenarios ───────────────────────────────────────────────────────────────

def user_msg(text: str) -> dict:
    return {"role": "user", "content": text}


def run_scenarios(client: httpx.Client, trace_path: Path) -> list[dict]:
    fixtures: list[dict] = []

    def trace_lines() -> list[str]:
        if not trace_path.exists():
            return []
        return [entry for entry in trace_path.read_text().splitlines() if entry.strip()]

    def record(name: str, description: str, request_body: dict | None,
               path: str, method: str, status: int, body: dict | list | None,
               trace_key: str | None = None, notes: str = "",
               volatile: bool = False, trace_offset: int = 0,
               request_headers: dict | None = None) -> None:
        # Session-less event types (no session_key field) that still belong
        # to THIS request when they appear inside the offset slice. The
        # slice bounds them: capture requests are strictly sequential.
        sessionless = {"key_rotation", "circuit"}
        events: list[dict] = []
        if trace_key:
            # Only events appended by THIS request (file is append-only;
            # sessions can share a key across scenarios, e.g. 05a/05b)
            events = normalize_trace([
                json.loads(line) for line in trace_lines()[trace_offset:]
                if json.loads(line).get("session_key") == trace_key
                or (json.loads(line).get("event") in sessionless
                    and "session_key" not in json.loads(line))
            ])
        fixtures.append({
            "name": name,
            "description": description,
            "request": {"method": method, "path": path,
                        "headers": request_headers or {}, "body": request_body},
            "response": {"status": status, "body": body},
            "trace_events": events,
            "volatile": volatile,
            "notes": notes,
        })

    def post_chat(name: str, description: str, messages: list[dict],
                  stream: bool = False, notes: str = "",
                  auth_header: str | None = AUTH_HEADER,
                  record_auth_header: str | None = None) -> None:
        body = {"model": "auto", "messages": messages, "stream": stream}
        key = session_key(
            messages[0]["content"] if isinstance(messages[0]["content"], str)
            else " ".join(p.get("text", "") for p in messages[0]["content"])
        )
        offset = len(trace_lines())  # events before this request
        hdrs = {"Authorization": auth_header} if auth_header else {}
        rec_hdrs = ({"Authorization": record_auth_header}
                    if record_auth_header else hdrs)
        if stream:
            chunks: list[str] = []
            status = 0
            with client.stream("POST", f"{ROUTER_URL}/v1/chat/completions", json=body, headers=hdrs) as r:
                status = r.status_code
                for line in r.iter_lines():
                    if line.strip():  # drop empty SSE framing lines
                        chunks.append(line)
            record(name, description, body, "/v1/chat/completions", "POST",
                   status, chunks, key, notes or "SSE transcript, in order",
                   trace_offset=offset, request_headers=rec_hdrs)
        else:
            r = client.post(f"{ROUTER_URL}/v1/chat/completions", json=body, headers=hdrs)
            record(name, description, body, "/v1/chat/completions", "POST",
                   r.status_code, r.json(), key, notes, trace_offset=offset,
                   request_headers=rec_hdrs)

    # 01 — override to simple tier, non-streaming
    post_chat("golden_01_simple_nonstream",
              "/use: override selects simple tier; transparent passthrough",
              [user_msg("/use:simple What is the capital of France?")])

    # 02 — override to complex tier, non-streaming
    post_chat("golden_02_complex_nonstream",
              "/use: override selects complex tier",
              [user_msg("/use:complex Write a Fibonacci memo example")])

    # 03 — streaming SSE passthrough
    post_chat("golden_03_streaming",
              "SSE passthrough of upstream chunks, terminated by [DONE]",
              [user_msg("/use:simple Tell me a short story")], stream=True)

    # 04 — fallback cascade: primary 500 -> fallback 200, response annotated
    post_chat("golden_04_fallback_cascade",
              "primary 500 -> fallback model succeeds; hermes_router.fallback_used added",
              [user_msg("/use:simple trigger-fallback please")],
              notes="Stub fails stub-simple on 'trigger-fallback'; stub-fallback always 200")

    # 05a/05b — session tier caching (same first user message -> same key)
    first = "/use:simple Remember this anchor"
    post_chat("golden_05a_session_first",
              "first message of a session: override classified and cached",
              [user_msg(first)])
    post_chat("golden_05b_session_cache_hit",
              "follow-up reuses cached tier (cache_hit trace event, no reclassify)",
              [user_msg(first),
               {"role": "assistant", "content": "Understood."},
               user_msg("what is 2+2")],
              notes="Same first user message -> same session key; neutral follow-up text")

    # 06 — contract error: no user message
    post_chat("golden_06_error_no_user",
              "400 router_error when no user message present",
              [{"role": "system", "content": "You are helpful."}])

    # 07 — multimodal content array (list content survives untouched upstream)
    post_chat("golden_07_multimodal",
              "content array (text part) routes via override; list content forwarded as-is",
              [{"role": "user", "content": [
                  {"type": "text", "text": "/use:complex Describe this image"}]}],
              notes="Upstream replaces str content only; list content keeps override prefix")

    # 08/09 — Stage 1 negative authz (auth is mandatory)
    r = client.post(f"{ROUTER_URL}/v1/chat/completions", json={"messages": [user_msg("/use:simple hi")]})
    record("golden_08_unauthorized_no_token",
           "401 when no Authorization header is presented (auth mandatory)",
           {"messages": [user_msg("/use:simple hi")]}, "/v1/chat/completions", "POST",
           r.status_code, r.json(), None,
           notes="No Authorization header; request never reaches the stub upstream")
    r = client.post(f"{ROUTER_URL}/v1/chat/completions", json={"messages": [user_msg("/use:simple hi")]},
                    headers={"Authorization": "Bearer wrong-key-value"})
    record("golden_09_unauthorized_wrong_token",
           "401 when a wrong bearer token is presented",
           {"messages": [user_msg("/use:simple hi")]}, "/v1/chat/completions", "POST",
           r.status_code, r.json(), None,
           request_headers={"Authorization": "Bearer [REDACTED-WRONG]"},
           notes="Wrong token; constant-time comparison rejects; never reaches upstream")

    # 10 — Stage 2: anthropic adapter, non-streaming translation
    post_chat("golden_10_anthropic_nonstream",
              "/use:anthropic — Anthropic Messages upstream translated to OpenAI shape",
              [user_msg("/use:anthropic What is the capital of France?")],
              notes="hermes_router.provider annotation marks the non-default provider")

    # 11 — Stage 2: anthropic adapter, SSE synthesis to OpenAI chunks
    post_chat("golden_11_anthropic_stream",
              "/use:anthropic streaming — Anthropic events synthesized as OpenAI chunks",
              [user_msg("/use:anthropic Tell me a short story")], stream=True,
              notes="role chunk, text deltas, finish chunk, usage chunk, [DONE]")

    # 12 — Stage 2: cross-protocol fallback cascade (anthropic -> openai)
    post_chat("golden_12_anthropic_cascade",
              "anthropic primary 500 -> openai fallback 200; translated + annotated",
              [user_msg("/use:anthropic trigger-fallback please")],
              notes="stub fails stub-anthropic on 'trigger-fallback'; openai stub-fallback 200")

    # 13 — Stage 2: anthropic tool_use -> OpenAI tool_calls synthesis
    post_chat("golden_13_anthropic_tool_call",
              "/use:anthropic — anthropic tool_use block becomes OpenAI tool_calls",
              [user_msg("/use:anthropic What is the weather in Paris?")],
              notes="stub returns tool_use for 'weather' prompts")

    # 14 — Stage 2: provider registry snapshot (deterministic)
    r = client.get(f"{ROUTER_URL}/admin/providers", headers={"Authorization": AUTH_HEADER})
    record("golden_14_admin_providers",
           "GET /admin/providers — registry metadata, aliases, per-category resolution",
           None, "/admin/providers", "GET", r.status_code, r.json(), None,
           notes="Registry content is static; category resolution follows the fixture config",
           request_headers={"Authorization": "Bearer [set]"})

    # 15 — Stage 3: pooled rotation — key-a 429 (retry-after: 1) -> key-b 200
    post_chat("golden_15_pool_rotation",
              "pool rotates to the next key on 429; served by the healthy key",
              [user_msg("/use:pooled pooled rotation please")],
              notes="key_rotation event carries from_key_id; route event carries serving key_id")

    # 16 — Stage 3: pooled exhaustion — both keys 429 -> synthetic 429 -> fallback 200
    post_chat("golden_16_pool_exhausted",
              "all pooled keys cooling -> synthetic 429 -> route-level fallback succeeds",
              [user_msg("/use:pooled_exhaust pooled exhaustion please")],
              notes="pool exhaustion engages the existing fallback cascade; fallback_used=true")

    # 17 — Stage 3: pooled streaming rotation — key-e 429 -> key-f streams
    post_chat("golden_17_pool_stream_rotation",
              "streaming through a pool: 429 on first key, SSE passthrough via second",
              [user_msg("/use:pooled_stream pooled stream please")], stream=True,
              notes="pool_429_cooldown_stream rotation then normal SSE synthesis")

    # ── Stage 4: billing plane — virtual keys, attribution, budgets ──────
    # Random per-capture material (vk ids/tokens, timestamps, chain hashes)
    # is scrubbed to stable labels so the fixtures pin the CONTRACT.

    def scrub_vk(obj: Any, mapping: dict[str, str]) -> Any:
        """Replace random vk ids and redact tokens/hashes recursively."""
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                if k == "key":
                    out[k] = "vk-[REDACTED]"
                elif k == "created_at" or k == "ts":
                    out[k] = "[ts]"
                elif k in ("virtual_key_id", "id") and isinstance(v, str) \
                        and v in mapping:
                    out[k] = mapping[v]
                elif k in ("row_hash", "prev_hash"):
                    out[k] = "[hash]"
                else:
                    out[k] = scrub_vk(v, mapping)
            return out
        if isinstance(obj, list):
            return [scrub_vk(x, mapping) for x in obj]
        if isinstance(obj, str) and obj in mapping:
            return mapping[obj]
        return obj

    def create_vk(name: str, body: dict, label: str, mapping: dict) -> dict:
        r = client.post(f"{ROUTER_URL}/admin/virtual_keys", json=body,
                        headers={"Authorization": AUTH_HEADER})
        vk = r.json()
        mapping[vk["id"]] = label
        record(f"golden_{name}",
               f"POST /admin/virtual_keys — create {label} ({body.get('budget_mode', 'none')})",
               body, "/admin/virtual_keys", "POST", r.status_code,
               scrub_vk(vk, mapping), None,
               notes="Plaintext token shown once (redacted here); only its "
                     "SHA-256 is stored server-side",
               request_headers={"Authorization": "Bearer [set]"})
        return vk

    # 18 — attribution end-to-end with two virtual keys (exit gate)
    VK_MAP: dict[str, str] = {}
    vk_a = create_vk("18a_vk_a_create", {"name": "subscriber-a"}, "VK_A", VK_MAP)
    vk_b = create_vk("18b_vk_b_create", {"name": "subscriber-b"}, "VK_B", VK_MAP)
    post_chat("golden_18c_vk_a_nonstream",
              "chat as virtual key A — attributed, free-tier model costs 0",
              [user_msg("/use:simple vk attribution a")],
              auth_header=f"Bearer {vk_a['key']}",
              record_auth_header="Bearer vk-[REDACTED]",
              notes="Ledger record 18e shows virtual_key_id=VK_A, cost 0 (free)")
    post_chat("golden_18d_vk_b_stream",
              "chat as virtual key B — streamed, usage sniffed from SSE, priced",
              [user_msg("/use:complex vk attribution b stream")], stream=True,
              auth_header=f"Bearer {vk_b['key']}",
              record_auth_header="Bearer vk-[REDACTED]",
              notes="stub-complex 5@1.25 + 6@5.0 = 6+30 = 36 micros; "
                    "usage captured from the include_usage chunk")
    r = client.get(f"{ROUTER_URL}/admin/ledger/records?virtual_key_id={vk_a['id']}",
                   headers={"Authorization": AUTH_HEADER})
    record("golden_18e_ledger_record_vk_a",
           "GET /admin/ledger/records?virtual_key_id=A — attribution contract",
           None, "/admin/ledger/records", "GET", r.status_code,
           scrub_vk(r.json(), VK_MAP), None,
           notes="virtual_key_id pinned to VK_A; ts/row_hash/prev_hash scrubbed; "
                 "admin-key traffic (scenarios 01-17) has virtual_key_id null",
           volatile=True,
           request_headers={"Authorization": "Bearer [set]"})
    r = client.get(f"{ROUTER_URL}/admin/ledger/records?virtual_key_id={vk_b['id']}",
                   headers={"Authorization": AUTH_HEADER})
    record("golden_18f_ledger_record_vk_b",
           "GET /admin/ledger/records?virtual_key_id=B — stream attribution",
           None, "/admin/ledger/records", "GET", r.status_code,
           scrub_vk(r.json(), VK_MAP), None,
           notes="stream=1, usage sniffed 5/6, cost_total_micros=36, price_source=model",
           volatile=True,
           request_headers={"Authorization": "Bearer [set]"})

    # 19 — blocking budget: VK_C allowed exactly one 36-micros complex request
    vk_c = create_vk("19a_vk_c_create",
                     {"name": "subscriber-c", "budget_mode": "cost_micros",
                      "budget_limit": 36, "budget_period": "total"},
                     "VK_C", VK_MAP)
    post_chat("golden_19b_budget_last_allowed",
              "VK_C first /use:complex request passes and spends exactly 36 micros",
              [user_msg("/use:complex budget check one")],
              auth_header=f"Bearer {vk_c['key']}",
              record_auth_header="Bearer vk-[REDACTED]",
              notes="spent (36) == limit (36) blocks the NEXT request")
    r = client.post(f"{ROUTER_URL}/v1/chat/completions",
                    json={"model": "auto", "stream": False,
                          "messages": [user_msg("/use:complex budget check two")]},
                    headers={"Authorization": f"Bearer {vk_c['key']}"})
    record("golden_19c_budget_block_402",
           "VK_C second request — blocked 402 BEFORE any upstream call",
           {"model": "auto", "stream": False,
            "messages": [user_msg("/use:complex budget check two")]},
           "/v1/chat/completions", "POST", r.status_code,
           scrub_vk(r.json(), VK_MAP), None,
           notes="HTTP 402 budget_exceeded; gate runs pre-routing, so the "
                 "stub upstream is never contacted",
           request_headers={"Authorization": "Bearer vk-[REDACTED]"})
    r = client.get(f"{ROUTER_URL}/admin/virtual_keys",
                   headers={"Authorization": AUTH_HEADER})
    record("golden_19d_virtual_keys_listing",
           "GET /admin/virtual_keys — spend rollup per key in its window",
           None, "/admin/virtual_keys", "GET", r.status_code,
           scrub_vk(r.json(), VK_MAP), None,
           notes="VK_C spent: 1 request, 36 micros; budget_mode none keys "
                 "report no spent block; created_at scrubbed",
           volatile=True,
           request_headers={"Authorization": "Bearer [set]"})

    # 20 — virtual keys are forbidden on admin surfaces
    r = client.get(f"{ROUTER_URL}/admin/ledger/records",
                   headers={"Authorization": f"Bearer {vk_c['key']}"})
    record("golden_20_vk_admin_forbidden",
           "GET /admin/ledger/records as a virtual key — 403 admin_only",
           None, "/admin/ledger/records", "GET", r.status_code,
           r.json(), None,
           notes="Virtual keys are inference-only: every /admin surface and "
                 "/metrics reject them with 403",
           request_headers={"Authorization": "Bearer vk-[REDACTED]"})

    # ── Stage 5: cost guard — limiter boundaries, reconciliation, watchdog ─
    # Boundary values are chosen so the pinned behavior PROVES each
    # mechanism (arithmetic in docs/STAGE5_COSTGUARD.md):
    #   * vk-two-requests (principal, requests, 2): VK_D 3rd request blocks
    #   * vk-token-budget (principal, tokens, 19): VK_E 2nd request blocks
    #     ONLY because the estimate was reconciled to actual (est-only
    #     sum 9+9=18 fits; reconciled 11+9=20 does not)
    #   * guard-pool-one-request (pool, requests, 1): guard_pool 2nd request
    #     blocks at the pooled dispatch door (synthetic 429)

    def scrub_cg(obj: Any, mapping: dict[str, str]) -> Any:
        """Cost-guard fixture scrub: vk ids (keys AND values), ts,
        timing-derived retry_after."""
        if isinstance(obj, dict):
            out: dict[str, Any] = {}
            for k, v in obj.items():
                if k == "ts":
                    out[k] = "[ts]"
                elif k == "retry_after_seconds":
                    out[k] = "[computed]"
                else:
                    out[mapping.get(k, k)] = scrub_cg(v, mapping)
            return out
        if isinstance(obj, list):
            return [scrub_cg(x, mapping) for x in obj]
        if isinstance(obj, str):
            return mapping.get(obj, obj)
        return obj

    # 21 — principal-scope request limiter boundary (VK_D)
    vk_d = create_vk("21a_vk_d_create", {"name": "subscriber-d"}, "VK_D", VK_MAP)
    post_chat("golden_21b_vk_d_request_one",
              "VK_D request 1/2 — admitted (requests window 1/2), stub-simple 200",
              [user_msg("/use:simple guard request one")],
              auth_header=f"Bearer {vk_d['key']}",
              record_auth_header="Bearer vk-[REDACTED]",
              notes="principal-scope requests limiter: used 0+1 <= 2")
    post_chat("golden_21c_vk_d_request_two",
              "VK_D request 2/2 — admitted at the exact boundary (2/2), 200",
              [user_msg("/use:simple guard request two")],
              auth_header=f"Bearer {vk_d['key']}",
              record_auth_header="Bearer vk-[REDACTED]",
              notes="boundary admit: used 1+1 == limit 2; token window also at "
                    "its boundary (8 est + 3 corr + 8 est = 19/19)")
    r = client.post(f"{ROUTER_URL}/v1/chat/completions",
                    json={"model": "auto", "stream": False,
                          "messages": [user_msg("/use:simple guard request three")]},
                    headers={"Authorization": f"Bearer {vk_d['key']}"})
    record("golden_21d_vk_d_request_block",
           "VK_D request 3 — blocked 429 BEFORE routing (requests 2/2 used)",
           {"model": "auto", "stream": False,
            "messages": [user_msg("/use:simple guard request three")]},
           "/v1/chat/completions", "POST", r.status_code,
           scrub_cg(r.json(), VK_MAP), None,
           notes="cost_guard_rate_limited; detail pins limiter/scope/dimension/"
                 "limit/used; retry_after is timing-derived (scrubbed); no "
                 "trace events — the request never routed",
           request_headers={"Authorization": "Bearer vk-[REDACTED]"})

    # 22 — estimate-to-actual reconciliation boundary (VK_E)
    vk_e = create_vk("22a_vk_e_create", {"name": "subscriber-e"}, "VK_E", VK_MAP)
    post_chat("golden_22b_vk_e_reconcile_admit",
              "VK_E request 1 — est 9 tokens admitted (9/19), reconciled to 11",
              [user_msg("/use:simple reconcile token budget")],
              auth_header=f"Bearer {vk_e['key']}",
              record_auth_header="Bearer vk-[REDACTED]",
              notes="stub usage 5+6=11 total; signed correction +2 lands on "
                    "the principal token window")
    r = client.post(f"{ROUTER_URL}/v1/chat/completions",
                    json={"model": "auto", "stream": False,
                          "messages": [user_msg("/use:simple reconcile token budget")]},
                    headers={"Authorization": f"Bearer {vk_e['key']}"})
    record("golden_22c_vk_e_token_block",
           "VK_E request 2 — blocked on TOKENS: used 11 + est 9 = 20 > 19 "
           "(estimate-only 9+9=18 would fit)",
           {"model": "auto", "stream": False,
            "messages": [user_msg("/use:simple reconcile token budget")]},
           "/v1/chat/completions", "POST", r.status_code,
           scrub_cg(r.json(), VK_MAP), None,
           notes="used=11 is the reconciliation proof: the window holds the "
                 "actual total, not the estimate",
           request_headers={"Authorization": "Bearer vk-[REDACTED]"})

    # 23 — pool-scope ceiling at the pooled dispatch door (guard_pool)
    post_chat("golden_23a_pool_gate_admit",
              "guard_pool request 1 — pool window 1/1, served 200 by key-g",
              [user_msg("/use:pooled_guard pool gate first")],
              notes="pool-scope limiter admits at the pooled dispatch door; "
                    "pooled route event carries key_id")
    r = client.post(f"{ROUTER_URL}/v1/chat/completions",
                    json={"model": "auto", "stream": False,
                          "messages": [user_msg("/use:pooled_guard pool gate second")]},
                    headers={"Authorization": AUTH_HEADER})
    record("golden_23b_pool_gate_block",
           "guard_pool request 2 — synthetic 429 at the dispatch door; no "
           "fallback tier -> 502 upstream_error envelope",
           {"model": "auto", "stream": False,
            "messages": [user_msg("/use:pooled_guard pool gate second")]},
           "/v1/chat/completions", "POST", r.status_code,
           r.json(), None,
           notes="pool-exhaustion semantics (Stage 3 contract) extended to "
                 "limiter blocks; envelope names the cost guard limiter",
           request_headers={"Authorization": "Bearer [set]"})

    # 24 — billing-leak watchdog: injected upstream 402
    post_chat("golden_24a_watchdog_402",
              "upstream 402 (stub payment trigger) — terminal on complex (no "
              "fallback) -> 502 envelope; watchdog records the signal",
              [user_msg("/use:complex trigger billing-leak")],
              notes="single-key lane observes the final 402; admission ticket "
                    "abandoned (unreconciled); /admin/costguard pins the event")

    # 92 — Stage 3: pool state snapshot (deterministic fields only)
    r = client.get(f"{ROUTER_URL}/admin/pools", headers={"Authorization": AUTH_HEADER})
    pools_out = []
    for p in r.json().get("pools", []):
        pools_out.append({
            "name": p["name"],
            "strategy": p["strategy"],
            "size": p["size"],
            "keys": [
                {k: k_meta[k] for k in ("key_id", "label", "capabilities",
                                        "total_acquired", "total_429",
                                        "total_success", "total_auth_failures",
                                        "disabled", "consecutive_429")}
                for k_meta in sorted(p["keys"], key=lambda k: k["key_id"])
            ],
        })
    record("golden_92_admin_pools",
           "GET /admin/pools — per-key counters after the pool scenarios "
           "(15-17 rotation/exhaustion/stream + 23 guard_pool)",
           None, "/admin/pools", "GET", r.status_code,
           {"pools": pools_out, "count": len(pools_out)}, None,
           notes="Counters are cumulative and deterministic for this scenario order; "
                 "cooldown/availability fields are volatile and omitted",
           volatile=True,
           request_headers={"Authorization": "Bearer [set]"})

    # 93 — Stage 5: cost guard snapshot (config + counters + window loads)
    r = client.get(f"{ROUTER_URL}/admin/costguard", headers={"Authorization": AUTH_HEADER})
    record("golden_93_admin_costguard",
           "GET /admin/costguard — limiters, in-window loads, pacer, watchdog, "
           "reconciliation, billing-visibility cross-check",
           None, "/admin/costguard", "GET", r.status_code,
           scrub_cg(r.json(), VK_MAP), None,
           notes="vk ids mapped to VK_* labels; watchdog event ts scrubbed; "
                 "counters deterministic for this scenario order; pacer "
                 "burst 50 >> capture traffic so pace_waits stays 0",
           volatile=True,
           request_headers={"Authorization": "Bearer [set]"})

    return fixtures


def capture_snapshots(client: httpx.Client) -> list[dict]:
    """Health/metrics snapshots — structure-only (values are volatile)."""
    snaps: list[dict] = []
    # /health is intentionally OPEN (liveness probes carry no credentials)
    r = client.get(f"{ROUTER_URL}/health")
    snaps.append({
        "name": "golden_90_health",
        "description": "GET /health — open liveness surface, no auth required",
        "request": {"method": "GET", "path": "/health", "headers": {}, "body": None},
        "response": {"status": r.status_code,
                     "body": {"status": r.json().get("status"),
                              "sessions_keys": sorted(r.json().keys())}},
        "trace_events": [], "volatile": True,
        "notes": "Body structure only; values vary per capture",
    })
    # /metrics is auth-protected since Stage 1 (operational intel + counters)
    r = client.get(f"{ROUTER_URL}/metrics", headers={"Authorization": AUTH_HEADER})
    m = r.json().get("metrics", {})
    prom_names = sorted(
        line.split()[0] for line in r.json().get("prometheus", "").splitlines()
        if line.startswith("hermes_router_")
    )
    snaps.append({
        "name": "golden_91_metrics",
        "description": "GET /metrics — auth-protected metric key inventory (counters volatile)",
        "request": {"method": "GET", "path": "/metrics",
                    "headers": {"Authorization": "Bearer [set]"}, "body": None},
        "response": {"status": r.status_code,
                     "body": {"metric_keys": sorted(m.keys()),
                              "prometheus_metric_names": prom_names}},
        "trace_events": [], "volatile": True,
        "notes": "Names/keys only; values vary per capture",
    })
    return snaps


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    import yaml

    tmp = Path(tempfile.mkdtemp(prefix="golden-capture-"))
    trace_dir = tmp / "traces"

    # 1. Runtime copy: upstream server + fixture config, untouched
    for fname in RUNTIME_FILES:
        shutil.copy2(REPO / fname, tmp / fname)
    shutil.copy2(CONFIG_SRC, tmp / "router_config.yaml")

    # 2. Stub upstream in background thread
    stub_server = uvicorn.Server(uvicorn.Config(
        build_stub_app(), host="127.0.0.1", port=STUB_PORT,
        log_level="error", access_log=False))
    threading.Thread(target=stub_server.run, daemon=True).start()

    # 3. Router subprocess (runs upstream server.py unmodified)
    env = {"PATH": "/usr/bin:/bin", "HOME": str(tmp),
           "TRACE_LOG_DIR": str(trace_dir), "TRACE_LOG_ENABLED": "true",
           "PYTHONUNBUFFERED": "1",
           "HERMES_ROUTER_FIXED_EPOCH": str(FIXED_CREATED),
           "GOLDEN_ROUTER_KEY": GOLDEN_ROUTER_KEY,
           "GOLDEN_POOL_KEY_A": POOL_KEY_A, "GOLDEN_POOL_KEY_B": POOL_KEY_B,
           "GOLDEN_POOL_KEY_C": POOL_KEY_C, "GOLDEN_POOL_KEY_D": POOL_KEY_D,
           "GOLDEN_POOL_KEY_E": POOL_KEY_E, "GOLDEN_POOL_KEY_F": POOL_KEY_F,
           "GOLDEN_POOL_KEY_G": POOL_KEY_G}
    env.update({k: v for k, v in __import__("os").environ.items()
                if k.startswith(("LC_", "LANG_"))})
    router_log = open(tmp / "router.stderr.log", "w")
    proc = subprocess.Popen(
        [sys.executable, str(tmp / "server.py")],
        cwd=tmp, env=env, stdout=router_log, stderr=router_log)
    try:
        wait_ready(f"{STUB_URL.lstrip('/').replace('/v1', '')}" if False else "http://127.0.0.1:9901/health")
        wait_ready(f"{ROUTER_URL}/health")

        trace_path = trace_dir / f"router-trace-{time.strftime('%Y%m%d', time.gmtime())}.jsonl"
        with httpx.Client(timeout=30.0) as client:
            fixtures = run_scenarios(client, trace_path)
            fixtures += capture_snapshots(client)

        index = {
            "captured": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "upstream_commit": subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True,
                text=True).stdout.strip(),
            "router_config": yaml.safe_load(CONFIG_SRC.read_text()),
            "notes": [
                "Captured from the staged server against a deterministic dual-protocol stub",
                "All tier selections use /use: overrides (classifier tiers bypassed)",
                "Volatile fields normalized; see 'volatile' flag per fixture",
                "Stage 2: trace route events carry provider; scenarios 10-14 exercise adapters",
                "Stage 3: scenarios 15-17 exercise key-pool rotation/exhaustion/streaming; "
                "route events carry key_id, key_rotation events carry from_key_id",
                "Stage 4: scenarios 18-20 exercise virtual-key attribution, streaming "
                "usage capture, blocking budgets (402) and admin gating (403); "
                "random vk ids/tokens/hashes scrubbed to VK_A/VK_B/VK_C labels",
                "Stage 5: scenarios 21-24 pin the cost guard — principal request/token "
                "limiter boundaries (VK_D/VK_E with reconciliation proof), pool-scope "
                "gate (guard_pool) and the injected-402 watchdog; snapshot 93 pins "
                "the /admin/costguard state (VK_D/VK_E labels, ts scrubbed)",
                "Re-capture at every stage boundary; diff = behavior regression signal",
            ],
            "fixtures": [f["name"] for f in fixtures],
        }

        FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
        for f in fixtures:
            (FIXTURE_DIR / f"{f['name']}.json").write_text(
                json.dumps(f, indent=2, ensure_ascii=False) + "\n")
        (FIXTURE_DIR / "golden_index.json").write_text(
            json.dumps(index, indent=2, ensure_ascii=False) + "\n")

        print(f"Captured {len(fixtures)} fixtures -> {FIXTURE_DIR}")
        for f in fixtures:
            print(f"  {f['name']}: status={f['response']['status']} "
                  f"trace_events={len(f['trace_events'])}")
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        router_log.close()
        stub_server.should_exit = True
        time.sleep(0.5)
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()

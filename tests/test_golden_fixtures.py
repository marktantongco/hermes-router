"""Golden fixture contract tests (Stage 0).

Validates the committed fixture transcripts (tests/fixtures/golden/) without
any network or server. These pin the BASELINE behavior of the unmodified
upstream server so every later stage can diff behavior against Stage 0.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "golden"
GOLDEN_FILES = sorted(
    p for p in FIXTURES.glob("golden_*.json") if p.name != "golden_index.json"
)
INDEX = FIXTURES / "golden_index.json"


# ── Suite integrity ─────────────────────────────────────────────────────────

def test_fixture_suite_is_present():
    names = [f.stem for f in GOLDEN_FILES]
    for required in [
        "golden_01_simple_nonstream",
        "golden_02_complex_nonstream",
        "golden_03_streaming",
        "golden_04_fallback_cascade",
        "golden_05a_session_first",
        "golden_05b_session_cache_hit",
        "golden_06_error_no_user",
        "golden_07_multimodal",
        "golden_08_unauthorized_no_token",
        "golden_09_unauthorized_wrong_token",
        "golden_90_health",
        "golden_91_metrics",
    ]:
        assert required in names, f"missing fixture: {required}"


def test_index_matches_files():
    idx = json.loads(INDEX.read_text())
    file_names = {f.stem for f in GOLDEN_FILES}
    assert set(idx["fixtures"]) == file_names
    assert len(idx.get("upstream_commit", "")) == 40, "index must pin upstream commit"


@pytest.mark.parametrize("path", GOLDEN_FILES, ids=lambda p: p.stem)
def test_common_schema(path):
    d = json.loads(path.read_text())
    assert d["request"]["method"] in {"GET", "POST"}
    assert d["request"]["path"].startswith("/")
    assert isinstance(d["request"].get("headers", {}), dict)
    assert isinstance(d["response"]["status"], int)
    assert isinstance(d["trace_events"], list)
    assert "volatile" in d


# ── Behavior contracts (baseline) ────────────────────────────────────────────

def _load(name):
    return json.loads((FIXTURES / f"{name}.json").read_text())


def test_01_simple_nonstream_contract():
    d = _load("golden_01_simple_nonstream")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert body["model"] == "stub-simple"
    assert body["object"] == "chat.completion"
    assert body["choices"][0]["message"]["role"] == "assistant"
    assert body["id"] == "chatcmpl-stub-golden"  # stub id passes through untouched
    assert d["trace_events"] == [{
        "event": "route", "tier": "simple", "model": "stub-simple",
        "upstream_status": 200, "stream": False, "provider": "openai",
    }]  # Stage 2: route events carry provider attribution


def test_02_complex_nonstream_contract():
    d = _load("golden_02_complex_nonstream")
    assert d["response"]["body"]["model"] == "stub-complex"
    assert d["trace_events"][0]["tier"] == "complex"


def test_03_streaming_contract():
    d = _load("golden_03_streaming")
    sse = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert all(isinstance(line, str) and line for line in sse)
    assert sum(1 for chunk in sse if '"delta"' in chunk) >= 3, "expect >=3 delta chunks"
    assert sse[-1] == "data: [DONE]"
    assert d["trace_events"][0]["stream"] is True


def test_04_fallback_cascade_contract():
    d = _load("golden_04_fallback_cascade")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert body["model"] == "stub-fallback"
    assert body["hermes_router"]["fallback_used"] is True
    events = d["trace_events"]
    assert [e["event"] for e in events] == ["route", "route"]
    assert events[0]["upstream_status"] == 500
    assert events[0]["fallback_level"] == 1
    assert events[0]["fallback_model"] == "stub-fallback"
    assert events[1]["upstream_status"] == 200
    assert events[1]["model"] == "stub-fallback"


def test_05_session_cache_contract():
    a = _load("golden_05a_session_first")
    b = _load("golden_05b_session_cache_hit")
    # Same first user message -> same routing decision without reclassify
    assert a["response"]["body"]["model"] == "stub-simple"
    assert b["response"]["body"]["model"] == "stub-simple"
    assert [e["event"] for e in b["trace_events"]] == ["cache_hit", "route"]
    assert b["trace_events"][0]["tier"] == "simple"


def test_06_error_no_user_contract():
    d = _load("golden_06_error_no_user")
    assert d["response"]["status"] == 400
    err = d["response"]["body"]["error"]
    assert err["type"] == "router_error"
    assert "user message" in err["message"].lower()


def test_07_multimodal_contract():
    d = _load("golden_07_multimodal")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert body["model"] == "stub-complex"
    # List content is forwarded untouched (override prefix retained upstream)
    sent = d["request"]["body"]["messages"][0]["content"]
    assert isinstance(sent, list) and sent[0]["type"] == "text"


# ── Stage 1 posture contracts ───────────────────────────────────────────

def test_all_protected_requests_carry_bearer():
    """Every success-path fixture must have been captured WITH auth (Stage 1)."""
    for name in ["golden_01_simple_nonstream", "golden_02_complex_nonstream",
                 "golden_03_streaming", "golden_04_fallback_cascade",
                 "golden_05a_session_first", "golden_05b_session_cache_hit",
                 "golden_07_multimodal"]:
        d = _load(name)
        auth = d["request"]["headers"].get("Authorization", "")
        assert auth.startswith("Bearer "), f"{name} captured without bearer auth"


def test_08_unauthorized_no_token_contract():
    d = _load("golden_08_unauthorized_no_token")
    assert d["response"]["status"] == 401
    # FastAPI wraps HTTPException detail: {"detail": {"error": {...}}}
    err = d["response"]["body"]["detail"]["error"]
    assert err["type"] == "auth_error"
    assert d["trace_events"] == []  # rejected before any routing


def test_09_unauthorized_wrong_token_contract():
    d = _load("golden_09_unauthorized_wrong_token")
    assert d["response"]["status"] == 401
    assert d["response"]["body"]["detail"]["error"]["type"] == "auth_error"


# ── Stage 2 provider contracts ───────────────────────────────────────────

def test_10_anthropic_nonstream_contract():
    d = _load("golden_10_anthropic_nonstream")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert body["object"] == "chat.completion"
    assert body["id"] == "msg_stub_anthropic"
    assert body["created"] == 1727000000
    assert body["choices"][0]["message"]["content"] == "Stub response for golden fixture."
    assert body["choices"][0]["finish_reason"] == "stop"
    assert body["usage"] == {"prompt_tokens": 7, "completion_tokens": 6,
                             "total_tokens": 13}
    assert body["hermes_router"]["provider"] == "anthropic"
    assert d["trace_events"][0]["provider"] == "anthropic"


def test_11_anthropic_stream_contract():
    d = _load("golden_11_anthropic_stream")
    sse = d["response"]["body"]
    assert d["response"]["status"] == 200
    chunks = [json.loads(c.removeprefix("data: ")) for c in sse if c != "data: [DONE]"]
    assert chunks[0]["choices"][0]["delta"] == {"role": "assistant", "content": ""}
    assert chunks[0]["id"] == "msg_stub_anthropic"
    content = "".join(ch["choices"][0]["delta"].get("content", "")
                      for ch in chunks[1:4] if ch.get("choices"))
    assert content == "Stub response for golden fixture."
    finishes = [ch["choices"][0]["finish_reason"] for ch in chunks
                if ch.get("choices") and ch["choices"][0]["finish_reason"]]
    assert finishes == ["stop"]
    usage_chunks = [ch for ch in chunks if ch.get("choices") == [] and "usage" in ch]
    assert usage_chunks[0]["usage"] == {"prompt_tokens": 7,
                                        "completion_tokens": 6, "total_tokens": 13}
    assert sse[-1] == "data: [DONE]"
    assert d["trace_events"][0]["provider"] == "anthropic"


def test_12_anthropic_cross_protocol_cascade_contract():
    d = _load("golden_12_anthropic_cascade")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert body["model"] == "stub-fallback"
    assert body["object"] == "chat.completion"
    assert body["hermes_router"]["fallback_used"] is True
    assert "provider" not in body["hermes_router"]  # openai producer stays unannotated
    events = d["trace_events"]
    assert events[0]["provider"] == "anthropic"      # failed anthropic primary
    assert events[0]["upstream_status"] == 500
    assert events[1]["provider"] == "openai"         # openai fallback served it
    assert events[1]["upstream_status"] == 200


def test_13_anthropic_tool_call_contract():
    d = _load("golden_13_anthropic_tool_call")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert body["choices"][0]["finish_reason"] == "tool_calls"
    msg = body["choices"][0]["message"]
    assert msg["content"] == "Checking the weather."
    assert msg["tool_calls"] == [{
        "id": "toolu_stub_01", "type": "function",
        "function": {"name": "get_weather",
                     "arguments": '{"city": "Paris"}'}}]
    assert body["usage"] == {"prompt_tokens": 12, "completion_tokens": 9,
                             "total_tokens": 21}
    assert body["hermes_router"]["provider"] == "anthropic"
    assert d["trace_events"][0]["provider"] == "anthropic"


def test_14_admin_providers_contract():
    d = _load("golden_14_admin_providers")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert set(body["registry"]) == {"openai", "anthropic"}
    assert body["registry"]["anthropic"]["protocol"] == "anthropic-messages"
    assert body["registry"]["openai"]["protocol"] == "openai-chat-completions"
    assert body["categories"]["anthropic"]["provider"] == "anthropic"
    assert body["categories"]["anthropic"]["fallback_provider"] == "openai"
    assert body["categories"]["simple"]["provider"] == "openai"


def test_90_health_open_and_91_metrics_protected():
    h = _load("golden_90_health")
    m = _load("golden_91_metrics")
    assert h["response"]["status"] == 200          # liveness stays open
    assert h["response"]["body"]["status"] == "ok"
    assert h["request"]["headers"] == {}           # captured WITHOUT credentials
    assert m["response"]["status"] == 200          # captured WITH credentials
    assert m["request"]["headers"].get("Authorization") == "Bearer [set]"
    assert "requests_total" in m["response"]["body"]["metric_keys"]

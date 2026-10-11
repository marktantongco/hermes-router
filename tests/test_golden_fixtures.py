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
        "golden_15_pool_rotation",
        "golden_16_pool_exhausted",
        "golden_17_pool_stream_rotation",
        "golden_18a_vk_a_create",
        "golden_18b_vk_b_create",
        "golden_18c_vk_a_nonstream",
        "golden_18d_vk_b_stream",
        "golden_18e_ledger_record_vk_a",
        "golden_18f_ledger_record_vk_b",
        "golden_19a_vk_c_create",
        "golden_19b_budget_last_allowed",
        "golden_19c_budget_block_402",
        "golden_19d_virtual_keys_listing",
        "golden_20_vk_admin_forbidden",
        "golden_21a_vk_d_create",
        "golden_21b_vk_d_request_one",
        "golden_21c_vk_d_request_two",
        "golden_21d_vk_d_request_block",
        "golden_22a_vk_e_create",
        "golden_22b_vk_e_reconcile_admit",
        "golden_22c_vk_e_token_block",
        "golden_23a_pool_gate_admit",
        "golden_23b_pool_gate_block",
        "golden_24a_watchdog_402",
        "golden_90_health",
        "golden_91_metrics",
        "golden_92_admin_pools",
        "golden_93_admin_costguard",
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


# ── Stage 3 key-pool contracts ───────────────────────────────────────────

def test_15_pool_rotation_contract():
    """429 on key-a cools it; the pool rotates; key-b serves the request."""
    d = _load("golden_15_pool_rotation")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert body["model"] == "stub-simple"
    assert body["id"] == "chatcmpl-stub-golden"  # openai producer: no annotation
    assert "hermes_router" not in body
    events = d["trace_events"]
    assert [e["event"] for e in events] == ["key_rotation", "route"]
    rot = events[0]
    assert rot["reason"] == "pool_429_cooldown"
    assert rot["from_key_id"] == "rotate_pool/key-a"
    assert rot["tier"] == "pooled"
    route = events[1]
    assert route["upstream_status"] == 200
    assert route["key_id"] == "rotate_pool/key-b"  # served by the healthy key


def test_16_pool_exhausted_contract():
    """All pooled keys 429 -> synthetic 429 -> route-level fallback succeeds."""
    d = _load("golden_16_pool_exhausted")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert body["model"] == "stub-fallback"
    assert body["hermes_router"]["fallback_used"] is True
    events = d["trace_events"]
    assert [e["event"] for e in events] == [
        "key_rotation", "key_rotation", "route", "route"]
    # both pool keys cooled with header-derived reasons
    assert [e["from_key_id"] for e in events[:2]] == [
        "exhaust_pool/key-c", "exhaust_pool/key-d"]
    assert all(e["reason"] == "pool_429_cooldown" for e in events[:2])
    # synthetic exhaustion engages the cascade at route level
    assert events[2]["upstream_status"] == 429
    assert events[2]["fallback_level"] == 1
    assert events[3]["upstream_status"] == 200
    assert events[3]["model"] == "stub-fallback"


def test_17_pool_stream_rotation_contract():
    """Streaming through a pool: first key 429s (circuit opens at 3rd
    consecutive failure incl. scenario 16), second key streams, circuit
    re-closes on success."""
    d = _load("golden_17_pool_stream_rotation")
    sse = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert sse[-1] == "data: [DONE]"
    assert sum(1 for chunk in sse if '"delta"' in chunk) >= 3
    events = d["trace_events"]
    assert [e["event"] for e in events] == [
        "circuit", "key_rotation", "circuit", "route"]
    assert events[0] == {"event": "circuit", "old_state": "closed",
                         "new_state": "open", "base_url": "<stub>"}
    assert events[1]["reason"] == "pool_429_cooldown_stream"
    assert events[1]["from_key_id"] == "stream_pool/key-e"
    assert events[2]["old_state"] == "open" and events[2]["new_state"] == "closed"
    assert events[3]["upstream_status"] == 200
    assert events[3]["stream"] is True
    assert events[3]["key_id"] == "stream_pool/key-f"


def test_92_admin_pools_contract():
    """Pool state after 15-17 + 23: counters deterministic, secrets absent."""
    d = _load("golden_92_admin_pools")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert body["count"] == 4
    pools = {p["name"]: p for p in body["pools"]}
    assert set(pools) == {"rotate_pool", "exhaust_pool", "stream_pool",
                          "guard_pool"}
    assert all(p["strategy"] == "round_robin" for p in pools.values())
    assert all(pools[n]["size"] == 2 for n in
               ("rotate_pool", "exhaust_pool", "stream_pool"))
    assert pools["guard_pool"]["size"] == 1
    keys = {k["label"]: k for p in pools.values() for k in p["keys"]}
    assert keys["key-a"]["total_429"] == 1 and keys["key-a"]["total_success"] == 0
    assert keys["key-b"]["total_success"] == 1
    assert keys["key-c"]["total_429"] == 1 and keys["key-d"]["total_429"] == 1
    assert keys["key-e"]["total_429"] == 1 and keys["key-f"]["total_success"] == 1
    # Stage 5: guard_pool served exactly one request (scenario 23a)
    assert keys["key-g"]["total_acquired"] == 1
    assert keys["key-g"]["total_success"] == 1
    assert keys["key-g"]["total_429"] == 0
    assert keys["key-a"]["consecutive_429"] == 1
    assert not any(k["disabled"] for k in keys.values())
    # no secret material anywhere in the admin surface
    assert "api_key" not in json.dumps(body) and "secret" not in json.dumps(body)
    assert all("ref" not in k for k in keys.values())


# ── Stage 4 billing-plane contracts ──────────────────────────────────────

def test_18_vk_create_contract():
    """Virtual keys are created via POST /admin/virtual_keys; the plaintext
    token is shown exactly once and never stored/pinned in the clear."""
    for name, subscriber in (("golden_18a_vk_a_create", "subscriber-a"),
                             ("golden_18b_vk_b_create", "subscriber-b")):
        d = _load(name)
        body = d["response"]["body"]
        assert d["response"]["status"] == 201
        assert body["name"] == subscriber
        assert body["enabled"] == 1
        assert body["budget_mode"] == "none"
        assert body["key"] == "vk-[REDACTED]"  # token redacted in the fixture
        assert d["request"]["headers"].get("Authorization") == "Bearer [set]"
        assert d["request"]["body"] == {"name": subscriber}  # no budget fields


def test_18_vk_attribution_contract():
    """Exit-gate e2e: two virtual keys, each request attributed to its key
    in the ledger; admin-key traffic (scenarios 01-17) stays unattributed."""
    a = _load("golden_18e_ledger_record_vk_a")["response"]["body"]
    b = _load("golden_18f_ledger_record_vk_b")["response"]["body"]
    assert a["count"] == 1 and b["count"] == 1  # filters are per-key exact
    rec_a, rec_b = a["records"][0], b["records"][0]
    # VK_A: free-tier simple model, non-stream, primary attempt
    assert rec_a["virtual_key_id"] == "VK_A"
    assert rec_a["tier"] == "simple" and rec_a["model"] == "stub-simple"
    assert rec_a["provider"] == "openai" and rec_a["stream"] == 0
    assert rec_a["fallback_level"] == 0
    assert rec_a["cost_total_micros"] == 0 and rec_a["price_source"] == "free"
    # VK_B: priced complex model over SSE, primary attempt (stream record
    # carries tier + level 0 — same numbering as the non-stream path)
    assert rec_b["virtual_key_id"] == "VK_B"
    assert rec_b["tier"] == "complex" and rec_b["model"] == "stub-complex"
    assert rec_b["stream"] == 1 and rec_b["fallback_level"] == 0
    assert rec_b["prompt_tokens"] == 5 and rec_b["completion_tokens"] == 6
    # integer money math: 5 tok @ 1.25 $/Mtok = 6 micros (truncated),
    # 6 tok @ 5.0 $/Mtok = 30 micros — no float drift
    assert (rec_b["cost_input_micros"], rec_b["cost_output_micros"],
            rec_b["cost_total_micros"]) == (6, 30, 36)
    assert rec_b["price_input_nano"] == 1_250_000_000
    assert rec_b["price_output_nano"] == 5_000_000_000
    # hash-chain material scrubbed to stable labels
    assert rec_a["row_hash"] == "[hash]" and rec_b["prev_hash"] == "[hash]"
    # admin traffic is NOT attributed to any virtual key
    assert a["total"] > a["count"]  # ledger holds the unattributed records too


def test_19_budget_block_contract():
    """Blocking budgets: last allowed request passes, the next is refused
    pre-routing with 402 budget_exceeded (upstream never contacted)."""
    create = _load("golden_19a_vk_c_create")["response"]["body"]
    assert create["budget_mode"] == "cost_micros"
    assert create["budget_limit"] == 36 and create["budget_period"] == "total"
    allowed = _load("golden_19b_budget_last_allowed")
    assert allowed["response"]["status"] == 200
    assert allowed["response"]["body"]["model"] == "stub-complex"
    blocked = _load("golden_19c_budget_block_402")
    assert blocked["response"]["status"] == 402
    err = blocked["response"]["body"]["error"]
    assert err["type"] == "budget_exceeded"
    assert err["detail"]["virtual_key_id"] == "VK_C"
    assert err["detail"]["budget_mode"] == "cost_micros"
    assert err["detail"]["budget_limit"] == 36 and err["detail"]["used"] == 36
    # the gate runs before routing: no trace events, stub never contacted
    assert blocked["trace_events"] == []


def test_19d_virtual_keys_listing_contract():
    """Admin listing shows all three keys with budget state; no secrets."""
    d = _load("golden_19d_virtual_keys_listing")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    vks = {v["name"]: v for v in body["virtual_keys"]}
    assert set(vks) == {"subscriber-a", "subscriber-b", "subscriber-c"}
    assert vks["subscriber-c"]["budget_mode"] == "cost_micros"
    assert vks["subscriber-c"]["budget_limit"] == 36
    assert all("key" not in v for v in body["virtual_keys"])  # no tokens


def test_20_vk_admin_forbidden_contract():
    """Virtual keys are inference-only: admin surfaces reject them (403)."""
    d = _load("golden_20_vk_admin_forbidden")
    assert d["response"]["status"] == 403
    body = d["response"]["body"]
    assert body["detail"]["error"]["type"] == "admin_only"
    assert d["request"]["headers"].get("Authorization") == "Bearer vk-[REDACTED]"


def test_no_vk_plaintext_tokens_in_fixtures():
    """Security sweep: no virtual-key token material anywhere in the suite."""
    for path in GOLDEN_FILES:
        raw = path.read_text()
        assert "vk-3" not in raw and "vk-0" not in raw, \
            f"possible plaintext token leak in {path.name}"
        assert "[REDACTED]" in raw or '"key"' not in raw, \
            f"unredacted key field in {path.name}"


# ── Stage 5 cost-guard contracts ─────────────────────────────────────────

def test_21_request_limiter_boundary_contract():
    """Principal-scope request limiter: two admits, third blocked pre-routing."""
    create = _load("golden_21a_vk_d_create")["response"]
    assert create["status"] == 201
    assert create["body"]["name"] == "subscriber-d"
    for name in ("golden_21b_vk_d_request_one", "golden_21c_vk_d_request_two"):
        d = _load(name)
        assert d["response"]["status"] == 200
        assert d["response"]["body"]["model"] == "stub-simple"
        assert d["request"]["headers"]["Authorization"] == "Bearer vk-[REDACTED]"
    blocked = _load("golden_21d_vk_d_request_block")
    assert blocked["response"]["status"] == 429
    err = blocked["response"]["body"]["error"]
    assert err["type"] == "cost_guard_rate_limited"
    detail = err["detail"]
    assert detail["limiter"] == "vk-two-requests"
    assert detail["scope"] == "principal" and detail["dimension"] == "requests"
    assert detail["identity"] == "VK_D"          # scrubbed to the stable label
    assert detail["used"] == 2 and detail["limit"] == 2
    assert detail["retry_after_seconds"] == "[computed]"  # timing-derived
    assert blocked["trace_events"] == []           # never routed


def test_22_token_reconciliation_boundary_contract():
    """Estimate-to-actual reconciliation proven by the block arithmetic:
    estimates alone (9+9=18) fit the 19-token limit; the reconciled window
    (11) + new estimate (9) = 20 does not. used=11 is the proof."""
    assert _load("golden_22a_vk_e_create")["response"]["status"] == 201
    admitted = _load("golden_22b_vk_e_reconcile_admit")
    assert admitted["response"]["status"] == 200
    blocked = _load("golden_22c_vk_e_token_block")
    assert blocked["response"]["status"] == 429
    detail = blocked["response"]["body"]["error"]["detail"]
    assert detail["limiter"] == "vk-token-budget"
    assert detail["dimension"] == "tokens"
    assert detail["identity"] == "VK_E"
    assert detail["used"] == 11 and detail["limit"] == 19
    assert blocked["trace_events"] == []


def test_23_pool_gate_boundary_contract():
    """Pool-scope ceiling at the dispatch door: first request served by the
    pooled key, second becomes a synthetic 429 (Stage 3 pool-exhaustion
    semantics) and — with no fallback tier — surfaces as the 502 envelope."""
    first = _load("golden_23a_pool_gate_admit")
    assert first["response"]["status"] == 200
    assert first["response"]["body"]["model"] == "stub-simple"
    assert first["trace_events"][0]["key_id"] == "guard_pool/key-g"
    second = _load("golden_23b_pool_gate_block")
    assert second["response"]["status"] == 502
    err = second["response"]["body"]["error"]
    assert err["type"] == "upstream_error" and err["status_code"] == 429
    assert "guard_pool" in err["message"]
    assert "guard-pool-one-request" in err["message"]
    assert "cost guard limiter" in err["message"]


def test_24_watchdog_injection_contract():
    """Injected upstream 402 (billing leak) is terminal on the no-fallback
    complex tier; the watchdog records the signal (pinned in 93)."""
    d = _load("golden_24a_watchdog_402")
    assert d["response"]["status"] == 502
    err = d["response"]["body"]["error"]
    assert err["type"] == "upstream_error" and err["status_code"] == 402
    assert "stub payment required" in err["message"]
    # no-fallback primary failure emits no route trace event (Stage 0
    # semantics: the cascade path is what gets traced)
    assert d["trace_events"] == []


def test_93_admin_costguard_contract():
    """Admin snapshot: limiter counters, window loads, pacer, watchdog and
    reconciliation are deterministic for the pinned scenario order; no
    principal ids beyond the VK_* labels; no secrets."""
    d = _load("golden_93_admin_costguard")
    body = d["response"]["body"]
    assert d["response"]["status"] == 200
    assert body["enabled"] is True
    assert body["estimate_chars_per_token"] == 4.0
    lms = {lm["name"]: lm for lm in body["limiters"]}
    assert set(lms) == {"vk-two-requests", "guard-pool-one-request",
                        "vk-token-budget", "global-requests"}
    # one block per exercised limiter; the global ceiling never fired
    assert lms["vk-two-requests"]["blocks"] == 1        # 21d
    assert lms["guard-pool-one-request"]["blocks"] == 1  # 23b
    assert lms["vk-token-budget"]["blocks"] == 1         # 22c
    assert lms["global-requests"]["blocks"] == 0
    assert lms["vk-two-requests"]["admits"] == 6          # 18c/18d/19b/21b/21c/22b
    assert lms["guard-pool-one-request"]["admits"] == 4   # one per pool identity
    assert lms["global-requests"]["admits"] == 24         # every admitted request
    # principal windows carry ONLY vk labels (admin exempt, ids scrubbed)
    for name in ("vk-two-requests", "vk-token-budget"):
        idents = set(lms[name]["identities"])
        assert idents == {"VK_A", "VK_B", "VK_C", "VK_D", "VK_E"}
    assert set(lms["guard-pool-one-request"]["identities"]) == {
        "rotate_pool", "exhaust_pool", "stream_pool", "guard_pool"}
    assert set(lms["global-requests"]["identities"]) == {"global"}
    # the reconciliation proof, as a window load: VK_E holds 11 tokens
    assert lms["vk-token-budget"]["identities"]["VK_E"]["used"] == 11
    # pacer never engaged (burst 50 >> capture traffic)
    assert body["pacer"] == {"enabled": True, "requests_per_second": 100.0,
                             "max_burst": 50, "pace_waits_total": 0,
                             "last_wait_seconds": 0.0}
    # watchdog: exactly the injected 402, ts scrubbed
    wd = body["watchdog"]
    assert wd["statuses"] == [401, 402]
    assert wd["events_total"] == 1 and wd["by_status"] == {"402": 1}
    assert wd["events"] == [{"ts": "[ts]", "status": 402,
                             "model": "stub-complex", "key_id": None}]
    assert wd["billing_visibility"] == "ok" and wd["unbilled_completions"] == 0
    # reconciliation counters for the pinned scenario order
    rec = body["reconciliation"]
    assert rec["estimates_total"] == 24
    assert rec["reconciled_total"] == 21   # completions with usage
    assert rec["unreconciled_total"] == 3  # 06 no-user + 23b pool block + 24a 402
    assert rec["drift_tokens_total"] > 0   # estimates are honest, not exact
    # no secret material on the snapshot
    raw = json.dumps(body)
    assert "poolkey" not in raw and "api_key" not in raw

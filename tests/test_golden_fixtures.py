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
        "upstream_status": 200, "stream": False,
    }]


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


def test_90_91_snapshots_are_structure_only():
    h = _load("golden_90_health")
    m = _load("golden_91_metrics")
    assert h["response"]["status"] == 200
    assert h["response"]["body"]["status"] == "ok"
    assert m["response"]["status"] == 200
    assert "requests_total" in m["response"]["body"]["metric_keys"]
    assert any("hermes_router_" in n for n in m["response"]["body"]["prometheus_metric_names"])

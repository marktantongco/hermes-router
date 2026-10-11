"""Stage 4 billing-plane unit tests (offline — no server process).

Exit-gate coverage (Chapter 9, Stage 4): ledger immutability tests
(field tampering, middle-row deletion, tail truncation, HMAC mode),
price-catalog snapshotting, exact integer cost math, virtual-key
lifecycle and blocking-budget boundaries.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ledger import (
    BUDGET_MODES,
    GENESIS_HASH,
    Ledger,
    LedgerError,
    PriceCatalog,
    PriceError,
    VirtualKeyStore,
    build_ledger,
    build_price_catalog,
    compute_cost,
    evaluate_budget,
    now_iso,
    period_start,
)

# ── Fixtures ────────────────────────────────────────────────────────────────

PRICING_CFG = {
    "pricing": {
        "default": {"input_per_mtok": 0.5, "output_per_mtok": 1.5},
        "entries": {
            "stub-simple": {"input_per_mtok": 0.0, "output_per_mtok": 0.0,
                            "free": True},
            "stub-complex": {"input_per_mtok": 1.25, "output_per_mtok": 5.0},
        },
    }
}


def make_record(**over):
    base = dict(
        record_type="usage", status="ok", session_key="sess-1", tier="complex",
        model="stub-complex", provider="openai", key_id=None,
        virtual_key_id=None, fallback_level=0, stream=0,
        prompt_tokens=5, completion_tokens=6, total_tokens=11,
        cost_input_micros=6, cost_output_micros=30, cost_total_micros=36,
        price_input_nano=1_250_000_000, price_output_nano=5_000_000_000,
        price_source="model", price_snapshot_id="snap",
    )
    base.update(over)
    return base


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "ledger.sqlite3"


@pytest.fixture()
def ledger(db_path: Path) -> Ledger:
    led = Ledger(db_path)
    led.append(make_record())
    led.append(make_record(session_key="sess-2"))
    led.append(make_record(session_key="sess-3", stream=1))
    return led


# ── Cost math (integer exactness) ───────────────────────────────────────────

def test_compute_cost_exact():
    # 1000 tokens @ 0.6 USD/MTok = 600 micros
    assert compute_cost(1000, 600_000_000) == 600
    # fixture complex: 5 @ 1.25 + 6 @ 5.0 = 6 + 30 = 36
    assert compute_cost(5, 1_250_000_000) == 6
    assert compute_cost(6, 5_000_000_000) == 30
    assert compute_cost(0, 5_000_000_000) == 0
    # rounding half-up at the sub-micro boundary
    assert compute_cost(1, 1_500_000_000) == 2  # 1.5 → 2


def test_free_model_costs_zero():
    assert compute_cost(10_000_000, 0) == 0


# ── Price catalog ───────────────────────────────────────────────────────────

def test_price_catalog_lookup_and_sources():
    cat = build_price_catalog(PRICING_CFG)
    assert cat.lookup("stub-complex").source == "model"
    assert cat.lookup("stub-complex").input_nano == 1_250_000_000
    assert cat.lookup("stub-simple").free is True
    d = cat.lookup("never-seen-model")
    assert d.source == "default" and d.input_nano == 500_000_000


def test_price_catalog_missing_section_is_unset():
    cat = build_price_catalog({})
    entry = cat.lookup("anything")
    assert entry.source == "unset"
    assert entry.input_nano == 0 and entry.output_nano == 0


def test_price_snapshot_id_changes_with_prices():
    cat_a = build_price_catalog(PRICING_CFG)
    changed = json.loads(json.dumps(PRICING_CFG))
    changed["pricing"]["entries"]["stub-complex"]["input_per_mtok"] = 9.9
    cat_b = build_price_catalog(changed)
    assert cat_a.snapshot_id != cat_b.snapshot_id
    assert build_price_catalog(PRICING_CFG).snapshot_id == cat_a.snapshot_id


def test_price_catalog_negative_price_fails_closed():
    with pytest.raises(PriceError):
        build_price_catalog({"pricing": {"default": {"input_per_mtok": -1}}})


def test_price_catalog_non_numeric_fails_closed():
    with pytest.raises(PriceError):
        build_price_catalog({"pricing": {"default": {"input_per_mtok": "abc"}}})


# ── Ledger chain integrity ──────────────────────────────────────────────────

def test_append_assigns_seq_and_genesis(tmp_path: Path):
    fresh = Ledger(tmp_path / "fresh.sqlite3")
    seq = fresh.append(make_record())
    assert seq == 1
    conn = sqlite3.connect(tmp_path / "fresh.sqlite3")
    prev, row_hash = conn.execute(
        "SELECT prev_hash, row_hash FROM ledger WHERE seq=1").fetchone()
    conn.close()
    assert prev == GENESIS_HASH
    assert len(row_hash) == 64


def test_verify_ok(ledger: Ledger):
    report = ledger.verify()
    assert report["ok"] is True
    assert report["records"] == 3
    assert report["first_bad_seq"] is None


def test_verify_detects_field_tampering(ledger: Ledger, db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE ledger SET prompt_tokens=999 WHERE seq=2")
    conn.commit()
    conn.close()
    report = ledger.verify()
    assert report["ok"] is False
    assert report["first_bad_seq"] == 2
    assert "hash" in report["reason"]


def test_verify_detects_middle_deletion(ledger: Ledger, db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM ledger WHERE seq=2")
    conn.commit()
    conn.close()
    report = ledger.verify()
    assert report["ok"] is False
    assert "linkage" in report["reason"]


def test_verify_detects_tail_truncation(ledger: Ledger, db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM ledger WHERE seq=3")
    conn.commit()
    conn.close()
    report = ledger.verify()
    assert report["ok"] is False
    assert "truncation" in report["reason"]


def test_verify_detects_head_mutation(ledger: Ledger, db_path: Path):
    """Overwriting the meta head must not hide a truncated tail."""
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM ledger WHERE seq=3")
    conn.execute("UPDATE meta SET v='2' WHERE k='chain_head_seq'")
    conn.commit()
    conn.close()
    assert ledger.verify()["ok"] is False


def test_hmac_chain_tamper_proof(db_path: Path):
    led = Ledger(db_path, hmac_key=b"secret-key")
    led.append(make_record())
    assert led.verify()["ok"]
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE ledger SET model='forged'")
    conn.commit()
    conn.close()
    assert led.verify()["ok"] is False


def test_hmac_chain_differs_from_plain(db_path: Path, tmp_path: Path):
    plain = Ledger(tmp_path / "plain.sqlite3")
    keyed = Ledger(db_path, hmac_key=b"secret-key")
    plain.append(make_record())
    keyed.append(make_record())
    assert plain.records()[0]["row_hash"] != keyed.records()[0]["row_hash"]


def test_append_rejects_unknown_fields(ledger: Ledger):
    with pytest.raises(LedgerError):
        ledger.append(make_record(bogus_field=1))


def test_append_rejects_wrong_types(ledger: Ledger):
    with pytest.raises(LedgerError):
        ledger.append(make_record(prompt_tokens="5"))  # type: ignore[arg-type]


def test_records_query_filters_by_vk(ledger: Ledger):
    rows = ledger.records(virtual_key_id="vk-x")
    assert rows == []
    rows = ledger.records(limit=2)
    assert [r["seq"] for r in rows] == [3, 2]  # newest first


def test_summary_groups(ledger: Ledger):
    by_model = ledger.summary("model")
    assert len(by_model) == 1
    assert by_model[0]["dim"] == "stub-complex"
    assert by_model[0]["requests"] == 3
    assert by_model[0]["cost_total_micros"] == 108  # 3 x 36
    by_day = ledger.summary("day")
    assert len(by_day) == 1  # all records share one UTC day
    with pytest.raises(LedgerError):
        ledger.summary("nope")


def test_spent_for_key_windows(ledger: Ledger, tmp_path: Path):
    vk_store = VirtualKeyStore(ledger.path)
    vk = vk_store.create("spender", "cost_micros", 100, "total")
    ledger.append(make_record(virtual_key_id=vk["id"]))
    spent = ledger.spent_for_key(vk["id"], "total")
    assert spent["requests"] == 1
    assert spent["cost_total_micros"] == 36
    assert ledger.spent_for_key("vk-none", "total")["requests"] == 0


# ── Budget windows ──────────────────────────────────────────────────────────

def test_period_start_bounds():
    now = datetime(2026, 9, 23, 15, 30, 45, tzinfo=timezone.utc)
    assert period_start("day", now) == "2026-09-23T00:00:00.000Z"
    assert period_start("month", now) == "2026-09-01T00:00:00.000Z"
    assert period_start("total", now) == ""
    with pytest.raises(LedgerError):
        period_start("week", now)


def test_evaluate_budget_boundaries(ledger: Ledger, tmp_path: Path):
    vk_store = VirtualKeyStore(ledger.path)
    vk = vk_store.create("bounded", "cost_micros", 72, "total")
    ledger.append(make_record(virtual_key_id=vk["id"]))          # 36
    assert evaluate_budget(ledger, vk)["blocked"] is False       # 36 < 72
    ledger.append(make_record(virtual_key_id=vk["id"]))          # 72
    assert evaluate_budget(ledger, vk)["blocked"] is True        # 72 >= 72


def test_evaluate_budget_token_mode(ledger: Ledger):
    ledger.append(make_record(virtual_key_id="vk-tok"))  # 11 tokens
    vk = {"id": "vk-tok", "budget_mode": "tokens", "budget_limit": 44,
          "budget_period": "total"}
    assert evaluate_budget(ledger, vk)["blocked"] is False   # 11 < 44
    vk["budget_limit"] = 11
    assert evaluate_budget(ledger, vk)["blocked"] is True    # 11 >= 11


def test_evaluate_budget_mode_none_never_blocks(ledger: Ledger):
    vk = {"id": "vk-free", "budget_mode": "none", "budget_limit": 0,
          "budget_period": "total"}
    assert evaluate_budget(ledger, vk)["blocked"] is False


def test_evaluate_budget_zero_limit_blocks_all(ledger: Ledger):
    vk = {"id": "vk-zero", "budget_mode": "cost_micros", "budget_limit": 0,
          "budget_period": "total"}
    assert evaluate_budget(ledger, vk)["blocked"] is True


def test_budget_modes_and_periods_constants():
    assert BUDGET_MODES == ("none", "tokens", "cost_micros")
    assert set(BUDGET_MODES) | set() == {"none", "tokens", "cost_micros"}


# ── Virtual key store ───────────────────────────────────────────────────────

def test_vk_create_authenticate_roundtrip(db_path: Path):
    store = VirtualKeyStore(db_path)
    vk = store.create("alice", "cost_micros", 100, "day")
    assert vk["key"].startswith("vk-") and len(vk["key"]) == 35
    assert vk["budget_mode"] == "cost_micros"
    got = store.authenticate(vk["key"])
    assert got is not None and got["id"] == vk["id"]
    assert "key_hash" in got and vk["key"] not in json.dumps(got)


def test_vk_wrong_token_and_disabled(db_path: Path):
    store = VirtualKeyStore(db_path)
    vk = store.create("bob")
    assert store.authenticate("vk-" + "0" * 32) is None
    assert store.authenticate("") is None
    store.set_enabled(vk["id"], False)
    assert store.authenticate(vk["key"]) is None
    store.set_enabled(vk["id"], True)
    assert store.authenticate(vk["key"]) is not None


def test_vk_unique_names(db_path: Path):
    store = VirtualKeyStore(db_path)
    store.create("same-name")
    with pytest.raises(LedgerError):
        store.create("same-name")


def test_vk_budget_validation(db_path: Path):
    store = VirtualKeyStore(db_path)
    with pytest.raises(LedgerError):
        store.create("x", budget_mode="credits")
    with pytest.raises(LedgerError):
        store.create("x", budget_period="week")
    with pytest.raises(LedgerError):
        store.create("x", budget_limit=-5)
    vk = store.create("x", budget_mode="tokens", budget_limit=10)
    assert store.update_budget(vk["id"], "cost_micros", 5, "month")
    assert store.get(vk["id"])["budget_limit"] == 5
    with pytest.raises(LedgerError):
        store.update_budget(vk["id"], "nope", 5, "month")


def test_vk_delete_keeps_ledger_rows(db_path: Path):
    led = Ledger(db_path)
    store = VirtualKeyStore(db_path)
    vk = store.create("gone")
    led.append(make_record(virtual_key_id=vk["id"]))
    assert store.delete(vk["id"]) is True
    assert store.delete(vk["id"]) is False
    assert store.authenticate(vk["key"]) is None
    assert led.records(virtual_key_id=vk["id"])[0]["model"] == "stub-complex"


def test_vk_list_shape(db_path: Path):
    store = VirtualKeyStore(db_path)
    store.create("a")
    store.create("b", "tokens", 5, "day")
    rows = store.list()
    assert len(rows) == 2
    assert all("key_hash" in r and "key" not in r for r in rows)


# ── Bootstrap ───────────────────────────────────────────────────────────────

def test_build_ledger_disabled(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    led, vk, cat = build_ledger({"ledger": {"enabled": False}})
    assert led is None and vk is None
    assert isinstance(cat, PriceCatalog)


def test_build_ledger_default_enabled(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LEDGER_DB", raising=False)
    led, vk, cat = build_ledger({})
    assert led is not None and vk is not None
    assert led.path == Path(".router/ledger.sqlite3")
    led.append(make_record())
    assert led.verify()["ok"]


def test_build_ledger_bad_hmac_ref_fails_closed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    bad = {"ledger": {"enabled": True, "db_path": str(tmp_path / "x.sqlite3"),
                      "hmac_key_env": "ks:missing-alias"}}
    with pytest.raises(LedgerError):
        build_ledger(bad)


def test_timestamp_format_matches_trace():
    ts = now_iso()
    assert ts.endswith("Z") and len(ts) == 24
    assert "T" in ts and "." in ts

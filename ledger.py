"""Immutable cost ledger for the Hermes Router-Proxy (Stage 4 billing plane).

Clean-room implementation (conflict rule R4): written from the agreed
specification only — no code, schema dumps, or templates were taken from any
AGPL-licensed repository. The polyrouter billing chapter was used as a
requirements source at spec level; everything here is first-party.

Purpose
-------
Every successful upstream completion becomes ONE append-only, hash-chained
cost record in a SQLite (WAL) database:

    row_hash = HMAC-SHA256(secret, canonical_json(record))    # secret optional
             = SHA-256(canonical_json(record))                # no secret set

where ``record`` embeds ``seq``, ``prev_hash`` and the full billing fact
(model, provider, key attribution, virtual-key attribution, token counts,
integer micro-USD costs, and the exact price snapshot used). Appends are
serialized; nothing ever UPDATEs or DELETEs a ledger row, so:

  * editing any field breaks that row's recomputed hash,
  * deleting a middle row breaks the prev_hash linkage,
  * truncating the tail is caught against the ``meta`` chain head,
  * all three are detected by ``Ledger.verify()`` (and ``python -m ledger
    verify`` offline).

Money math is integer-only: prices are stored as integer nano-USD per
million tokens (``price_*_nano``) and costs as integer micro-USD
(``cost_*_micros``) — no floating-point drift accumulates in sums.

Virtual keys
------------
Downstream subscribers authenticate with virtual keys (``vk-<32 hex>``).
Only the SHA-256 of the token is stored. Each request presented with a
virtual key is attributed to it in the ledger and can be blocked BEFORE
routing when its budget is exhausted (mode ``tokens`` or ``cost_micros``
over a ``day`` / ``month`` / ``total`` window). The window sums come from
the ledger itself, so attribution, budgets and billing all read the same
immutable facts.

Configuration (router_config.yaml)::

    ledger:
      enabled: true                    # default true
      db_path: ".router/ledger.sqlite3"  # env LEDGER_DB overrides
      hmac_key_env: ""                 # env name or ks: alias; empty = SHA-256
    pricing:
      default: {input_per_mtok: 0.5, output_per_mtok: 1.5}
      entries:
        "glm-4.7": {input_per_mtok: 0.6, output_per_mtok: 2.2}
        "free-model": {input_per_mtok: 0.0, output_per_mtok: 0.0, free: true}

License: MIT
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

__all__ = [
    "LedgerError", "PriceError", "GENESIS_HASH", "PriceCatalog",
    "PriceEntry", "Ledger", "VirtualKeyStore", "build_price_catalog",
    "build_ledger", "evaluate_budget", "compute_cost", "now_iso",
    "RECORD_FIELDS", "BUDGET_MODES", "BUDGET_PERIODS",
]

# ── Constants ───────────────────────────────────────────────────────────────

GENESIS_HASH = "0" * 64
SCHEMA_VERSION = "4"
DB_ENV = "LEDGER_DB"
DEFAULT_DB = ".router/ledger.sqlite3"

BUDGET_MODES = ("none", "tokens", "cost_micros")
BUDGET_PERIODS = ("day", "month", "total")

# Single source of truth for the hashed record shape. Order matters for the
# SQLite schema; the hash canonicalizes with sort_keys, so only membership
# and types matter there. ``row_hash`` itself is excluded by design.
RECORD_FIELDS: dict[str, type | tuple[type, type]] = {
    "seq": int,
    "ts": str,
    "record_type": str,          # "usage" | "usage_missing"
    "status": str,               # "ok" | "no_usage" | "interrupted"
    "session_key": (str, type(None)),
    "tier": (str, type(None)),
    "model": str,
    "provider": (str, type(None)),
    "key_id": (str, type(None)),
    "virtual_key_id": (str, type(None)),
    "fallback_level": int,
    "stream": int,               # 0 | 1 (kept int for hash determinism)
    "prompt_tokens": int,
    "completion_tokens": int,
    "total_tokens": int,
    "cost_input_micros": int,
    "cost_output_micros": int,
    "cost_total_micros": int,
    "price_input_nano": int,
    "price_output_nano": int,
    "price_source": str,         # "model" | "default" | "free" | "unset"
    "price_snapshot_id": str,
    "prev_hash": str,
}


class LedgerError(RuntimeError):
    """Raised on ledger open/append/verify failures (fail closed)."""


class PriceError(RuntimeError):
    """Raised on invalid pricing configuration (fail closed)."""


def now_iso() -> str:
    """UTC timestamp in the same format as trace.py (ms precision, Z suffix)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def compute_cost(tokens: int, price_nano: int) -> int:
    """Micro-USD for ``tokens`` at ``price_nano`` nano-USD per million tokens.

    Exact integer math: micros = tokens * nano / 1e9, rounded half-up.
    (1 USD = 1e6 micros; 1 USD/Mtok = 1e9 nano/Mtok; tokens * nano / 1e6
    tokens/Mtok / 1e9 nano/USD → USD → *1e6 micros → /1e9.)
    """
    return (tokens * price_nano + 500_000_000) // 1_000_000_000


def _canonical(fields: dict[str, Any]) -> bytes:
    return json.dumps(
        fields, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")


def _row_hash(fields: dict[str, Any], hmac_key: bytes | None) -> str:
    material = _canonical(fields)
    if hmac_key:
        return hmac.new(hmac_key, material, hashlib.sha256).hexdigest()
    return hashlib.sha256(material).hexdigest()


# ── Price catalog ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PriceEntry:
    """Resolved price for one model (integer nano-USD per million tokens)."""

    input_nano: int
    output_nano: int
    free: bool
    source: str  # "model" | "default" | "unset"


def _nano_from_cfg(raw: Any, label: str) -> int:
    """Convert a per-Mtok USD price (int/float/str) to integer nano-USD."""
    if isinstance(raw, bool) or not isinstance(raw, (int, float, str)):
        raise PriceError(f"pricing.{label} must be a number, got {raw!r}")
    try:
        usd = float(raw)
    except ValueError as exc:
        raise PriceError(f"pricing.{label} not a number: {raw!r}") from exc
    if usd < 0:
        raise PriceError(f"pricing.{label} must be >= 0, got {usd}")
    nano = round(usd * 1_000_000_000)
    if nano <= 0 and usd > 0:
        nano = 1  # smallest representable unit (never silently free)
    return nano


def _entry_from_cfg(raw: Any, label: str, source: str) -> PriceEntry:
    if raw is None:
        return PriceEntry(0, 0, False, "unset" if source == "default" else source)
    if not isinstance(raw, dict):
        raise PriceError(f"pricing.{label} must be a mapping, got {type(raw).__name__}")
    free = bool(raw.get("free", False))
    return PriceEntry(
        input_nano=_nano_from_cfg(raw.get("input_per_mtok", 0), f"{label}.input_per_mtok"),
        output_nano=_nano_from_cfg(raw.get("output_per_mtok", 0), f"{label}.output_per_mtok"),
        free=free,
        source=source,
    )


class PriceCatalog:
    """Immutable price lookup with a content-hashed snapshot id.

    The snapshot id pins WHICH price table produced a ledger record's costs.
    Repricing later changes the id for new records only — history keeps the
    recorded integer prices, so old rows never reinterpret.
    """

    def __init__(self, entries: dict[str, PriceEntry], default: PriceEntry):
        self._entries = dict(entries)
        self._default = default
        payload = {
            "default": {"in": default.input_nano, "out": default.output_nano,
                        "free": default.free},
            "entries": {
                m: {"in": e.input_nano, "out": e.output_nano, "free": e.free}
                for m, e in sorted(entries.items())
            },
        }
        self.snapshot_id = hashlib.sha256(
            _canonical(payload)).hexdigest()[:16]

    def lookup(self, model: str) -> PriceEntry:
        entry = self._entries.get(model)
        if entry is not None:
            return entry
        if self._default.source == "unset":
            return PriceEntry(0, 0, False, "unset")
        return PriceEntry(self._default.input_nano, self._default.output_nano,
                          self._default.free, "default")

    def describe(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "default": {"input_nano": self._default.input_nano,
                        "output_nano": self._default.output_nano,
                        "free": self._default.free, "source": self._default.source},
            "models": {m: {"input_nano": e.input_nano, "output_nano": e.output_nano,
                           "free": e.free, "source": e.source}
                       for m, e in sorted(self._entries.items())},
        }


def build_price_catalog(cfg: dict) -> PriceCatalog:
    """Build from ``cfg["pricing"]``. Missing section → unset (costs 0, marked)."""
    raw = cfg.get("pricing") or {}
    if not isinstance(raw, dict):
        raise PriceError("pricing section must be a mapping")
    entries_raw = raw.get("entries") or {}
    if not isinstance(entries_raw, dict):
        raise PriceError("pricing.entries must be a mapping")
    entries = {str(m): _entry_from_cfg(e, f"entries.{m}", "model")
               for m, e in entries_raw.items()}
    default = _entry_from_cfg(raw.get("default"), "default", "default")
    return PriceCatalog(entries, default)


# ── Ledger (SQLite, WAL, hash-chained) ──────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger (
    seq INTEGER PRIMARY KEY,
    ts TEXT NOT NULL,
    record_type TEXT NOT NULL,
    status TEXT NOT NULL,
    session_key TEXT,
    tier TEXT,
    model TEXT NOT NULL,
    provider TEXT,
    key_id TEXT,
    virtual_key_id TEXT,
    fallback_level INTEGER NOT NULL DEFAULT 0,
    stream INTEGER NOT NULL DEFAULT 0,
    prompt_tokens INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    cost_input_micros INTEGER NOT NULL DEFAULT 0,
    cost_output_micros INTEGER NOT NULL DEFAULT 0,
    cost_total_micros INTEGER NOT NULL DEFAULT 0,
    price_input_nano INTEGER NOT NULL DEFAULT 0,
    price_output_nano INTEGER NOT NULL DEFAULT 0,
    price_source TEXT NOT NULL,
    price_snapshot_id TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    row_hash TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_ledger_vk_ts ON ledger(virtual_key_id, ts);
CREATE INDEX IF NOT EXISTS idx_ledger_ts ON ledger(ts);
CREATE TABLE IF NOT EXISTS virtual_keys (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    budget_mode TEXT NOT NULL DEFAULT 'none',
    budget_limit INTEGER NOT NULL DEFAULT 0,
    budget_period TEXT NOT NULL DEFAULT 'total'
);
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
"""


class Ledger:
    """Append-only hash-chained cost ledger (single writer, WAL readers)."""

    def __init__(self, db_path: Path | str, hmac_key: bytes | None = None):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._hmac_key = hmac_key
        self._lock = threading.Lock()
        try:
            self._conn = sqlite3.connect(
                str(self.path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.executescript(_SCHEMA)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.commit()
        except sqlite3.Error as exc:
            raise LedgerError(f"cannot open ledger db {self.path}: {exc}") from exc
        with self._lock:
            meta = dict(self._conn.execute("SELECT k, v FROM meta").fetchall())
            if "schema_version" not in meta:
                self._conn.execute(
                    "INSERT INTO meta(k, v) VALUES('schema_version', ?)",
                    (SCHEMA_VERSION,))
                self._conn.execute(
                    "INSERT INTO meta(k, v) VALUES('chain_head_seq', '0')")
                self._conn.execute(
                    "INSERT INTO meta(k, v) VALUES('chain_head_hash', ?)",
                    (GENESIS_HASH,))
                self._conn.commit()
            elif meta.get("schema_version") != SCHEMA_VERSION:
                raise LedgerError(
                    f"ledger schema version mismatch: db={meta['schema_version']} "
                    f"code={SCHEMA_VERSION}")

    # ── append ──────────────────────────────────────────────────────────
    def append(self, record: dict[str, Any]) -> int:
        """Append one record; assigns seq/ts/prev_hash/row_hash. Returns seq."""
        unknown = set(record) - set(RECORD_FIELDS)
        if unknown:
            raise LedgerError(f"unknown ledger fields: {sorted(unknown)}")
        fields = {k: record.get(k) for k in RECORD_FIELDS}
        fields["ts"] = fields.get("ts") or now_iso()  # server may supply its own
        with self._lock:
            try:
                head = self._conn.execute(
                    "SELECT v FROM meta WHERE k='chain_head_hash'").fetchone()
                seq_row = self._conn.execute(
                    "SELECT v FROM meta WHERE k='chain_head_seq'").fetchone()
                prev_hash = head["v"] if head else GENESIS_HASH
                seq = int(seq_row["v"]) + 1 if seq_row else 1
                fields["seq"] = seq
                fields["prev_hash"] = prev_hash
                for name, want in RECORD_FIELDS.items():
                    val = fields[name]
                    allowed = want if isinstance(want, tuple) else (want,)
                    if val is None:
                        if type(None) in allowed:
                            continue
                        raise LedgerError(f"missing ledger field: {name}")
                    if not isinstance(val, allowed):
                        raise LedgerError(
                            f"ledger field {name} must be "
                            f"{'/'.join(a.__name__ for a in allowed)}, "
                            f"got {type(val).__name__}")
                row_hash = _row_hash(fields, self._hmac_key)
                cols = ", ".join(RECORD_FIELDS)
                marks = ", ".join("?" for _ in RECORD_FIELDS)
                # B608 safe: cols/marks derive from the RECORD_FIELDS constant,
                # never from user input; values are fully parameterized.
                self._conn.execute(
                    f"INSERT INTO ledger({cols}, row_hash) VALUES({marks}, ?)",  # nosec B608
                    [fields[k] for k in RECORD_FIELDS] + [row_hash])
                self._conn.execute(
                    "UPDATE meta SET v=? WHERE k='chain_head_seq'", (str(seq),))
                self._conn.execute(
                    "UPDATE meta SET v=? WHERE k='chain_head_hash'", (row_hash,))
                self._conn.commit()
            except sqlite3.Error as exc:
                self._conn.rollback()
                raise LedgerError(f"ledger append failed: {exc}") from exc
        return seq

    # ── verification ────────────────────────────────────────────────────
    def verify(self) -> dict[str, Any]:
        """Recompute the full chain. Returns {ok, records, first_bad_seq, reason}."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM ledger ORDER BY seq ASC").fetchall()
            meta = dict(self._conn.execute("SELECT k, v FROM meta").fetchall())
        prev = GENESIS_HASH
        for row in rows:
            fields = {k: row[k] for k in RECORD_FIELDS}
            if row["prev_hash"] != prev:
                return {"ok": False, "records": row["seq"] - 1,
                        "first_bad_seq": row["seq"],
                        "reason": "prev_hash linkage break"}
            if _row_hash(fields, self._hmac_key) != row["row_hash"]:
                return {"ok": False, "records": row["seq"] - 1,
                        "first_bad_seq": row["seq"],
                        "reason": "row hash mismatch (field tampering)"}
            prev = row["row_hash"]
        head_seq = int(meta.get("chain_head_seq", "0"))
        head_hash = meta.get("chain_head_hash", GENESIS_HASH)
        last_seq = rows[-1]["seq"] if rows else 0
        last_hash = rows[-1]["row_hash"] if rows else GENESIS_HASH
        if (head_seq, head_hash) != (last_seq, last_hash):
            return {"ok": False, "records": len(rows),
                    "first_bad_seq": last_seq + 1,
                    "reason": "chain head mismatch (tail truncation)"}
        return {"ok": True, "records": len(rows), "first_bad_seq": None,
                "reason": None}

    # ── queries ─────────────────────────────────────────────────────────
    def records(self, limit: int = 50, offset: int = 0,
                virtual_key_id: str | None = None) -> list[dict[str, Any]]:
        q = "SELECT * FROM ledger"
        args: list[Any] = []
        if virtual_key_id is not None:
            q += " WHERE virtual_key_id = ?"
            args.append(virtual_key_id)
        q += " ORDER BY seq DESC LIMIT ? OFFSET ?"
        args += [max(0, min(limit, 500)), max(0, offset)]
        with self._lock:
            return [dict(r) for r in self._conn.execute(q, args).fetchall()]

    def summary(self, group_by: str, since: str = "", until: str = "",
                virtual_key_id: str | None = None) -> list[dict[str, Any]]:
        """Aggregate spend. group_by: model | key | session | day."""
        dims = {
            "model": "model",
            "key": "COALESCE(key_id, 'none')",
            "session": "COALESCE(session_key, 'none')",
            "day": "substr(ts, 1, 10)",
        }
        if group_by not in dims:
            raise LedgerError(f"group_by must be one of {sorted(dims)}")
        dim = dims[group_by]  # B608 safe: dim is from the closed set above
        where, args = [], []
        if since:
            where.append("ts >= ?")
            args.append(since)
        if until:
            where.append("ts <= ?")
            args.append(until)
        if virtual_key_id is not None:
            where.append("virtual_key_id = ?")
            args.append(virtual_key_id)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        # B608 safe: dim comes from the fixed `dims` mapping; all values are
        # parameterized below.
        q = (f"SELECT {dim} AS dim, COUNT(*) AS requests, "  # nosec B608
             "SUM(prompt_tokens) AS prompt_tokens, "
             "SUM(completion_tokens) AS completion_tokens, "
             "SUM(total_tokens) AS total_tokens, "
             "SUM(cost_input_micros) AS cost_input_micros, "
             "SUM(cost_output_micros) AS cost_output_micros, "
             "SUM(cost_total_micros) AS cost_total_micros "
             f"FROM ledger {clause} GROUP BY dim ORDER BY dim")
        with self._lock:
            return [dict(r) for r in self._conn.execute(q, args).fetchall()]

    def count(self) -> int:
        with self._lock:
            return int(self._conn.execute(
                "SELECT COUNT(*) AS n FROM ledger").fetchone()["n"])

    def spent_for_key(self, virtual_key_id: str, period: str) -> dict[str, int]:
        """Spend for one virtual key inside its budget window."""
        start = period_start(period)
        where = "virtual_key_id = ?"
        args: list[Any] = [virtual_key_id]
        if start:
            where += " AND ts >= ?"
            args.append(start)
        with self._lock:
            row = self._conn.execute(
                # B608 safe: `where` is assembled from fixed literals only.
                "SELECT COUNT(*) AS requests, "
                "COALESCE(SUM(total_tokens), 0) AS total_tokens, "
                "COALESCE(SUM(cost_total_micros), 0) AS cost_total_micros "
                f"FROM ledger WHERE {where}", args).fetchone()  # nosec B608
        return {"requests": row["requests"], "total_tokens": row["total_tokens"],
                "cost_total_micros": row["cost_total_micros"]}

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.commit()
                self._conn.close()
            except sqlite3.Error:  # pragma: no cover
                pass


def period_start(period: str, now: datetime | None = None) -> str:
    """Inclusive lower bound (trace-format ISO) for a budget window.

    ``day`` → today 00:00 UTC, ``month`` → 1st of month 00:00 UTC,
    ``total`` → "" (no lower bound). Lexicographic comparison against the
    stored ts format is exact because both share one format.
    """
    if period == "total":
        return ""
    if period not in BUDGET_PERIODS:
        raise LedgerError(f"budget period must be one of {list(BUDGET_PERIODS)}")
    now = now or datetime.now(timezone.utc)
    if period == "day":
        return now.strftime("%Y-%m-%d") + "T00:00:00.000Z"
    return now.strftime("%Y-%m") + "-01T00:00:00.000Z"


def evaluate_budget(ledger: Ledger, vk: dict[str, Any]) -> dict[str, Any]:
    """Blocking-budget verdict for a virtual key (checked BEFORE routing)."""
    mode = vk["budget_mode"]
    info = {"blocked": False, "mode": mode, "limit": vk["budget_limit"],
            "period": vk["budget_period"], "used": 0}
    if mode == "none":
        return info
    spent = ledger.spent_for_key(vk["id"], vk["budget_period"])
    used = spent["total_tokens"] if mode == "tokens" else spent["cost_total_micros"]
    info["used"] = used
    info["requests"] = spent["requests"]
    # limit == 0 with a spending mode = zero allowance (block everything);
    # mode "none" is the explicit unlimited setting.
    info["blocked"] = used >= vk["budget_limit"]
    return info


# ── Virtual keys ────────────────────────────────────────────────────────────

class VirtualKeyStore:
    """Gateway-facing subscriber keys (sha256-at-rest, budgets inline)."""

    def __init__(self, db_path: Path | str):
        self.path = Path(db_path)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    @staticmethod
    def _hash_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def create(self, name: str, budget_mode: str = "none",
               budget_limit: int = 0, budget_period: str = "total") -> dict[str, Any]:
        if not name or not isinstance(name, str):
            raise LedgerError("virtual key name must be a non-empty string")
        if budget_mode not in BUDGET_MODES:
            raise LedgerError(f"budget_mode must be one of {list(BUDGET_MODES)}")
        if budget_period not in BUDGET_PERIODS:
            raise LedgerError(f"budget_period must be one of {list(BUDGET_PERIODS)}")
        if not isinstance(budget_limit, int) or isinstance(budget_limit, bool) \
                or budget_limit < 0:
            raise LedgerError("budget_limit must be a non-negative integer")
        token = "vk-" + secrets.token_hex(16)
        vk_id = "vk-" + secrets.token_hex(4)
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO virtual_keys(id, name, key_hash, created_at, "
                    "enabled, budget_mode, budget_limit, budget_period) "
                    "VALUES(?, ?, ?, ?, 1, ?, ?, ?)",
                    (vk_id, name, self._hash_token(token), now_iso(),
                     budget_mode, budget_limit, budget_period))
                self._conn.commit()
            except sqlite3.IntegrityError as exc:
                raise LedgerError(f"virtual key name conflict: {exc}") from exc
        row = self.get(vk_id)
        if row is None:  # pragma: no cover — row was just inserted
            raise LedgerError("virtual key vanished after insert")
        out = dict(row)
        out["key"] = token  # plaintext shown exactly once
        return out

    def get(self, vk_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM virtual_keys WHERE id = ?", (vk_id,)).fetchone()
        return dict(row) if row else None

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM virtual_keys ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]

    def authenticate(self, token: str) -> dict[str, Any] | None:
        """Return the active virtual key row for a bearer token, else None."""
        if not token:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM virtual_keys WHERE key_hash = ?",
                (self._hash_token(token),)).fetchone()
        if row is None or not row["enabled"]:
            return None
        return dict(row)

    def set_enabled(self, vk_id: str, enabled: bool) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE virtual_keys SET enabled = ? WHERE id = ?",
                (1 if enabled else 0, vk_id))
            self._conn.commit()
        return cur.rowcount > 0

    def update_budget(self, vk_id: str, budget_mode: str,
                      budget_limit: int, budget_period: str) -> bool:
        if budget_mode not in BUDGET_MODES:
            raise LedgerError(f"budget_mode must be one of {list(BUDGET_MODES)}")
        if budget_period not in BUDGET_PERIODS:
            raise LedgerError(f"budget_period must be one of {list(BUDGET_PERIODS)}")
        if not isinstance(budget_limit, int) or isinstance(budget_limit, bool) \
                or budget_limit < 0:
            raise LedgerError("budget_limit must be a non-negative integer")
        with self._lock:
            cur = self._conn.execute(
                "UPDATE virtual_keys SET budget_mode = ?, budget_limit = ?, "
                "budget_period = ? WHERE id = ?",
                (budget_mode, budget_limit, budget_period, vk_id))
            self._conn.commit()
        return cur.rowcount > 0

    def delete(self, vk_id: str) -> bool:
        """Remove the key (ledger rows keep their historical attribution)."""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM virtual_keys WHERE id = ?", (vk_id,))
            self._conn.commit()
        return cur.rowcount > 0

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.commit()
                self._conn.close()
            except sqlite3.Error:  # pragma: no cover
                pass


# ── Bootstrap ───────────────────────────────────────────────────────────────

def build_ledger(cfg: dict) -> tuple[Ledger | None, VirtualKeyStore | None,
                                     PriceCatalog]:
    """Build the billing plane from config. Returns (ledger, vkeys, pricing).

    ``ledger.enabled`` defaults to true. ``db_path`` honors the LEDGER_DB env
    override. The HMAC secret resolves through the Stage 1 key resolver
    (env name or ``ks:`` alias); empty/unresolvable means SHA-256 chaining.
    """
    from keystore import KeystoreError, resolve_key  # local: circular-free import

    section = cfg.get("ledger") or {}
    if not isinstance(section, dict):
        raise LedgerError("ledger config section must be a mapping")
    if not bool(section.get("enabled", True)):
        return None, None, build_price_catalog(cfg)
    db_path = os.environ.get(DB_ENV, "").strip() or \
        str(section.get("db_path", DEFAULT_DB))
    hmac_key: bytes | None = None
    ref = str(section.get("hmac_key_env", "") or "").strip()
    if ref:
        try:
            secret = resolve_key(ref)
        except KeystoreError as exc:
            raise LedgerError(f"ledger hmac key unresolvable: {exc}") from exc
        if secret:
            hmac_key = secret.encode("utf-8")
    ledger = Ledger(db_path, hmac_key=hmac_key)
    vkeys = VirtualKeyStore(db_path)
    return ledger, vkeys, build_price_catalog(cfg)


# ── CLI ─────────────────────────────────────────────────────────────────────

def _main(argv: list[str]) -> int:
    """``python -m ledger verify [--db PATH]`` — offline chain audit."""
    args = list(argv[1:])
    db_path = os.environ.get(DB_ENV, DEFAULT_DB)
    if args and args[0] == "verify":
        args = args[1:]
    if args and args[0] == "--db":
        if len(args) < 2:
            print("error: --db requires a path", flush=True)
            return 2
        db_path = args[1]
        args = args[2:]
    if args:
        print(__doc__ or "", flush=True)
        return 2
    if not Path(db_path).exists():
        print(f"no ledger database at {db_path}", flush=True)
        return 1
    report = Ledger(db_path).verify()
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    import sys

    raise SystemExit(_main(sys.argv))

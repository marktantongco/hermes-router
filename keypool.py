#!/usr/bin/env python3
"""Key pool with 429-header-aware cooldown rotation and capability tags (Stage 3).

Adapted from the llm-keypool pooling core (runner-up, MIT — D1 runner-up
contribution "almost verbatim" per the dossier) onto the Stage 2 provider
interface. One pool owns KEY-level state; the circuit breaker keeps owning
ENDPOINT-level state (per base_url+model). The two layers are orthogonal:

    circuit open   -> skip the endpoint entirely (existing behavior)
    pool exhausted -> synthetic 429 so the route-level fallback cascade engages

Design (dossier Chapter 9, Stage 3):
  * round-robin over AVAILABLE keys (not cooling down, not disabled, and
    holding every capability the category requires)
  * HTTP 429 puts the used key into a cooldown derived from the provider's
    own retry headers (Retry-After seconds / HTTP-date, OpenAI-style
    x-ratelimit-reset-requests duration tokens) with a policy default and a
    hard clamp
  * HTTP 401/403 quarantines the key (disabled until reload or unlock) —
    a dead credential never earns another request
  * capability tags let a category (`require_capabilities`) filter the pool
    (e.g. vision-capable keys for multimodal tiers)
  * key ids ("<pool>/<label>") are stable identifiers reserved as the
    Stage 4 ledger's per-key attribution column

Fail-closed rules mirror Stage 1/2: unknown pool references, malformed pool
config, or unresolvable keystore aliases refuse to start (and refuse hot
reload). Empty resolved values fail at acquire time (env may be injected
late by orchestrators).

Secrets never leave this module: snapshots and admin surfaces expose
metadata only.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Mapping

from keystore import KeystoreError, resolve_key

log = logging.getLogger("hermes-router.keypool")

__all__ = [
    "PoolError",
    "Lease",
    "KeyPool",
    "parse_retry_after",
    "validate_pool_config",
    "build_pools",
    "register_pools",
    "get_pool",
    "all_pools",
]


class PoolError(RuntimeError):
    """Raised for malformed pool config or unresolvable key references."""


@dataclass
class Lease:
    """One acquired key lease. ``api_key`` is the secret; never logged."""

    key_id: str
    label: str
    api_key: str


@dataclass
class PoolKeyState:
    """Config + runtime state of one pooled key."""

    ref: str                    # env name or ks:<alias> as written in config
    label: str
    key_id: str                 # "<pool>/<label>" — stable ledger identifier
    capabilities: frozenset[str] = frozenset()
    # ── runtime state ──
    cooldown_until: float = 0.0
    consecutive_429: int = 0
    total_acquired: int = 0
    total_success: int = 0
    total_429: int = 0
    total_auth_failures: int = 0
    disabled: bool = False
    last_status: int = 0

    def available(self, now: float) -> bool:
        return not self.disabled and now >= self.cooldown_until

    def metadata(self, now: float) -> dict[str, Any]:
        """Admin-safe snapshot (no secret material)."""
        return {
            "key_id": self.key_id,
            "label": self.label,
            "ref_kind": "keystore" if self.ref.startswith("ks:") else "env",
            "capabilities": sorted(self.capabilities),
            "available": self.available(now),
            "cooldown_remaining_sec": round(max(0.0, self.cooldown_until - now), 3),
            "consecutive_429": self.consecutive_429,
            "total_acquired": self.total_acquired,
            "total_success": self.total_success,
            "total_429": self.total_429,
            "total_auth_failures": self.total_auth_failures,
            "disabled": self.disabled,
            "last_status": self.last_status,
        }


# ── Retry-header parsing ────────────────────────────────────────────────────

_DURATION_TOKEN = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)", re.IGNORECASE)
_DURATION_MULT = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def _parse_duration_tokens(raw: str) -> float | None:
    """Parse OpenAI-style duration strings ("1s", "6m0s", "1d0h0m0s", "500ms")."""
    total = 0.0
    matched = False
    for value, unit in _DURATION_TOKEN.findall(raw):
        total += float(value) * _DURATION_MULT[unit.lower()]
        matched = True
    if matched:
        return total
    return None


def parse_retry_after(
    headers: Mapping[str, str] | None,
    *,
    default: float,
    max_seconds: float,
) -> float:
    """Cooldown seconds derived from provider retry headers.

    Priority (first parseable value wins):
      1. ``Retry-After``      — delta-seconds (int/float) or RFC-7231 HTTP-date
      2. ``x-ratelimit-reset-requests`` — duration tokens ("6m0s")
      3. ``x-ratelimit-reset-tokens``   — duration tokens
    Anything unparseable falls back to *default*. The result is clamped to
    [0, max_seconds] — provider garbage can never disable a key for good.
    """
    h = {str(k).lower(): str(v).strip() for k, v in (headers or {}).items() if v}
    candidates: list[float] = []

    raw = h.get("retry-after", "")
    if raw:
        try:
            candidates.append(float(raw))
        except ValueError:
            try:
                # RFC 7231: an HTTP-date in the past means "retry now" (0),
                # not the policy default.
                dt = parsedate_to_datetime(raw)
                candidates.append(max(dt.timestamp() - time.time(), 0.0))
            except (TypeError, ValueError, OverflowError):
                pass

    for alt in ("x-ratelimit-reset-requests", "x-ratelimit-reset-tokens"):
        raw = h.get(alt, "")
        if raw:
            parsed = _parse_duration_tokens(raw)
            if parsed is not None:
                candidates.append(parsed)

    value = candidates[0] if candidates else default
    return round(min(max(value, 0.0), max_seconds), 3)


# ── Pool ────────────────────────────────────────────────────────────────────

class KeyPool:
    """Round-robin key pool with cooldown, quarantine and capability tags.

    Thread-safe (a single lock guards pointer + key state; acquire does no
    I/O, so contention is negligible on the request path).
    """

    def __init__(
        self,
        name: str,
        keys: list[PoolKeyState],
        *,
        strategy: str = "round_robin",
        default_cooldown_seconds: float = 60.0,
        max_cooldown_seconds: float = 3600.0,
        disable_on_auth_failure: bool = True,
        auth_failure_cooldown_seconds: float = 900.0,
    ) -> None:
        self.name = name
        self.keys = keys
        self.strategy = strategy
        self.default_cooldown_seconds = default_cooldown_seconds
        self.max_cooldown_seconds = max_cooldown_seconds
        self.disable_on_auth_failure = disable_on_auth_failure
        self.auth_failure_cooldown_seconds = auth_failure_cooldown_seconds
        self._lock = threading.Lock()
        self._rr = 0  # round-robin pointer (index into keys)

    @property
    def size(self) -> int:
        return len(self.keys)

    def _resolve_secret(self, key: PoolKeyState) -> str:
        try:
            value = resolve_key(key.ref)
        except KeystoreError as exc:
            raise PoolError(f"pool {self.name!r} key {key.key_id!r}: {exc}") from exc
        if not value:
            raise PoolError(
                f"pool {self.name!r} key {key.key_id!r}: reference {key.ref!r} "
                "resolves to an empty value")
        return value

    def _eligible(self, key: PoolKeyState, now: float, require: frozenset[str]) -> bool:
        return key.available(now) and require <= key.capabilities

    def acquire(self, require: frozenset[str] = frozenset()) -> Lease | None:
        """Next available key in round-robin order (advances the pointer).

        Returns None only when the pool is exhausted (all keys cooling down,
        disabled, or missing required capabilities). Raises PoolError when
        the chosen key's secret cannot be resolved (fail-closed).
        """
        with self._lock:
            now = time.time()
            n = len(self.keys)
            for offset in range(n):
                idx = (self._rr + offset) % n
                key = self.keys[idx]
                if not self._eligible(key, now, require):
                    continue
                self._rr = (idx + 1) % n
                key.total_acquired += 1
                secret = self._resolve_secret(key)  # inside lock: cheap env/keystore read
                return Lease(key_id=key.key_id, label=key.label, api_key=secret)
        return None

    def acquire_all(self, require: frozenset[str] = frozenset()) -> list[Lease]:
        """Snapshot of ALL currently-available keys in round-robin order.

        Used by the streaming path, which builds its key-attempt list up
        front. Advances the pointer past the last returned key. Raises
        PoolError on unresolvable secrets — in that case nothing is returned
        (fail-closed, the tier is skipped).
        """
        with self._lock:
            now = time.time()
            n = len(self.keys)
            leases: list[Lease] = []
            last_idx: int | None = None
            for offset in range(n):
                idx = (self._rr + offset) % n
                key = self.keys[idx]
                if not self._eligible(key, now, require):
                    continue
                leases.append(Lease(key_id=key.key_id, label=key.label,
                                    api_key=self._resolve_secret(key)))
                key.total_acquired += 1
                last_idx = idx
            if last_idx is not None:
                self._rr = (last_idx + 1) % n
        return leases

    def report_result(
        self,
        key_id: str,
        status_code: int,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Feed one attempt outcome back into the pool's key state."""
        with self._lock:
            key = next((k for k in self.keys if k.key_id == key_id), None)
            if key is None:  # internal invariant; defensive, never crash the hot path
                log.error("report_result for unknown key_id %r in pool %r", key_id, self.name)
                return
            now = time.time()
            key.last_status = status_code
            if status_code == 429:
                cooldown = parse_retry_after(
                    headers,
                    default=self.default_cooldown_seconds,
                    max_seconds=self.max_cooldown_seconds,
                )
                key.cooldown_until = now + cooldown
                key.consecutive_429 += 1
                key.total_429 += 1
                log.warning("Key %s rate-limited — cooldown %.1fs (consecutive: %d)",
                            key.key_id, cooldown, key.consecutive_429)
            elif status_code in (401, 403):
                key.total_auth_failures += 1
                if self.disable_on_auth_failure:
                    key.disabled = True
                    log.error("Key %s quarantined (auth failure %d) — "
                              "fix the credential, then reload or unlock",
                              key.key_id, status_code)
                else:
                    key.cooldown_until = now + self.auth_failure_cooldown_seconds
                    log.warning("Key %s auth failure %d — cooldown %.0fs",
                                key.key_id, status_code,
                                self.auth_failure_cooldown_seconds)
            elif 200 <= status_code < 300:
                key.total_success += 1
                key.consecutive_429 = 0
            # other statuses (5xx, 500...) belong to the circuit breaker's
            # endpoint-level accounting — recorded only as last_status.

    def unlock(self) -> int:
        """Clear quarantine + cooldowns (operator action). Returns keys reset."""
        with self._lock:
            reset = 0
            for key in self.keys:
                if key.disabled or key.cooldown_until > time.time():
                    reset += 1
                key.disabled = False
                key.cooldown_until = 0.0
                key.consecutive_429 = 0
            return reset

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            return {
                "name": self.name,
                "strategy": self.strategy,
                "size": self.size,
                "policy": {
                    "default_cooldown_seconds": self.default_cooldown_seconds,
                    "max_cooldown_seconds": self.max_cooldown_seconds,
                    "disable_on_auth_failure": self.disable_on_auth_failure,
                    "auth_failure_cooldown_seconds": self.auth_failure_cooldown_seconds,
                },
                "keys_available": sum(1 for k in self.keys if k.available(now)),
                "keys": [k.metadata(now) for k in self.keys],
            }


# ── Config validation + construction (fail-closed) ──────────────────────────

_POLICY_NUMERIC_FIELDS = {
    "default_cooldown_seconds": 60.0,
    "max_cooldown_seconds": 3600.0,
    "auth_failure_cooldown_seconds": 900.0,
}
_KNOWN_POOL_FIELDS = (
    set(_POLICY_NUMERIC_FIELDS)
    | {"strategy", "disable_on_auth_failure", "keys"}
)
_KNOWN_KEY_FIELDS = {"ref", "label", "capabilities"}


def _as_bool(value: Any) -> bool:
    return bool(value)


def validate_pool_config(cfg: dict[str, Any]) -> list[str]:
    """Return a list of human-readable config errors (empty == valid).

    Checks pool definitions (shape, policy fields, duplicate labels) AND all
    references from categories/fallbacks (existence, mutual exclusion with
    single-key fields, capability requirements).
    """
    errors: list[str] = []
    pools_cfg = cfg.get("key_pools") or {}
    if not isinstance(pools_cfg, dict):
        return ["key_pools: must be a mapping of pool name -> pool config"]

    pool_names: set[str] = set()
    for name, p_cfg in pools_cfg.items():
        pool_names.add(str(name))
        where = f"key_pools.{name}"
        if not isinstance(p_cfg, dict):
            errors.append(f"{where}: must be a mapping")
            continue
        unknown = set(p_cfg) - _KNOWN_POOL_FIELDS
        if unknown:
            errors.append(f"{where}: unknown fields {sorted(unknown)} "
                          f"(known: {sorted(_KNOWN_POOL_FIELDS)})")
        strategy = p_cfg.get("strategy", "round_robin")
        if strategy != "round_robin":
            errors.append(f"{where}.strategy: unsupported value {strategy!r} "
                          "(only 'round_robin' in v1)")
        for fname, default in _POLICY_NUMERIC_FIELDS.items():
            if fname in p_cfg:
                try:
                    if float(p_cfg[fname]) < 0:
                        errors.append(f"{where}.{fname}: must be >= 0")
                except (TypeError, ValueError):
                    errors.append(f"{where}.{fname}: must be a number")
            elif default <= 0:
                errors.append(f"{where}.{fname}: must be a positive number")
        if "disable_on_auth_failure" in p_cfg and not isinstance(
                p_cfg["disable_on_auth_failure"], bool):
            errors.append(f"{where}.disable_on_auth_failure: must be a boolean")

        keys = p_cfg.get("keys")
        if not isinstance(keys, list) or not keys:
            errors.append(f"{where}.keys: must be a non-empty list")
            continue
        labels: set[str] = set()
        for i, k_cfg in enumerate(keys):
            kwhere = f"{where}.keys[{i}]"
            if not isinstance(k_cfg, dict):
                errors.append(f"{kwhere}: must be a mapping")
                continue
            unknown_k = set(k_cfg) - _KNOWN_KEY_FIELDS
            if unknown_k:
                errors.append(f"{kwhere}: unknown fields {sorted(unknown_k)}")
            ref = k_cfg.get("ref")
            if not isinstance(ref, str) or not ref.strip():
                errors.append(f"{kwhere}.ref: non-empty string required")
            label = k_cfg.get("label")
            if not isinstance(label, str) or not label.strip():
                errors.append(f"{kwhere}.label: non-empty string required")
            elif label in labels:
                errors.append(f"{where}: duplicate key label {label!r}")
            labels.add(label if isinstance(label, str) else "")
            caps = k_cfg.get("capabilities", [])
            if not isinstance(caps, list) or not all(
                    isinstance(c, str) and c.strip() for c in caps):
                errors.append(f"{kwhere}.capabilities: must be a list of non-empty strings")

    # ── references from categories ──
    categories = cfg.get("categories") or {}
    if not isinstance(categories, dict):
        return errors + ["categories: must be a mapping"]

    def _check_ref(holder: str, entry: dict[str, Any]) -> None:
        pool_name = entry.get("key_pool", "")
        if not pool_name and entry.get("require_capabilities"):
            errors.append(f"{holder}: require_capabilities requires key_pool")
        if pool_name:
            if not isinstance(pool_name, str) or pool_name not in pool_names:
                errors.append(f"{holder}: key_pool {pool_name!r} is not defined "
                              f"(defined: {sorted(pool_names) or 'none'})")
            if entry.get("api_key_env"):
                errors.append(f"{holder}: key_pool and api_key_env are mutually exclusive")
            if entry.get("alternate_key_env"):
                errors.append(f"{holder}: key_pool and alternate_key_env are mutually "
                              "exclusive (the pool IS the alternates)")
        req = entry.get("require_capabilities", [])
        if not isinstance(req, list) or not all(isinstance(c, str) for c in req):
            errors.append(f"{holder}: require_capabilities must be a list of strings")
        fb_pool = entry.get("fallback_key_pool", "")
        if fb_pool:
            if not isinstance(fb_pool, str) or fb_pool not in pool_names:
                errors.append(f"{holder}: fallback_key_pool {fb_pool!r} is not defined")
            if entry.get("fallback_key_env"):
                errors.append(f"{holder}: fallback_key_pool and fallback_key_env "
                              "are mutually exclusive")
        fb2_pool = entry.get("fallback2_key_pool", "")
        if fb2_pool:
            if not isinstance(fb2_pool, str) or fb2_pool not in pool_names:
                errors.append(f"{holder}: fallback2_key_pool {fb2_pool!r} is not defined")
            if entry.get("fallback2_key_env"):
                errors.append(f"{holder}: fallback2_key_pool and fallback2_key_env "
                              "are mutually exclusive")

    for cat_name, cat_cfg in categories.items():
        if isinstance(cat_cfg, dict):
            _check_ref(f"categories.{cat_name}", cat_cfg)
    return errors


def build_pools(cfg: dict[str, Any]) -> dict[str, KeyPool]:
    """Build all KeyPool instances from validated config.

    Resolves every key reference exactly once at build time so keystore
    problems (missing master key / alias) fail closed BEFORE the socket
    binds. Empty env values only warn here (orchestrators may inject them
    later); they fail at acquire time.
    """
    problems = validate_pool_config(cfg)
    if problems:
        raise PoolError("; ".join(problems))

    pools: dict[str, KeyPool] = {}
    for name, p_cfg in (cfg.get("key_pools") or {}).items():
        keys: list[PoolKeyState] = []
        for k_cfg in p_cfg.get("keys", []):
            state = PoolKeyState(
                ref=str(k_cfg["ref"]).strip(),
                label=str(k_cfg["label"]).strip(),
                key_id=f"{name}/{str(k_cfg['label']).strip()}",
                capabilities=frozenset(
                    c.strip() for c in k_cfg.get("capabilities", []) or []),
            )
            try:
                secret = resolve_key(state.ref)
            except KeystoreError as exc:
                raise PoolError(f"key {state.key_id!r}: {exc}") from exc
            if not secret:
                log.warning("Key %s: reference %r resolves to an empty value now — "
                            "requests using it will fail closed until the "
                            "credential is present", state.key_id, state.ref)
            keys.append(state)

        pools[str(name)] = KeyPool(
            str(name),
            keys,
            strategy=str(p_cfg.get("strategy", "round_robin")),
            default_cooldown_seconds=float(
                p_cfg.get("default_cooldown_seconds", 60.0)),
            max_cooldown_seconds=float(p_cfg.get("max_cooldown_seconds", 3600.0)),
            disable_on_auth_failure=_as_bool(
                p_cfg.get("disable_on_auth_failure", True)),
            auth_failure_cooldown_seconds=float(
                p_cfg.get("auth_failure_cooldown_seconds", 900.0)),
        )
        log.info("Key pool %r: %d keys (capabilities: %s)",
                 name, len(keys),
                 sorted({c for k in keys for c in k.capabilities}) or "none")
    return pools


# ── Process-wide registry (server lifecycle owns it) ────────────────────────

_POOLS: dict[str, KeyPool] = {}


def register_pools(pools: dict[str, KeyPool]) -> None:
    """Atomically swap the process-wide pool registry (startup / hot reload)."""
    global _POOLS
    _POOLS = dict(pools)


def get_pool(name: str) -> KeyPool:
    pool = _POOLS.get(name)
    if pool is None:
        raise PoolError(f"key pool {name!r} is not registered "
                        f"(registered: {sorted(_POOLS)})")
    return pool


def all_pools() -> dict[str, KeyPool]:
    return dict(_POOLS)

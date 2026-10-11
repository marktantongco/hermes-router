"""Encrypted key store for the Hermes Router-Proxy (Stage 1 hardening).

Purpose: provider/admin API keys must never sit in plaintext on disk.
References of the form ``ks:<alias>`` anywhere a key is configured
(``api_key_env``, ``alternate_key_env``, the auth key) resolve through this
store instead of the process environment.

Design:
  - Store file (default ``.router/keys.enc``) holds a single Fernet token
    encrypting a JSON blob ``{"version": 1, "aliases": {alias: secret}}``.
  - Master key comes from the ``ROUTER_KEYSTORE_KEY`` environment variable
    (a Fernet key; generate with ``python -m keystore genkey``). The master
    key never touches disk.
  - Wrong master key, corrupted store, or missing alias raises
    ``KeystoreError`` (fail closed).
  - Everything else stays backward compatible: a plain name (or ``env:NAME``)
    resolves from the process environment exactly as before.

CLI:
    python -m keystore genkey                 # print a fresh Fernet master key
    python -m keystore put <alias>            # read secret from stdin
    python -m keystore list                   # list aliases only (no secrets)
    python -m keystore delete <alias>

License: MIT
"""

from __future__ import annotations

import json
import os
import sys
import threading
from pathlib import Path

__all__ = ["KeystoreError", "resolve_key", "load_store", "put_alias", "delete_alias", "list_aliases"]

KS_PREFIX = "ks:"
MASTER_KEY_ENV = "ROUTER_KEYSTORE_KEY"
STORE_FILE_ENV = "ROUTER_KEYSTORE_FILE"
DEFAULT_STORE = ".router/keys.enc"

_lock = threading.Lock()
_cache: dict[str, str] | None = None
_cached_fingerprint: tuple[str, str] | None = None


class KeystoreError(RuntimeError):
    """Raised when a keystore reference cannot be resolved (fail closed)."""


def _store_path() -> Path:
    return Path(os.environ.get(STORE_FILE_ENV, DEFAULT_STORE))


def _master_key() -> bytes:
    key = os.environ.get(MASTER_KEY_ENV, "").strip()
    if not key:
        raise KeystoreError(
            f"Keystore reference used but {MASTER_KEY_ENV} is not set — "
            "generate one with: python -m keystore genkey"
        )
    return key.encode()


def _fernet():
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:  # pragma: no cover
        raise KeystoreError(
            "The 'cryptography' package is required for ks: key references "
            "(pip install cryptography)"
        ) from exc
    return Fernet(_master_key())


def _store_file() -> dict:
    path = _store_path()
    if not path.exists():
        return {"version": 1, "aliases": {}}
    try:
        blob = _fernet().decrypt(path.read_bytes())
        data = json.loads(blob.decode("utf-8"))
        if not isinstance(data.get("aliases"), dict):
            raise KeystoreError("Keystore store file has invalid structure")
        return data
    except KeystoreError:
        raise
    except Exception as exc:
        # Corrupted store or wrong master key — never fall back to plaintext
        raise KeystoreError(
            f"Keystore store unreadable (wrong master key or corruption): {type(exc).__name__}"
        ) from exc


def _flush(data: dict) -> None:
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    token = _fernet().encrypt(json.dumps(data, ensure_ascii=False).encode("utf-8"))
    path.write_bytes(token)
    try:
        path.chmod(0o600)  # owner-only even though contents are encrypted
    except OSError:  # pragma: no cover
        pass


def load_store() -> dict[str, str]:
    """Return the alias -> secret map (decrypted in memory only)."""
    global _cache, _cached_fingerprint
    fp = (_store_path().as_posix(), os.environ.get(MASTER_KEY_ENV, ""))
    with _lock:
        if _cache is not None and _cached_fingerprint == fp:
            return _cache
        data = _store_file()
        _cache = dict(data["aliases"])
        _cached_fingerprint = fp
        return _cache


def put_alias(alias: str, secret: str) -> None:
    if not alias or ":" in alias or alias.startswith(" "):
        raise KeystoreError("Alias must be non-empty and contain no ':'")
    global _cache, _cached_fingerprint
    with _lock:
        data = _store_file()
        data["aliases"][alias] = secret
        _flush(data)
        _cache = None  # invalidate
        _cached_fingerprint = None


def delete_alias(alias: str) -> bool:
    global _cache, _cached_fingerprint
    with _lock:
        data = _store_file()
        if alias not in data["aliases"]:
            return False
        del data["aliases"][alias]
        _flush(data)
        _cache = None
        _cached_fingerprint = None
        return True


def list_aliases() -> list[str]:
    return sorted(load_store().keys())


def resolve_key(ref: str) -> str:
    """Resolve a key reference.

    ``ks:<alias>``  -> secret from the encrypted keystore
    ``env:NAME`` or ``NAME`` -> process environment (backward compatible)
    Returns "" when the env name is unset; raises KeystoreError only for
    keystore-side failures (missing master key/alias, corrupt store).
    """
    ref = (ref or "").strip()
    if not ref:
        return ""
    if ref.startswith(KS_PREFIX):
        alias = ref[len(KS_PREFIX):].strip()
        secret = load_store().get(alias)
        if secret is None:
            raise KeystoreError(f"Keystore alias not found: {alias!r}")
        return secret
    return os.environ.get(ref, "").strip()


# ── CLI ─────────────────────────────────────────────────────────────────────

def _main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[1] == "genkey":
        from cryptography.fernet import Fernet
        print(Fernet.generate_key().decode())
        return 0
    if len(argv) >= 3 and argv[1] == "put":
        secret = sys.stdin.read().strip() if len(argv) < 4 else argv[3]
        if not secret:
            print("error: empty secret", file=sys.stderr)
            return 2
        put_alias(argv[2], secret)
        print(f"stored alias {argv[2]!r} -> {_store_path()}")
        return 0
    if len(argv) >= 2 and argv[1] == "list":
        for alias in list_aliases():
            print(alias)
        return 0
    if len(argv) >= 3 and argv[1] == "delete":
        print("deleted" if delete_alias(argv[2]) else "not found")
        return 0
    print(__doc__ or "", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))

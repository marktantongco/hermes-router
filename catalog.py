#!/usr/bin/env python3
"""Free-tier provider catalog: ingestion, validation, refresh job (Stage 3).

Adapted from the free-llm-router catalog concept (DevvGwardo, MIT —
"auto-detects models and limits for seven free tiers") into a data-driven,
fail-closed component:

  * the bundled snapshot ships at ``catalog/free_tiers.yaml`` (seven free
    tiers identified by the audit: Groq, Cerebras, Mistral, Google Gemini,
    OpenRouter, SambaNova, Nous portal)
  * a category can inherit provider facts via ``catalog_ref: <name>`` —
    explicit category fields always win over catalog defaults
  * ``CatalogStore`` owns the loaded document and its refresh lifecycle:
    periodic file re-read, optional remote snapshot pull (explicit
    ``allow_remote`` opt-in, size-capped, no redirects), atomic swap with
    last-good retention on validation failure

The catalog is intentionally dumb data. Stage 4's ledger and Stage 5's
limiters consume the numbers; nothing here makes billing decisions.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import httpx
import yaml

log = logging.getLogger("hermes-router.catalog")

__all__ = [
    "CatalogError",
    "DEFAULT_CATALOG_PATH",
    "CatalogStore",
    "load_catalog",
    "build_catalog_from_cfg",
    "ingest_catalog_refs",
]

DEFAULT_CATALOG_PATH = Path(__file__).resolve().parent / "catalog" / "free_tiers.yaml"

_PROVIDER_NAME = re.compile(r"^[a-z0-9_]+$")
_MAX_REMOTE_BYTES = 1_000_000  # 1 MiB cap on pulled catalog snapshots
_KNOWN_CFG_FIELDS = {"path", "url", "allow_remote", "refresh_minutes"}
_REQUIRED_FIELDS = ("label", "base_url", "provider", "capabilities", "limits")
_LIMIT_NUMERIC = ("rpm", "rps", "tpm", "rpd", "tokens_per_day", "tokens_per_month")


class CatalogError(RuntimeError):
    """Raised for missing/invalid catalog documents or bad catalog config."""


def _validate_doc(doc: Any) -> dict[str, Any]:
    """Validate a catalog document, returning it typed. Raises CatalogError."""
    if not isinstance(doc, dict):
        raise CatalogError("catalog document must be a mapping")
    if doc.get("version") != 1:
        raise CatalogError(f"catalog version must be 1, got {doc.get('version')!r}")
    if not isinstance(doc.get("updated"), str) or not doc["updated"].strip():
        raise CatalogError("catalog 'updated' must be a non-empty date string")
    providers = doc.get("providers")
    if not isinstance(providers, dict) or not providers:
        raise CatalogError("catalog 'providers' must be a non-empty mapping")

    # Adapter names are validated against the live provider registry.
    from providers import PROVIDER_REGISTRY, resolve_provider_name

    for name, entry in providers.items():
        where = f"providers.{name}"
        if not isinstance(name, str) or not _PROVIDER_NAME.match(name):
            raise CatalogError(f"{where}: provider name must match {_PROVIDER_NAME.pattern}")
        if not isinstance(entry, dict):
            raise CatalogError(f"{where}: entry must be a mapping")
        for field_name in _REQUIRED_FIELDS:
            if field_name not in entry:
                raise CatalogError(f"{where}: missing required field {field_name!r}")
        base_url = entry["base_url"]
        if not isinstance(base_url, str) or not base_url.startswith(("http://", "https://")):
            raise CatalogError(f"{where}.base_url: must be an http(s) URL")
        provider = entry["provider"]
        if not isinstance(provider, str) or resolve_provider_name(provider) not in PROVIDER_REGISTRY:
            raise CatalogError(
                f"{where}.provider: {provider!r} is not a registered adapter "
                f"(known: {sorted(PROVIDER_REGISTRY)})")
        caps = entry["capabilities"]
        if not isinstance(caps, list) or not all(isinstance(c, str) for c in caps):
            raise CatalogError(f"{where}.capabilities: must be a list of strings")
        limits = entry["limits"]
        if not isinstance(limits, dict) or not limits:
            raise CatalogError(f"{where}.limits: must be a non-empty mapping")
        if "confidence" not in limits:
            raise CatalogError(f"{where}.limits.confidence: required "
                               "(vendor-docs | community | unverified)")
        has_number = any(isinstance(limits.get(k), (int, float)) for k in _LIMIT_NUMERIC)
        if not has_number and limits.get("confidence") != "unverified":
            raise CatalogError(
                f"{where}.limits: needs at least one numeric cap ({_LIMIT_NUMERIC}) "
                "unless confidence is 'unverified'")
        models = entry.get("models", [])
        if not isinstance(models, list) or not all(isinstance(m, str) for m in models):
            raise CatalogError(f"{where}.models: must be a list of strings")
    return doc


def load_catalog(path: Path | str) -> dict[str, Any]:
    """Load + validate a catalog file. Raises CatalogError (fail-closed)."""
    path = Path(path)
    try:
        raw = path.read_text()
    except OSError as exc:
        raise CatalogError(f"catalog file unreadable: {path}: {exc}") from exc
    try:
        doc = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise CatalogError(f"catalog YAML invalid: {path}: {exc}") from exc
    return _validate_doc(doc)


class CatalogStore:
    """Holds the validated catalog document and its refresh lifecycle."""

    def __init__(
        self,
        path: Path | str = DEFAULT_CATALOG_PATH,
        *,
        url: str = "",
        allow_remote: bool = False,
        refresh_seconds: float = 0.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.path = Path(path)
        self.url = url
        self.allow_remote = allow_remote
        self.refresh_seconds = refresh_seconds
        self._transport = transport  # test injection
        self.data: dict[str, Any] | None = None
        self.last_refresh_source: str = ""
        self.last_refresh_error: str = ""

    def load(self) -> None:
        """Initial load — failures raise (fail-closed at boot)."""
        self.data = load_catalog(self.path)
        self.last_refresh_source = "file"
        log.info("Catalog loaded: %d providers from %s",
                 len(self.data.get("providers", {})), self.path)

    def refresh(self) -> dict[str, Any]:
        """Re-fetch + re-validate + atomically swap. Returns a summary.

        On any failure the previously-loaded document stays live and
        CatalogError carries the reason (the refresh JOB decides whether
        that is fatal — background refreshes never are).
        """
        if self.url:
            if not self.allow_remote:
                raise CatalogError("catalog.url is set but catalog.allow_remote "
                                   "is false — remote pulls are refused")
            new_doc = self._pull_remote()
            source = "url"
        else:
            new_doc = load_catalog(self.path)
            source = "file"
        self.data = new_doc
        self.last_refresh_source = source
        self.last_refresh_error = ""
        summary = self.summary()
        log.info("Catalog refreshed (%s): %d providers", source,
                 summary["provider_count"])
        return summary

    def _pull_remote(self) -> dict[str, Any]:
        try:
            with httpx.Client(transport=self._transport, timeout=10.0,
                              follow_redirects=False) as client:
                resp = client.get(self.url)
            resp.raise_for_status()
            payload = resp.content
            if len(payload) > _MAX_REMOTE_BYTES:
                raise CatalogError(f"remote catalog exceeds {_MAX_REMOTE_BYTES} bytes")
            return _validate_doc(yaml.safe_load(payload))
        except CatalogError:
            raise
        except Exception as exc:
            raise CatalogError(f"remote catalog pull failed: {exc}") from exc

    def get(self, name: str) -> dict[str, Any] | None:
        if not self.data:
            return None
        entry = self.data.get("providers", {}).get(name)
        return dict(entry) if isinstance(entry, dict) else None

    def summary(self) -> dict[str, Any]:
        """Admin-safe snapshot (auth-gated upstream)."""
        if not self.data:
            return {"loaded": False}
        providers = self.data.get("providers", {})
        return {
            "loaded": True,
            "updated": self.data.get("updated", ""),
            "path": str(self.path),
            "url": self.url,
            "allow_remote": self.allow_remote,
            "refresh_seconds": self.refresh_seconds,
            "last_refresh_source": self.last_refresh_source,
            "last_refresh_error": self.last_refresh_error,
            "provider_count": len(providers),
            "providers": {
                name: {
                    "label": e.get("label", name),
                    "base_url": e.get("base_url", ""),
                    "provider": e.get("provider", ""),
                    "capabilities": e.get("capabilities", []),
                    "limits": e.get("limits", {}),
                    "model_count": len(e.get("models", [])),
                    "signup": e.get("signup", ""),
                }
                for name, e in providers.items()
            },
        }


def build_catalog_from_cfg(cfg: dict[str, Any]) -> CatalogStore | None:
    """Build the catalog store from the optional ``catalog:`` config section.

    Returns None when the section is absent (catalog features off — the
    zero-config behavior is unchanged). Raises CatalogError on bad section
    shape; load() failures surface at the caller (boot fails closed).
    """
    section = cfg.get("catalog")
    if not section:
        return None
    if not isinstance(section, dict):
        raise CatalogError("catalog: must be a mapping")
    unknown = set(section) - _KNOWN_CFG_FIELDS
    if unknown:
        raise CatalogError(f"catalog: unknown fields {sorted(unknown)} "
                           f"(known: {sorted(_KNOWN_CFG_FIELDS)})")
    path = str(section.get("path") or DEFAULT_CATALOG_PATH)
    url = str(section.get("url") or "")
    allow_remote = bool(section.get("allow_remote", False))
    try:
        refresh_seconds = float(section.get("refresh_minutes", 0) or 0) * 60.0
    except (TypeError, ValueError) as exc:
        raise CatalogError(f"catalog.refresh_minutes: {exc}") from exc
    if refresh_seconds < 0:
        raise CatalogError("catalog.refresh_minutes: must be >= 0")
    return CatalogStore(path, url=url, allow_remote=allow_remote,
                        refresh_seconds=refresh_seconds)


def ingest_catalog_refs(cfg: dict[str, Any], store: CatalogStore | None) -> list[str]:
    """Fill category defaults from ``catalog_ref`` entries. Returns errors.

    Only ABSENT fields are filled (base_url, provider, and model from the
    catalog's first model when the category has none). Explicit config
    always wins. Unknown references are errors (fail-closed at boot and
    hot reload) — a typo must never silently route to an empty target.
    """
    if store is None or not store.data:
        categories = cfg.get("categories") or {}
        refs = [c for c in categories.values()
                if isinstance(c, dict) and c.get("catalog_ref")]
        if refs:
            return ["catalog_ref used but no catalog is configured"]
        return []

    errors: list[str] = []
    for cat_name, cat_cfg in (cfg.get("categories") or {}).items():
        if not isinstance(cat_cfg, dict):
            continue
        ref = cat_cfg.get("catalog_ref")
        if not ref:
            continue
        entry = store.get(str(ref))
        if entry is None:
            errors.append(
                f"categories.{cat_name}: catalog_ref {ref!r} not found in catalog "
                f"(known: {sorted(store.data.get('providers', {}))})")
            continue
        if not cat_cfg.get("base_url"):
            cat_cfg["base_url"] = entry["base_url"]
        if not cat_cfg.get("provider"):
            cat_cfg["provider"] = entry["provider"]
        if not cat_cfg.get("model"):
            models = entry.get("models", [])
            if models:
                cat_cfg["model"] = models[0]
        log.info("Category %r inherits catalog defaults from %r "
                 "(explicit fields win)", cat_name, ref)
    return errors

"""Stage 3 free-tier catalog tests: schema, seven-tier coverage, refresh.

Exit criterion (dossier Stage 3): "catalog covers the seven free tiers".
"""
from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import yaml

from catalog import (
    CatalogError,
    CatalogStore,
    build_catalog_from_cfg,
    ingest_catalog_refs,
    load_catalog,
)

REPO = Path(__file__).resolve().parents[1]
BUNDLED = REPO / "catalog" / "free_tiers.yaml"

# The seven free tiers identified by the dossier audit
SEVEN_TIERS = {
    "groq", "cerebras", "mistral", "google_gemini",
    "openrouter", "sambanova", "nous_portal",
}


# ── Bundled snapshot ─────────────────────────────────────────────────────────

class TestBundledCatalog:
    def test_loads_and_validates(self):
        doc = load_catalog(BUNDLED)
        assert doc["version"] == 1

    def test_covers_the_seven_free_tiers(self):
        doc = load_catalog(BUNDLED)
        assert SEVEN_TIERS <= set(doc["providers"])

    def test_every_entry_has_required_fields(self):
        doc = load_catalog(BUNDLED)
        for name, entry in doc["providers"].items():
            for field in ("label", "base_url", "provider", "capabilities", "limits"):
                assert field in entry, f"{name}: missing {field}"
            assert entry["base_url"].startswith("https://")
            assert entry["provider"] == "openai"
            assert "confidence" in entry["limits"]
            assert "source" in entry["limits"]
            assert entry.get("signup", ""), f"{name}: missing signup"

    def test_adapters_resolve_against_registry(self):
        from providers import PROVIDER_REGISTRY, resolve_provider_name
        doc = load_catalog(BUNDLED)
        for entry in doc["providers"].values():
            assert resolve_provider_name(entry["provider"]) in PROVIDER_REGISTRY

    def test_unverified_entries_are_marked(self):
        doc = load_catalog(BUNDLED)
        assert doc["providers"]["nous_portal"]["limits"]["confidence"] == "unverified"


# ── Schema validation (fail-closed) ─────────────────────────────────────────

def valid_doc() -> dict:
    return {
        "version": 1,
        "updated": "2026-09-22",
        "providers": {
            "testprov": {
                "label": "Test", "base_url": "https://api.test/v1",
                "provider": "openai", "capabilities": ["fast"],
                "limits": {"rpm": 10, "confidence": "vendor-docs",
                            "source": "test"},
                "models": ["m1"], "signup": "https://test",
            },
        },
    }


class TestValidation:
    def _write(self, tmp_path: Path, doc) -> Path:
        p = tmp_path / "cat.yaml"
        p.write_text(yaml.safe_dump(doc))
        return p

    def test_valid_document_loads(self, tmp_path):
        assert load_catalog(self._write(tmp_path, valid_doc()))["version"] == 1

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(CatalogError, match="unreadable"):
            load_catalog(tmp_path / "nope.yaml")

    def test_bad_yaml_raises(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text("version: [unclosed")
        with pytest.raises(CatalogError, match="invalid"):
            load_catalog(p)

    def test_wrong_version_raises(self, tmp_path):
        doc = valid_doc()
        doc["version"] = 2
        with pytest.raises(CatalogError, match="version"):
            load_catalog(self._write(tmp_path, doc))

    def test_missing_provider_field_raises(self, tmp_path):
        doc = valid_doc()
        del doc["providers"]["testprov"]["base_url"]
        with pytest.raises(CatalogError, match="base_url"):
            load_catalog(self._write(tmp_path, doc))

    def test_unknown_adapter_raises(self, tmp_path):
        doc = valid_doc()
        doc["providers"]["testprov"]["provider"] = "not-an-adapter"
        with pytest.raises(CatalogError, match="registered adapter"):
            load_catalog(self._write(tmp_path, doc))

    def test_limits_without_confidence_raises(self, tmp_path):
        doc = valid_doc()
        del doc["providers"]["testprov"]["limits"]["confidence"]
        with pytest.raises(CatalogError, match="confidence"):
            load_catalog(self._write(tmp_path, doc))

    def test_numeric_cap_required_unless_unverified(self, tmp_path):
        doc = valid_doc()
        doc["providers"]["testprov"]["limits"] = {"confidence": "vendor-docs"}
        with pytest.raises(CatalogError, match="numeric cap"):
            load_catalog(self._write(tmp_path, doc))
        doc["providers"]["testprov"]["limits"] = {"confidence": "unverified"}
        assert load_catalog(self._write(tmp_path, doc))


# ── Config section + refresh lifecycle ───────────────────────────────────────

class TestStoreAndRefresh:
    def test_absent_section_disables_catalog(self):
        assert build_catalog_from_cfg({}) is None
        assert build_catalog_from_cfg({"other": 1}) is None

    def test_section_builds_store(self, tmp_path):
        p = self._write_doc(tmp_path)
        store = build_catalog_from_cfg({"catalog": {"path": str(p)}})
        assert store is not None
        store.load()
        assert store.get("testprov")["label"] == "Test"

    def _write_doc(self, tmp_path: Path) -> Path:
        p = tmp_path / "cat.yaml"
        p.write_text(yaml.safe_dump(valid_doc()))
        return p

    def test_unknown_section_fields_rejected(self, tmp_path):
        with pytest.raises(CatalogError, match="unknown fields"):
            build_catalog_from_cfg({"catalog": {"path": "x", "bogus": 1}})

    def test_refresh_picks_up_file_changes(self, tmp_path):
        p = self._write_doc(tmp_path)
        store = CatalogStore(p)
        store.load()
        doc = valid_doc()
        doc["providers"]["testprov"]["label"] = "Renamed"
        p.write_text(yaml.safe_dump(doc))
        summary = store.refresh()
        assert summary["provider_count"] == 1
        assert store.get("testprov")["label"] == "Renamed"

    def test_invalid_refresh_keeps_last_good(self, tmp_path):
        p = self._write_doc(tmp_path)
        store = CatalogStore(p)
        store.load()
        p.write_text("version: 9\nupdated: x\nproviders: {}")
        with pytest.raises(CatalogError):
            store.refresh()
        assert store.data["version"] == 1  # last-good retained
        assert store.get("testprov") is not None

    def test_remote_pull_disabled_by_default(self, tmp_path):
        store = CatalogStore(tmp_path / "cat.yaml",
                             url="https://example.com/cat.yaml")
        with pytest.raises(CatalogError, match="allow_remote"):
            store.refresh()

    def test_remote_pull_via_mock_transport(self, tmp_path):
        doc = valid_doc()
        doc["providers"]["testprov"]["label"] = "Remote"
        payload = yaml.safe_dump(doc).encode()

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=payload)

        store = CatalogStore(tmp_path / "unused.yaml",
                             url="https://example.com/cat.yaml",
                             allow_remote=True,
                             transport=httpx.MockTransport(handler))
        store.refresh()
        assert store.last_refresh_source == "url"
        assert store.get("testprov")["label"] == "Remote"

    def test_remote_pull_rejects_invalid_payload(self, tmp_path):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"version: 9")
        store = CatalogStore(tmp_path / "unused.yaml",
                             url="https://example.com/cat.yaml",
                             allow_remote=True,
                             transport=httpx.MockTransport(handler))
        with pytest.raises(CatalogError):
            store.refresh()
        assert store.data is None

    def test_remote_pull_size_cap(self, tmp_path):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"x" * 2_000_000)
        store = CatalogStore(tmp_path / "unused.yaml",
                             url="https://example.com/cat.yaml",
                             allow_remote=True,
                             transport=httpx.MockTransport(handler))
        with pytest.raises(CatalogError, match="bytes"):
            store.refresh()

    def test_summary_shape(self, tmp_path):
        store = CatalogStore(self._write_doc(tmp_path))
        store.load()
        s = store.summary()
        assert s["loaded"] is True and s["provider_count"] == 1
        assert "testprov" in s["providers"]
        assert "limits" in s["providers"]["testprov"]


# ── Ingestion: catalog_ref fills absent category fields ──────────────────────

class TestIngest:
    def _store(self, tmp_path: Path) -> CatalogStore:
        store = CatalogStore(REPO / "catalog" / "free_tiers.yaml")
        store.load()
        return store

    def test_fills_absent_fields(self, tmp_path):
        cfg = {"categories": {"chat": {"model": "llama-3.3-70b-versatile",
                                        "catalog_ref": "groq"}}}
        errs = ingest_catalog_refs(cfg, self._store(tmp_path))
        assert errs == []
        cat = cfg["categories"]["chat"]
        assert cat["base_url"] == "https://api.groq.com/openai/v1"
        assert cat["provider"] == "openai"

    def test_explicit_fields_win(self, tmp_path):
        cfg = {"categories": {"chat": {"model": "m",
                                        "base_url": "https://override/v1",
                                        "catalog_ref": "groq"}}}
        errs = ingest_catalog_refs(cfg, self._store(tmp_path))
        assert errs == []
        assert cfg["categories"]["chat"]["base_url"] == "https://override/v1"

    def test_unknown_ref_fails_closed(self, tmp_path):
        cfg = {"categories": {"chat": {"model": "m", "catalog_ref": "ghost"}}}
        errs = ingest_catalog_refs(cfg, self._store(tmp_path))
        assert errs and "not found" in errs[0]

    def test_ref_without_catalog_configured_fails(self):
        cfg = {"categories": {"chat": {"model": "m", "catalog_ref": "groq"}}}
        assert ingest_catalog_refs(cfg, None)  # error reported

    def test_no_refs_no_errors(self):
        cfg = {"categories": {"chat": {"model": "m", "base_url": "https://x/v1"}}}
        assert ingest_catalog_refs(cfg, None) == []

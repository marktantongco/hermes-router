"""Stage 6 deployment-blueprint contracts (compose profile + packaging).

The exit gate "compose deploys clean" is enforced in two layers:

1. Here, structurally — the compose file must parse and carry the full
   hardening contract (loopback binding, caps, healthcheck, least
   privilege, persistent state volume, log caps), the Dockerfile must
   match the runtime the CI gates verify, and the systemd unit must
   exist for bare-metal operators.
2. On a docker host, at deploy time — `docker compose config && docker
   compose up -d` (documented in the runbook; docker is not available
   in the CI sandbox, so this suite pins the file instead).

Also pins the documentation contract: the runbook must exist and cover
the operational sections the dossier's Stage 6 row requires.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

COMPOSE = yaml.safe_load((REPO / "docker-compose.yaml").read_text())


# ── Compose profile: hardening contract ─────────────────────────────────────

def test_compose_has_single_router_service():
    assert set(COMPOSE["services"]) == {"router-proxy"}


def test_compose_loopback_only_port_mapping():
    ports = COMPOSE["services"]["router-proxy"]["ports"]
    assert ports == ["127.0.0.1:8766:8766"], (
        "default exposure must be host-loopback only (Stage 1 posture)")


def test_compose_restart_and_init():
    svc = COMPOSE["services"]["router-proxy"]
    assert svc["restart"] == "unless-stopped"
    assert svc["init"] is True


def test_compose_resource_caps():
    svc = COMPOSE["services"]["router-proxy"]
    assert svc["mem_limit"] == "512m"
    assert svc["memswap_limit"] == "512m"      # equal -> swap disabled
    assert float(svc["cpus"]) == 1.0


def test_compose_healthcheck_contract():
    hc = COMPOSE["services"]["router-proxy"]["healthcheck"]
    test_cmd = " ".join(hc["test"])
    assert "urllib" in test_cmd, "probe must be stdlib (no httpx in probe)"
    assert "/health" in test_cmd
    assert hc["interval"] == "30s"
    assert hc["timeout"] == "5s"
    assert hc["retries"] == 3
    assert hc["start_period"] == "10s"


def test_compose_least_privilege():
    svc = COMPOSE["services"]["router-proxy"]
    assert svc["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in svc["security_opt"]
    assert svc["read_only"] is True
    assert "/tmp" in svc["tmpfs"]


def test_compose_state_volume_and_config_mount():
    svc = COMPOSE["services"]["router-proxy"]
    assert "./router_config.yaml:/app/router_config.yaml:ro" in svc["volumes"], (
        "config must be mounted read-only")
    assert "router-state:/app/.router" in svc["volumes"], (
        "ledger/keystore/traces state must survive redeploys")
    assert "router-state" in COMPOSE.get("volumes", {})


def test_compose_log_caps_and_env():
    svc = COMPOSE["services"]["router-proxy"]
    assert svc["logging"]["driver"] == "json-file"
    assert svc["logging"]["options"]["max-size"] == "10m"
    assert svc["env_file"], "secrets come from env_file, never the image"


# ── Dockerfile contract ─────────────────────────────────────────────────────

def test_dockerfile_matches_runtime():
    df = (REPO / "Dockerfile").read_text()
    assert "cryptography" in df            # Stage 1 keystore dependency
    assert "ROUTER_HOST=0.0.0.0" in df     # container-internal bind only
    assert "EXPOSE 8766" in df
    assert "server.py" in df


# ── systemd unit contract (bare-metal operators) ────────────────────────────

def test_systemd_unit_present_and_sane():
    unit = (REPO / "router-config" / "systemd" / "hermes-router.service")
    assert unit.exists()
    text = unit.read_text()
    assert "ExecStart" in text
    assert "Restart=" in text


# ── Example config: alerts block documented, keys valid ─────────────────────

def test_example_config_documents_alerts():
    cfg = yaml.safe_load((REPO / "router_config.example.yaml").read_text())
    raw = (REPO / "router_config.example.yaml").read_text()
    assert "alerts:" in raw, "example must document the alerts section"
    if cfg.get("alerts") is not None:
        from alerts import _KNOWN_FIELDS
        assert set(cfg["alerts"]) <= _KNOWN_FIELDS, (
            "example alerts keys must match the validated surface")
    # comments must explain the two headline rules
    assert "budget" in raw.lower()
    assert "webhook" in raw.lower()


def test_example_config_still_parses():
    cfg = yaml.safe_load((REPO / "router_config.example.yaml").read_text())
    for section in ("classifier", "routing", "server", "categories"):
        assert section in cfg


# ── Documentation contract (runbook reviewed) ───────────────────────────────

def test_runbook_exists_and_covers_operations():
    runbook = REPO / "docs" / "RUNBOOK.md"
    assert runbook.exists(), "docs/RUNBOOK.md is a Stage 6 exit-gate item"
    text = runbook.read_text()
    for required in ("Deploy", "Backup", "Alert", "Reload",
                     "Ledger", "Pool", "Key rotation", "Drill"):
        assert required.lower() in text.lower(), (
            f"runbook must cover: {required}")


def test_readme_documents_stage6_surfaces():
    text = (REPO / "README.md").read_text()
    for required in ("/dashboard", "/admin/status", "/admin/alerts",
                     "runbook"):
        assert required in text, f"README must mention {required}"

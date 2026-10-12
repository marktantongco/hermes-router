#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# hermes-router — ultimate installer (v1.0.0)
# ═══════════════════════════════════════════════════════════════════════════
# Installs the hardened gateway on a bare-metal host (the docker path is
# docker-compose.yaml). Everything is self-contained:
#
#   bash install.sh                 # install to /opt/hermes-router (or ~/...)
#   bash install.sh --home /srv/hr  # custom location
#   bash install.sh --no-service    # files + venv + config only, no systemd
#   bash install.sh doctor          # post-install health report
#   bash install.sh uninstall       # stop + remove (state preserved by default)
#
# What install does, in order:
#   0. preflight   — python >= 3.12, pip/venv, openssl, port, target writable
#   1. venv        — dedicated virtualenv + pinned runtime deps
#   2. runtime     — the 14 frozen runtime modules + tools/ + catalog/
#   3. config      — router_config.yaml from the example (never overwritten)
#   4. secrets     — router.env (0600) with a generated gateway key
#   5. service     — hardened systemd unit (system or user mode)
#   6. verify      — service boots, /health answers, key works
#
# No LLM calls, no network beyond pip. Idempotent: re-running refreshes
# code, never touches your config, keys or ledger.
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'
say()  { echo -e "$*"; }
ok()   { say "  ${GREEN}✓${NC} $*"; }
warn() { say "  ${YELLOW}⚠${NC} $*"; }
fail() { say "  ${RED}✗${NC} $*"; }
die()  { fail "$*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_FILES=(server.py providers.py trace.py keystore.py keypool.py
               catalog.py ledger.py costguard.py alerts.py dashboard.py
               surrogate_models.py zero_shot_classifier.py
               fasttext_classifier.py setfit_classifier.py)
# Runtime deps: the frozen gateway imports surrogate_models (numpy +
# scikit-learn) unconditionally at boot — classifier *training* is
# optional, but the import surface is not. Keep in sync with pyproject.
RUNTIME_DEPS=(fastapi uvicorn httpx pyyaml cryptography numpy scikit-learn)

COMMAND="install"
HOME_DIR=""
PORT="8766"
DRY_RUN=false
NO_SERVICE=false
ASSUME_YES=false

# ── arg parsing ─────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        doctor|uninstall|install) COMMAND="$1" ;;
        --home) HOME_DIR="${2:?--home needs a path}"; shift ;;
        --port) PORT="${2:?--port needs a number}"; shift ;;
        --dry-run) DRY_RUN=true ;;
        --no-service) NO_SERVICE=true ;;
        -y|--yes) ASSUME_YES=true ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        *) die "unknown argument: $1 (see --help)" ;;
    esac
    shift
done

# default home: /opt when running as root, else ~/.hermes-router
if [[ -z "$HOME_DIR" ]]; then
    if [[ "$(id -u)" == "0" ]]; then HOME_DIR="/opt/hermes-router"; else HOME_DIR="$HOME/.hermes-router"; fi
fi
VENV_DIR="$HOME_DIR/venv"
VENV_PY="$VENV_DIR/bin/python"
ENV_FILE="$HOME_DIR/router.env"
UNIT_SRC="$SCRIPT_DIR/router-config/systemd/hermes-router.service"
UNIT_NAME="hermes-router.service"

run() { # run unless dry-run
    if $DRY_RUN; then say "  ${DIM}[dry-run]${NC} $*"; else "$@"; fi
}

service_ctl() {  # systemctl (system or user) wrapper
    if [[ "$(id -u)" == "0" ]]; then systemctl "$@"; else systemctl --user "$@"; fi
}

# ══════════════════════════════════════════════════════════════════════════
# 0. PREFLIGHT
# ══════════════════════════════════════════════════════════════════════════
preflight() {
    say "${BOLD}── preflight ──${NC}"

    command -v python3 >/dev/null 2>&1 || die "python3 not found in PATH"
    local pv; pv="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
    [[ "$(python3 -c 'import sys; print(1 if sys.version_info >= (3,12) else 0)')" == "1" ]] \
        || die "python >= 3.12 required (found $pv)"
    ok "python3 $pv"

    python3 -c "import venv" 2>/dev/null || die "python3-venv missing (apt install python3-venv)"
    command -v openssl >/dev/null 2>&1 || die "openssl not found (key generation needs it)"
    ok "venv + openssl available"

    for f in "${RUNTIME_FILES[@]}"; do
        [[ -f "$SCRIPT_DIR/$f" ]] || die "runtime file missing from install source: $f"
    done
    ok "install source complete (${#RUNTIME_FILES[@]} runtime modules)"

    if ! $NO_SERVICE; then
        command -v systemctl >/dev/null 2>&1 || { warn "systemctl not found — continuing without service"; NO_SERVICE=true; }
    fi

    if ss -tln 2>/dev/null | grep -q ":$PORT "; then
        warn "port $PORT already listening — the service will fail to boot until it is free"
    fi

    if [[ -d "$HOME_DIR" ]] && [[ ! -f "$HOME_DIR/.installed" ]]; then
        warn "$HOME_DIR exists without .installed marker — contents may be unrelated"
    fi
    ok "target: $HOME_DIR (port $PORT)"
}

# ══════════════════════════════════════════════════════════════════════════
# 1-4. INSTALL
# ══════════════════════════════════════════════════════════════════════════
do_install() {
    preflight
    say ""
    say "${BOLD}── install → $HOME_DIR ──${NC}"

    # 1. venv + deps --------------------------------------------------------
    if [[ ! -x "$VENV_PY" ]]; then
        run python3 -m venv "$VENV_DIR"
        ok "virtualenv created"
    else
        ok "virtualenv already present (reused)"
    fi
    if ! $DRY_RUN; then
        "$VENV_PY" -m pip install --quiet --disable-pip-version-check \
            "${RUNTIME_DEPS[@]}" || die "pip install failed"
    fi
    ok "runtime dependencies: ${RUNTIME_DEPS[*]}"

    # 2. runtime files ------------------------------------------------------
    run mkdir -p "$HOME_DIR"
    for f in "${RUNTIME_FILES[@]}"; do
        run cp "$SCRIPT_DIR/$f" "$HOME_DIR/$f"
    done
    run mkdir -p "$HOME_DIR/tools" "$HOME_DIR/catalog"
    run cp "$SCRIPT_DIR/tools/soak.py" "$HOME_DIR/tools/soak.py"
    [[ -f "$SCRIPT_DIR/tools/capture_golden.py" ]] && run cp "$SCRIPT_DIR/tools/capture_golden.py" "$HOME_DIR/tools/"
    run cp "$SCRIPT_DIR/catalog/free_tiers.yaml" "$HOME_DIR/catalog/free_tiers.yaml"
    ok "runtime + tools + catalog staged"

    # 3. config (never overwrite) -------------------------------------------
    if [[ ! -f "$HOME_DIR/router_config.yaml" ]]; then
        run cp "$SCRIPT_DIR/router_config.example.yaml" "$HOME_DIR/router_config.yaml"
        ok "router_config.yaml created from example — EDIT IT before production"
    else
        ok "router_config.yaml already exists (preserved)"
    fi

    # 4. secrets -------------------------------------------------------------
    if [[ ! -f "$ENV_FILE" ]]; then
        local GW_KEY; GW_KEY="hr-$(openssl rand -hex 24)"
        if ! $DRY_RUN; then
            ( umask 077; cat > "$ENV_FILE" <<EOF
# hermes-router environment — generated by install.sh, chmod 0600.
# The gateway refuses to boot without a resolvable auth key.
ROUTER_PROXY_API_KEY=$GW_KEY

# Provider keys — fill in what your categories reference (or use the
# encrypted keystore instead: $VENV_PY -m keystore --help):
# OLLAMA_API_KEY=
# DEEPSEEK_API_KEY=
# ANTHROPIC_API_KEY=
EOF
            )
        fi
        say "  ${GREEN}✓${NC} $ENV_FILE created (0600) with a generated gateway key"
        say "      ${DIM}gateway key: see \$ROUTER_PROXY_API_KEY inside the file${NC}"
    else
        ok "$ENV_FILE already exists (preserved)"
    fi
    run touch "$HOME_DIR/.installed"

    # 5. service --------------------------------------------------------------
    if $NO_SERVICE; then
        say "  ${DIM}service skipped (--no-service)${NC}"
    elif $DRY_RUN; then
        say "  ${DIM}[dry-run]${NC} would install + enable $UNIT_NAME"
    else
        local UNIT_DST
        if [[ "$(id -u)" == "0" ]]; then
            UNIT_DST="/etc/systemd/system/$UNIT_NAME"
        else
            mkdir -p "$HOME/.config/systemd/user"
            UNIT_DST="$HOME/.config/systemd/user/$UNIT_NAME"
        fi
        sed -e "s|@HERMES_HOME@|$HOME_DIR|g" "$UNIT_SRC" > "$UNIT_DST"
        service_ctl daemon-reload
        service_ctl enable "$UNIT_NAME" >/dev/null 2>&1 || warn "could not enable unit"
        service_ctl restart "$UNIT_NAME"
        ok "systemd unit installed + restarted ($UNIT_DST)"
    fi

    # 6. verify ----------------------------------------------------------------
    say ""
    say "${BOLD}── verify ──${NC}"
    if $NO_SERVICE || $DRY_RUN; then
        say "  ${DIM}skipped boot check (no service started)${NC}"
    else
        local healthy=false
        for _ in $(seq 1 40); do
            if curl -fsS -m 2 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then healthy=true; break; fi
            sleep 0.5
        done
        if $healthy; then
            ok "/health answers on 127.0.0.1:$PORT"
            local KEY; KEY="$(grep -m1 '^ROUTER_PROXY_API_KEY=' "$ENV_FILE" | cut -d= -f2-)"
            if curl -fsS -m 3 -H "Authorization: Bearer $KEY" \
                 "http://127.0.0.1:$PORT/admin/status" >/dev/null 2>&1; then
                ok "admin auth works with the generated key"
            else
                warn "/admin/status check failed — verify ROUTER_PROXY_API_KEY in $ENV_FILE"
            fi
        else
            warn "service did not answer /health within 20s — inspect: journalctl -u $UNIT_NAME -n 50"
        fi
    fi

    say ""
    say "${GREEN}${BOLD}Next steps${NC}"
    say "  1. Edit config:   ${BOLD}$HOME_DIR/router_config.yaml${NC} (categories, providers, pools)"
    say "  2. Add keys:      edit ${BOLD}$ENV_FILE${NC} or use the keystore ($VENV_PY -m keystore)"
    say "  3. Apply config:  service_ctl restart hermes-router   (or POST /reload)"
    say "  4. Sanity check:  ${BOLD}bash $SCRIPT_DIR/install.sh doctor${NC}"
    say "  5. Walkthrough:   docs/WALKTHROUGH.md — zero to production, step by step"
}

# ══════════════════════════════════════════════════════════════════════════
# DOCTOR
# ══════════════════════════════════════════════════════════════════════════
do_doctor() {
    say "${BOLD}── doctor: $HOME_DIR ──${NC}"
    [[ -f "$HOME_DIR/.installed" ]] || die "not an installed home (no .installed marker) — run install first"

    local bad=0

    [[ -x "$VENV_PY" ]] && ok "venv present" || { fail "venv missing"; bad=1; }
    if [[ -x "$VENV_PY" ]]; then
        if "$VENV_PY" -c "import fastapi, uvicorn, httpx, yaml, cryptography" 2>/dev/null; then
            ok "runtime dependencies importable"
        else
            fail "dependencies broken — re-run install"; bad=1
        fi
        "$VENV_PY" -c "import sys; sys.exit(0 if sys.version_info >= (3,12) else 1)" \
            && ok "venv python >= 3.12" || { fail "venv python too old"; bad=1; }
    fi

    if [[ -f "$HOME_DIR/router_config.yaml" ]]; then
        local PORT_CFG="$PORT"
        if "$VENV_PY" -c "import yaml; yaml.safe_load(open('$HOME_DIR/router_config.yaml'))" 2>/dev/null; then
            ok "router_config.yaml parses"
            PORT_CFG="$("$VENV_PY" -c "import yaml; print(yaml.safe_load(open('$HOME_DIR/router_config.yaml')).get('server',{}).get('port',$PORT))")"
            local KEY_ENV
            KEY_ENV="$("$VENV_PY" -c "import yaml; print(yaml.safe_load(open('$HOME_DIR/router_config.yaml')).get('auth',{}).get('api_key_env',''))")"
            if [[ -n "$KEY_ENV" ]] && grep -q "^$KEY_ENV=" "$ENV_FILE" 2>/dev/null; then
                ok "auth key '$KEY_ENV' defined in $(basename "$ENV_FILE")"
            else
                fail "auth key env '$KEY_ENV' not defined in $ENV_FILE (gateway will refuse to boot)"; bad=1
            fi
            ok "configured port: $PORT_CFG"
        else
            fail "router_config.yaml does not parse"; bad=1
        fi
    else
        fail "router_config.yaml missing"; bad=1
    fi
    local PROBE_PORT="${PORT_CFG:-$PORT}"

    if curl -fsS -m 3 "http://127.0.0.1:$PROBE_PORT/health" >/dev/null 2>&1; then
        ok "gateway live on :$PROBE_PORT (/health ok)"
        local KEY; KEY="$(grep -m1 '^ROUTER_PROXY_API_KEY=' "$ENV_FILE" 2>/dev/null | cut -d= -f2-)"
        if [[ -n "$KEY" ]]; then
            if curl -fsS -m 3 -H "Authorization: Bearer $KEY" "http://127.0.0.1:$PROBE_PORT/admin/ledger/verify" >/dev/null 2>&1; then
                ok "ledger chain verifies (admin API)"
            else
                warn "ledger verify call failed (ledger disabled or not yet created?)"
            fi
        fi
    else
        warn "gateway not answering on :$PROBE_PORT — start it: service_ctl start hermes-router"
    fi

    local LEDGER="$HOME_DIR/.router/ledger.sqlite3"
    if [[ -f "$LEDGER" ]]; then
        if (cd "$HOME_DIR" && "$VENV_PY" -m ledger verify >/dev/null 2>&1); then
            ok "offline ledger verify: chain intact"
        else
            fail "offline ledger verify FAILED — follow RUNBOOK §7 (tamper procedure)"; bad=1
        fi
    fi

    local FREE_KB; FREE_KB="$(df -Pk "$HOME_DIR" 2>/dev/null | awk 'NR==2{print $4}')"
    if [[ -n "${FREE_KB:-}" ]] && [[ "$FREE_KB" -lt 1048576 ]]; then
        warn "less than 1 GB free under \$HOME_DIR (ledger + traces grow here)"
    else
        ok "disk headroom OK (${FREE_KB:-?} KB free)"
    fi

    service_ctl is-active --quiet "$UNIT_NAME" 2>/dev/null \
        && ok "systemd unit active" || warn "systemd unit not active (fine for --no-service setups)"

    say ""
    if [[ "$bad" == "0" ]]; then
        say "${GREEN}${BOLD}doctor: all checks passed${NC}"
    else
        say "${RED}${BOLD}doctor: problems found — fix the ✗ items above${NC}"; exit 1
    fi
}

# ══════════════════════════════════════════════════════════════════════════
# UNINSTALL
# ══════════════════════════════════════════════════════════════════════════
do_uninstall() {
    say "${BOLD}── uninstall: $HOME_DIR ──${NC}"
    [[ -f "$HOME_DIR/.installed" ]] || die "not an installed home (no .installed marker)"

    if service_ctl is-active --quiet "$UNIT_NAME" 2>/dev/null; then
        run service_ctl stop "$UNIT_NAME"
        ok "service stopped"
    fi
    local UNIT_DST="/etc/systemd/system/$UNIT_NAME"
    [[ "$(id -u)" != "0" ]] && UNIT_DST="$HOME/.config/systemd/user/$UNIT_NAME"
    if [[ -f "$UNIT_DST" ]]; then
        run service_ctl disable "$UNIT_NAME" 2>/dev/null || true
        run rm -f "$UNIT_DST"
        run service_ctl daemon-reload
        ok "unit removed"
    fi

    if [[ -f "$HOME_DIR/.router/ledger.sqlite3" ]] && ! $ASSUME_YES; then
        warn "state found: $HOME_DIR/.router (ledger, keystore, traces) — this is your billing history"
        read -r -p "  delete state too? [y/N] " ANS
        if [[ "${ANS:-n}" =~ ^[Yy]$ ]]; then
            run rm -rf "$HOME_DIR/.router"; ok "state deleted"
        else
            ok "state preserved at $HOME_DIR/.router"
        fi
    fi
    run rm -rf "$VENV_DIR" "$HOME_DIR"/{server,providers,trace,keystore,keypool,catalog,ledger,costguard,alerts,dashboard,surrogate_models,zero_shot_classifier,fasttext_classifier,setfit_classifier}.py "$HOME_DIR/tools" "$HOME_DIR/.installed"
    ok "runtime removed (config and router.env preserved for reinstall)"
    say "  ${DIM}full removal: rm -rf $HOME_DIR${NC}"
}

# ── entrypoint ──────────────────────────────────────────────────────────────
case "$COMMAND" in
    install)   do_install ;;
    doctor)    do_doctor ;;
    uninstall) do_uninstall ;;
    *) die "unreachable" ;;
esac

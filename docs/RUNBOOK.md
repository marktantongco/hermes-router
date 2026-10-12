# Operations Runbook — Hermes Router-Proxy

Audience: the operator of a self-hosted gateway deployment. This is the
Stage 6 deliverable for the dossier's "docs / runbook reviewed" exit
criterion. Everything here assumes the posture established in Stages 0–5:
mandatory bearer auth, encrypted keystore, immutable hash-chained ledger,
blocking budgets, cost-guard limiters with a billing-leak watchdog.

Conventions: `ROUTER` is the base URL (default `http://127.0.0.1:8766`),
`$ADMIN` is the gateway admin key. Every surface except `/health` requires
it; virtual keys (`vk-...`) are inference-only and get 403 on all admin
surfaces.

```bash
AUTH="Authorization: Bearer $ADMIN"
curl -fsS -H "$AUTH" $ROUTER/admin/status | jq .
```

---

## 1. Surfaces at a glance

| Surface | Method | Purpose |
|---|---|---|
| `/health` | GET | liveness (open, no credentials — for probes) |
| `/dashboard` | GET | operations dashboard (HTML, admin key); `?format=json`, `?page=N`, `?vk=<id>`, `?refresh=30..3600` |
| `/admin/status` | GET | light JSON status (cards, features, active alerts, pools, circuits) |
| `/admin/alerts` | GET | evaluate alert rules now; active + history + config echo |
| `/admin/alerts/test` | POST | push a synthetic event through the webhook pipe |
| `/admin/config` | GET | effective config |
| `/admin/providers` | GET | provider registry resolution |
| `/admin/pools`, `/admin/pools/{name}/unlock` | GET/POST | key pool state / clear cooldowns + quarantine |
| `/admin/catalog`, `/admin/catalog/refresh` | GET/POST | free-tier catalog |
| `/admin/costguard` | GET | limiters, pacer, watchdog, reconciliation |
| `/admin/virtual_keys` (+`/{id}`) | CRUD | subscriber keys and budgets |
| `/admin/ledger/{verify,records,summary}` | GET | billing plane |
| `/metrics` | GET | Prometheus text + JSON counters |
| `/reload` | POST | hot config reload |

## 2. Deploy

### Docker Compose (profile in `docker-compose.yaml`)

The Stage 6 profile is the hardened default: loopback-only port mapping,
512 MB / 1 CPU caps with swap disabled, stdlib healthcheck against
`/health`, `no-new-privileges`, all capabilities dropped, read-only
rootfs, and a named `router-state` volume so the ledger, keystore and
traces survive redeploys.

```bash
cd gateway/base
docker compose config          # validate the file (structure is also
                               # pinned by tests/test_deploy.py)
docker compose up -d           # build + start
curl -fsS localhost:8766/health
docker compose logs -f router-proxy
```

Requirements before first start:

1. **Secrets** in an env file (default path `/root/.hermes/.env`):
   the admin key (`ROUTER_KEY` or whatever `auth.api_key_env` names) and
   every provider key referenced by pools/categories or keystore aliases.
   Secrets never live in the image or in `router_config.yaml`.
2. **Config** at `./router_config.yaml` (mounted read-only). Start from
   `router_config.example.yaml`.
3. If the gateway should be reachable beyond the host, change the port
   mapping left side explicitly (`"8766:8766"`) and put your own TLS
   reverse proxy with auth in front — never expose the router directly.

### Bare metal / systemd

The installer does the whole path — preflight, venv, pinned runtime,
config from the annotated example, a generated gateway key in
`router.env` (0600), the hardened systemd unit, and a boot check:

```bash
bash install.sh                    # /opt/hermes-router (root) or ~/.hermes-router
bash install.sh doctor             # post-install health report
bash install.sh uninstall          # stops, removes code, preserves state by default
```

Manual equivalent: install `router-config/systemd/hermes-router.service`
(after substituting `@HERMES_HOME@`), `systemctl daemon-reload`,
`systemctl enable --now hermes-router`. The server binds `127.0.0.1` by
default (`ROUTER_HOST`/`ROUTER_PORT` override; binding `0.0.0.0` outside a
container logs a loud warning by design).

## 3. First boot checklist

1. `python -m keystore genkey` → set the master key env (`ROUTER_MASTER_KEY`);
   store it in your secret manager — the keystore is undecryptable without it.
2. `python -m keystore put <alias>` for each provider key; reference them as
   `ks:<alias>` in the config. Plaintext keys never touch the config file.
3. Confirm the boot log lines: security posture, providers, pools, billing
   plane, cost guard, alerting, dashboard — each stage prints its state and
   any misconfiguration refuses to start (fail-closed).
4. `curl -fsS -H "$AUTH" $ROUTER/admin/status` → all features you configured
   report `true`, no active alerts.
5. Create your first subscriber virtual key and send one test request with it.

## 4. Routine operations

### Reload config (no restart)

```bash
curl -fsS -X POST -H "$AUTH" $ROUTER/reload
```

All-or-nothing per stage: validation runs for providers, pools, catalog,
ledger, cost guard and alerting BEFORE the swap; on any error the previous
config stays live and the error names the offending section. The ledger,
keystore and trace state survive; in-memory alert history resets on an
alerts-section change (documented behavior, not data loss — the ledger is
the durable record).

### Key rotation (provider keys)

1. Add the new key: `python -m keystore put <alias>-new` (or a new env var).
2. Add it to the pool's `keys:` list, `POST /reload`.
3. Traffic rotates to it round-robin immediately.
4. Remove the old ref, `POST /reload`. Cooldowns/quarantine state for the
   old key disappears with it; the ledger keeps its historical `<pool>/<label>`
   attribution untouched (rows are immutable).

### Unlock a quarantined pool

401/403 from a provider quarantines the key (by default). Fix the credential
(keystore/env), then:

```bash
curl -fsS -X POST -H "$AUTH" $ROUTER/admin/pools/free_tier/unlock
```

### Raise a subscriber budget (unblock a 402)

```bash
curl -fsS -X PATCH -H "$AUTH" -H 'Content-Type: application/json' \
  $ROUTER/admin/virtual_keys/vk-xxxx \
  -d '{"budget_limit": 500000}'
```

Budgets block at `used >= limit`; raising the limit unblocks immediately.
Lowering below current usage re-blocks on the next request.

### Refresh the free-tier catalog

```bash
curl -fsS -X POST -H "$AUTH" $ROUTER/admin/catalog/refresh
```

Last-good document stays live on failure (remote pull is opt-in and
size-capped; see `catalog.py` header).

## 5. Observability

### Dashboard

`GET /dashboard` (admin key, browser): status cards, active alerts, spend by
model / key / day, virtual-key budgets with usage bars, a paginated ledger
browser (`?page=N`, `?vk=<id>`), pool availability, circuit states, cost
guard summary, free-tier catalog. `?refresh=30` adds auto-refresh;
`?format=json` returns the exact snapshot the page renders. Zero external
assets by design — renders in air-gapped browsers, no CDN, no JavaScript.

### The heartbeat pattern (drives alerting)

Alert evaluation is pull-model: every `GET /dashboard`, `/admin/status` or
`/admin/alerts` evaluates all enabled rules, and state transitions fire the
webhook exactly once. Point your uptime checker at `/admin/status`
(cheap — no spend aggregation) on a 30–60 s interval; that poll IS the
alert engine's heartbeat. Without any poll, alerts still evaluate whenever
an operator opens the dashboard.

### Alert triage

| Alert | Severity | Meaning | First action |
|---|---|---|---|
| `budget:<vk>` | warning ≥80%, critical ≥100% | subscriber budget nearly/fully consumed | check `/admin/ledger/summary?group_by=key&virtual_key_id=<id>`; raise limit or let it block (402 is working as designed) |
| `pool:<name>` | critical | every key cooling down or quarantined | `/admin/pools` → per-key state; unlock after fixing credentials, or wait out 429 cooldowns |
| `circuit:<key>` | warning | circuit breaker open after repeated failures | upstream outage/429 storm; check provider status, breaker half-opens after `recovery_timeout_sec` |
| `billing_leak` | critical | upstream 401/402 observed within the window | credential death or provider payment wall; verify the key, consider failing over to a free tier |
| `ledger_append_failures` | critical | ledger writes failing | disk full / db lock; completions may be unrecorded — see §7 backup & verify |
| `ledger_verify` (opt-in) | critical | hash chain broken — tamper evidence | treat as integrity incident: freeze writes, see §7 |

Webhook: `alerts.webhook_url` (http/https) receives
`{"event": "raised"|"cleared"|"test", "alert": {...}}`. Delivery is
fail-soft — failures are counted in `/metrics`
(`hermes_router_alerts_webhook_failures_total`) and never break a request.
Verify the pipe any time with `POST /admin/alerts/test`.

### Prometheus

`GET /metrics` exposes request/429/fallback counters per category, pool
gauges, ledger counters, the nine `cost_guard_*` counters and the five
`alerts_*` counters. Scrape with the admin key as bearer token.

## 6. Backup and restore

Durable state (all under `.router/` by default; the compose profile keeps
them in the `router-state` volume):

| Artifact | What it is | Backup action |
|---|---|---|
| `ledger.sqlite3` (+wal/shm) | immutable cost ledger | `sqlite3 ledger.sqlite3 ".backup '/backup/ledger-$(date +%F).db'"` (online, consistent) |
| `keys.enc` | Fernet-encrypted keystore | plain file copy (already encrypted; protect the master key separately) |
| `router_config.yaml` | configuration | version control (contains refs, never secrets) |
| `traces/*.jsonl` | audit trail | rotate + archive |

Restore = stop the gateway, put files back, start. The ledger verifies on
boot only if `ledger_verify` is enabled; verify on demand any time:

```bash
curl -fsS -H "$AUTH" $ROUTER/admin/ledger/verify
# or offline:
python -m ledger verify --db ledger.sqlite3
```

`{"ok": true}` → chain intact. Anything else names the first bad seq and
reason (field tamper / linkage break / tail truncation).

Disaster recovery drill (repeat quarterly; formalized as Drill C in §8.5
with an automated end-to-end test): restore yesterday's ledger backup
on a scratch machine, run `python -m ledger verify`, confirm `ok: true`,
then diff `/admin/ledger/summary?group_by=day` against the live
instance.

## 7. Ledger integrity incident (tamper alert)

1. Stop writes: stop the gateway (or `systemctl stop`), keep the disk
   image untouched.
2. Run the offline verify, note `first_bad_seq` and reason.
3. If the tail is intact through seq N and only later rows are damaged:
   restore from the most recent backup, accept the gap, document it.
   There is no "repair" path by design — the ledger's value is that
   tampering is detectable, not reversible.
4. Rotate every credential that had access, then investigate how the file
   was modified (the gateway itself never UPDATEs or DELETEs rows).

## 8. Failure drills (Stage 8 — automated + manual rehearsals)

Each drill below has two forms: the **automated drill** (run with
`python -m pytest tests/test_failure_drills.py -v` — deterministic,
against the real gateway process) and the **manual rehearsal** for
production. Run the automated set before every release; rehearse the
manual forms quarterly.

### 8.1 Provider outage (circuit breaker + cascade)

*Automated*: `TestDrillAProviderOutage` — dead primary URL → circuit
opens at the shipped threshold (3 consecutive failures) → every client
request still 200 via the fallback cascade → `circuit:` alert raised +
webhook delivered → provider revives → half-open trial closes the
circuit and clears the alert → manual `POST /circuits/reset` drilled
separately.

*Manual*: point a category's primary at a dead URL (or stop the
upstream). Watch `/dashboard#circuits` (or `GET /circuits`) open;
confirm client traffic still succeeds via the fallback model; confirm
the `circuit:` warning alert on `GET /admin/alerts` and your webhook.
Restore the provider; after `recovery_timeout_sec` (default 30 s) the
next request is the half-open trial — confirm the circuit closes. To
skip the wait, `POST /circuits/reset`.

### 8.2 Key exhaustion (pool cooldowns + synthetic 429 cascade)

*Automated*: `TestDrillBKeyExhaustion` — both pool keys rate-limited:
the first request takes two raw 429s (cooldowns) and cascades; the
second finds no available lease and engages the synthetic
`pool_exhausted` 429 → fallback. `/admin/pools` shows 0 available with
per-key `consecutive_429`; critical `pool:` alert + webhook; natural
cooldown recovery; `POST /admin/pools/{name}/unlock` resets 2 keys
immediately; alert clears.

*Manual*: point the pool's keys at a rate-limited stub (or wait for a
real rate-limit window). Watch `/admin/pools` cooldowns climb and
availability drop; confirm clients still 200 via the fallback; confirm
the `pool:` critical alert. To recover without waiting for the
cooldown: `POST /admin/pools/{name}/unlock`.

### 8.3 Budget exhaustion (pre-routing 402)

*Automated*: pinned since Stage 5 (`tests/test_costguard.py` +
fixtures 21a-d) and Stage 6 (`test_dashboard.py` — budget-exhaustion →
critical alert → webhook → no-double-fire).

*Manual*: create a VK with a tiny budget (`POST /admin/virtual_keys`),
spend past it, confirm the 402 pre-routing block, the `budget:`
critical alert, and the webhook. Unblock by raising the budget
(§4 "Raise a subscriber budget").

### 8.4 Billing leak (watchdog)

*Automated*: pinned since Stage 5 (fixtures 22a-c, 24a) — an upstream
402/401 the ledger never billed becomes a watchdog event and a
`billing_leak` alert; `billing_visibility` stays `ok` while appends
keep up.

*Manual*: stub a 402 upstream response, confirm
`GET /admin/costguard` shows the event and the alert fires.

### 8.5 Ledger corruption + recovery (R9 drill)

*Automated*: `TestDrillCLedgerCorruption` — the full §6/§7 procedure
end-to-end: online backup → stop → tamper a middle row's cost →
offline `python -m ledger verify --db` exits 1 naming `first_bad_seq`
→ restart boots (detection, not refusal) with `/admin/ledger/verify`
red and the `ledger_verify` critical alert + webhook → stop → restore
the backup (remove `-wal`/`-shm` sidecars) → start → chain green,
history intact, appending resumes with seq continuity.

*Manual*: rehearse §6's disaster-recovery drill quarterly on a scratch
machine — never on the live ledger file.

### 8.6 The 24-hour soak (release + quarterly)

The in-suite soak (`tests/test_load_soak.py`) proves the failure
lattice deterministically in a compressed window; the 24-hour run is
this procedure, driven by `tools/soak.py` (stdlib-only — no installs
on the deployed host):

```bash
python tools/soak.py --url http://127.0.0.1:8000 \
    --key "$ROUTER_KEY" --duration 24h --concurrency 24 \
    --report soak_$(date +%F)
echo "exit=$?"   # 0 = PASS
```

* Before every release tag and quarterly in production.
* Traffic mix defaults to 70% streaming / 30% non-stream; every request
  carries a unique marker and streams are reassembled client-side, so
  cross-stream corruption and half-read streams are counted as errors.
* The sampler records the alerting heartbeat timeline
  (`/admin/status`) into the report.
* **Pass criteria** (exit 0): zero 5xx, error rate within
  `--max-error-rate` (default 0.0), zero incomplete bodies. Relax the
  rate only for deliberate fault-injection soaks, and say so in the
  report's margin.
* Reports: `soak_<date>.json` (full timeline) + `.md` (summary) —
  file both with the release evidence.

## 9. Security notes (Stage 1 posture, still true)

- Auth is mandatory unless `auth.mode: disabled` is set explicitly;
  authless boots are refused.
- The keystore is encrypted at rest (Fernet, 0600); the master key is
  env-only and never written anywhere by the gateway.
- Logs scrub bearer/vk/sk/rk/fk tokens (RedactFilter).
- CORS is off by default (same-origin only); allow-lists are explicit and
  a wildcard drops credentials.
- The dashboard is an admin surface: same bearer auth, virtual keys get
  403, and it renders zero third-party assets.

# Contributing

The project runs on **exit gates** — every change keeps them green, the
same stack CI enforces (`.github/workflows/ci.yml`).

## The gates

```bash
ruff check .          # lint: 0 findings (E4/E7/E9/F + documented ratchets)
mypy server.py trace.py providers.py keystore.py keypool.py catalog.py \
      ledger.py costguard.py alerts.py dashboard.py \
      plugins/hermes-router-provider/provider.py      # types: 0 errors
bandit -r server.py providers.py keystore.py keypool.py catalog.py \
      ledger.py costguard.py trace.py alerts.py dashboard.py \
      plugins/hermes-router-provider/provider.py      # security lint
pip-audit -r requirements-audit.txt --no-deps --desc off
pytest                # 491 passed, 4 skipped (live-Ollama lane)
```

## What the suite pins (know before you change things)

* **Golden fixtures** (`tests/fixtures/golden/`, 46 transcripts) — the
  external behavior surface, byte-stable. A runtime change that alters
  a transcript needs a deliberate re-capture via `tools/capture_golden.py`
  and a diff explanation in the PR.
* **Freeze contracts** (`tests/test_freeze.py`) — README endpoint table
  parity with the live route registry (both directions), RUNBOOK §8
  coverage, RELEASE structure, version consistency. Documentation is
  code here.
* **Billing exactness** (`tests/test_load_soak.py`) — exactly one
  ledger row per completion, including under 32-way concurrent
  streaming and client disconnects.
* **Failure drills** (`tests/test_failure_drills.py`) — outage,
  exhaustion, ledger corruption + restore.

## Workflow

1. Fork, branch from `main`.
2. Make your change; if it touches runtime behavior, re-capture the
   golden fixtures deliberately and include the diff.
3. Run the full gate stack locally.
4. PR: describe which plane your change touches (security / routing /
   dispatch / billing / ops), how you verified the gates, and any
   fixture re-captures.

## Conventions

* Runtime modules are stdlib-lean; the operator tools under `tools/`
  are strictly stdlib-only (they run on deployed hosts with no pip).
* Configuration is fail-closed: unknown names and invalid values
  refuse boot and hot-reload.
* Costs are integer micro-USD end to end — no floats in money paths.
* New admin surfaces must answer virtual keys with `403 admin_only`.

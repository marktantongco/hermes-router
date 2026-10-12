# Release soak evidence — v1.0.0

Operator soak runs per [RUNBOOK §8.6](../RUNBOOK.md), executed against
a real gateway deployment of the frozen v1.0.0 runtime as part of the
publication gate.

## The deployment the numbers come from

* **Runtime**: the 14 frozen v1.0.0 runtime modules (`server.py` +
  13), booted directly — the same code path the systemd unit runs
  (bare-metal profile; the compose profile is structurally pinned by
  `tests/test_deploy.py`).
* **Config**: production-shaped — mandatory auth, per-request
  classification (unique probe content ⇒ a new session per request),
  three categories (`chat` / `code` / `research`) spread round-robin by
  the classifier, a 2-key pool with capability filtering on two
  categories and a single-key path on the third, ledger with HMAC,
  per-model pricing (incl. one free tier), cost guard with limiters +
  pacer + watchdog sized for saturated soak hardware, full alert rules
  with a live webhook sink, tracing on.
* **Upstreams**: deterministic local stubs (OpenAI-compatible), so the
  soak drives the complete request lifecycle — auth → gates →
  classification → pooled dispatch → streaming/non-streaming bodies →
  ledger append → observation — at full rate without provider spend.
  The stub contract mirrors `tools/capture_golden.py`.
* **Driver**: `tools/soak.py`, RUNBOOK §8.6 defaults — concurrency 24,
  70% streaming, 30-second heartbeat sampling, `--max-error-rate 0`.
* **Hardware**: 2 vCPU, 4 GB RAM (saturated for the window — a
  stability soak, not a benchmark).

## Runs

| Run | Window | Verdict | Report |
|---|---|---|---|
| Release soak (v1.0.0) | 60 min real time | **PASS** — 211,287 requests, 100% HTTP 200, error rate 0.0, zero incomplete bodies | `soak_release_v1.0.0_60m.md` / `.json` |
| 24-hour soak (RUNBOOK §8.6) | 24 h real time | launched at publication; artifacts land in the deployment directory | procedure below |

### Release soak results (v1.0.0, 2026-10-12)

* **211,287 requests in 3,601 s (58.68 rps sustained)** at concurrency
  24, 70% streaming — status histogram `200: 211287`, nothing else.
* **Latency** (saturated 2-vCPU host): p50 404.0 ms / p95 459.1 ms /
  p99 523.8 ms / max 807.7 ms — each request includes a classifier
  round trip plus the chat round trip.
* **Ledger**: 212,896 hash-chained records at window end
  (soak + pre-release sanity traffic), `GET /admin/ledger/verify` →
  `ok: true`, HMAC chain intact — exactly one cost row per
  completion, including every streamed request.
* **Category spread**: 70,965 / 70,965 / 70,966 across the three
  configured categories — the classifier rotation held for the full
  hour; costs differentiated per pricing tier (free tier recorded at
  zero).
* **Resource hygiene** (external sampler, 252 samples / 15 s):
  gateway RSS bounded 228–250 MB (session-expiry sawtooth, no
  growth trend), file descriptors stable at 49 after warm-up
  (23 → 49, then flat), threads constant at 9.
* **Control planes**: zero cost-guard blocks (limiters sized for the
  window), zero open circuits, pools 2/2 available throughout, zero
  active alerts, webhook sink silent (a clean window raises nothing —
  the fault lattice is covered by `tests/test_load_soak.py` and the
  RUNBOOK §8 drills).

## Interpreting the reports

* `*.json` — full timeline: per-request status histogram, latency
  percentiles (p50/p95/p99/max), every heartbeat sample
  (requests / errors / ledger records / pools available / circuits /
  active alerts), first errors if any.
* `*.md` — the human summary (verdict, window, histogram, ~60-row
  heartbeat table).
* `resources_*.csv` — external sampler (15 s interval): gateway + stub
  RSS, file descriptors and threads — the resource-hygiene companion
  the in-suite load gate asserts (`tests/test_load_soak.py`), extended
  over the long window.

**PASS criteria** (exit 0): zero 5xx, error rate within
`--max-error-rate` (0.0), zero incomplete bodies (every stream
reassembled and marker-verified client-side).

## The 24-hour procedure (RUNBOOK §8.6)

```bash
python tools/soak.py --url http://127.0.0.1:8000 \
    --key "$ROUTER_KEY" --duration 24h --concurrency 24 \
    --report soak_$(date +%F)
echo "exit=$?"   # 0 = PASS
```

Before every release tag and quarterly in production; file both
reports with the release evidence.

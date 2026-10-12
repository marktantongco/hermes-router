#!/usr/bin/env python3
"""tools/soak.py — Stage 8 operator soak driver (24-hour stability runs).

Dossier Ch.9 Table 7 row 8 requires soak testing ("24-hour stability");
the in-suite compressed soak (tests/test_load_soak.py) proves the
failure lattice deterministically, while THIS tool drives the real
operator procedure against an already-running gateway (compose profile
or systemd unit — see RUNBOOK §8):

    python tools/soak.py --url http://127.0.0.1:8000 \
        --key "$ROUTER_KEY" --duration 24h --concurrency 24

Design constraints (mirroring the gateway runtime modules):

  * stdlib-only — runs on the deployed host with no pip installs
  * never mutates gateway state: chat traffic + read-only admin
    surfaces (/admin/status, /metrics) only
  * every request carries a unique marker; stream bodies are
    reassembled and checked so cross-stream corruption is visible
  * a sampler thread records the alerting heartbeat timeline
  * SIGINT/SIGTERM finalize gracefully and still write the report

Exit codes: 0 = soak passed (zero 5xx, error rate within --max-error-rate),
1 = failed or aborted-with-errors. Reports: JSON (full timeline) + Markdown
(human summary) next to each other under --report.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

DEFAULT_DURATION = "24h"
DEFAULT_CONCURRENCY = 24
DEFAULT_STREAM_FRAC = 0.7
DEFAULT_SAMPLE_INTERVAL = 30.0
DEFAULT_TIMEOUT = 60.0
DEFAULT_MAX_ERROR_RATE = 0.0

USER_AGENT = "hermes-router-soak/1.0 (Stage 8 operator driver)"


# ── duration parsing ─────────────────────────────────────────────────────────

def parse_duration(text: str) -> float:
    """'90s' | '30m' | '24h' | '2d' | bare seconds -> seconds."""
    units = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
    text = text.strip().lower()
    if not text:
        raise ValueError("empty duration")
    if text[-1] in units:
        return float(text[:-1]) * units[text[-1]]
    return float(text)  # bare seconds


def fmt_duration(seconds: float) -> str:
    seconds = int(round(seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d {h:02d}h {m:02d}m"
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m}m {s:02d}s"


# ── HTTP helpers (urllib; no third-party deps) ───────────────────────────────

class Http:
    def __init__(self, base_url: str, key: str, timeout: float) -> None:
        self.base = base_url.rstrip("/")
        self.key = key
        self.timeout = timeout

    def _open(self, req: urllib.request.Request) -> Any:
        req.add_header("Authorization", f"Bearer {self.key}")
        req.add_header("User-Agent", USER_AGENT)
        return urllib.request.urlopen(req, timeout=self.timeout)

    def get_json(self, path: str) -> dict:
        with self._open(urllib.request.Request(self.base + path)) as resp:
            return json.loads(resp.read().decode())

    def post_chat(self, marker: str, stream: bool) -> dict:
        """One chat completion. Stream bodies are fully consumed and
        reassembled client-side (the soak must never half-read)."""
        body = json.dumps({
            "model": "auto", "stream": stream,
            "messages": [{"role": "user",
                          "content": f"soak-probe {marker}"}],
        }).encode()
        req = urllib.request.Request(
            self.base + "/v1/chat/completions", data=body,
            headers={"Content-Type": "application/json"})
        started = time.monotonic()
        try:
            with self._open(req) as resp:
                status = resp.status
                if stream:
                    content, done, usage = "", False, False
                    for raw in resp:
                        line = raw.decode(errors="replace").strip()
                        if not line.startswith("data: "):
                            continue
                        data = line[6:]
                        if data == "[DONE]":
                            done = True
                            break
                        try:
                            frame = json.loads(data)
                        except json.JSONDecodeError:
                            continue
                        choices = frame.get("choices", [])
                        if choices and choices[0].get("delta", {}) \
                                .get("content"):
                            content += choices[0]["delta"]["content"]
                        if "usage" in frame and not choices:
                            usage = True
                    body_out: dict[str, Any] = {"content": content,
                                                "done": done, "usage": usage}
                else:
                    payload = json.loads(resp.read().decode())
                    body_out = {"content":
                                payload.get("choices", [{}])[0]
                                .get("message", {}).get("content", ""),
                                "done": True, "usage": True}
        except urllib.error.HTTPError as exc:
            return {"status": exc.code, "latency": time.monotonic() - started,
                    "error": exc.read().decode(errors="replace")[:200],
                    "content": "", "done": False, "usage": False}
        except Exception as exc:  # noqa: BLE001 — soak clients never crash
            return {"status": 0, "latency": time.monotonic() - started,
                    "error": f"{type(exc).__name__}: {exc}",
                    "content": "", "done": False, "usage": False}
        return {"status": status, "latency": time.monotonic() - started,
                "error": "", **body_out}


# ── the soak ─────────────────────────────────────────────────────────────────

class Soak:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.http = Http(args.url, args.key, args.timeout)
        self.duration = parse_duration(args.duration)
        self.stop = threading.Event()
        self.results: list[dict] = []
        self.lock = threading.Lock()
        self.samples: list[dict] = []
        self.t0 = 0.0
        self.seq = 0

    # -- worker ------------------------------------------------------------
    def _worker(self) -> None:
        while not self.stop.is_set():
            with self.lock:
                self.seq += 1
                seq = self.seq
            marker = f"soak-{seq}"
            stream = (seq % 100) < int(self.args.stream_frac * 100)
            res = self.http.post_chat(marker, stream)
            res["marker"] = marker
            res["stream"] = stream
            with self.lock:
                self.results.append(res)

    # -- sampler -----------------------------------------------------------
    def _sample(self) -> None:
        try:
            status = self.http.get_json("/admin/status")
            active = len(status.get("alerts", {}).get("active", []))
            cards = status.get("cards", {})
            self.samples.append({
                "t": round(time.monotonic() - self.t0, 1),
                "uptime": status.get("uptime_seconds"),
                "requests": cards.get("requests"),
                "errors": cards.get("errors"),
                "ledger_records": cards.get("ledger_records"),
                "budget_blocks": cards.get("budget_blocks"),
                "cost_guard_blocks": cards.get("cost_guard_blocks"),
                "circuits_open": cards.get("circuits_open"),
                "pools_available": cards.get("pools_available"),
                "active_alerts": active,
            })
        except Exception:  # noqa: BLE001 — sampling is best-effort
            self.samples.append({"t": round(time.monotonic() - self.t0, 1),
                                 "error": "sample failed"})

    def _sampler(self) -> None:
        while not self.stop.is_set():
            self._sample()
            self.stop.wait(self.args.sample_interval)
        self._sample()  # final sample

    # -- run ---------------------------------------------------------------
    def run(self) -> int:
        # Preflight: auth + surfaces must answer before we commit 24h.
        try:
            health = self.http.get_json("/health")
            self.http.get_json("/admin/status")
        except Exception as exc:
            print(f"preflight failed (bad --url/--key?): {exc}",
                  file=sys.stderr)
            return 1
        print(f"soak: {self.args.url} for {fmt_duration(self.duration)} "
              f"@ concurrency {self.args.concurrency} "
              f"(health ok: {json.dumps(health)[:60]})")
        print("press Ctrl-C to end early — the report is still written")

        self.t0 = time.monotonic()
        sampler = threading.Thread(target=self._sampler, daemon=True)
        sampler.start()
        with ThreadPoolExecutor(max_workers=self.args.concurrency) as pool:
            futures = [pool.submit(self._worker)
                       for _ in range(self.args.concurrency)]
            target = self.t0 + self.duration
            while not self.stop.is_set() and time.monotonic() < target:
                self.stop.wait(1.0)
            self.stop.set()
            for fut in futures:
                fut.result(timeout=max(30.0, self.args.timeout))
        elapsed = time.monotonic() - self.t0
        time.sleep(0.5)
        return self.report(elapsed)

    # -- reporting ---------------------------------------------------------
    def report(self, elapsed: float) -> int:
        with self.lock:
            results = list(self.results)
        statuses: dict[int, int] = {}
        latencies, bad, incomplete = [], [], []
        for res in results:
            statuses[res["status"]] = statuses.get(res["status"], 0) + 1
            if res["status"] == 200:
                latencies.append(res["latency"])
                ok_body = (res["marker"] in res["content"]
                           and res["done"]
                           and (not res["stream"] or res["usage"]))
                if not ok_body:
                    incomplete.append(res["marker"])
            else:
                bad.append(res)
        n = len(results)
        errors = n - statuses.get(200, 0)
        err_rate = (errors / n) if n else 0.0

        def pct(p: float) -> float:
            if not latencies:
                return 0.0
            ordered = sorted(latencies)
            return ordered[min(len(ordered) - 1,
                               int(round(p * (len(ordered) - 1))))]

        verdict = ("PASS" if (statuses.get(500, 0) == 0
                              and all(s < 500 for s in statuses)
                              and err_rate <= self.args.max_error_rate
                              and not incomplete) else "FAIL")

        summary = {
            "verdict": verdict,
            "url": self.args.url,
            "duration_planned_s": round(self.duration, 1),
            "duration_actual_s": round(elapsed, 1),
            "concurrency": self.args.concurrency,
            "requests": n,
            "requests_per_sec": round(n / elapsed, 2) if elapsed else 0,
            "status_histogram": {str(k): v for k, v in sorted(statuses.items())},
            "error_rate": round(err_rate, 6),
            "max_error_rate_allowed": self.args.max_error_rate,
            "incomplete_bodies": incomplete[:20],
            "incomplete_count": len(incomplete),
            "latency_ms": {
                "p50": round(pct(0.50) * 1000, 1),
                "p95": round(pct(0.95) * 1000, 1),
                "p99": round(pct(0.99) * 1000, 1),
                "max": round(max(latencies) * 1000, 1) if latencies else 0},
            "samples": self.samples,
            "first_errors": [r.get("error", "")[:120] for r in bad[:5]],
        }
        report_base = Path(self.args.report)
        report_base.parent.mkdir(parents=True, exist_ok=True)
        report_base.with_suffix(".json").write_text(
            json.dumps(summary, indent=2))
        report_base.with_suffix(".md").write_text(self._markdown(summary))
        print(self._markdown(summary, console=True))
        return 0 if verdict == "PASS" else 1

    def _markdown(self, s: dict, console: bool = False) -> str:
        lines = [
            "# Soak report — hermes-router",
            "",
            f"* **Verdict**: **{s['verdict']}**",
            f"* **Endpoint**: `{s['url']}`",
            f"* **Window**: planned {fmt_duration(s['duration_planned_s'])}"
            f" / actual {fmt_duration(s['duration_actual_s'])} at "
            f"concurrency {s['concurrency']}",
            f"* **Requests**: {s['requests']} "
            f"({s['requests_per_sec']} rps)",
            "* **Status histogram**: "
            + ", ".join(f"{k}: {v}" for k, v
                        in s["status_histogram"].items()),
            f"* **Error rate**: {s['error_rate']} "
            f"(allowed {s['max_error_rate_allowed']}); "
            f"incomplete bodies: {s['incomplete_count']}",
            f"* **Latency ms**: p50 {s['latency_ms']['p50']} / "
            f"p95 {s['latency_ms']['p95']} / p99 {s['latency_ms']['p99']} / "
            f"max {s['latency_ms']['max']}",
            "",
            "## Heartbeat timeline (/admin/status samples)",
            "",
            "| t (s) | requests | errors | ledger | pools avail | "
            "circuits open | active alerts |",
            "|---|---|---|---|---|---|---|",
        ]
        rows = s["samples"]
        stride = max(1, (len(rows) + 59) // 60)  # cap the table at ~60 rows
        for row in rows[::stride]:
            if "error" in row:
                lines.append(f"| {row['t']} | (sample failed) | | | | | |")
                continue
            lines.append(
                f"| {row['t']} | {row.get('requests')} | "
                f"{row.get('errors')} | {row.get('ledger_records')} | "
                f"{row.get('pools_available')} | "
                f"{row.get('circuits_open')} | "
                f"{row.get('active_alerts')} |")
        if s["first_errors"]:
            lines += ["", "## First errors", ""]
            lines += [f"`{e}`" for e in s["first_errors"]]
        if console:
            return "\n".join(lines[:14]) + "\n(full report: " \
                f"{self.args.report}.md / .json)"
        return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Stage 8 operator soak driver (RUNBOOK §8). "
                    "Sustained mixed streaming/non-streaming traffic "
                    "against a running gateway, with heartbeat sampling "
                    "and a JSON+Markdown report.")
    parser.add_argument("--url", required=True,
                        help="gateway base URL (e.g. http://127.0.0.1:8000)")
    parser.add_argument("--key", default="",
                        help="admin gateway key (or env "
                             "HERMES_ROUTER_SOAK_KEY)")
    parser.add_argument("--duration", default=DEFAULT_DURATION,
                        help=f"soak window: 90s | 30m | 24h | 2d "
                             f"(default {DEFAULT_DURATION})")
    parser.add_argument("--concurrency", type=int,
                        default=DEFAULT_CONCURRENCY,
                        help=f"worker threads (default {DEFAULT_CONCURRENCY})")
    parser.add_argument("--stream-frac", type=float,
                        default=DEFAULT_STREAM_FRAC,
                        help="fraction of requests that stream "
                             f"(default {DEFAULT_STREAM_FRAC})")
    parser.add_argument("--sample-interval", type=float,
                        default=DEFAULT_SAMPLE_INTERVAL,
                        help=f"/admin/status sample interval seconds "
                             f"(default {DEFAULT_SAMPLE_INTERVAL})")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                        help=f"per-request timeout seconds "
                             f"(default {DEFAULT_TIMEOUT})")
    parser.add_argument("--max-error-rate", type=float,
                        default=DEFAULT_MAX_ERROR_RATE,
                        help="non-200 fraction tolerated in the verdict "
                             f"(default {DEFAULT_MAX_ERROR_RATE})")
    parser.add_argument("--report", default="soak_report",
                        help="report path prefix (writes .json and .md)")
    args = parser.parse_args(argv)

    args.key = args.key or os.environ.get(
        "HERMES_ROUTER_SOAK_KEY", "")
    if not args.key:
        print("error: --key or HERMES_ROUTER_SOAK_KEY is required",
              file=sys.stderr)
        return 2
    if args.concurrency < 1 or args.stream_frac < 0 \
            or args.stream_frac > 1:
        print("error: --concurrency >= 1 and 0 <= --stream-frac <= 1 "
              "required", file=sys.stderr)
        return 2

    soak = Soak(args)

    def _sig(_signum, _frame):  # noqa: ANN001 — signal API
        soak.stop.set()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    return soak.run()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

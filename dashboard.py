"""Operations dashboard for the Hermes Router-Proxy (Stage 6 operations plane).

A single pure function, ``render_dashboard(ctx) -> str``, turns a plain
snapshot dict (assembled by server.py from the existing admin surfaces)
into a complete, self-contained HTML page.

Design constraints (deliberate):

* **Zero external assets** — inline CSS, system font stack, no CDN, no
  JavaScript, no images. The gateway's security posture forbids
  third-party egress from an admin surface, and an ops page must render
  in air-gapped browsers.
* **Everything escaped** — every interpolated value passes through
  ``html.escape`` (including query params reflected into links), so
  virtual-key names, model names and filters cannot inject markup.
* **Total rendering** — missing surfaces render as "disabled/—", never
  as a 500. An operator needs the page most when something is broken.
* **Stable section ids** — the ids below are a contract: golden fixture
  ``golden_95_dashboard`` pins them, and operators deep-link to them.

Spend tables come from the ledger summary queries; the ledger browser
paginates the immutable records; budgets show used/limit with progress
bars. Numbers are integer money (micro-USD) everywhere on the data side;
only the final display divides.
"""

from __future__ import annotations

import html
from typing import Any
from urllib.parse import urlencode

__all__ = ["render_dashboard", "DASHBOARD_SECTIONS", "PAGE_SIZE"]

DASHBOARD_SECTIONS = (
    "alerts", "status", "spend", "budgets", "ledger",
    "pools", "circuits", "costguard", "catalog",
)
PAGE_SIZE = 25

_STYLE = """
:root{--ink:#1c2333;--dim:#6b7280;--line:#e5e7eb;--bg:#f6f7f9;
--card:#ffffff;--accent:#2563eb;--ok:#16a34a;--warn:#d97706;--crit:#dc2626}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,
"Helvetica Neue",Arial,sans-serif}
header{background:var(--card);border-bottom:1px solid var(--line);
padding:16px 24px}
h1{margin:0 0 4px;font-size:20px}
h2{font-size:15px;margin:0 0 10px}
h3{font-size:13px;margin:14px 0 6px;color:var(--dim)}
.meta{color:var(--dim);font-size:12px}
.chips{margin-top:8px}
.chip{display:inline-block;border:1px solid var(--line);border-radius:999px;
padding:1px 10px;margin-right:6px;font-size:12px;color:var(--dim)}
.chip.on{border-color:var(--ok);color:var(--ok)}
.links{margin-top:6px;font-size:12px}
.links a{color:var(--accent);text-decoration:none;margin-right:10px}
main{max-width:1180px;margin:0 auto;padding:20px 24px 60px}
section{background:var(--card);border:1px solid var(--line);border-radius:8px;
padding:16px 18px;margin-bottom:18px}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));
gap:10px}
.card{border:1px solid var(--line);border-radius:6px;padding:10px 12px}
.card .n{font-size:22px;font-variant-numeric:tabular-nums;font-weight:600}
.card .l{color:var(--dim);font-size:12px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:5px 8px;border-bottom:1px solid var(--line);
white-space:nowrap}
th{color:var(--dim);font-weight:600;font-size:12px;
text-transform:uppercase;letter-spacing:.03em}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
.bar{background:var(--line);border-radius:4px;height:8px;min-width:90px;
overflow:hidden}
.bar>i{display:block;height:100%;background:var(--accent)}
.bar.warn>i{background:var(--warn)}
.bar.crit>i{background:var(--crit)}
.bar.ok>i{background:var(--ok)}
.sev{display:inline-block;border-radius:4px;padding:0 7px;font-size:12px;
font-weight:600;color:#fff}
.sev.critical{background:var(--crit)}
.sev.warning{background:var(--warn)}
.sev.info{background:var(--accent)}
.sev.none{background:var(--ok)}
.muted{color:var(--dim)}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
font-size:12px}
.pager{margin-top:10px;font-size:13px}
.pager a,.pager span{margin-right:10px}
.pager a{color:var(--accent);text-decoration:none}
.empty{color:var(--dim);padding:8px 0}
.alert{border-left:3px solid var(--crit);padding:6px 10px;margin-bottom:6px}
.alert.warning{border-left-color:var(--warn)}
.alert .t{font-weight:600}
.alert .d{color:var(--dim);font-size:12px}
footer{max-width:1180px;margin:0 auto;padding:0 24px 30px;
color:var(--dim);font-size:12px}
"""


def _esc(value: Any) -> str:
    """HTML-escape any value for text or attribute context."""
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


def _money(micros: Any) -> str:
    try:
        return f"${int(micros) / 1_000_000:.6f}"
    except (TypeError, ValueError):
        return "—"


def _int(value: Any) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "—"


def _pct(ratio: Any) -> str:
    try:
        return f"{float(ratio) * 100:.1f}%"
    except (TypeError, ValueError):
        return "—"


def _uptime(seconds: Any) -> str:
    try:
        s = int(seconds)
    except (TypeError, ValueError):
        return "—"
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, _ = divmod(s, 60)
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


def _short(value: Any, limit: int = 14) -> str:
    text = "" if value is None else str(value)
    if len(text) <= limit:
        return _esc(text)
    return _esc(text[:limit] + "…")


def _query(base: dict[str, Any], **overrides: Any) -> str:
    params = {k: v for k, v in base.items() if v not in (None, "", "html")}
    params.update({k: v for k, v in overrides.items() if v not in (None, "")})
    if not params:
        return "?"
    return "?" + urlencode(params)


def _bar(ratio: Any, css: str = "") -> str:
    try:
        width = max(0.0, min(1.0, float(ratio))) * 100
    except (TypeError, ValueError):
        width = 0.0
    cls = f"bar {css}".strip()
    return f'<div class="{cls}"><i style="width:{width:.1f}%"></i></div>'


def _severity_badge(severity: Any) -> str:
    sev = str(severity or "info").lower()
    if sev not in ("critical", "warning", "info"):
        sev = "info"
    return f'<span class="sev {sev}">{_esc(sev)}</span>'


def _spend_table(rows: list[dict[str, Any]], dim_label: str) -> str:
    if not rows:
        return (f'<p class="empty">No ledger records yet — '
                f'{dim_label.lower()} spend appears after the first billed '
                f'completion.</p>')
    out = ['<table><thead><tr>'
           f'<th>{_esc(dim_label)}</th><th class="num">Requests</th>'
           '<th class="num">Prompt tok</th>'
           '<th class="num">Completion tok</th>'
           '<th class="num">Total tok</th>'
           '<th class="num">Cost</th></tr></thead><tbody>']
    for row in rows:
        out.append(
            f'<tr><td>{_esc(row.get("dim"))}</td>'
            f'<td class="num">{_int(row.get("requests"))}</td>'
            f'<td class="num">{_int(row.get("prompt_tokens"))}</td>'
            f'<td class="num">{_int(row.get("completion_tokens"))}</td>'
            f'<td class="num">{_int(row.get("total_tokens"))}</td>'
            f'<td class="num">{_money(row.get("cost_total_micros"))}</td>'
            '</tr>')
    out.append("</tbody></table>")
    return "".join(out)


def _render_alerts(alerts: dict[str, Any]) -> str:
    if not alerts or not alerts.get("enabled"):
        return ('<p class="empty">Alerting disabled — add an '
                '<span class="mono">alerts:</span> section to '
                'router_config.yaml to enable rule evaluation.</p>')
    active = alerts.get("active", [])
    if not active:
        return ('<p class="empty"><span class="sev none">ok</span> '
                'No active alerts.</p>')
    out = []
    for alert in active:
        detail = alert.get("detail") or {}
        detail_bits = "; ".join(
            f"{_esc(k)}={_esc(v)}" for k, v in detail.items()
            if v not in (None, "", [], {}))
        severity = _esc(alert.get("severity", "info"))
        out.append(
            f'<div class="alert {severity}">'
            f'{_severity_badge(alert.get("severity"))} '
            f'<span class="t">{_esc(alert.get("title"))}</span> '
            f'<span class="muted mono">({_esc(alert.get("id"))})</span>'
            f'<div class="d">{detail_bits}</div></div>')
    return "".join(out)


def _render_cards(cards: dict[str, Any]) -> str:
    out = ['<div class="cards">']
    for key, label in (
            ("requests", "Requests routed"),
            ("errors", "Internal errors"),
            ("sessions", "Active sessions"),
            ("circuits_open", "Circuits open"),
            ("pools_available", "Pool keys available"),
            ("ledger_records", "Ledger records"),
            ("budget_blocks", "Budget blocks (402)"),
            ("cost_guard_blocks", "Cost-guard blocks (429)")):
        out.append(f'<div class="card"><div class="n">'
                   f'{_int(cards.get(key))}</div>'
                   f'<div class="l">{label}</div></div>')
    out.append("</div>")
    return "".join(out)


def _render_budgets(budgets: list[dict[str, Any]]) -> str:
    if not budgets:
        return ('<p class="empty">No virtual keys — create one with '
                '<span class="mono">POST /admin/virtual_keys</span>.</p>')
    out = ['<table><thead><tr><th>Key</th><th>Budget</th>'
           '<th class="num">Used</th><th class="num">Limit</th>'
           '<th class="num">Used %</th><th>Status</th></tr></thead><tbody>']
    for vk in budgets:
        name_cell = (f'{_esc(vk.get("name"))} '
                     f'<span class="mono muted">{_short(vk.get("id"), 10)}</span>')
        if vk.get("budget_mode", "none") == "none":
            status = ("active" if vk.get("enabled", True) else "disabled")
            badge = "none" if vk.get("enabled", True) else "critical"
            out.append(
                f'<tr><td>{name_cell}</td>'
                f'<td class="muted">unlimited</td><td class="num muted">—</td>'
                f'<td class="num muted">—</td><td class="num muted">—</td>'
                f'<td><span class="sev {badge}">{status}</span></td></tr>')
            continue
        ratio = vk.get("used_ratio")
        css = "ok"
        if vk.get("blocked") or (ratio is not None and ratio >= 1.0):
            css = "crit"
        elif ratio is not None and ratio >= 0.8:
            css = "warn"
        unit = "tok" if vk.get("budget_mode") == "tokens" else "µ$"
        out.append(
            f'<tr><td>{name_cell}</td>'
            f'<td>{_esc(vk.get("budget_mode"))} / '
            f'{_esc(vk.get("budget_period"))}</td>'
            f'<td class="num">{_int(vk.get("used"))} {unit}</td>'
            f'<td class="num">{_int(vk.get("budget_limit"))} {unit}</td>'
            f'<td class="num">{_pct(ratio)}</td>'
            f'<td>{_bar(ratio, css)}</td></tr>')
    out.append("</tbody></table>")
    return "".join(out)


def _render_ledger(ledger: dict[str, Any], base_q: dict[str, Any]) -> str:
    if not ledger.get("enabled"):
        return ('<p class="empty">Billing plane disabled '
                '(<span class="mono">ledger.enabled false</span>) — no cost '
                'records are kept.</p>')
    rows = ledger.get("rows", [])
    total = ledger.get("total", 0)
    page = ledger.get("page", 1)
    pages = max(1, ledger.get("pages", 1))
    vk_filter = ledger.get("vk_filter")
    out = []
    if vk_filter:
        out.append(f'<p class="muted">Filtered by virtual key '
                   f'<span class="mono">{_esc(vk_filter)}</span> — '
                   f'<a href="{_query(base_q, vk=None, page=1)}">clear</a></p>')
    if not rows:
        out.append('<p class="empty">No records on this page.</p>')
    else:
        out.append('<table><thead><tr><th class="num">Seq</th><th>Time</th>'
                   '<th>Key</th><th>Model</th><th>Provider</th>'
                   '<th class="num">Tok in/out</th><th class="num">Cost</th>'
                   '<th>Flags</th></tr></thead><tbody>')
        for row in rows:
            vk = row.get("virtual_key_id")
            key_cell = _short(vk, 10) if vk else '<span class="muted">admin</span>'
            flags = []
            if row.get("record_type") == "usage_missing":
                flags.append("no-usage")
            if row.get("status") == "interrupted":
                flags.append("interrupted")
            if row.get("fallback_level"):
                flags.append(f"fb{row.get('fallback_level')}")
            if row.get("stream"):
                flags.append("stream")
            out.append(
                f'<tr><td class="num">{_int(row.get("seq"))}</td>'
                f'<td class="mono">{_esc((row.get("ts") or "")[:19])}</td>'
                f'<td class="mono">{key_cell}</td>'
                f'<td>{_esc(row.get("model"))}</td>'
                f'<td>{_esc(row.get("provider"))}</td>'
                f'<td class="num">{_int(row.get("prompt_tokens"))}'
                f'/{_int(row.get("completion_tokens"))}</td>'
                f'<td class="num">{_money(row.get("cost_total_micros"))}</td>'
                f'<td class="muted">{_esc(" ".join(flags))}</td></tr>')
        out.append("</tbody></table>")
    out.append(f'<div class="pager">'
               f'<span>Page {page} / {pages} · {_int(total)} records</span>')
    if page > 1:
        out.append(f'<a href="{_query(base_q, page=page - 1)}">&larr; newer</a>')
    if page < pages:
        out.append(f'<a href="{_query(base_q, page=page + 1)}">older &rarr;</a>')
    out.append("</div>")
    return "".join(out)


def _render_pools(pools: list[dict[str, Any]]) -> str:
    if not pools:
        return '<p class="empty">No key pools configured.</p>'
    out = ['<table><thead><tr><th>Pool</th><th>Strategy</th>'
           '<th class="num">Keys</th><th class="num">Available</th>'
           '<th>Availability</th></tr></thead><tbody>']
    for pool in pools:
        size = pool.get("size", 0)
        available = pool.get("keys_available", 0)
        css = "ok" if available else "crit"
        ratio = (available / size) if size else 0
        out.append(
            f'<tr><td>{_esc(pool.get("name"))}</td>'
            f'<td>{_esc(pool.get("strategy"))}</td>'
            f'<td class="num">{_int(size)}</td>'
            f'<td class="num">{_int(available)}</td>'
            f'<td>{_bar(ratio, css)}</td></tr>')
    out.append("</tbody></table>")
    return "".join(out)


def _render_circuits(circuits: list[dict[str, Any]]) -> str:
    if not circuits:
        return '<p class="empty">No circuits recorded yet.</p>'
    out = ['<table><thead><tr><th>Circuit</th><th>State</th>'
           '<th class="num">Failures</th></tr></thead><tbody>']
    for circuit in circuits:
        state = circuit.get("state", "closed")
        badge = {"open": "critical", "half_open": "warning",
                 "closed": "none"}.get(state, "info")
        out.append(
            f'<tr><td class="mono">{_esc(circuit.get("key"))}</td>'
            f'<td><span class="sev {badge}">{_esc(state)}</span></td>'
            f'<td class="num">{_int(circuit.get("failures"))}</td></tr>')
    out.append("</tbody></table>")
    return "".join(out)


def _render_costguard(cg: dict[str, Any]) -> str:
    if not cg or not cg.get("enabled"):
        return ('<p class="empty">Cost guard disabled — no '
                '<span class="mono">cost_guard</span> section configured.</p>')
    wd = cg.get("watchdog", {})
    rec = cg.get("reconciliation", {})
    limiters = cg.get("limiters", [])
    visibility = wd.get("billing_visibility")
    vis_badge = "critical" if visibility == "leaking" else "none"
    out = [
        f'<p><span class="sev {vis_badge}">billing visibility: '
        f'{_esc(visibility)}</span> '
        f'<span class="muted">· watchdog events {_int(wd.get("events_total"))}'
        f' · completions {_int(wd.get("completions_total"))}'
        f' · unbilled {_int(wd.get("unbilled_completions"))}</span></p>',
        f'<p class="muted">estimates {_int(rec.get("estimates_total"))}'
        f' · reconciled {_int(rec.get("reconciled_total"))}'
        f' · unreconciled {_int(rec.get("unreconciled_total"))}'
        f' · drift {_int(rec.get("drift_tokens_total"))} tok</p>']
    if limiters:
        out.append('<table><thead><tr><th>Limiter</th><th>Scope</th>'
                   '<th>Dimension</th><th class="num">Limit</th>'
                   '<th class="num">Admits</th><th class="num">Blocks</th>'
                   '</tr></thead><tbody>')
        for lm in limiters:
            out.append(
                f'<tr><td>{_esc(lm.get("name"))}</td>'
                f'<td>{_esc(lm.get("scope"))}</td>'
                f'<td>{_esc(lm.get("dimension"))}</td>'
                f'<td class="num">{_int(lm.get("limit"))}</td>'
                f'<td class="num">{_int(lm.get("admits"))}</td>'
                f'<td class="num">{_int(lm.get("blocks"))}</td></tr>')
        out.append("</tbody></table>")
    return "".join(out)


def _render_catalog(catalog: dict[str, Any] | None) -> str:
    if not catalog:
        return '<p class="empty">No free-tier catalog configured.</p>'
    providers = catalog.get("providers", [])
    out = [f'<p class="muted">{_int(catalog.get("provider_count", len(providers)))}'
           f' free tiers · source {_esc(catalog.get("source", "bundled"))}'
           f' · refreshed {_esc(catalog.get("refreshed_at", "never"))}</p>']
    if providers:
        out.append('<table><thead><tr><th>Provider</th>'
                   '<th class="num">RPM</th><th class="num">TPM</th>'
                   '<th class="num">RPD</th><th>Confidence</th>'
                   '</tr></thead><tbody>')
        for provider in providers:
            limits = provider.get("limits", {}) if isinstance(
                    provider.get("limits"), dict) else {}
            out.append(
                f'<tr><td>{_esc(provider.get("name"))}</td>'
                f'<td class="num">{_esc(limits.get("requests_per_minute", "—"))}</td>'
                f'<td class="num">{_esc(limits.get("tokens_per_minute", "—"))}</td>'
                f'<td class="num">{_esc(limits.get("requests_per_day", "—"))}</td>'
                f'<td class="muted">{_esc(provider.get("confidence", "—"))}</td></tr>')
        out.append("</tbody></table>")
    return "".join(out)


def render_dashboard(ctx: dict[str, Any]) -> str:
    """Render the full dashboard page from a snapshot ctx (see module
    docstring for the contract). Total: renders with any subset of keys."""
    base_q: dict[str, Any] = {}
    refresh = ctx.get("query", {}).get("refresh")
    if refresh:
        base_q["refresh"] = refresh
    vk_filter = ctx.get("ledger", {}).get("vk_filter")
    if vk_filter:
        base_q["vk"] = vk_filter

    features = ctx.get("features", {})
    chips = []
    for name in ("billing", "cost_guard", "alerts", "pools", "catalog"):
        chips.append(
            f'<span class="chip{" on" if features.get(name) else ""}">'
            f'{_esc(name)}: {"on" if features.get(name) else "off"}</span>')

    refresh_links = '<span class="muted">auto-refresh:</span> '
    for secs in (30, 60, 300):
        refresh_links += (f'<a href="{_query(base_q, refresh=secs)}">'
                          f'{secs}s</a> ')
    refresh_links += f'<a href="{_query(base_q, refresh=None)}">off</a>'
    refresh_meta = ""
    if refresh:
        refresh_meta = (f'<meta http-equiv="refresh" '
                        f'content="{_esc(refresh)}">')

    spend = ctx.get("spend", {})
    sections = {
        "alerts": _render_alerts(ctx.get("alerts") or {}),
        "status": _render_cards(ctx.get("cards", {})),
        "spend": (
            '<h3>By model</h3>'
            + _spend_table(spend.get("by_model", []), "Model")
            + '<h3>By key</h3>'
            + _spend_table(spend.get("by_key", []), "Key")
            + '<h3>By day</h3>'
            + _spend_table(spend.get("by_day", []), "Day")),
        "budgets": _render_budgets(ctx.get("budgets", [])),
        "ledger": _render_ledger(ctx.get("ledger") or {}, base_q),
        "pools": _render_pools(ctx.get("pools", [])),
        "circuits": _render_circuits(ctx.get("circuits", [])),
        "costguard": _render_costguard(ctx.get("cost_guard") or {}),
        "catalog": _render_catalog(ctx.get("catalog")),
    }
    titles = {
        "alerts": "Alerts", "status": "Status", "spend": "Spend",
        "budgets": "Virtual keys &amp; budgets", "ledger": "Ledger browser",
        "pools": "Key pools", "circuits": "Circuit breakers",
        "costguard": "Cost guard", "catalog": "Free-tier catalog",
    }
    body_sections = "".join(
        f'<section id="{sid}"><h2>{titles[sid]}</h2>{sections[sid]}</section>'
        for sid in DASHBOARD_SECTIONS)

    server = ctx.get("server", {})
    title = _esc(ctx.get("title", "Hermes Router — Operations Dashboard"))
    return (f'<!doctype html>\n<html lang="en">\n<head>\n'
            f'<meta charset="utf-8">\n'
            f'<meta name="viewport" content="width=device-width, initial-scale=1">\n'
            f'{refresh_meta}\n'
            f'<title>{title}</title>\n'
            f'<style>{_STYLE}</style>\n</head>\n<body>\n'
            f'<header>\n<h1>{title}</h1>\n'
            f'<div class="meta">generated {_esc(ctx.get("generated_at"))}'
            f' · uptime {_uptime(ctx.get("uptime_seconds"))}'
            f' · v{_esc(ctx.get("version"))}'
            f' · {_esc(server.get("host"))}:{_esc(server.get("port"))}</div>\n'
            f'<div class="chips">{"".join(chips)}</div>\n'
            f'<div class="links">{refresh_links}'
            f'<a href="?format=json">JSON</a>'
            f'<a href="#alerts">alerts</a><a href="#spend">spend</a>'
            f'<a href="#ledger">ledger</a></div>\n</header>\n'
            f'<main>\n{body_sections}\n</main>\n'
            f'<footer>hermes-router operations dashboard · every value on '
            f'this page is also available as JSON (?format=json) · all '
            f'surfaces require the admin key</footer>\n</body>\n</html>\n')


def page_count(total: int, page_size: int = PAGE_SIZE) -> int:
    """Total pages for the ledger browser (>= 1)."""
    if total <= 0:
        return 1
    return (total + page_size - 1) // page_size

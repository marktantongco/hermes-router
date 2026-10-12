# Security Policy

## Posture (v1.0.0)

hermes-router is built fail-closed, and the posture is enforced by
tests, not just configuration:

* **Authentication is mandatory** — an authless boot is refused; the
  configured key must resolve at startup (Stage 1).
* **Virtual keys are least-privilege** — chat-only by construction;
  every admin surface and `/metrics` answers `403 admin_only`.
* **Secrets** — provider keys live in the Fernet-encrypted keystore
  (`0600`, master key env-only) or env files; `router_config.yaml`
  carries variable *names*, never values. Logs are scrubbed by a
  redaction filter (bearer/vk/sk/rk/fk tokens).
* **Network** — binds `127.0.0.1` by default; CORS off by default
  (same-origin); a wildcard origin drops credentials; the container
  maps loopback only unless you widen it deliberately.
* **Accounting integrity** — the cost ledger is hash-chained (optional
  HMAC); `GET /admin/ledger/verify` and the offline CLI detect
  tampering; corruption handling is a rehearsed drill (RUNBOOK §7/§8).
* **Supply chain** — `bandit` on the runtime surface and `pip-audit`
  on the pinned closure run in CI on every push and PR.

## Deployment boundaries

Never expose the gateway directly to the internet. Terminate TLS at a
reverse proxy with its own auth, and keep the port mapping on
loopback. The dashboard is an admin surface — same bearer rules.

## Reporting a vulnerability

Please use GitHub's **private security advisory** feature (Security →
Report a vulnerability) rather than opening a public issue. Include
reproduction steps and affected surfaces. Reports are acknowledged
within 72 hours.

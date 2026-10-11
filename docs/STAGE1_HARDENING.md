# Stage 1 — Security Hardening Memo and Checklist

**Upstream pin:** `0a9560e` (unchanged) · **Stage 0 baseline:** `3c4d789` · **Stage:** 1 of 9 (approved scope: D1+D2, Stages 0–5)
**Mandate (Ch. 9):** default localhost bind · mandatory bearer auth · encrypted key store · secrets redaction · CORS lockdown · pip-audit and bandit gates.
**Exit gate:** security checklist pass · negative authz tests pass · no plaintext secrets on disk.

---

## 1. Changes Delivered (all behavioral changes are security-posture changes)

| Area | Change | Files |
|---|---|---|
| **Mandatory auth** | Auth is now required by default: `validate_security_posture()` runs at startup and the process **refuses to start** when `auth.mode` is `required` (default) but `auth.api_key_env` is missing or resolves empty. Opt-out exists only via explicit `auth.mode: disabled` (logged loudly). Fail-closed also enforced per-request (pre-existing Bug #22 behavior retained). Key may be an env-var name or a keystore alias (`ks:<alias>`). | `server.py` (`verify_auth`, `validate_security_posture`, `_startup`, `__main__`) |
| **Encrypted key store** | New `keystore.py`: Fernet-encrypted store (`.router/keys.enc`, 0600) holding alias→secret JSON; master key lives only in `ROUTER_KEYSTORE_KEY` env. `ks:` references resolve at runtime in every key position (admin key, provider keys, alternate keys); plain names stay env-resolved (backward compatible). CLI: `python -m keystore genkey|put|list|delete`. Wrong master key / missing alias / corrupt store raise `KeystoreError` — fail closed, never plaintext fallback. | `keystore.py` (new), `server.py` (`env_key`) |
| **Encrypted-at-rest proof** | Store bytes begin with the `gAAAA` Fernet envelope; unit test asserts the plaintext secret appears nowhere in the file. | `tests/test_security.py` |
| **CORS lockdown** | Default: **no CORS middleware** (same-origin only). `CORS_ORIGINS` may set an explicit allow-list (credentials honored, non-allowlisted origins get no ACAO). Wildcard `*` is honored only with `allow_credentials` force-disabled + loud warning. | `server.py` |
| **Localhost default bind** | Default bind is `127.0.0.1` via code default and configs; `ROUTER_HOST`/`ROUTER_PORT` env overrides exist for containers (Dockerfile sets `ROUTER_HOST=0.0.0.0` intentionally — exposure is the port mapping's job). Binding to all interfaces logs a loud warning. Example config hardened. | `server.py`, `Dockerfile`, `router_config.example.yaml` |
| **Secrets redaction** | `RedactFilter` attached to the root log handler (covers uvicorn loggers too): scrubs `Bearer <token>` and `sk-`/`rk-`/`fk-`-shaped strings from every record before emission. Unit-tested on real LogRecords. | `server.py` |
| **Repo hygiene** | The tracked live `router_config.yaml` (author's endpoint topology, zero literal secrets) removed from the git index — `.gitignore` intent finally honored; file kept on disk. | git index |
| **Dependency security floor** | pip-audit drove upgrades: fastapi 0.128→**0.141.1**, starlette 0.50→**1.6.0**, cryptography 44→**50.0.1** (fixes GHSA-537c-gmf6-5ccf + 2 PYSEC), anyio→**4.14.2**, idna→**3.20**, click→**8.3.3**, pytest→9.0.3. `requirements-audit.txt` pins the audited closure (29 dists). | `pyproject.toml`, `requirements-audit.txt` |
| **Security gates in CI** | `bandit -r server.py keystore.py trace.py` (clean) and `pip-audit -r requirements-audit.txt --no-deps` (clean) added as blocking CI steps. | `.github/workflows/ci.yml` |

## 2. Security Checklist (exit gate item 1)

- [x] **Default localhost bind** — code default `127.0.0.1`; example config hardened; container path explicit via `ROUTER_HOST`; all-interface binds warn loudly.
- [x] **Mandatory bearer auth** — required by default; startup refuses authless/empty-key boots; per-request fail-closed; explicit `mode: disabled` is the only opt-out; auth extends to ingress, `/metrics`, `/reload`, `/circuits`, `/admin/*` (verified live); `/health` intentionally open (liveness probes carry no credentials).
- [x] **Encrypted key store** — Fernet store implemented and integrated (`ks:` refs anywhere a key is accepted); master key never on disk; owner-only file mode; E2E test proves the auth key can live exclusively in the store.
- [x] **Secrets redaction** — log filter scrubs bearer tokens and key-shaped strings; 401 responses never echo tokens; traces store no headers.
- [x] **CORS lockdown** — off by default; allow-list honors credentials; wildcard drops credentials; non-allowlisted origins get no ACAO (all four behaviors live-tested).
- [x] **pip-audit gate** — project dependency closure audits clean ("No known vulnerabilities found").
- [x] **bandit gate** — runtime surface (server/keystore/trace) audits clean; 3 `# nosec B104` annotations document bind-*guard* string literals (comparisons/display, not binds).

## 3. Negative-Authorization Test Matrix (exit gate item 2)

`tests/test_security.py` — 14 tests against the **real server process** (subprocess harness, no auth-path mocks): no-token 401, wrong-token 401, malformed (non-Bearer) 401 on ingress; 401 on `/metrics`, `/admin/config`, `/admin/sessions`, `/reload`, `/circuits`; valid key → 200 round trip to the stub upstream; authless config → process exits non-zero; empty-key env → process exits non-zero; explicit `mode: disabled` boots and serves anonymously (documented opt-out only); CORS default/wildcard/allow-list/block tests; keystore round-trip, at-rest encryption, wrong-master fail-closed, missing-alias fail-closed, CLI; keystore-backed auth key E2E; log-redaction unit tests. **14/14 pass.**

## 4. No Plaintext Secrets on Disk (exit gate item 3)

- Keystore file is Fernet-encrypted (asserted byte-level in tests); master key exists only in the process environment.
- No config file carries a literal secret (`api_key_env` holds *names/references* only) — verified across shipped, example, and fixture configs.
- The author's tracked `router_config.yaml` is untracked; `.env` and `traces/` remain gitignored; test keys (`golden-test-key-0001`, `security-test-key-7f3a`) are throwaway fixture constants documented as non-credentials.

## 5. Behavior Regression Proof

The golden fixture suite was re-captured after all hardening changes AND again after the fastapi/starlette major upgrades: **all 11 scenario transcripts byte-identical** to Stage 0 (only the index capture-timestamp differs); the only behavioral delta is the intended posture change — success-path fixtures now carry `Authorization` headers, and two new fixtures (`golden_08`/`golden_09`) pin the 401 contract. Two new posture tests assert every success fixture was captured *with* credentials and that `/health` remains open while `/metrics` requires them.

## 6. Ratchet Update

- **R-L2 (server.py, 8× E731) — CLEARED**: trace-fallback and classifier-import lambdas converted to defs; per-file ignore removed.
- **R-M1 (server.py, 3 mypy quirks) — CLEARED**: formatter annotation, `prev: list[int]`, `key_attempts` annotation; mypy override removed and verified unnecessary.
- **R-M2 (surrogate_models.py) — retained** (module untouched this stage).
- R-L1, R-L3, R-L4 and the extended rule-set debt (42 findings) remain catalogued for the stages that touch those files.

## 7. Gate Results

| Gate | Result |
|---|---|
| `ruff check .` | 0 errors |
| `mypy server.py trace.py` | 0 errors (server override retired) |
| `bandit -r server.py keystore.py trace.py` | No issues identified |
| `pip-audit` (29-dist closure) | No known vulnerabilities |
| `pytest` | **66 passed** (27 trace · 25 fixture contracts · 14 security) |

**Stage 1 verdict: PASS.** Next: Stage 2 — Provider abstraction (internal `Provider` interface; port neostryder-style registry and Anthropic translation as adapters; per-adapter contract tests against the golden fixtures).

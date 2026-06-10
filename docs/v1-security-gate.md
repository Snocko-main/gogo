# v1 Security Gate Evidence

Status: draft release-gate evidence. No framework code blocker was found in the
reviewed areas, but the v1 gate is blocked until vulnerability validation is
rerun with a patched Go toolchain.

Review date: 2026-06-10
Base: `origin/main` at `8c36f0b`

## Release Decision

Do not cut v1 from validation performed with `go1.26.3`. Both govulncheck
scans report reachable Go standard-library vulnerabilities fixed in
`go1.26.4`:

- [GO-2026-5039](https://pkg.go.dev/vuln/GO-2026-5039) in `net/textproto`;
  trace reaches `readMultipartPart` through multipart header parsing.
- [GO-2026-5037](https://pkg.go.dev/vuln/GO-2026-5037) in `crypto/x509`;
  traces reach multipart stream draining and route registration error paths.

Remediation: rerun release validation with Go `1.26.4` or newer, or the current
patched supported release line selected by release engineering. Keep this PR
draft until both govulncheck scans pass.

## Evidence Summary

| Area | Evidence | Gate result |
| --- | --- | --- |
| Proxy trust | `Config.TrustProxy` defaults off; `TrustedProxies` accepts IP/CIDR ranges and auto-enables peer IP capture. Forwarded protocol and IP helpers honor `X-Forwarded-*` only when the immediate peer is trusted. Ops docs require the trusted edge to strip or overwrite forwarded headers. See [`types.go`](../types.go#L544-L567), [`types.go`](../types.go#L677-L728), [`types.go`](../types.go#L1048-L1085), [`types.go`](../types.go#L5561-L5659), and [`docs/ops.md`](ops.md#L37-L69). | Pass for framework controls. Production apps must use concrete proxy ranges and edge header sanitization. |
| WebSocket origin/auth | The nil `Upgrade` default rejects browser handshakes carrying `Origin`; `UnsafeAutoUpgrade` is explicitly documented as only for public, non-cookie endpoints. `middleware.WebSocketAuth` is fail-closed by default, validates exact origins, can reject missing origins, supports a `Verify` callback, and allow-lists subprotocols. See [`types.go`](../types.go#L316-L352) and [`middleware/wsauth.go`](../middleware/wsauth.go#L10-L191). | Pass for defaults and helper API. App routes still need explicit `Upgrade` callbacks for browser-capable or credentialed sockets. |
| CORS credentials | `middleware.CORS` warns that zero value is permissive for public APIs, panics on `AllowCredentials=true` with wildcard origins, panics when `*` is mixed with explicit origins, and filters preflight requested headers against configured `AllowHeaders`. See [`middleware/cors.go`](../middleware/cors.go#L12-L24) and [`middleware/cors.go`](../middleware/cors.go#L78-L218). | Pass for dangerous wildcard-with-credentials behavior. Production browser-authenticated apps must configure explicit origins and headers. |
| Cookies, sessions, CSRF | `SetCookie` validates name/value/path/domain/expires/SameSite and rejects `SameSite=None` without `Secure`. Signed cookies use HMAC-SHA256, require 32-byte secrets, and support rotation. Session cookies are `HttpOnly`, default `SameSite=Lax`, and carry a TTL. CSRF uses a signed double-submit cookie/header token and caps token length. See [`types.go`](../types.go#L6025-L6126), [`cookie_signed.go`](../cookie_signed.go#L45-L157), [`middleware/session.go`](../middleware/session.go#L254-L390), and [`middleware/csrf.go`](../middleware/csrf.go#L77-L180). | Pass for framework primitives. Production apps must set `Secure` under TLS, choose cookie scope, rotate secrets, and use shared stores when horizontally scaled. |
| JWT validation | JWT middleware locks verification to the configured algorithm, rejects algorithm mismatch, requires strong HMAC secrets or matching asymmetric key families, validates `exp`/`nbf`, supports `Issuer`, `Audience`, `RequiredClaims`, and caps compact tokens at 16 KiB by default. See [`middleware/jwt.go`](../middleware/jwt.go#L31-L124), [`middleware/jwt.go`](../middleware/jwt.go#L167-L214), and [`middleware/jwt.go`](../middleware/jwt.go#L382-L533). | Pass for built-in validation. Production apps must configure issuer/audience/required identity claims and own key rotation or JWKS dispatch. |
| Body limits | `Config.BodyLimit` defaults to 4 MiB, `BodyReadTimeout` defaults to 30 seconds, `Response.Body` clamps route reads to the app cap, and multipart parsing defaults to an 8 MiB per-part cap unless an explicit option or external total-size cap is used. See [`types.go`](../types.go#L437-L521), [`types.go`](../types.go#L677-L728), [`types.go`](../types.go#L4950-L5024), and [`multipart.go`](../multipart.go#L17-L51). | Pass for app-level body controls. Production upload routes should choose explicit per-route limits. |
| Header limits | Header-derived auth values have parser-level caps: BasicAuth credentials default to 8 KiB, JWT compact tokens default to 16 KiB, and CSRF tokens default to 256 bytes. The public checklist still records total request-header size policy as residual risk. See [`middleware/basicauth.go`](../middleware/basicauth.go#L50-L60), [`middleware/jwt.go`](../middleware/jwt.go#L121-L130), [`middleware/csrf.go`](../middleware/csrf.go#L62-L81), and [`docs/security-checklist.md`](security-checklist.md#L177-L192). | Residual risk. Until a framework-wide header cap is exposed or documented from native/uWS behavior, production deployments must enforce reverse-proxy header limits and release notes must call this out. |
| Dependency vulnerability status | `govulncheck ./...` and `govulncheck -tags gogo ./...` both report the same two reachable Go standard-library vulnerabilities with `go1.26.3`; no vulnerable required module call path was reported. | Blocked by toolchain vulnerability status, not by a third-party module in `go.mod`. |

## Validation

Tooling:

- `go version`: `go1.26.3 darwin/arm64`
- `govulncheck -version`: `govulncheck@v1.3.0`, DB updated
  `2026-06-02 21:39:47 +0000 UTC`

Commands run:

| Command | Result |
| --- | --- |
| `go test ./...` | Pass |
| `CGO_ENABLED=1 go test -tags gogo ./...` | Pass |
| `govulncheck ./...` | Fail: affected by GO-2026-5039 and GO-2026-5037 in Go standard library packages from `go1.26.3`; fixed in `go1.26.4`. |
| `govulncheck -tags gogo ./...` | Fail: same GO-2026-5039 and GO-2026-5037 findings. |

## Residual Risk

- Total request-header size is not yet a public `Config` knob in the framework.
  Header-sourced credentials are capped, but production deployments should set
  request-header limits at nginx, Caddy, CDN, or load-balancer layers and record
  those limits in release/deployment notes.
- App-specific route configuration remains outside this framework evidence:
  concrete CORS origins, WebSocket origins, cookie flags, JWT audiences, session
  stores, body limits, and proxy CIDR ranges must be verified in each consuming
  application.

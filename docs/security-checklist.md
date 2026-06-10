# v1 Security Checklist

Use this checklist as the v1 Security Gate lane. Before cutting a v1 release
candidate, every unchecked item needs evidence in a linked issue, PR, release
note, or accepted residual-risk note.

Checked items record framework behavior that is already implemented and
documented. They are not deployment sign-off: production applications still
need to choose their own values, origins, secrets, proxy ranges, and limits.

## Reference Map

- Proxy trust and body limits: `gogo.Config`, `Request.Protocol`,
  `Request.Secure`, `Request.IP`, `Request.IPs`, the README configuration
  sections, and `docs/configuration.md`.
- WebSocket origin/auth: `WebSocketBehavior.Upgrade`, `UnsafeAutoUpgrade`,
  `middleware.WebSocketAuth`, and the README WebSocket upgrade-auth section.
- CORS: `middleware.CORS` and the README bundled middleware section.
- Cookies, CSRF, and sessions: `gogo.Cookie`, `Response.SetCookie`,
  `Response.SetCookieSigned`, `Request.CookieSigned`, `middleware.CSRF`, and
  `middleware.NewSession`.
- JWT: `middleware.JWT`, `JWTOptions`, `SignJWT`, and JWT middleware tests.
- Body/header limits: `Config.BodyLimit`, `Config.BodyReadTimeout`,
  `MultipartOptions`, `DefaultMultipartPartLimit`, `MaxTokenBytes` options,
  and the `No*Limit` sentinels.

## Proxy Trust

- [x] `TrustProxy` defaults off, and `TrustedProxies` provides an IP/CIDR
      allow-list for deployments where only specific immediate peers may supply
      forwarded headers.
- [x] `req.Protocol()`, `req.Secure()`, and `req.IPs()` ignore forwarded
      headers unless the immediate peer is trusted by `TrustProxy` or
      `TrustedProxies`.
- [x] `TrustedProxies` automatically enables peer-IP capture so async and
      shared-dispatch routes can decide whether forwarded headers are trusted.
- [x] README and `docs/configuration.md` document that the trusted edge must
      strip or overwrite client-supplied `X-Forwarded-*`, `Forwarded`, and
      `X-Real-IP` headers.
- [ ] v1 production examples use `TrustedProxies` instead of `TrustProxy: true`
      unless the surrounding network guarantees every immediate peer is trusted.
- [ ] Reverse-proxy deployment docs confirm header overwrite behavior for nginx,
      Caddy, Cloudflare, and load balancers, or link to accepted residual risk.

## WebSocket Origin And Auth

- [x] The nil `Upgrade` default accepts non-browser clients that omit `Origin`
      and rejects browser-style handshakes that include `Origin` unless an
      explicit upgrade callback is installed.
- [x] `UnsafeAutoUpgrade` is opt-in and documented for public, non-cookie
      endpoints where cross-origin WebSocket access is intentional.
- [x] `middleware.WebSocketAuth` has a fail-closed zero value, explicit
      `AllowedOrigins`, `AllowMissingOrigin` for non-browser clients, optional
      `Verify`, and `AllowedSubprotocols`.
- [x] README documents that HTTP CORS middleware does not protect the WebSocket
      upgrade path and calls out CSWSH risk.
- [ ] Every browser-capable WebSocket route installs an explicit `Upgrade`
      callback, preferably `middleware.WebSocketAuth`.
- [ ] Browser WebSocket origins are allow-listed exactly. `AllowedOrigins` set
      to `[]string{"*"}` appears only on public endpoints that do not rely on
      cookies, bearer headers, or other ambient credentials.
- [ ] Upgrade-time auth runs before `ctx.Accept`. Rejected handshakes return
      401 for auth failures or 403 for origin/policy failures.
- [ ] `AllowMissingOrigin` is used only for CLI or service-to-service clients
      that genuinely omit `Origin`, and those routes do not share cookie-backed
      browser sessions.
- [ ] Subprotocols are allow-listed when they carry authorization, tenant,
      schema, or protocol-version meaning.
- [ ] Production WebSocket routes choose explicit `MaxPayloadLength`,
      `IdleTimeout`, and `MaxBackpressure` values and test oversized, idle, and
      slow-consumer clients.

## CORS

- [x] `middleware.CORS` panics when `AllowCredentials=true` is combined with
      `AllowOrigins: []string{"*"}`.
- [x] `middleware.CORS` panics when wildcard and explicit origins are mixed.
- [x] Preflight `Access-Control-Allow-Headers` is filtered against the
      configured `AllowHeaders` list instead of echoing arbitrary requested
      headers.
- [x] README documents that `AllowOrigins: []string{"*"}` is for public APIs
      only and that WebSocket upgrades need their own origin checks.
- [ ] Production browser-authenticated apps configure explicit origins,
      methods, headers, exposed headers, credentials behavior, and `MaxAge`;
      they do not use the permissive CORS zero value.
- [ ] Credentialed CORS endpoints are paired with cookie/CSRF/JWT controls that
      do not depend on CORS as an authentication boundary.
- [ ] Wildcard subdomain patterns are reviewed for tenant isolation, takeover,
      and preview-environment risk before v1 release notes recommend them.

## Cookies And CSRF

- [x] `SetCookie` validates names, values, path, domain, expires, and SameSite
      attributes before writing `Set-Cookie`.
- [x] `SetCookie` rejects `SameSite=None` unless `Secure=true`.
- [x] `SetCookieSigned` and `CookieSigned` use HMAC-SHA256, require at least 32
      bytes for signing secrets, and support rotation by verifying multiple
      secrets.
- [x] `middleware.Session` requires a 32-byte secret, defaults session
      `SameSite` to `Lax`, and documents memory store vs external store use.
- [x] `middleware.CSRF` requires a 32-byte secret, defaults `SameSite` to `Lax`,
      caps token size by default, and protects unsafe methods with a
      double-submit cookie/header check.
- [ ] Auth/session cookies set `HttpOnly`, `Secure` in HTTPS deployments,
      bounded `MaxAge`/`TTL`, intentional `Path`/`Domain`, and `SameSite=Lax`
      or `Strict` unless a documented cross-site flow requires otherwise.
- [ ] Any `SameSite=None` auth/session cookie has `Secure=true` and an explicit
      CSRF strategy.
- [ ] Cookie values that carry authority are either signed with
      `SetCookieSigned`/`SignCookieValue` or map to server-side session state.
- [ ] Cookie, session, and CSRF secrets have a rotation procedure that accepts
      old keys only for the necessary expiry window.
- [ ] Horizontally scaled deployments use Redis, SQL, or another shared
      `SessionStore`; in-memory sessions are limited to single-process apps or
      accepted residual risk.

## JWT

- [x] `middleware.JWT` verifies only the configured `Algorithm` and rejects
      `alg: none` and algorithm-confusion attempts.
- [x] HMAC JWT secrets must be at least 32 bytes; asymmetric keys are checked
      against the selected RS, PS, or ES algorithm family at construction.
- [x] `exp` and `nbf` validation rejects expired/not-yet-valid tokens and
      malformed non-numeric claims.
- [x] `JWTOptions` supports `Issuer`, `Audience`, and `RequiredClaims`; tests
      cover accepted and rejected values.
- [x] `MaxTokenBytes` defaults to 16 KiB, and `NoJWTTokenLimit` is documented as
      safe only behind an external header-size limit.
- [ ] Production JWT middleware config sets expected `Algorithm`, key/secret,
      `Issuer`, `Audience`, required identity claims, and clock-skew `Leeway`
      intentionally.
- [ ] Token sources are documented per app. Authorization headers are preferred
      for browser/API flows; query-string tokens are short-lived and protected
      from logs if they are unavoidable.
- [ ] Key rotation behavior is documented. If `kid`/JWKS is needed, the app
      owns a verifier/cache wrapper because built-in JWKS dispatch is not
      first-class.

## Body Limits

- [x] `Config.BodyLimit` defaults to 4 MiB and `NoBodyLimit` is documented for
      trusted deployments that enforce an external body-size cap.
- [x] `Config.BodyReadTimeout` defaults to 30 seconds and can be tuned for API
      vs upload routes.
- [x] Declared `Content-Length`, chunked `OnData`, `Response.Body`, and PUT /
      PATCH / DELETE helpers are covered by body-limit tests.
- [x] `Response.Body(maxBytes, ...)` clamps to the lower of the route cap and
      the app-level `BodyLimit`.
- [x] Multipart parsing has an 8 MiB default per-part cap, route-level
      `MultipartOptions{MaxPartBytes: ...}`, and `NoMultipartPartLimit`
      guidance that requires another total-size cap.
- [ ] v1 production examples choose explicit body limits for normal APIs and
      separate upload endpoints instead of disabling `BodyLimit` globally.
- [ ] Any use of `NoBodyLimit`, `NoBodyReadTimeout`, or
      `NoMultipartPartLimit` is tied to a reverse-proxy/body-storage boundary
      that enforces an equal or stricter limit.

## Header Limits

- [x] Header-sourced credential parsers keep individual caps by default:
      `BasicAuth.MaxCredentialBytes`, `JWTOptions.MaxTokenBytes`, and
      `CSRFOptions.MaxTokenBytes`.
- [x] `NoBasicAuthCredentialLimit`, `NoJWTTokenLimit`, and `NoCSRFTokenLimit`
      are documented as safe only behind an external header-size limit.
- [ ] Define the v1 request-header size policy: public `Config`, documented
      native/uWS cap, required reverse-proxy cap, or accepted residual risk.
- [ ] Reverse-proxy docs set concrete request-header limits for normal headers,
      cookies, JWT/CSRF token headers, CORS preflight headers, and WebSocket
      upgrade headers.
- [ ] Large-header tests cover normal HTTP routes, async/shared snapshots,
      JWT/CSRF/BasicAuth middleware, CORS preflights, and WebSocket upgrades.
- [ ] Release notes call out any remaining header-limit risk, especially when a
      deployment disables token/credential caps with `No*Limit` sentinels.

# v1 Security Checklist

Use this checklist as the Security Gate lane for v1 readiness. For each item,
record the evidence in a linked issue, PR, or release note before cutting a
release candidate. Unchecked items are release blockers unless explicitly
accepted as residual risk.

## Proxy Trust

- [x] Decide whether `Config.TrustProxy bool` is final for v1 or must become a
      trusted CIDR/range allow-list. v0.3 extends it with
      `Config.TrustedProxies` while keeping the bool as a compatibility
      shortcut.
- [x] When `TrustProxy` is enabled, document the trusted edge component that
      strips or overwrites `X-Forwarded-*`, `Forwarded`, and `X-Real-IP` from
      untrusted clients.
- [x] Verify `req.Protocol()`, `req.Secure()`, `req.IP()`, and `req.IPs()` do
      not trust forwarded headers when the app is directly internet-facing.
- [x] Document `CapturePeerIP` requirements for async/shared-dispatch routes,
      rate limiting, audit logs, and auth decisions that depend on peer IP.

## WebSocket Origin And Auth

- [ ] Every browser-capable WebSocket route installs an explicit
      `WebSocketBehavior.Upgrade` callback, preferably `middleware.WebSocketAuth`.
- [ ] `UnsafeAutoUpgrade` is absent from authenticated or cookie-bearing
      endpoints; any use is documented as public and intentionally cross-origin.
- [ ] `AllowedOrigins` is explicit for browser clients. Wildcard origins are
      accepted only for public endpoints that do not rely on ambient credentials.
- [ ] Upgrade verification authenticates the user or client before `Accept`, and
      rejected handshakes return an appropriate 401/403.
- [ ] Subprotocols are allow-listed when they carry authorization or protocol
      version meaning.
- [ ] `MaxPayloadLength`, `IdleTimeout`, and `MaxBackpressure` are documented for
      production WebSocket routes and covered by tests for oversized, idle, and
      slow-consumer clients.
- [ ] The nil `Upgrade` default, missing `Origin`, and legacy
      `Sec-WebSocket-Origin` behavior are frozen and documented for v1.

## CORS

- [ ] CORS policy is explicit for production; the permissive zero value is not
      used for browser-authenticated applications.
- [ ] `AllowCredentials=true` is used only with explicit trusted origins, never
      with `*`.
- [ ] Allowed methods and headers are minimized to what clients actually need,
      and preflight responses do not echo arbitrary requested headers.
- [ ] CORS documentation states that HTTP CORS middleware does not protect
      WebSocket upgrades.

## Cookies And CSRF

- [ ] Auth/session cookies use `HttpOnly`, bounded lifetime, and `Secure` in
      HTTPS production deployments.
- [ ] `SameSite=Lax` or `Strict` is the default for auth/session cookies.
      `SameSite=None` is used only with `Secure` and an explicit CSRF strategy.
- [ ] Cookie values that carry authority are signed with at least 32 bytes of
      secret entropy or map to server-side session state.
- [ ] Cookie-signing and session secrets have a documented rotation procedure
      that accepts old keys only for the necessary expiry window.
- [ ] Cookie-authenticated unsafe methods use CSRF protection or document why
      they are not reachable from browsers.
- [ ] Session storage guidance distinguishes single-process memory storage from
      Redis/SQL storage required for horizontally scaled deployments.

## JWT

- [ ] JWT middleware requires the expected algorithm and rejects `alg: none` or
      algorithm-confusion attempts.
- [ ] HMAC secrets are at least 32 bytes of entropy; asymmetric keys are matched
      to their configured algorithm family.
- [ ] `exp` and `nbf` validation is covered by tests, including non-numeric and
      expired values.
- [ ] v1 either adds issuer, audience, and required-claim validation or documents
      the supported extension point and the release risk.
- [ ] Token sources are documented. Query-string tokens are avoided for browser
      flows unless short-lived and protected from logging.
- [ ] `MaxTokenBytes` remains enabled by default; `NoJWTTokenLimit` is allowed
      only behind a documented header-size limit.
- [ ] Key rotation and `kid`/JWKS behavior are documented if not first-class.

## Body And Header Limits

- [ ] `Config.BodyLimit` default and production recommendations are documented,
      including when `NoBodyLimit` is acceptable.
- [ ] `BodyReadTimeout` is documented for slow upload protection, with guidance
      for API and legitimate upload endpoints.
- [ ] Per-route body collection clamps to the app-level `BodyLimit`; tests cover
      declared `Content-Length`, chunked bodies, `Response.Body`, and `OnData`.
- [ ] Multipart parsing keeps a per-part cap through `MultipartOptions` or
      `DefaultMultipartPartLimit`; disabling it requires another total-size cap.
- [ ] Define the v1 request-header limit policy: public config, native/uWS cap,
      or required reverse-proxy cap.
- [ ] Large-header tests cover normal HTTP routes, async/shared snapshots,
      JWT/CSRF token headers, CORS preflight headers, and WebSocket upgrades.
- [ ] `NoJWTTokenLimit` and `NoCSRFTokenLimit` are documented as unsafe unless
      an external header limit is enforced.

## File Serving And Uploads

- [ ] `SendFile` and `Download` callers never pass raw request paths directly to
      the filesystem; paths are rooted, cleaned, authorized, and allow-listed.
- [ ] v1 either ships a rooted file-serving helper or documents safe patterns for
      `SendFile`, `Download`, and multipart saves.
- [ ] `MaxSendFileBytes`, `SendFileChunkBytes`, and
      `SendFileBackpressureBytes` have production guidance and tests for large
      files and slow consumers.
- [ ] `NoSendFileLimit` is used only for trusted, authorized file routes with an
      external size boundary.
- [ ] Multipart uploads validate filenames, extensions/content type, overwrite
      behavior, file permissions, and storage location before saving.
- [ ] Download filenames and response headers cannot be influenced by unchecked
      CR/LF, path separator, or control characters.

## Native Boundary

- [ ] Every Go-to-C/C-to-Go string, length, and buffer conversion is checked for
      truncation, overflow, and lifetime safety.
- [ ] Native request snapshots have documented caps for method, URL, query,
      params, headers, peer IP, and body, including fallback behavior when caps
      are exceeded.
- [ ] `cgo.Handle` values attached to WebSockets or callbacks are released on
      every close, reject, panic, and shutdown path.
- [ ] Loop-thread ownership and allowed goroutine/thread usage are documented for
      route registration, `Listen`, `Run`, `Close`, WebSocket send/end, and
      deferred responses.
- [x] Shared-dispatch shutdown quiesces active work before native app memory is
      freed.
- [ ] Shared handler registry cleanup releases per-app handler closures after
      graceful/shared drain without reusing stale handler IDs unsafely.
- [ ] Native fuzz/stress coverage includes oversized headers, bodies, WebSocket
      frames, malformed subprotocols, aborted streams, and shutdown races.
- [ ] Vendored native dependencies, patches, licenses, and required build tools
      are documented for Linux and macOS release builds.

## Required Release Checks

- [ ] `go test ./...`
- [ ] `CGO_ENABLED=1 go test -tags gogo ./...`
- [ ] `govulncheck ./...`
- [ ] `govulncheck -tags gogo ./...`
- [ ] Race and stress suites for HTTP, middleware, WebSocket, async/shared
      dispatch, file serving, and native shutdown paths.
- [ ] Downstream smoke test from a clean module:
      `go get github.com/Snocko-main/gogo@<sha>` and
      `CGO_ENABLED=1 go build -tags gogo .`
- [ ] Release-tag smoke test repeats the downstream build for `@vX.Y.Z` and
      `@latest`.
- [ ] CI matrix covers supported Linux/macOS targets, C++20, zlib, Go 1.24, and
      the current stable Go version.
- [ ] Release notes list all accepted residual security risks and confirm that no
      security blocker remains open.

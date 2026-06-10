# Changelog

Notable changes to gogo are documented here.

The project uses the `v0.x` line as a public preview. Until `v1.0.0`,
minor releases may include breaking API changes when they move the project
toward a stable API. Breaking changes should be called out in the release
notes for the release that introduces them.

## Unreleased - v1.0.0-rc.1

This section prepares the release notes for the `v1.0.0-rc.1` tag. The tag has
not been cut yet. These notes summarize notable changes since the `v0.1.x`
public preview, using the completed roadmap items and merged release history as
the source of truth.

### Routing and Public API

- Locked v1 routing decisions for `App.Use(args ...any)`, `Get(pattern,
  target any)`, `Router.Get`, route naming, `Group`-based scoping, route
  pattern syntax, route matching, reverse routing, and precedence behavior.
- Fixed dynamic and wildcard `MethodNotAllowed` behavior and tightened child
  route pattern validation.
- Added compatibility coverage for route matching and reverse routing.
- Added `Request.ParamInt64`.
- Exported and documented `ErrStreamAborted` for streaming drain behavior.
- Fixed async body timeout behavior and clarified synchronous body parsing and
  `ErrNoBody` documentation.
- Refreshed Server-Sent Events guidance for async body methods and disconnect
  handling.

### Config and Lifecycle

- Kept `NewApp(cfg ...Config)` as the v1-compatible constructor shape and
  validated unsupported extra configs, negative limits, disabled limits,
  timeout sentinels, and zero-value defaults.
- Added `ShutdownContext(ctx)` as the blocking graceful-shutdown API.
- Locked lifecycle hook behavior for nil hooks, panic recovery, and repeated
  shutdown calls.
- Kept `SetPanicHandler` as the supported global panic-recovery model.
- Added trusted proxy CIDR/range support and documented `CapturePeerIP`
  behavior for async and shared-dispatch routes.
- Documented multicore config and immediate shutdown limits.
- Defined native loop and OS-thread ownership for single-app use.
- Defined shared-dispatch quiescence and cleaned up shared handler registry
  lifetime so stale handler IDs cannot call removed handlers.

### Middleware and Security

- Standardized middleware error behavior, `OnError` handling, ordering, and
  async placement guidance.
- Added Redis/external-store guidance for sessions and Redis integration tests
  for the rate-limit adapter.
- Prevented async rate limiting from using an empty or spoofable default IP key
  when `CapturePeerIP` is disabled.
- Added JWT issuer, audience, and required-claim validation.
- Rechecked CORS wildcard and credentials behavior plus Session, CSRF, JWT,
  BasicAuth, Helmet/HSTS, and WebSocketAuth defaults.
- Added production auth-stack guidance and a v1 security checklist covering
  proxy trust, WebSocket origin/auth, CORS, cookies, JWT, body limits, and
  header limits.
- Added safe file-serving guidance for `SendFile`, `Download`, and multipart
  saves.

### WebSocket and Streaming

- Documented the `WSHubAdapter` contract for ordering, retry, delivery
  guarantees, cancellation, slow subscribers, close semantics, start
  idempotency, deliver concurrency, worker ordering, and topic adapter retry
  behavior.
- Added Redis `WSHub` integration tests and WebSocket hub stress tests.
- Froze `UnsafeAutoUpgrade`, nil `Upgrade`, browser Origin, and
  `WebSocketAuth` default semantics before v1.
- Documented WebSocket auth, origin, and subprotocol production usage.
- Added stream/SSE backpressure memory benchmark coverage.

### Testing, Performance, and Operations

- Added `TestServerOptions`.
- Added testing documentation for sync, async, body, middleware, WebSocket, and
  graceful shutdown paths, including native `NewTestServer` serialization.
- Documented `HTTPAdapter` use for pprof and expvar migration/debug endpoints.
- Added reproducible benchmark baselines for HTTP routes, async workloads,
  stream backpressure, and WebSocket publish batches.
- Pre-lowercased CORS origins at construction and clamped native length, size,
  and config-width conversions across the cgo boundary.
- Stabilized `middleware.Metrics` labels, buckets, and output format.
- Documented OpenTelemetry integration direction, `RunMultiCore`, graceful
  shutdown with OS signals, reverse proxy deployment, `ulimit`, `GOMAXPROCS`,
  DB pool sizing, and panic/error logging.

### Release Readiness

- Added CI coverage for normal and native test jobs, release vetting,
  vulnerability checks, downstream smoke builds, native source tracking, branch
  protection expectations, and release checklist steps.
- Release candidate freeze policy is documented in
  `docs/release-candidate.md`.

## v0.1.1 - 2026-06-05

### Added

- Root Apache-2.0 project license and third-party notices for bundled
  uWebSockets/uSockets native sources.

## v0.1.0 - 2026-05-30

Initial public preview release of gogo.

### Added

- Native uWebSockets/uSockets-backed HTTP server binding for Go with the
  opt-in `gogo` build tag and vendored native sources.
- Core routing for sync handlers, async handlers, static replies, route
  groups, mounting, typed route parameters, named routes, reverse routing,
  query access, custom 404/405 handlers, and panic recovery.
- Response helpers for one-shot sends, JSON/JSONP, redirects, file and
  download responses, streaming, templates, cookies, and signed cookies.
- Request body support for bounded async body collection and multipart
  handling.
- Middleware package covering CORS, Helmet, compression, logging, request
  IDs, rate limiting, sessions, CSRF, JWT, basic auth, metrics, async
  middleware, and WebSocket auth helpers.
- Server-Sent Events and WebSocket support, including pub/sub, `WSHub`,
  multicore fanout, and Redis-backed hub and rate-limit adapters.
- Testing and migration helpers, including `NewTestServer`,
  `NewTestServerT`, `HTTPAdapter`, and `HTTPAdapterWithBody`.
- Examples and benchmark fixtures for HTTP, middleware, uploads, SSE,
  WebSocket, multicore, and comparable framework baselines.

### Notes

- Native runtime use requires Go 1.24 or newer, cgo, a C compiler, a
  C++20-capable compiler, zlib, and `-tags gogo`.
- Without `-tags gogo`, the package builds a stub so ordinary Go tooling can
  import the module, but `NewApp` returns a setup error instead of running a
  native uWebSockets server.
- The `v0.1.0` tag did not include a root license file; the project license is
  documented in a later `v0.1.x` release.

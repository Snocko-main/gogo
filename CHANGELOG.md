# Changelog

Notable changes to gogo are documented here.

The project uses the `v0.x` line as a public preview. Until `v1.0.0`,
minor releases may include breaking API changes when they move the project
toward a stable API. Breaking changes should be called out in the release
notes for the release that introduces them.

## Unreleased

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

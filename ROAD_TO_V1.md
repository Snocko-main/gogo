# Road to v1.0.0

This document is the execution plan after `v0.1.0`. It is intentionally
split into small PR-sized tasks so humans and agents can work in parallel
without stepping on the same files.

`ROAD_TO_V1.md` is the source of truth for active v1 work. The older
`ROADMAP.md` has been removed to avoid competing plans.

## Release Policy

- `v0.x`: public preview. Breaking API changes are allowed when they move the
  project closer to a stable `v1`.
- `v0.9.x`: release-candidate period. Breaking changes require an explicit
  v1-readiness reason.
- `v1.0.0`: routing, middleware, WebSocket, testing, and config APIs are frozen
  except for backward-compatible additions.

## Agent Lanes

Use separate agents for independent lanes. Agents should not edit overlapping
files in the same PR unless one agent is explicitly assigned as integrator.

| Lane | Main ownership | Output |
| --- | --- | --- |
| Go API | `types.go`, `testing.go`, public Go docs, API tests | API stability findings, small compatibility PRs |
| C++ / Native | `native_enabled.go`, `uws_bridge.*`, `usockets_vendor.c`, `internal/native/**`, build scripts | bridge safety, build portability, native perf PRs |
| Middleware | `middleware/**`, `adapters/**`, middleware docs/tests | production defaults, store behavior, integration tests |
| WebSocket | `ws_*.go`, `ws_hub*.go`, Redis WS adapter/tests | hub contract, backpressure/security tests |
| Security Gate | review across Go API, Native, Middleware, and WebSocket lanes | threat model, security issues, release blockers |
| Docs / DX | `README.md`, `doc.go`, examples, CI docs | install/build docs, changelog, migration guides |
| Release Engineering | `.github/**`, `scripts/**`, release docs | CI, smoke tests, release checklist, branch protection notes |

## Parallel Work Rules

1. One branch per agent lane, prefixed with `feat/`, `fix/`, `chore/`, or
   `doc/` according to the change type.
2. Each PR should have a single owner lane and a short issue checklist.
3. Avoid cross-lane edits unless the PR description names the dependency.
4. Every issue should state owner lane, type, touched files, required tests,
   and whether it is a decision, docs PR, API PR, CI PR, or implementation PR.
5. Every PR must run:
   - `go test ./...`
   - `CGO_ENABLED=1 go test -tags gogo ./...`
6. Native or security PRs must include a downstream smoke build when relevant:
   - `go get github.com/Snocko-main/gogo@<sha>`
   - `CGO_ENABLED=1 go build -tags gogo .`
7. Security-gate work should review and file issues, not make broad cross-lane
   edits unless explicitly assigned as integrator.
8. Review output should start with findings ordered by severity, then tests,
   then residual risk.

## v0.1.x - Release Hygiene

- [x] Migrate any useful historical context from the removed `ROADMAP.md` into
      issues or this plan before opening v1 work.
- [x] Add `CHANGELOG.md` starting with `v0.1.0`.
- [x] Choose and add a root `LICENSE` / notice file before presenting the
      project as an open-source framework.
- [x] Add `SECURITY.md` with supported versions and vulnerability reporting.
- [x] Add CI matrix for Linux and macOS with C++20, zlib, Go 1.24, and the
      current stable Go version.
- [x] Add normal test job: `go test ./...`.
- [x] Add native test job: `CGO_ENABLED=1 go test -tags gogo ./...`.
- [x] Decide whether `go vet` and `CGO_ENABLED=1 go vet -tags gogo ./...` are
      CI gates; if yes, fix or document the native vet baseline first.
- [x] Add `govulncheck ./...` and `govulncheck -tags gogo ./...` as release
      checks.
- [x] Add a downstream smoke script or exact temp-module recipe that tests
      `@<sha>`, `@vX.Y.Z`, and `@latest` by importing gogo and building with
      `CGO_ENABLED=1 go build -tags gogo .`.
- [x] Add a tracked-file check proving downstream builds do not depend on local
      `third_party/`.
- [x] Add README version policy for `v0.x` vs `v1`.
- [x] Add release checklist for future tags.
- [x] Document branch protection expectations before the v1 release candidate.
- [x] Verify runnable README examples and `examples/...` packages compile;
      README snippets are treated as documented fragments.

## v0.2.0 - Correctness and API Freeze Prep

- [x] Decide whether `App.Use(args ...any)` remains as the v1 API.
- [x] Decide whether `Get(pattern, target any)` and `Router.Get` keep the
      `any` target API, or get typed helpers before v1.
- [x] Add or explicitly reject async method helpers for `PutAsync`,
      `PatchAsync`, and `DeleteAsync` on both `App` and `Router`, including
      body collection semantics and middleware behavior.
- [x] Explicitly skip typed helpers such as `UsePath` / `UseAsyncPath` for
      v1; `Group` remains the preferred typed scoping API.
- [x] Fix or permanently document `MethodNotAllowed` behavior for dynamic and
      wildcard routes.
- [x] Lock router child-pattern validation so `router.Get("users", ...)` cannot
      accidentally register an unexpected concatenated path.
- [ ] Decide whether route registration should return a route handle for
      fluent naming, e.g. `app.Get(...).Name(...)`.
- [x] Freeze route pattern syntax: named params, typed params, wildcard,
      trailing slash, and case-sensitivity behavior.
- [x] Add table-driven compatibility tests for route matching and reverse
      routing.
- [x] Document group, mount, scoped middleware, and precedence rules.
- [x] Add a public global-state audit for `SetPanicHandler`,
      `RegisterParamType`, worker-count settings, and package-level limits.
- [ ] Add request/response API cleanup issues for exported abort/drain errors,
      stale docs, and any missing named helpers.

## v0.3.0 - Config Behavior

- [ ] Decide whether `NewApp(cfg ...Config)` stays variadic for v1.
- [x] Validate config inputs before v1: extra configs, negative limits,
      disabled limits, timeout sentinels, and zero-value defaults.
- [ ] Add `ShutdownContext(ctx)` or another blocking graceful-shutdown API if
      the current non-blocking graceful shutdown is not enough.
- [x] Fix lifecycle hook contract: `OnListen` panic recovery, nil hooks, and
      repeated `Shutdown` / `ShutdownGracefully` hook behavior.
- [ ] Add app-scoped panic handling in `Config`, or explicitly keep the global
      handler as the supported model.
- [ ] Add `RunMultiCore` config/options support or explicitly document the
      current process-wide config limits.
- [ ] Add graceful multicore shutdown or document immediate shutdown only.
- [ ] Replace or extend `TrustProxy bool` with trusted proxy CIDR/range support
      before v1, or document why the boolean API is final.
- [ ] Document `CapturePeerIP` behavior for async and shared-dispatch routes.
- [ ] Add config examples for local development, reverse proxy, and production.
- [x] Add tests that lock zero-value config defaults.
- [ ] Define uWS loop and OS-thread ownership for single-app usage: own a
      locked native loop goroutine internally, or document/enforce same-thread
      `NewApp` / route registration / `Listen` / `Run` / `Close`.
- [ ] Define shared-dispatch quiescence before freeing native app memory.
- [ ] Decide shared handler registry lifetime: cleanup, generations/tombstones,
      or documented process-lifetime retention.

## v0.4.0 - Middleware Production Pass

- [ ] Standardize middleware error behavior: fail-open, fail-closed, and
      `OnError` semantics.
- [ ] Add Redis or external store guidance for sessions.
- [ ] Add Redis integration tests for the rate-limit adapter.
- [ ] Prevent async rate limiting from using an empty or spoofable default IP
      key when `CapturePeerIP` is disabled.
- [ ] Add first-class JWT issuer, audience, and required-claim validation.
- [ ] Recheck CORS wildcard and credentials behavior.
- [ ] Recheck Session, CSRF, JWT, BasicAuth, and WebSocketAuth defaults.
- [ ] Revisit middleware zero-value production defaults, especially CORS and
      Helmet/HSTS behavior.
- [ ] Document middleware ordering and async placement.
- [ ] Add a production-ish auth stack example.

## v0.5.0 - WebSocket Stabilization

- [ ] Write the `WSHubAdapter` contract: ordering, retry, delivery guarantees,
      cancellation, slow subscribers, and close semantics.
- [ ] Include `WSHubAdapter.Start` idempotency, deliver concurrency, worker
      ordering, close/cancel behavior, and topic adapter retry semantics in the
      contract.
- [ ] Add Redis WSHub integration tests.
- [ ] Decide whether to expose drain/backpressure visibility before v1.
- [ ] Decide whether ping/pong callbacks belong in the public API.
- [ ] Freeze `UnsafeAutoUpgrade` naming and default security behavior.
- [ ] Freeze nil `Upgrade`, browser Origin, and `WebSocketAuth` default
      semantics before v1.
- [ ] Document or enforce the thread contract for `WebSocket.Send`,
      `SendText`, and `End`, or add safe deferred send/close APIs.
- [ ] Add WebSocket auth/origin/subprotocol production example.
- [ ] Add stress tests for subscribe, publish, unsubscribe, close, and hub
      adapter failure.

## v0.6.0 - Testing Helpers and Migration

- [ ] Add `TestServerOptions`.
- [ ] Add a setup variant that can return an error.
- [ ] Document why `NewTestServer` serializes native tests.
- [ ] Decide whether `TestServer.App()` permits route or middleware
      registration after the server has started; test or document the contract.
- [ ] Decide whether to expose a public WebSocket test client.
- [ ] Decide whether `HTTPAdapter` is a migration API, a testing helper, or
      both.
- [ ] Add pprof and expvar examples through `HTTPAdapter`.
- [ ] Add testing docs for sync, async, body, middleware, WebSocket, and
      graceful shutdown paths.

## v0.7.0 - Performance and Native Polish

- [ ] Add reproducible benchmarks for plain, params, JSON, middleware, async,
      and WebSocket routes.
- [ ] Define a cgo crossing budget for hot request paths.
- [ ] Add shared async close-safety benchmarks or stress tests.
- [ ] Add stream/SSE backpressure memory benchmarks.
- [ ] Add WebSocket publish-batch baselines.
- [ ] Pre-lowercase CORS origins at construction.
- [ ] Pre-grow pending header buffers.
- [ ] Clamp and validate all native length, size, and config-width conversions
      across the cgo boundary.
- [ ] Measure every perf PR before and after.
- [ ] Consider C++-side prefetch for method, query, and common headers.
- [ ] Keep performance docs tied to reproducible commands and raw results.

## v0.8.0 - Observability and Ops

- [ ] Stabilize `middleware.Metrics` labels, buckets, and output format.
- [ ] Decide the OpenTelemetry story: built-in hooks or documented middleware.
- [ ] Add `RunMultiCore` production example.
- [ ] Add graceful shutdown example with OS signal handling.
- [ ] Add reverse proxy docs for nginx, Caddy, Cloudflare, and load balancers.
- [ ] Add deployment notes for `ulimit`, `GOMAXPROCS`, and DB pools.
- [ ] Add panic/error logging docs.
- [ ] Add safe file-serving guidance or a rooted helper for `SendFile`,
      `Download`, and multipart saves.
- [ ] Add a v1 security checklist covering proxy trust, WebSocket origin/auth,
      CORS, cookies, JWT, body limits, and header limits.

## v0.9.0 - v1 Release Candidate

- [ ] Freeze public API list.
- [ ] Audit every exported symbol for naming, behavior, and compatibility.
- [ ] Audit README, `doc.go`, examples, and generated package docs.
- [ ] Run stress and race suites.
- [ ] Run downstream smoke tests from a clean module.
- [ ] Tag `v1.0.0-rc.1`.
- [ ] Accept only blocker fixes or explicitly approved v1-readiness changes.

## v1.0.0 Criteria

- [ ] No known security blocker remains open.
- [ ] Routing, middleware, WebSocket, testing, and config APIs are stable.
- [ ] CI passes normal and native builds on supported platforms.
- [ ] Install and build requirements are documented.
- [ ] Production examples cover HTTP, middleware, WebSocket, and graceful
      shutdown.
- [ ] Changelog and release notes are complete.
- [ ] Benchmark baseline is reproducible.
- [ ] Downstream install smoke test passes for the release tag.

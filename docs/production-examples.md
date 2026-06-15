# Production Examples Coverage

This page maps the v1 production-example criteria to the runnable examples and
long-form docs. It is a coverage index, not a deployment template for every
service shape.

## Coverage Map

| Area | Runnable examples | Primary docs | Production checks shown |
| --- | --- | --- | --- |
| HTTP routes | `examples/hello`, `examples/restapi`, `examples/upload`, `examples/httpadapter` | README routing, handler styles, request body parsing, file uploads, and `docs/ops.md` | sync vs async handlers, JSON responses, explicit body caps, safe file/upload patterns, stdlib adapter limits |
| Middleware stack | `examples/authmw` | README middleware section, `docs/configuration.md`, `docs/security-checklist.md` | Helmet, credentialed CORS, session/CSRF, BasicAuth, JWT issuer/audience/claims, production env checks |
| WebSocket | `examples/websocket`, `examples/authmw` | README WebSocket section, `docs/websocket-hub-adapter.md`, `docs/testing.md` | explicit `Upgrade`, `middleware.WebSocketAuth`, origin allow-list, token verification, subprotocols, payload/idle/backpressure limits, hub pub/sub |
| Graceful shutdown | `examples/graceful` | README Graceful Shutdown, package docs in `doc.go`, `docs/testing.md` | signal handling, `ShutdownContext`, deadline fallback, waiting for `Run` before `Close` |
| Multicore operations | `examples/multicore` | README Multi-core, `docs/ops.md`, `docs/global-state.md` | shared resources outside setup, `gogo.Run`, `GOMAXPROCS`, optional core/worker overrides, metrics endpoint, immediate worker-group shutdown boundary |
| Install/build | README Requirements and Native Build, `docs/install-build.md`, `docs/downstream-smoke.md` | `docs/release-checklist.md` | Go/cgo/C++20/zlib prerequisites, `-tags gogo`, native example builds, downstream clean-module smoke |

## Production Shape

A typical single-app production service should combine:

- `gogo.NewApp(gogo.Config{...})` with an intentional `BindAddr`,
  `BodyLimit`, `BodyReadTimeout`, and `TrustedProxies`.
- cheap sync middleware for headers, request IDs, CORS preflights, and simple
  auth decisions.
- async routes or async middleware for database, Redis, HTTP client, and other
  blocking work.
- explicit request-body caps on body-async routes.
- WebSocket `Upgrade` callbacks, usually via `middleware.WebSocketAuth`, for
  browser origins, tokens, and subprotocols.
- `ShutdownContext` on SIGINT/SIGTERM, then `Close` after `Run` returns.

`examples/authmw`, `examples/websocket`, and `examples/graceful` are the most
direct starting points for this shape.

## Boundaries To Keep Visible

- gogo serves plaintext HTTP; terminate TLS, HTTP/2, HTTP/3, CDN policy, and
  edge request-size policy at a reverse proxy or load balancer.
- HTTP CORS middleware does not protect WebSocket upgrades. Browser-capable
  WebSocket routes need their own origin/auth checks.
- Multicore helpers expose immediate group shutdown, not the single-app
  graceful drain API. `RunWithOptions` can apply one `Config` to every worker.
- In-memory session and rate-limit stores are process-local. Use a shared store
  such as Redis when limits or sessions must be consistent across replicas.
- Native builds currently target macOS and Linux and require the toolchain
  listed in `docs/install-build.md`.

## Validation Commands

Run these when changing README snippets, package docs, or examples:

```sh
scripts/check_readme_examples.sh
go test ./examples/...
CGO_ENABLED=1 go test -tags gogo ./examples/...
```

For release install behavior, also run the downstream smoke build from a clean
temporary module:

```sh
scripts/downstream_smoke_build.sh "$(git rev-parse HEAD)" vX.Y.Z latest
```

# gogo~

A Go HTTP framework built on the uWebSockets C++ HTTP server, designed for
low cgo overhead and high concurrency on real-world IO-bound workloads.

```sh
go get github.com/Snocko-main/gogo
```

```go
import gogo "github.com/Snocko-main/gogo"
```

It is intentionally thin:

- Go owns route registration and handlers.
- C++ owns uWebSockets templates, the event loop, and response/request calls.
- Async dispatch uses a shared-memory ring so the request hot path crosses
  cgo zero times for shared-mode handlers.

## Scope

- Non-TLS `uWS::App` (TLS / HTTP/2 / WS publish-subscribe out of scope for now)
- `GET`, `POST`, `Any` routes
- `GetAsync` / `PostAsync` with goroutine handlers + request snapshot
- Middleware chain (`app.Use`)
- Cookies, JSON helpers
- Graceful shutdown, panic recovery, request validation
- WebSocket open / message / close / send / end

## Install Native Dependencies

The binding expects uWebSockets to be vendored here:

```txt
third_party/uWebSockets
```

One way to set that up:

```sh
sh scripts/bootstrap_uwebsockets.sh
```

Then run an example:

```sh
CGO_ENABLED=1 go run -tags gogo ./examples/hello
curl http://localhost:3000/hello/inon
```

Without `-tags gogo`, the package builds a stub and `NewApp` returns a
clear setup error. This keeps normal Go tooling usable before the native
dependency is present.

## Examples

- `examples/hello` — static reply + sync handler + async handler
- `examples/restapi` — in-memory CRUD with JSON + query filter
- `examples/authmw` — logger middleware (before+after) + bearer auth
- `examples/upload` — POST body collection with 413 + low-level OnData stream

## Why There Is a C++ Bridge

uWebSockets is not a C library. Its public API is C++ template-heavy, so cgo
cannot call it directly in a pleasant or stable way. The `uws_bridge.cpp` file
turns the parts Go needs into a small C ABI.

## Caveats

- `*Request` and `*Response` from sync handlers are valid only during the
  handler callback. Async handlers receive a snapshot Request that survives
  past the C-side lifetime.
- `GetShared` was merged into `GetAsync` — pick the shared path automatically
  when no middleware is registered.
- `net/http` middleware is not directly compatible (different signature).

## Benchmarking

There are three comparable HTTP benchmark servers:

- `benchmark/nethttp`: Go standard library `net/http`
- `benchmark/gogo`: this binding
- `benchmark/fiber`: gofiber/fiber on fasthttp

Start each in a separate terminal:

```sh
go run ./benchmark/nethttp
CGO_ENABLED=1 go run -tags gogo ./benchmark/gogo
go run ./benchmark/fiber
```

Run wrk against them (use `-t 1` so client threads don't compete with the
single-threaded uWS loop for CPU):

```sh
wrk -t 1 -c 100 -d 10s http://localhost:3002/health   # gogo
wrk -t 1 -c 100 -d 10s http://localhost:3004/health   # fiber
```

Use the same machine, same power mode, same payloads, and repeat each run
3–5 times. Watch both throughput and latency percentiles.

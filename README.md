# uWebSockets for Go

This is a small cgo binding that makes the C++ uWebSockets HTTP app usable from
Go.

It is intentionally thin:

- Go owns route registration and handlers.
- C++ owns uWebSockets templates, the event loop, and response/request calls.
- Request and response objects are valid only during the handler callback.

## Current Scope

- Non-TLS `uWS::App`
- `GET`, `POST`, and `ANY` routes
- Request URL, headers, and route parameters
- Response status, headers, write, and end
- WebSocket open, message, close, send, and end callbacks

TLS app options, publish/subscribe, async responses, backpressure tuning, and
multi-threaded app-per-core orchestration are deliberately left out of the first
pass.

## Install Native Dependencies

The binding expects uWebSockets to be vendored here:

```txt
uwebsockets/third_party/uWebSockets
```

One way to set that up:

```sh
sh scripts/bootstrap_uwebsockets.sh
```

Then run the example:

```sh
CGO_ENABLED=1 go run -tags uwebsockets ./example/hello
curl http://localhost:3000/hello/inon
```

Without `-tags uwebsockets`, the package builds a stub and `NewApp` returns a
clear setup error. This keeps normal Go tooling usable before the native
dependency is present.

## Why There Is a C++ Bridge

uWebSockets is not a C library. Its public API is C++ template-heavy, so cgo
cannot call it directly in a pleasant or stable way. The `uws_bridge.cpp` file
turns the parts Go needs into a small C ABI.

## Caveats

- Do not store `*Request` or `*Response` outside a handler.
- Do not call response methods from another goroutine in this first version.
- Do not expect `net/http` middleware compatibility.
- Treat this as a native binding, not a pure Go server.

## Benchmarking

There are three comparable HTTP benchmark servers:

- `benchmark/nethttp`: Go standard library `net/http`
- `benchmark/uwsgo`: this Go binding
- `benchmark/node-uwebsockets`: uWebSockets.js on Node

Start each server in a separate terminal:

```sh
go run ./benchmark/nethttp
CGO_ENABLED=1 go run -tags uwebsockets ./benchmark/uwsgo
cd benchmark/node-uwebsockets && npm install && npm start
```

Then run:

```sh
npm install --prefix .tools/npm autocannon
sh scripts/bench_http.sh
```

Use the same machine, same CPU governor/power mode, same payloads, and repeat
the run several times. Watch both throughput and latency percentiles. For this
binding specifically, also compare `/plain` with `/hello/:name`: route
parameters cross the C++/Go boundary and reveal cgo overhead better than a pure
constant response.

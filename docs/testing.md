# Testing gogo Applications

Use `NewTestServerT` for most route tests. It starts a real loopback listener,
waits until the socket accepts connections, and registers cleanup with
`testing.TB`. Use `NewTestServer` when the test needs to inspect setup errors
instead of failing immediately.

The test server exercises the same network, native, middleware, async, body,
and WebSocket paths as a normal gogo app. It is not an in-memory
`httptest.ResponseRecorder` dispatcher.

## Basic Pattern

Register routes, middleware, and WebSocket behavior inside the setup callback.
Then drive the app with `ts.Get`, `ts.Post`, `ts.Do`, `ts.Client`, or any client
that can connect to `ts.URL()`.

```go
func TestPing(t *testing.T) {
    ts := gogo.NewTestServerT(t, func(app *gogo.App) {
        app.Get("/ping", func(res *gogo.Response, req *gogo.Request) {
            res.Send(200, "text/plain", "pong")
        })
    })

    resp, err := ts.Get("/ping")
    if err != nil {
        t.Fatal(err)
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        t.Fatal(err)
    }
    if resp.StatusCode != 200 || string(body) != "pong" {
        t.Fatalf("GET /ping = %d %q, want 200 pong", resp.StatusCode, body)
    }
}
```

Use `ts.Do` when the test needs request headers, custom methods, or a request
made with `httptest.NewRequest`:

```go
req := httptest.NewRequest("GET", "/users/42?expand=true", nil)
req.Header.Set("Authorization", "Bearer test-token")

resp, err := ts.Do(req)
```

`ts.Do` rewrites the request URL to the loopback server while preserving the
method, path, query, headers, and body.

## Sync Routes

Sync route tests can assert directly on the HTTP response. Keep blocking work
out of sync handlers, including test-only channel waits, because sync handlers
run on the event-loop thread.

```go
ts := gogo.NewTestServerT(t, func(app *gogo.App) {
    app.Get("/users/:id", func(res *gogo.Response, req *gogo.Request) {
        res.JSON(200, map[string]string{"id": req.Param("id")})
    })
})

resp, err := ts.Get("/users/7")
```

Static `Get` targets, typed route parameters, route groups, and path-scoped
middleware can all be tested through the same server.

## Async Routes

`GetAsync` handlers run on worker goroutines and receive a snapshot request.
Assert through the HTTP response, and use bounded channels only when the test
needs to observe internal ordering.

```go
done := make(chan struct{})

ts := gogo.NewTestServerT(t, func(app *gogo.App) {
    app.GetAsync("/work/:id", func(res *gogo.Response, req *gogo.Request) {
        defer close(done)
        res.Send(200, "text/plain", "worked:"+req.Param("id"))
    })
})

resp, err := ts.Get("/work/42")
if err != nil {
    t.Fatal(err)
}
defer resp.Body.Close()

select {
case <-done:
case <-time.After(time.Second):
    t.Fatal("async handler did not run")
}
```

For async routes that read `req.IP()` from the request snapshot, configure
`CapturePeerIP` or `TrustedProxies` in the app under test.

## Body Routes

Prefer `PostAsync`, `PutAsync`, `PatchAsync`, and `DeleteAsync` for handlers
that need the full request body. The framework collects up to `maxBodyBytes`
and sends 413 automatically when the body is too large.

```go
ts := gogo.NewTestServerT(t, func(app *gogo.App) {
    app.PostAsync("/echo", 64*1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
        res.Send(200, "application/json", string(body))
    })
})

resp, err := ts.Post("/echo", "application/json", strings.NewReader(`{"ok":true}`))
```

Use `Response.Body` only when the test is covering the low-level streaming body
primitive itself:

```go
app.Post("/sum", func(res *gogo.Response, req *gogo.Request) {
    res.Body(16*1024, func(body []byte, err error) {
        if err != nil {
            res.Send(413, "text/plain", err.Error())
            return
        }
        res.Send(200, "text/plain", strconv.Itoa(len(body)))
    })
})
```

The `Response.Body` callback runs on the loop thread, so move blocking work to
a goroutine before doing slow I/O.

## Middleware

Register middleware before the routes that should use it. Middleware composes
at registration time for global middleware and through route/group metadata for
scoped middleware, so tests should build the same order the app uses in
production.

```go
ts := gogo.NewTestServerT(t, func(app *gogo.App) {
    app.Use(func(next gogo.Handler) gogo.Handler {
        return func(res *gogo.Response, req *gogo.Request) {
            res.Header("X-Test-Middleware", "seen")
            next(res, req)
        }
    })

    app.Get("/protected", func(res *gogo.Response, req *gogo.Request) {
        res.Send(200, "text/plain", "ok")
    })
})

resp, err := ts.Get("/protected")
if err != nil {
    t.Fatal(err)
}
defer resp.Body.Close()

if got := resp.Header.Get("X-Test-Middleware"); got != "seen" {
    t.Fatalf("middleware header = %q, want seen", got)
}
```

For middleware that rejects a request, assert the status, response body, and
security-relevant headers. For async middleware on body routes, `req.Body()`
contains the collected body in the async chain.

## WebSocket Tests

v0.6 does not expose a public WebSocket test client. `NewTestServer` already
runs a real loopback listener, so application tests can connect with any RFC
6455 client they already trust. Keeping the client out of gogo's public API
avoids freezing a handshake and frame helper surface that is not central to the
server contract before v1.

Build the WebSocket URL from the test server URL:

```go
wsURL := "ws://" + strings.TrimPrefix(ts.URL(), "http://") + "/ws"
```

Then connect with the RFC 6455 client used by your app tests and assert the
server behavior:

- Accepted upgrades return HTTP 101 and, when negotiated, the expected
  `Sec-WebSocket-Protocol`.
- Rejected `Upgrade` callbacks return the expected HTTP status and body.
- `Open`, `Message`, `Close`, `Send`, `Publish`, `Subscribe`, and
  `Unsubscribe` behavior is visible through text or binary frames.
- Browser-facing endpoints should include `Origin` coverage, because HTTP CORS
  middleware does not protect the WebSocket upgrade path.

For upgrade-only tests, a raw `net.Conn` plus `http.ReadResponse` can be enough.
For frame round trips, use a real WebSocket client rather than hand-parsing
frames in every application test.

## Graceful Shutdown

For ordinary route tests, let `NewTestServerT` call `Close` from `t.Cleanup`.
For shutdown semantics, start an app manually so the test controls `Run`,
in-flight requests, and the shutdown call being exercised.

```go
app, err := gogo.NewApp()
if err != nil {
    t.Fatal(err)
}
app.GetAsync("/slow", func(res *gogo.Response, req *gogo.Request) {
    // Coordinate with the test, then finish the response.
    res.Send(200, "text/plain", "done")
})

port := pickFreePort(t) // test helper that chooses a 127.0.0.1 port
if !app.Listen(port) {
    t.Fatalf("Listen :%d failed", port)
}

runDone := make(chan struct{})
go func() {
    app.Run()
    close(runDone)
}()

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := app.ShutdownContext(ctx); err != nil {
    t.Fatalf("ShutdownContext: %v", err)
}
<-runDone
app.Close()
```

Use `ShutdownContext` when the test must prove in-flight requests drain.
Use `ShutdownGracefully` when the test wants to initiate a graceful drain and
observe completion elsewhere. Use `Shutdown` when the expected behavior is an
immediate stop.

## TestServer Serialization

`NewTestServer` serializes public test-server lifetimes in native builds. The
native binding has process-wide shared worker and ring state, and serialization
keeps package tests that call `t.Parallel` from running multiple native apps in
the same process at once.

This means a test may still call `t.Parallel`, but server construction and
server lifetime are intentionally one-at-a-time. Keep `TestServer` tests short,
close response bodies promptly, and avoid leaving long-lived streaming or
WebSocket clients open after the assertion completes.

## App Route-Registration Contract

Register routes, middleware, groups, and WebSocket behavior in the setup
callback before `NewTestServer` starts listening. After `NewTestServer` returns,
use `ts.App()` only for APIs that are designed to run against an already-running
app, such as publishing to WebSocket topics from a worker goroutine.

Do not rely on registering new routes or middleware through `ts.App()` after the
server has started. That late-registration behavior is not the testing contract
and may be tightened before v1.

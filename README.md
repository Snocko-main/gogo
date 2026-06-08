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

## Benchmark Snapshot

[![gogo single-worker HTTP benchmark throughput](benchmark/results/http-benchmark-throughput.svg)](#benchmarking)

gogo leads every single-worker route in the local HTTP benchmark matrix,
including static GETs, parameterized routes, SQLite reads, body echo, and
body-parse + SQLite query paths.

[Jump to benchmark details](#benchmarking)

## Table of Contents

- [Benchmark Snapshot](#benchmark-snapshot)
- [Requirements and Native Build](#requirements-and-native-build)
- [Version Policy](#version-policy)
- [License](#license)
- [Hello World](#hello-world)
- [Routing](#routing)
  - [Basic routes](#basic-routes)
  - [Route pattern syntax and precedence](#route-pattern-syntax-and-precedence)
  - [Route parameters](#route-parameters)
  - [Typed parameters](#typed-parameters)
  - [Query strings](#query-strings)
  - [Named routes & reverse routing](#named-routes--reverse-routing)
  - [Route groups](#route-groups)
- [Handler Styles](#handler-styles)
- [Responses](#responses)
  - [JSON, redirect, file](#json-redirect-file)
  - [Streaming](#streaming)
  - [Templates](#templates)
- [Request Body Parsing](#request-body-parsing)
- [Cookies](#cookies)
- [Middleware](#middleware)
  - [Sync middleware](#sync-middleware)
  - [Async middleware](#async-middleware)
  - [Post-handler cleanup with `Response.OnFinish`](#post-handler-cleanup-with-responseonfinish)
  - [Bundled middleware](#bundled-middleware)
  - [RequestID — 128-bit IDs](#requestid--128-bit-ids)
  - [RateLimit memory cap](#ratelimit-memory-cap)
  - [Sessions](#sessions)
- [Database Connection](#database-connection)
- [Server-Sent Events](#server-sent-events-sse)
- [WebSocket](#websocket)
  - [Echo](#echo-server)
  - [Pub/Sub](#pubsub)
  - [Upgrade-time auth](#upgrade-time-auth-and-subprotocols)
- [File Uploads](#file-uploads)
- [Multi-core](#multi-core)
- [Graceful Shutdown](#graceful-shutdown)
- [Configuration](#configuration)
  - [Global state audit](#global-state-audit)
  - [Test server helpers](#test-server-helpers)
  - [TrustProxy and client IPs](#trustproxy-and-client-ips)
  - [CapturePeerIP and async routes](#capturepeerip-and-async-routes)
  - [net/http adapter body cap](#nethttp-adapter-body-cap)
  - [Redirect and open redirects](#redirect-and-open-redirects)
- [Error Handling & Panic Recovery](#error-handling--panic-recovery)
- [Caveats](#caveats)
- [Benchmarking](#benchmarking)

## Requirements and Native Build

The native binding vendors the required uWebSockets/uSockets source inside
this module:

```txt
internal/native/uwebsockets
```

Consumers do not need a `third_party` checkout or a prebuilt `uSockets.a`;
`go get github.com/Snocko-main/gogo` fetches the native source that cgo
compiles with the package.

To run a real gogo server, the machine building your app needs:

- Go 1.24 or newer
- cgo enabled (`CGO_ENABLED=1`)
- a C compiler and C++20-capable compiler (`clang` or `gcc`/`g++`)
- zlib headers/library from the host system

Install those native build dependencies:

```sh
# macOS: install Apple Command Line Tools
xcode-select --install

# Debian / Ubuntu
sudo apt-get update
sudo apt-get install -y build-essential zlib1g-dev

# Fedora
sudo dnf install -y gcc gcc-c++ zlib-devel

# Alpine
sudo apk add build-base zlib-dev
```

Add gogo to your app:

```sh
go get github.com/Snocko-main/gogo@latest
```

Build, run, or test with the `gogo` build tag:

```sh
CGO_ENABLED=1 go build -tags gogo ./...
CGO_ENABLED=1 go run -tags gogo .
CGO_ENABLED=1 go test -tags gogo ./...
```

For a production binary:

```sh
CGO_ENABLED=1 go build -tags gogo -o my-server .
```

Without `-tags gogo`, the package builds a stub and `NewApp` returns a clear
setup error. This keeps normal Go tooling usable, but it will not run a native
uWebSockets server. The native build is currently intended for macOS and Linux.

The maintainer-only `scripts/bootstrap_uwebsockets.sh` script refreshes the
vendored source from the pinned uWebSockets commit and reapplies
`patches/uSockets-kqueue-ready-polls.patch`. Override `UWEBSOCKETS_REF` only
when deliberately testing an upstream update.

Then run an example:

```sh
CGO_ENABLED=1 go run -tags gogo ./examples/hello
curl http://localhost:3000/hello/inon
```

## Version Policy

gogo is currently on the `v0.x` public preview line.

- `v0.x`: APIs may change, including breaking changes, when the change moves
  the project closer to a stable `v1`. Release notes should call out breaking
  changes and migration steps.
- `v0.9.x`: planned release-candidate period. Breaking changes need a specific
  v1-readiness reason.
- `v1.0.0`: routing, middleware, WebSocket, testing, and configuration APIs
  are expected to be stable except for backward-compatible additions.

Use pinned tags for applications that need repeatable builds:

```sh
go get github.com/Snocko-main/gogo@v0.1.0
```

Release maintainers should follow [`docs/release-checklist.md`](docs/release-checklist.md)
and [`docs/branch-protection.md`](docs/branch-protection.md) before cutting
public tags.

## License

gogo is licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).

Vendored uWebSockets/uSockets native sources retain their upstream Apache-2.0
license notices. See [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

## Hello World

The smallest possible gogo~ server:

```go
package main

import (
    "log"
    gogo "github.com/Snocko-main/gogo"
)

func main() {
    app, err := gogo.NewApp()
    if err != nil {
        log.Fatal(err)
    }
    defer app.Close()

    app.Get("/", func(res *gogo.Response, req *gogo.Request) {
        res.Send(200, "text/plain", "hello, world\n")
    })

    if !app.Listen(3000) {
        log.Fatal("listen :3000 failed")
    }
    log.Println("listening on http://localhost:3000")
    app.Run()
}
```

Run it:

```sh
CGO_ENABLED=1 go run -tags gogo ./yourapp
curl http://localhost:3000/
```

### Static replies (zero cgo per request)

If the response never changes, register a `gogo.Reply`. When no matching
sync middleware is installed and no typed-parameter constraint needs checking,
it is served entirely from C++ with no cgo callback per request. If middleware
such as auth, CORS, logging, or rate limiting matches the route, or the pattern
uses a typed parameter like `:id<int>`, gogo automatically falls back to the
dynamic path so the middleware/constraint still runs:

```go
app.Get("/health", gogo.Reply{
    Status:      200,
    ContentType: "application/json",
    Body:        `{"ok":true}`,
})
```

## Routing

### Basic routes

Sync route handlers run on the uWS loop thread and should stay fast:

```go
app.Get("/users", listUsers)
app.Post("/users", createUser)
app.Put("/users/:id", updateUser)
app.Patch("/users/:id", patchUser)
app.Delete("/users/:id", deleteUser)
app.Options("/users", optionsUsers)
app.Head("/users", headUsers)
app.Any("/echo", anyMethod)
```

Async route handlers run on a goroutine and receive a request snapshot:

```go
app.GetAsync("/users/:id", showUserFromDB)
app.PostAsync("/uploads", 10<<20, uploadFile) // max body bytes, then handler
app.PutAsync("/users/:id", 1<<20, replaceUser)
app.PatchAsync("/users/:id", 1<<20, patchUser)
app.DeleteAsync("/users/:id", 64<<10, deleteUserWithBody)
```

The same route registration APIs are available on a `*gogo.Router` returned
by `Group` or `Mount`, so scoped routes can use sync and async handlers:

```go
api := app.Group("/api")
api.Get("/health", health)
api.GetAsync("/users/:id", showUserFromDB)
api.Post("/users", createUser)
api.PostAsync("/uploads", 10<<20, uploadFile)
api.PatchAsync("/users/:id", 1<<20, patchUser)
```

Route API surface:

| API | `App` | `Router` | handler / target |
|---|---:|---:|---|
| `Get(pattern, target)` | yes | yes | `Handler`, `func(*Response, *Request)`, `Reply`, `string`, or `[]byte` |
| `GetAsync(pattern, handler)` | yes | yes | `AsyncHandler` |
| `Post(pattern, handler)` | yes | yes | `Handler` |
| `PostAsync(pattern, maxBodyBytes, handler)` | yes | yes | `BodyAsyncHandler` / `PostAsyncHandler` with collected body |
| `Put(pattern, handler)` | yes | yes | `Handler` |
| `PutAsync(pattern, maxBodyBytes, handler)` | yes | yes | `BodyAsyncHandler` with collected body |
| `Patch(pattern, handler)` | yes | yes | `Handler` |
| `PatchAsync(pattern, maxBodyBytes, handler)` | yes | yes | `BodyAsyncHandler` with collected body |
| `Delete(pattern, handler)` | yes | yes | `Handler` |
| `DeleteAsync(pattern, maxBodyBytes, handler)` | yes | yes | `BodyAsyncHandler` with collected body |
| `Options(pattern, handler)` | yes | yes | `Handler` |
| `Head(pattern, handler)` | yes | yes | `Handler` |
| `Any(pattern, handler)` | yes | yes | `Handler` for every HTTP method |
| `WebSocket(pattern, behavior)` | yes | yes | `WebSocketBehavior` |
| `Group(prefix, ...middleware)` | yes | yes | returns a scoped `*Router` |
| `Use(...middleware)` | yes | yes | sync middleware; `App.Use` also supports a path prefix |
| `UseAsync(...middleware)` | yes | yes | async middleware for `GetAsync` and body-async routes; `App.UseAsync` also supports a path prefix |
| `Mount(prefix, func(*Router))` | yes | no | callback sugar over `Group` |
| `Name(name, pattern)` | yes | yes | names a route pattern for reverse routing |
| `URL(name, params)` | yes | no | builds a URL for a named route |
| `NotFound(handler)` | yes | no | fallback for unmatched routes |
| `MethodNotAllowed(handler)` | yes | no | fallback for known path with unsupported method |

### Route pattern syntax and precedence

Route patterns are path patterns, not full URLs. Query strings are not part
of matching; read them with `req.QueryParam`, `req.QueryInt`, or
`req.QueryBool`. A pattern must be non-empty, start with `/`, and contain no
NUL, CR, or LF bytes. Invalid patterns panic during registration. Matching is
case-sensitive. Trailing slashes are significant for route patterns: `/users`
and `/users/` are different routes unless you register both.

Supported route segments:

| Segment | Meaning |
|---|---|
| `users` | literal path segment; matches only `users` |
| `:id` | one non-empty path segment; available with `req.Parameter(i)` or `req.Param("id")` |
| `:id<int>` | named segment plus a gogo type constraint checked before middleware and the handler |
| trailing wildcard segment | catch-all wildcard, for example `/files/*`, `/files/**`, or `/*` |

Parameter names are metadata for request helpers and reverse routing; they do
not make two otherwise identical native route shapes distinct. Avoid
registering ambiguous patterns such as `/users/:id` and `/users/:name` for the
same method.

For a request on a concrete HTTP method, the native router prefers literal
segments over named parameters, and named parameters over wildcard catch-alls.
Method-specific routes are tried before `Any` routes. gogo's own catch-all for
`NotFound`, `MethodNotAllowed`, and middleware on unmatched paths is installed
just before `Listen`, after user routes, so explicit routes keep precedence.

`MethodNotAllowed` distinguishes wrong-method requests for literal,
named-parameter, typed-parameter, and terminal wildcard routes. Typed
constraints must match before a route contributes to the `Allow` header; a
typed mismatch is still a path miss and falls through to `NotFound`.

### Route parameters

Read positional parameters with `req.Parameter(i)` or by name with
`req.Param(name)`.

```go
app.Get("/users/:id", func(res *gogo.Response, req *gogo.Request) {
    id := req.Param("id")              // by name
    // or: id := req.Parameter(0)      // by index
    res.Send(200, "text/plain", "user="+id)
})
```

Integer convenience helpers skip strconv boilerplate:

```go
app.Get("/posts/:id", func(res *gogo.Response, req *gogo.Request) {
    id := req.ParamInt("id", 0)        // default 0 on parse failure
    res.JSON(200, map[string]int{"id": id})
})
```

Use `req.ParamInt64(name, def)` or `req.ParameterInt64(i, def)` for larger
integer identifiers.

Wildcards are supported via uWS pattern syntax:

```go
app.Get("/files/*", serveFile)         // matches /files/anything/here
app.Any("/*", serveSPA)                // catch-all fallback
```

Wildcard text is not exposed as a route parameter. Use `req.URL()` if the
handler needs to inspect the matched suffix.

### Typed parameters

Add a `<type>` annotation to a named parameter — the framework refuses to
call the handler if the value doesn't match the constraint and returns 404
instead:

```go
app.Get("/users/:id<int>", showUser)          // /users/abc → 404
app.Get("/posts/:slug<slug>", showPost)        // /posts/My_Post → 404
app.Get("/blobs/:digest<uuid>", showBlob)
```

Built-in constraints: `int`, `uint`, `uuid`, `alpha`, `alnum`, `slug`.
Register custom ones:

```go
gogo.RegisterParamType("hex", func(s string) bool {
    for i := 0; i < len(s); i++ {
        c := s[i]
        if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
            return false
        }
    }
    return len(s) > 0
})

app.Get("/blobs/:digest<hex>", handler)
```

Malformed annotations, empty parameter names, and unknown type names panic at
registration. Constraint checks run before middleware and before static replies
are sent, so a typed route cannot bypass auth or serve a cached body for an
invalid value.

### Query strings

```go
// /search?q=gogo&page=2&strict=true
app.Get("/search", func(res *gogo.Response, req *gogo.Request) {
    q := req.QueryParam("q")
    page := req.QueryInt("page", 1)
    strict := req.QueryBool("strict", false)
    _ = q; _ = page; _ = strict
    res.Send(200, "text/plain", "ok")
})
```

### Named routes & reverse routing

```go
app.Name("user.show", "/users/:id")
url, _ := app.URL("user.show", map[string]string{"id": "42"})
// url == "/users/42"

api := app.Group("/api/v1")
api.Get("/users/:id", showUser)
api.Name("api.user.show", "/users/:id")
url, _ = app.URL("api.user.show", map[string]string{"id": "42"})
// url == "/api/v1/users/42"
```

Named routes may use typed annotations; `URL` strips the annotation before
substitution. Parameter values are path-escaped. Missing params, unknown route
names, and wildcard patterns return errors.

Route registration methods intentionally do not return fluent route handles.
Use `Name(name, pattern)` explicitly when a route needs reverse routing.

### Route groups

`App.Group` and `Router.Group` bind middleware and a path prefix to a
subtree of routes. Group-bound middleware composes at registration time, so
there is no per-request URL check. A group prefix must start with `/`, may
contain named or typed parameters, must not contain wildcards, and has trailing
slashes stripped. `Group("/")` is equivalent to no prefix. Child route patterns
must also start with `/`; gogo concatenates the group prefix and child pattern
literally, then parses them as one route.

```go
api := app.Group("/api/v1", authMW, loggerMW)
api.Get("/users", listUsers)               // → GET /api/v1/users
api.Post("/users", createUser)

admin := api.Group("/admin", adminMW)      // composes auth + logger + admin
admin.Delete("/users/:id", deleteUser)     // → DELETE /api/v1/admin/users/:id
```

`Router.Use` and `Router.UseAsync` append middleware to that router for routes
registered afterward. Child groups created after the call inherit the new
middleware. Execution is outermost-first: app middleware, parent group
middleware, child group middleware, then the handler. Typed-parameter
rejection happens before all middleware.

`App.Mount` is a tiny convenience for building a Router with a callback:

```go
app.Mount("/api/v1", func(r *gogo.Router) {
    r.Use(authMW)
    r.Get("/users", listUsers)
    r.Post("/users", createUser)
})
```

`Mount` returns the `*Router`, so callers can keep registering on it after the
callback. Calling `Mount` twice with the same prefix creates independent
routers; there is no automatic merge.

Path-scoped app middleware uses the same segment-aware prefix rules without
creating a router:

```go
app.Use("/api/*", authMW)        // /api and /api/...; not /apiv2
app.Use("/api/**", auditMW)      // same scope as /api/*
app.Use("/", requestIDMW)        // global
```

Scoped `App.Use` still follows registration order: only later routes see the
middleware. The prefix is matched against the live request URL at request time,
not the registered route pattern, so dynamic and wildcard routes cannot bypass
a scoped guard. Prefer `Group` for new subtrees when possible; it gives the
same scope by router identity with registration-time composition.

## Handler Styles

gogo offers three handler styles, picking the lowest-overhead dispatch
path that still satisfies the constraints of the route:

```go
// 1) Sync handler — runs on the uWS loop thread. MUST NOT block.
//    Use for fast in-memory replies, simple JSON, static lookups.
app.Get("/plain", func(res *gogo.Response, req *gogo.Request) {
    res.Send(200, "text/plain", "ok")
})

// 2) GetAsync — runs on a goroutine, free to block (DB, HTTP, sleep).
//    Without middleware, dispatched through a shared-memory ring with
//    ZERO cgo crossings per request.
app.GetAsync("/db", func(res *gogo.Response, req *gogo.Request) {
    var name string
    _ = db.QueryRow("SELECT name FROM users WHERE id=$1", 1).Scan(&name)
    res.Send(200, "text/plain", "hi "+name)
})

// 3) Body async — async handler with the body fully collected up to maxBodyBytes.
//    Oversize bodies → 413 automatically.
app.PostAsync("/upload", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
    res.JSON(200, map[string]int{"size": len(body)})
})
```

`Async` family handlers receive a *snapshot* `*Request` that survives past
the C-side request lifetime. Sync handlers receive a live request that is
only valid during the callback.

## Responses

### JSON, redirect, file

```go
app.Get("/json", func(res *gogo.Response, req *gogo.Request) {
    res.JSON(200, map[string]any{"ok": true, "n": 42})
})

// Optional at App construction time:
// app, _ := gogo.NewApp(gogo.Config{
//     JSONEncoder: sonic.Marshal,
//     JSONDecoder: sonic.Unmarshal,
// })

app.Get("/old", func(res *gogo.Response, req *gogo.Request) {
    res.Redirect("/new", 301)
})

// SendFile streams a file from disk with ETag, Last-Modified, and Range
// negotiation. Download forces an attachment Content-Disposition.
app.Get("/files/:name", func(res *gogo.Response, req *gogo.Request) {
    path, err := safePublicPath("./public", req.Param("name"))
    if err != nil {
        res.Send(404, "text/plain; charset=utf-8", "not found\n")
        return
    }
    _ = res.SendFile(req, path)
})
app.Get("/download/:name", func(res *gogo.Response, req *gogo.Request) {
    path, err := safePublicPath("./public", req.Param("name"))
    if err != nil {
        res.Send(404, "text/plain; charset=utf-8", "not found\n")
        return
    }
    _ = res.Download(req, path, req.Param("name"))
})

// JSONP — same as JSON but wrapped in a callback for legacy clients.
app.Get("/jsonp", func(res *gogo.Response, req *gogo.Request) {
    res.JSONP(req.QueryParam("callback"), map[string]any{"ok": true})
})

// Manual status + headers chained.
app.Get("/custom", func(res *gogo.Response, req *gogo.Request) {
    res.Status(202).
        Header("X-Custom", "yes").
        Send(202, "text/plain", "queued\n")
})
```

When the filename comes from the URL, do not concatenate it directly onto a
directory. Clean it first and verify the result still lives under the intended
root:

```go
func safePublicPath(root, name string) (string, error) {
    cleanRoot, err := filepath.Abs(root)
    if err != nil {
        return "", err
    }
    cleanPath, err := filepath.Abs(filepath.Join(cleanRoot, filepath.Clean("/"+name)))
    if err != nil {
        return "", err
    }
    rel, err := filepath.Rel(cleanRoot, cleanPath)
    if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
        return "", os.ErrPermission
    }
    return cleanPath, nil
}
```

`SendFile` intentionally opens the path you pass it; path allow-listing belongs
in the route because different apps expose different roots. Large-file serving
is governed by atomic knobs: `SetMaxSendFileBytes`, `SetSendFileChunkBytes`,
and `SetSendFileBackpressureBytes`. The legacy package variables still work
for startup-time configuration, but prefer the setters if the server may be
serving requests. Use `gogo.NoSendFileLimit` only for trusted file-serving
routes where path allow-listing, authorization, or an external layer already
bounds what may be served.

### Streaming

`Response.Stream` writes a chunked-encoded response. Available from async
handlers only.

```go
app.GetAsync("/stream", func(res *gogo.Response, req *gogo.Request) {
    err := res.Stream(200, "text/plain", func(w io.Writer) error {
        for i := 0; i < 5; i++ {
            if _, err := fmt.Fprintf(w, "chunk %d\n", i); err != nil {
                return err
            }
            time.Sleep(200 * time.Millisecond)
        }
        return nil
    })
    if err != nil && !errors.Is(err, gogo.ErrStreamAborted) {
        log.Printf("stream error: %v", err)
    }
})
```

`Response.AwaitDrain` and stream writes return `gogo.ErrStreamAborted` when a
client disconnects while the producer is waiting for backpressure to drain.

### Templates

Install an `html/template`-backed engine via `SetTemplateEngine`:

```go
app.SetTemplateEngine(gogo.NewHTMLTemplateEngine(gogo.HTMLTemplateOptions{
    Root:   "./views",     // directory containing template files
    Suffix: ".tmpl",        // file extension to register (default ".tmpl")
    Reload: false,          // re-parse on every Render during dev
}))

app.Get("/", func(res *gogo.Response, req *gogo.Request) {
    res.Render("index", map[string]any{
        "Title": "gogo~",
        "User":  "alice",
    })
})
```

Templates are named by their path relative to `Root` with the suffix stripped
— `views/user/profile.tmpl` is rendered as `user/profile`. Render output is
capped by `gogo.GetMaxRenderBytes()` (default 8 MiB; set to
`gogo.SetMaxRenderBytes(gogo.NoRenderLimit)` to disable) before it is sent, so
oversized templates fail with a generic 500 instead of staging unbounded memory.
Bring your own engine by implementing `TemplateEngine`:

```go
type TemplateEngine interface {
    Render(w *bytes.Buffer, name string, data any) error
}
```

Custom engines that can stop early may also implement `LimitedTemplateEngine`;
the built-in HTML engine does this so the cap is enforced while rendering.

## Request Body Parsing

`Request.BodyParser` deserializes the body into a struct based on
Content-Type. Supported: `application/json`, `application/x-www-form-urlencoded`,
`multipart/form-data` (value parts only). JSON uses `Config.JSONDecoder` when
configured; `Response.JSON` and `Response.JSONP` use `Config.JSONEncoder`.
Use it from body-async handlers such as `PostAsync`, `PutAsync`, `PatchAsync`,
and `DeleteAsync`.

```go
type CreateUser struct {
    Name  string `json:"name"  form:"name"`
    Email string `json:"email" form:"email"`
}

app.PostAsync("/users", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
    var in CreateUser
    if err := req.BodyParser(&in); err != nil {
        if errors.Is(err, gogo.ErrUnsupportedMediaType) {
            res.Send(415, "text/plain", "unsupported media type\n")
            return
        }
        res.Send(400, "text/plain", "bad body\n")
        return
    }
    res.JSON(201, in)
})
```

Sync handlers that collect a body manually with `Response.Body` should call
`gogo.ParseBody(contentType, body, &out)` inside the callback instead of
`Request.BodyParser`.

If the client disconnects while `Response.Body` is still collecting, the body
callback is not invoked. Use `res.OnAborted()` or `req.Context()` for abort
cleanup and cancellation.

Multipart value parts parsed by `BodyParser` and `ParseMultipart` are capped by
`GetDefaultMultipartPartLimit()` (8 MiB by default). Override the process default
with `SetDefaultMultipartPartLimit(n)` before registering handlers, or pass
`MultipartOptions{MaxPartBytes: n}` to multipart APIs for route-specific limits.
Use `gogo.NoMultipartPartLimit` only for trusted upload flows where
`Config.BodyLimit` or an external proxy still bounds total request size. The
older `DefaultMultipartPartLimit = n` assignment style still works during
startup, but the setter is preferred for runtime-safe updates.

## Cookies

```go
// Read
sid := req.Cookie("session")

// Write
res.SetCookie(gogo.Cookie{
    Name:     "session",
    Value:    "abc123",
    HttpOnly: true,
    Secure:   true,
    SameSite: gogo.SameSiteLax,
    MaxAge:   3600,
    Path:     "/",
})

// Signed cookies — tamper-evident with HMAC-SHA256.
cookieSecret := os.Getenv("COOKIE_SECRET") // at least 32 bytes
res.SetCookieSigned(gogo.Cookie{Name: "uid", Value: "42"}, cookieSecret)
uid, ok := req.CookieSigned("uid", cookieSecret)
if !ok {
    res.Send(401, "text/plain", "bad cookie\n")
    return
}
_ = uid
```

Signed-cookie secrets shorter than 32 bytes panic when signing and never
verify when reading; use a secret manager or CSPRNG-generated value.

## Middleware

### Sync middleware

Sync middleware runs on the uWS loop thread and **must not block**. Use it
for cheap cross-cutting work — auth header check, logging, CORS preflight.

```go
authMW := func(next gogo.Handler) gogo.Handler {
    return func(res *gogo.Response, req *gogo.Request) {
        if req.Header("authorization") == "" {
            res.Send(401, "text/plain", "no token\n")
            return
        }
        next(res, req)
    }
}

app.Use(authMW)                                  // global
app.Use("/api/*", authMW, corsMW)                // scoped to /api/*
app.Get("/api/me", handleMe)
```

The first argument to `Use` may optionally be a path pattern that scopes
the middleware to request URLs under that prefix. Trailing `/*` or `/**`
is stripped, so `/api/*`, `/api/**`, and `/api` all mean exact `/api` plus
children under `/api/`; `/apiv2` does not match. `/`, `/*`, and `/**` mean
global. The match uses the live request URL at request time, so parametric
and wildcard routes cannot bypass a scoped guard.

Middleware runs left-to-right: the first middleware passed to `Use` is the
outermost wrapper and sees the request first. A middleware that rejects a
request should write the response and return without calling `next`.
Middleware that observes final status, persists state, or records audit data
should register `res.OnFinish` and then call `next`; see the cleanup section
below for the panic-safe form.

### Async middleware

`AsyncMiddleware` wraps `GetAsync` and body-async handlers and runs on the
same goroutine as the user handler, so it IS free to block (DB lookups,
remote calls). Typical use: resolve a user from a token, then pass it down
via `SetLocal`.

```go
app.Use("/api/*", func(next gogo.AsyncHandler) gogo.AsyncHandler {
    return func(res *gogo.Response, req *gogo.Request) {
        token := req.Header("authorization")
        user, err := db.LoadUserByToken(token)    // blocking — ok in async mw
        if err != nil {
            res.Send(401, "text/plain", "unauthorized\n")
            return
        }
        req.SetLocal("user", user)
        next(res, req)
    }
})

app.GetAsync("/api/me", func(res *gogo.Response, req *gogo.Request) {
    user := req.Local("user").(*User)
    res.JSON(200, user)
})
```

Bundled middleware declares its placement:

| Placement | Runs on | Use for |
| --- | --- | --- |
| Sync | uWS loop thread | cheap checks and headers: CORS preflight, BasicAuth, JWT verification, CSRF checks |
| Async | worker goroutine | blocking stores or network calls: Redis rate limits, database/session stores |
| Both | sync routes on the loop, async routes in the worker | cheap cross-cutting behavior that should apply everywhere: RequestID, Logger, Metrics, Helmet, Compress |

`middleware.Async(mw)` forces a sync-shaped middleware into the async chain
for `GetAsync` and body-async routes. Use it only when the middleware may
block; sync routes will not see async-only middleware.

### Post-handler cleanup with `Response.OnFinish`

Middleware that runs **after** the handler — saving session state,
flushing metrics, closing a tracing span, recording an audit line —
faces a subtle lifecycle bug if you write it as the naive `next; cleanup`
pattern:

```go
// ⚠️ Footgun — cleanup may run BEFORE the handler's real work
app.Use(func(next gogo.Handler) gogo.Handler {
    return func(res *gogo.Response, req *gogo.Request) {
        state := beginRequest(req)
        next(res, req)
        commit(state)                  // ← runs when next() returns
    }
})
```

The problem appears when the handler calls `res.Async(fn)`:

```go
app.Get("/job", func(res *gogo.Response, req *gogo.Request) {
    res.Async(func() {
        time.Sleep(50 * time.Millisecond)
        mutate(state)                  // happens AFTER commit(state) above
    })
})
```

`res.Async` spawns a goroutine and returns immediately. From the
middleware's point of view, `next` has finished — but the user's real
work hasn't started yet. `commit(state)` saves stale state.

`Response.OnFinish(fn func())` is the safe hook for this pattern. It
picks the right moment automatically:

- **Sync handler** (no `res.Async` upgrade): `fn` runs inline when
  registered — equivalent to the original `next; cleanup` ordering.
- **Async handler** (`GetAsync` / `PostAsync`, or sync handler that
  upgraded via `res.Async`): `fn` is queued and fires after the
  goroutine completes, via the framework's `finishAsync` cleanup.
- **Late registration** (rare — goroutine finished before middleware
  reached `OnFinish`): `fn` runs inline so it's never orphaned.

Rewrite the custom middleware:

```go
// ✅ Correct — cleanup runs after the handler (incl. any res.Async) finishes
app.Use(func(next gogo.Handler) gogo.Handler {
    return func(res *gogo.Response, req *gogo.Request) {
        state := beginRequest(req)
        next(res, req)
        res.OnFinish(func() { commit(state) })
    }
})
```

Real-world example — an audit middleware that records the final
response status and the user ID set by an auth middleware deeper in
the chain:

```go
app.Use(func(next gogo.Handler) gogo.Handler {
    return func(res *gogo.Response, req *gogo.Request) {
        start := time.Now()
        next(res, req)
        res.OnFinish(func() {
            user, _ := req.Local("user").(*User)
            log.Printf("audit method=%s path=%s status=%d user=%v dur=%s",
                req.Method(), req.URL(), res.StatusCode(), user, time.Since(start))
        })
    }
})
```

The status / user ID / duration now reflect the post-handler state
regardless of whether the handler upgraded to async.

**Multiple registrations** fire FIFO — middleware higher in the chain
registers first and runs first. Panics inside an `OnFinish` callback
are caught by the framework's panic handler and don't prevent later
callbacks from firing, mirroring `http.ResponseWriter` recovery
semantics.

**Recording the panic case** — if you want the cleanup to fire even
when the handler panics, register via `defer`:

```go
app.Use(func(next gogo.Handler) gogo.Handler {
    return func(res *gogo.Response, req *gogo.Request) {
        start := time.Now()
        defer res.OnFinish(func() {
            log.Printf("status=%d dur=%s", res.StatusCode(), time.Since(start))
        })
        next(res, req)
    }
})
```

A bare `next(res, req); res.OnFinish(...)` is skipped on panic because
the panic unwinds past the registration. `defer res.OnFinish(...)`
registers on the unwind, before the framework's outer panic handler
catches and emits the 500. Use the defer form for observability
middleware (metrics, audit, tracing) and for cleanup/state that must
commit even when a handler fails.

**Built-in middleware** using this hook: `mw.NewSession` (commits
session mutations and destroys even when a handler panics) and
`mw.NewMetrics` (records the final status / duration even on panic).
Custom middleware with the same requirements should use the defer form.

### Bundled middleware

The `middleware` subpackage ships production-ready middleware:

```go
import (
    gogo "github.com/Snocko-main/gogo"
    mw "github.com/Snocko-main/gogo/middleware"
)

app.Use(mw.RequestID())                              // X-Request-ID, 128-bit
app.Use(mw.Logger(mw.LoggerOptions{
    Format:    mw.JSONFormat,                        // structured logs
    SkipPaths: []string{"/healthz", "/metrics"},
}))
app.Use(mw.CORS(mw.CORSOptions{
    AllowOrigins:     []string{"https://app.example.com"},
    AllowCredentials: true,
    MaxAge:           86400,
}))
app.Use(mw.Helmet())                                  // common security headers
app.Use(mw.Compress())                                // gzip / deflate
app.Use(mw.RateLimit(mw.RateLimitOptions{
    Max:        100,
    Window:     time.Minute,                          // 100 req / IP / minute
    MaxBuckets: 100_000,                              // see "RateLimit memory cap" below
}))
app.Use("/admin/*", mw.BasicAuth(mw.BasicAuthOptions{
    Users:              map[string]string{"alice": "secret"},
    MaxCredentialBytes: 8 << 10, // default; mw.NoBasicAuthCredentialLimit disables the cap
}))

// JWT verification with HS256 (HMAC).
app.Use("/api/*", mw.JWT(mw.JWTOptions{
    Algorithm: mw.JWTHS256,
    Secret:    []byte(os.Getenv("JWT_SECRET")),
}))
app.GetAsync("/api/me", func(res *gogo.Response, req *gogo.Request) {
    claims := req.Local(mw.JWTLocalKey).(map[string]any)
    res.JSON(200, claims)
})

// CSRF — double-submit cookie pattern.
app.Use(mw.CSRF(mw.CSRFOptions{
    Secret:        []byte(os.Getenv("CSRF_SECRET")),
    MaxTokenBytes: 256, // default; mw.NoCSRFTokenLimit disables the cap
}))

// Prometheus-flavored metrics with /metrics handler.
metrics := mw.NewMetrics()
app.Use(metrics.Middleware())
app.Get("/metrics", metrics.Handler())
```

Use `AllowOrigins: []string{"*"}` only by itself for public APIs. gogo
panics at startup if `"*"` is mixed with explicit origins, or combined with
`AllowCredentials`, so ambiguous CORS policy fails before serving traffic.

HMAC-backed middleware secrets (`JWT` with HS*, `CSRF`, and `NewSession`)
must be at least 32 bytes. Generate them from a secret manager or a CSPRNG;
short demo strings panic at startup instead of silently weakening token
integrity.

### Middleware failure policy

Middleware failures fall into three buckets:

| Failure | Default behavior | Operator hook |
| --- | --- | --- |
| Invalid configuration at startup | panic before serving traffic | fix config; tests should assert construction panics |
| Request authentication or validation failure | fail closed with 401/403/429 or omit CORS allow headers | custom `OnLimit`, `SkipFunc`, WebSocket `Verify`, or explicit route logic |
| Backend/store failure after startup | middleware-specific; network stores should document fail-open/fail-closed behavior | `OnError` where provided, plus metrics/logging around the store |

Built-in auth middleware is fail-closed by default: `BasicAuth` and `JWT`
return 401, `CSRF` returns 403, `WebSocketAuth` rejects the upgrade, and
`RateLimit` returns 429 when a key exceeds its quota. `CORS` is different: it
does not authenticate a request, so disallowed origins simply do not receive
allow headers and browsers block the response.

Store-backed middleware must make outage behavior explicit. The bundled Redis
rate-limit adapter defaults to fail-open and reports failures through
`OnError`; set `FailClosed` when protecting scarce or expensive resources.
Session persistence errors are not sent to clients after the response has
started, so production `SessionStore` implementations should log or measure
their own save/delete failures and should be paired with `AsyncStore` when they
can block.

### RequestID — 128-bit IDs

`mw.RequestID()` emits **32-hex-char IDs (128 bits of entropy)** by default.
Drop-in compatible with most tracing systems, but worth checking before
upgrade if your downstream pipeline hard-codes ID length:

- ❌ `VARCHAR(16)` / `CHAR(16)` columns will silently truncate — widen to
  `VARCHAR(64)` or `TEXT`.
- ❌ Regex like `^[a-f0-9]{16}$` — drop the count or update to `{32}`.
- ✅ Treating the ID as an opaque string anywhere (logs, JSON, headers).

If you must keep 16-char IDs for an existing parser, plug a custom
generator:

```go
import (
    "crypto/rand"
    "encoding/hex"
)

app.Use(mw.RequestID(mw.RequestIDOptions{
    Generator: func() string {
        var buf [8]byte
        if _, err := rand.Read(buf[:]); err != nil {
            panic("request id entropy unavailable: " + err.Error())
        }
        return hex.EncodeToString(buf[:])   // 16 chars, 64-bit entropy
    },
}))
```

The 64-bit variant has measurable collision risk past ~10⁹ IDs (≈ 30 req/s
for a year). 128 bits keeps collision probability astronomically low — use
the default for new systems.

### RateLimit memory cap

`MemoryRateLimitStore` is capped at **100,000 buckets** by default to
protect against attacker-driven cardinality explosion. When the cap is
hit the store evicts expired buckets first, then drops the oldest-`resetAt`
bucket.

The cap binds tightly when your `KeyFunc` returns many distinct values
per window — user IDs, API keys, tokens, headers. For `KeyFunc = req.IP()`
behind a CDN it almost never binds.

The default `KeyFunc` is `req.IP()`, which is the immediate TCP peer. Behind
a trusted proxy or CDN that usually means the proxy address, not the end
client. To rate-limit by end-client IP, configure `TrustedProxies` and provide
a `KeyFunc` that chooses from `req.IPs()` with a fallback to `req.IP()`.

| `KeyFunc` returns        | Typical cardinality      | 100k enough? |
| ------------------------ | ------------------------ | ------------ |
| `req.IP()` behind a CDN  | 1 (the CDN's address)    | yes          |
| `req.IP()` public-facing | ~50k unique IPs/min      | yes          |
| `userID` (SaaS, 10k DAU) | ~10k                     | yes          |
| `apiKey` for a partner-heavy API | 500k+            | **raise it** |
| Header you don't control (User-Agent, etc.) | unbounded | the cap is the protection |

Raise it when you know cardinality is high:

```go
app.Use(mw.RateLimit(mw.RateLimitOptions{
    Max:        100,
    Window:     time.Minute,
    KeyFunc:    func(req *gogo.Request) string { return req.Local("userID").(string) },
    MaxBuckets: 2_000_000,                    // ~200 MiB worst case
}))
```

Example client-IP key behind trusted proxies:

```go
app.Use(mw.RateLimit(mw.RateLimitOptions{
    Max:    100,
    Window: time.Minute,
    KeyFunc: func(req *gogo.Request) string {
        if ips := req.IPs(); len(ips) > 0 {
            return ips[0]
        }
        return req.IP()
    },
}))
```

Set `MaxBuckets: mw.NoRateLimitBucketLimit` to disable the cap entirely
(tests only — re-introduces the OOM risk).

For multi-instance fleets, plug the bundled Redis-backed `RateLimitStore`
from `adapters/redis` instead — the cap is irrelevant when state lives in
Redis, and counters stay consistent across instances:

```go
import redisadapter "github.com/Snocko-main/gogo/adapters/redis"

store, err := redisadapter.NewRateLimitStore(redisadapter.RateLimitOptions{
    URL: "redis://localhost:6379/0",
    // FailClosed: true,   // reject when Redis is unreachable (default fails open)
})
if err != nil {
    log.Fatal(err)
}
defer store.Close()

app.Use(mw.RateLimit(mw.RateLimitOptions{
    Max:        100,
    Window:     time.Minute,
    Store:      store,
    AsyncStore: true, // run the Redis round-trip off the event-loop thread
}))
```

It uses an atomic `INCR` + `PEXPIRE` Lua script for the same fixed-window
algorithm as the in-memory store. To reuse an existing client, call
`redisadapter.NewRateLimitStoreClient(client, "gogo:rl:")`.

**Side effect when the cap binds**: eviction resets the rate-limit counter
for the evicted key. An attacker spamming new keys to fill the cap will
push legitimate users' buckets out faster than their window naturally
expires, effectively *weakening* the rate limit for those users. The cap
itself is a memory-safety bound — pair with `KeyFunc` choices that don't
let unauthenticated clients invent unlimited keys.

### Sessions

```go
app.Use(mw.NewSession(mw.SessionOptions{
    Secret:       []byte(os.Getenv("SESSION_SECRET")),
    Store:        mw.NewMemorySessionStore(),    // swap for Redis in prod
    TTL:          24 * time.Hour,
    CookieSecure: true,
    MaxEntries:   100_000,                       // default; cap memory
}))

app.GetAsync("/login", func(res *gogo.Response, req *gogo.Request) {
    s := req.Local(mw.SessionLocalKey).(*mw.Session)
    s.Set("user_id", 42)
    res.Send(200, "text/plain", "logged in\n")
})
app.GetAsync("/me", func(res *gogo.Response, req *gogo.Request) {
    s := req.Local(mw.SessionLocalKey).(*mw.Session)
    uid := s.Get("user_id")
    if uid == nil {
        res.Send(401, "text/plain", "no session\n")
        return
    }
    res.JSON(200, map[string]any{"user_id": uid})
})
```

`SESSION_SECRET` must be at least 32 bytes; rotate it intentionally because
rotation invalidates existing session cookies.

Sessions persist automatically at request completion via
`Response.OnFinish` — that means mutations made inside a
`res.Async(...)` goroutine are saved correctly (the persist call
fires after the goroutine finishes, not after `next` returns).
`sess.Destroy()` deletes the store row, expires the browser cookie when headers
are still writable, and stale signed cookies are rotated to a fresh session ID
before their next write instead of reusing the destroyed identifier.

For handlers that need an explicit mid-flight commit — checkpointing
before launching a background job, persisting auth state before an
SSE stream starts emitting events — call `sess.Save()`:

```go
app.Get("/checkpoint", func(res *gogo.Response, req *gogo.Request) {
    s := req.Local(mw.SessionLocalKey).(*mw.Session)
    s.Set("phase", "starting")
    s.Save()                          // commits to the store NOW

    res.Async(func() {
        // The background goroutine can rely on the row being
        // visible to other requests that arrive while it runs.
        runJob(s.ID)
        s.Set("phase", "done")
        res.Send(200, "text/plain", "ok\n")
    })
    // OnFinish still saves the "done" state when the goroutine
    // completes — no extra Save() needed at the end.
})
```

The default in-memory store is bounded at **100,000 entries**. Expired
sessions are evicted lazily on `Load` (the previous implementation kept
expired rows alive until `GC()` was called manually). When the cap is
hit on a fresh `Save`, the store sweeps expired entries first and
otherwise drops the oldest-`expires` entry to make room. Raise the cap
via `MaxEntries` if your workload legitimately keeps many concurrent
sessions; `mw.NoSessionEntryLimit` disables the cap (not recommended outside
tests).

For multi-instance fleets, implement `SessionStore` on top of Redis, Memcache,
SQL, or another shared backend. A production store should:

- apply the provided TTL on every `Save`
- make `Load`, `Save`, and `Delete` safe for concurrent requests
- copy maps at the boundary so request code cannot mutate shared store state
- log or metric save/delete failures internally, because deferred persistence
  may run after headers are already committed
- run with `AsyncStore: true` if it performs network, disk, or database I/O

Use the same rule for rate limiting: single-process memory stores are fine for
one instance; Redis or another shared `RateLimitStore` is required when limits
must be consistent across a fleet.

## Database Connection

gogo handlers play nicely with the standard `database/sql` package. Create
the pool **once** at startup and capture it into every handler closure —
do NOT open a fresh `*sql.DB` per request. With `RunMultiCore`, the same
pool is captured by every worker.

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "time"

    gogo "github.com/Snocko-main/gogo"
    _ "github.com/jackc/pgx/v5/stdlib"          // or your driver of choice
)

type User struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}

func main() {
    db, err := sql.Open("pgx", "postgres://user:pass@localhost/app?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    db.SetMaxOpenConns(100)
    db.SetMaxIdleConns(25)
    db.SetConnMaxLifetime(time.Hour)
    if err := db.Ping(); err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    app, err := gogo.NewApp()
    if err != nil {
        log.Fatal(err)
    }
    defer app.Close()

    // GetAsync handler is free to block on the DB — it runs on a goroutine.
    app.GetAsync("/users/:id<int>", func(res *gogo.Response, req *gogo.Request) {
        id := req.ParamInt("id", 0)

        ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
        defer cancel()

        var u User
        err := db.QueryRowContext(ctx,
            "SELECT id, name FROM users WHERE id = $1", id).
            Scan(&u.ID, &u.Name)
        if err == sql.ErrNoRows {
            res.Send(404, "text/plain", "not found\n")
            return
        }
        if err != nil {
            log.Printf("db query: %v", err)
            res.Send(500, "text/plain", "db error\n")
            return
        }
        res.JSON(200, u)
    })

    // Body-async methods pre-collect the body; BodyParser deserializes JSON / form.
    app.PostAsync("/users", 1<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
        var in struct {
            Name string `json:"name"`
        }
        if err := req.BodyParser(&in); err != nil || in.Name == "" {
            res.Send(400, "text/plain", "bad body\n")
            return
        }
        var id int
        err := db.QueryRow(
            "INSERT INTO users (name) VALUES ($1) RETURNING id",
            in.Name,
        ).Scan(&id)
        if err != nil {
            log.Printf("db insert: %v", err)
            res.Send(500, "text/plain", "db error\n")
            return
        }
        res.JSON(201, User{ID: id, Name: in.Name})
    })

    if !app.Listen(3000) {
        log.Fatal("listen :3000 failed")
    }
    log.Println("listening on http://localhost:3000")
    app.Run()
}
```

### Loading the user from a token (async middleware + DB)

```go
app.Use("/api/*", func(next gogo.AsyncHandler) gogo.AsyncHandler {
    return func(res *gogo.Response, req *gogo.Request) {
        token := req.Header("authorization")
        var u User
        err := db.QueryRow(
            "SELECT id, name FROM users WHERE token = $1", token,
        ).Scan(&u.ID, &u.Name)
        if err == sql.ErrNoRows {
            res.Send(401, "text/plain", "unauthorized\n")
            return
        }
        if err != nil {
            res.Send(500, "text/plain", "db error\n")
            return
        }
        req.SetLocal("user", &u)
        next(res, req)
    }
})
```

## Server-Sent Events (SSE)

SSE is async-only — register the route via `GetAsync`, a body-async route
such as `PostAsync`, `PutAsync`, `PatchAsync`, or `DeleteAsync`, or call
`res.Async(...)` before `res.SSE(...)`. The framework installs the right headers
(`Content-Type: text/event-stream`, `Cache-Control: no-cache`,
`X-Accel-Buffering: no`) and hands you a `*SSEStream`.

```go
app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
    // Honor Last-Event-ID for reconnects.
    resumeFrom := req.Header("last-event-id")

    res.SSE(func(s *gogo.SSEStream) error {
        if resumeFrom != "" {
            _ = s.Comment("resuming after id=" + resumeFrom)
        }
        ticker := time.NewTicker(time.Second)
        defer ticker.Stop()
        pinger := time.NewTicker(15 * time.Second)
        defer pinger.Stop()

        var n int64
        for {
            select {
            case t := <-ticker.C:
                n++
                if err := s.SendEvent(gogo.SSEEvent{
                    ID:    fmt.Sprintf("%d", n),
                    Event: "tick",
                    Data:  map[string]any{"n": n, "at": t.Format(time.RFC3339)},
                }); err != nil {
                    return err
                }
            case <-pinger.C:
                if err := s.Ping(); err != nil {
                    return err
                }
            }
        }
    })
})
```

SSE uses `Response.Stream`, so disconnects while waiting on backpressure return
`gogo.ErrStreamAborted`.

Browser-side:

```html
<script>
const ev = new EventSource('/events');
ev.addEventListener('tick', e => {
    console.log(JSON.parse(e.data));
});
</script>
```

Or from the CLI:

```sh
curl -N http://localhost:3000/events
```

## WebSocket

Register a WebSocket route with `app.WebSocket` or `router.WebSocket`.
Browser clients connect with the normal `WebSocket` API. Because browsers
send an `Origin` header, browser-facing routes should include an `Upgrade`
callback that explicitly accepts or rejects the handshake.

### Echo server

```go
app.WebSocket("/ws", gogo.WebSocketBehavior{
    Upgrade: func(ctx *gogo.UpgradeContext) {
        // Local dev page served from the same app.
        origin := ctx.Header("origin")
        if origin != "" && origin != "http://localhost:3000" {
            ctx.Reject(403, "bad origin")
            return
        }
        ctx.Accept("")
    },
    Open: func(ws *gogo.WebSocket) {
        log.Println("client connected")
        ws.SendText("welcome\n")
    },
    Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
        ws.Send(msg, op)                         // echo back
    },
    Close: func(ws *gogo.WebSocket, code int, msg []byte) {
        log.Printf("client closed: %d %s", code, msg)
    },

    // Limits — set explicitly; do not leave at zero hoping for "unlimited".
    MaxPayloadLength: 1 << 20,                   // 1 MiB
    IdleTimeout:      120 * time.Second,
    MaxBackpressure:  64 * 1024,
})
```

Browser-side:

```html
<script>
const ws = new WebSocket('ws://localhost:3000/ws');

ws.addEventListener('open', () => {
    ws.send('hello from the browser');
});

ws.addEventListener('message', e => {
    console.log('server:', e.data);
});
</script>
```

Or from the CLI:

```sh
websocat ws://localhost:3000/ws
```

### Upgrade gate

For browser clients, add an `Upgrade` callback and explicitly accept or
reject the handshake. This is where origin checks, token checks,
subprotocol negotiation, and per-connection user data belong.

The order is:

1. Client sends an HTTP `GET` request with `Upgrade: websocket`.
2. gogo calls `Upgrade` while the request is still an HTTP handshake.
3. `ctx.Reject(...)` returns an HTTP error response and no socket opens.
4. `ctx.Accept(...)` completes the `101 Switching Protocols` handshake.
5. `Open` runs after the connection is established, then `Message` runs for
   frames from that client.

```go
app.WebSocket("/ws", gogo.WebSocketBehavior{
    Upgrade: func(ctx *gogo.UpgradeContext) {
        if ctx.Header("origin") != "https://app.example.com" {
            ctx.Reject(403, "bad origin")
            return
        }

        user, ok := loadUserFromToken(ctx.QueryParam("token"))
        if !ok {
            ctx.Reject(401, "bad token")
            return
        }

        ctx.SetUserData(user) // available later via ws.UserData()
        ctx.Accept("")        // accept with no subprotocol
    },
    Open: func(ws *gogo.WebSocket) {
        user := ws.UserData().(*User)
        ws.SendText("welcome, " + user.Name + "\n")
    },
    Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
        ws.Send(msg, op)
    },
})
```

Browser-side with a token:

```html
<script>
const token = encodeURIComponent(window.localStorage.getItem('token') || '');
const ws = new WebSocket(`wss://api.example.com/ws?token=${token}`);
</script>
```

### Pub/Sub

For app code, prefer `WSHub`. It keeps the fast uWS local path, fans out
across every `App` attached in the current process, and can bridge multiple
processes or hosts through an adapter such as Redis.

```go
hub := gogo.NewWSHub()
defer hub.Close()

hub.WebSocket(app, "/chat", gogo.WebSocketBehavior{
    Upgrade: func(ctx *gogo.UpgradeContext) {
        ctx.Accept("")
    },
    Open: func(ws *gogo.WebSocket) {
        hub.Subscribe(ws, "room.general")
        ws.SendText("joined room.general\n")
    },
    Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
        ws.SendText("you: " + string(msg))
        _ = hub.PublishFrom(ws, "room.general", msg, op)
    },
})
```

`PublishFrom` skips the sender and is safe to call from any goroutine. Register
the route with `hub.WebSocket` or `hub.Wrap` so the hub can identify the socket;
subscriptions made with either `hub.Subscribe` or raw `ws.Subscribe` are tracked.
Adapter publishes are queued onto a hub worker, so Redis/network I/O never
blocks the WebSocket loop. From worker goroutines, scheduled jobs, or HTTP
handlers, use `hub.Publish` or `hub.PublishBatch`. A successful publish call
means local fan-out completed and the adapter message was queued; Redis/network
errors are reported through `WithWSHubAdapterErrorHandler`.

If you call raw `ws.Publish` from a WebSocket handler in `RunMultiCore`, gogo
publishes locally on the current loop and schedules one copied publish on each
peer loop. That keeps delivery correct across cores, but the peer part is
O(worker count); use `WSHub` when you want the skip-sender semantics plus a
clear place to add Redis/cluster fan-out.

```go
go func() {
    for {
        time.Sleep(10 * time.Second)
        _ = hub.Publish("room.general", []byte("server tick"), gogo.Text)
    }
}()
```

For bursty fan-out, batch publishes keep the same one-cgo-crossing local
fast path as `App.PublishBatch`:

```go
_ = hub.PublishBatch([]gogo.PublishMessage{
    {Topic: "room.general", Message: []byte("hi 1"), OpCode: gogo.Text},
    {Topic: "room.general", Message: []byte("hi 2"), OpCode: gogo.Text},
    {Topic: "alerts",       Message: []byte("ping"), OpCode: gogo.Text},
})
```

For multi-process or multi-host deployments, add a Redis adapter. Redis
Pub/Sub is best-effort realtime fan-out: fast and simple, but disconnected
processes do not replay missed messages.

```go
import redisadapter "github.com/Snocko-main/gogo/adapters/redis"

adapter, err := redisadapter.New(redisadapter.Options{
    URL: "redis://localhost:6379/0",
    // Optional for large fleets: subscribe Redis only to topics that have
    // local WebSocket subscribers. The hub updates Redis from its adapter
    // worker, so subscribe/unsubscribe never blocks the uWS loop.
    DynamicSubscriptions: true,
    // Optional: tune burst absorption before go-redis can drop Pub/Sub
    // messages because the receive channel is full.
    ChannelSize: 4096,
    // Defaults to 16 MiB, matching WebSocket MaxPayloadLength.
    MaxMessageSize: 16 << 20,
})
if err != nil {
    log.Fatal(err)
}
hub := gogo.NewWSHub(
    gogo.WithWSHubAdapter(adapter),
    gogo.WithWSHubCloseTimeout(5*time.Second),
    gogo.WithWSHubAdapterErrorHandler(func(err error) {
        log.Printf("websocket hub adapter: %v", err)
    }),
)
defer hub.Close()
if err := hub.Start(); err != nil {
    log.Fatal(err)
}
```

Keep the default one adapter worker when cross-process message order matters.
If your workload can tolerate reordering, `gogo.WithWSHubAdapterWorkers(4)` can
raise Redis publish throughput.

The public adapter contract is documented in
[`docs/websocket-hub-adapter.md`](docs/websocket-hub-adapter.md). In short,
hub adapter fan-out is best-effort: the hub preserves adapter publish call
order only with the default single worker, does not retry failed broker
publishes after they leave the queue, and relies on `Close`/contexts to stop
adapter receive loops and in-flight operations.

`DynamicSubscriptions` uses gogo's Go-side `ws.Subscribe` / `ws.Unsubscribe`
tracking rather than a uWS subscription callback, so it does not add an extra
C-to-Go callback on the WebSocket hot path. HTTP `GetAsync` / `PostAsync`
handlers keep the same shared-memory zero-cgo dispatch path; the Redis work is
isolated behind the hub's adapter queue. Subscription changes are reconciled to
the latest local topic state, so rapid leave/join churn cannot leave Redis
subscribed to the wrong final state. For very high subscription churn,
`gogo.WithWSHubAdapterTopicWorkers(2)` can parallelize reconciliation.

With `RunMultiCore`, create one hub outside setup and register every worker's
route through it:

```go
hub := gogo.NewWSHub(gogo.WithWSHubAdapter(adapter))
handle, err := gogo.RunMultiCore(4, 3000, func(app *gogo.App) {
    hub.WebSocket(app, "/chat", behavior)
})
```

### Upgrade-time auth and subprotocols

⚠️ **CSWSH** — the HTTP CORS middleware does NOT cover the WebSocket
upgrade path. If a WebSocket endpoint accepts a browser handshake from
`https://evil.example`, that page can open `ws://yoursite/ws` and ride
the user's session cookies. Same idea as CSRF, different protocol.

Secure default: when `Upgrade` is nil, gogo accepts non-browser clients
that omit `Origin` (CLI tools, service-to-service clients) and rejects
browser-style handshakes that include `Origin` with `403 origin not
allowed`. For browser clients, install an explicit `Upgrade` callback
that checks an origin allow-list.

**Use `middleware.WebSocketAuth` for the common case** — origin
allow-list, optional Verify callback, optional subprotocol gating:

```go
import mw "github.com/Snocko-main/gogo/middleware"

app.WebSocket("/ws", gogo.WebSocketBehavior{
    Upgrade: mw.WebSocketAuth(mw.WebSocketAuthOptions{
        AllowedOrigins: []string{"https://app.example.com"},
        Verify: func(ctx *gogo.UpgradeContext) (any, bool, int, string) {
            user, err := db.LoadUserByToken(ctx.QueryParam("token"))
            if err != nil {
                return nil, false, 401, "bad token"
            }
            return user, true, 0, ""           // userData = user
        },
        AllowedSubprotocols: []string{"chat.v2", "chat.v1"},
    }),
    Open: func(ws *gogo.WebSocket) {
        u := ws.UserData().(*User)             // set by Verify
        ws.SendText("welcome, " + u.Name + "\n")
        ws.Subscribe("user." + strconv.Itoa(u.ID))
    },
    Message: handleWSMessage,
})
```

Defaults:
- Zero-value `WebSocketAuth` rejects every handshake until you opt into
  browser origins or missing-Origin CLI/service clients.
- Empty `AllowedOrigins` + `AllowMissingOrigin=false` → reject browser
  handshakes because their Origin is not allow-listed, and reject
  non-browser handshakes because the Origin header is missing. Set
  `AllowedOrigins` explicitly before going to production.
- `AllowMissingOrigin=true` → permit handshakes without an Origin
  (CLI tools like `websocat`). Safe IF you have no browser clients
  on this endpoint.
- `AllowedOrigins: []string{"*"}` → accept any origin. Opt-in for
  public APIs that don't rely on ambient cookie auth. Use `"*"` only by
  itself; gogo panics at startup if it is mixed with explicit origins.

**Legacy auto-accept** — if you intentionally want the old uWS behavior
of accepting every handshake when `Upgrade` is nil, set
`UnsafeAutoUpgrade: true`. Use this only for public, non-cookie
endpoints where cross-origin WebSocket access is expected:

```go
app.WebSocket("/public-events", gogo.WebSocketBehavior{
    UnsafeAutoUpgrade: true,
    Open: func(ws *gogo.WebSocket) {
        ws.Subscribe("public.events")
    },
})
```

**Roll-your-own** — if `WebSocketAuth` doesn't fit, write the
callback directly. The same hooks apply: inspect `ctx.Header("origin")`,
`ctx.QueryParam(...)`, `ctx.Protocols()`, call `ctx.SetUserData(...)`
+ `ctx.Accept(protocol)` or `ctx.Reject(status, body)`:

```go
app.WebSocket("/ws", gogo.WebSocketBehavior{
    Upgrade: func(ctx *gogo.UpgradeContext) {
        // YOU are responsible for the origin check here.
        if !allowOrigin(ctx.Header("origin")) {
            ctx.Reject(403, "bad origin")
            return
        }
        // ... rest of auth ...
        ctx.Accept("")
    },
    Open:    handleOpen,
    Message: handleMessage,
})
```

## File Uploads

```go
// 1) Pre-collected body (simplest). Oversize → 413 automatically.
app.PostAsync("/upload", 10<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
    sum := sha256.Sum256(body)
    res.JSON(200, map[string]any{
        "size":   len(body),
        "sha256": hex.EncodeToString(sum[:]),
    })
})

// 2) multipart/form-data with file parts.
app.PostAsync("/upload-form", 50<<20, func(res *gogo.Response, req *gogo.Request, body []byte) {
    err := req.Multipart(func(p *gogo.MultipartPart) error {
        if p.IsFile() {
            _, err := p.SaveInto("./uploads")
            return err
        }
        log.Printf("field %s = %s", p.Name, p.Data)
        return nil
    })
    if err != nil {
        res.Send(400, "text/plain", "bad multipart\n")
        return
    }
    res.Send(200, "text/plain", "ok\n")
})

// 3) Streaming with OnData — bytes never held all-at-once.
app.Post("/upload-stream", func(res *gogo.Response, req *gogo.Request) {
    var total int
    res.OnData(func(chunk []byte, isLast bool) {
        total += len(chunk)
        if isLast {
            res.Send(200, "text/plain", fmt.Sprintf("counted %d bytes\n", total))
        }
    })
})
```

## Multi-core

Single-loop mode (`NewApp` + `Run`) caps throughput at one OS thread. To
saturate every vCPU, use `RunMultiCore` — N independent App instances bound
to the same port. Accepted sockets are round-robined across the App loops, so
scaling does not depend on the kernel's `SO_REUSEPORT` hash distributing
connections evenly.

```go
func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    // Shared resources — create ONCE outside setup so every worker captures
    // the same pointers.
    db := mustOpenDB()
    defer db.Close()

    handle, err := gogo.RunMultiCore(runtime.NumCPU(), 3000, func(app *gogo.App) {
        app.Get("/plain", func(res *gogo.Response, req *gogo.Request) {
            res.Send(200, "text/plain", "ok")
        })
        app.GetAsync("/db", func(res *gogo.Response, req *gogo.Request) {
            var n int
            _ = db.QueryRow("SELECT 1").Scan(&n)
            res.JSON(200, map[string]int{"n": n})
        })
    })
    if err != nil {
        log.Fatal(err)
    }

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh
    log.Println("stopping workers")
    handle.Shutdown()
    handle.Wait()
}
```

`RunMultiCore` currently has no `Config` or options parameter. Each worker
`App` is created with the zero-value `Config`, so app-scoped fields such as
`BodyLimit`, `BodyReadTimeout`, `BindAddr`, `CapturePeerIP`, `TrustProxy`,
`JSONEncoder`, and `JSONDecoder` cannot be supplied through this helper today.
Set process-wide knobs before `RunMultiCore`, register per-route/per-middleware
options inside `setup`, and create shared resources outside `setup` so every
worker captures the same instance.

`MultiCoreHandle.Shutdown` is immediate: it calls `Shutdown` on every worker,
which closes the listen socket and active connections. It is safe and
idempotent, and `Wait` blocks until every worker loop exits and native
resources are freed. There is not yet a multicore equivalent of
`App.ShutdownGracefully`; use a single-loop `App` when you need the built-in
graceful drain behavior.

Tuning knobs that actually matter:

- `GOMAXPROCS` — pin to the same N you passed to `RunMultiCore`.
- `gogo.SetWorkerCount(n)` — controls the `GetAsync` worker pool. Default
  is `NumCPU`; with `RunMultiCore` consider halving this since each loop
  already owns one core.
- Pin shared resources (DB pools, caches) to one allocation outside
  `setup`.
- For strict CPU pinning, run under `taskset -c 0-(N-1)`.

See `examples/multicore` for a full setup with `/metrics`.

## Graceful Shutdown

```go
app.OnShutdown(func() {
    log.Println("draining…")
    db.Close()
})

runDone := make(chan struct{})
go func() {
    app.Run()
    close(runDone)
}()

sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
<-sigCh

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := app.ShutdownContext(ctx); err != nil {
    log.Printf("forced shutdown: %v", err)
}
<-runDone
app.Close() // free native resources after Run returns
```

`ShutdownContext` starts a graceful drain and returns nil after `Run` exits. If
the context expires first, it force-closes active connections and returns the
context error; wait for `Run` to return before calling `Close`.
`ShutdownGracefully` starts the same graceful drain without blocking. `Shutdown`
closes the listen socket and active connections immediately; `Close` frees
native resources. Shutdown APIs are safe from any goroutine.

Native builds own the uWS loop on an internal locked goroutine. Application
code does not need `runtime.LockOSThread` for the normal `NewApp` / route
registration / `Listen` / `Run` / `Close` lifecycle. Register routes,
middleware, and WebSocket behavior before `Listen` / `Run`; shutdown and
publish APIs are the cross-goroutine entry points once the loop is running.

`ShutdownGracefully` is an `App` API. `RunMultiCore` currently exposes only
`MultiCoreHandle.Shutdown`, which stops every worker immediately and may drop
in-flight responses.

## Configuration

```go
app, _ := gogo.NewApp(gogo.Config{
    BodyLimit:       4 << 20,                    // 4 MiB; oversize -> 413
    BodyReadTimeout: 30 * time.Second,           // slow body upload deadline
    BindAddr:        "127.0.0.1",                // localhost only
    TrustedProxies:  []string{"10.0.0.0/8"},     // honor forwarded headers from these peers
    // JSONEncoder:  sonic.Marshal,    // optional: faster JSON responses
    // JSONDecoder:  sonic.Unmarshal,  // optional: faster BodyParser JSON
})
```

`NewApp` intentionally accepts either no argument or one `Config`: `NewApp()`
uses safe defaults, and `NewApp(gogo.Config{...})` applies overrides. Passing
multiple configs returns an error so configuration stays unambiguous.

| Field              | Default                   | Notes                                                     |
| ------------------ | ------------------------- | --------------------------------------------------------- |
| `BodyLimit`        | 4 MiB                     | Reject Content-Length > limit with 413 on the C++ side    |
| `BodyReadTimeout`  | 30s                       | Deadline for `Response.Body` to finish reading the body   |
| `BindAddr`         | `""`                      | Empty = all interfaces (`0.0.0.0`)                        |
| `CapturePeerIP`    | `false`                   | Snapshot peer IP for async / shared-dispatch paths        |
| `TrustProxy`       | `false`                   | Compatibility shortcut: trust forwarded headers from any peer |
| `TrustedProxies`   | empty                     | Trust forwarded headers only from listed IPs/CIDR ranges  |
| `JSONEncoder`      | `encoding/json.Marshal`   | Encoder for `Response.JSON` and `Response.JSONP`          |
| `JSONDecoder`      | `encoding/json.Unmarshal` | Decoder for `Request.BodyParser` JSON bodies              |

See [`docs/configuration.md`](docs/configuration.md) for local development,
reverse proxy, and production configuration examples.

`BodyReadTimeout` protects `Response.Body` users from slow body uploads that
drip bytes forever without exceeding `BodyLimit`. Keep the 30s default for
ordinary APIs, lower it for small JSON-only endpoints, raise it for legitimate
large uploads, or set `BodyReadTimeout: gogo.NoBodyReadTimeout` only when
intentionally disabling the deadline for trusted traffic/tests.

`BodyLimit: 0` uses the safe 4 MiB default. Set `BodyLimit: gogo.NoBodyLimit`
only for trusted deployments that already enforce a request-body cap at an
external layer such as a reverse proxy.

`JSONEncoder` and `JSONDecoder` let you plug in drop-in JSON libraries such as
`sonic`, `go-json`, or `jsoniter` without adding a framework dependency. Leave
them nil for the standard library defaults. `JSONP` still escapes script-breakout
characters defensively even when a custom encoder does not mirror
`encoding/json`'s HTML escaping. `JSONStream` exposes `*json.Encoder` directly,
so it intentionally keeps using `encoding/json`; use `Response.Stream` when you
need to stream custom-encoded chunks.

### Global state audit

Most configuration is per `App`, route, middleware, or hub instance. The
remaining process-wide knobs and exported mutable sentinels are audited in
[`docs/global-state.md`](docs/global-state.md), including panic handling, typed
parameter registration, worker count settings, and package-level limits.

### Test server helpers

`NewTestServer` starts a real loopback listener for integration-style tests.
`NewTestServerT` is the `testing.TB`-friendly wrapper: it fails the test on
startup errors and registers `Close` with `t.Cleanup`.
See [`docs/testing.md`](docs/testing.md) for sync, async, body, middleware,
WebSocket, graceful shutdown, and test-server serialization guidance.

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
    // assert status/body/headers
}
```

### TrustProxy and client IPs

When `TrustProxy` and `TrustedProxies` are both unset (the default), the
framework treats every `X-Forwarded-*` header as untrusted attacker input:

- `req.Protocol()` / `req.Secure()` ignore `X-Forwarded-Proto`.
- `req.IPs()` returns `nil` (the X-Forwarded-For chain is not exposed).
- `req.IP()` returns the immediate TCP peer — the proxy itself if you have one.

Use `TrustedProxies` for production when only specific reverse proxies,
load balancers, or private edge ranges should be trusted. Entries may be
single IPs or CIDR ranges. When the immediate TCP peer matches the list,
`req.Protocol()` / `req.Secure()` honor `X-Forwarded-Proto` and `req.IPs()`
normalizes valid entries from `X-Forwarded-For` in order. The leftmost entry is
the client IP as reported by your proxy chain.

The trusted edge must strip or overwrite incoming `X-Forwarded-*`, `Forwarded`,
and `X-Real-IP` headers from untrusted clients before adding its own values.
Otherwise an attacker can smuggle a spoofed client IP into the forwarded chain.

```go
// Behind a private load balancer or CDN edge range.
app, _ := gogo.NewApp(gogo.Config{
    TrustedProxies: []string{"10.0.0.0/8", "127.0.0.1"},
})

app.Get("/whoami", func(res *gogo.Response, req *gogo.Request) {
    ips := req.IPs()
    client := req.IP()
    if len(ips) > 0 {
        client = ips[0]                      // leftmost = original client
    }
    res.Send(200, "text/plain", "you are "+client+"\n")
})
```

`TrustedProxies` automatically enables `CapturePeerIP` so async and
shared-dispatch routes can evaluate the immediate peer before trusting
forwarded headers. You may still set `CapturePeerIP` yourself when async
handlers need `req.IP()` even without proxy headers.

`TrustProxy: true` remains available for deployments where every possible
immediate peer is already trusted by the network boundary. Prefer
`TrustedProxies` when the app can be reached from both trusted and untrusted
peers, or when you want the application to enforce the proxy trust boundary.

Internet-facing servers that read X-Forwarded-For anyway (against
recommendation) must call `req.Header("x-forwarded-for")` and parse it
themselves, accepting that any client can forge the value.

### CapturePeerIP and async routes

`CapturePeerIP` is separate from `TrustProxy`. It controls whether gogo copies
the immediate TCP peer IP into the request snapshot used by async route helpers.
Leave it off unless an async handler needs `req.IP()` for logging, audit,
rate-limiting, or auth decisions.

| Route style | `CapturePeerIP=false` | `CapturePeerIP=true` |
| --- | --- | --- |
| Sync handlers | `req.IP()` lazily reads the live uWS response and works normally | Same behavior |
| `GetAsync` / `Router.GetAsync` | `req.IP()` is `""` in the async snapshot | `req.IP()` is populated from the TCP peer |
| Body-async routes (`PostAsync`, `PutAsync`, `PatchAsync`, `DeleteAsync`) | `req.IP()` is `""` in the async snapshot | `req.IP()` is populated from the TCP peer |

`req.IPs()` reads `X-Forwarded-For` from request headers and is controlled by
`TrustProxy` / `TrustedProxies`, not by `CapturePeerIP`. If an async route sits
behind a trusted proxy and wants the original client, configure
`TrustedProxies` and use `req.IPs()`.
If it wants the proxy/socket peer, enable `CapturePeerIP` and use `req.IP()`.

### net/http adapter body cap

`gogo.HTTPAdapter(h)` and `gogo.HTTPAdapterWithBody(h, body)` are compatibility
helpers for small stdlib handlers: use them as a route-by-route migration
bridge, or as a testing/ops convenience for stdlib endpoints such as expvar and
pprof. They stage the wrapped handler's response before sending it through
gogo, so the staged body is capped by
`gogo.GetMaxHTTPAdapterBodyBytes()` (default 8 MiB; set to
`gogo.SetMaxHTTPAdapterBodyBytes(gogo.NoHTTPAdapterBodyLimit)` to disable).
The adapter accepts `http.Flusher` for compatibility, but `Flush()` only
commits the staged status code; it does not stream bytes to the client.
Large or streaming routes should be ported to native gogo APIs instead of
going through the adapter. See `examples/httpadapter` for expvar and pprof
debug endpoints registered through `HTTPAdapter`.

### Redirect and open redirects

`res.Redirect(loc, code)` writes any string into the `Location` header
that gogo can validate is free of header-injection control characters
(CR/LF/NUL — those return 500 without panicking). The framework does
**NOT** validate that the target stays within your own host.

Passing user-controlled input straight to `Redirect` is an open-redirect
bug — attackers craft links that look like your domain but bounce to a
phishing page:

```go
// ❌ Vulnerable
app.Get("/login", func(res *gogo.Response, req *gogo.Request) {
    res.Redirect(req.QueryParam("next"), 302)    // attacker: ?next=https://evil.com
})

// ✅ Safe — validate against an allow-list
var safeNextPaths = map[string]bool{"/": true, "/dashboard": true, "/profile": true}

app.Get("/login", func(res *gogo.Response, req *gogo.Request) {
    next := req.QueryParam("next")
    if !safeNextPaths[next] {
        next = "/"
    }
    res.Redirect(next, 302)
})
```

For more flexible targets, parse the URL and verify the host matches
your own before redirecting.

## Error Handling & Panic Recovery

Install a custom panic handler to ship recovery events to your error tracker:

```go
gogo.SetPanicHandler(func(recovered any) {
    log.Printf("PANIC: %v\n%s", recovered, debug.Stack())
    sentry.CaptureException(fmt.Errorf("%v", recovered))
})
```

The framework catches handler panics across HTTP, async, WebSocket, defer,
and body callbacks. It emits a best-effort 500 where an HTTP response is
still available and keeps the server alive. `SetPanicHandler` is process-wide;
install it during startup when a process hosts multiple `App` instances.

Custom 404 / 405:

```go
app.NotFound(func(res *gogo.Response, req *gogo.Request) {
    res.JSON(404, map[string]string{"error": "not found", "path": req.URL()})
})

app.MethodNotAllowed(func(res *gogo.Response, req *gogo.Request) {
    allow := strings.Join(app.AllowedMethods(req.URL()), ", ")
    res.Status(405)
    res.Header("Allow", allow)
    res.Header("Content-Type", "application/json")
    res.End(`{"error":"method not allowed"}`)
})
```

## Caveats

- `*Request` and `*Response` from sync handlers are valid only during the
  handler callback. Async handlers receive a snapshot Request that survives
  past the C-side lifetime.
- Sync handlers run on the uWS loop thread — never block them. Use
  `GetAsync` / `PostAsync` for anything that does IO.
- Native lifecycle decisions from the v0.3 config pass are recorded in
  [`docs/native-lifecycle-v0.3.md`](docs/native-lifecycle-v0.3.md).
- `net/http` middleware is not directly compatible (different signature).
  Adapt with a small wrapper or use the bundled `middleware` package.
- WebSocket pub/sub topics are exact-match strings — no MQTT-style
  wildcards.
- WebSocket `Upgrade == nil` rejects browser handshakes with an
  `Origin` header, but accepts non-browser handshakes that omit it.
  Use an explicit `Upgrade` callback for browser clients. Set
  `UnsafeAutoUpgrade: true` only when cross-origin auto-accept is
  intentional and the endpoint does not rely on ambient cookies. HTTP
  CORS middleware does NOT cover the upgrade. See
  [Upgrade-time auth and subprotocols](#upgrade-time-auth-and-subprotocols)
  for `middleware.WebSocketAuth`.
- TLS / HTTP/2 are out of scope here; terminate at a reverse proxy
  (nginx, Caddy, an L7 load balancer).
- `req.IPs()` returns `nil` unless `Config.TrustProxy=true` or the immediate
  peer matches `Config.TrustedProxies` — see
  [TrustProxy and client IPs](#trustproxy-and-client-ips).
- `res.Redirect` does not protect against open redirects — caller must
  allow-list targets. See [Redirect and open redirects](#redirect-and-open-redirects).
- `mw.RequestID()` emits 32-hex-char (128-bit) IDs by default — see
  [RequestID — 128-bit IDs](#requestid--128-bit-ids) if you have a
  downstream parser that hard-codes 16-char IDs.
- `mw.RateLimit()` caps the in-memory store at 100k buckets — raise via
  `MaxBuckets` or plug a Redis store for high-cardinality keys. See
  [RateLimit memory cap](#ratelimit-memory-cap).
- `mw.RateLimit()` defaults to the immediate peer IP. Behind trusted proxies,
  use a `KeyFunc` based on `req.IPs()` when you want end-client quotas.
- Custom middleware that runs cleanup AFTER the handler must use
  `Response.OnFinish` (not `next; cleanup` directly) — otherwise a
  handler that upgrades via `res.Async` will run its real work after
  cleanup already fired. See
  [Post-handler cleanup](#post-handler-cleanup-with-responseonfinish).

## Examples

- [`examples/hello`](examples/hello) — static reply + sync + async handler
- [`examples/restapi`](examples/restapi) — in-memory CRUD with JSON + query filter
- [`examples/authmw`](examples/authmw) — logger + bearer auth (sync & async middleware)
- [`examples/upload`](examples/upload) — POST body collection with 413 + streaming OnData
- [`examples/sse`](examples/sse) — Server-Sent Events with reconnect resume
- [`examples/websocket`](examples/websocket) — browser WebSocket + upgrade gate + pub/sub
- [`examples/multicore`](examples/multicore) — `RunMultiCore` + `/metrics` + graceful shutdown
- [`examples/httpadapter`](examples/httpadapter) — stdlib expvar + pprof through `HTTPAdapter`

## Why There Is a C++ Bridge

uWebSockets is not a C library. Its public API is C++ template-heavy, so cgo
cannot call it directly in a pleasant or stable way. The `uws_bridge.cpp` file
turns the parts Go needs into a small C ABI.

## Benchmarking

There are six comparable HTTP benchmark servers, spanning Go, Node,
Bun, and Rust:

| benchmark dir | framework | language / runtime |
|---|---|---|
| `benchmark/gogo`            | this binding (uWebSockets) | Go (cgo → C++) |
| `benchmark/actix`           | actix-web 4 (release + fat LTO) | Rust |
| `benchmark/fiber`           | gofiber/fiber (fasthttp)     | Go |
| `benchmark/node-uwebsockets`| uWebSockets.js               | JavaScript (Node) |
| `benchmark/bun-elysia`      | Elysia                       | TypeScript (Bun) |
| `benchmark/nethttp`         | Go standard library `net/http` | Go |

`scripts/bench_wrk.sh` starts each server, hits the GET endpoints
(`/hello`, `/hello/:name`, `/db`), then POSTs against `/echo` (sync
body-echo) and `/query` (body carries an id, server runs a SQLite
lookup), and tears it down. See the script header for the env knobs.

Go benchmark dependencies live in the nested `benchmark` module so importing
gogo does not pull benchmark-only frameworks or database drivers into your
application module graph.

### Results

Median across `wrk -t {1,2,4,8} -c 500 -d 10s` — this is not an
average of hand-picked thread counts. Each result comes from the
same four `wrk` thread counts, sorted, then medianed.

- `POST /echo` — sync handler reads the body and writes it back
  unchanged (50-byte JSON). Exercises the pure body-collection +
  response-write path. gogo uses `app.Post` here (no goroutine
  handoff).
- `POST /query` — body carries an integer id; handler parses it and
  runs `SELECT … FROM users WHERE id = ?` against SQLite, returns the
  row. Realistic API shape: body parse + blocking I/O + JSON
  response. gogo uses `PostAsync` here — the small body fits the
  shared-dispatch cap so the request crosses zero cgo callbacks on
  the hot path, and the handler runs on a worker goroutine so the
  blocking `sql.DB.QueryRow` doesn't pin the loop thread.

> **Hardware note** — these numbers come from a local Apple M3 laptop
> (Darwin arm64, 4 performance cores + 4 efficiency cores, 16 GB RAM).
> The run used Go 1.26.3, Node 22.15.0, Bun 1.3.14, Rust 1.95.0, and
> `wrk` 4.2.0. Multi-worker mode is capped at 4 server workers/processes
> for every framework (`MULTI_WORKERS=4`), matching the performance-core
> count. Using all 8 logical CPUs put event loops on efficiency cores and
> made the server compete harder with `wrk` and async helper goroutines,
> which inflated tail latency and made the comparison less fair.
> Absolute rps is hardware-sensitive; compare the relative shape on your
> own target machine before making capacity decisions.

#### Highlights

- **gogo has the highest throughput in every single-worker workload**
  in this matrix.
- **gogo keeps the top 4-worker throughput in every workload**
  while staying inside the 4 performance-core cap.
- **Latency stays competitive while leading throughput**: gogo p99 is
  within a few milliseconds of the best tail in most routes, and avoids
  the large SQLite p99 spikes seen in Actix and net/http.

#### Throughput summary

| workload | gogo single | best non-gogo single | gogo 4-worker | best non-gogo 4-worker |
|---|---:|---:|---:|---:|
| `GET /hello` | **269k** | uWS.js 206k | **263k** | Actix 192k |
| `GET /hello/:name` | **254k** | uWS.js 208k | **248k** | uWS.js 206k |
| `GET /db` | **167k** | uWS.js 140k | **167k** | uWS.js 136k |
| `POST /echo` | **205k** | Fiber 185k | **199k** | Actix 186k |
| `POST /query` | **153k** | uWS.js 127k | **132k** | uWS.js 122k |

#### Tail latency summary

Lower p99 is better.

| workload | gogo single p99 | best non-gogo single p99 | gogo 4-worker p99 | best non-gogo 4-worker p99 |
|---|---:|---:|---:|---:|
| `GET /hello` | 3.6 ms | uWS.js 3.3 ms | 3.9 ms | Fiber 5.4 ms |
| `GET /hello/:name` | 3.6 ms | uWS.js 3.8 ms | 3.9 ms | Fiber 4.9 ms |
| `GET /db` | 7.5 ms | uWS.js 5.1 ms | 7.8 ms | uWS.js 7.4 ms |
| `POST /echo` | 3.6 ms | uWS.js 3.7 ms | 4.9 ms | uWS.js 5.7 ms |
| `POST /query` | 8.1 ms | uWS.js 5.3 ms | 10.9 ms | uWS.js 7.3 ms |

<details>
<summary>Full per-framework median results</summary>

Each cell shows req/s on the first line, `p50 / p99` latency on the
second.

#### Single worker (1 thread / event loop)

| framework  | language | `/hello`                          | `/hello/:name`                    | `/db`                              | `POST /echo`                       | `POST /query`                      |
|------------|----------|----------------------------------:|----------------------------------:|-----------------------------------:|-----------------------------------:|-----------------------------------:|
| **gogo**   | Go (cgo) | 269k rps<br>p50 1.6 / p99 3.6 ms | 254k rps<br>p50 1.8 / p99 3.6 ms | 167k rps<br>p50 2.7 / p99 7.5 ms | 205k rps<br>p50 2.3 / p99 3.6 ms | 153k rps<br>p50 3.0 / p99 8.1 ms |
| uwsjs      | JS (Node)| 206k rps<br>p50 2.3 / p99 3.3 ms | 208k rps<br>p50 2.3 / p99 3.8 ms | 140k rps<br>p50 3.4 / p99 5.1 ms | 181k rps<br>p50 2.7 / p99 3.7 ms | 127k rps<br>p50 3.8 / p99 5.3 ms |
| fiber      | Go       | 203k rps<br>p50 2.3 / p99 4.0 ms | 199k rps<br>p50 2.4 / p99 4.0 ms |  87k rps<br>p50 5.5 / p99 7.8 ms | 185k rps<br>p50 2.6 / p99 4.0 ms |  88k rps<br>p50 5.5 / p99 8.3 ms |
| actix      | Rust     | 177k rps<br>p50 2.6 / p99 5.1 ms | 185k rps<br>p50 2.6 / p99 5.5 ms |  99k rps<br>p50 4.7 / p99 28.0 ms | 170k rps<br>p50 2.8 / p99 5.2 ms |  78k rps<br>p50 5.4 / p99 36.4 ms |
| bun+elysia | TS (Bun) | 155k rps<br>p50 3.1 / p99 6.4 ms | 146k rps<br>p50 3.2 / p99 7.9 ms | 101k rps<br>p50 4.8 / p99 11.1 ms | 116k rps<br>p50 4.1 / p99 8.7 ms |  87k rps<br>p50 5.5 / p99 12.5 ms |
| net/http   | Go       | 120k rps<br>p50 3.9 / p99 6.9 ms | 123k rps<br>p50 3.8 / p99 6.1 ms |  64k rps<br>p50 7.6 / p99 10.7 ms | 102k rps<br>p50 4.5 / p99 7.6 ms |  62k rps<br>p50 8.0 / p99 11.2 ms |

#### Multi-worker (4 server workers / processes)

| framework  | language | `/hello`                          | `/hello/:name`                    | `/db`                              | `POST /echo`                       | `POST /query`                      |
|------------|----------|----------------------------------:|----------------------------------:|-----------------------------------:|-----------------------------------:|-----------------------------------:|
| **gogo**   | Go (cgo) | 263k rps<br>p50 1.7 / p99 3.9 ms | 248k rps<br>p50 1.8 / p99 3.9 ms | 167k rps<br>p50 2.7 / p99 7.8 ms | 199k rps<br>p50 2.4 / p99 4.9 ms | 132k rps<br>p50 3.4 / p99 10.9 ms |
| actix      | Rust     | 192k rps<br>p50 1.6 / p99 11.6 ms | 194k rps<br>p50 1.6 / p99 8.0 ms | 114k rps<br>p50 2.8 / p99 34.6 ms | 186k rps<br>p50 1.7 / p99 10.3 ms | 106k rps<br>p50 3.1 / p99 35.6 ms |
| uwsjs      | JS (Node)| 181k rps<br>p50 2.5 / p99 5.5 ms | 206k rps<br>p50 2.3 / p99 5.3 ms | 136k rps<br>p50 3.5 / p99 7.4 ms | 168k rps<br>p50 2.8 / p99 5.7 ms | 122k rps<br>p50 3.9 / p99 7.3 ms |
| net/http   | Go       | 179k rps<br>p50 1.8 / p99 7.8 ms | 161k rps<br>p50 2.1 / p99 10.3 ms |  79k rps<br>p50 5.9 / p99 27.5 ms | 153k rps<br>p50 2.3 / p99 9.5 ms |  87k rps<br>p50 5.4 / p99 21.9 ms |
| fiber      | Go       | 174k rps<br>p50 2.7 / p99 5.4 ms | 193k rps<br>p50 2.5 / p99 4.9 ms |  86k rps<br>p50 5.6 / p99 10.4 ms | 163k rps<br>p50 2.8 / p99 5.9 ms |  71k rps<br>p50 6.7 / p99 13.2 ms |
| bun+elysia | TS (Bun) | 157k rps<br>p50 3.0 / p99 6.0 ms | 160k rps<br>p50 2.9 / p99 5.5 ms | 104k rps<br>p50 4.6 / p99 10.6 ms | 124k rps<br>p50 3.8 / p99 8.0 ms |  92k rps<br>p50 5.2 / p99 11.5 ms |

</details>

`/db` reads one row from a 1000-row SQLite table with a random id —
exercises the framework + driver, not just the HTTP layer.

Notes on the spread:

- **Throughput**: gogo leads the single-worker table on all five
  endpoints and keeps the strongest 4-worker throughput on all five
  endpoints on this machine. uwsjs remains close on the uWebSockets-shaped
  routes, while Actix is competitive on pure GET/echo paths and does well
  on SQLite throughput.
- **Tail latency (p99)**: single-worker uwsjs has the tightest tail on
  `/db` and `POST /query`; gogo is close while carrying higher
  throughput. In 4-worker mode, gogo and uwsjs keep the tightest p99
  on most routes. Actix and net/http have good median latency and
  throughput but still show wider p99 on the SQLite endpoints.
- **POST /echo (sync)** — gogo's sync `app.Post` + `Response.Body`
  collects the body on the loop thread and writes it back without a
  goroutine handoff. It is the fastest single-worker echo result here;
  it also has the top 4-worker echo rps. Earlier versions of this
  benchmark used `PostAsync` for /echo and lost to actix here; the
  fair-comparison shape is sync for pure echo.
- **POST /query (PostAsync + SQLite)** — gogo is the fastest
  single-worker result and remains near the top in 4-worker mode,
  because the small body hits the shared-dispatch fast path (zero cgo
  callbacks) and the blocking SQLite query runs on a worker goroutine
  without stalling the loop. uwsjs has the tightest 4-worker p99 here;
  Actix and net/http have higher throughput than before with the
  4-worker cap, but wider tails.
- **Actix Rust** posts strong GET and echo throughput, especially with
  multiple workers, but this run still shows wider SQLite p99 than the
  uWS-backed servers.
- **Fiber** is a very strong pure-Go baseline on this Mac: fast on GET
  and echo, and with consistently tight p99. Its SQLite endpoints still
  trail gogo and uwsjs on throughput.
- **net/http** — the standard-library baseline is much faster on this
  Apple Silicon run than the older Linux-container numbers suggested.
  The 4-worker cap removes the extreme SQLite p99 spikes seen when
  all 8 logical CPUs were used, though the SQLite tail is still wider
  than the uWS-backed servers.

To reproduce:

```sh
export CGO_ENABLED=1
# Pre-build the Actix release binary once (skip if you don't want
# to compare against Rust):
cargo build --release --manifest-path benchmark/actix/Cargo.toml
# On this Apple M3 machine, the default multi-worker cap is 4.
# Override with MULTI_WORKERS=N if your target host has a different shape.
./scripts/bench_wrk.sh
```

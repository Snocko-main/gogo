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

## Table of Contents

- [Install Native Dependencies](#install-native-dependencies)
- [Hello World](#hello-world)
- [Routing](#routing)
  - [Basic routes](#basic-routes)
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
  - [TrustProxy and client IPs](#trustproxy-and-client-ips)
  - [Redirect and open redirects](#redirect-and-open-redirects)
- [Error Handling & Panic Recovery](#error-handling--panic-recovery)
- [Caveats](#caveats)
- [Benchmarking](#benchmarking)

## Install Native Dependencies

The binding expects uWebSockets to be vendored at:

```txt
third_party/uWebSockets
```

One way to set that up:

```sh
sh scripts/bootstrap_uwebsockets.sh
```

Run the same bootstrap step in clean CI jobs before native `-tags gogo`
tests. The script also applies the local uSockets patch in
`patches/uSockets-kqueue-ready-polls.patch` before building `uSockets.a`;
Linux builds use the epoll backend and do not hit the macOS/kqueue bug the
patch fixes, but using the bootstrap script keeps every environment on the
same vendored dependency setup.

Then run an example:

```sh
CGO_ENABLED=1 go run -tags gogo ./examples/hello
curl http://localhost:3000/hello/inon
```

Without `-tags gogo`, the package builds a stub and `NewApp` returns a
clear setup error. This keeps normal Go tooling usable before the native
dependency is present.

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

If the response never changes, register a `gogo.Reply` — it is served
entirely from C++ with no cgo callback per request:

```go
app.Get("/health", gogo.Reply{
    Status:      200,
    ContentType: "application/json",
    Body:        `{"ok":true}`,
})
```

## Routing

### Basic routes

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

Wildcards are supported via uWS pattern syntax:

```go
app.Get("/files/*", serveFile)         // matches /files/anything/here
```

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
```

### Route groups

`App.Group` and `Router.Group` bind middleware and a path prefix to a
subtree of routes. Group-bound middleware composes at registration time, so
there is no per-request URL check.

```go
api := app.Group("/api/v1", authMW, loggerMW)
api.Get("/users", listUsers)               // → GET /api/v1/users
api.Post("/users", createUser)

admin := api.Group("/admin", adminMW)      // composes auth + logger + admin
admin.Delete("/users/:id", deleteUser)     // → DELETE /api/v1/admin/users/:id
```

`App.Mount` is a tiny convenience for building a Router with a callback:

```go
app.Mount("/api/v1", func(r *gogo.Router) {
    r.Use(authMW)
    r.Get("/users", listUsers)
    r.Post("/users", createUser)
})
```

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

// 3) PostAsync — async handler with the body fully collected up to maxBodyBytes.
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

app.Get("/old", func(res *gogo.Response, req *gogo.Request) {
    res.Redirect("/new", 301)
})

// SendFile streams a file from disk with ETag, Last-Modified, and Range
// negotiation. Download forces an attachment Content-Disposition.
app.Get("/files/:name", func(res *gogo.Response, req *gogo.Request) {
    _ = res.SendFile(req, "./public/"+req.Param("name"))
})
app.Get("/download/:name", func(res *gogo.Response, req *gogo.Request) {
    _ = res.Download(req, "./public/"+req.Param("name"), req.Param("name"))
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

### Streaming

`Response.Stream` writes a chunked-encoded response. Available from async
handlers only.

```go
app.GetAsync("/stream", func(res *gogo.Response, req *gogo.Request) {
    err := res.Stream(200, "text/plain", func(w io.Writer) error {
        for i := 0; i < 5; i++ {
            fmt.Fprintf(w, "chunk %d\n", i)
            time.Sleep(200 * time.Millisecond)
        }
        return nil
    })
    if err != nil {
        log.Printf("stream error: %v", err)
    }
})
```

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
— `views/user/profile.tmpl` is rendered as `user/profile`. Bring your own
engine by implementing `TemplateEngine`:

```go
type TemplateEngine interface {
    Render(w *bytes.Buffer, name string, data any) error
}
```

## Request Body Parsing

`Request.BodyParser` deserializes the body into a struct based on
Content-Type. Supported: `application/json`, `application/x-www-form-urlencoded`,
`multipart/form-data` (value parts only).

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

Multipart value parts parsed by `BodyParser` and `ParseMultipart` are capped by
`GetDefaultMultipartPartLimit()` (8 MiB by default). Override the process default
with `SetDefaultMultipartPartLimit(n)` before registering handlers, or pass
`MultipartOptions{MaxPartBytes: n}` to multipart APIs for route-specific limits.
The older `DefaultMultipartPartLimit = n` assignment style still works during
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
res.SetCookieSigned(gogo.Cookie{Name: "uid", Value: "42"}, "my-secret")
uid, ok := req.CookieSigned("uid", "my-secret")
if !ok {
    res.Send(401, "text/plain", "bad cookie\n")
    return
}
_ = uid
```

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
the middleware to routes whose URL starts with that prefix at request
time. Trailing `/*` or `/**` is stripped — `/api/*` and `/api` mean the
same thing.

### Async middleware

`AsyncMiddleware` wraps `GetAsync` / `PostAsync` handlers and runs on the
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
middleware (metrics, audit, tracing); use the inline form for state-
commit middleware where panic = "don't persist".

**Built-in middleware** using this hook: `mw.NewSession` (commits
state inline post-handler — panics are intentionally NOT persisted),
`mw.NewMetrics` (records the final status / duration even on panic
via the defer form). Custom middleware following either shape should
do the same.

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
    Users: map[string]string{"alice": "secret"},
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
app.Use(mw.CSRF(mw.CSRFOptions{Secret: []byte("32-byte-secret-...")}))

// Prometheus-flavored metrics with /metrics handler.
metrics := mw.NewMetrics()
app.Use(metrics.Middleware())
app.Get("/metrics", metrics.Handler())
```

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
        rand.Read(buf[:])
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

Set `MaxBuckets: -1` to disable the cap entirely (tests only — re-introduces
the OOM risk).

For multi-instance fleets, plug a Redis-backed `RateLimitStore` instead —
the cap is irrelevant when state lives in Redis, and counters stay
consistent across instances:

```go
app.Use(mw.RateLimit(mw.RateLimitOptions{
    Max:    100,
    Window: time.Minute,
    Store:  &MyRedisStore{client: redisClient},   // implement RateLimitStore
}))
```

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
sessions; pass a negative value to disable (not recommended outside
tests).

For multi-instance fleets, implement `SessionStore` (and `RateLimitStore`)
on top of Redis / Memcache.

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

    // PostAsync — body is pre-collected, BodyParser deserializes JSON / form.
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

SSE is async-only — register the route via `GetAsync` or `PostAsync` and
call `res.SSE(...)`. The framework installs the right headers
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
                    return err               // client disconnected
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

### Echo server

```go
app.WebSocket("/ws", gogo.WebSocketBehavior{
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

### Pub/Sub

uWebSockets has built-in pub/sub. Subscribe inside `Open`, publish anywhere
on the loop with `ws.Publish` or anywhere off the loop with `app.Publish`.

```go
app.WebSocket("/chat", gogo.WebSocketBehavior{
    Open: func(ws *gogo.WebSocket) {
        ws.Subscribe("room.general")
    },
    Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
        ws.Publish("room.general", msg, op)      // broadcast to subscribers
    },
})

// From a worker goroutine — must use App.Publish (loop-safe, copies bytes).
go func() {
    for {
        time.Sleep(10 * time.Second)
        app.Publish("room.general", []byte("server tick"), gogo.Text)
    }
}()
```

For bursty fan-out, batch publishes save one cgo crossing + one mutex per
message:

```go
app.PublishBatch([]gogo.PublishMessage{
    {Topic: "room.general", Message: []byte("hi 1"), OpCode: gogo.Text},
    {Topic: "room.general", Message: []byte("hi 2"), OpCode: gogo.Text},
    {Topic: "alerts",       Message: []byte("ping"), OpCode: gogo.Text},
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
- Empty `AllowedOrigins` + `AllowMissingOrigin=false` → reject every
  request that arrives with an Origin header — i.e. all browsers.
  Set `AllowedOrigins` explicitly before going to production.
- `AllowMissingOrigin=true` → permit handshakes without an Origin
  (CLI tools like `websocat`). Safe IF you have no browser clients
  on this endpoint.
- `AllowedOrigins: []string{"*"}` → accept any origin. Opt-in for
  public APIs that don't rely on ambient cookie auth.

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
            return p.SaveAt("./uploads/" + filepath.Base(p.FileName))
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
to the same port via `SO_REUSEPORT`. The kernel load-balances connections
across the listening sockets.

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
    log.Println("draining workers")
    handle.Shutdown()
    handle.Wait()
}
```

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

sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
<-sigCh

app.ShutdownGracefully(30 * time.Second)   // wait for in-flight requests
// Run() returns once the loop has drained.
```

`Shutdown` closes the listen socket and drains the loop; `Close` frees
native resources. Both are safe from any goroutine.

## Configuration

```go
app, _ := gogo.NewApp(gogo.Config{
    BodyLimit:       4 << 20,          // 4 MiB; oversize -> 413
    BodyReadTimeout: 30 * time.Second, // slow body upload deadline
    BindAddr:        "127.0.0.1",      // localhost only
    CapturePeerIP:   true,             // populate req.IP() on async paths
    TrustProxy:      true,             // honor X-Forwarded-*
})
```

| Field             | Default | Notes                                                   |
| ----------------- | ------- | ------------------------------------------------------- |
| `BodyLimit`       | 4 MiB   | Reject Content-Length > limit with 413 on the C++ side  |
| `BodyReadTimeout` | 30s     | Deadline for `Response.Body` to finish reading the body |
| `BindAddr`        | `""`    | Empty = all interfaces (`0.0.0.0`)                      |
| `CapturePeerIP`   | `false` | Snapshot peer IP for async / shared-dispatch paths      |
| `TrustProxy`      | `false` | Honor `X-Forwarded-*` in `Protocol()`/`Secure()`/`IPs()` |

`BodyReadTimeout` protects `Response.Body` users from slow body uploads that
drip bytes forever without exceeding `BodyLimit`. Keep the 30s default for
ordinary APIs, lower it for small JSON-only endpoints, raise it for legitimate
large uploads, or set a negative value only when intentionally disabling the
deadline for trusted traffic/tests.

### TrustProxy and client IPs

When `TrustProxy` is **off** (the default), the framework treats every
`X-Forwarded-*` header as untrusted attacker input:

- `req.Protocol()` / `req.Secure()` ignore `X-Forwarded-Proto`.
- `req.IPs()` returns `nil` (the X-Forwarded-For chain is not exposed).
- `req.IP()` returns the immediate TCP peer — the proxy itself if you have one.

Turn `TrustProxy` **on** only when the server actually sits behind a
trusted reverse proxy (nginx, an L7 load balancer, a CDN with origin
shielding). Once on, the leftmost entry in `req.IPs()` is the client IP
as reported by your proxy chain.

```go
// Behind a CDN — opt in so req.IPs() returns the real client.
app, _ := gogo.NewApp(gogo.Config{TrustProxy: true})

app.Get("/whoami", func(res *gogo.Response, req *gogo.Request) {
    ips := req.IPs()
    client := req.IP()
    if len(ips) > 0 {
        client = ips[0]                      // leftmost = original client
    }
    res.Send(200, "text/plain", "you are "+client+"\n")
})
```

Internet-facing servers that read X-Forwarded-For anyway (against
recommendation) must call `req.Header("x-forwarded-for")` and parse it
themselves, accepting that any client can forge the value.

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
still available and keeps the server alive.

Custom 404 / 405:

```go
app.NotFound(func(res *gogo.Response, req *gogo.Request) {
    res.JSON(404, map[string]string{"error": "not found", "path": req.URL()})
})

app.MethodNotAllowed(func(res *gogo.Response, req *gogo.Request) {
    allow := strings.Join(app.AllowedMethods(req.URL()), ", ")
    res.Header("Allow", allow)
    res.JSON(405, map[string]string{"error": "method not allowed"})
})
```

## Caveats

- `*Request` and `*Response` from sync handlers are valid only during the
  handler callback. Async handlers receive a snapshot Request that survives
  past the C-side lifetime.
- Sync handlers run on the uWS loop thread — never block them. Use
  `GetAsync` / `PostAsync` for anything that does IO.
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
- `req.IPs()` returns `nil` unless `Config.TrustProxy=true` — see
  [TrustProxy and client IPs](#trustproxy-and-client-ips).
- `res.Redirect` does not protect against open redirects — caller must
  allow-list targets. See [Redirect and open redirects](#redirect-and-open-redirects).
- `mw.RequestID()` emits 32-hex-char (128-bit) IDs by default — see
  [RequestID — 128-bit IDs](#requestid--128-bit-ids) if you have a
  downstream parser that hard-codes 16-char IDs.
- `mw.RateLimit()` caps the in-memory store at 100k buckets — raise via
  `MaxBuckets` or plug a Redis store for high-cardinality keys. See
  [RateLimit memory cap](#ratelimit-memory-cap).
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
- [`examples/multicore`](examples/multicore) — `RunMultiCore` + `/metrics` + graceful shutdown

## Why There Is a C++ Bridge

uWebSockets is not a C library. Its public API is C++ template-heavy, so cgo
cannot call it directly in a pleasant or stable way. The `uws_bridge.cpp` file
turns the parts Go needs into a small C ABI.

## Benchmarking

There are five comparable HTTP benchmark servers:

- `benchmark/gogo`: this binding
- `benchmark/nethttp`: Go standard library `net/http`
- `benchmark/fiber`: gofiber/fiber on fasthttp
- `benchmark/node-uwebsockets`: uWebSockets.js on Node
- `benchmark/bun-elysia`: Elysia on Bun

`scripts/bench_wrk.sh` starts each server, hits `/hello`, `/hello/:name`,
and `/db` with `wrk`, then tears it down. See the script header for the
env knobs.

### Results

Single-worker, median req/s across `wrk -t {1,2,4,8} -c 500 -d 15s`,
Apple M3 8-core, macOS 26.

| framework  |       `/hello` | `/hello/:name` |          `/db` |
|------------|---------------:|---------------:|---------------:|
| **gogo**   |    **296,793** |    **273,654** |    **192,319** |
| uwsjs      |        248,810 |        246,972 |        167,335 |
| fiber      |        243,596 |        227,940 |         98,999 |
| bun+elysia |        210,421 |        201,456 |        131,955 |
| net/http   |        148,165 |        142,569 |         74,639 |

`/db` reads one row from a 1000-row SQLite table with a random id —
exercises the framework + driver, not just the HTTP layer.

To reproduce:

```sh
export CGO_ENABLED=1
./scripts/bench_wrk.sh
```

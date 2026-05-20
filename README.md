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
  - [Bundled middleware](#bundled-middleware)
  - [Logger, Request ID, CORS, Rate Limiting, JWT](#bundled-middleware)
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

### Bundled middleware

The `middleware` subpackage ships production-ready middleware:

```go
import (
    gogo "github.com/Snocko-main/gogo"
    mw "github.com/Snocko-main/gogo/middleware"
)

app.Use(mw.RequestID())                              // X-Request-ID
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
    Max:    100,
    Window: time.Minute,                              // 100 req / IP / minute
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

### Sessions

```go
app.Use(mw.NewSession(mw.SessionOptions{
    Secret:       []byte(os.Getenv("SESSION_SECRET")),
    Store:        mw.NewMemorySessionStore(),    // swap for Redis in prod
    TTL:          24 * time.Hour,
    CookieSecure: true,
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

Use the `Upgrade` callback to inspect request headers, negotiate a
subprotocol, and attach per-connection user data:

```go
app.WebSocket("/ws", gogo.WebSocketBehavior{
    Upgrade: func(ctx *gogo.UpgradeContext) {
        // Authenticate at handshake time.
        token := ctx.QueryParam("token")
        if token == "" {
            ctx.Reject(401, "missing token")
            return
        }
        user, err := db.LoadUserByToken(token)
        if err != nil {
            ctx.Reject(401, "bad token")
            return
        }

        // Pick a subprotocol from the client's offer.
        chosen := ""
        for _, p := range ctx.Protocols() {
            if p == "chat.v1" {
                chosen = p
                break
            }
        }
        ctx.SetUserData(user)
        ctx.Accept(chosen)
    },
    Open: func(ws *gogo.WebSocket) {
        u := ws.UserData().(*User)
        ws.SendText("welcome, " + u.Name + "\n")
        ws.Subscribe("user." + strconv.Itoa(u.ID))
    },
    Message: handleWSMessage,
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
    BodyLimit:     4 << 20,         // 4 MiB; oversize → 413
    BindAddr:      "127.0.0.1",     // localhost only
    CapturePeerIP: true,            // populate req.IP() on async paths
    TrustProxy:    true,            // honor X-Forwarded-*
})
```

| Field           | Default | Notes                                                   |
| --------------- | ------- | ------------------------------------------------------- |
| `BodyLimit`     | 4 MiB   | Reject Content-Length > limit with 413 on the C++ side  |
| `BindAddr`      | `""`    | Empty = all interfaces (`0.0.0.0`)                      |
| `CapturePeerIP` | `false` | Snapshot peer IP for async / shared-dispatch paths      |
| `TrustProxy`    | `false` | Honor `X-Forwarded-Proto` in `req.Protocol()`/`Secure()` |

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
- TLS / HTTP/2 are out of scope here; terminate at a reverse proxy
  (nginx, Caddy, an L7 load balancer).

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

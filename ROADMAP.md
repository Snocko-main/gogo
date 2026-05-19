# gogo Roadmap — Security Fixes + Path to Beat fiber/express

This document is the result of a fresh security + feature-parity audit done
after PR #1 (middleware bypass + Router/Group + race fixes) landed. Issues
are grouped by priority, each with file/line references, effort estimate,
and a one-line "fix" sketch.

---

## Status

### Shipped in PR #1 (merged into `feature/optimize-io` as `b7ba9e9`)

- Middleware bypass via parametric / wildcard routes — fixed.
- `App.Group` / `Router` API with nested groups, sync + async middleware.
- 15 new integration tests covering bypass + Group lifecycle.
- C++ bridge pre-fetches URL; `Request.URL()` is now lazy + cgo-free for
  sync routes.
- Two pre-existing data races repaired (`asyncSendShared` ring publish UAF
  and the `Response` wrapper double-recycle).
- `Response` wrapper lifecycle is now an `atomic.Int32` refcount; the
  decrement that drops the count to zero is the unique recycle point.

Race detector clean on `TestStressShortBursts -race -count=10`. 30 ×
back-to-back stress runs without `-race`: 0 crashes. Benchmark:
gogo `+4–14%` RPS over fiber on `/api/plain`, `/api/hello/:name`, `/db`;
`/db` p99 is **4×** better (2 ms vs 8.4 ms).

---

## P0 — Security fixes (must ship in 1.0)

These are open security issues confirmed in the audit. Each is small and
should land before any 1.0 release.

### P0-1. Validate `Reply.ContentType` for CRLF injection

`gogo/types.go:432-435` — `app.Get(pattern, Reply{ContentType: …})` passes
the content type straight into the C++ static-response slot without ever
running it through `validateHeaderValue`. A programmer who plumbs user
input into `Reply.ContentType` (rare but possible — version reflection
endpoints, content-negotiated static responses) opens a response-splitting
hole.

**Fix:** call `validateHeaderValue("Content-Type", v.ContentType)` in the
`Reply` branch of `App.Get`. Same treatment for the future `Put`/`Patch`/…
helpers. Effort: ~5 lines.

### P0-2. Default body limit on `Post` / `Any`

`gogo/types.go:577-586` — only `PostAsync` enforces a body cap.
`app.Post(pattern, handler)` and `app.Any(pattern, handler)` let the user
call `res.Body(maxBytes, …)` themselves; if they forget, uWS will hand
through unlimited body bytes to user code. Memory-exhaustion DoS.

**Fix:** add `App.Config{BodyLimit: N}` (default 4 MiB, matching fiber)
and enforce it at the C++ side before dispatch. Or at minimum, panic at
registration if `Post`/`Any` is used without a `Body(...)` call in the
handler. Effort: medium — touches bridge + types.

### P0-3. Default panic logging

`gogo/types.go:104-125` — `SetPanicHandler(nil)` means panics in user
code are silently swallowed. In stress + abort scenarios the framework
emits a best-effort 500 to the client, but the server operator never sees
the panic. Bugs and attack-triggered panics go invisible in production.

**Fix:** default the panic handler to `log.Printf("gogo: panic: %v\n%s",
v, debug.Stack())` and document that `SetPanicHandler(nil)` is opt-out.
Effort: ~10 lines.

### P0-4. `JSON()` error leaks internal detail

`gogo/types.go:1130-1138` — when `json.Marshal` fails, the marshal error
message goes back to the client as plain text. Marshal failures only
happen for programmer-shaped values (channels, funcs, cycles), but Go
runtime messages can include type names and package paths.

**Fix:** log server-side, return `500 Internal Server Error` with a
generic body. Effort: trivial.

### P0-5. WebSocket has no max-payload / idle config

`gogo/types.go:152-157` — `WebSocketBehavior` exposes only `Open`,
`Message`, `Close`. uWS itself supports `maxPayloadLength`,
`idleTimeout`, `maxBackpressure`, `sendPingsAutomatically` — all of those
are security-relevant and should be configurable. Without them a single
client can pin one server thread sending a multi-megabyte frame.

**Fix:** widen `WebSocketBehavior` with optional fields; forward to the
C++ `uWS::App::WebSocketBehavior` template parameters. Effort: medium —
bridge ABI change.

### P0-6. `Listen` is `0.0.0.0`-only

`gogo/types.go:878-881` — no way to bind to `127.0.0.1` or a specific
interface. Forces operators to firewall externally for what should be a
one-line config.

**Fix:** `Listen(addr string, port int)` or `ListenAddr("127.0.0.1:3000")`,
forwarded to `uWS::App::listen(host, port, …)`. Effort: small — bridge
signature change.

### P0-7. No per-request / read / write timeout

uWS supports per-socket idle timeout but gogo doesn't expose it. No
slowloris guard. A handful of slow clients holding header / body
connections can DoS a single-threaded loop.

**Fix:** `App.Config{IdleTimeout, ReadTimeout, WriteTimeout}` plumbed to
uWS. Effort: medium.

### P0-8. `headersAll()` has no upper bound

`gogo/native_enabled.go:662-670` — the headersAll() Go side allocates
exactly what uWS reports. uWS has internal limits (~8 KiB by default)
but they are not surfaced as a gogo-level cap and the snapshot path
already enforces 4 KiB. Mismatched limits between sync and async paths
make it hard to reason about.

**Fix:** explicit 4 KiB cap on the sync path too; reject (413/431) at
the C++ side. Effort: small.

### P0-9. Cookie value quoted-string handling

`gogo/types.go:1261-1285` — `Cookie(name)` returns `pair[eq+1:]` verbatim.
RFC 6265 allows the value to be wrapped in `"..."`; most frameworks strip
those quotes. Not a security bug per se but a correctness gap that bites
session-cookie interop.

**Fix:** strip a single pair of surrounding double quotes before return.
Effort: trivial.

---

## P1 — Production essentials (ship in 1.0 or shortly after)

Without these, gogo is hard to deploy in real production environments.

### P1-1. TLS / HTTPS — **Won't Fix**

**Status: deferred indefinitely.** Production deployments terminate TLS
at a gateway (Kubernetes Ingress, AWS ALB / NLB, GCP LB, Cloudflare /
Fastly edge, or self-hosted nginx / Envoy / Caddy / HAProxy). Those
edge proxies do cert automation (Let's Encrypt / ACME), SNI, OCSP
stapling, HTTP/2 + HTTP/3, and crypto throughput optimization better
than an in-process implementation can.

Forwarding to a backend over plain HTTP on a trusted network is the
dominant pattern. fiber and express are deployed this way the vast
majority of the time. gogo is the *upstream* — it should be excellent
at that and not duplicate edge proxy work.

The framework supports the gateway flow:

- `Config.TrustProxy` (added in this same PR) — opt-in flag that lets
  `req.Protocol()` and `req.Secure()` honor `X-Forwarded-Proto` from a
  trusted gateway. Leave OFF when directly internet-facing so clients
  can't spoof.
- `req.IPs()` already parses `X-Forwarded-For` (P1-6).
- `req.Hostname()` reads the Host header so virtual hosting works.

If a future deployment scenario genuinely needs in-process TLS
(no gateway available, certain embedded use cases) this entry will
reopen. Until then the cost — `~600-1500` lines of templated C++
refactor plus ongoing maintenance burden (cert reload, SNI, TLS 1.3
ciphers, OCSP) — buys less value than other roadmap items.

**Example nginx config in front of gogo:**

```nginx
server {
    listen 443 ssl http2;
    server_name api.example.com;
    ssl_certificate     /etc/letsencrypt/live/api.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Enable `Config{TrustProxy: true}` in the app so `req.Secure()` returns
true and `req.IP()` surfaces the real client IP.

### P1-2. App-level `Config` struct

fiber's `fiber.Config{...}` is a single argument with ~30 knobs. gogo
currently has nothing. We'd need at least:

- `BodyLimit`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout`
- `BindAddr`
- `ErrorHandler` (replaces JSON-leak default)
- `NotFoundHandler` (currently no 404 customization)
- `MethodNotAllowedHandler`
- `DisableStartupMessage`

**Plan:** add `type Config struct { ... }` and `NewApp(Config)` (keep the
zero-arg form). Effort: medium — touches every entrypoint.

### P1-3. Graceful shutdown timeout

`App.Shutdown()` does not block; `Run()` returns when in-flight responses
drain. There is no upper bound — a misbehaving handler can pin the
process forever. Need `ShutdownTimeout(d time.Duration)` that forces
close.

**Effort:** small — bridge `app->close()` already exists; just need a
loop-side timer.

### P1-4. Hooks: `OnRequest`, `OnResponse`, `OnShutdown`, `OnError`

Observability needs first-class hook points. Express has `app.use` for
this; fiber has `app.Hooks().OnX`. Bundle into Config or expose as
methods.

**Effort:** medium.

### P1-5. `NotFoundHandler` / `MethodNotAllowedHandler`

uWS returns a hard-coded 404; we can't customize the body, log the miss,
or do canary routing. Both routes need a registration entry point and
typed handler signature.

**Effort:** small — add `app.NotFound(handler)` and an internal catch-all.

### P1-6. Request-introspection helpers

Most fiber/express migrants reach for these immediately:

- `req.IP()` — peer address.
- `req.IPs()` — parsed `X-Forwarded-For` chain.
- `req.Hostname()` — `Host` header.
- `req.Protocol()` / `req.Secure()` — http vs https.
- `req.Get(key)` — alias for `req.Header(key)`.

**Effort:** small — uWS already has these on the C++ side; just expose.

### P1-7. `Redirect`, `SendFile`, `Download`

The three response helpers everyone reaches for. `Redirect` is trivial
(just a Location header + status). `SendFile` + `Download` need
streaming + `Content-Type` sniffing + range requests; medium effort.

---

## P2 — Feature parity with fiber / express

These remove migration friction. Each is medium effort on its own; total
is large.

### Routing

- HTTP method helpers: `Put`, `Patch`, `Delete`, `Options`, `Head`.
- Route grouping already shipped via `Group`.
- Sub-apps / mounting (`app.Mount("/api", subApp)`).
- Regex / typed-param routes (`:id<int>`).
- Reverse routing / named routes for redirects.

### Request

- `BodyParser(out)` auto-binds JSON / form / multipart based on
  Content-Type. Use reflection or codegen.
- Multipart upload helpers — iterate parts, save to disk, stream to S3.
- Query-param type conversion (`ParamInt`, `QueryParamInt`, default
  values).
- Cookie signing (HMAC) helpers.

### Response

- Streaming chunked writer.
- `JSONP`, `Render` (templates — html/template + pluggable engines).
- `res.Append(key, val)` for multi-value headers.

### Middleware ecosystem (bundle as `gogo/middleware/*`)

- CORS. **DONE** — `middleware.CORS` (PR #8).
- Compression (gzip/brotli) — uWS has native compression for WS, HTTP
  needs Go layer; consider C++ side gzip. **DONE (gzip + deflate)** —
  `middleware.Compress` via the new `Response.SetBodyEncoder` hook.
  Brotli left to callers that import a brotli encoder and install
  their own encoder; sync handlers only (async path bypasses encoder).
- Helmet-style security headers (HSTS, X-Frame-Options, CSP).
  **DONE** — `middleware.Helmet`.
- CSRF (sync + signed cookie). **DONE** — `middleware.CSRF`
  (double-submit cookie with HMAC-bound tokens).
- Rate limiter (in-memory + Redis). **DONE (in-memory)** —
  `middleware.RateLimit` with a pluggable `RateLimitStore` interface;
  Redis backend is a future Store implementation.
- Request ID. **DONE** — `middleware.RequestID` (PR #8).
- Logger (bundle the example from `examples/authmw`). **DONE** —
  `middleware.Logger` (PR #8).
- Basic Auth, JWT verification. **DONE** — `middleware.BasicAuth`
  and `middleware.JWT` (HS256/384/512; asymmetric algorithms left
  out by design — wire them with a custom middleware).
- Session middleware (cookie, server-side store). **DONE** —
  `middleware.NewSession` + `*middleware.Session` handle, with a
  built-in `MemorySessionStore` and a `SessionStore` interface for
  Redis / SQL backends.

### WebSocket

- Pub/sub: `ws.Subscribe(topic)`, `ws.Publish(topic, msg)`, `ws.Unsubscribe`.
  uWS has this natively, just expose.
- Subprotocols / upgrade headers.

### Testing

- `App.Test(httpReq) (*httptest.ResponseRecorder, error)` — in-process
  dispatch without a real socket. Requires a "fake" `responseNative`
  shim.
- `net/http.Handler` adapter — register an existing `http.Handler` as a
  gogo route. Lets users migrate one route at a time.

---

## P3 — Differentiator opportunities (beat fiber / express)

These are things uWS lets gogo do that fiber/fasthttp can't easily match.
Each is a marketing surface.

### D-1. Pre-cache *everything* on the C++ side

PR #1 did this for URL. Extending to `method`, `query`, `parameter[i]`,
and headers eliminates cgo from `req.*` reads in the typical handler.
Theoretical: gogo's hot path becomes a single cgo crossing per request
(in) plus one for the response (out). Lower latency floor than fiber's
pure-Go fasthttp.

**Plan:** pass method + query + a parsed header map into
`uwsgoHandleHTTP` alongside URL. Cache lazily. Bridge ABI change.

### D-2. Native WebSocket pub/sub

Real-time apps love pub/sub. uWS has it: O(log n) topic dispatch on the
loop thread. Expose as a flat API on `*WebSocket`, no Go-side fanout
needed. fiber's WS doesn't have a built-in pub/sub.

### D-3. Multi-core polish

`RunMultiCore` works via `SO_REUSEPORT`. Document tuning knobs (pinning,
GOMAXPROCS, per-worker DB pools). Add a `RunMultiCore` example with
metrics + graceful shutdown.

### D-4. C++-side compression

For static `Reply` bodies, pre-compress at registration into gzip +
brotli + identity variants, then `Accept-Encoding`-negotiate on the C++
side — zero Go work for cached static content. Same trick fiber can't do
because compression is in Go middleware.

### D-5. Native backpressure

uWS exposes a "tryEnd" path that returns false on backpressure. Surface
it so user code can throttle DB reads when the network is the
bottleneck — built-in flow control instead of just dropping data.

### D-6. Zero-cgo shared dispatch for more methods

`GetAsync` uses a zero-cgo shared-memory ring. Extend the same path to
`PostAsync` for small bodies — and to `Get` / `Post` sync handlers when
the handler is "pure" (declared as `Reply{}`-like).

### D-7. Built-in observability

OpenTelemetry hooks, /metrics endpoint, automatic latency histograms.
Operators love this and most Go frameworks ship it as an afterthought.

---

## Perf tune-ups (deferred follow-ups)

Optimizations identified during reviews but not yet acted on. Each is
scoped tightly enough to land as its own small PR with a focused
benchmark, separately from the feature work that surfaced it.

### T-1. Batch `flushPendingHeaders` into a single cgo crossing

PR #8 buffers `Response.Header` calls Go-side so middleware doesn't
race uWS's auto-`200`-on-first-`writeHeader` quirk. Today the flush
loop calls `r.inner.header(...)` once per buffered header — one cgo
crossing each. With CORS adding 3-4 headers per request that is
3-4 × ~80 ns = ~300 ns/req of avoidable cgo overhead.

**Plan:** add `uwsgo_res_write_headers_batch(res, packed_buf, count)`
to the C bridge. Go serializes pendingHeaders into a single
`"key1\0val1\0key2\0val2\0..."` buffer and crosses once; C++ iterates
inside the same call. Saves `(N-1)` cgo crossings per request with
N buffered headers.

**Expected impact:** ~3 % CPU on a CORS-enabled `/plain`-ish route at
100 k RPS, scaling linearly with RPS. The current bench harness
(`/plain`, `/db` without middleware) won't show it — needs a
CORS-on `/plain` bench to measure.

**Effort:** small (one new bridge function, one Go helper).

### T-2. Pre-lowercase CORS `AllowOrigins` at construction

`middleware.matchOrigin` uses `strings.EqualFold` for case-insensitive
match. Cheaper to lowercase the configured patterns once at
construction and `strings.ToLower(origin)` once per request, then
direct `==`. Saves ~30-50 ns/req with CORS enabled.

**Effort:** trivial.

### T-3. Faster `RequestID` generator option

`crypto/rand.Read` is ~1 µs per call — the dominant cost of
`middleware.RequestID` on requests that don't carry an incoming
header. Drop-in alternative: seed a `math/rand/v2.ChaCha8` from
crypto/rand once, then 50 ns per ID. Still strong enough for tracing
IDs that aren't security tokens.

**Plan:** add `middleware.FastRequestIDGenerator` (or document the
recipe under `RequestIDOptions.Generator`) so users can opt-in
without abandoning the safe default.

**Effort:** small.

### T-4. Pre-grow `Response.pendingHeaders` capacity

Each Response wrapper's `pendingHeaders` slice starts at `cap=0`;
typical middleware (CORS) appends 3-4 entries, triggering 2-3
small reallocs on the first request through that wrapper. The
backing array is then retained across `sync.Pool` recycles so this is
amortized — but a one-time `make([]responseHeader, 0, 8)` on
wrapper creation eliminates even the first-request cost.

**Effort:** trivial. Marginal impact (mostly amortized already).

### T-5. CORS Origin via header prefetch (shared-dispatch only)

Sync-path `req.Header("origin")` is a cgo crossing per request even
when no Origin is sent. Pre-caching `Origin` alongside the existing
prefetched fields (method / URL / query / first-4 params) would
eliminate it. Bridge ABI change — defer until D-1 lands, since that
work already touches the same prefetch path.

**Effort:** medium (bridge ABI bump). Coordinates with D-1.

---

## Performance targets (1.0)

Hardware reference: 4 vCPU @ 2.10 GHz, single-thread wrk.

Current (PR #1):

| Route                    | gogo            | fiber           | edge   |
| ------------------------ | --------------- | --------------- | ------ |
| `/api/plain` (literal)   | 161,241 RPS     | 154,415 RPS     | +4.4%  |
| `/api/hello/:name`       | 162,665 RPS     | 155,051 RPS     | +4.9%  |
| `/db` (SQLite)           |  87,740 RPS     |  76,788 RPS     | +14.3% |
| `/db` p99 latency        |   2.0 ms        |   8.4 ms        | 4.1×   |

1.0 target after D-1 (pre-cache everything):

- `+8–12%` on `/api/plain` over fiber.
- Maintain `+12%+` on IO-bound routes like `/db`.
- Multi-core scaling: linear up to 4 cores via `RunMultiCore`.

---

## Suggested release plan

### 0.x — security hardening (now → 2 weeks)

P0-1 through P0-9. Each is small. Land as a single PR per group:

- **PR A**: P0-1 + P0-2 + P0-4 + P0-9 (header / body / JSON / cookie
  hardening).
- **PR B**: P0-3 default panic logging + tests.
- **PR C**: P0-5 WebSocket config widening.
- **PR D**: P0-6 bind addr + P0-7 timeouts + P0-8 header cap (Config
  struct begins here).

### 1.0 — production-ready (2–6 weeks)

P1-2 through P1-7. P1-1 (TLS) is **won't fix** — see the section above.

- **PR E**: `App.Config{}` baseline (P1-2) + graceful shutdown timeout
  (P1-3) + hooks (P1-4).
- **PR F**: Not-found / method-not-allowed handlers (P1-5).
- **PR G**: Request introspection helpers (P1-6).
- **PR H**: Redirect / SendFile / Download (P1-7).
- **PR I**: HTTP method helpers (`Put`/`Patch`/`Delete`/`Options`/`Head`)
  + `Config.TrustProxy` + `req.QueryInt` / `ParamInt` / `QueryBool` +
  `res.Append` — quick wins before P2.

### 1.x — feature parity (1–3 months)

P2 split into routing / request / response / middleware / WebSocket /
testing PRs. Each PR is a `gogo/middleware/*` subpackage or a
`Router` method addition; isolated reviews.

### 2.0 — differentiators (3–6 months)

D-1 pre-cache everything (bridge ABI bump → 2.0).
D-2 WS pub/sub.
D-3 multi-core polish.
D-4 C++-side compression.
D-5 backpressure.
D-6 zero-cgo for more methods.
D-7 observability bundle.

---

## Out of scope for now

- HTTP/2, HTTP/3 — uWS has SSL support but HTTP/2 is uneven and HTTP/3
  needs lsquic. Reasonable for 3.0 once 1.x and 2.0 are stable.
- gRPC. Different protocol surface; better as a sibling package.
- Cluster mode beyond `SO_REUSEPORT`. Single-node multi-core is enough
  for the common case.

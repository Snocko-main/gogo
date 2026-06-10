# Operations Guide

This page covers the v0.8 operations basics for running gogo behind a proxy,
sizing a deployment, and getting useful production logs. It is intentionally
limited to behavior the framework exposes today.

gogo serves plaintext HTTP. Terminate TLS, HTTP/2, HTTP/3, compression policy,
and public edge concerns at nginx, Caddy, a CDN, or an L7 load balancer.

## Baseline App Shape

When a local or private reverse proxy is the only public entry point, bind gogo
to the loopback interface and trust only that immediate proxy:

```go
app, err := gogo.NewApp(gogo.Config{
	BindAddr:        "127.0.0.1",
	BodyLimit:       8 << 20,
	BodyReadTimeout: 30 * time.Second,
	TrustedProxies:  []string{"127.0.0.1", "::1"},
})
if err != nil {
	log.Fatal(err)
}
```

For a private load balancer, replace the loopback entries with the balancer's
private IPs or CIDR ranges, for example `10.0.0.0/8`.

Current boundary: `RunMultiCore` does not accept `Config`. Each worker uses the
zero-value app config, so app-scoped settings such as `BindAddr`, `BodyLimit`,
`BodyReadTimeout`, `CapturePeerIP`, `TrustProxy`, `TrustedProxies`, and custom
JSON codecs cannot be supplied through that helper yet. Use `NewApp(Config)`
when handlers depend on those settings, or keep proxy trust and header
normalization entirely at the layer in front of the multicore process.

## Proxy Trust Model

Prefer `Config.TrustedProxies` over `Config.TrustProxy` in production.
`TrustedProxies` accepts single IPs and CIDR ranges. When the immediate TCP
peer matches, gogo helpers trust the proxy-supplied headers:

- `req.Protocol()` and `req.Secure()` honor `X-Forwarded-Proto`.
- `req.IPs()` returns normalized `X-Forwarded-For` entries in order.
- `req.IP()` still returns the immediate TCP peer, usually the proxy itself.

The leftmost `req.IPs()` value is the original client as reported by your
trusted proxy chain:

```go
clientIP := req.IP()
if ips := req.IPs(); len(ips) > 0 {
	clientIP = ips[0]
}
```

`TrustedProxies` also enables `CapturePeerIP` automatically so async and
shared-dispatch routes can decide whether the immediate peer is trusted before
using forwarded headers.

The first trusted edge must strip or overwrite untrusted `X-Forwarded-For`,
`X-Forwarded-Proto`, `Forwarded`, and `X-Real-IP` headers before forwarding to
gogo. gogo's helpers currently consume `X-Forwarded-For` and
`X-Forwarded-Proto`; the other headers should still be cleaned so application
code and downstream tools do not accidentally trust spoofed values.

Use `TrustProxy: true` only when every possible immediate peer is trusted by
network topology. Do not enable it on a socket that untrusted clients can reach
directly.

## nginx

For nginx as the public edge on the same host, overwrite forwarded headers with
values nginx observed directly. Do not use `$proxy_add_x_forwarded_for` at the
first public edge unless an earlier trusted proxy has already sanitized the
incoming chain.

```nginx
# http {}
map $http_upgrade $connection_upgrade {
    default upgrade;
    ""      "";
}

server {
    listen 443 ssl http2;
    server_name api.example.com;

    client_max_body_size 8m;
    proxy_read_timeout 65s;
    proxy_send_timeout 65s;

    location / {
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header Forwarded "";
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_buffering off;
        proxy_pass http://127.0.0.1:3000;
    }
}
```

Pair that with:

```go
app, err := gogo.NewApp(gogo.Config{
	BindAddr:       "127.0.0.1",
	TrustedProxies: []string{"127.0.0.1", "::1"},
	BodyLimit:      8 << 20,
})
```

If nginx sits behind a CDN or load balancer, configure nginx's real-IP module
for only those trusted upstream ranges first, then forward the normalized
client IP to gogo. Keep the gogo process unreachable from the public internet.

## Caddy

Caddy's `reverse_proxy` handles WebSocket upgrades automatically. On Caddy
v2.10+, `request_body` can align edge and app body limits; on older releases,
enforce the same cap at another edge layer. The important parts for gogo are
body-size alignment and explicit forwarded-header policy:

```caddyfile
api.example.com {
	request_body {
		max_size 8MB
	}

	reverse_proxy 127.0.0.1:3000 {
		header_up Host {host}
		header_up X-Forwarded-For {remote_host}
		header_up X-Forwarded-Proto {scheme}
		header_up X-Real-IP {remote_host}
		header_up -Forwarded
	}
}
```

Use the same loopback `TrustedProxies` config as nginx. If Caddy is behind
another trusted edge, configure Caddy's trusted-proxy behavior there and only
forward normalized headers to gogo.

## Cloudflare

The safest shape is:

```txt
client -> Cloudflare -> origin proxy -> gogo
```

In that shape, list the origin proxy as `TrustedProxies`, not every Cloudflare
range, because the origin proxy is the immediate TCP peer gogo sees.

At the origin proxy:

- Allow only Cloudflare traffic, or use Cloudflare Tunnel.
- Normalize Cloudflare's client IP signal into `X-Forwarded-For`.
- Pass a validated scheme as `X-Forwarded-Proto`.
- Strip any incoming forwarded headers from traffic that did not pass the
  Cloudflare source check.

For nginx behind Cloudflare, after the source allow-list is enforced:

```nginx
proxy_set_header X-Forwarded-For $http_cf_connecting_ip;
proxy_set_header X-Forwarded-Proto $http_x_forwarded_proto;
proxy_set_header X-Real-IP $http_cf_connecting_ip;
proxy_set_header Forwarded "";
```

gogo does not read `CF-Connecting-IP` directly. If Cloudflare connects straight
to gogo with no origin proxy, generate `TrustedProxies` from Cloudflare's
published IPv4 and IPv6 ranges during deployment and refresh them when those
ranges change, and still firewall the origin so non-Cloudflare clients cannot
bypass the edge. Do not copy a static range list from this guide, and do not
use `TrustProxy: true` on a public bind as a shortcut.

## Load Balancers

For L7 load balancers and gateway proxies:

- Enable or set `X-Forwarded-For` and `X-Forwarded-Proto`.
- Make sure the balancer strips or overwrites client-supplied forwarded
  headers.
- Put only the balancer's private addresses or subnets in `TrustedProxies`.
- Keep the balancer's request body limit at or below `Config.BodyLimit`.
- Set read, idle, and WebSocket timeouts long enough for legitimate streaming,
  SSE, and WebSocket traffic.
- Use a cheap health route such as `/healthz` and skip it in access logs.

For L4 load balancers, forwarded headers are not added. `req.IP()` may be the
load balancer peer unless the platform preserves the client source address.
gogo does not document PROXY protocol support, so use an L7 proxy that converts
the source address into sanitized `X-Forwarded-For` if handlers need the
original client IP.

## Deployment Notes

### File Descriptors

Every active client connection, upstream DB socket, log file, and accepted
WebSocket consumes a file descriptor. Set the process limit with enough headroom
for peak concurrency and backend connections.

```sh
ulimit -n
ulimit -n 65535
```

For systemd services:

```ini
[Service]
LimitNOFILE=65535
```

In containers, set both the container runtime limit and the host or service
manager limit. Confirm the running process sees the intended value rather than
only the interactive shell.

### GOMAXPROCS

Single-app mode (`NewApp` plus `Run`) has one uWS event loop. Raising
`GOMAXPROCS` does not make sync routes run on multiple loop threads, but it
does give async handlers, DB drivers, and background goroutines scheduler
capacity.

For `RunMultiCore`, choose an explicit worker count and pin `GOMAXPROCS` to the
same value unless benchmarks show your workload needs a different split:

```go
cores := runtime.NumCPU()
runtime.GOMAXPROCS(cores)

handle, err := gogo.RunMultiCore(cores, 3000, func(app *gogo.App) {
	// Register the same routes and middleware on every worker.
})
```

`gogo.SetWorkerCount(n)` controls the shared-dispatch `GetAsync` worker pool
and must be called before the first `GetAsync` registration. The default is
`runtime.NumCPU()`. In `RunMultiCore` deployments with mostly short async work,
measure whether a smaller worker pool leaves more CPU for the loop threads.

### DB Pools

Do blocking database work in async routes or `Response.Async`, not in sync
handlers running on the uWS loop thread.

Create DB pools once per process, not once per request. In `RunMultiCore`, also
create shared pools outside the setup callback so every worker captures the same
pool:

```go
db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
if err != nil {
	log.Fatal(err)
}
db.SetMaxOpenConns(40)
db.SetMaxIdleConns(10)
db.SetConnMaxLifetime(time.Hour)
db.SetConnMaxIdleTime(5 * time.Minute)

handle, err := gogo.RunMultiCore(cores, 3000, func(app *gogo.App) {
	app.GetAsync("/users/:id<int>", func(res *gogo.Response, req *gogo.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()

		var name string
		err := db.QueryRowContext(ctx,
			"select name from users where id = $1",
			req.ParamInt("id", 0),
		).Scan(&name)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			res.Send(500, "text/plain; charset=utf-8", "database error\n")
			return
		}
		res.JSON(200, map[string]string{"name": name})
	})
})
```

Size `MaxOpenConns` against the database, not just one app process. Total
possible connections are roughly:

```txt
replicas * MaxOpenConns
```

Keep per-query timeouts shorter than your proxy idle timeout and long enough for
normal p95/p99 query latency. `req.Context()` is canceled when the client aborts
before a response is sent, so `QueryContext` and upstream HTTP calls can stop
work for disconnected clients.

## Panic And Error Logging

gogo catches panics from HTTP handlers, async handlers, WebSocket callbacks,
defer callbacks, body callbacks, and `OnFinish` callbacks. It sends a
best-effort 500 when an HTTP response is still available and keeps serving. The
default panic handler writes the recovered value and a stack trace to stderr.

Install a process-wide handler during startup to send panics to your logger or
error tracker:

```go
gogo.SetPanicHandler(func(recovered any) {
	log.Printf("gogo panic: %v\n%s", recovered, debug.Stack())
	// sentry.CaptureException(fmt.Errorf("%v", recovered))
})
```

`SetPanicHandler(nil)` restores the default stderr handler. The panic handler is
global to the process, which matters when one process hosts multiple `App`
instances through `RunMultiCore`.

For request logs, combine request IDs with the bundled logger:

```go
app.Use(mw.RequestID())
app.Use(mw.Logger(mw.LoggerOptions{
	Format:    mw.JSONFormat,
	SkipPaths: []string{"/healthz", "/metrics"},
}))
```

`middleware.Logger` records method, URL, status, duration, peer IP
(`req.IP()`), and user agent. Behind a reverse proxy, that IP is usually the
proxy. If access logs need the original client from `req.IPs()`, snapshot it
before the handler runs and emit it from `Response.OnFinish`:

```go
app.Use(func(next gogo.Handler) gogo.Handler {
	return func(res *gogo.Response, req *gogo.Request) {
		method, url := req.Method(), req.URL()
		clientIP := req.IP()
		if ips := req.IPs(); len(ips) > 0 {
			clientIP = ips[0]
		}
		defer res.OnFinish(func() {
			log.Printf("method=%s url=%s status=%d client_ip=%s",
				method, url, res.StatusCode(), clientIP)
		})
		next(res, req)
	}
})
```

Request IDs are stored in `req.Local(middleware.RequestIDLocalKey)` for
handlers or custom middleware that need to add them to application logs.

Handlers do not return errors to the framework. Log operational failures where
you make the response decision, then send a client-safe message:

```go
func sendServerError(res *gogo.Response, req *gogo.Request, msg string, err error) {
	requestID, _ := req.Local(mw.RequestIDLocalKey).(string)
	log.Printf("request_id=%s method=%s url=%s error=%s: %v",
		requestID, req.Method(), req.URL(), msg, err)
	res.Send(500, "text/plain; charset=utf-8", "internal server error\n")
}
```

For work that continues after the handler returns, register abort handling
synchronously before returning:

```go
app.Get("/report", func(res *gogo.Response, req *gogo.Request) {
	aborted := res.OnAborted()
	ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
	requestID, _ := req.Local(mw.RequestIDLocalKey).(string)
	method, url := req.Method(), req.URL()

	res.Async(func() {
		defer cancel()

		report, err := buildReport(ctx)
		if err != nil {
			if ctx.Err() != nil || aborted.Load() {
				return
			}
			log.Printf("request_id=%s method=%s url=%s error=build-report: %v",
				requestID, method, url, err)
			res.Send(500, "text/plain; charset=utf-8", "internal server error\n")
			return
		}
		if aborted.Load() {
			return
		}
		res.JSON(200, report)
	})
})
```

Middleware and adapters may expose their own error hooks. For example, the
Redis rate-limit adapter reports store failures through
`redisadapter.RateLimitOptions.OnError`, and WebSocket hub adapter errors can
be routed with `gogo.WithWSHubAdapterErrorHandler`. These failures are not all
panics, so wire the relevant hooks into the same logging or metrics pipeline as
your panic handler.

## Ops Checklist

- The gogo port is reachable only from the trusted proxy or load balancer.
- The proxy overwrites forwarded headers before gogo sees them.
- `TrustedProxies` lists immediate proxy peers, not arbitrary public clients.
- Proxy body limits are no larger than `Config.BodyLimit`.
- `ulimit -n` covers peak clients, WebSockets, DB sockets, and log files.
- `GOMAXPROCS`, `RunMultiCore`, and `SetWorkerCount` are sized intentionally.
- DB pool limits are multiplied across all replicas before comparing to the DB
  server limit.
- `SetPanicHandler`, request logging, adapter error hooks, and abort handling
  are installed before production traffic.

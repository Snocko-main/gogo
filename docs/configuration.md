# Configuration Examples

This page collects starting points for common deployments. The values below are
not magic defaults; they show which knobs usually deserve an explicit choice.

## Local Development

Use localhost binding, keep safe body defaults, and leave proxy trust off.

```go
app, err := gogo.NewApp(gogo.Config{
	BindAddr:        "127.0.0.1",
	BodyLimit:       4 << 20,
	BodyReadTimeout: 30 * time.Second,
})
if err != nil {
	log.Fatal(err)
}
```

Why:

- `BindAddr: "127.0.0.1"` keeps the service local to the machine.
- `TrustProxy` stays `false`, so forwarded headers cannot spoof protocol or IP
  helpers during development.
- `CapturePeerIP` can stay `false` unless async handlers need `req.IP()`.

## Behind A Trusted Reverse Proxy

Use this shape when nginx, Caddy, a load balancer, or a CDN is the only public
entry point and it overwrites untrusted forwarded headers.

```go
app, err := gogo.NewApp(gogo.Config{
	BindAddr:        "127.0.0.1",
	BodyLimit:       8 << 20,
	BodyReadTimeout: 30 * time.Second,
	TrustProxy:      true,
	CapturePeerIP:   true,
})
if err != nil {
	log.Fatal(err)
}
```

Why:

- `TrustProxy: true` makes `Protocol`, `Secure`, and `IPs` honor forwarded
  headers from the trusted edge.
- `CapturePeerIP: true` lets async routes read the immediate TCP peer with
  `req.IP()` for audit logs or socket-peer rate limits.
- Keep a proxy-level request body cap at or below the app-level `BodyLimit`.

## Internet-Facing Without A Proxy

Bind publicly only when gogo receives client traffic directly. Do not enable
proxy trust in this mode.

```go
app, err := gogo.NewApp(gogo.Config{
	BindAddr:        "",
	BodyLimit:       4 << 20,
	BodyReadTimeout: 15 * time.Second,
	CapturePeerIP:   true,
})
if err != nil {
	log.Fatal(err)
}
```

Why:

- Empty `BindAddr` uses the uWS default of all interfaces.
- `TrustProxy` stays `false`, so attacker-controlled `X-Forwarded-*` headers
  are ignored by helper APIs.
- A shorter `BodyReadTimeout` is often appropriate for JSON APIs with small
  bodies. Raise it for legitimate upload endpoints.

## Production Checklist

- Pick a `BindAddr` intentionally. Use localhost behind a reverse proxy, or all
  interfaces only when the process is the public edge.
- Keep `BodyLimit` enabled unless another trusted layer enforces an equal or
  stricter cap.
- Keep `BodyReadTimeout` enabled. Use route-level upload design rather than
  disabling the timeout globally.
- Enable `TrustProxy` only behind an edge that strips or overwrites forwarded
  headers from untrusted clients.
- Enable `CapturePeerIP` when async handlers, rate limiters, auth, or audit
  logs need the socket peer through `req.IP()`.
- Leave `JSONEncoder` and `JSONDecoder` nil until a benchmark shows JSON is a
  real bottleneck.

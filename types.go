package gogo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Snocko-main/gogo/internal/mwhint"
)

// commonStatusLines caches the formatted status line for codes likely to
// appear on the hot path so Send/JSON don't pay 2 allocs per response.
// Anything not in this table falls back to the slow strconv+concat path.
var commonStatusLines = map[int]string{
	200: "200 OK",
	201: "201 Created",
	202: "202 Accepted",
	204: "204 No Content",
	301: "301 Moved Permanently",
	302: "302 Found",
	304: "304 Not Modified",
	400: "400 Bad Request",
	401: "401 Unauthorized",
	403: "403 Forbidden",
	404: "404 Not Found",
	405: "405 Method Not Allowed",
	409: "409 Conflict",
	413: "413 Request Entity Too Large",
	429: "429 Too Many Requests",
	500: "500 Internal Server Error",
	502: "502 Bad Gateway",
	503: "503 Service Unavailable",
	504: "504 Gateway Timeout",
}

// statusLine formats an HTTP status code into the "<code> <reason>" string
// uWS writes verbatim onto the wire. Standard codes get their canonical reason
// phrase via net/http; unknown codes fall back to just the number. Common
// codes hit the cache and avoid all allocation.
func statusLine(code int) string {
	validateStatusCode(code)
	if cached, ok := commonStatusLines[code]; ok {
		return cached
	}
	text := http.StatusText(code)
	if text == "" {
		return strconv.Itoa(code)
	}
	return strconv.Itoa(code) + " " + text
}

func validateStatusCode(code int) {
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("gogo: invalid HTTP status code %d", code))
	}
}

// validatePattern rejects registration-time patterns that would crash uWS or
// produce surprising routes. Patterns must be non-empty, start with '/', and
// contain no control characters (NUL/CR/LF).
func validatePattern(pattern string) {
	if pattern == "" {
		panic("gogo: route pattern must not be empty")
	}
	if pattern[0] != '/' {
		panic(fmt.Sprintf("gogo: route pattern %q must start with '/'", pattern))
	}
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if c == 0 || c == '\r' || c == '\n' {
			panic(fmt.Sprintf("gogo: route pattern %q contains a control character", pattern))
		}
	}
}

// validateHeaderValue rejects header values that contain CR/LF/NUL, which
// would otherwise allow HTTP response splitting if user-controlled input is
// passed straight through.
func validateHeaderValue(key, value string) {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '\r' || c == '\n' || c == 0 {
			panic(fmt.Sprintf("gogo: header %q value contains a control character (CR/LF/NUL not allowed)", key))
		}
	}
}

// validateHeaderName enforces the HTTP token grammar (RFC 7230 §3.2.6)
// on response header field names. The framework writes header names
// to the wire — and to the cgo bridge's null-terminated header blob —
// so anything outside the token set risks:
//
//   - CR/LF in the name: response splitting on the wire.
//   - NUL in the name: corrupts the bridge's pack-and-cross batching
//     (headerBlobPool entries are framed name\0value\0...).
//   - Space, colon: fragments the wire header line ("X-Foo : 1" sends
//     two malformed lines).
//   - Anything else outside RFC 7230 tchar: not a well-formed header
//     per the grammar; likely a sign that user input or misconfig
//     ended up at a Header(key, ...) call site.
//
// Empty names also panic — uWS would treat them as ": value" which is
// useless to the client and a clear programmer error.
func validateHeaderName(name string) {
	if name == "" {
		panic("gogo: header name is empty")
	}
	for i := 0; i < len(name); i++ {
		if !isHTTPTokenChar(name[i]) {
			panic(fmt.Sprintf("gogo: header name %q contains invalid byte 0x%02x (must be RFC 7230 tchar)", name, name[i]))
		}
	}
}

// isHTTPTokenChar reports whether c is a valid character in an HTTP
// token (RFC 7230 §3.2.6):
//
//	tchar = "!" / "#" / "$" / "%" / "&" / "'" / "*" / "+" / "-" /
//	        "." / "^" / "_" / "`" / "|" / "~" / DIGIT / ALPHA
func isHTTPTokenChar(c byte) bool {
	if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
		return true
	}
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}

// containsCtlForHeader is the panic-free twin of validateHeaderValue used
// on code paths where the input typically comes from a request (so a
// hostile peer should not be able to trigger a panic by simply sending
// a malformed value).
func containsCtlForHeader(value string) bool {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '\r' || c == '\n' || c == 0 {
			return true
		}
	}
	return false
}

var (
	// Pre-grow pendingHeaders to a small capacity so middleware
	// that buffers a handful of headers (CORS: 3-4, Helmet: ~10)
	// doesn't trigger the first-request realloc storm. The backing
	// array is retained across pool recycles, so the cost only
	// fires once per wrapper's lifetime.
	responsePool = sync.Pool{New: func() any {
		return &Response{pendingHeaders: make([]responseHeader, 0, 8)}
	}}
	requestPool    = sync.Pool{New: func() any { return &Request{} }}
	asyncStatePool = sync.Pool{New: func() any { return &asyncState{} }}

	// bodyEncoderPool recycles the bodyEncoder staging buffer used by
	// Compress middleware. Each encoder is ~24 bytes plus a growable
	// []byte. Recycling the struct avoids one heap allocation per
	// compressed response; the []byte's backing array is also retained
	// (up to bodyEncoderMaxCap) so typical-size payloads pay zero
	// buffer alloc after warm-up.
	bodyEncoderPool = sync.Pool{New: func() any { return &bodyEncoder{} }}
)

// bodyEncoderMaxCap caps the bodyEncoder.buf slice we retain in the
// pool. A pathological request that compresses a 4 MiB body would
// otherwise leave a 4 MiB buffer alive in the pool indefinitely; the
// 64 KiB ceiling fits the overwhelming majority of HTML/JSON
// responses while keeping pool memory predictable under spike loads.
const bodyEncoderMaxCap = 64 * 1024

// PanicHandler is invoked when user code panics inside an HTTP, async,
// WebSocket, defer, or body callback. HTTP paths emit a best-effort 500 when a
// response is still available. The argument is the recovered value (the panic
// payload).
//
// The framework ships a default handler that prints the panic value plus a
// goroutine stack trace to stderr, so production deployments never have a
// panic disappear silently. Call SetPanicHandler with a custom function to
// route panics elsewhere (structured logger, error tracker), or pass nil
// to restore the default.
type PanicHandler func(recovered any)

// panicHandlerFn is the active panic handler. Read on every panic-recover
// site across the framework (HTTP / async / WebSocket / defer / body /
// OnFinish / OnData / etc.) — atomic.Pointer keeps the read lock-free
// instead of the RWMutex.RLock+defer pair that used to bracket every
// access. Writes go through SetPanicHandler which is rare.
var panicHandlerFn atomic.Pointer[PanicHandler]

func init() {
	fn := PanicHandler(defaultPanicHandler)
	panicHandlerFn.Store(&fn)
	sendFileChunkBytesAtomic.Store(int64(SendFileChunkBytes))
}

// defaultPanicHandler writes the recovered value and a goroutine stack
// trace to stderr. Matches the format Go's runtime uses for unrecovered
// panics so operators have something familiar to grep for.
func defaultPanicHandler(recovered any) {
	fmt.Fprintf(os.Stderr, "gogo: recovered panic: %v\n%s\n",
		recovered, debug.Stack())
}

// SetPanicHandler registers fn as the global panic handler. Pass nil to
// restore the default stderr logger. The handler must not panic itself
// (any panic inside it is recovered silently).
func SetPanicHandler(fn PanicHandler) {
	if fn == nil {
		fn = defaultPanicHandler
	}
	panicHandlerFn.Store(&fn)
}

func getPanicHandler() PanicHandler {
	return *panicHandlerFn.Load()
}

func reportPanic(recovered any) {
	panicHandler := getPanicHandler()
	if panicHandler == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	panicHandler(recovered)
}

// Handler handles a single HTTP request.
//
// The Request and Response values are only valid for the duration of the
// callback. Do not store them or use them from another goroutine.
type Handler func(*Response, *Request)

// AsyncHandler handles a request on a goroutine that is free to block.
// The Response arrives in async mode with the abort context pre-attached.
// The Request is a snapshot copied from the live uWS request before it was
// freed. Shared zero-cgo routes reject snapshots past their fixed caps
// (URL 256, query 512, each param 64, headers buffer 8 KB); middleware
// fallback snapshots copy the live request fields via cgo before spawning.
type AsyncHandler func(*Response, *Request)

// OpCode identifies a WebSocket frame type.
type OpCode int

const (
	// Text is a UTF-8 WebSocket message.
	Text OpCode = 1

	// Binary is a binary WebSocket message.
	Binary OpCode = 2
)

// WebSocketBehavior contains callbacks and per-route limits for a
// WebSocket endpoint. Limit fields default to safe production values
// when zero — pick explicit numbers when you need different limits, do
// not leave them at zero hoping for "unlimited".
type WebSocketBehavior struct {
	Open    func(*WebSocket)
	Message func(*WebSocket, []byte, OpCode)
	Close   func(*WebSocket, int, []byte)

	app *App

	// MaxPayloadLength is the largest single incoming message the
	// server will accept. Frames over this cap cause uWS to close the
	// connection. Default 16 MiB.
	MaxPayloadLength int

	// IdleTimeout is the maximum time a WebSocket may sit idle (no
	// frames in either direction) before uWS closes it. Default 120s.
	IdleTimeout time.Duration

	// MaxBackpressure is the bytes uWS will queue per-socket for a
	// slow consumer before closing the connection. Protects the
	// loop from being held hostage by a single non-draining client.
	// Default 64 KiB.
	MaxBackpressure int

	// DisablePings turns off uWS's built-in ping/pong keepalive.
	// Default (zero) leaves automatic pings ON so an idle connection
	// doesn't get reaped by NAT boxes; set true only if your client
	// drives its own ping protocol.
	DisablePings bool

	// UnsafeAutoUpgrade restores uWS's legacy default of accepting every
	// WebSocket handshake when Upgrade is nil. The secure default rejects
	// browser handshakes that carry an Origin header unless the app installs
	// an explicit Upgrade callback. Only set this for public, non-cookie
	// endpoints where cross-origin WebSocket access is intentional.
	UnsafeAutoUpgrade bool

	// Upgrade runs synchronously on the uWS loop thread for every
	// incoming WebSocket handshake before the connection is
	// established. The callback inspects request headers / query /
	// peer IP / offered subprotocols and MUST call ctx.Accept(...) or
	// ctx.Reject(...) before returning — failing to do either causes
	// the framework to refuse the upgrade with a 500 (and log the
	// misuse).
	//
	// Typical uses:
	//
	//   - subprotocol negotiation: pick one of ctx.Protocols()
	//   - cookie / token auth at handshake time, with the resolved
	//     identity stashed via ctx.SetUserData(user) for the rest
	//     of the connection's lifetime
	//   - rejecting based on origin / referrer / API quota
	//
	// SECURITY: when Upgrade is nil the framework accepts non-browser
	// handshakes that omit Origin, but rejects browser handshakes that
	// carry Origin. The HTTP middleware chain (CORS in particular) does
	// NOT cover the WebSocket upgrade path, so a Same-Origin Policy gate
	// that protects fetch() does NOT protect WebSocket(). If the endpoint
	// uses session cookies or any other ambient credential, an attacker
	// page can open ws://yoursite/... from the user's browser and ride the
	// user's session — Cross-Site WebSocket Hijacking (CSWSH).
	//
	// At minimum verify the Origin header against an allow-list. The
	// middleware package ships middleware.WebSocketAuth for the
	// common case (origin check + optional bearer / cookie /
	// subprotocol gating).
	Upgrade func(*UpgradeContext)
}

// Middleware wraps a Handler with cross-cutting behavior (auth, logging,
// CORS, etc.). The returned Handler is invoked per request; call next(res,
// req) to continue the chain or write a response and return to short-circuit.
//
// Middleware composes at registration time, so there is no per-request
// allocation. Middleware applies to routes registered AFTER the call to
// App.Use that introduced it; reordering Use and route registration changes
// the effective chain for those routes.
//
// Middleware runs on the uWS loop thread and MUST NOT block — no DB queries,
// no remote calls. For middleware that needs to block (e.g. resolving a user
// from a session token via a DB lookup), use AsyncMiddleware with UseAsync;
// it runs on a goroutine and is free to block.
//
// Static replies (Reply, string, []byte targets of App.Get) use their C++
// fast path only when no matching sync middleware or typed-param constraint
// needs a Go-side handler. GetAsync and small PostAsync routes keep their
// shared-memory fast path when only async-capable middleware applies;
// sync-only middleware makes them fall back to a wrapped sync entry point.
type Middleware func(next Handler) Handler

// AsyncMiddleware wraps an AsyncHandler the same way Middleware wraps a
// Handler, but executes on the goroutine that runs the user's async handler
// so it is free to block (DB queries, downstream HTTP calls, etc.).
//
// Async middleware applies only to GetAsync and body-async routes such as
// PostAsync, PutAsync, PatchAsync, and DeleteAsync. Use it when the
// cross-cutting work itself needs to block; for cheap header / query
// inspection prefer sync Middleware (smaller per-request overhead and also
// applicable to sync routes).
//
// Pass data through to the user handler via Request.SetLocal / Request.Local.
type AsyncMiddleware func(next AsyncHandler) AsyncHandler

// middlewareEntry is a single Use call's middleware bound to an optional
// path prefix. prefix == "" means "global" (apply to every later route);
// otherwise the middleware applies only to routes whose pattern is the
// prefix itself or sits under "prefix/".
type middlewareEntry struct {
	prefix string
	mw     Middleware
	// alsoAsync marks entries that have a sibling copy in
	// asyncMiddlewares (PlaceBoth registrations). The framework uses
	// this to keep the zero-cgo shared-memory dispatch path for async
	// routes: hasMatchingMiddleware ignores alsoAsync entries because
	// their work is already going to fire from the async chain inside
	// the worker. Without this flag, registering a perf-neutral
	// middleware like Logger via PlaceBoth would force every GetAsync
	// route off the fast path.
	alsoAsync bool
}

// asyncMiddlewareEntry mirrors middlewareEntry for the async chain.
type asyncMiddlewareEntry struct {
	prefix string
	mw     AsyncMiddleware
	// alsoSync flags PlaceBoth registrations: this entry has a
	// twin in the sync chain. When the framework takes the slow
	// path for an async route (because some other PlaceSync entry
	// matches), the sync wrapper already fires the twin — running
	// it again from the async chain would double-execute the
	// middleware. wrapAsyncForSlowPath filters these out.
	alsoSync bool
}

const (
	// NoBodyLimit disables Config.BodyLimit. Use only behind an external
	// body-size limit, such as a trusted reverse proxy.
	NoBodyLimit = -1

	// NoBodyReadTimeout disables Config.BodyReadTimeout. Use only for tests,
	// trusted local traffic, or routes protected by an external upload
	// deadline.
	NoBodyReadTimeout time.Duration = -1
)

// Config tunes per-App behavior. All fields are optional; the zero value
// is a safe production default. Pass to NewApp; values are applied at
// app creation and bind time. The struct is intentionally narrow — knobs
// only get added here when they need a single, app-wide value.
//
// # Connection-level timeouts and limits
//
// A few knobs that look like they belong here are deliberately not
// exposed:
//
//   - HTTP idle timeout (TCP connection sits open without sending a
//     request, or keep-alive between requests). Fixed by uWebSockets
//     at 10 seconds via the HTTP_IDLE_TIMEOUT_S constant in
//     HttpContext.h — a connection that goes silent for 10 s is closed
//     automatically. Exposing this as a config knob would require
//     patching vendored uWS source or wiring up the uWS filter
//     mechanism through the C++ bridge; the default is aggressive
//     enough that this hasn't been done yet. If you need a longer
//     idle for a legitimate long-poll-style workload, use a WebSocket
//     route (WebSocketBehavior.IdleTimeout is configurable).
//
//   - Maximum concurrent connections. Not implemented in this
//     framework because the bound that matters in practice is the OS
//     file-descriptor limit (`ulimit -n`) and any reverse proxy in
//     front (nginx `limit_conn_zone`, etc.). uWS holds an idle TCP
//     connection in ~10 KB of RAM, so 100k connections is ~1 GB —
//     usually the FD cap fires long before that. If you genuinely
//     need application-level admission control, terminate at a proxy
//     and apply limits there.
//
//   - Slow-loris on the request body itself IS covered: see
//     BodyReadTimeout below.
type Config struct {
	// BodyLimit caps the request-body bytes a Post / Any route will
	// accept. Enforced at three layers so every intake shape gets the
	// same upper bound:
	//
	//   - Content-Length declared: rejected with 413 at arrival on the
	//     C++ side before any cgo crossing — zero per-request cost
	//     beyond the existing header lookup.
	//   - Chunked transfer-encoded + Response.OnData: the Go-side
	//     accumulator inside OnData totals chunk sizes and emits
	//     413 + close as soon as the running total crosses the cap.
	//   - Chunked transfer-encoded + Response.Body: the caller-supplied
	//     maxBytes is clamped down by BodyLimit when BodyLimit is
	//     smaller, so handlers that ask Body(10 MiB) on an app capped
	//     at 1 MiB top out at 1 MiB.
	//
	// Zero uses the safe default of 4 MiB. Set to NoBodyLimit to disable
	// the cap entirely when an external layer enforces a trusted body-size
	// limit.
	BodyLimit int

	// BodyReadTimeout caps the wall-clock time the framework will
	// wait for a request body to finish arriving. Applied per call
	// to Response.Body: a timer starts when Body registers its
	// chunk listener and fires done(nil, ErrBodyTimeout) if the
	// last chunk hasn't landed by the deadline. Defeats slow-loris
	// drip uploads where the client keeps the request open but
	// sends bytes too slowly to ever exhaust BodyLimit.
	//
	// Zero uses the safe default of 30s. Reasonable production values
	// fall between 10s for API endpoints and 60s+ for legitimate
	// upload flows. Set to NoBodyReadTimeout to disable the timeout
	// explicitly (not recommended outside tests or trusted local traffic).
	// The timer fires on a goroutine that hands the
	// cancellation back to the loop thread via Loop.Defer so done()
	// and the connection close run serially with onData / onAborted
	// — callers don't have to think about races.
	BodyReadTimeout time.Duration

	// BindAddr is the local interface to bind on. Empty string means
	// "all interfaces" (uWS default 0.0.0.0). Use "127.0.0.1" for a
	// localhost-only service. Applied at Listen time.
	BindAddr string

	// CapturePeerIP enables snapshotting the peer IP on the C++ side
	// before shared-dispatch / async handlers run. When false (default),
	// req.IP() in shared-dispatch GetAsync handlers and in async handlers
	// that fell through to the snapshot path will return "". Sync
	// handlers always get a usable req.IP() — the lookup is lazy and
	// only pays cgo when actually called, so the flag has no effect
	// there.
	//
	// Cost when enabled: one std::string_view format + ~50-byte memcpy
	// per shared-dispatch request, plus 64 extra bytes on every
	// AsyncCtx. Measured at roughly 2–3% throughput on small responses
	// (e.g. /db at ~75K rps); negligible on routes with significant
	// per-request work. Enable it when handlers behind GetAsync need to
	// read the peer IP; otherwise leave it off.
	CapturePeerIP bool

	// TrustProxy declares that the server sits behind a trusted reverse
	// proxy (e.g. nginx, an L7 load balancer, a CDN), so X-Forwarded-* /
	// X-Real-IP / Forwarded headers from the client are safe to surface
	// to the application:
	//
	//   - req.Protocol() / req.Secure() honor X-Forwarded-Proto.
	//   - req.IPs() exposes the normalized X-Forwarded-For chain.
	//
	// Leave OFF when the server is directly internet-facing — otherwise
	// any client can spoof their apparent protocol / origin by sending
	// X-Forwarded-* headers. Default false.
	TrustProxy bool

	// JSONEncoder is used by Response.JSON and Response.JSONP. Nil uses
	// encoding/json.Marshal. Override it with a faster compatible encoder
	// such as sonic.Marshal, go-json.Marshal, or jsoniter.Marshal when JSON
	// reflection cost dominates your handlers.
	JSONEncoder JSONEncoder

	// JSONDecoder is used by Request.BodyParser for application/json and
	// text/json request bodies. Nil uses encoding/json.Unmarshal.
	JSONDecoder JSONDecoder
}

// JSONEncoder is the marshaling function used by App-scoped JSON helpers.
// Its shape matches encoding/json.Marshal and common third-party drop-ins.
type JSONEncoder func(v any) ([]byte, error)

// JSONDecoder is the unmarshaling function used by App-scoped JSON body
// parsing helpers. Its shape matches encoding/json.Unmarshal and common
// third-party drop-ins.
type JSONDecoder func(data []byte, v any) error

// App is a uWebSockets HTTP application.
type App struct {
	inner                   appNative
	middlewares             []middlewareEntry
	asyncMiddlewares        []asyncMiddlewareEntry
	cfg                     Config
	notFoundHandler         Handler
	methodNotAllowedHandler Handler

	// userCatchAllRegistered is true when the user has already
	// registered Any("/*") (or Get/Post/.../Options "/*") — in that
	// case Listen does not install the framework's own catch-all on
	// top, since uWS allows only one handler per (method, pattern)
	// and the user's handler should win.
	userCatchAllRegistered bool

	// stopping flips as soon as Shutdown / ShutdownGracefully starts,
	// so cross-thread producers stop enqueueing native work before
	// the loop begins closing sockets. closed + pendingTimers
	// coordinate ShutdownGracefully's force-close goroutine with
	// Close. Without this, a force-close timer that fires after the
	// user has already called Close races against the freed
	// appNative.ptr inside a cgo call.
	//
	// Atomic Int32 instead of WaitGroup so the happens-before edge
	// between Add and Close's drain is purely Go-side; WaitGroup's
	// Add/Wait race detector requires a happens-before that the
	// cgo-mediated loop close doesn't provide.
	stopping      atomic.Bool
	closed        atomic.Bool
	pendingTimers atomic.Int32
	// nativeMu serializes App.Close with cross-thread native calls that keep
	// using the app pointer after leaving Go, such as WebSocket publishes.
	nativeMu sync.RWMutex
	// workerRefAcquired / workerRefDropped pair a shared-dispatch
	// worker-pool reference with Apps that actually register at
	// least one shared fast-path route. Sync-only Apps must not hold
	// the pool open.
	workerRefAcquired atomic.Bool
	workerRefDropped  atomic.Bool
	// routeMethods records every method-specific route registered on
	// the App. The catch-all installed at Listen uses these entries to
	// distinguish "path exists but the method is wrong" (405 + Allow)
	// from "path doesn't exist" (404), including dynamic and wildcard
	// patterns. routeMethodIndex lets repeated registration of the same
	// method/pattern replace the prior metadata, matching uWS's
	// remove-then-add behavior.
	routeMethods       []routeMethodEntry
	routeMethodIndex   map[string]int
	onListenHooks      []func(port int)
	onShutdownHooks    []func()
	shutdownHooksFired atomic.Bool

	// namedRoutes maps a user-chosen name to the uWS-stripped pattern
	// so App.URL can do reverse routing (`URL("user.show", {"id":"42"})`
	// → "/users/42"). Names are set via App.Name or Router.Name.
	namedRoutes map[string]string

	// templateEngine is the TemplateEngine installed via SetTemplateEngine.
	// nil means "no engine installed" — Render then responds 500 with an
	// operator-visible error logged via reportPanic.
	templateEngine atomic.Pointer[templateEngineSlot]

	// pubsubPeers is populated by RunMultiCore before listeners start. uWS's
	// TopicTree is loop-local, so App.Publish fans out to every peer loop.
	pubsubPeers []*App
}

const defaultBodyReadTimeout = 30 * time.Second

// validateConfig rejects ambiguous or unsafe config values before native
// resources are allocated.
func validateConfig(c Config) error {
	if c.BodyLimit < 0 && c.BodyLimit != NoBodyLimit {
		return fmt.Errorf("gogo: Config.BodyLimit must be non-negative or NoBodyLimit (got %d)", c.BodyLimit)
	}
	if c.BodyReadTimeout < 0 && c.BodyReadTimeout != NoBodyReadTimeout {
		return fmt.Errorf("gogo: Config.BodyReadTimeout must be non-negative or NoBodyReadTimeout (got %s)", c.BodyReadTimeout)
	}
	return nil
}

// defaultConfig fills in safe production defaults for any zero Config fields.
// Mutates and returns the input. Call validateConfig before defaultConfig when
// accepting user input.
func defaultConfig(c Config) Config {
	if c.BodyLimit == 0 {
		c.BodyLimit = 4 << 20 // 4 MiB
	} else if c.BodyLimit == NoBodyLimit {
		c.BodyLimit = 0
	}
	if c.BodyReadTimeout == 0 {
		c.BodyReadTimeout = defaultBodyReadTimeout
	}
	if c.JSONEncoder == nil {
		c.JSONEncoder = json.Marshal
	}
	if c.JSONDecoder == nil {
		c.JSONDecoder = json.Unmarshal
	}
	return c
}

// NewApp creates a non-TLS uWebSockets app. With no Config the app uses safe
// production defaults; pass at most one Config to override. The variadic shape
// is retained only for backward compatibility with the old zero-arg signature.
func NewApp(cfg ...Config) (*App, error) {
	if len(cfg) > 1 {
		return nil, fmt.Errorf("gogo: NewApp accepts at most one Config (got %d)", len(cfg))
	}
	var c Config
	if len(cfg) > 0 {
		c = cfg[0]
	}
	if err := validateConfig(c); err != nil {
		return nil, err
	}
	c = defaultConfig(c)
	inner, err := newAppNative()
	if err != nil {
		return nil, err
	}
	initSharedLayout()
	inner.setBodyLimit(c.BodyLimit)
	inner.setCapturePeerIP(c.CapturePeerIP)
	return &App{inner: inner, cfg: c}, nil
}

func (a *App) acquireSharedWorkerRef() {
	if !a.workerRefAcquired.Swap(true) {
		acquireSharedWorkerAppRef()
	}
}

// Reply is a static response captured once at registration time. Routes
// registered with this target are served entirely by the C++ event loop with
// no cgo callback per request — use it for /health, /version, cached config,
// or any constant response.
type Reply struct {
	Status      int    // defaults to 200 when zero
	ContentType string // omits Content-Type header when empty
	Body        string
}

// Use appends middleware to the chain. Each registered route that follows
// this call wraps its handler in the current chain.
//
// The first argument may optionally be a path pattern (string), scoping the
// middleware to URLs that fall under that prefix at request time:
//
//	app.Use(authMW)                       // applies to every later route
//	app.Use("/api/*", authMW)             // applies to URLs under /api/
//	app.Use("/admin", auditMW, rateMW)    // /admin and URLs under /admin/
//
// Trailing "/*" or "/**" on the prefix is stripped — "/api/*" and "/api"
// mean the same thing (prefix = "/api"). A pattern of "/*" or "/" means
// "every route" (equivalent to no pattern).
//
// Scoped Use matches the live request URL via req.URL(), not the route
// pattern string. This makes it safe against parametric / wildcard routes
// (e.g. Get("/api/:section") serving /api/admin will go through a
// Use("/api/admin", auth) middleware). Cost: one URL string compare per
// scoped entry per request — global Use (no prefix) still composes at
// registration with zero per-request cost.
//
// Prefer App.Group(prefix, mws...) for scoping new code: Group binds
// middleware by Router identity, so the chain composes at registration
// time and matching is unambiguous without the per-request URL check.
//
// Middlewares run left-to-right — the first argument runs first (outermost).
// Safe to call multiple times.
func (a *App) Use(args ...any) {
	if len(args) == 0 {
		return
	}

	var (
		prefix     string
		hasPrefix  bool
		startIndex int
	)
	if s, ok := args[0].(string); ok {
		prefix = normalizeMWPrefix(s)
		hasPrefix = true
		startIndex = 1
	}

	if startIndex == len(args) {
		if hasPrefix {
			panic("gogo: Use: path pattern given but no middleware passed")
		}
		return
	}

	for i := startIndex; i < len(args); i++ {
		switch v := args[i].(type) {
		case mwhint.Hinted:
			a.registerHinted(prefix, v)
		case Middleware:
			// Raw middleware default = run in both chains. Sync
			// routes fire it on the loop thread, async routes fire
			// it inside the worker. Non-blocking middleware works
			// everywhere; blocking middleware should be wrapped via
			// middleware.Async before being passed here.
			a.registerHinted(prefix, mwhint.Hinted{Mw: v, Place: mwhint.Both})
		case func(next Handler) Handler:
			a.registerHinted(prefix, mwhint.Hinted{Mw: Middleware(v), Place: mwhint.Both})
		case AsyncMiddleware:
			// AsyncMiddleware shares the underlying function shape
			// with Middleware; accept it for backward compatibility
			// with code that named the type explicitly.
			mw := Middleware(func(next Handler) Handler {
				return Handler(v(AsyncHandler(next)))
			})
			a.registerHinted(prefix, mwhint.Hinted{Mw: mw, Place: mwhint.Async})
		case func(next AsyncHandler) AsyncHandler:
			mw := Middleware(func(next Handler) Handler {
				return Handler(v(AsyncHandler(next)))
			})
			a.registerHinted(prefix, mwhint.Hinted{Mw: mw, Place: mwhint.Async})
		case string:
			panic("gogo: Use: only the first argument may be a path pattern")
		default:
			panic(fmt.Sprintf("gogo: Use: unsupported argument type %T at index %d", v, i))
		}
	}
}

// registerHinted turns a mwhint.Hinted into one or two chain
// insertions. The middleware is type-asserted back to gogo.Middleware
// (mwhint stores it as `any` to avoid an import cycle).
func (a *App) registerHinted(prefix string, h mwhint.Hinted) {
	syncMw, ok := h.Mw.(Middleware)
	if !ok {
		// Allow the underlying func type too; values declared with
		// the raw signature satisfy the assertion via a conversion.
		if fn, fok := h.Mw.(func(next Handler) Handler); fok {
			syncMw = Middleware(fn)
		} else {
			panic(fmt.Sprintf("gogo: registerHinted: unsupported Mw type %T", h.Mw))
		}
	}
	asyncMw := AsyncMiddleware(func(next AsyncHandler) AsyncHandler {
		return AsyncHandler(syncMw(Handler(next)))
	})
	switch h.Place {
	case mwhint.Sync:
		a.middlewares = append(a.middlewares, middlewareEntry{prefix: prefix, mw: syncMw})
	case mwhint.Async:
		a.asyncMiddlewares = append(a.asyncMiddlewares, asyncMiddlewareEntry{prefix: prefix, mw: asyncMw})
	case mwhint.Both:
		a.middlewares = append(a.middlewares, middlewareEntry{prefix: prefix, mw: syncMw, alsoAsync: true})
		a.asyncMiddlewares = append(a.asyncMiddlewares, asyncMiddlewareEntry{prefix: prefix, mw: asyncMw, alsoSync: true})
	default:
		panic(fmt.Sprintf("gogo: invalid Placement %d", h.Place))
	}
}

// normalizeMWPrefix turns a user-facing Use pattern into the stored prefix
// form. "/api/*" → "/api"; "/api/**" → "/api"; "/" or "/*" → "" (global).
func normalizeMWPrefix(p string) string {
	validatePattern(p)
	for strings.HasSuffix(p, "/**") {
		p = p[:len(p)-3]
	}
	for strings.HasSuffix(p, "/*") {
		p = p[:len(p)-2]
	}
	if p == "" || p == "/" {
		return ""
	}
	return p
}

// mwMatches reports whether a middleware bound to prefix should apply to a
// route registered with routePattern.
func mwMatches(prefix, routePattern string) bool {
	if prefix == "" {
		return true
	}
	if routePattern == prefix {
		return true
	}
	return strings.HasPrefix(routePattern, prefix+"/")
}

// wrap composes registered middleware around h. Global middleware (prefix
// == "") is wrapped at registration time with zero per-request cost. When
// any path-scoped middleware is present, composition is deferred to request
// time and matched against the live URL via req.URL() rather than the route
// pattern string — this closes the bypass that pattern-string matching had
// for dynamic / wildcard routes (e.g. Get("/api/:section") serving
// /api/admin would not have triggered Use("/api/admin", auth) under the
// old scheme because the literal strings "/api/:section" and "/api/admin"
// do not share a prefix).
// applyMeta wraps h with a preamble that populates Request.paramNames
// from meta and validates each typed constraint. The preamble runs
// BEFORE middleware so middleware can call req.Param(name), and a
// failed constraint short-circuits with a 404 without running the
// middleware chain or handler.
//
// meta == nil is the fast path: returns h unchanged.
func (a *App) applyMeta(meta *routeMeta, h Handler) Handler {
	if meta == nil {
		return h
	}
	names := meta.paramNames
	constraints := meta.constraints
	inner := h
	return func(res *Response, req *Request) {
		if len(names) > 0 {
			req.paramNames = names
		}
		for idx, c := range constraints {
			if !c.check(req.Parameter(idx)) {
				res.Send(404, "text/plain; charset=utf-8", "Not Found\n")
				return
			}
		}
		inner(res, req)
	}
}

// applyAppRefAsync wraps h so res.app points back to a before the
// handler chain runs. Used by GetAsync's fast path where the
// shared-memory dispatch into the worker bypasses the sync wrap
// that normally stamps res.app. Without this Response.Render and
// friends would see res.app == nil on every async-route request.
func (a *App) applyAppRefAsync(h AsyncHandler) AsyncHandler {
	app := a
	trustProxy := a.cfg.TrustProxy
	return func(res *Response, req *Request) {
		res.app = app
		if trustProxy {
			req.trustProxy = true
		}
		h(res, req)
	}
}

// applyMetaAsync is the async-handler twin of applyMeta. Used by the
// zero-cgo fast path for GetAsync, where the snapshot is built in C++
// and never sees the sync-side preamble — the worker goroutine has to
// populate snap.paramNames itself and run constraints there.
func (a *App) applyMetaAsync(meta *routeMeta, h AsyncHandler) AsyncHandler {
	if meta == nil {
		return h
	}
	names := meta.paramNames
	constraints := meta.constraints
	inner := h
	return func(res *Response, req *Request) {
		if len(names) > 0 && req.snap != nil {
			req.snap.paramNames = names
		}
		for idx, c := range constraints {
			if !c.check(req.Parameter(idx)) {
				res.Send(404, "text/plain; charset=utf-8", "Not Found\n")
				return
			}
		}
		inner(res, req)
	}
}

func (a *App) wrap(routePattern string, h Handler) Handler {
	trustProxy := a.cfg.TrustProxy
	if len(a.middlewares) == 0 {
		// No middleware: still wrap so res.app gets the back-pointer
		// (Response.Render and friends need it). One extra function
		// frame per request is cheaper than a wider invariant ("only
		// some handlers see res.app").
		inner := h
		return func(res *Response, req *Request) {
			res.app = a
			if trustProxy {
				req.trustProxy = true
			}
			inner(res, req)
		}
	}
	hasScoped := false
	for _, e := range a.middlewares {
		if e.prefix != "" {
			hasScoped = true
			break
		}
	}
	if !hasScoped {
		for i := len(a.middlewares) - 1; i >= 0; i-- {
			h = a.middlewares[i].mw(h)
		}
		inner := h
		return func(res *Response, req *Request) {
			res.app = a
			if trustProxy {
				req.trustProxy = true
			}
			inner(res, req)
		}
	}
	entries := make([]middlewareEntry, len(a.middlewares))
	copy(entries, a.middlewares)
	inner := h
	return func(res *Response, req *Request) {
		res.app = a
		if trustProxy {
			req.trustProxy = true
		}
		url := req.URL()
		chain := inner
		for i := len(entries) - 1; i >= 0; i-- {
			e := entries[i]
			if e.prefix == "" || urlUnderPrefix(url, e.prefix) {
				chain = e.mw(chain)
			}
		}
		chain(res, req)
	}
}

// urlUnderPrefix reports whether the request URL falls within the path
// prefix scope: exact match or any path under "prefix/".
func urlUnderPrefix(url, prefix string) bool {
	if url == prefix {
		return true
	}
	return strings.HasPrefix(url, prefix+"/")
}

// hasMatchingMiddleware reports whether any registered sync-only
// middleware could apply to a request that uWS will route to
// routePattern. Used by GetAsync to choose between the zero-cgo
// shared path and the sync wrapper fallback.
//
// Entries with alsoAsync=true (PlaceBoth) are ignored — their work
// already happens inside the worker via the async chain, so the
// fast path can keep running.
//
// Conservatively reports true when the pattern is dynamic (contains
// : or *) and any scoped sync-only middleware exists, because the
// pattern string alone does not tell us which URLs the route will
// actually serve.
func (a *App) hasMatchingMiddleware(routePattern string) bool {
	if len(a.middlewares) == 0 {
		return false
	}
	dynamic := strings.ContainsAny(routePattern, ":*")
	for _, e := range a.middlewares {
		if e.alsoAsync {
			continue
		}
		if e.prefix == "" {
			return true
		}
		if dynamic {
			return true
		}
		if mwMatches(e.prefix, routePattern) {
			return true
		}
	}
	return false
}

// hasSyncMiddleware reports whether any sync-side middleware could apply to
// routePattern. Unlike hasMatchingMiddleware, it includes PlaceBoth entries
// because static GET fallbacks execute on the sync route path and must not
// bypass app.Use(auth) / app.Use(logger) just because those middlewares also
// have an async twin.
func (a *App) hasSyncMiddleware(routePattern string) bool {
	if len(a.middlewares) == 0 {
		return false
	}
	dynamic := strings.ContainsAny(routePattern, ":*")
	for _, e := range a.middlewares {
		if e.prefix == "" {
			return true
		}
		if dynamic {
			return true
		}
		if mwMatches(e.prefix, routePattern) {
			return true
		}
	}
	return false
}

func hasRouteConstraints(meta *routeMeta) bool {
	return meta != nil && len(meta.constraints) > 0
}

// UseAsync is retained as a thin alias for App.Use to keep existing
// code compiling. App.Use now handles all middleware: bundled
// middleware carries its own placement, raw AsyncMiddleware values
// register in the async chain, and any custom middleware that must
// block on I/O can be wrapped via middleware.Async(...).
//
// Prefer App.Use in new code — UseAsync exists only for backward
// compatibility and will be removed once callers migrate.
func (a *App) UseAsync(args ...any) {
	a.Use(args...)
}

// wrapAsync composes registered async middleware around h. Mirrors wrap:
// global async MW wraps at registration; path-scoped async MW composes at
// request time against the live URL so dynamic routes can't bypass scoped
// middleware via pattern/URL mismatch.
func (a *App) wrapAsync(routePattern string, h AsyncHandler) AsyncHandler {
	return a.wrapAsyncFiltered(routePattern, h, false)
}

// wrapAsyncFiltered behaves like wrapAsync but, when skipAlsoSync is
// true, omits PlaceBoth entries (alsoSync == true). The slow path
// for async routes calls this with skipAlsoSync=true because the
// sync wrapper has already fired the twin entries — running them
// again from the async chain would double-execute.
func (a *App) wrapAsyncFiltered(routePattern string, h AsyncHandler, skipAlsoSync bool) AsyncHandler {
	if len(a.asyncMiddlewares) == 0 {
		return h
	}
	hasScoped := false
	for _, e := range a.asyncMiddlewares {
		if e.prefix != "" {
			hasScoped = true
			break
		}
	}
	if !hasScoped {
		for i := len(a.asyncMiddlewares) - 1; i >= 0; i-- {
			e := a.asyncMiddlewares[i]
			if skipAlsoSync && e.alsoSync {
				continue
			}
			h = e.mw(h)
		}
		return h
	}
	entries := make([]asyncMiddlewareEntry, len(a.asyncMiddlewares))
	copy(entries, a.asyncMiddlewares)
	inner := h
	return func(res *Response, req *Request) {
		url := req.URL()
		chain := inner
		for i := len(entries) - 1; i >= 0; i-- {
			e := entries[i]
			if skipAlsoSync && e.alsoSync {
				continue
			}
			if e.prefix == "" || urlUnderPrefix(url, e.prefix) {
				chain = e.mw(chain)
			}
		}
		chain(res, req)
	}
}

// Get registers a GET route. The target may be:
//
//   - Handler / func(*Response, *Request) — dynamic, invoked per request via cgo
//   - string                              — static body served by C++ (no cgo per request)
//   - []byte                              — same as string
//   - Reply                               — static body with explicit status and Content-Type
//
// Static targets are served entirely in C++ with no Go work per request when
// no matching sync middleware or typed-param constraints need to run.
func (a *App) Get(pattern string, target any) {
	uwsPattern, meta := a.preRoute("get", pattern)
	switch v := target.(type) {
	case Handler:
		a.inner.get(uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, v)))
	case func(*Response, *Request):
		a.inner.get(uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, Handler(v))))
	case Reply:
		code := v.Status
		if code == 0 {
			code = 200
		}
		if v.ContentType != "" {
			validateHeaderValue("Content-Type", v.ContentType)
		}
		if hasRouteConstraints(meta) || a.hasSyncMiddleware(uwsPattern) {
			cType, body := v.ContentType, v.Body
			h := a.applyMeta(meta, a.wrap(uwsPattern, func(res *Response, req *Request) {
				res.Send(code, cType, body)
			}))
			a.inner.get(uwsPattern, h)
			return
		}
		a.inner.getStatic(uwsPattern, statusLine(code), v.ContentType, v.Body)
	case string:
		if hasRouteConstraints(meta) || a.hasSyncMiddleware(uwsPattern) {
			body := v
			h := a.applyMeta(meta, a.wrap(uwsPattern, func(res *Response, req *Request) {
				res.Send(200, "", body)
			}))
			a.inner.get(uwsPattern, h)
			return
		}
		a.inner.getStatic(uwsPattern, statusLine(200), "", v)
	case []byte:
		body := string(v)
		if hasRouteConstraints(meta) || a.hasSyncMiddleware(uwsPattern) {
			h := a.applyMeta(meta, a.wrap(uwsPattern, func(res *Response, req *Request) {
				res.Send(200, "", body)
			}))
			a.inner.get(uwsPattern, h)
			return
		}
		a.inner.getStatic(uwsPattern, statusLine(200), "", body)
	default:
		panic(fmt.Sprintf("gogo: unsupported Get target type %T for %q", target, pattern))
	}
}

// preRoute is the shared registration prelude: parse the user-facing
// pattern (extracting typed-param annotations into a routeMeta and
// stripping them to a uWS-compatible form), validate the stripped
// pattern, and track the method-routing entry. Returns the
// uWS-compatible pattern (for a.inner.* calls) and the meta (nil if
// no named or typed params, so the wrapper hot path stays clean).
func (a *App) preRoute(method, pattern string) (string, *routeMeta) {
	uwsPattern, meta := parseRoutePattern(pattern)
	validatePattern(uwsPattern)
	if method != "" {
		a.trackRouteMethod(method, uwsPattern, meta)
	}
	return uwsPattern, meta
}

// GetAsync registers a GET route whose handler runs on a goroutine and
// receives a Response in async mode plus a snapshot Request (URL/method/
// query/params/headers captured from the live uWS request before it was
// freed).
//
// Without middleware, GetAsync uses the zero-cgo shared-memory dispatch path:
// C++ snapshots the request, pushes onto a lock-free ring, and a long-lived
// Go worker pool drains it. SendShared in the handler completes the response
// without any cgo crossing per request.
//
// With middleware registered, GetAsync falls back to a sync cgo handler that
// runs the middleware chain with the live request, then captures a snapshot
// and switches to async mode for the user handler. One extra cgo callback
// per request only when middleware is in use.
func (a *App) GetAsync(pattern string, handler AsyncHandler) {
	uwsPattern, meta := a.preRoute("get", pattern)

	if !a.hasMatchingMiddleware(uwsPattern) {
		// No sync middleware matches → keep the zero-cgo shared-memory
		// dispatch path. The async chain composes inside the worker
		// goroutine alongside the user handler; PlaceBoth entries fire
		// here because no sync wrapper is running. applyMetaAsync sets
		// snap.paramNames and runs typed-param validation inside the
		// worker (the snapshot built in C++ has no Go-side meta).
		wrappedAsync := a.applyAppRefAsync(a.applyMetaAsync(meta, a.wrapAsync(uwsPattern, handler)))
		a.acquireSharedWorkerRef()
		a.inner.getShared(uwsPattern, wrappedAsync)
		return
	}

	// Slow path: a sync wrap runs the sync chain on the loop thread
	// before dispatching to the worker. PlaceBoth entries fire there,
	// so the async chain composed below must SKIP their twins —
	// otherwise the same middleware runs twice per request.
	// applyMeta on the sync side sets req.paramNames + runs typed
	// validation BEFORE the snapshot is built, so snapshotFromSync
	// then carries paramNames into the worker automatically.
	wrappedAsync := a.wrapAsyncFiltered(uwsPattern, handler, true)
	a.inner.get(uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, func(res *Response, req *Request) {
		snap := req.snapshotFromSync(a.cfg.CapturePeerIP)
		res.Async(func() {
			snapReq := requestPool.Get().(*Request)
			snapReq.snap = snap
			snapReq.res = res
			wrappedAsync(res, snapReq)
			snapReq.resetForPool()
			requestPool.Put(snapReq)
		})
	})))
}

// Post registers a POST route.
func (a *App) Post(pattern string, handler Handler) {
	uwsPattern, meta := a.preRoute("post", pattern)
	a.inner.post(uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, handler)))
}

// postSharedBodyCap mirrors SNAP_BODY_CAP in the C++ bridge. POST
// routes whose maxBodyBytes fits under this cap route through the
// zero-cgo shared-dispatch path; larger bodies fall back to the
// cgo-mediated async path that collects via res.Body. Keep these
// two constants in sync with the C side (gogo/uws_bridge.cpp).
const postSharedBodyCap = 8 * 1024

// BodyAsyncHandler is the handler signature for async routes that collect a
// request body before dispatching to a goroutine. It receives the response, a
// snapshot of the request (URL/query/params/headers all captured before uWS
// freed the live request), and the fully-collected body.
type BodyAsyncHandler func(res *Response, req *Request, body []byte)

// PostAsyncHandler is kept for source compatibility with earlier releases.
type PostAsyncHandler = BodyAsyncHandler

// PostAsync registers a POST route that collects the full request body up to
// maxBodyBytes, then invokes handler on a goroutine with the collected bytes
// plus a request snapshot. On bodies that exceed maxBodyBytes the framework
// sends 413 Payload Too Large automatically; on Config.BodyReadTimeout it sends
// 408 Request Timeout. In both cases the handler is not called.
func (a *App) PostAsync(pattern string, maxBodyBytes int, handler PostAsyncHandler) {
	a.bodyAsync("post", pattern, maxBodyBytes, handler)
}

// PutAsync registers a PUT route that collects the full request body up to
// maxBodyBytes, then invokes handler on a goroutine with the collected bytes
// plus a request snapshot. On bodies that exceed maxBodyBytes the framework
// sends 413 Payload Too Large automatically; on Config.BodyReadTimeout it sends
// 408 Request Timeout. In both cases the handler is not called.
func (a *App) PutAsync(pattern string, maxBodyBytes int, handler BodyAsyncHandler) {
	a.bodyAsync("put", pattern, maxBodyBytes, handler)
}

// PatchAsync registers a PATCH route that collects the full request body up to
// maxBodyBytes, then invokes handler on a goroutine with the collected bytes
// plus a request snapshot. On bodies that exceed maxBodyBytes the framework
// sends 413 Payload Too Large automatically; on Config.BodyReadTimeout it sends
// 408 Request Timeout. In both cases the handler is not called.
func (a *App) PatchAsync(pattern string, maxBodyBytes int, handler BodyAsyncHandler) {
	a.bodyAsync("patch", pattern, maxBodyBytes, handler)
}

// DeleteAsync registers a DELETE route that collects the full request body up
// to maxBodyBytes, then invokes handler on a goroutine with the collected
// bytes plus a request snapshot. On bodies that exceed maxBodyBytes the
// framework sends 413 Payload Too Large automatically; on
// Config.BodyReadTimeout it sends 408 Request Timeout. In both cases the
// handler is not called.
func (a *App) DeleteAsync(pattern string, maxBodyBytes int, handler BodyAsyncHandler) {
	a.bodyAsync("delete", pattern, maxBodyBytes, handler)
}

func (a *App) bodyAsync(method, pattern string, maxBodyBytes int, handler BodyAsyncHandler) {
	uwsPattern, meta := a.preRoute(method, pattern)

	// Adapt the body-receiving handler into the AsyncHandler shape that
	// AsyncMiddleware expects. The body is stashed on req.body in the
	// runtime wrapper below; middleware can read it via req.Body() and the
	// final user handler still receives it as a parameter.
	finalAsync := AsyncHandler(func(res *Response, req *Request) {
		handler(res, req, req.body)
	})

	// Zero-cgo fast path: small bodies (within the per-ctx
	// SNAP_BODY_CAP) and no sync middleware → register on the
	// shared-dispatch ring with body collection in C++. The worker
	// goroutine reads the assembled body via req.snap + req.body
	// without paying a single cgo crossing per request.
	if method == "post" &&
		maxBodyBytes > 0 && maxBodyBytes <= postSharedBodyCap &&
		meta == nil && !a.hasMatchingMiddleware(uwsPattern) {
		wrappedAsync := a.applyAppRefAsync(a.applyMetaAsync(meta, a.wrapAsync(uwsPattern, finalAsync)))
		a.acquireSharedWorkerRef()
		a.inner.postShared(uwsPattern, wrappedAsync, maxBodyBytes)
		return
	}

	// Body async methods run the sync chain via a.wrap before
	// dispatching the worker — PlaceBoth twins fire there, so the
	// async chain composed inside res.Async must skip them.
	wrappedAsync := a.wrapAsyncFiltered(uwsPattern, finalAsync, true)

	a.registerBodyRoute(method, uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, func(res *Response, req *Request) {
		// Snapshot the request before its lifetime ends. Body collection
		// happens via onData callbacks fired after we return, by which time
		// req would be invalid; the snapshot survives.
		snap := req.snapshotFromSync(a.cfg.CapturePeerIP)
		res.Body(maxBodyBytes, func(body []byte, err error) {
			if handleBodyCollectionError(res, err) {
				return
			}
			// Async spawns a goroutine and re-arms onAborted with the async
			// ctx hookup. Body's onAborted has fulfilled its purpose by now.
			res.Async(func() {
				snapReq := requestPool.Get().(*Request)
				snapReq.snap = snap
				snapReq.body = body
				snapReq.res = res
				wrappedAsync(res, snapReq)
				snapReq.resetForPool()
				requestPool.Put(snapReq)
			})
		})
	})))
}

func handleBodyCollectionError(res *Response, err error) bool {
	switch err {
	case nil:
		return false
	case ErrBodyTooLarge:
		res.Send(413, "text/plain; charset=utf-8", "payload too large\n")
	case ErrBodyTimeout:
		res.Send(408, "text/plain; charset=utf-8", "request timeout\n")
	default:
		reportPanic(err)
		res.Send(500, "text/plain; charset=utf-8", "internal server error\n")
	}
	return true
}

func (a *App) registerBodyRoute(method, pattern string, handler Handler) {
	switch method {
	case "post":
		a.inner.post(pattern, handler)
	case "put":
		a.inner.put(pattern, handler)
	case "patch":
		a.inner.patch(pattern, handler)
	case "delete":
		a.inner.deleteM(pattern, handler)
	default:
		panic(fmt.Sprintf("gogo: unsupported async body method %q for %q", method, pattern))
	}
}

// Any registers a route for every HTTP method.
func (a *App) Any(pattern string, handler Handler) {
	uwsPattern, meta := a.preRoute("", pattern)
	if uwsPattern == "/*" {
		a.userCatchAllRegistered = true
	}
	a.inner.any(uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, handler)))
}

// Put registers a PUT route. PUT requests carry bodies and are subject
// to BodyLimit, like Post.
func (a *App) Put(pattern string, handler Handler) {
	uwsPattern, meta := a.preRoute("put", pattern)
	a.inner.put(uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, handler)))
}

// Patch registers a PATCH route. PATCH requests carry bodies and are
// subject to BodyLimit, like Post.
func (a *App) Patch(pattern string, handler Handler) {
	uwsPattern, meta := a.preRoute("patch", pattern)
	a.inner.patch(uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, handler)))
}

// Delete registers a DELETE route. DELETE may carry a body per
// RFC 9110 §9.3.5 and is subject to BodyLimit.
func (a *App) Delete(pattern string, handler Handler) {
	uwsPattern, meta := a.preRoute("delete", pattern)
	a.inner.deleteM(uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, handler)))
}

// Options registers an OPTIONS route. OPTIONS is bodyless and skips
// the BodyLimit check.
func (a *App) Options(pattern string, handler Handler) {
	uwsPattern, meta := a.preRoute("options", pattern)
	a.inner.options(uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, handler)))
}

// Head registers a HEAD route. HEAD is bodyless and skips the
// BodyLimit check. Per RFC 9110, HEAD responses must omit the body;
// the framework does not enforce this — handlers should call res.End("")
// after writing the headers.
func (a *App) Head(pattern string, handler Handler) {
	uwsPattern, meta := a.preRoute("head", pattern)
	a.inner.head(uwsPattern, a.applyMeta(meta, a.wrap(uwsPattern, handler)))
}

// WebSocket registers a WebSocket route.
func (a *App) WebSocket(pattern string, behavior WebSocketBehavior) {
	validatePattern(pattern)
	behavior.app = a
	a.inner.websocket(pattern, behavior)
}

// Publish broadcasts a WebSocket message to every subscriber of topic.
// Use it from outside a WebSocket handler — typically a worker
// goroutine that finished some work and wants to notify connected
// clients — where calling WebSocket.Publish directly would touch
// uWS's loop-thread-local TopicTree from the wrong thread.
//
// In RunMultiCore mode the publish is dispatched onto every peer
// App's loop so subscribers on all cores receive it. The topic +
// message bytes are copied before scheduling, so the caller's
// buffers can be reused or reclaimed as soon as Publish returns.
//
// Topics are exact-match strings — uWS's TopicTree v20 does not
// support MQTT-style "+" / "#" wildcards. Publish to the same string
// each subscriber used in ws.Subscribe.
//
// opcode picks the WebSocket frame type (Text / Binary). For JSON
// payloads use Text so browser clients receive them as strings via
// onmessage.data.
//
// Returns immediately — delivery happens asynchronously on the loop(s).
// There is no error / delivery-count return because the loop may not
// have processed the publish yet when this returns; uWS itself does
// not surface that count back to the publisher.
// Calls after Shutdown, ShutdownGracefully, or Close are ignored.
//
// Performance — pick the right entry point:
//   - Inside an Open/Message/Close handler (loop thread): prefer
//     WebSocket.Publish. On a single App it bypasses the cross-thread defer
//     mutex and message copy. In RunMultiCore it still has to schedule one
//     copied peer-loop publish per other App, so the cost is O(peer loops).
//   - From a worker goroutine, single message: App.Publish. Measures
//     ~750 ns/op on this VM end-to-end including cgo + heap copy +
//     Loop::defer mutex + wakeup.
//   - From a worker goroutine, two or more messages at once (fan-out,
//     batch notification): App.PublishBatch — one cgo crossing + one
//     defer mutex for the whole batch. Crossover is at N=2 on this
//     VM (PublishBatch beats a Publish loop from there up), climbing
//     to ~8x faster at N=100. See PublishBatch's godoc for the full
//     measured curve.
func (a *App) Publish(topic string, message []byte, opcode OpCode) {
	if a.closed.Load() || a.stopping.Load() {
		return
	}
	peers := a.pubsubPeers
	if len(peers) > 1 {
		for _, peer := range peers {
			peer.publishLocal(topic, message, opcode)
		}
		return
	}
	a.publishLocal(topic, message, opcode)
}

func (a *App) publishLocal(topic string, message []byte, opcode OpCode) {
	if a.closed.Load() || a.stopping.Load() {
		return
	}
	a.nativeMu.RLock()
	defer a.nativeMu.RUnlock()
	if a.closed.Load() || a.stopping.Load() {
		return
	}
	a.inner.publish(topic, message, opcode)
}

func (a *App) publishPeersExcept(skip *App, topic string, message []byte, opcode OpCode) {
	for _, peer := range a.pubsubPeers {
		if peer != skip {
			peer.publishLocal(topic, message, opcode)
		}
	}
}

// PublishMessage is one entry in an App.PublishBatch call.
//
// Topic is the exact subscriber topic string (no wildcards — see
// App.Publish). Message is the payload bytes. OpCode picks Text vs
// Binary framing per-message, so a single batch can mix the two.
type PublishMessage struct {
	Topic   string
	Message []byte
	OpCode  OpCode
}

// PublishBatch broadcasts N WebSocket messages in a single cgo
// crossing with one Loop::defer (one mutex acquire, one wakeup) on
// the loop side. The whole batch is packed into one contiguous Go
// buffer + a parallel POD-only metadata array, copied once into the
// loop's heap, then iterated under the defer.
//
// Use this when a worker goroutine needs to push many messages at
// once — for example, a fan-out notification that has to land on
// multiple topics, or a periodic stats-tick that updates several
// dashboards. App.Publish in a loop pays the cgo + defer-mutex cost
// per call; PublishBatch pays it once for the whole batch.
//
// Measured speedup over the equivalent App.Publish loop on this VM
// (128-byte payload, 5 counts each, median ns per batch):
//
//	N=1     0.67x  (batch SLOWER — packing overhead > savings)
//	N=2     1.49x
//	N=5     2.61x
//	N=10    2.92x
//	N=50    6.24x
//	N=100   8.41x
//
// Crossover is at N=2 — below that, single App.Publish is faster.
// Per-publish cost drops from ~750 ns (App.Publish) to ~86 ns at
// N=100, so batching pays off hard for real fan-out workloads.
//
// Each PublishMessage's Topic and Message bytes are copied before
// the loop sees them, so caller buffers can be reused immediately.
// Mixed Text/Binary opcodes in one batch are fine.
//
// Returns immediately. Like Publish, delivery happens later on the
// loop(s) and there is no per-message delivery-count. In RunMultiCore
// mode the batch is dispatched once per peer App so subscribers on all
// cores receive it. Calls after
// Shutdown, ShutdownGracefully, or Close are ignored.
func (a *App) PublishBatch(msgs []PublishMessage) {
	if a.closed.Load() || a.stopping.Load() {
		return
	}
	peers := a.pubsubPeers
	if len(peers) > 1 {
		for _, peer := range peers {
			peer.publishBatchLocal(msgs)
		}
		return
	}
	a.publishBatchLocal(msgs)
}

func (a *App) publishBatchLocal(msgs []PublishMessage) {
	if a.closed.Load() || a.stopping.Load() {
		return
	}
	a.nativeMu.RLock()
	defer a.nativeMu.RUnlock()
	if a.closed.Load() || a.stopping.Load() {
		return
	}
	a.inner.publishBatch(msgs)
}

// Router scopes middleware and a path prefix to a subtree of routes. Created
// by App.Group or Router.Group. Routes registered through a Router have the
// Router's prefix prepended and inherit the Router's middleware stack on top
// of the App's global / scoped middleware.
//
// Group scope is identity-based — a route is wrapped because it is registered
// through this Router, not because its pattern string starts with some
// prefix. That avoids the pattern/URL mismatch that App.Use(prefix, mw) has
// to guard against at request time, so Group middleware composes at
// registration with zero per-request cost.
//
// Prefer Group over App.Use(prefix, mw) for scoping new code.
type Router struct {
	app     *App
	prefix  string
	syncMW  []Middleware
	asyncMW []AsyncMiddleware
}

// Group returns a Router scoped to prefix with mws applied to every route
// subsequently registered through it. Prefix must start with '/' and contain
// no wildcards; trailing slash is stripped so Group("/api") and Group("/api/")
// behave identically. Group("/") is equivalent to no prefix.
func (a *App) Group(prefix string, mws ...Middleware) *Router {
	return &Router{
		app:    a,
		prefix: normalizeGroupPrefix(prefix),
		syncMW: append([]Middleware(nil), mws...),
	}
}

// Group creates a nested Router. The child prefix is appended to the parent's
// prefix and middleware is inherited then extended — mws here run inside the
// parent group's middleware.
func (r *Router) Group(prefix string, mws ...Middleware) *Router {
	return &Router{
		app:     r.app,
		prefix:  r.prefix + normalizeGroupPrefix(prefix),
		syncMW:  append(append([]Middleware(nil), r.syncMW...), mws...),
		asyncMW: append([]AsyncMiddleware(nil), r.asyncMW...),
	}
}

// Use appends sync middleware to this Router. Applies to every route
// subsequently registered through this Router (or any child Group created
// after this call).
func (r *Router) Use(mws ...Middleware) {
	r.syncMW = append(r.syncMW, mws...)
}

// UseAsync appends async middleware to this Router. Applies only to GetAsync
// and body-async routes registered through this Router.
func (r *Router) UseAsync(mws ...AsyncMiddleware) {
	r.asyncMW = append(r.asyncMW, mws...)
}

// normalizeGroupPrefix validates a Group prefix and trims trailing slashes.
// Wildcards (* / **) are rejected because the prefix is concatenated literally
// onto child route patterns; expressing "everything under here" is the job of
// Group itself, not its prefix.
func normalizeGroupPrefix(p string) string {
	validatePattern(p)
	if strings.ContainsAny(p, "*") {
		panic(fmt.Sprintf("gogo: Group prefix %q must not contain wildcards", p))
	}
	for len(p) > 1 && p[len(p)-1] == '/' {
		p = p[:len(p)-1]
	}
	if p == "/" {
		return ""
	}
	return p
}

func (r *Router) wrapGroupSync(h Handler) Handler {
	for i := len(r.syncMW) - 1; i >= 0; i-- {
		h = r.syncMW[i](h)
	}
	return h
}

func (r *Router) wrapGroupAsync(h AsyncHandler) AsyncHandler {
	for i := len(r.asyncMW) - 1; i >= 0; i-- {
		h = r.asyncMW[i](h)
	}
	return h
}

// hasGroupOrAppMW reports whether any sync middleware (group or app, global
// or scoped) could touch a route registered through this Router. Used to
// decide whether a static target (Reply / string / []byte) can take the
// zero-cgo static path or must fall back to a dynamic handler so middleware
// can intercept.
func (r *Router) hasGroupOrAppMW(fullPattern string) bool {
	return len(r.syncMW) > 0 || r.app.hasSyncMiddleware(fullPattern)
}

// needsDynamicStatic reports whether a static target needs the dynamic path
// to preserve route semantics: middleware must run and typed-param
// constraints must reject before the static body is sent.
func (r *Router) needsDynamicStatic(meta *routeMeta, fullPattern string) bool {
	return hasRouteConstraints(meta) || r.hasGroupOrAppMW(fullPattern)
}

// Get registers a GET route under this Router. Target follows the same rules
// as App.Get: Handler, func, Reply, string, []byte. Static targets take the
// zero-cgo path only when neither middleware nor typed-param constraints need
// to run; otherwise the static body is served by a synthetic dynamic handler.
func (r *Router) Get(pattern string, target any) {
	full, meta := r.preRoute("get", pattern)
	switch v := target.(type) {
	case Handler:
		h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(v)))
		r.app.inner.get(full, h)
	case func(*Response, *Request):
		h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(Handler(v))))
		r.app.inner.get(full, h)
	case Reply:
		code := v.Status
		if code == 0 {
			code = 200
		}
		if v.ContentType != "" {
			validateHeaderValue("Content-Type", v.ContentType)
		}
		if !r.needsDynamicStatic(meta, full) {
			r.app.inner.getStatic(full, statusLine(code), v.ContentType, v.Body)
			return
		}
		cType, body := v.ContentType, v.Body
		h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(func(res *Response, req *Request) {
			res.Send(code, cType, body)
		})))
		r.app.inner.get(full, h)
	case string:
		if !r.needsDynamicStatic(meta, full) {
			r.app.inner.getStatic(full, statusLine(200), "", v)
			return
		}
		body := v
		h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(func(res *Response, req *Request) {
			res.Send(200, "", body)
		})))
		r.app.inner.get(full, h)
	case []byte:
		body := string(v)
		if !r.needsDynamicStatic(meta, full) {
			r.app.inner.getStatic(full, statusLine(200), "", body)
			return
		}
		h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(func(res *Response, req *Request) {
			res.Send(200, "", body)
		})))
		r.app.inner.get(full, h)
	default:
		panic(fmt.Sprintf("gogo: unsupported Get target type %T for %q", target, full))
	}
}

// preRoute is the Router's twin of App.preRoute. The child pattern must be a
// complete route fragment starting with '/', then it is concatenated with the
// router's prefix and parsed as a single unit so typed-param annotations
// anywhere along the full path are recognized — e.g. a
// Group("/users/:userID<int>") with a child Get("/posts/:postID<uuid>") yields
// a routeMeta covering both names and both constraints.
func (r *Router) preRoute(method, pattern string) (string, *routeMeta) {
	validatePattern(pattern)
	full, meta := parseRoutePattern(r.prefix + pattern)
	validatePattern(full)
	if method != "" {
		r.app.trackRouteMethod(method, full, meta)
	}
	return full, meta
}

// Post registers a POST route under this Router.
func (r *Router) Post(pattern string, handler Handler) {
	full, meta := r.preRoute("post", pattern)
	h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(handler)))
	r.app.inner.post(full, h)
}

// Any registers a route for every HTTP method under this Router.
func (r *Router) Any(pattern string, handler Handler) {
	full, meta := r.preRoute("", pattern)
	h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(handler)))
	r.app.inner.any(full, h)
}

// Put registers a PUT route under this Router.
func (r *Router) Put(pattern string, handler Handler) {
	full, meta := r.preRoute("put", pattern)
	h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(handler)))
	r.app.inner.put(full, h)
}

// Patch registers a PATCH route under this Router.
func (r *Router) Patch(pattern string, handler Handler) {
	full, meta := r.preRoute("patch", pattern)
	h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(handler)))
	r.app.inner.patch(full, h)
}

// Delete registers a DELETE route under this Router.
func (r *Router) Delete(pattern string, handler Handler) {
	full, meta := r.preRoute("delete", pattern)
	h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(handler)))
	r.app.inner.deleteM(full, h)
}

// Options registers an OPTIONS route under this Router.
func (r *Router) Options(pattern string, handler Handler) {
	full, meta := r.preRoute("options", pattern)
	h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(handler)))
	r.app.inner.options(full, h)
}

// Head registers a HEAD route under this Router.
func (r *Router) Head(pattern string, handler Handler) {
	full, meta := r.preRoute("head", pattern)
	h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(handler)))
	r.app.inner.head(full, h)
}

// GetAsync registers a GET route under this Router that runs on a goroutine.
// Uses the zero-cgo shared-memory dispatch path only when no sync middleware
// (group or app) touches this route.
func (r *Router) GetAsync(pattern string, handler AsyncHandler) {
	full, meta := r.preRoute("get", pattern)

	if len(r.syncMW) == 0 && !r.app.hasMatchingMiddleware(full) {
		// Fast path — no sync wrapper fires, async chain owns all
		// middleware. applyMetaAsync sets snap.paramNames and runs
		// typed validation inside the worker; applyAppRefAsync sets
		// res.app so Response.Render works on async fast-path routes.
		wrappedAsync := r.app.applyAppRefAsync(r.app.applyMetaAsync(meta, r.app.wrapAsync(full, r.wrapGroupAsync(handler))))
		r.app.acquireSharedWorkerRef()
		r.app.inner.getShared(full, wrappedAsync)
		return
	}

	// Slow path: sync wrapper runs PlaceBoth twins on the loop
	// thread, so the async chain must skip alsoSync entries. The
	// sync side's applyMeta sets req.paramNames + runs typed
	// validation before snapshotFromSync carries names into the
	// worker.
	wrappedAsync := r.app.wrapAsyncFiltered(full, r.wrapGroupAsync(handler), true)
	syncEntry := func(res *Response, req *Request) {
		snap := req.snapshotFromSync(r.app.cfg.CapturePeerIP)
		res.Async(func() {
			snapReq := requestPool.Get().(*Request)
			snapReq.snap = snap
			snapReq.res = res
			wrappedAsync(res, snapReq)
			snapReq.resetForPool()
			requestPool.Put(snapReq)
		})
	}
	h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(Handler(syncEntry))))
	r.app.inner.get(full, h)
}

// PostAsync registers a POST route under this Router that collects the body
// up to maxBodyBytes then runs handler on a goroutine. On bodies over the cap
// the framework sends 413; on Config.BodyReadTimeout it sends 408. In both
// cases the handler is not called.
func (r *Router) PostAsync(pattern string, maxBodyBytes int, handler PostAsyncHandler) {
	r.bodyAsync("post", pattern, maxBodyBytes, handler)
}

// PutAsync registers a PUT route under this Router that collects the body up
// to maxBodyBytes then runs handler on a goroutine. On bodies over the cap the
// framework sends 413; on Config.BodyReadTimeout it sends 408. In both cases
// the handler is not called.
func (r *Router) PutAsync(pattern string, maxBodyBytes int, handler BodyAsyncHandler) {
	r.bodyAsync("put", pattern, maxBodyBytes, handler)
}

// PatchAsync registers a PATCH route under this Router that collects the body
// up to maxBodyBytes then runs handler on a goroutine. On bodies over the cap
// the framework sends 413; on Config.BodyReadTimeout it sends 408. In both
// cases the handler is not called.
func (r *Router) PatchAsync(pattern string, maxBodyBytes int, handler BodyAsyncHandler) {
	r.bodyAsync("patch", pattern, maxBodyBytes, handler)
}

// DeleteAsync registers a DELETE route under this Router that collects the
// body up to maxBodyBytes then runs handler on a goroutine. On bodies over the
// cap the framework sends 413; on Config.BodyReadTimeout it sends 408. In both
// cases the handler is not called.
func (r *Router) DeleteAsync(pattern string, maxBodyBytes int, handler BodyAsyncHandler) {
	r.bodyAsync("delete", pattern, maxBodyBytes, handler)
}

func (r *Router) bodyAsync(method, pattern string, maxBodyBytes int, handler BodyAsyncHandler) {
	full, meta := r.preRoute(method, pattern)

	finalAsync := AsyncHandler(func(res *Response, req *Request) {
		handler(res, req, req.body)
	})
	// Body async methods wrap with the sync chain first, so skip
	// PlaceBoth twins in the async chain to avoid double-firing the
	// same middleware.
	wrappedAsync := r.app.wrapAsyncFiltered(full, r.wrapGroupAsync(finalAsync), true)

	syncEntry := func(res *Response, req *Request) {
		snap := req.snapshotFromSync(r.app.cfg.CapturePeerIP)
		res.Body(maxBodyBytes, func(body []byte, err error) {
			if handleBodyCollectionError(res, err) {
				return
			}
			res.Async(func() {
				snapReq := requestPool.Get().(*Request)
				snapReq.snap = snap
				snapReq.body = body
				snapReq.res = res
				wrappedAsync(res, snapReq)
				snapReq.resetForPool()
				requestPool.Put(snapReq)
			})
		})
	}
	h := r.app.applyMeta(meta, r.app.wrap(full, r.wrapGroupSync(Handler(syncEntry))))
	r.app.registerBodyRoute(method, full, h)
}

// WebSocket registers a WebSocket route under this Router. Middleware does
// not run around WebSocket upgrade — uWS does not expose a chain at the
// upgrade boundary.
func (r *Router) WebSocket(pattern string, behavior WebSocketBehavior) {
	validatePattern(pattern)
	full := r.prefix + pattern
	behavior.app = r.app
	r.app.inner.websocket(full, behavior)
}

// NotFound sets the fallback handler for requests that don't match any
// registered route. The framework registers it as the lowest-priority
// catch-all (any-method /*) just before Listen binds — explicit user
// routes always win. Without a registered NotFound handler uWS falls
// back to its built-in 404 reply, which has no body and no
// customisation. Calling NotFound(nil) clears the handler.
//
// Middleware registered before Listen wraps the NotFound handler the
// same way it wraps any other dynamic route.
func (a *App) NotFound(h Handler) {
	a.notFoundHandler = h
}

// Name tags a previously-registered route pattern with a name so App.URL can
// perform reverse routing. Route registration methods intentionally do not
// return fluent route handles; use Name explicitly when a route needs reverse
// routing. The pattern must be the same (post-strip) form that the route was
// registered with — typically the literal string you passed to Get / Post /
// etc., minus any <type> annotations. The simplest usage:
//
//	app.Get("/users/:id", showUser)
//	app.Name("user.show", "/users/:id")
//
//	url, _ := app.URL("user.show", map[string]string{"id": "42"})
//	// url == "/users/42"
//
// Name overwrites any previous mapping for the same name.
func (a *App) Name(name, pattern string) {
	if name == "" {
		panic("gogo: Name requires a non-empty name")
	}
	stripped, _ := parseRoutePattern(pattern)
	if a.namedRoutes == nil {
		a.namedRoutes = make(map[string]string)
	}
	a.namedRoutes[name] = stripped
}

// URL builds the path for a named route by substituting params into
// each :name segment of the pattern. Wildcards (`*`, `**`) cannot be
// reverse-routed — the function returns an error if the pattern
// contains them.
//
//	app.Get("/users/:userID/posts/:postID", showPost)
//	app.Name("post.show", "/users/:userID/posts/:postID")
//
//	url, err := app.URL("post.show", map[string]string{
//	    "userID": "alice",
//	    "postID": "42",
//	})
//	// url == "/users/alice/posts/42"
//
// Returns an error when:
//   - the name is unknown,
//   - the pattern references a :param missing from the params map,
//   - the pattern contains a wildcard (* or **).
func (a *App) URL(name string, params map[string]string) (string, error) {
	pattern, ok := a.namedRoutes[name]
	if !ok {
		return "", fmt.Errorf("gogo: URL: no route named %q", name)
	}
	var out strings.Builder
	out.Grow(len(pattern))
	i := 0
	for i < len(pattern) {
		c := pattern[i]
		switch c {
		case '*':
			return "", fmt.Errorf("gogo: URL: route %q has a wildcard %q and cannot be reverse-routed", name, pattern)
		case ':':
			start := i + 1
			j := start
			for j < len(pattern) && pattern[j] != '/' {
				j++
			}
			pName := pattern[start:j]
			val, ok := params[pName]
			if !ok {
				return "", fmt.Errorf("gogo: URL: route %q requires param %q", name, pName)
			}
			out.WriteString(url.PathEscape(val))
			i = j
		default:
			out.WriteByte(c)
			i++
		}
	}
	return out.String(), nil
}

// Name tags a route pattern under this Router's prefix for App.URL reverse
// routing. The pattern is the same one you pass to Router.Get/Post/etc.; the
// router prefix is applied automatically.
//
//	api := app.Group("/api/v1")
//	api.Get("/users/:id", showUser)
//	api.Name("api.user.show", "/users/:id")
//
//	url, _ := app.URL("api.user.show", map[string]string{"id": "42"})
//	// url == "/api/v1/users/42"
func (r *Router) Name(name, pattern string) {
	if r == nil || r.app == nil {
		panic("gogo: Router.Name called on nil Router")
	}
	validatePattern(pattern)
	r.app.Name(name, r.prefix+pattern)
}

// Mount registers routes onto a sub-router rooted at prefix and runs
// the provided callback against it. It is sugar over App.Group plus
// the callback pattern that Express / Fiber users expect:
//
//	app.Mount("/api/v1", func(api *gogo.Router) {
//	    api.Use(middleware.JWT(opts))
//	    api.Get("/users", listUsers)
//	    api.Post("/users", createUser)
//	})
//
// Equivalent to:
//
//	api := app.Group("/api/v1")
//	api.Use(middleware.JWT(opts))
//	api.Get("/users", listUsers)
//	api.Post("/users", createUser)
//
// Mount returns the Router in case the caller wants to register more
// routes against it after the callback returns. Calling Mount twice
// with the same prefix creates two independent Routers — there is no
// merging across calls.
func (a *App) Mount(prefix string, register func(r *Router)) *Router {
	r := a.Group(prefix)
	if register != nil {
		register(r)
	}
	return r
}

// Listen binds the app to the given port and reports whether binding
// succeeded. The bind interface comes from Config.BindAddr; an empty
// BindAddr keeps the uWS default of all interfaces (0.0.0.0).
//
// If a NotFound handler is registered, Listen wires it as the catch-all
// route immediately before binding so user-registered routes retain
// precedence.
func (a *App) Listen(port int) bool {
	// Single catch-all when any of these is set:
	//   - NotFound or MethodNotAllowed → custom 404 / 405 bodies.
	//   - Global middleware → fires for unmatched paths too, so that
	//     things like CORS preflight (OPTIONS /unknown) and request
	//     logging see every request the way express / fiber middleware
	//     does. Without this, uWS short-circuits unmatched paths to
	//     its built-in 404 before the middleware chain runs.
	//
	// User-registered routes always win specificity (literal >
	// parametric > wildcard) so the catch-all only fires when nothing
	// else matched.
	needCatchAll := a.notFoundHandler != nil ||
		a.methodNotAllowedHandler != nil ||
		len(a.middlewares) > 0
	if needCatchAll && !a.userCatchAllRegistered {
		a.inner.any("/*", a.wrap("/*", a.catchAllRoutingHandler()))
	}
	ok := a.inner.listen(a.cfg.BindAddr, port)
	if ok {
		a.fireListenHooks(port)
	}
	return ok
}

// MethodNotAllowed sets the fallback handler for requests whose path
// matches a registered route but whose method has no registered
// handler, e.g. app.Get("/users", h) and the client sends POST
// /users. Literal, parametric, typed-param, and terminal wildcard
// routes participate in the lookup. A typed-param route counts only
// when the live path satisfies its constraint; /api/* matches /api/
// and descendants, but not bare /api, matching uWS route behavior.
//
// The default fallback writes a 405 response with the standard Allow
// header. Custom handlers are responsible for their own response and
// can call AllowedMethods(req.URL()) to build the Allow header.
//
// Calling MethodNotAllowed(nil) clears the handler.
func (a *App) MethodNotAllowed(h Handler) {
	a.methodNotAllowedHandler = h
}

type routeMethodSegmentKind uint8

const (
	routeMethodSegmentStatic routeMethodSegmentKind = iota
	routeMethodSegmentParam
	routeMethodSegmentWildcard
)

type routeMethodSegment struct {
	kind  routeMethodSegmentKind
	value string
}

type routeMethodEntry struct {
	method   string
	segments []routeMethodSegment
	meta     *routeMeta
}

func newRouteMethodEntry(method, pattern string, meta *routeMeta) routeMethodEntry {
	rawSegments := splitRoutePath(pattern)
	segments := make([]routeMethodSegment, len(rawSegments))
	for i, segment := range rawSegments {
		kind := routeMethodSegmentStatic
		if segment != "" {
			switch segment[0] {
			case ':':
				kind = routeMethodSegmentParam
			case '*':
				kind = routeMethodSegmentWildcard
			}
		}
		segments[i] = routeMethodSegment{kind: kind, value: segment}
	}
	return routeMethodEntry{
		method:   method,
		segments: segments,
		meta:     meta,
	}
}

func splitRoutePath(path string) []string {
	if path == "" || path[0] != '/' {
		return nil
	}
	segments := make([]string, 0, strings.Count(path, "/"))
	for len(path) > 0 {
		path = path[1:]
		nextSlash := strings.IndexByte(path, '/')
		if nextSlash < 0 {
			return append(segments, path)
		}
		segments = append(segments, path[:nextSlash])
		path = path[nextSlash:]
	}
	return segments
}

func (e routeMethodEntry) matches(path string) bool {
	pathSegments := splitRoutePath(path)
	if len(pathSegments) == 0 && path != "/" {
		return false
	}
	paramIdx := 0
	for i, segment := range e.segments {
		if i >= len(pathSegments) {
			return false
		}
		value := pathSegments[i]
		switch segment.kind {
		case routeMethodSegmentWildcard:
			return i == len(e.segments)-1
		case routeMethodSegmentParam:
			if value == "" {
				return false
			}
			if e.meta != nil {
				if constraint, ok := e.meta.constraints[paramIdx]; ok && !constraint.check(value) {
					return false
				}
			}
			paramIdx++
		default:
			if value != segment.value {
				return false
			}
		}
	}
	return len(pathSegments) == len(e.segments)
}

func routeMethodKey(method, pattern string) string {
	return method + "\x00" + pattern
}

// trackRouteMethod is called from every method-specific route-registration
// helper so the catch-all installed at Listen can distinguish 404 from 405.
func (a *App) trackRouteMethod(method, pattern string, meta *routeMeta) {
	entry := newRouteMethodEntry(method, pattern, meta)
	if a.routeMethodIndex == nil {
		a.routeMethodIndex = make(map[string]int)
	}
	key := routeMethodKey(method, pattern)
	if idx, ok := a.routeMethodIndex[key]; ok {
		a.routeMethods[idx] = entry
		return
	}
	a.routeMethodIndex[key] = len(a.routeMethods)
	a.routeMethods = append(a.routeMethods, entry)
}

// AllowedMethods returns the HTTP methods registered for path (uppercase,
// canonical order). Use it inside a MethodNotAllowed handler to build
// the standard Allow header for 405 responses:
//
//	app.MethodNotAllowed(func(res *gogo.Response, req *gogo.Request) {
//	    allow := strings.Join(app.AllowedMethods(req.URL()), ", ")
//	    res.Status(405)
//	    res.Header("Allow", allow)
//	    res.Header("Content-Type", "text/plain")
//	    res.End("no\n")
//	})
//
// Returns nil if the path has no registered routes. Parametric and wildcard
// routes are included using the same route semantics described on
// MethodNotAllowed.
func (a *App) AllowedMethods(path string) []string {
	methods := a.allowedMethodSet(path)
	if len(methods) == 0 {
		return nil
	}
	out := make([]string, 0, len(methods))
	for k := range methods {
		out = append(out, strings.ToUpper(k))
	}
	sort.Strings(out)
	return out
}

func (a *App) allowedMethodSet(path string) map[string]struct{} {
	var methods map[string]struct{}
	for _, entry := range a.routeMethods {
		if !entry.matches(path) {
			continue
		}
		if methods == nil {
			methods = make(map[string]struct{})
		}
		methods[entry.method] = struct{}{}
	}
	return methods
}

// catchAllRoutingHandler is the single Any("/*") handler installed at
// Listen when either NotFound or MethodNotAllowed is configured. It
// looks up the live URL in routeMethods to decide which fallback
// applies. Framework defaults emit a properly-ordered status / Allow /
// content-type / body so the response is well-formed; custom handlers
// are responsible for the full response themselves (see AllowedMethods
// for the Allow-header helper).
func (a *App) catchAllRoutingHandler() Handler {
	return func(res *Response, req *Request) {
		url := req.URL()
		if methods := a.allowedMethodSet(url); len(methods) > 0 {
			// Path exists; this is a method mismatch → 405.
			if a.methodNotAllowedHandler != nil {
				a.methodNotAllowedHandler(res, req)
				return
			}
			res.Status(405)
			res.Header("Allow", joinAllowHeader(methods))
			res.Header("Content-Type", "text/plain; charset=utf-8")
			res.End("method not allowed\n")
			return
		}
		if a.notFoundHandler != nil {
			a.notFoundHandler(res, req)
			return
		}
		res.Status(404)
		res.Header("Content-Type", "text/plain; charset=utf-8")
		res.End("not found\n")
	}
}

// joinAllowHeader formats a set of HTTP methods (uWS-style lowercase)
// into the canonical comma-separated Allow header form.
func joinAllowHeader(methods map[string]struct{}) string {
	if len(methods) == 0 {
		return ""
	}
	ordered := make([]string, 0, len(methods))
	for m := range methods {
		ordered = append(ordered, strings.ToUpper(m))
	}
	// Stable order so the header is deterministic between requests.
	sort.Strings(ordered)
	return strings.Join(ordered, ", ")
}

// OnListen registers a callback that fires synchronously after Listen
// binds the socket successfully, before Listen returns. Common uses:
// logging the bound address, registering with a service discovery
// agent, sending a "ready" signal to a supervisor. Hooks run in the
// order they were registered and panic-recover at framework level so a
// misbehaving hook can't block the rest. Safe to call before or after
// route registration; not safe to call concurrently with Listen. Calling
// OnListen(nil) is a no-op.
func (a *App) OnListen(fn func(port int)) {
	if fn == nil {
		return
	}
	a.onListenHooks = append(a.onListenHooks, fn)
}

// fireListenHooks runs every registered OnListen callback with per-callback
// panic recovery so a buggy hook cannot prevent later hooks from running.
func (a *App) fireListenHooks(port int) {
	for _, fn := range a.onListenHooks {
		func(f func(int)) {
			defer func() {
				if r := recover(); r != nil {
					reportPanic(r)
				}
			}()
			f(port)
		}(fn)
	}
}

// OnShutdown registers a callback that fires synchronously at the start
// of Shutdown / ShutdownGracefully (before the C++ close is dispatched
// to the loop). Use it to flush logs, close DB pools, etc. Hooks run in
// registration order and run on whatever goroutine called Shutdown.
// Hooks fire at most once per App lifecycle, even if Shutdown and
// ShutdownGracefully are both called. Calling OnShutdown(nil) is a no-op.
func (a *App) OnShutdown(fn func()) {
	if fn == nil {
		return
	}
	a.onShutdownHooks = append(a.onShutdownHooks, fn)
}

// fireShutdownHooks runs every registered OnShutdown callback with
// per-callback panic recovery so a buggy hook doesn't strand the
// shutdown.
func (a *App) fireShutdownHooks() {
	if !a.shutdownHooksFired.CompareAndSwap(false, true) {
		return
	}
	for _, fn := range a.onShutdownHooks {
		func(f func()) {
			defer func() {
				if r := recover(); r != nil {
					reportPanic(r)
				}
			}()
			f()
		}(fn)
	}
}

// Run starts the uWebSockets event loop and blocks. Before running, installs
// the shared-memory drain timer on this loop so SendShared responses can be
// flushed by the loop thread.
func (a *App) Run() {
	a.inner.startSharedDrain(200) // 200μs drain interval
	a.inner.run()
}

// Shutdown stops the app immediately: the listen socket and every
// active connection are closed at once. Run returns as soon as the
// loop drains. In-flight responses are dropped — use ShutdownGracefully
// when you need to wait for active clients to finish.
//
// Safe to call from any goroutine; idempotent. Returns immediately —
// call Close after Run returns to free native resources. Registered
// OnShutdown hooks fire synchronously before the close is dispatched.
func (a *App) Shutdown() {
	if a.closed.Load() {
		return
	}
	a.stopping.Store(true)
	a.fireShutdownHooks()
	a.nativeMu.Lock()
	defer a.nativeMu.Unlock()
	if a.closed.Load() {
		return
	}
	a.inner.stop()
}

// ShutdownGracefully closes only the listen socket so no new connections
// arrive, then waits up to timeout for the already-accepted connections
// to finish their in-flight responses naturally. If the timeout fires
// before everything drains, the remaining sockets are force-closed via
// Shutdown so the loop can exit. timeout = 0 disables the force-close
// (wait indefinitely).
//
// Returns immediately — the wait + force-close run on a background
// goroutine. Call Close after Run returns. Registered OnShutdown hooks
// fire synchronously before the listen socket is closed. Close waits
// for the force-close goroutine to finish before freeing native
// resources, so it is always safe to call Close after Run returns
// regardless of how the loop exited.
func (a *App) ShutdownGracefully(timeout time.Duration) {
	if a.closed.Load() {
		return
	}
	a.stopping.Store(true)
	a.fireShutdownHooks()
	a.nativeMu.Lock()
	if a.closed.Load() {
		a.nativeMu.Unlock()
		return
	}
	a.inner.closeListen()
	a.nativeMu.Unlock()
	if timeout <= 0 {
		return
	}
	a.pendingTimers.Add(1)
	go func() {
		defer a.pendingTimers.Add(-1)
		time.Sleep(timeout)
		if a.closed.Load() {
			return
		}
		a.nativeMu.Lock()
		defer a.nativeMu.Unlock()
		if a.closed.Load() {
			return
		}
		a.inner.stop()
	}()
}

// Close frees native resources. Call it only after Run has returned, or
// before Run if the app was never started. Waits for any in-flight
// ShutdownGracefully force-close goroutine to settle so a delayed
// timer can't make a cgo call against a freed app pointer.
//
// Drops this App's reference on the shared-dispatch worker pool if
// it registered any shared fast-path routes. When the last shared
// App in the process is closed the worker goroutines exit cleanly so
// long-running supervisors (tests, hot-reload, multi-tenant hosts)
// don't accumulate dead workers spinning against a ring no app is
// feeding anymore.
func (a *App) Close() {
	a.stopping.Store(true)
	a.closed.Store(true)
	// Drain any in-flight ShutdownGracefully timer goroutine before
	// freeing native resources. Spin with Gosched — the only callers
	// are timer goroutines that have already passed their sleep, so
	// the wait is microseconds at most.
	for a.pendingTimers.Load() > 0 {
		runtime.Gosched()
	}
	a.nativeMu.Lock()
	a.inner.close()
	a.nativeMu.Unlock()
	// Drop the worker pool reference exactly once per shared App.
	// Multiple Close calls (defensive teardown, force-close timer
	// overlap) must not over-decrement the global active-apps counter.
	if a.workerRefAcquired.Load() && !a.workerRefDropped.Swap(true) {
		stopSharedWorkersIfIdle()
	}
}

// MultiCoreHandle controls a group of App instances started by RunMultiCore.
// Shutdown stops all of them; Wait blocks until every Run loop has exited.
type MultiCoreHandle struct {
	apps []*App
	done chan struct{}
}

// Shutdown initiates graceful stop on every App in the group. Idempotent;
// safe to call from any goroutine.
func (h *MultiCoreHandle) Shutdown() {
	for _, a := range h.apps {
		a.Shutdown()
	}
}

// Wait blocks until every App in the group has exited its Run loop and
// freed native resources. Returns immediately once all loops have finished.
func (h *MultiCoreHandle) Wait() {
	<-h.done
}

// RunMultiCore spawns n independent App instances on dedicated OS threads.
// Each instance binds to the given port, then every listener round-robins
// accepted sockets across every App. This keeps scaling predictable even on
// kernels or loopback paths where SO_REUSEPORT hashes connections to only one
// listening socket. setup is called once per App, on the thread that instance
// will run on, to register routes / middleware / etc.
//
// setup MUST register the same routes on every App for consistent behavior;
// the framework just calls setup(app) and trusts user code to be
// deterministic. Heavy shared state (DB pools, caches) and any fallible
// initialization should be created ONCE outside RunMultiCore and captured into
// the handler closures so per-App initialization stays cheap. If setup panics,
// RunMultiCore converts it to an error and closes any Apps created so far.
//
// Returns a MultiCoreHandle that can Shutdown or Wait. Returns an error if
// any App fails to start; in that case already-started Apps are shut down
// before returning.
func RunMultiCore(n int, port int, setup func(app *App)) (*MultiCoreHandle, error) {
	if n <= 0 {
		return nil, fmt.Errorf("gogo: RunMultiCore needs n>0, got %d", n)
	}
	if setup == nil {
		return nil, fmt.Errorf("gogo: RunMultiCore requires a setup function")
	}

	type startResult struct {
		idx int
		app *App
		err error
	}
	starts := make(chan startResult, n)
	setups := make(chan startResult, n)
	listens := make(chan startResult, n)
	configureSetup := make(chan struct{})
	configureChildren := make(chan struct{})
	runLoops := make(chan struct{})
	runReturned := make(chan struct{}, n)
	closeApps := make(chan struct{})
	abort := make(chan struct{})
	done := make(chan struct{})
	apps := make([]*App, n)
	var abortOnce sync.Once
	abortAll := func() {
		abortOnce.Do(func() {
			close(abort)
		})
	}

	var runWg sync.WaitGroup
	for i := 0; i < n; i++ {
		idx := i
		runWg.Add(1)
		go func() {
			defer runWg.Done()
			// uWS::Loop is bound to the OS thread that created the App, so
			// every API call against this App must happen on this thread.
			// LockOSThread keeps us pinned for the lifetime of Run.
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			app, err := NewApp()
			if err != nil {
				starts <- startResult{idx: idx, err: err}
				return
			}

			starts <- startResult{idx: idx, app: app}
			select {
			case <-configureSetup:
			case <-abort:
				app.Close()
				return
			}

			if err := runMultiCoreSetup(idx, app, setup); err != nil {
				setups <- startResult{idx: idx, app: app, err: err}
				app.Close()
				return
			}
			setups <- startResult{idx: idx, app: app}
			select {
			case <-configureChildren:
			case <-abort:
				app.Close()
				return
			}

			if n > 1 {
				for childIdx, child := range apps {
					// Keep the accepting App in the child set. This mirrors uWS's
					// LocalCluster pattern and gives every listener the same complete
					// round-robin destination list.
					if !app.inner.addChild(child.inner) {
						app.Close()
						listens <- startResult{
							idx: idx,
							err: fmt.Errorf("gogo: failed to configure multicore child app %d for worker %d", childIdx, idx),
						}
						return
					}
				}
			}

			if !app.Listen(port) {
				app.Close()
				listens <- startResult{idx: idx, err: fmt.Errorf("gogo: failed to Listen on :%d", port)}
				return
			}

			listens <- startResult{idx: idx, app: app}
			select {
			case <-runLoops:
				app.Run()
				runReturned <- struct{}{}
				<-closeApps
				app.Close()
			case <-abort:
				app.Close()
			}
		}()
	}

	// First create every App so peer pub/sub is fully wired before user
	// setup can start background publishers. Child routing is installed only
	// after routes are registered and all App pointers exist, because each
	// listener needs the full set of destination loops for accepted-socket
	// round-robin.
	for i := 0; i < n; i++ {
		r := <-starts
		if r.err != nil {
			abortAll()
			runWg.Wait()
			return nil, r.err
		}
		apps[r.idx] = r.app
	}
	for _, app := range apps {
		app.pubsubPeers = apps
	}
	close(configureSetup)

	for i := 0; i < n; i++ {
		r := <-setups
		if r.err != nil {
			abortAll()
			runWg.Wait()
			return nil, r.err
		}
	}

	close(configureChildren)

	for i := 0; i < n; i++ {
		r := <-listens
		if r.err != nil {
			abortAll()
			runWg.Wait()
			return nil, r.err
		}
	}
	close(runLoops)

	go func() {
		for i := 0; i < n; i++ {
			<-runReturned
		}
		close(closeApps)
		runWg.Wait()
		close(done)
	}()

	return &MultiCoreHandle{apps: apps, done: done}, nil
}

func runMultiCoreSetup(idx int, app *App, setup func(app *App)) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("gogo: RunMultiCore setup panic on worker %d: %v", idx, recovered)
		}
	}()
	setup(app)
	return nil
}

// Response wraps a uWebSockets response.
type Response struct {
	inner responseNative

	// app back-references the App this response belongs to. Set by
	// the request wrapper before middleware / handler fire. Used by
	// features that need App-scoped state (e.g. Response.Render
	// looks up App.templateEngine). Reset to nil on pool return.
	app *App

	// async is non-nil after Async has been called. Subsequent Status/Header/
	// Write/End calls buffer into it instead of touching the C++ response;
	// End flushes the buffer back onto the loop with a single cork.
	async *asyncState

	// refs is the wrapper's reference count. The wrapper is alive (in use,
	// safe to dereference) while refs > 0. Each holder that may outlive
	// the main HTTP handler — the body-collection callback chain, the
	// Async goroutine, the shared-dispatch worker, etc. — increments refs
	// when it takes ownership and decrements (via releaseRef) when it is
	// done. The decrement that drops refs to zero returns the wrapper to
	// its sync.Pool. This makes the wrapper lifecycle CAS-correct: a late-
	// firing OnData callback and a fast Async goroutine can no longer race
	// each other into double-recycling the same wrapper.
	refs atomic.Int32

	// statusCode is the last status code passed to Status, Send, or
	// JSON. Zero means "never set" — StatusCode() returns 200 in that
	// case to match uWS's default. Exists so middleware can inspect
	// what the handler responded with (e.g. for logging).
	statusCode int

	// pendingHeaders buffers Header calls until a status / end / send
	// / write actually goes to the wire. uWS::HttpResponse::writeHeader
	// auto-emits "200 OK" for the status line if nothing was written
	// first, then locks the status — so if middleware writes a header
	// before the handler picks its status code, the user-facing
	// res.Send(404, ...) silently becomes 200. Buffering Go-side keeps
	// the canonical write order (status → headers → body) regardless
	// of the order the caller used. Sync path only; async mode uses
	// asyncState's pre-built status/CT pair.
	pendingHeaders []responseHeader

	// encoder, when non-nil, transforms the response body before it
	// goes to uWS. Used by compression middleware (gzip / brotli) to
	// buffer the handler's Write / End / Send / JSON output, compress
	// the buffered bytes, and attach Content-Encoding before the
	// status line is flushed. Works in both sync and async modes; the
	// async path routes through uwsgo_res_defer_send_with_headers
	// when the encoder added Content-Encoding/Vary, bypassing the
	// shared-memory fast path which has no slot for extra headers.
	encoder *bodyEncoder

	// onFinish holds callbacks registered via Response.OnFinish that
	// must run after the user's handler (and any Response.Async
	// goroutine it spawned) has fully completed. Used primarily by
	// middleware that needs post-handler cleanup — session save,
	// metrics flush, span close — and that must NOT fire while the
	// async goroutine is still mutating state. Access is guarded by
	// finishMu because the goroutine and the original sync handler
	// both touch the slice (middleware appends post-next() on the
	// sync side; finishAsync drains on the goroutine side).
	finishMu sync.Mutex
	onFinish []func()
	// finished flips to true the moment finishAsync drains onFinish,
	// so a callback registered after the goroutine ran ahead of the
	// middleware fires inline instead of being orphaned.
	finished bool

	// abort holds Go-side callbacks registered against uWS's single
	// onAborted slot. uWS only stores one handler, so Response-level
	// users must multiplex rather than calling r.inner.onAborted from
	// multiple features and overwriting each other.
	abort *responseAbortState
}

// bodyEncoder is the staging buffer + transformer used to defer
// response body emission until middleware can compress it.
//
// buf is a plain []byte rather than strings.Builder so the wrapper
// can be pooled and its backing array reused across requests —
// strings.Builder.Reset() drops its buffer, defeating the pool.
type bodyEncoder struct {
	buf     []byte
	encode  func(body []byte, contentType string) (encoded []byte, contentEncoding string)
	max     int
	applied bool
}

type asyncState struct {
	loopPtr     uintptr
	ctxHandle   uintptr
	status      string
	contentType string
	body        strings.Builder
	sent        bool
}

type responseAbortState struct {
	mu        sync.Mutex
	callbacks []func()
	fired     bool
}

// Status sets the HTTP status code. The standard reason phrase from
// net/http is appended automatically (e.g. 200 → "200 OK", 404 → "404 Not
// Found"); unknown codes are written as the bare number.
func (r *Response) Status(code int) *Response {
	r.statusCode = code
	line := statusLine(code)
	if r.async != nil {
		r.async.status = line
		return r
	}
	r.inner.status(line)
	r.flushPendingHeaders()
	return r
}

// headerBlobPool holds the packing buffer used by
// flushPendingHeaders for the 2+ headers path. Pooling the buffer
// eliminates the per-request heap allocation that strings.Builder
// otherwise produced — at 70k RPS that was ~18 MB/sec of garbage
// that swamped the cgo savings the batch path was meant to
// capture. The pool's New function pre-grows each fresh buffer to
// 512 bytes (8 headers × 64 bytes average — wide enough to absorb
// Helmet-sized stacks without ever reallocating).
var headerBlobPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// flushPendingHeaders writes every buffered header to the wire.
// Must be called after status was written (or auto-200'd). Resets
// the buffer length to zero so the same response wrapper recycled
// later doesn't re-emit stale headers. Cheap no-op when the slice
// is empty (which is the common case — no buffered headers means
// no middleware that added them).
//
// One-header fast path uses the single-call header() bridge (one
// cgo crossing). Two-or-more-headers batches the writes via
// headersBatch() — packs the headers into a pooled key\0value\0…
// buffer and crosses once for the whole set. With CORS adding 3–4
// headers per request this saves (N-1) cgo crossings per response
// at the cost of the append loop. The buffer comes from a
// sync.Pool so the hot path allocates nothing.
func (r *Response) flushPendingHeaders() {
	n := len(r.pendingHeaders)
	if n == 0 {
		return
	}
	if n == 1 {
		h := r.pendingHeaders[0]
		r.inner.header(h.name, h.value)
		r.pendingHeaders = r.pendingHeaders[:0]
		return
	}
	// 2+ headers: pack once, cross once. Buffer is pooled to
	// avoid per-request heap churn under sustained load.
	bufp := headerBlobPool.Get().(*[]byte)
	buf := (*bufp)[:0]
	for _, h := range r.pendingHeaders {
		buf = append(buf, h.name...)
		buf = append(buf, 0)
		buf = append(buf, h.value...)
		buf = append(buf, 0)
	}
	r.inner.headersBatch(buf, n)
	// Stash the (possibly grown) backing array back into the pool
	// so the next request inherits the capacity. Empty the buffer
	// before return so a recycled []byte never carries stale
	// content past a Get / Put boundary.
	*bufp = buf[:0]
	headerBlobPool.Put(bufp)
	r.pendingHeaders = r.pendingHeaders[:0]
}

// ensureStatusSync writes a 200 OK status line if none was set yet.
// Lets End / Write own the order (status → buffered headers → body)
// instead of letting uWS auto-emit "200 OK" via the first writeHeader
// (which would lock the status before pendingHeaders flush).
func (r *Response) ensureStatusSync() {
	if r.statusCode == 0 {
		r.statusCode = 200
		r.inner.status("200 OK")
	}
}

// StatusCode returns the last status code passed to Status / Send /
// JSON. Returns 200 (the uWS default) if no status was ever set.
// Useful from middleware that needs to observe how a handler
// responded — e.g. a logger that records the final status.
func (r *Response) StatusCode() int {
	if r.statusCode == 0 {
		return 200
	}
	return r.statusCode
}

// Header writes a response header.
//
// Each Header call appends to the wire output; calling Header twice with
// the same key emits two header lines (no replace semantic — uWS does
// not support that). For multi-value headers (Set-Cookie, Vary, Link)
// call Header / Append repeatedly with the same key.
//
// In async mode Content-Type is short-circuited onto the async fast
// path (stored on r.async directly), and any other header is buffered
// in pendingHeaders. flushAsync routes pendingHeaders through
// uwsgo_res_defer_send_with_headers, which writes them between the
// status line and the body. That means the shared-memory fast path
// (zero cgo) is only available for responses that set Content-Type
// alone; the moment a handler attaches any extra header the response
// goes through the cgo defer-send shim instead.
func (r *Response) Header(key, value string) *Response {
	validateHeaderName(key)
	validateHeaderValue(key, value)
	if r.async != nil && strings.EqualFold(key, "Content-Type") {
		r.async.contentType = value
		return r
	}
	// Buffer Go-side and flush on the first Status / Send / End / Write
	// so the order (status → headers → body) is preserved regardless of
	// how the caller sequenced their calls. See pendingHeaders comment
	// in Response for the uWS quirk this protects against.
	r.pendingHeaders = append(r.pendingHeaders, responseHeader{key, value})
	return r
}

// Append adds a header value without replacing existing ones. Identical
// in effect to Header — included for parity with fiber/express idiom
// where Set replaces and Append accumulates. uWS doesn't support
// replace, so both helpers do the same thing.
//
// Use for multi-value headers: Set-Cookie, Vary, Link, etc.
func (r *Response) Append(key, value string) *Response {
	return r.Header(key, value)
}

// Write appends a response chunk without ending the response.
func (r *Response) Write(body string) *Response {
	if r.encoder != nil && !r.encoder.applied {
		if prefix, overflow := r.encoder.writeString(body); !overflow {
			return r
		} else {
			r.encoder = nil
			if r.async != nil {
				r.async.body.WriteString(prefix)
				r.async.body.WriteString(body)
				return r
			}
			r.ensureStatusSync()
			r.flushPendingHeaders()
			if prefix != "" {
				r.inner.write(prefix)
			}
			r.inner.write(body)
			return r
		}
	}
	if r.async != nil {
		r.async.body.WriteString(body)
		return r
	}
	r.ensureStatusSync()
	r.flushPendingHeaders()
	r.inner.write(body)
	return r
}

// End finishes the response.
func (r *Response) End(body string) {
	if r.encoder != nil && !r.encoder.applied {
		if prefix, overflow := r.encoder.writeString(body); overflow {
			r.encoder = nil
			if r.async != nil {
				r.async.body.WriteString(prefix)
				r.async.body.WriteString(body)
				r.flushAsync()
				return
			}
			r.ensureStatusSync()
			r.flushPendingHeaders()
			if prefix != "" {
				r.inner.write(prefix)
			}
			r.inner.end(body)
			return
		} else {
			body = r.applyEncoder("")
		}
	}
	if r.async != nil {
		r.async.body.WriteString(body)
		r.flushAsync()
		return
	}
	r.ensureStatusSync()
	r.flushPendingHeaders()
	r.inner.end(body)
}

// Send writes status code, an optional Content-Type header, and body in
// one call. The framework picks the lowest-overhead path based on the
// handler's mode:
//
//   - Sync handler (Get / Post / Any): one cgo crossing into uWS.
//   - Async handler (GetAsync / small PostAsync) with body up to 8 KB:
//     ZERO cgo per request — written to shared-memory inline buffers and
//     pushed onto the App's response ring; the loop drains it.
//   - Async handler with body > 8 KB: cgo Loop::defer with the full
//     payload, status, and Content-Type.
//
// Pass an empty contentType to omit the header.
func (r *Response) Send(code int, contentType, body string) {
	r.statusCode = code
	if contentType != "" {
		validateHeaderValue("Content-Type", contentType)
	}
	line := statusLine(code)

	if r.encoder != nil && !r.encoder.applied {
		if prefix, overflow := r.encoder.writeString(body); overflow {
			r.encoder = nil
			if r.async != nil && !r.async.sent {
				r.async.status = line
				r.async.contentType = contentType
				r.async.body.WriteString(prefix)
				r.async.body.WriteString(body)
				r.flushAsync()
				return
			}
			r.sendSplitSync(line, contentType, prefix, body)
			return
		} else {
			body = r.applyEncoder(contentType)
		}
	}

	// Async mode: try the zero-cgo shared-memory path first. Falls back
	// to the cgo defer path when the body exceeds inline capacity OR when
	// extra response headers were buffered (the shared path has no slot
	// for headers beyond Content-Type, so compression's
	// Content-Encoding/Vary or any middleware-buffered headers force the
	// defer-send-with-headers route).
	if r.async != nil && !r.async.sent {
		if r.dropAsyncIfAborted() {
			return
		}
		if len(r.pendingHeaders) == 0 {
			if asyncSendShared(r.async.ctxHandle, line, contentType, body) {
				r.async.sent = true
				return
			}
		}
		r.async.status = line
		r.async.contentType = contentType
		r.async.body.WriteString(body)
		r.flushAsync()
		return
	}

	// Sync mode: fast path when no headers were buffered — one cgo
	// crossing for status + Content-Type + body. When the caller (or
	// middleware) added Headers, split into status → buffered headers
	// → Content-Type → body so order is preserved.
	if len(r.pendingHeaders) == 0 {
		r.inner.send(line, contentType, body)
		return
	}
	r.inner.status(line)
	r.flushPendingHeaders()
	if contentType != "" {
		r.inner.header("Content-Type", contentType)
	}
	r.inner.end(body)
}

func (r *Response) sendSplitSync(status, contentType, prefix, body string) {
	if len(r.pendingHeaders) == 0 {
		r.inner.sendSplit(status, contentType, nil, prefix, body)
		return
	}
	bufp := headerBlobPool.Get().(*[]byte)
	buf := (*bufp)[:0]
	for _, h := range r.pendingHeaders {
		buf = append(buf, h.name...)
		buf = append(buf, 0)
		buf = append(buf, h.value...)
		buf = append(buf, 0)
	}
	r.inner.sendSplit(status, contentType, buf, prefix, body)
	*bufp = buf[:0]
	headerBlobPool.Put(bufp)
	r.pendingHeaders = r.pendingHeaders[:0]
}

// JSON marshals v and sends it with Content-Type: application/json. If
// marshalling fails the response is replaced with a generic 500 and the
// underlying marshal error is reported through the panic handler so the
// programmer sees it server-side without leaking type / package names to
// the network. With the default encoder, marshal failures happen for
// unsupported value shapes (channels, functions, cyclic structures), so
// failures here usually indicate a bug in caller code.
func (r *Response) JSON(code int, v any) {
	data, err := r.jsonEncoder()(v)
	if err != nil {
		reportPanic(fmt.Errorf("gogo: Response.JSON Config.JSONEncoder: %w", err))
		r.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
		return
	}
	r.Send(code, "application/json", bytesAsString(data))
	runtime.KeepAlive(data)
}

func (r *Response) jsonEncoder() JSONEncoder {
	if r != nil && r.app != nil && r.app.cfg.JSONEncoder != nil {
		return r.app.cfg.JSONEncoder
	}
	return json.Marshal
}

// JSONBytes writes a pre-marshaled JSON body. Skips json.Marshal so
// handlers that already hold an encoded payload — cached responses,
// proxied bytes from another service, custom-encoder output (sonic,
// segmentio, etc.) — don't pay the reflection cost a second time.
//
// The caller is responsible for the bytes being valid JSON; the
// framework does not validate. Content-Type is set to
// application/json automatically.
func (r *Response) JSONBytes(code int, b []byte) {
	r.Send(code, "application/json", bytesAsString(b))
	runtime.KeepAlive(b)
}

// JSONStream emits a JSON body through a streaming encoder, avoiding
// the staging-buffer allocation that json.Marshal makes for the
// whole document. Useful for large arrays, NDJSON-style feeds, or
// any response whose JSON would otherwise dominate the handler's
// memory peak.
//
// fn receives a *json.Encoder writing through a chunked response
// stream — each Encode call emits one JSON value followed by a
// newline (json.Encoder's default). For a single top-level array
// the caller is responsible for writing the framing characters
// themselves; for newline-delimited feeds Encode is enough.
// JSONStream intentionally uses encoding/json's streaming encoder
// rather than Config.JSONEncoder, whose contract is whole-value
// marshal. For custom codec output, marshal each value yourself and
// write through Response.Stream or send pre-marshaled bytes with
// Response.JSONBytes.
//
// Async only — call from GetAsync, a body-async route, or wrap a sync handler
// in Response.Async. The underlying Response.Stream applies the
// default backpressure cap (StreamBackpressureBytes), so a slow
// consumer parks the producer goroutine rather than spiking memory.
func (r *Response) JSONStream(code int, fn func(*json.Encoder) error) error {
	return r.Stream(code, "application/json", func(w io.Writer) error {
		if fn == nil {
			return nil
		}
		return fn(json.NewEncoder(w))
	})
}

// Stream writes a chunked HTTP/1.1 response by handing the caller an
// io.Writer that buffers each Write call onto the uWS loop's
// deferred-write queue. Order is preserved (FIFO), so chunks reach
// the wire in the same order the writer emits them.
//
// Stream is meant for Server-Sent Events, NDJSON feeds, log tails,
// and similar long-running responses where you don't know the full
// body up front and you don't want to materialize it before the
// first byte goes out. Call it from a GetAsync or body-async handler.
// On a sync route call res.Async() first; on a plain sync handler
// streaming would block the event-loop thread, which is almost
// never what you want.
//
//	app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
//	    err := res.Stream(200, "text/event-stream", func(w io.Writer) error {
//	        ticker := time.NewTicker(time.Second)
//	        defer ticker.Stop()
//	        for i := 0; i < 5; i++ {
//	            <-ticker.C
//	            if _, err := fmt.Fprintf(w, "data: tick %d\n\n", i); err != nil {
//	                return err
//	            }
//	        }
//	        return nil
//	    })
//	    if err != nil { /* logged via reportPanic */ }
//	})
//
// Headers buffered via res.Header before Stream go out in the
// initial response frame. After Stream returns, the response is
// closed — do not call Send / End / Stream again on the same
// response.
//
// uWS automatically applies HTTP/1.1 transfer-encoding: chunked
// when no Content-Length is set, which is the expected mode for
// streaming. If the client disconnects mid-stream the underlying
// AsyncCtx is marked aborted and subsequent Write calls become
// silent no-ops on the C side. If the stream is parked on backpressure,
// Write and AwaitDrain return ErrStreamAborted so callers can stop
// their generator early.
func (r *Response) Stream(status int, contentType string, fn func(w io.Writer) error) error {
	if r.async == nil {
		panic("gogo: Stream requires an async response; call from GetAsync, a body-async route, or use res.Async first")
	}
	if r.async.sent {
		panic("gogo: Stream: response already sent")
	}
	if status == 0 {
		status = 200
	}
	r.statusCode = status
	line := statusLine(status)
	if contentType != "" {
		validateHeaderValue("Content-Type", contentType)
	}
	if r.dropAsyncIfAborted() {
		return nil
	}

	// Pack pendingHeaders for the initial frame so middleware-set
	// headers (RequestID echo, CSP, etc.) ship with the status line
	// instead of being stranded on the now-skipped flushAsync path.
	var hb strings.Builder
	for _, h := range r.pendingHeaders {
		hb.Grow(len(h.name) + len(h.value) + 2)
		hb.WriteString(h.name)
		hb.WriteByte(0)
		hb.WriteString(h.value)
		hb.WriteByte(0)
	}
	r.pendingHeaders = r.pendingHeaders[:0]

	ctxHandle := r.async.ctxHandle
	loopPtr := r.async.loopPtr
	if !asyncDeferStreamStart(loopPtr, ctxHandle, line, contentType, hb.String()) {
		r.async.sent = true
		asyncCtxRelease(ctxHandle)
		return nil
	}

	// Mark sent so the normal async release path treats this response as
	// complete. Stream owns the original async ctx ref from here; every native
	// stream operation retains/releases its own defer ref, and the defer below
	// drops the original ref exactly once even if fn panics.
	r.async.sent = true
	defer func() {
		if !asyncCtxAborted(ctxHandle) {
			asyncDeferStreamEnd(loopPtr, ctxHandle)
		}
		asyncCtxRelease(ctxHandle)
	}()

	sw := &streamWriter{r: r}
	fnErr := fn(sw)
	// The deferred close above runs even on user error so the response doesn't
	// hang the connection. The user's error is propagated back to the caller for
	// logging / metrics.
	return fnErr
}

// StreamBackpressureBytes is the high-water mark, in bytes of uWS's
// per-socket send buffer, at which streamWriter.Write automatically
// parks the calling goroutine on AwaitDrain. The check fires after
// every successful chunk write — a slow consumer therefore can't
// drive the Go producer faster than the kernel can flush.
//
// Default 1 MiB. Set to 0 to disable the automatic check (the
// handler is then responsible for invoking BufferedAmount /
// AwaitDrain itself, the pre-default behavior).
//
// The same threshold protects every Stream / SSE caller — file
// serving paths still use SendFileBackpressureBytes for its own
// reads-from-disk loop.
//
// Deprecated for runtime mutation: direct assignment remains supported for
// startup-time configuration. Use SetStreamBackpressureBytes /
// GetStreamBackpressureBytes for changes while requests may be running.
var StreamBackpressureBytes uint64 = 1 << 20

// SetStreamBackpressureBytes updates the automatic Stream / SSE backpressure
// threshold atomically. Zero disables the automatic check.
func SetStreamBackpressureBytes(backpressureBytes uint64) {
	atomic.StoreUint64(&StreamBackpressureBytes, backpressureBytes)
}

// GetStreamBackpressureBytes returns the current automatic Stream / SSE
// backpressure threshold.
func GetStreamBackpressureBytes() uint64 {
	return atomic.LoadUint64(&StreamBackpressureBytes)
}

// streamWriter is the io.Writer handed to Stream's callback. Each
// Write call schedules a defer to the uWS loop thread that calls
// res->write(chunk); the cgo bridge dups the bytes before the defer
// is queued so the caller may reuse the buffer immediately.
//
// After each write streamWriter consults BufferedAmount; when uWS's
// send buffer climbs past StreamBackpressureBytes the goroutine
// parks on AwaitDrain until uWS signals the buffer has flushed.
// This keeps slow consumers from forcing the framework to copy
// unbounded chunks into the C heap (one per cgo defer entry) faster
// than the loop can drain them. Handlers that need to opt out can
// set StreamBackpressureBytes = 0 and drive BufferedAmount manually.
type streamWriter struct {
	r *Response
}

func (s *streamWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if asyncCtxAborted(s.r.async.ctxHandle) {
		return len(p), nil
	}
	asyncDeferStreamWrite(s.r.async.loopPtr, s.r.async.ctxHandle, string(p))
	threshold := GetStreamBackpressureBytes()
	if threshold == 0 {
		return len(p), nil
	}
	if s.r.streamBufferedAmount() <= threshold {
		return len(p), nil
	}
	// uWS is buffering more than threshold for this socket — park
	// here so we don't queue another cgo defer (each of which heap-
	// copies the chunk on the C side) until the consumer catches up.
	if err := s.r.AwaitDrain(threshold); err != nil {
		// Stream is aborted (client disconnected). The bytes we just
		// queued may or may not have made the wire; surface the error
		// so the user's stream loop can stop emitting.
		return len(p), err
	}
	return len(p), nil
}

// BufferedAmount returns the byte count uWS has accepted for
// sending but not yet flushed to the kernel socket. Grows when
// the client isn't draining fast enough — the standard
// backpressure signal — and shrinks as the OS acknowledges
// sends.
//
// Intended use is from inside a Stream callback to decide
// whether to pause emitting more bytes:
//
//	res.Stream(200, "text/event-stream", func(w io.Writer) error {
//	    for event := range events {
//	        // Yield to the loop when uWS has > 1 MiB buffered;
//	        // otherwise a slow client can OOM the server with
//	        // queued chunks.
//	        if err := res.AwaitDrain(1 << 20); err != nil {
//	            return err
//	        }
//	        if _, err := w.Write([]byte(event)); err != nil {
//	            return err
//	        }
//	    }
//	    return nil
//	})
//
// The read is sampled from a worker goroutine without a loop
// hop — uWS's internal counter is an atomic-aligned size_t in
// uSockets, so reads are coherent but may lag by a few
// microseconds. Adequate for throttling decisions; do not use
// for transactional accounting.
func (r *Response) BufferedAmount() uint64 {
	return innerBufferedAmount(r.inner)
}

func (r *Response) streamBufferedAmount() uint64 {
	buffered := r.BufferedAmount()
	if r.async == nil {
		return buffered
	}
	pending := asyncCtxStreamPendingBytes(r.async.ctxHandle)
	if pending > ^uint64(0)-buffered {
		return ^uint64(0)
	}
	return buffered + pending
}

// AwaitDrain blocks the caller until BufferedAmount falls below
// threshold, or returns nil immediately if it's already below.
// It samples uWS's buffered byte counter at a short interval from the
// producer goroutine. The polling fallback is intentional: uWS's
// onWritable signal is edge-triggered and can be missed when a buffer
// drains before the callback is armed, which would otherwise park the
// stream permanently.
//
// Only valid while a Stream is in flight (r.async != nil); calling
// outside that scope returns nil with no work done.
//
// Returns ErrStreamAborted when the client disconnects before the buffer
// drains. Callers in a streaming loop should propagate the error to break
// out of their generator.
func (r *Response) AwaitDrain(threshold uint64) error {
	if r.async == nil {
		return nil
	}
	return waitForDrain(
		func() uint64 { return r.streamBufferedAmount() },
		func() bool { return r.async == nil || asyncCtxAborted(r.async.ctxHandle) },
		threshold,
	)
}

const drainPollInterval = 2 * time.Millisecond

func waitForDrain(buffered func() uint64, aborted func() bool, threshold uint64) error {
	for buffered() > threshold {
		if aborted() {
			return ErrStreamAborted
		}
		time.Sleep(drainPollInterval)
	}
	return nil
}

// ErrStreamAborted is returned by AwaitDrain, and by Stream writes that are
// parked on AwaitDrain, when the client disconnects before the response's
// backpressure buffer drains.
var ErrStreamAborted = errors.New("gogo: stream aborted while waiting on backpressure drain")

// JSONP writes a JSONP response — the JSON-encoded value v wrapped in
// a function call named callback, served with Content-Type
// application/javascript. Useful for cross-origin reads from older
// clients that pre-date CORS; most modern apps should prefer JSON +
// proper CORS configuration via middleware.CORS.
//
// The callback name is validated to contain only the JavaScript
// identifier characters [A-Za-z0-9_$.] so an attacker can't slip
// `</script>` or a closing parenthesis into the response and pivot
// the JSONP payload into an XSS sink. An invalid callback (empty or
// containing other characters) returns 400 with no body.
//
// The marshaled JSON is also escaped against U+2028 / U+2029 — line
// separators that are legal in JSON but break JavaScript parsing
// when not escaped (each becomes a literal newline outside a
// string).
//
//	app.Get("/api/users", func(res *gogo.Response, req *gogo.Request) {
//	    cb := req.QueryParam("callback")
//	    res.JSONP(cb, []User{...})
//	})
func (r *Response) JSONP(callback string, v any) {
	if !validJSONPCallback(callback) {
		r.Send(400, "text/plain; charset=utf-8", "invalid jsonp callback\n")
		return
	}
	data, err := r.jsonEncoder()(v)
	if err != nil {
		reportPanic(fmt.Errorf("gogo: Response.JSONP Config.JSONEncoder: %w", err))
		r.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
		return
	}
	body := escapeJSONP(data)

	var b strings.Builder
	b.Grow(len(callback) + len(body) + len("/**/") + len("(") + len(");"))
	// Leading "/**/" defuses content-sniffing attacks where a browser
	// would interpret a buffered JSONP response as something other
	// than JS. The comment is harmless to actual JS parsers.
	b.WriteString("/**/")
	b.WriteString(callback)
	b.WriteByte('(')
	b.Write(body)
	b.WriteString(");")
	r.Send(200, "application/javascript; charset=utf-8", b.String())
}

// escapeJSONP keeps JSONP safe even when Config.JSONEncoder does not mirror
// encoding/json's HTMLEscape behavior. It prevents `</script>` breakouts and
// legacy line-separator parser hazards when JSONP is consumed via a script tag.
func escapeJSONP(data []byte) []byte {
	var out []byte
	for i := 0; i < len(data); i++ {
		repl := ""
		switch data[i] {
		case '<':
			repl = "\\u003c"
		case '>':
			repl = "\\u003e"
		case '&':
			repl = "\\u0026"
		case 0xC2:
			if i+1 < len(data) && data[i+1] == 0x85 {
				if out == nil {
					out = make([]byte, 0, len(data)+jsonpEscapeExtra(data, i))
					out = append(out, data[:i]...)
				}
				out = append(out, "\\u0085"...)
				i++
				continue
			}
		case 0xE2:
			if i+2 < len(data) && data[i+1] == 0x80 {
				switch data[i+2] {
				case 0xA8:
					repl = "\\u2028"
				case 0xA9:
					repl = "\\u2029"
				}
				if repl != "" {
					if out == nil {
						out = make([]byte, 0, len(data)+jsonpEscapeExtra(data, i))
						out = append(out, data[:i]...)
					}
					out = append(out, repl...)
					i += 2
					continue
				}
			}
		}
		if repl == "" {
			if out != nil {
				out = append(out, data[i])
			}
			continue
		}
		if out == nil {
			out = make([]byte, 0, len(data)+jsonpEscapeExtra(data, i))
			out = append(out, data[:i]...)
		}
		out = append(out, repl...)
	}
	if out == nil {
		return data
	}
	return out
}

func jsonpEscapeExtra(data []byte, start int) int {
	extra := 0
	for i := start; i < len(data); i++ {
		switch data[i] {
		case '<', '>', '&':
			extra += len("\\u003c") - 1
		case 0xC2:
			if i+1 < len(data) && data[i+1] == 0x85 {
				extra += len("\\u0085") - 2
				i++
			}
		case 0xE2:
			if i+2 < len(data) && data[i+1] == 0x80 && (data[i+2] == 0xA8 || data[i+2] == 0xA9) {
				extra += len("\\u2028") - 3
				i += 2
			}
		}
	}
	return extra
}

// validJSONPCallback accepts only characters that can legally appear
// in a JavaScript identifier or dotted member expression: letters,
// digits, '_', '$', '.'. The first character must be a letter, '_',
// or '$' (digit-leading idents are illegal). Empty is rejected.
func validJSONPCallback(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '$'
		isDigit := c >= '0' && c <= '9'
		if i == 0 {
			if !isAlpha {
				return false
			}
			continue
		}
		if !(isAlpha || isDigit || c == '.') {
			return false
		}
	}
	return true
}

// Redirect sends an HTTP redirect to location with the given status code.
// Standard codes: 301 (moved permanently), 302 (found / temporary, common
// default), 303 (see other — POST → GET), 307 (temp, preserves method),
// 308 (permanent, preserves method). Status 0 defaults to 302.
//
// CRLF / NUL in location is treated as a programming or input-validation
// bug: the framework refuses to write the bad header and responds 500
// instead of panicking. This keeps a hostile request from amplifying
// into a panic-handler trigger when handler code passes user input
// straight to Redirect (e.g. res.Redirect(req.QueryParam("next"), 302)).
//
// SECURITY: gogo does NOT validate that location stays within your own
// host. Passing user-controlled input here without a host allow-list is
// an open-redirect bug — attackers can craft links that look like they
// land on your site but bounce to a phishing page. Sanitize the target
// (compare against a known list of paths or hostnames) before calling
// Redirect.
//
// In async mode this schedules a Cork on the loop so the status, Location
// header, and empty body go out as a single packet; do not call Send /
// End on the same response afterwards.
func (r *Response) Redirect(location string, code int) {
	if code == 0 {
		code = 302
	}
	if containsCtlForHeader(location) {
		// Untrusted input that would inject a header break. Report so
		// operators see it, but don't panic — that turns one bad
		// request into a 500 storm via the panic handler. Send a
		// plain 500 instead.
		reportPanic(fmt.Errorf("gogo: Redirect: location contains a control character (CR/LF/NUL); responding 500"))
		r.Send(500, "text/plain; charset=utf-8", "internal error\n")
		return
	}
	r.statusCode = code
	line := statusLine(code)
	if r.async != nil && !r.async.sent {
		if r.dropAsyncIfAborted() {
			return
		}
		r.async.sent = true
		headers := captureResponseHeaders(r.pendingHeaders, []responseHeader{{
			name:  "Location",
			value: location,
		}})
		r.pendingHeaders = r.pendingHeaders[:0]
		asyncDeferSendWithHeaders(r.async.loopPtr, r.async.ctxHandle, line, "", responseHeadersBlob(headers), "")
		return
	}
	r.inner.status(line)
	r.flushPendingHeaders()
	r.inner.header("Location", location)
	r.inner.end("")
}

// MaxSendFileBytes is a sanity cap on the largest file SendFile and
// Download will agree to serve — a misconfiguration guard, not a
// memory limit. The streaming body path uses an O(chunk) buffer
// regardless of file size, so memory pressure no longer scales with
// the file. Files larger than the cap return ErrFileTooLarge without
// touching the response; raise this at startup if your workload
// legitimately serves bigger blobs.
//
// Default 100 MiB.
//
// Deprecated for runtime mutation: direct assignment remains supported for
// startup-time configuration. Use SetMaxSendFileBytes / GetMaxSendFileBytes
// for changes while requests may be running.
var MaxSendFileBytes int64 = 100 << 20

// NoSendFileLimit disables the SendFile / Download file-size cap. Use only
// for trusted file-serving routes where path allow-listing, authorization, or
// an external layer already bounds what may be served.
const NoSendFileLimit int64 = -1

// SendFileChunkBytes is the buffer size used for each disk read +
// stream write iteration. Memory used per concurrent SendFile call
// is bounded by this value plus uWS's internal write buffer (which
// grows up to SendFileBackpressureBytes before AwaitDrain parks the
// goroutine). 64 KiB matches the typical filesystem read-ahead
// granularity and uWS's default send-batch size.
//
// Deprecated for runtime mutation: direct assignment remains supported for
// startup-time configuration. Use SetSendFileChunkBytes /
// GetSendFileChunkBytes for changes while requests may be running.
var SendFileChunkBytes int = 64 << 10

// SendFileBackpressureBytes is the high-water mark for uWS's
// per-socket send buffer. When the buffer climbs above this value
// the streaming SendFile path parks on AwaitDrain until uWS notifies
// it the buffer has drained — this keeps a slow consumer from
// holding the goroutine hostage AND keeps the kernel buffer bounded
// at the same level regardless of file size.
//
// Deprecated for runtime mutation: direct assignment remains supported for
// startup-time configuration. Use SetSendFileBackpressureBytes /
// GetSendFileBackpressureBytes for changes while requests may be running.
var SendFileBackpressureBytes uint64 = 1 << 20

// ErrFileTooLarge is returned by SendFile / Download when the target
// file is larger than MaxSendFileBytes.
var ErrFileTooLarge = errors.New("gogo: file exceeds MaxSendFileBytes")

var (
	sendFileChunkBytesAtomic atomic.Int64
	sendFileChunkBytesSet    atomic.Bool
)

// SetMaxSendFileBytes updates the SendFile / Download file-size cap
// atomically. Set to NoSendFileLimit to disable the cap.
func SetMaxSendFileBytes(maxBytes int64) {
	atomic.StoreInt64(&MaxSendFileBytes, maxBytes)
}

// GetMaxSendFileBytes returns the current SendFile / Download file-size cap.
func GetMaxSendFileBytes() int64 {
	return atomic.LoadInt64(&MaxSendFileBytes)
}

func sendFileTooLarge(size, maxBytes int64) bool {
	return maxBytes >= 0 && size > maxBytes
}

// SetSendFileChunkBytes updates the per-read SendFile buffer size atomically.
// Values at or below zero restore the default.
func SetSendFileChunkBytes(chunkBytes int) {
	if chunkBytes <= 0 {
		chunkBytes = 64 << 10
	}
	sendFileChunkBytesSet.Store(true)
	sendFileChunkBytesAtomic.Store(int64(chunkBytes))
}

// GetSendFileChunkBytes returns the current per-read SendFile buffer size.
func GetSendFileChunkBytes() int {
	if sendFileChunkBytesSet.Load() {
		current := sendFileChunkBytesAtomic.Load()
		if current <= 0 {
			return 64 << 10
		}
		return int(current)
	}
	// Legacy direct assignment is supported for startup-time configuration.
	// Use SetSendFileChunkBytes once requests may be running.
	if SendFileChunkBytes <= 0 {
		return 64 << 10
	}
	return SendFileChunkBytes
}

// SetSendFileBackpressureBytes updates the SendFile buffered-byte high-water
// mark atomically. Zero restores the default.
func SetSendFileBackpressureBytes(backpressureBytes uint64) {
	if backpressureBytes == 0 {
		backpressureBytes = 1 << 20
	}
	atomic.StoreUint64(&SendFileBackpressureBytes, backpressureBytes)
}

// GetSendFileBackpressureBytes returns the current SendFile backpressure
// high-water mark.
func GetSendFileBackpressureBytes() uint64 {
	v := atomic.LoadUint64(&SendFileBackpressureBytes)
	if v == 0 {
		return 1 << 20
	}
	return v
}

// SendFile reads path from disk and writes it as the response body.
// Content-Type is picked from the file extension via mime.TypeByExtension;
// for unknown extensions the first 512 bytes are passed through
// http.DetectContentType. Last-Modified and a weak ETag derived from
// (size, mtime) are set automatically.
//
// Conditional requests are honored: If-None-Match against the ETag and
// If-Modified-Since against the file mtime each short-circuit to 304
// Not Modified with no body. Single-byte range requests on req
// (`Range: bytes=start-end`, `bytes=start-`, `bytes=-suffix`) return
// 206 Partial Content with the requested slice; multi-range and
// non-bytes units fall through to the full 200.
//
// The helper refuses files larger than MaxSendFileBytes (default
// 100 MiB) and returns ErrFileTooLarge. Returns an error when path is
// missing, a directory, or unreadable; the response is left untouched
// in that case so the caller can decide what to send.
//
// Memory usage is bounded by SendFileChunkBytes (default 64 KiB) +
// SendFileBackpressureBytes (default 1 MiB) per concurrent call,
// regardless of file size. The body is streamed via chunked
// transfer-encoding using Response.Stream, so a slow consumer
// applies backpressure to the read loop rather than buffering the
// entire file.
//
// From a sync handler SendFile transparently upgrades to async via
// Response.Async so the uWS loop thread isn't blocked on disk I/O;
// the function returns nil immediately after the open/stat/range
// prep, and the body streams from a goroutine.
func (r *Response) SendFile(req *Request, path string) error {
	return r.sendFile(req, path, "", false)
}

// Download is SendFile plus Content-Disposition: attachment, which
// prompts browsers to save the body to disk instead of rendering it
// inline. filename overrides the suggested name in the header; pass ""
// to default to filepath.Base(path). The filename is wrapped in
// quoted-string form with quote / backslash escaped per RFC 6266;
// non-ASCII filenames receive an RFC 5987 filename* parameter so
// non-Latin-1 names survive transport.
func (r *Response) Download(req *Request, path, filename string) error {
	if filename == "" {
		filename = filepath.Base(path)
	}
	return r.sendFile(req, path, filename, true)
}

type responseHeader struct {
	name, value string
}

func (r *Response) sendFile(req *Request, path, filename string, attachment bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if info.IsDir() {
		f.Close()
		return fmt.Errorf("gogo: SendFile: %s is a directory", path)
	}
	size := info.Size()
	if sendFileTooLarge(size, GetMaxSendFileBytes()) {
		f.Close()
		return ErrFileTooLarge
	}
	mtime := info.ModTime().UTC()
	etag := fmt.Sprintf(`W/"%x-%x"`, size, mtime.UnixNano())
	lastMod := mtime.Format(http.TimeFormat)

	// Conditional GET — 304 short-circuit. If-None-Match wins over
	// If-Modified-Since per RFC 7232 §6. Header names are lowercased
	// because the sync Request.Header path passes them straight to uWS,
	// which stores headers in lower-case form on parse.
	if req != nil {
		if inm := req.Header("if-none-match"); inm != "" {
			if etagMatch(inm, etag) {
				f.Close()
				r.sendBytes(304, []responseHeader{{"ETag", etag}}, nil)
				return nil
			}
		} else if ims := req.Header("if-modified-since"); ims != "" {
			if t, err := http.ParseTime(ims); err == nil &&
				!mtime.Truncate(time.Second).After(t) {
				f.Close()
				r.sendBytes(304, []responseHeader{{"ETag", etag}}, nil)
				return nil
			}
		}
	}

	// Content-Type — extension lookup first, sniff fallback.
	ctype := mime.TypeByExtension(filepath.Ext(path))
	if ctype == "" {
		var sniff [512]byte
		n, _ := f.ReadAt(sniff[:], 0)
		ctype = http.DetectContentType(sniff[:n])
	}

	rangeHeader := ""
	if req != nil {
		rangeHeader = req.Header("range")
	}

	// Range parsing → start offset + content length for the body slice.
	var (
		start         int64 // offset into the file
		contentLength = size
		status        = 200
		rangeRespVal  string
	)
	if rs, re, ok, unsatisfiable := parseSingleByteRange(rangeHeader, size); unsatisfiable {
		f.Close()
		headers := []responseHeader{
			{"Accept-Ranges", "bytes"},
			{"Last-Modified", lastMod},
			{"ETag", etag},
			{"Content-Range", fmt.Sprintf("bytes */%d", size)},
			{"Content-Length", "0"},
		}
		r.sendBytes(416, headers, nil)
		return nil
	} else if ok {
		start = rs
		contentLength = re - rs + 1
		status = 206
		rangeRespVal = fmt.Sprintf("bytes %d-%d/%d", rs, re, size)
	}

	// Empty body short-circuit — 0-byte file or empty range. Skip the
	// streaming path entirely and emit a fixed-length empty response.
	if contentLength == 0 {
		f.Close()
		headers := []responseHeader{
			{"Content-Type", ctype},
			{"Accept-Ranges", "bytes"},
			{"Last-Modified", lastMod},
			{"ETag", etag},
			{"Content-Length", "0"},
		}
		if attachment {
			headers = append(headers, responseHeader{"Content-Disposition", buildDisposition(filename, true)})
		}
		if rangeRespVal != "" {
			headers = append(headers, responseHeader{"Content-Range", rangeRespVal})
		}
		r.sendBytes(status, headers, nil)
		return nil
	}

	// Stage every header before Stream so they ship in the initial
	// status-line frame. Stream uses chunked transfer-encoding for
	// the body, so we intentionally omit Content-Length here — uWS
	// would otherwise advertise both a fixed length and chunked
	// framing, which is malformed per RFC 7230.
	r.Header("Accept-Ranges", "bytes")
	r.Header("Last-Modified", lastMod)
	r.Header("ETag", etag)
	if attachment {
		r.Header("Content-Disposition", buildDisposition(filename, true))
	}
	if rangeRespVal != "" {
		r.Header("Content-Range", rangeRespVal)
	}

	stream := func() {
		defer f.Close()
		_ = r.Stream(status, ctype, func(w io.Writer) error {
			if start > 0 {
				if _, err := f.Seek(start, io.SeekStart); err != nil {
					return err
				}
			}
			chunk := GetSendFileChunkBytes()
			backpressure := GetSendFileBackpressureBytes()
			buf := make([]byte, chunk)
			remaining := contentLength
			for remaining > 0 {
				toRead := int64(chunk)
				if remaining < toRead {
					toRead = remaining
				}
				n, rerr := f.Read(buf[:toRead])
				if n > 0 {
					if _, werr := w.Write(buf[:n]); werr != nil {
						return werr
					}
					remaining -= int64(n)
					if r.BufferedAmount() > backpressure {
						if derr := r.AwaitDrain(backpressure); derr != nil {
							return derr
						}
					}
				}
				if rerr == io.EOF {
					break
				}
				if rerr != nil {
					return rerr
				}
			}
			return nil
		})
	}

	// Sync handler → upgrade to async so disk I/O doesn't stall the
	// uWS loop thread and so Stream's prerequisites are met. Async
	// handlers stream inline on the worker goroutine.
	if r.async == nil {
		r.Async(stream)
		return nil
	}
	stream()
	return nil
}

// sendBytes writes a complete response (status + N headers + body) from
// any handler mode. Sync mode: direct cgo calls. Async mode: defers a
// corked write onto the loop. The body slice is captured by reference;
// callers must not mutate it after handing it over.
//
// The async path uses the loop pointer cached in asyncState rather than
// querying res.Loop() — uWS::Loop::get() is thread-local, and worker
// goroutines (shared-dispatch GetAsync) are not on the loop thread, so
// res.Loop() would return the wrong loop and the defer would never
// fire.
func (r *Response) sendBytes(code int, headers []responseHeader, body []byte) {
	r.statusCode = code
	line := statusLine(code)
	if r.async != nil && !r.async.sent {
		if r.dropAsyncIfAborted() {
			runtime.KeepAlive(body)
			return
		}
		r.async.sent = true
		hs := captureResponseHeaders(r.pendingHeaders, headers)
		r.pendingHeaders = r.pendingHeaders[:0]
		asyncDeferSendWithHeaders(r.async.loopPtr, r.async.ctxHandle, line, "", responseHeadersBlob(hs), bytesAsString(body))
		runtime.KeepAlive(body)
		return
	}
	r.inner.status(line)
	r.flushPendingHeaders()
	for _, h := range headers {
		r.inner.header(h.name, h.value)
	}
	r.inner.end(bytesAsString(body))
	runtime.KeepAlive(body)
}

func captureResponseHeaders(pending, extra []responseHeader) []responseHeader {
	if len(pending) == 0 {
		return extra
	}
	out := make([]responseHeader, 0, len(pending)+len(extra))
	out = append(out, pending...)
	out = append(out, extra...)
	return out
}

func responseHeadersBlob(headers []responseHeader) string {
	if len(headers) == 0 {
		return ""
	}
	var b strings.Builder
	for _, h := range headers {
		b.Grow(len(h.name) + len(h.value) + 2)
		b.WriteString(h.name)
		b.WriteByte(0)
		b.WriteString(h.value)
		b.WriteByte(0)
	}
	return b.String()
}

// bytesAsString aliases body as a Go string without copying. The string
// is only safe to use while body remains alive; pair with runtime.KeepAlive
// when crossing into cgo. Returns "" for the empty/nil slice.
func bytesAsString(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(body), len(body))
}

// etagMatch reports whether the If-None-Match header value matches etag.
// Handles the "*" wildcard and comma-separated lists. Strong and weak
// forms ("W/" prefix) compare equivalent for SendFile's purposes.
func etagMatch(inm, etag string) bool {
	inm = strings.TrimSpace(inm)
	if inm == "*" {
		return true
	}
	target := strings.TrimPrefix(etag, "W/")
	for _, candidate := range strings.Split(inm, ",") {
		candidate = strings.TrimSpace(candidate)
		candidate = strings.TrimPrefix(candidate, "W/")
		if candidate == target {
			return true
		}
	}
	return false
}

// parseSingleByteRange parses a Range header value with a single byte
// range. Returns ok=true with an inclusive [start,end] on success.
// unsatisfiable=true means the header was syntactically valid but cannot
// select bytes from this representation; the caller should return 416.
// Malformed headers, multi-range requests, and non-bytes units return
// ok=false/unsatisfiable=false and are ignored. Accepted forms:
//
//	bytes=start-end      — explicit range
//	bytes=start-         — start through size-1
//	bytes=-suffix        — last `suffix` bytes
//
// Per RFC 7233, malformed Range headers are ignored — the caller falls
// through to a normal 200 response with the full body.
func parseSingleByteRange(h string, size int64) (start, end int64, ok, unsatisfiable bool) {
	if h == "" || size == 0 {
		return 0, 0, false, false
	}
	const prefix = "bytes="
	if !strings.HasPrefix(h, prefix) {
		return 0, 0, false, false
	}
	spec := h[len(prefix):]
	if strings.Contains(spec, ",") {
		return 0, 0, false, false
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false, false
	}
	sStr := strings.TrimSpace(spec[:dash])
	eStr := strings.TrimSpace(spec[dash+1:])
	if sStr == "" {
		if eStr == "" {
			return 0, 0, false, false
		}
		suffix, err := strconv.ParseInt(eStr, 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false, false
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, true, false
	}
	sVal, err := strconv.ParseInt(sStr, 10, 64)
	if err != nil || sVal < 0 {
		return 0, 0, false, false
	}
	if sVal >= size {
		return 0, 0, false, true
	}
	if eStr == "" {
		return sVal, size - 1, true, false
	}
	eVal, err := strconv.ParseInt(eStr, 10, 64)
	if err != nil {
		return 0, 0, false, false
	}
	if eVal < sVal {
		return 0, 0, false, true
	}
	if eVal >= size {
		eVal = size - 1
	}
	return sVal, eVal, true, false
}

// buildDisposition builds a Content-Disposition header value per RFC 6266.
// filename is wrapped in quoted-string with " and \ escaped. Non-ASCII
// filenames also carry an RFC 5987 filename* parameter so clients that
// honor it can recover the original UTF-8 name. Control characters in
// the filename are dropped.
func buildDisposition(filename string, attachment bool) string {
	dispType := "inline"
	if attachment {
		dispType = "attachment"
	}
	if filename == "" {
		return dispType
	}
	var sb strings.Builder
	sb.WriteString(dispType)
	sb.WriteString(`; filename="`)
	asciiOnly := true
	for _, c := range filename {
		if c < 0x20 || c == 0x7f {
			continue
		}
		if c == '"' || c == '\\' {
			sb.WriteByte('\\')
		}
		if c < 0x80 {
			sb.WriteRune(c)
		} else {
			asciiOnly = false
			sb.WriteByte('_')
		}
	}
	sb.WriteByte('"')
	if !asciiOnly {
		sb.WriteString(`; filename*=UTF-8''`)
		sb.WriteString(percentEncodeRFC5987(filename))
	}
	return sb.String()
}

// percentEncodeRFC5987 percent-encodes s per RFC 5987 ext-value rules:
// ALPHA / DIGIT and a small set of punctuation pass through verbatim;
// every other byte (including all multi-byte UTF-8 continuation bytes)
// is percent-encoded.
func percentEncodeRFC5987(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case (c >= 'A' && c <= 'Z') ||
			(c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') ||
			c == '!' || c == '#' || c == '$' || c == '&' ||
			c == '+' || c == '-' || c == '.' || c == '^' ||
			c == '_' || c == '`' || c == '|' || c == '~':
			sb.WriteByte(c)
		default:
			const hex = "0123456789ABCDEF"
			sb.WriteByte('%')
			sb.WriteByte(hex[c>>4])
			sb.WriteByte(hex[c&0x0F])
		}
	}
	return sb.String()
}

// Async marks the response for asynchronous handling and runs fn on a new
// goroutine. After calling Async, subsequent Status/Header/Write calls buffer
// Go-side and End flushes the buffered response back onto the event loop with
// a single cork. fn may block freely.
//
// Call Async at most once per response, and before any synchronous
// Status/Header/Write/End calls. The outer handler should return immediately
// after calling Async.
func (r *Response) Async(fn func()) {
	if r.async != nil {
		return
	}
	a := asyncStatePool.Get().(*asyncState)
	a.loopPtr, a.ctxHandle = r.inner.beginAsync()
	a.status = "200 OK"
	a.sent = false
	r.async = a
	r.acquireRef()

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				reportPanic(recovered)
				if !a.sent {
					r.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
				}
			}
			if !a.sent {
				// fn returned without invoking Send/End, or a panic prevented
				// the best-effort 500 from being sent. Drop the response.
				asyncCtxRelease(a.ctxHandle)
			}
			r.finishAsync(a)
		}()
		fn()
	}()
}

func (r *Response) flushAsync() {
	a := r.async
	if a.sent {
		return
	}
	if r.dropAsyncIfAborted() {
		return
	}
	if len(r.pendingHeaders) > 0 {
		// Pack buffered headers as name\0value\0... and route through the
		// defer-send-with-headers cgo shim. Used by Compress (which emits
		// Content-Encoding + Vary) and by any pre-Async middleware that
		// added headers via res.Header().
		var b strings.Builder
		for _, h := range r.pendingHeaders {
			b.Grow(len(h.name) + len(h.value) + 2)
			b.WriteString(h.name)
			b.WriteByte(0)
			b.WriteString(h.value)
			b.WriteByte(0)
		}
		asyncDeferSendWithHeaders(a.loopPtr, a.ctxHandle, a.status, a.contentType, b.String(), a.body.String())
		r.pendingHeaders = r.pendingHeaders[:0]
	} else {
		asyncDeferSend(a.loopPtr, a.ctxHandle, a.status, a.contentType, a.body.String())
	}
	a.sent = true
}

func (r *Response) dropAsyncIfAborted() bool {
	a := r.async
	if a == nil || a.sent || !asyncCtxAborted(a.ctxHandle) {
		return false
	}
	asyncCtxRelease(a.ctxHandle)
	a.sent = true
	return true
}

// acquireRef adds one to the wrapper's refcount. Callers must pair every
// acquireRef with exactly one releaseRef. Safe to call from any goroutine.
func (r *Response) acquireRef() {
	r.refs.Add(1)
}

// releaseRef drops one ref. The decrement that takes the count to zero is
// the unique recycle point: it returns the wrapper's inner pointer to nil
// and Puts the wrapper back into responsePool. Any later access via a stale
// pointer is the caller's bug (they kept a ref past releaseRef).
func (r *Response) releaseRef() {
	if r.refs.Add(-1) != 0 {
		return
	}
	// Drain any OnFinish callbacks before tearing the response state
	// down. For sync responses this is the ONLY drain point — and it
	// runs from the HTTP handler's defer AFTER the framework's panic
	// recover + Send(500), so callbacks see the final statusCode.
	// For async responses finishAsync already drained the queue;
	// we'll see an empty slice and skip.
	r.finishMu.Lock()
	fns := r.onFinish
	r.onFinish = nil
	r.finished = true
	r.finishMu.Unlock()
	for _, fn := range fns {
		runFinishCallback(fn)
	}

	r.inner = responseNative{}
	r.async = nil
	r.statusCode = 0
	r.app = nil
	if r.pendingHeaders != nil {
		r.pendingHeaders = r.pendingHeaders[:0]
	}
	if r.encoder != nil {
		releaseBodyEncoder(r.encoder)
		r.encoder = nil
	}
	r.abort = nil
	// Reset the finish state for the next pool use — we already
	// drained above, but the recycled wrapper needs a clean slate.
	r.finishMu.Lock()
	r.finished = false
	r.finishMu.Unlock()
	responsePool.Put(r)
}

// SetBodyEncoder installs a body transformer to apply right before
// the response body is flushed to uWS. The encoder receives the
// concatenated bytes from any Write / End / Send / JSON calls, and
// returns the encoded body together with a Content-Encoding value to
// attach (or empty string to skip encoding and emit the original
// bytes unchanged).
//
// Compression middleware uses this hook to transparently gzip /
// brotli the handler's output without requiring handlers to be aware
// of the encoding. The encoder runs in both sync and async modes;
// when the encoder emits a Content-Encoding header, the response is
// routed through the defer-send-with-headers C shim (async fast
// shared-memory path is bypassed because it has no slot for extra
// headers).
//
// Only one encoder per response — calling SetBodyEncoder a second
// time replaces the first. Once the encoder has run (on End or Send)
// it is consumed; subsequent writes go through the normal direct
// path.
func (r *Response) SetBodyEncoder(encode func(body []byte, contentType string) (encoded []byte, contentEncoding string)) {
	r.SetBodyEncoderLimit(0, encode)
}

// SetBodyEncoderLimit is SetBodyEncoder plus a staging-buffer cap. When
// maxBytes is positive and the buffered body would grow beyond it, the encoder
// is bypassed and the response continues unencoded from the original bytes.
// This lets middleware such as Compress bound both compression work and the
// pre-compression staging buffer.
func (r *Response) SetBodyEncoderLimit(maxBytes int, encode func(body []byte, contentType string) (encoded []byte, contentEncoding string)) {
	if encode == nil {
		// Replacing an installed encoder with nil — return the previous
		// one to the pool so its backing buffer can be reused.
		if r.encoder != nil {
			releaseBodyEncoder(r.encoder)
			r.encoder = nil
		}
		return
	}
	if maxBytes < 0 {
		maxBytes = 0
	}
	e := bodyEncoderPool.Get().(*bodyEncoder)
	e.encode = encode
	e.max = maxBytes
	e.applied = false
	// Truncate keeps capacity; the previous request's bytes (if any)
	// are overwritten by subsequent appends.
	e.buf = e.buf[:0]
	r.encoder = e
}

// releaseBodyEncoder clears the encoder's fields and returns it to
// the pool. The backing buffer is dropped if it exceeds
// bodyEncoderMaxCap so a single huge response can't anchor a large
// allocation in the pool indefinitely.
func releaseBodyEncoder(e *bodyEncoder) {
	if cap(e.buf) > bodyEncoderMaxCap {
		e.buf = nil
	} else {
		e.buf = e.buf[:0]
	}
	e.encode = nil
	e.max = 0
	e.applied = false
	bodyEncoderPool.Put(e)
}

func (e *bodyEncoder) writeString(s string) (prefix string, overflow bool) {
	if e.max > 0 && len(s) > e.max-len(e.buf) {
		e.applied = true
		prefix = string(e.buf)
		e.buf = e.buf[:0]
		return prefix, true
	}
	e.buf = append(e.buf, s...)
	return "", false
}

// applyEncoder runs the buffered body through the encoder, attaches
// Content-Encoding and Vary headers if the encoder returned a
// non-empty encoding, and returns the encoded body string ready to
// hand to uWS. Marks the encoder as applied so a follow-up Write
// after End (programmer error) takes the direct path instead of
// double-encoding.
//
// contentTypeHint is the Content-Type passed inline to Send (which
// goes straight to uWS, bypassing pendingHeaders). When empty,
// applyEncoder scans pendingHeaders for an explicit Content-Type so
// the encoder can decide compressibility from the MIME type.
func (r *Response) applyEncoder(contentTypeHint string) string {
	e := r.encoder
	if e == nil || e.applied {
		return ""
	}
	ct := contentTypeHint
	if ct == "" {
		for _, h := range r.pendingHeaders {
			if strings.EqualFold(h.name, "Content-Type") {
				ct = h.value
				break
			}
		}
	}
	if r.hasPendingHeader("Content-Encoding") {
		e.applied = true
		return string(e.buf)
	}
	encoded, contentEncoding := e.encode(e.buf, ct)
	e.applied = true
	if contentEncoding != "" {
		r.pendingHeaders = append(r.pendingHeaders,
			responseHeader{name: "Content-Encoding", value: contentEncoding},
			responseHeader{name: "Vary", value: "Accept-Encoding"},
		)
	}
	return string(encoded)
}

func (r *Response) hasPendingHeader(name string) bool {
	for _, h := range r.pendingHeaders {
		if strings.EqualFold(h.name, name) {
			return true
		}
	}
	return false
}

// finishAsync returns the asyncState to its pool, then drops one wrapper
// ref. Used by the Async goroutine after fn returns and by runSharedHandler
// once the user-supplied AsyncHandler is done.
func (r *Response) finishAsync(a *asyncState) {
	// Drain OnFinish callbacks before recycling. Callbacks registered
	// after this point (e.g. a fast goroutine that finished before
	// the outer middleware called OnFinish) fall into the
	// r.finished == true branch and execute inline.
	r.finishMu.Lock()
	fns := r.onFinish
	r.onFinish = nil
	r.finished = true
	r.finishMu.Unlock()
	for _, fn := range fns {
		runFinishCallback(fn)
	}

	a.loopPtr = 0
	a.ctxHandle = 0
	a.status = ""
	a.contentType = ""
	a.body.Reset()
	a.sent = false
	asyncStatePool.Put(a)
	r.releaseRef()
}

// runFinishCallback isolates a single OnFinish callback's panic so
// one misbehaving cleanup hook can't poison the rest of the chain
// (and so the underlying response release still runs).
func runFinishCallback(fn func()) {
	defer func() {
		if rec := recover(); rec != nil {
			reportPanic(rec)
		}
	}()
	fn()
}

// OnFinish registers fn to fire after the response's handler — and
// any goroutine spawned via Response.Async — has fully completed.
// This is the safe hook for middleware that needs to persist or
// flush state derived from the request:
//
//	// Inside middleware, before calling next(res, req):
//	defer res.OnFinish(func() { saveSession(s) })
//	next(res, req)
//
// The hook decides automatically which mode applies:
//
//   - If the handler never upgraded to async (sync route, no
//     Response.Async call), fn runs synchronously now — the handler
//     has already returned so its state is stable.
//   - If the handler went async via Response.Async or the route was
//     GetAsync, fn is queued and runs inside finishAsync after the
//     goroutine completes.
//   - If the async goroutine finished BEFORE the middleware reached
//     OnFinish (fast handlers), fn runs inline so it's never
//     orphaned.
//
// Multiple registrations fire in FIFO order. Panics inside fn are
// captured by the framework's panic handler and do not prevent
// later callbacks from running, mirroring net/http's
// http.ResponseWriter recovery posture.
//
// Without this hook, middleware that does
// `next(res, req); persist(state)` and a handler that calls
// Response.Async would silently persist pre-async state — the
// goroutine's mutations land after persist already ran.
//
// Drain timing:
//
//   - Sync response: callbacks queue and fire from releaseRef, which
//     runs from the framework's HTTP-handler defer AFTER any panic
//     recovery and Send(500). This is what makes Logger / Metrics see
//     status=500 when the handler panics rather than the pre-panic
//     statusCode value.
//   - Async response (Response.Async or GetAsync): callbacks fire
//     from finishAsync as soon as the goroutine completes, then
//     releaseRef finds the queue empty when it eventually recycles.
//   - Late-registration race in async mode: if the goroutine drained
//     and reset r.finished before the middleware reached OnFinish,
//     the callback runs inline — the response wrapper is still alive
//     because the caller still holds a ref.
func (r *Response) OnFinish(fn func()) {
	if fn == nil {
		return
	}
	r.finishMu.Lock()
	if r.finished {
		// Async goroutine already drained the queue — register lost
		// the race to finishAsync. Run inline; the response wrapper
		// is still alive (caller holds a ref).
		r.finishMu.Unlock()
		runFinishCallback(fn)
		return
	}
	r.onFinish = append(r.onFinish, fn)
	r.finishMu.Unlock()
}

// Loop returns the event loop that owns this response. Capture it inside the
// handler before spawning a goroutine. The returned Loop is safe to use from
// any goroutine; the Response itself is not.
func (r *Response) Loop() *Loop {
	return &Loop{inner: r.inner.loop()}
}

// OnAborted registers an abort callback and returns an atomic flag that is set
// to true if the client disconnects before the response is sent. Must be called
// synchronously inside the route handler when responding asynchronously.
func (r *Response) OnAborted() *Aborted {
	state := &Aborted{}
	r.onAbort(func() {
		state.state.Store(true)
	})
	return state
}

func (r *Response) onAbort(fn func()) {
	if fn == nil {
		return
	}
	state := r.abort
	if state == nil {
		state = &responseAbortState{}
		r.abort = state
		r.inner.onAborted(func() {
			state.run()
		})
	}
	state.add(fn)
}

func (r *Response) onAbortFirst(fn func()) {
	if fn == nil {
		return
	}
	state := r.abort
	if state == nil {
		state = &responseAbortState{}
		r.abort = state
		r.inner.onAborted(func() {
			state.run()
		})
	}
	state.prepend(fn)
}

func (s *responseAbortState) add(fn func()) {
	s.mu.Lock()
	if s.fired {
		s.mu.Unlock()
		runAbortCallback(fn)
		return
	}
	s.callbacks = append(s.callbacks, fn)
	s.mu.Unlock()
}

func (s *responseAbortState) prepend(fn func()) {
	s.mu.Lock()
	if s.fired {
		s.mu.Unlock()
		runAbortCallback(fn)
		return
	}
	s.callbacks = append(s.callbacks, nil)
	copy(s.callbacks[1:], s.callbacks[:len(s.callbacks)-1])
	s.callbacks[0] = fn
	s.mu.Unlock()
}

func (s *responseAbortState) run() {
	s.mu.Lock()
	if s.fired {
		s.mu.Unlock()
		return
	}
	s.fired = true
	fns := s.callbacks
	s.callbacks = nil
	s.mu.Unlock()

	for _, fn := range fns {
		runAbortCallback(fn)
	}
}

func runAbortCallback(fn func()) {
	defer func() {
		if rec := recover(); rec != nil {
			reportPanic(rec)
		}
	}()
	fn()
}

// Cork batches all response writes inside fn into a single packet. Required
// when sending a response from a deferred callback to avoid uWebSockets warning
// about uncorked writes.
func (r *Response) Cork(fn func()) {
	r.inner.cork(fn)
}

// OnData registers a body-chunk callback. fn is invoked once per chunk uWS
// receives, with isLast == true on the final chunk. Must be called inside
// the route handler, before it returns. The callback runs on the loop thread
// — spawn a goroutine inside it if the work can block.
//
// Each chunk slice is freshly allocated; the caller owns it and may retain
// references after fn returns.
// OnData takes one wrapper ref at registration. The ref is released on
// isLast — at which point the cgo handle for the lambda is also freed.
// Body's onAborted handler releases this ref on early abort (when the last
// chunk would never fire).
// OnData registers a body-chunk callback. Each chunk is freshly allocated;
// the caller owns it. fn runs on the loop thread.
//
// OnData takes one wrapper ref at registration and releases it on the
// final chunk (isLast == true). When the connection aborts before isLast
// fires the ref is held until the response is destroyed; for the
// Body() collector that case is covered explicitly via its own onAborted.
func (r *Response) OnData(fn func(chunk []byte, isLast bool)) {
	// Mirror the C++ side's Content-Length-based body_limit_rejects
	// gate for chunked transfer-encoded requests, which have no
	// Content-Length and therefore slip past that pre-handler check.
	// Without this, a handler that called OnData against an
	// unbounded chunked upload would keep accumulating bytes
	// forever — the C++ comment acknowledged the gap and told the
	// caller to police it themselves; we now police it here so
	// every OnData consumer inherits the same cap as Post/Any
	// routes with a known Content-Length.
	//
	// Body(maxBytes, done) already enforces its own cap; this gate
	// applies on top, so a handler that asks for 10 MiB max but the
	// app config sets BodyLimit=4 MiB still tops out at 4 MiB.
	var bodyLimit int
	if r.app != nil {
		bodyLimit = r.app.cfg.BodyLimit
	}
	var total int
	var exceeded bool
	var released bool
	release := func() {
		if released {
			return
		}
		released = true
		r.releaseRef()
	}
	r.acquireRef()
	// uWS rejects handler returns without either a response or an
	// abort handler ("Returning from a request handler without
	// responding or attaching an abort handler is forbidden!").
	// OnData is the "I'll respond later from a chunk callback"
	// shape, so we register a no-op aborted handler here to satisfy
	// that check — without it, a connection abort mid-body crashes
	// the process. The handler also releases our pinned ref so the
	// wrapper can recycle even when the client disconnects.
	r.onAbortFirst(release)
	r.inner.onData(func(chunk []byte, isLast bool) {
		if exceeded {
			// Already emitted 413 — swallow tail chunks until
			// isLast so we can release the wrapper ref once.
			if isLast {
				release()
			}
			return
		}
		total += len(chunk)
		if bodyLimit > 0 && total > bodyLimit {
			exceeded = true
			r.Send(413, "text/plain; charset=utf-8", "payload too large\n")
			if isLast {
				release()
			}
			return
		}
		fn(chunk, isLast)
		if isLast {
			release()
		}
	})
}

// ErrBodyTooLarge is reported by Body when the request body exceeds the
// caller-supplied max size.
var ErrBodyTooLarge = errFramework("body exceeds max size")

// ErrBodyTimeout is reported by Body when the request body does not
// finish arriving within Config.BodyReadTimeout. The handler sees the
// error in its done callback exactly once; later chunks from the
// slow client are dropped on the floor.
var ErrBodyTimeout = errFramework("body read deadline exceeded")

type errFramework string

func (e errFramework) Error() string { return string(e) }

// Body collects the full request body and invokes done once it has arrived.
// If the body exceeds the effective limit, done is called with err =
// ErrBodyTooLarge and the response is closed without sending. If the client
// aborts before the body completes, done is not called; use OnAborted or
// Request.Context for abort cleanup. Call inside the route handler before it
// returns; done runs on the loop thread (spawn a goroutine for blocking work).
//
// The effective limit is the LOWER of maxBytes and Config.BodyLimit when
// BOTH are positive. A handler that asks for 10 MiB on an app configured
// with BodyLimit=1 MiB tops out at 1 MiB — the app cap wins. This
// mirrors the OnData and Content-Length gates so all three intake paths
// enforce the same upper bound; without the clamp here, a chunked upload
// (which sidesteps the C++ Content-Length pre-check) could exceed the
// app's BodyLimit whenever the handler's local cap was larger.
//
// maxBytes <= 0 is honored literally and NOT widened by Config.BodyLimit:
// Body(0, ...) accepts a zero-byte body and rejects everything else;
// Body(-1, ...) rejects every chunk. Clamping is one-directional — the
// app cap can tighten the caller's request, never loosen it.
func (r *Response) Body(maxBytes int, done func(body []byte, err error)) {
	if r.app != nil && maxBytes > 0 {
		if bl := r.app.cfg.BodyLimit; bl > 0 && bl < maxBytes {
			maxBytes = bl
		}
	}
	var (
		buf      []byte
		finished bool
		timer    *time.Timer
	)
	aborted := &Aborted{}
	// Body takes one wrapper ref that's released exactly once on whichever
	// of these fires first: the final chunk, body-too-large, abort, or
	// timeout. Going through r.inner.onData directly (instead of
	// Response.OnData) keeps that release symmetric — Response.OnData
	// would release on isLast on its own and double-release with us on
	// bodyTooLarge/abort.
	r.acquireRef()
	release := func() {
		if finished {
			return
		}
		finished = true
		if timer != nil {
			timer.Stop()
		}
		r.releaseRef()
	}
	r.onAbortFirst(func() {
		aborted.state.Store(true)
		release()
	})

	// Arm a body-read deadline if Config.BodyReadTimeout is set.
	// Timer fires on a goroutine; we hand the cancellation back to
	// the loop thread via Loop.Defer so done() and release run
	// serialized with the onData / onAborted callbacks above.
	if r.app != nil && r.app.cfg.BodyReadTimeout > 0 {
		loop := r.Loop()
		timer = time.AfterFunc(r.app.cfg.BodyReadTimeout, func() {
			loop.Defer(func() {
				if finished {
					return
				}
				done(nil, ErrBodyTimeout)
				release()
			})
		})
	}

	r.inner.onData(func(chunk []byte, isLast bool) {
		if finished || aborted.Load() {
			return
		}
		if len(buf)+len(chunk) > maxBytes {
			done(nil, ErrBodyTooLarge)
			release()
			return
		}
		if buf == nil && len(chunk) > 0 {
			buf = make([]byte, 0, len(chunk))
		}
		buf = append(buf, chunk...)
		if isLast {
			done(buf, nil)
			release()
		}
	})
}

// Loop is a uWebSockets event loop. Use Defer to schedule work back onto the
// loop thread from any goroutine.
type Loop struct {
	inner loopNative
}

// Defer schedules fn to run on the loop thread. Safe to call from any
// goroutine. fn runs once, in FIFO order with other deferred callbacks.
func (l *Loop) Defer(fn func()) {
	l.inner.defer_(fn)
}

// Aborted is a thread-safe flag set when a client aborts before the response is
// fully sent. Check Load before touching the Response from a deferred callback.
type Aborted struct {
	state atomic.Bool
}

// Load reports whether the response was aborted.
func (a *Aborted) Load() bool {
	return a.state.Load()
}

// Request wraps a uWebSockets request. Sync handlers receive a Request
// backed by the live uWS HttpRequest; async/shared handlers receive a
// Request backed by a snapshot copied into the AsyncCtx before the original
// request was freed.
//
// Both modes expose the same accessors. The shared snapshot has fixed capacity
// per field (URL 256, query 512, params 64 each up to 8, headers 8 KB total);
// requests past those caps are rejected with 431 before reaching user code.
type Request struct {
	inner requestNative
	snap  *requestSnapshot

	// body is the fully collected request body for body-async routes, set
	// by the runtime wrapper before invoking the async middleware chain so
	// async middleware can inspect it via Body() and the final user handler
	// receives it as a parameter.
	body []byte

	// locals carries request-scoped key/value state between middleware and
	// the final handler. Lazy: nil until the first SetLocal. Cleared (but
	// the map is reused) when the Request returns to the pool.
	locals map[string]any

	// sync{Method,URL,Query}{Ptr,Len} and syncParam{Ptrs,Lens} point at
	// uWS-parsed bytes inside the live HttpRequest. The C++ bridge fills
	// them at handler entry so Request's accessors can materialize a Go
	// string on first read without a cgo round-trip. Pointers are valid
	// only for the lifetime of the sync callback; resetForPool clears them
	// before the wrapper returns to the pool.
	//
	// Only the first four route parameters are pre-cached; reads past
	// index 3 fall through to the cgo Parameter helper. Real routes
	// rarely exceed that.
	syncMethodPtr unsafe.Pointer
	syncMethodLen int
	syncURLPtr    unsafe.Pointer
	syncURLLen    int
	syncQueryPtr  unsafe.Pointer
	syncQueryLen  int
	syncParamPtrs [4]unsafe.Pointer
	syncParamLens [4]int

	// syncHeadersPtr / syncHeadersLen point at the per-request
	// `name\0value\0name\0value\0…` blob that dispatch_sync packs in
	// a stack scratch buffer before calling into Go. Request.Header
	// scans this blob first and falls back to the cgo getHeader helper
	// only when the C++ scratch buffer overflowed. Middleware that
	// probes optional headers avoids a cgo call on ordinary misses.
	//
	// The pointer is valid only for the lifetime of the cgo
	// callback (= the lifetime of reqWrap before it returns to the
	// pool). resetForPool clears it on return.
	syncHeadersPtr      unsafe.Pointer
	syncHeadersLen      int
	syncHeadersComplete bool

	// syncResPtr is the live uWS response pointer for sync-mode handlers.
	// req.IP() uses it to lazily fetch the peer address via cgo on demand
	// (most handlers don't read IP, so pre-caching would be wasted work).
	// Async / shared snapshots carry the IP in r.snap.ip and don't need
	// this pointer.
	syncResPtr unsafe.Pointer

	// trustProxy is set by the App.wrap closure when Config.TrustProxy is
	// enabled, so Protocol() / Secure() know to honor X-Forwarded-Proto.
	// Default false: in the common direct-exposed deployment the
	// X-Forwarded-* family is client-controllable and must not be
	// believed.
	trustProxy bool

	// cached{URL,Method,Query,Params,IP} hold the materialized Go strings
	// allocated lazily on the first accessor call. The corresponding
	// *Cached field is true once the cache slot is valid (the empty
	// string is a legitimate cached value).
	cachedURL    string
	urlCached    bool
	cachedMethod string
	methodCached bool
	cachedQuery  string
	queryCached  bool
	cachedParams [4]string
	paramCached  [4]bool
	cachedIP     string
	ipCached     bool
	// paramNames maps the route pattern's positional :params to their
	// declared names. Set by the route wrapper before middleware
	// runs; used by Request.Param(name) to translate a name back to
	// an index lookup. Nil for routes that have no named params or
	// for legacy code paths that bypass the wrapper.
	paramNames []string

	// res is the back-pointer to the Response wrapper for this
	// request, set by the dispatch site before user middleware /
	// handlers run. Used by Context() to register a cancellation
	// hook against the response's onAborted multiplexer. Nil for
	// requests that bypass the dispatch path (test fixtures); in
	// that case Context() returns a non-cancelable background
	// context so handlers still get a usable value.
	res *Response

	// ctx / ctxCancel back Request.Context(). Lazy-initialized on
	// the first Context() call so handlers that never touch it pay
	// nothing. Sync handlers hook ctxCancel to the Response's
	// onAborted callback list. Async handlers cannot safely register
	// uWS onAborted callbacks from worker goroutines, so they watch
	// the AsyncCtx abort flag instead.
	ctx       context.Context
	ctxCancel context.CancelFunc
}

// requestSnapshot holds the Go-side captured copy of an HttpRequest, used by
// async and shared-dispatch handlers where uWS has already freed the
// underlying request. Populated either by the worker reading AsyncCtx fields
// (shared path) or by the sync wrapper before spawning a goroutine
// (middleware fallback path).
type requestSnapshot struct {
	method     string
	url        string
	query      string
	ip         string
	params     []string
	paramNames []string
	truncated  bool
	// headers is the raw "name\0value\0name\0value\0..." buffer captured from
	// C++; we parse on access rather than building a map up front so the hot
	// path stays allocation-light when headers aren't read.
	headers []byte
}

// SetLocal stores a request-scoped value under key. Intended for passing
// state from middleware down to the handler (e.g. an authenticated user
// resolved by async middleware). The value lives only as long as the
// request — the map is cleared when the Request returns to its pool.
//
// SetLocal is not safe for concurrent use within a single request; treat
// the Request as owned by whatever goroutine is currently running it.
func (r *Request) SetLocal(key string, value any) {
	if r.locals == nil {
		r.locals = make(map[string]any, 4)
	}
	r.locals[key] = value
}

// Local fetches a value previously stored with SetLocal. Returns nil if
// the key is absent.
func (r *Request) Local(key string) any {
	if r.locals == nil {
		return nil
	}
	return r.locals[key]
}

// Body returns the fully collected request body for body-async routes such as
// PostAsync, PutAsync, PatchAsync, and DeleteAsync; for other routes it
// returns nil. The slice is owned by the framework — do not retain it past the
// handler call.
func (r *Request) Body() []byte {
	return r.body
}

// Context returns a context.Context bound to this request's lifetime.
// It is canceled (with a non-nil Err) when the client aborts the
// connection before the response is sent, so downstream calls that
// accept a context — db.QueryContext, http.NewRequestWithContext,
// rate-limited goroutines — will short-circuit instead of doing wasted
// work on behalf of a vanished caller.
//
// The context is lazy: created on the first call and reused for
// subsequent calls on the same request. Handlers that never touch it
// pay nothing. It is canceled both on client abort and on handler
// completion (via the pool reset), so callbacks registered via
// context.AfterFunc fire reliably even on the success path.
//
// For requests constructed outside the dispatch pipeline (e.g. test
// fixtures that allocate a Request directly), Context() returns
// context.Background() — usable but non-cancelable.
func (r *Request) Context() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	if r.res == nil {
		// No back-pointer means there's no response to hook onAborted
		// against; hand the caller a usable background context rather
		// than panicking. Test code and legacy callers land here.
		return context.Background()
	}
	r.ctx, r.ctxCancel = context.WithCancel(context.Background())
	if a := r.res.async; a != nil && a.ctxHandle != 0 {
		watchAsyncRequestAbort(r.ctx, r.ctxCancel, a.ctxHandle)
	} else {
		r.res.onAbort(r.ctxCancel)
	}
	return r.ctx
}

const requestContextAbortPollInterval = time.Millisecond

func watchAsyncRequestAbort(ctx context.Context, cancel context.CancelFunc, ctxHandle uintptr) {
	if asyncCtxAborted(ctxHandle) {
		cancel()
		return
	}
	asyncCtxRetain(ctxHandle)
	go func() {
		defer asyncCtxRelease(ctxHandle)
		ticker := time.NewTicker(requestContextAbortPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if asyncCtxAborted(ctxHandle) {
					cancel()
					return
				}
			}
		}
	}()
}

// resetForPool clears every request-scoped field so the wrapper can return
// to the sync.Pool without leaking the previous request's data into the
// next user. Keeping the locals map alive avoids re-allocating on the next
// SetLocal — just empty it.
func (r *Request) resetForPool() {
	r.inner = requestNative{}
	r.snap = nil
	r.body = nil
	r.syncMethodPtr = nil
	r.syncMethodLen = 0
	r.syncURLPtr = nil
	r.syncURLLen = 0
	r.syncQueryPtr = nil
	r.syncQueryLen = 0
	r.syncHeadersPtr = nil
	r.syncHeadersLen = 0
	r.syncHeadersComplete = false
	r.syncParamPtrs = [4]unsafe.Pointer{}
	r.syncParamLens = [4]int{}
	r.syncResPtr = nil
	r.cachedURL = ""
	r.urlCached = false
	r.cachedMethod = ""
	r.methodCached = false
	r.cachedQuery = ""
	r.queryCached = false
	r.cachedParams = [4]string{}
	r.paramCached = [4]bool{}
	r.cachedIP = ""
	r.ipCached = false
	r.trustProxy = false
	r.paramNames = nil
	// Cancel any live request context so goroutines blocked on
	// req.Context().Done() unblock and release. Idempotent; safe to
	// call when the context already fired from onAborted.
	if r.ctxCancel != nil {
		r.ctxCancel()
	}
	r.ctx = nil
	r.ctxCancel = nil
	r.res = nil
	for k := range r.locals {
		delete(r.locals, k)
	}
}

// URL returns the request URL path. Query string is exposed separately via
// Query(); URL() does not include it.
//
// In sync mode the bridge stashes the URL bytes uWS already parsed onto
// the Request at handler entry, so the first call materializes a Go string
// from those bytes (one allocation, no cgo). Subsequent calls return the
// cached string. Async/shared handlers read from the captured snapshot.
func (r *Request) URL() string {
	if r.snap != nil {
		return r.snap.url
	}
	if r.urlCached {
		return r.cachedURL
	}
	if r.syncURLPtr != nil {
		r.cachedURL = goStringFromC(r.syncURLPtr, r.syncURLLen)
		r.urlCached = true
		return r.cachedURL
	}
	// Fallback: bridge did not pre-fill the URL (shouldn't happen for sync
	// HTTP handlers, but the cgo path remains available for safety).
	s := r.inner.url()
	r.cachedURL = s
	r.urlCached = true
	return s
}

// Method returns the HTTP method ("get", "post", ...). uWS lower-cases it
// during parsing. Sync handlers serve this from the pre-cached method
// pointer the bridge stashed at handler entry; first call allocates a
// Go string, subsequent calls return the cached value — no cgo on the
// hot path.
func (r *Request) Method() string {
	if r.snap != nil {
		return r.snap.method
	}
	if r.methodCached {
		return r.cachedMethod
	}
	if r.syncMethodPtr != nil {
		r.cachedMethod = goStringFromC(r.syncMethodPtr, r.syncMethodLen)
		r.methodCached = true
		return r.cachedMethod
	}
	s := r.inner.method()
	r.cachedMethod = s
	r.methodCached = true
	return s
}

// Header returns a request header value. Header lookups in async/shared
// handlers parse the snapshot buffer on every call; cache the value if you
// need it multiple times.
//
// In sync mode the C++ dispatcher packs request headers into a stack
// scratch blob before calling into Go, so this scan is allocation-free
// and pays zero cgo per call. Ordinary misses return from Go; only
// requests whose headers overflow the 8 KB scratch buffer fall back to
// the uWS getHeader helper via cgo.
func (r *Request) Header(name string) string {
	if r.snap != nil {
		return r.snap.lookupHeader(name)
	}
	if r.syncHeadersPtr != nil {
		if r.syncHeadersLen > 0 {
			if v, ok := lookupHeaderInSyncBlob(r.syncHeadersPtr, r.syncHeadersLen, name); ok {
				return v
			}
		}
		if r.syncHeadersComplete {
			return ""
		}
		// Miss on an incomplete packed blob can mean the header was
		// past the scratch cap. Fall through to cgo so uWS remains
		// the source of truth for overflow requests.
	}
	return r.inner.header(name)
}

// lookupHeaderInSyncBlob scans the dispatcher-packed
// `name\0value\0…` blob for a header by name. Returns
// (value, true) on hit, ("", false) on miss. Name comparison is
// case-insensitive — uWS stores header names lowercase so the
// search needle is normalized once up front.
func lookupHeaderInSyncBlob(ptr unsafe.Pointer, ln int, name string) (string, bool) {
	if name == "" {
		return "", false
	}
	buf := unsafe.Slice((*byte)(ptr), ln)
	needle := name
	for i := 0; i < len(needle); i++ {
		c := needle[i]
		if c >= 'A' && c <= 'Z' {
			b := make([]byte, len(needle))
			for j := 0; j < len(needle); j++ {
				x := needle[j]
				if x >= 'A' && x <= 'Z' {
					x += 'a' - 'A'
				}
				b[j] = x
			}
			needle = string(b)
			break
		}
	}
	for len(buf) > 0 {
		j := indexOfZero(buf)
		if j < 0 {
			return "", false
		}
		key := buf[:j]
		buf = buf[j+1:]
		if len(buf) == 0 {
			return "", false
		}
		j = indexOfZero(buf)
		if j < 0 {
			return "", false
		}
		value := buf[:j]
		buf = buf[j+1:]
		if len(key) == len(needle) && bytesEqualLower(key, needle) {
			return string(value), true
		}
	}
	return "", false
}

// Get is an alias for Header (case-insensitive header lookup). Mirrors the
// req.get(name) helper that fiber / express users reach for first.
func (r *Request) Get(name string) string {
	return r.Header(name)
}

// Headers iterates every request header pair, lowercase name first.
// fn returns false to stop early — same convention as
// sync.Map.Range. Returns the number of headers visited.
//
// In sync mode this walks the per-call scratch blob the C++ dispatcher packs
// into the request when the blob is complete. Requests whose header set
// overflows that scratch space fall back to one native full-header dump so
// trailing headers are not silently hidden. Async handlers walk the snapshot
// blob captured before the live request was freed. Names are returned in the
// order uWS parsed them, which matches the order on the wire.
//
// Useful for middleware that copies headers verbatim (tracing
// context propagation, raw audit logs, etc.) without paying one
// Header(name) call per known header.
//
//	req.Headers(func(name, value string) bool {
//	    out.Header(name, value)
//	    return true
//	})
func (r *Request) Headers(fn func(name, value string) bool) int {
	if fn == nil {
		return 0
	}
	blob := r.headersBlobForIteration(r.inner.headersAll)
	count := 0
	for len(blob) > 0 {
		j := indexOfZero(blob)
		if j < 0 {
			break
		}
		name := string(blob[:j])
		blob = blob[j+1:]
		if len(blob) == 0 {
			break
		}
		j = indexOfZero(blob)
		if j < 0 {
			break
		}
		value := string(blob[:j])
		blob = blob[j+1:]
		count++
		if !fn(name, value) {
			return count
		}
	}
	return count
}

func (r *Request) headersBlobForIteration(fullDump func() []byte) []byte {
	switch {
	case r.snap != nil:
		return r.snap.headers
	case r.syncHeadersPtr != nil:
		if r.syncHeadersComplete {
			return unsafe.Slice((*byte)(r.syncHeadersPtr), r.syncHeadersLen)
		}
		// The dispatcher stopped before packing every header. Pull a
		// complete dump while the live uWS request is still valid so
		// middleware that audits or forwards all headers sees the same
		// data as targeted Header(name) lookups.
		return fullDump()
	default:
		// No pre-packed blob available — pull the full header
		// dump via cgo and walk that. Rare path: only fires when
		// the dispatcher hasn't populated the scratch pointer
		// (test stubs, custom request construction).
		return fullDump()
	}
}

// Hostname returns the host portion of the Host header, with any ":port"
// suffix stripped. Returns "" if the request has no Host header.
func (r *Request) Hostname() string {
	host := r.Header("host")
	if host == "" {
		return ""
	}
	return hostnameFromHostHeader(host)
}

func hostnameFromHostHeader(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if host[0] == '[' {
		if end := strings.IndexByte(host, ']'); end > 0 {
			return host[1:end]
		}
		return host
	}
	if strings.Count(host, ":") == 1 {
		if i := strings.IndexByte(host, ':'); i >= 0 {
			return host[:i]
		}
	}
	return host
}

// Protocol returns "http" or "https". The framework itself only speaks
// plaintext — gogo is intended to run behind a TLS-terminating gateway
// such as nginx, an L7 load balancer, or a CDN. When Config.TrustProxy
// is enabled the X-Forwarded-Proto header from the gateway is honored
// so the application sees the original client protocol; otherwise the
// answer is always "http" so an untrusted client can't spoof its way
// to appearing as https.
func (r *Request) Protocol() string {
	if r.trustProxy {
		if v := r.Header("x-forwarded-proto"); v != "" {
			// First value of a comma-separated list per RFC 7239.
			if i := strings.IndexByte(v, ','); i >= 0 {
				v = v[:i]
			}
			v = strings.TrimSpace(v)
			if strings.EqualFold(v, "https") {
				return "https"
			}
		}
	}
	return "http"
}

// Secure reports whether the connection is encrypted (TLS / HTTPS).
// Honors X-Forwarded-Proto only when Config.TrustProxy is enabled.
func (r *Request) Secure() bool {
	return r.Protocol() == "https"
}

// IP returns the formatted peer IP for this connection. For routes
// behind a proxy use IPs() and pick from the X-Forwarded-For chain
// instead — this returns the immediate TCP peer, which will be the
// proxy itself.
//
// Sync handlers lazily cgo into uWS on first read and cache the result
// for subsequent reads. Async / shared handlers serve from the
// snapshot captured at request arrival.
func (r *Request) IP() string {
	if r.ipCached {
		return r.cachedIP
	}
	if r.snap != nil {
		r.cachedIP = normalizePeerIP(r.snap.ip)
	} else if r.syncResPtr != nil {
		r.cachedIP = normalizePeerIP(remoteAddrFromPtr(r.syncResPtr))
	}
	r.ipCached = true
	return r.cachedIP
}

func normalizePeerIP(ip string) string {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return ""
	}
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}
	if addr.Is4In6() {
		addr = addr.Unmap()
	}
	return addr.String()
}

// IPs parses the X-Forwarded-For header into a slice of normalized IPs in the
// order the proxies appended them (leftmost = original client). Returns nil if
// the header is absent, empty, or contains no valid IP entries. Empty and
// malformed entries are skipped; IPv4-mapped IPv6 addresses are unmapped.
//
// Returns nil when Config.TrustProxy is false — without that flag, the
// X-Forwarded-For header is attacker-controlled and any IP in it should
// be treated as untrusted input, not exposed via this helper. Callers
// that genuinely need the raw header value on an internet-facing server
// (rare, and almost always a logging mistake) can read it via
// req.Header("x-forwarded-for") and parse it themselves.
func (r *Request) IPs() []string {
	if !r.trustProxy {
		return nil
	}
	xff := r.Header("x-forwarded-for")
	if xff == "" {
		return nil
	}
	parts := strings.Split(xff, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		ip, ok := normalizeForwardedIP(p)
		if !ok {
			continue
		}
		out = append(out, ip)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeForwardedIP(raw string) (string, bool) {
	p := strings.TrimSpace(raw)
	if p == "" {
		return "", false
	}
	if host, _, err := net.SplitHostPort(p); err == nil {
		p = host
	} else if strings.HasPrefix(p, "[") && strings.HasSuffix(p, "]") {
		p = strings.TrimPrefix(strings.TrimSuffix(p, "]"), "[")
	} else if strings.IndexByte(p, '.') >= 0 && strings.Count(p, ":") == 1 {
		if i := strings.LastIndexByte(p, ':'); i >= 0 {
			p = p[:i]
		}
	}
	addr, err := netip.ParseAddr(p)
	if err != nil {
		return "", false
	}
	if addr.Is4In6() {
		addr = addr.Unmap()
	}
	return addr.String(), true
}

// Parameter returns a route parameter by index. Returns "" for negative or
// out-of-range indices. Snapshot mode caps at 8 parameters; sync mode
// caches indices 0..3 inline (the bridge pre-fills them at handler entry
// — no cgo on first read) and falls back to the cgo getParameter helper
// for indices 4+.
func (r *Request) Parameter(index int) string {
	if index < 0 {
		return ""
	}
	if r.snap != nil {
		if index >= len(r.snap.params) {
			return ""
		}
		return r.snap.params[index]
	}
	if index < 4 {
		if !r.paramCached[index] {
			r.cachedParams[index] = goStringFromC(r.syncParamPtrs[index], r.syncParamLens[index])
			r.paramCached[index] = true
		}
		return r.cachedParams[index]
	}
	return r.inner.parameter(index)
}

// Param looks up a route parameter by name. The name is the identifier
// written after ':' in the route pattern — e.g. for `/users/:id/posts/:postID`
// the names are "id" and "postID".
//
// Returns "" if name was never declared in the route pattern (typo,
// or registered via a code path that bypasses the framework's
// wrapper). Use Parameter(index) for positional access when you know
// the index ahead of time.
//
//	app.Get("/users/:id/posts/:postID", func(res *gogo.Response, req *gogo.Request) {
//	    id := req.Param("id")
//	    post := req.Param("postID")
//	    ...
//	})
func (r *Request) Param(name string) string {
	names := r.paramNames
	if r.snap != nil {
		names = r.snap.paramNames
	}
	for i, n := range names {
		if n == name {
			return r.Parameter(i)
		}
	}
	return ""
}

// ParamInt parses Param(name) as a signed decimal integer. Returns
// def when the param is missing or doesn't parse. Mirrors QueryInt.
func (r *Request) ParamInt(name string, def int) int {
	v := r.Param(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// ParamInt64 parses Param(name) as a signed decimal int64. Returns def when
// the param is missing or doesn't parse. Mirrors QueryInt64.
func (r *Request) ParamInt64(name string, def int64) int64 {
	v := r.Param(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

// Query returns the raw query string portion of the URL with the leading '?'
// stripped. Returns "" if the request has no query string.
//
// For parsed access, prefer QueryParam(key) for single keys or pass the
// result to net/url.ParseQuery for a full map.
//
// Sync handlers serve from the pre-cached query pointer (no cgo) on first
// call; subsequent calls hit the cache.
func (r *Request) Query() string {
	if r.snap != nil {
		return r.snap.query
	}
	if r.queryCached {
		return r.cachedQuery
	}
	if r.syncQueryPtr != nil {
		r.cachedQuery = goStringFromC(r.syncQueryPtr, r.syncQueryLen)
		r.queryCached = true
		return r.cachedQuery
	}
	s := r.inner.query()
	r.cachedQuery = s
	r.queryCached = true
	return s
}

// QueryParam returns the value of a single query parameter. Returns "" if
// the key is absent. Snapshot mode parses the raw query string on each call;
// cache the result if you need it more than once. Returns "" for an empty key.
func (r *Request) QueryParam(name string) string {
	if name == "" {
		return ""
	}
	if r.snap != nil {
		return parseSingleQueryParam(r.snap.query, name)
	}
	return r.inner.queryParam(name)
}

// Truncated reports whether an async request snapshot exceeded one of gogo's
// fixed capture buffers. The shared fast path rejects truncated requests before
// invoking handlers; this remains useful for diagnostics and future snapshot
// paths.
func (r *Request) Truncated() bool {
	return r.snap != nil && r.snap.truncated
}

// QueryInt parses the named query parameter as a base-10 int and
// returns it. Missing or non-numeric values fall back to def. Negative
// values are accepted.
func (r *Request) QueryInt(name string, def int) int {
	v := r.QueryParam(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// QueryInt64 parses the named query parameter as a base-10 int64.
// Missing or non-numeric values fall back to def.
func (r *Request) QueryInt64(name string, def int64) int64 {
	v := r.QueryParam(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

// QueryBool parses the named query parameter as a boolean. Accepts
// "1", "true", "t", "yes", "y", "on" as true and "0", "false", "f",
// "no", "n", "off" as false (all case-insensitive). Missing or
// unrecognized values fall back to def. Empty string ("?flag&") is
// treated as missing — pass def=true if you want the bare-flag idiom.
func (r *Request) QueryBool(name string, def bool) bool {
	v := r.QueryParam(name)
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	return def
}

// ParameterInt parses the route parameter at index as a base-10 int.
// Missing or non-numeric values fall back to def. Positional twin of
// ParamInt(name, def); use whichever matches your access style.
func (r *Request) ParameterInt(index int, def int) int {
	v := r.Parameter(index)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// ParameterInt64 parses the route parameter at index as a base-10 int64.
// Missing or non-numeric values fall back to def. Positional twin of
// ParamInt64(name, def); use whichever matches your access style.
func (r *Request) ParameterInt64(index int, def int64) int64 {
	v := r.Parameter(index)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

// lookupHeader scans the raw "name\0value\0..." buffer for a matching key.
// Header names are case-insensitive (uWS lower-cases on parse).
func (s *requestSnapshot) lookupHeader(name string) string {
	if len(s.headers) == 0 {
		return ""
	}
	// Match against the lower-cased name so callers don't have to.
	needle := strings.ToLower(name)
	buf := s.headers
	for len(buf) > 0 {
		i := indexOfZero(buf)
		if i < 0 {
			return ""
		}
		key := buf[:i]
		buf = buf[i+1:]
		j := indexOfZero(buf)
		if j < 0 {
			return ""
		}
		value := buf[:j]
		buf = buf[j+1:]
		if len(key) == len(needle) && bytesEqualLower(key, needle) {
			return string(value)
		}
	}
	return ""
}

func indexOfZero(b []byte) int {
	for i, c := range b {
		if c == 0 {
			return i
		}
	}
	return -1
}

func bytesEqualLower(b []byte, lower string) bool {
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != lower[i] {
			return false
		}
	}
	return true
}

// snapshotFromSync materializes a snapshot from a sync-mode Request so the
// data survives past the cgo callback's return. The middleware-fallback path
// in async route helpers calls this before spawning the async goroutine.
// capturePeerIP=false skips the cgo getRemoteAddressAsText lookup; the
// resulting snap.ip is empty and req.IP() in the async goroutine returns "".
func (r *Request) snapshotFromSync(capturePeerIP bool) *requestSnapshot {
	if r.snap != nil {
		return r.snap
	}
	snap := &requestSnapshot{
		method:  r.inner.method(),
		url:     r.inner.url(),
		query:   r.inner.query(),
		headers: r.inner.headersAll(),
	}
	if capturePeerIP {
		snap.ip = remoteAddrFromPtr(r.syncResPtr)
	}
	for i := 0; i < 8; i++ {
		p := r.inner.parameter(i)
		if p == "" {
			break
		}
		snap.params = append(snap.params, p)
	}
	// Propagate the route's param names to the snapshot so async
	// handlers (which see the snapshot, not the live request) can
	// resolve req.Param(name) without re-parsing the pattern. The
	// slice is owned by the route's closure — safe to share.
	if r.paramNames != nil {
		snap.paramNames = r.paramNames
	}
	return snap
}

// Cookie returns the value of a named cookie from the Cookie header, or ""
// if the cookie is absent. Linear scan; cache the value if you need it more
// than once. Cookie names are case-sensitive per RFC 6265.
func (r *Request) Cookie(name string) string {
	if name == "" {
		return ""
	}
	header := r.Header("cookie")
	if header == "" {
		return ""
	}
	return parseCookieValue(header, name)
}

// parseCookieValue scans a Cookie header for a name=value pair. Pairs are
// separated by ";" optionally followed by whitespace. Values surrounded by
// a matched pair of ASCII double quotes are unquoted per RFC 6265 §5.2;
// single quotes are not special.
func parseCookieValue(header, name string) string {
	for len(header) > 0 {
		// Skip leading whitespace.
		for len(header) > 0 && (header[0] == ' ' || header[0] == '\t') {
			header = header[1:]
		}
		semi := strings.IndexByte(header, ';')
		var pair string
		if semi < 0 {
			pair = header
			header = ""
		} else {
			pair = header[:semi]
			header = header[semi+1:]
		}
		eq := strings.IndexByte(pair, '=')
		if eq < 0 {
			continue
		}
		if pair[:eq] == name {
			v := pair[eq+1:]
			if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
				v = v[1 : len(v)-1]
			}
			return v
		}
	}
	return ""
}

// SameSite is the value of the SameSite cookie attribute. Empty means the
// attribute is not emitted (browser default applies).
type SameSite string

const (
	SameSiteStrict SameSite = "Strict"
	SameSiteLax    SameSite = "Lax"
	SameSiteNone   SameSite = "None"
)

// Cookie configures a Set-Cookie header. Zero-value fields are omitted —
// browsers fall back to their defaults (session cookie, no Path, etc.).
type Cookie struct {
	Name     string
	Value    string
	Path     string
	Domain   string
	MaxAge   int // seconds; <0 means delete, 0 means session, >0 explicit
	Expires  string
	Secure   bool
	HttpOnly bool
	SameSite SameSite
}

// SetCookie writes a Set-Cookie response header. Every field that
// reaches the wire is validated against the relevant grammar before
// serialization:
//
//   - Name: RFC 6265 cookie-name (token grammar, no CTL/separator)
//   - Value: RFC 6265 cookie-octet
//   - Path / Domain / Expires: reject CTLs and ";" so user-controlled
//     input cannot inject additional attributes by smuggling a
//     semicolon (e.g. Path="/; Domain=evil.com; Secure=false")
//   - SameSite: exact match against "", "Strict", "Lax", "None"
//
// Invalid characters panic — they typically indicate a programming bug
// or untrusted input reaching a Cookie field unprotected.
// SetCookieSigned routes through SetCookie so this validation covers
// signed cookies too.
//
// Multiple calls append multiple Set-Cookie headers; browsers handle them
// independently.
func (r *Response) SetCookie(c Cookie) {
	if c.Name == "" {
		panic("gogo: SetCookie requires a name")
	}
	validateCookieName(c.Name)
	validateCookieValue(c.Value)
	if c.Path != "" {
		validateCookiePath(c.Path)
	}
	if c.Domain != "" {
		validateCookieDomain(c.Domain)
	}
	if c.Expires != "" {
		validateCookieExpires(c.Expires)
	}
	validateCookieSameSite(c.SameSite)
	if c.SameSite == SameSiteNone && !c.Secure {
		// RFC 6265bis §5.5: cookies with SameSite=None MUST also be
		// Secure or browsers reject them entirely. Catching at the
		// framework boundary turns a silent "cookie never set in the
		// browser" footgun into an obvious programmer error.
		panic("gogo: SetCookie: SameSite=None requires Secure=true (RFC 6265bis §5.5)")
	}

	var b strings.Builder
	b.Grow(len(c.Name) + len(c.Value) + 64)
	b.WriteString(c.Name)
	b.WriteByte('=')
	b.WriteString(c.Value)
	if c.Path != "" {
		b.WriteString("; Path=")
		b.WriteString(c.Path)
	}
	if c.Domain != "" {
		b.WriteString("; Domain=")
		b.WriteString(c.Domain)
	}
	if c.MaxAge > 0 {
		b.WriteString("; Max-Age=")
		b.WriteString(strconv.Itoa(c.MaxAge))
	} else if c.MaxAge < 0 {
		// Negative Max-Age tells the browser to discard immediately.
		b.WriteString("; Max-Age=0")
	}
	if c.Expires != "" {
		b.WriteString("; Expires=")
		b.WriteString(c.Expires)
	}
	if c.Secure {
		b.WriteString("; Secure")
	}
	if c.HttpOnly {
		b.WriteString("; HttpOnly")
	}
	if c.SameSite != "" {
		b.WriteString("; SameSite=")
		b.WriteString(string(c.SameSite))
	}
	r.Header("Set-Cookie", b.String())
}

func validateCookieName(name string) {
	for i := 0; i < len(name); i++ {
		c := name[i]
		// RFC 6265 cookie-name = token; reject CTL, space, separators.
		if c < 0x20 || c == 0x7f {
			panic(fmt.Sprintf("gogo: cookie name %q contains a control character", name))
		}
		switch c {
		case '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', '{', '}', ' ', '\t':
			panic(fmt.Sprintf("gogo: cookie name %q contains an invalid character %q", name, c))
		}
	}
}

func validateCookieValue(value string) {
	// RFC 6265 cookie-octet excludes CTL, whitespace, '"', ',', ';', '\'.
	// We're strict because CR/LF here would enable response splitting via
	// the Set-Cookie header that Response.Header already guards.
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 0x20 || c == 0x7f || c == '"' || c == ',' || c == ';' || c == '\\' || c == ' ' {
			panic(fmt.Sprintf("gogo: cookie value contains invalid byte %q at offset %d", c, i))
		}
	}
}

// validateCookiePath rejects bytes that would break the Set-Cookie
// attribute framing. Per RFC 6265 §4.1.1, path-value is any character
// except CTLs and ";". An attacker who could land user input here
// without this check could inject extra attributes
// (Path="/; Domain=evil.com; Secure=false").
func validateCookiePath(path string) {
	for i := 0; i < len(path); i++ {
		c := path[i]
		if c < 0x20 || c == 0x7f || c == ';' {
			panic(fmt.Sprintf("gogo: cookie Path %q contains invalid byte 0x%02x at offset %d", path, c, i))
		}
	}
}

// validateCookieDomain rejects bytes that would break attribute framing
// or fragment the header line. The strict RFC 6265 §4.1.1 grammar
// requires a subdomain per RFC 1034/1123, but real-world cookies use
// a slightly wider character set (leading dot, internationalized
// domain labels post-encoding) — so we reject only the bytes that are
// definitely dangerous: CTLs, ";", "," , and whitespace.
func validateCookieDomain(domain string) {
	for i := 0; i < len(domain); i++ {
		c := domain[i]
		if c < 0x20 || c == 0x7f || c == ';' || c == ',' || c == ' ' || c == '\t' {
			panic(fmt.Sprintf("gogo: cookie Domain %q contains invalid byte 0x%02x at offset %d", domain, c, i))
		}
	}
}

// validateCookieExpires rejects CTLs and ";". The expected IMF-fixdate
// format (RFC 7231 §7.1.1) uses ASCII letters, digits, ":", " ", and
// "," (the comma after day-of-week), all of which we permit. ";" would
// inject another attribute; CTLs would split the header line.
func validateCookieExpires(expires string) {
	for i := 0; i < len(expires); i++ {
		c := expires[i]
		if c < 0x20 || c == 0x7f || c == ';' {
			panic(fmt.Sprintf("gogo: cookie Expires %q contains invalid byte 0x%02x at offset %d", expires, c, i))
		}
	}
}

// validateCookieSameSite ensures SameSite is one of the three defined
// values (or empty, which omits the attribute). The field's type is
// just a string alias, so a caller can cast arbitrary content into it
// (gogo.SameSite("Lax; Domain=evil")) — we have to gate that here.
func validateCookieSameSite(s SameSite) {
	switch s {
	case "", SameSiteStrict, SameSiteLax, SameSiteNone:
		return
	}
	panic(fmt.Sprintf("gogo: cookie SameSite %q must be one of \"\", \"Strict\", \"Lax\", \"None\"", s))
}

// parseSingleQueryParam mirrors uWS::HttpRequest::getQuery(key): linear scan
// of the form name=value&name=value, no allocations for the search; the value
// returned is a new string copied out of the query buffer.
func parseSingleQueryParam(q, name string) string {
	for len(q) > 0 {
		amp := strings.IndexByte(q, '&')
		var pair string
		if amp < 0 {
			pair = q
			q = ""
		} else {
			pair = q[:amp]
			q = q[amp+1:]
		}
		eq := strings.IndexByte(pair, '=')
		var k, v string
		if eq < 0 {
			k = pair
		} else {
			k = pair[:eq]
			v = pair[eq+1:]
		}
		if k == name {
			return v
		}
	}
	return ""
}

// WebSocket wraps a uWebSockets WebSocket connection.
type WebSocket struct {
	inner    websocketNative
	app      *App
	hubToken atomic.Uint64
}

// Send sends a WebSocket message.
func (ws *WebSocket) Send(message []byte, opcode OpCode) bool {
	return ws.inner.send(message, opcode)
}

// SendText sends a text WebSocket message.
func (ws *WebSocket) SendText(message string) bool {
	return ws.inner.sendString(message, Text)
}

// End closes the WebSocket connection.
func (ws *WebSocket) End(code int, message string) {
	ws.inner.end(code, message)
}

// Subscribe enrolls this WebSocket as a subscriber to topic. Future
// App.Publish / WebSocket.Publish calls targeting the same topic
// string deliver to this connection. Returns true when the
// subscription is now active (either added by this call or already
// in place).
//
// Topics are exact-match strings — uWS's TopicTree in v20 does not
// support MQTT-style "+" / "#" wildcards. Build your own fan-out
// scheme (e.g. subscribe to every relevant topic at connect time)
// if you need pattern matching.
//
// Must be called from inside an Open / Message / Close handler — the
// underlying TopicTree is loop-thread-local; calling Subscribe from
// a worker goroutine corrupts uWS state.
func (ws *WebSocket) Subscribe(topic string) bool {
	if !ws.inner.subscribe(topic) {
		return false
	}
	if h, ok := trackWSHubSubscribe(ws, topic); !ok {
		if !ws.inner.unsubscribe(topic) {
			if h != nil {
				h.reportAdapterError(fmt.Errorf("gogo: websocket hub subscribe rollback failed for topic %q", topic))
			}
		}
		return false
	}
	return true
}

// Unsubscribe removes this WebSocket's subscription to topic. Returns
// true when a subscription existed and was removed. Like Subscribe,
// must be called from a WebSocket handler.
func (ws *WebSocket) Unsubscribe(topic string) bool {
	key, h := hubForWebSocket(ws)
	token := ws.hubToken.Load()
	removed, last := false, false
	if h != nil {
		removed, last = h.removeMembershipForUnsubscribe(key, token, topic)
	}
	if !ws.inner.unsubscribe(topic) {
		if removed && h != nil {
			first, restored := h.restoreMembershipIfCurrent(key, token, topic)
			if !restored {
				h.reportAdapterError(fmt.Errorf("gogo: websocket hub unsubscribe restore failed for topic %q", topic))
			} else if first {
				h.queueAdapterTopic(topic)
			}
		}
		return false
	}
	if removed && last {
		h.queueAdapterTopic(topic)
	}
	return true
}

// Publish broadcasts message to every OTHER subscriber of topic, including
// subscribers on peer RunMultiCore loops.
// uWS deliberately excludes the publishing socket from its own
// broadcast — per WebSocket.h: "Publish as sender, does not receive
// its own messages even if subscribed to relevant topics" — so if
// you want every subscriber including the caller, use App.Publish
// instead.
//
// opcode picks the WebSocket frame type (Text or Binary). Returns
// true when the message was queued for delivery to at least one
// subscriber on the publishing socket's local loop. In RunMultiCore
// mode, peer-loop fanout is fire-and-forget and is not reflected in
// this return value.
//
// This is the fast path for the publishing socket's local loop: no cross-thread
// defer, no message copy, just a cgo crossing into uWS's TopicTree publish. In
// RunMultiCore, peer loops are reached by scheduling one copied publish per
// other App, so the peer fan-out cost is O(peer loops). App.Publish exists for
// the worker-goroutine case where you don't have a live WebSocket pointer on
// the loop thread.
//
// Calling this from off the loop thread (e.g. a goroutine you
// spawned from a handler) corrupts uWS state — the WebSocket
// pointer is only valid on the loop.
func (ws *WebSocket) Publish(topic string, message []byte, opcode OpCode) bool {
	ok := ws.inner.publish(topic, message, opcode)
	if ws.app != nil && len(ws.app.pubsubPeers) > 1 {
		ws.app.publishPeersExcept(ws.app, topic, message, opcode)
	}
	return ok
}

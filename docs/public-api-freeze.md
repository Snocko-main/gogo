# v0.9 Public API Freeze

This document freezes the exported API surface for the v0.9 release-candidate
lane. It covers the reusable public packages:

- `github.com/Snocko-main/gogo`
- `github.com/Snocko-main/gogo/middleware`
- `github.com/Snocko-main/gogo/adapters/redis`

Generated inventory was checked on 2026-06-10 from `origin/main` at
`4f0605a` using:

- `go list -f '{{.ImportPath}} {{.Name}} {{.Dir}}' ./...`
- `go doc -short` and `go doc -all` for the root, middleware, and Redis
  adapter packages
- `CGO_ENABLED=1 GOFLAGS='-tags=gogo' go list -json github.com/Snocko-main/gogo`
  for native build-tag file coverage
- `rg -n "^(func|type|const|var) [A-Z]" -g '*.go' -g '!**/*_test.go'`
  as a source backstop

## Findings

No v1 public API blockers remain in this audit. One blocker was found during
the initial inventory and resolved in this PR; the audited exported symbols
below are freeze-ready for v1 unless a later v0.9 blocker PR proves another
concrete compatibility or correctness problem.

Resolved blocker:

- B1: Bundled middleware factories return `internal/mwhint.Hinted`. `App.Use`,
  `App.Group`, `Router.Group`, and `Router.Use` now accept the same middleware
  registration values, so docs and examples can use scoped bundled middleware
  naturally (`api.Use(middleware.JWT(opts))`, `app.Group("/api",
  middleware.RequestID())`) without falling back to prefix-scoped `App.Use`.

Notes frozen by this audit:

- `NewApp(cfg ...Config)` remains the v1 constructor shape. Zero or one config
  is the supported contract; extra configs are rejected by the implementation.
- `App.Get(pattern, target any)`, `Router.Get(pattern, target any)`, and
  `App.Use(args ...any)` remain intentionally flexible APIs for v1. Typed route
  helpers are not required before v1.
- `UseAsync` and `PostAsyncHandler` remain compatibility aliases. New docs can
  prefer `Use` plus `middleware.Async` and `BodyAsyncHandler`, but keeping the
  aliases avoids unnecessary churn.
- Bundled middleware constructors return the internal marker type
  `mwhint.Hinted`. That return type is accepted by `App.Use`, `App.Group`,
  `Router.Group`, and `Router.Use`; users do not construct it directly.
- `SetWorkerCount` is a native-build-only exported tuning hook. It appears in
  source under `native_enabled.go` and in the global-state docs; callers that
  use it must build with `CGO_ENABLED=1 -tags gogo`. New multicore services
  should prefer per-run `RunOptions.Workers`. `WaitForSharedWorkers` remains
  available in stub builds as a no-op compatibility function.
- `TestServer.App()` is frozen as a runtime accessor for supported running-app
  operations such as WebSocket publish calls. Late route or middleware
  registration through the returned app is not part of the testing contract.

## Construction And Config

Status: stable. Names are clear, zero values are documented, and behavior is
covered by existing roadmap decisions and docs.

Root package symbols:

- `NewApp(cfg ...Config) (*App, error)`
- `Run(port int, setup func(*App)) (*MultiCoreHandle, error)`
- `RunWithOptions(port int, setup func(*App), opts RunOptions) (*MultiCoreHandle, error)`
- `RunOptions`
- `Config` with fields `BodyLimit`, `BodyReadTimeout`, `BindAddr`,
  `CapturePeerIP`, `TrustProxy`, `TrustedProxies`, `JSONEncoder`, `JSONDecoder`
- `NoBodyLimit`, `NoBodyReadTimeout`
- `JSONEncoder`, `JSONDecoder`
- `PanicHandler`, `SetPanicHandler`
- `SetWorkerCount` in native builds
- Global runtime-safe limit accessors:
  `SetDefaultMultipartPartLimit`, `GetDefaultMultipartPartLimit`,
  `SetMaxHTTPAdapterBodyBytes`, `GetMaxHTTPAdapterBodyBytes`,
  `SetMaxRenderBytes`, `GetMaxRenderBytes`, `SetStreamBackpressureBytes`,
  `GetStreamBackpressureBytes`, `SetMaxSendFileBytes`,
  `GetMaxSendFileBytes`, `SetSendFileChunkBytes`, `GetSendFileChunkBytes`,
  `SetSendFileBackpressureBytes`, `GetSendFileBackpressureBytes`
- Global limit variables and sentinels:
  `DefaultMultipartPartLimit`, `NoMultipartPartLimit`,
  `MaxHTTPAdapterBodyBytes`, `NoHTTPAdapterBodyLimit`, `MaxRenderBytes`,
  `NoRenderLimit`, `StreamBackpressureBytes`, `MaxSendFileBytes`,
  `NoSendFileLimit`, `SendFileChunkBytes`, `SendFileBackpressureBytes`

Freeze notes:

- Package-level mutable variables remain assignable for startup-time backward
  compatibility, but setter/getter pairs are the runtime-safe v1 surface.
- Process-wide panic handling stays outside `Config`; recovery sites include
  package-owned workers and callbacks.

## Routing

Status: stable. The public routing API keeps the existing compact method set,
route naming API, flexible `Get` targets, and group/mount scoping.

Root package types and functions:

- Handler types: `Handler`, `AsyncHandler`, `BodyAsyncHandler`,
  `PostAsyncHandler`, `Middleware`, `AsyncMiddleware`
- Static reply type: `Reply` with fields `Status`, `ContentType`, `Body`
- Route pattern extension: `RegisterParamType(name string, check func(string) bool)`
- `App` route methods: `Any`, `Get`, `GetAsync`, `Post`, `PostAsync`, `Put`,
  `PutAsync`, `Patch`, `PatchAsync`, `Delete`, `DeleteAsync`, `Options`,
  `Head`, `WebSocket`
- `App` route organization: `Use`, `UseAsync`, `Group`, `Mount`, `Name`,
  `URL`, `AllowedMethods`, `NotFound`, `MethodNotAllowed`
- `Router` route methods: `Any`, `Get`, `GetAsync`, `Post`, `PostAsync`,
  `Put`, `PutAsync`, `Patch`, `PatchAsync`, `Delete`, `DeleteAsync`,
  `Options`, `Head`, `WebSocket`
- `Router` organization: `Use`, `UseAsync`, `Group`, `Name`

Freeze notes:

- Route methods do not return fluent route handles for v1; explicit
  `Name(name, pattern)` remains the naming surface.
- `Group` and `Mount` remain the preferred typed scoping APIs. Prefix-scoped
  `Use` is retained and documented, but group identity gives clearer behavior.
- `Group` and `Router.Use` accept the same registration values as `App.Use`,
  including bundled middleware and `middleware.Async(...)` hints.

## Request And Response

Status: stable. Request snapshots, sync request accessors, body collection,
streaming, rendering, file-serving, cookies, and multipart helpers are all
documented as user-facing APIs.

Request symbols:

- `Request` methods: `URL`, `Method`, `Query`, `QueryParam`, `QueryInt`,
  `QueryInt64`, `QueryBool`, `Parameter`, `ParameterInt`, `ParameterInt64`,
  `Param`, `ParamInt`, `ParamInt64`, `Header`, `Get`, `Headers`, `Hostname`,
  `Protocol`, `Secure`, `IP`, `IPs`, `Cookie`, `CookieSigned`, `Context`,
  `SetLocal`, `Local`, `Body`, `BodyParser`, `Multipart`,
  `MultipartWithOptions`, `MultipartStream`, `Truncated`
- Body parsing: `ParseBody`, `ErrNoBody`, `ErrUnsupportedMediaType`,
  `ErrBodyTooLarge`, `ErrBodyTimeout`
- Cookies: `SameSite`, `SameSiteStrict`, `SameSiteLax`, `SameSiteNone`,
  `Cookie` fields `Name`, `Value`, `Path`, `Domain`, `MaxAge`, `Expires`,
  `Secure`, `HttpOnly`, `SameSite`, plus `SignCookieValue`,
  `VerifyCookieValue`
- Multipart: `MultipartOptions` field `MaxPartBytes`, `MultipartPart` fields
  `Name`, `FileName`, `ContentType`, `Data`, `Header`, `MultipartPart` methods
  `IsFile`, `SaveAt`, `SaveAtNew`, `SaveInto`, `MultipartStreamPart` fields
  `Name`, `FileName`, `ContentType`, `Header`, `Reader`,
  `MultipartStreamPart` methods `IsFile`, `SaveInto`, `ParseMultipart`,
  `ParseMultipartWithOptions`, `ParseMultipartStream`,
  `ErrMultipartPartTooLarge`

Response symbols:

- `Response` methods: `Status`, `StatusCode`, `Header`, `Append`, `SetCookie`,
  `SetCookieSigned`, `Write`, `End`, `Send`, `JSON`, `JSONBytes`, `JSONP`,
  `JSONStream`, `Redirect`, `Render`, `SendFile`, `Download`, `Stream`, `SSE`,
  `Body`, `OnData`, `OnAborted`, `OnFinish`, `Async`, `Cork`, `Loop`,
  `BufferedAmount`, `AwaitDrain`, `SetBodyEncoder`, `SetBodyEncoderLimit`
- Rendering: `TemplateEngine`, `LimitedTemplateEngine`,
  `NewHTMLTemplateEngine`, `HTMLTemplateOptions` fields `Root`, `Suffix`,
  `Reload`, `FuncMap`, `App.SetTemplateEngine`, `ErrRenderTooLarge`
- Streaming and SSE: `SSEEvent` fields `ID`, `Event`, `Data`, `Retry`,
  `SSEStream` methods `Send`, `SendEvent`, `Comment`, `Ping`,
  `ErrStreamAborted`
- File serving: `ErrFileTooLarge`

Freeze notes:

- `JSONStream` intentionally uses `encoding/json.Encoder`, not
  `Config.JSONEncoder`, because it is a streaming contract.
- `Response.Stream`, `SSE`, `SendFile`, and `Download` keep package-level
  safety limits and backpressure knobs rather than adding per-call option
  structs before v1.

## Middleware

Status: stable. Middleware factories, option structs, local keys, stores, and
metrics are ready to freeze. Constructor panics for invalid security-sensitive
configuration are part of the current contract.

Top-level middleware constructors and helpers:

- Placement helper: `Async`
- HTTP middleware: `BasicAuth`, `CORS`, `CSRF`, `Compress`, `Helmet`, `JWT`,
  `Logger`, `NewSession`, `RateLimit`, `RequestID`
- WebSocket helper: `WebSocketAuth`
- Metrics: `NewMetrics`
- Logging helpers: `DefaultFormat`, `JSONFormat`
- JWT helpers: `SignJWT`, `ParseRSAPublicKey`, `ParseECPublicKey`
- Request ID helper: `FastRequestIDGenerator`

Constants:

- Local keys: `BasicAuthLocalKey`, `CSRFLocalKey`, `JWTLocalKey`,
  `RequestIDLocalKey`, `SessionLocalKey`
- Limit sentinels: `NoBasicAuthCredentialLimit`, `NoCSRFTokenLimit`,
  `NoJWTTokenLimit`, `NoRateLimitBucketLimit`, `NoSessionEntryLimit`,
  `CompressNoMaxSize`
- JWT algorithms: `JWTHS256`, `JWTHS384`, `JWTHS512`, `JWTRS256`, `JWTRS384`,
  `JWTRS512`, `JWTPS256`, `JWTPS384`, `JWTPS512`, `JWTES256`, `JWTES384`,
  `JWTES512`

Option and data types:

- `BasicAuthOptions` fields `Users`, `Validator`, `Realm`, `LocalKey`,
  `SkipFunc`, `MaxCredentialBytes`
- `CORSOptions` fields `AllowOrigins`, `AllowMethods`, `AllowHeaders`,
  `ExposeHeaders`, `AllowCredentials`, `MaxAge`
- `CSRFOptions` fields `Secret`, `CookieName`, `HeaderName`, `CookiePath`,
  `CookieDomain`, `CookieSecure`, `CookieSameSite`, `CookieMaxAge`,
  `MaxTokenBytes`, `SkipFunc`, `LocalKey`
- `CompressOptions` fields `Level`, `MinSize`, `MaxSize`, `Filter`,
  `SkipFunc`
- `HelmetOptions` fields `HSTS`, `ContentSecurityPolicy`, `FrameOptions`,
  `ContentTypeOptions`, `ReferrerPolicy`, `XSSProtection`,
  `DNSPrefetchControl`, `DownloadOptions`, `PermittedCrossDomainPolicies`,
  `CrossOriginOpenerPolicy`, `CrossOriginResourcePolicy`
- `JWTAlgorithm`, `JWTOptions` fields `Secret`, `Key`, `Algorithm`, `Issuer`,
  `Audience`, `RequiredClaims`, `TokenFunc`, `LocalKey`, `SkipFunc`,
  `Optional`, `Leeway`, `MaxTokenBytes`
- `LoggerOptions` fields `Output`, `Format`, `SkipPaths`; `LogEntry` fields
  `Method`, `URL`, `Status`, `Duration`, `IP`, `UserAgent`
- `RequestIDOptions` fields `Header`, `MaxLength`, `Validator`, `Generator`
- `RateLimitOptions` fields `Max`, `Window`, `KeyFunc`, `SkipFunc`,
  `OnLimit`, `Store`, `MaxBuckets`, `AsyncStore`
- `RateLimitStore` interface method `Hit`
- `MemoryRateLimitStore` constructor `NewMemoryRateLimitStore` and methods
  `Hit`, `GC`
- `SessionOptions` fields `Secret`, `Store`, `CookieName`, `CookiePath`,
  `CookieDomain`, `CookieSecure`, `CookieSameSite`, `TTL`, `SkipFunc`,
  `LocalKey`, `MaxEntries`, `AsyncStore`
- `SessionStore` interface methods `Load`, `Save`, `Delete`
- `Session` field `ID` and methods `Get`, `Set`, `Delete`, `Save`, `Destroy`
- `MemorySessionStore` constructor `NewMemorySessionStore` and methods
  `Load`, `Save`, `Delete`, `GC`
- `MetricsOptions` fields `Buckets`, `Namespace`, `Subsystem`,
  `OnObservation`
- `Metrics` methods `Middleware`, `Handler`, `Snapshot`, `ObserveBytesIn`,
  `ObserveBytesOut`
- `Snapshot` fields `TotalRequests`, `InFlight`, `MeanLatency`, `Status`,
  `Method`, `BucketLE`, `BucketCounts`, `BytesIn`, `BytesOut`
- `WebSocketAuthOptions` fields `AllowedOrigins`, `AllowMissingOrigin`,
  `Verify`, `AllowedSubprotocols`

Freeze notes:

- Middleware constructors returning `mwhint.Hinted` remain registration values,
  not a public extension API. Custom middleware should use `gogo.Middleware`,
  `gogo.AsyncMiddleware`, or `middleware.Async`.
- `NewSession` keeps the `New` prefix to avoid colliding conceptually with the
  per-request `Session` handle.

## WebSocket

Status: stable. The public API keeps callbacks and topic publish methods narrow,
with thread-affinity documented on `WebSocket` methods and cross-goroutine
fan-out directed through `App` or `WSHub`.

Root WebSocket symbols:

- `OpCode`, `Text`, `Binary`
- `WebSocketBehavior` fields `Open`, `Message`, `Close`, `MaxPayloadLength`,
  `IdleTimeout`, `MaxBackpressure`, `DisablePings`, `UnsafeAutoUpgrade`,
  `Upgrade`
- `UpgradeContext` methods `Accept`, `Reject`, `Header`, `Method`, `URL`,
  `Query`, `QueryParam`, `Protocols`, `IP`, `SetUserData`
- `WebSocket` methods `Send`, `SendText`, `End`, `Publish`, `Subscribe`,
  `Unsubscribe`, `UserData`, `SetUserData`
- `App.Publish`, `App.PublishBatch`, `PublishMessage` fields `Topic`,
  `Message`, `OpCode`

Hub symbols:

- `WSHubMessage` fields `NodeID`, `Topic`, `Message`, `OpCode`
- `WSHubAdapter` methods `Start`, `Publish`, `Close`
- `WSHubTopicAdapter` methods `Subscribe`, `Unsubscribe` in addition to
  `WSHubAdapter`
- `WSHub`, `NewWSHub`, and methods `Attach`, `Start`, `Close`, `Publish`,
  `PublishBatch`, `PublishFrom`, `Subscribe`, `Unsubscribe`, `WebSocket`,
  `Wrap`
- `WSHubOption`, `WithWSHubNodeID`, `WithWSHubAdapter`,
  `WithWSHubAdapterQueueSize`, `WithWSHubAdapterWorkers`,
  `WithWSHubAdapterTopicWorkers`, `WithWSHubAdapterPublishTimeout`,
  `WithWSHubCloseTimeout`, `WithWSHubAdapterErrorHandler`
- Hub errors: `ErrWSHubClosed`, `ErrWSHubCloseTimeout`,
  `ErrWSHubAdapterQueueFull`, `ErrWSHubUntrackedSocket`,
  `ErrWSHubInvalidOpCode`, `ErrWSHubTrackFailed`

Freeze notes:

- Ping/pong callbacks are intentionally not exposed for v1.
- `UnsafeAutoUpgrade` keeps its explicit risk-bearing name and secure default.
- `WebSocket.Send`, `SendText`, `End`, `Subscribe`, and `Unsubscribe` remain
  loop-thread APIs; `App.Publish`, `App.PublishBatch`, and `WSHub` are the
  supported cross-goroutine surfaces.

## Testing And Migration Helpers

Status: stable. Testing helpers are public framework API. `HTTPAdapter` is both
a migration bridge and an examples-adjacent helper for stdlib integration.

Testing symbols:

- `TestServer`, constructors `NewTestServer`, `NewTestServerWithOptions`,
  `NewTestServerT`, `NewTestServerTWithOptions`
- `TestServerOptions` fields `Config`, `StartupTimeout`, `Client`
- `TestServer` methods `App`, `URL`, `Port`, `Client`, `Do`, `Get`, `Post`,
  `Close`

HTTP adapter symbols:

- `HTTPAdapter`, `HTTPAdapterWithBody`
- `MaxHTTPAdapterBodyBytes`, `NoHTTPAdapterBodyLimit`,
  `ErrHTTPAdapterBodyTooLarge`
- `SetMaxHTTPAdapterBodyBytes`, `GetMaxHTTPAdapterBodyBytes`

Freeze notes:

- `NewTestServerWithOptions` is the setup variant that returns setup errors.
- No public WebSocket test client is frozen for v1. Tests should use a real
  RFC 6455 client against `TestServer.URL()`.
- Example packages under `examples/...` are `package main`; exported `User`
  structs there are example-local names, not reusable module API.

## Lifecycle

Status: stable. Single-app lifecycle, graceful shutdown, multicore immediate
shutdown, loop defer, and abort observation are documented enough to freeze.

Root package symbols:

- `App.Listen`, `App.Run`, `App.Close`
- `App.Shutdown`, `App.ShutdownGracefully`, `App.ShutdownContext`
- `App.OnListen`, `App.OnShutdown`
- `RunMultiCore`, `RunMultiCoreWithOptions`, `RunMultiCoreOptions`,
  `MultiCoreMode`, `MultiCoreHandle` methods `Shutdown`, `Wait`
- `Loop` method `Defer`
- `Aborted` method `Load`
- `Response.Loop`, `Response.OnAborted`, `Response.OnFinish`, `Response.Async`,
  `Response.Cork`
- `WaitForSharedWorkers`

Freeze notes:

- `ShutdownContext` is the blocking graceful-shutdown API.
- `Run` is the production-oriented auto multicore entry point. `NewApp` plus
  `App.Run` remains the explicit single-loop shape.
- `RunMultiCore(n, port, setup)` keeps the low-level explicit loop-count shape
  with zero-value `Config`; `RunWithOptions` with `Cores` is the explicit
  loop-count shape that also applies one `Config` to every worker.
- Multicore still has no graceful-drain equivalent; this is documented as a
  lifecycle boundary, not an open API blocker.
- `Close` remains a native resource cleanup method to call after `Run` returns
  or before a never-listened app is discarded.

## Adapters

Status: stable. Redis adapter package exports two narrow integration surfaces:
`WSHub` transport and rate-limit storage.

`github.com/Snocko-main/gogo/adapters/redis` symbols:

- WebSocket hub adapter: `Options`, `Adapter`, constructors `New`,
  `NewClient`, `NewClientOptions`, and methods `Start`, `Publish`,
  `Subscribe`, `Unsubscribe`, `Close`
- `Options` fields `URL`, `Addr`, `Username`, `Password`, `DB`,
  `ChannelPrefix`, `ChannelSize`, `ChannelSendTimeout`, `MaxMessageSize`,
  `DynamicSubscriptions`
- Rate-limit adapter: `RateLimitOptions`, `RateLimitStore`, constructors
  `NewRateLimitStore`, `NewRateLimitStoreClient`,
  `NewRateLimitStoreClientOptions`, and methods `Hit`, `Close`
- `RateLimitOptions` fields `URL`, `Addr`, `Username`, `Password`, `DB`,
  `KeyPrefix`, `Timeout`, `FailClosed`, `OnError`

Freeze notes:

- Redis Pub/Sub delivery remains best-effort; stronger delivery semantics are
  not implied by the adapter API.
- `RateLimitStore.Hit` cannot return an error because it implements
  `middleware.RateLimitStore`; Redis errors are surfaced through `OnError` and
  the fail-open/fail-closed policy.

## Exclusions

The following were intentionally excluded from the public API freeze:

- `internal/...`, including `internal/mwhint`, except where an internal type
  appears as a bundled middleware return value.
- `benchmark/...` programs.
- `examples/...` `package main` symbols. Their exported names are local to the
  example binaries and are not importable library API.
- Test functions in `*_test.go`.

## Residual Risk

This PR adds targeted behavioral coverage for the resolved router/group
middleware-registration blocker. The broader compatibility conclusion still
relies on the existing API docs, route tests, global-state docs, middleware
tests, WebSocket hub tests, and testing-helper docs already present in the
repository.

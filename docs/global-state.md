# Global State Audit

This audit covers public process-wide state in the root
`github.com/Snocko-main/gogo` package for the v0.2.0 documentation lane. It
also notes public APIs that mutate unexported package-level state when that
state affects every `App` in the process.

The `middleware` and `adapters/redis` packages were checked for exported
package-level mutable state. Their public configuration is per middleware,
store, adapter, or hub instance; no additional package-level mutable knobs were
found there.

## Operating Rules

- Prefer per-`App` `Config`, per-route options, or per-instance options when
  they exist. Package-level knobs affect every app in the process.
- Set global knobs during process startup, before route registration or before
  requests are served, when possible.
- If a global limit may change while requests are running, use the documented
  setter/getter pair. The exported variables remain assignable for startup-time
  compatibility, but direct assignment during request handling can race readers.
- Tests that change global state should restore it with `t.Cleanup` and avoid
  parallel execution unless the test owns the whole process state for its
  duration.

## RunMultiCore Configuration Boundary

`RunMultiCore(n, port, setup)` currently creates each worker with `NewApp()`
and the zero-value `Config`. It has no `Config` or options parameter, so these
app-scoped fields cannot be supplied through the multicore helper today:

- `BodyLimit`
- `BodyReadTimeout`
- `BindAddr`
- `CapturePeerIP`
- `TrustProxy`
- `JSONEncoder`
- `JSONDecoder`

Configuration that is process-wide by design, such as `SetWorkerCount`,
`SetPanicHandler`, `RegisterParamType`, and the package-level limit setters
below, should be applied before `RunMultiCore`. Route, middleware, WebSocket,
hub, upload, and file-serving options should be registered inside `setup` so
every worker receives the same routes and options. Use single-loop
`NewApp(cfg)` + `Run` when an app-scoped `Config` field is required.

## Public Global Configuration

| API or state | Default | Scope | Mutation and timing |
| --- | --- | --- | --- |
| `SetPanicHandler(fn)` | Default handler prints the recovered value and stack to `stderr` | All framework recovery sites across HTTP handlers, async handlers, body callbacks, WebSocket callbacks, deferred callbacks, and the default WebSocket hub adapter error handler | Atomic pointer swap. Takes effect for future recovered panics immediately. Passing `nil` restores the default handler. Panics inside the handler are recovered silently. |
| `RegisterParamType(name, check)` | Built-ins: `int`, `uint`, `uuid`, `alpha`, `alnum`, `slug` | Typed route parameter registry for the whole process | Protected by a mutex. Route registration captures the check function, so changes affect routes registered after the call; existing routes keep their previously captured checker. Re-registering any name, including a built-in, overwrites that registry entry for future routes. |
| `SetWorkerCount(n)` | `0`, interpreted as `ceil(1.5 × worker-hint loops)` when workers start. A single `App` and default `RunMultiCore` reuseport mode use the one-loop hint; balanced mode uses the full loop count. | Native shared-dispatch worker pool used by shared async routes | Native builds only. Negative values are clamped to `0`. The value is read when a shared worker generation starts. Calls after workers are already running do not resize that generation; set it before the first shared async route is registered. |
| `WaitForSharedWorkers(timeout)` | Not a setting | Native shared-dispatch worker generations | Observes worker drain state. It does not configure global state, but it is the public observation hook for the process-wide shared worker pool. Stub builds always return `true`. |

`SetPanicHandler` is intentionally the supported panic-recovery configuration
model for v1. There is no per-`App` `Config` hook: some recovery sites are
owned by package-level workers or shared callback paths where no single `App`
is available, so a process-wide handler keeps reporting behavior consistent.

## Package-Level Limits

These exported variables remain assignable for compatibility, but the setter
and getter APIs are the runtime-safe surface.

| Limit | Default | Disable or reset value | Scope and guidance |
| --- | --- | --- | --- |
| `MaxRenderBytes` / `SetMaxRenderBytes` / `GetMaxRenderBytes` | 8 MiB | `NoRenderLimit` (`-1`) disables the render cap | Caps bytes staged by `Response.Render` across all apps. Prefer per-template or per-app policy in application code when different routes need different budgets. |
| `MaxHTTPAdapterBodyBytes` / `SetMaxHTTPAdapterBodyBytes` / `GetMaxHTTPAdapterBodyBytes` | 8 MiB | `NoHTTPAdapterBodyLimit` (`-1`) disables the adapter cap | Caps the response body staged by `HTTPAdapter` and `HTTPAdapterWithBody` before copying it into a gogo response. Large streaming handlers should use native gogo streaming APIs instead of the adapter. |
| `DefaultMultipartPartLimit` / `SetDefaultMultipartPartLimit` / `GetDefaultMultipartPartLimit` | 8 MiB | `NoMultipartPartLimit` (`-1`) disables the default per-part cap; legacy non-positive default values also disable it | Process default used when `MultipartOptions.MaxPartBytes` is zero. Prefer explicit `MultipartOptions` for route-specific upload policies. |
| `StreamBackpressureBytes` / `SetStreamBackpressureBytes` / `GetStreamBackpressureBytes` | 1 MiB | `0` disables automatic `Stream` / SSE backpressure checks | Applies to `Response.Stream` and SSE producers across all apps. File serving uses `SendFileBackpressureBytes` instead. |
| `MaxSendFileBytes` / `SetMaxSendFileBytes` / `GetMaxSendFileBytes` | 100 MiB | `NoSendFileLimit` (`-1`) disables the file-size cap | Caps the largest file `SendFile` and `Download` will serve. This is a misconfiguration guard, not the streaming memory budget. |
| `SendFileChunkBytes` / `SetSendFileChunkBytes` / `GetSendFileChunkBytes` | 64 KiB | Setter values at or below zero restore the default | Controls per-read buffer size for `SendFile` and `Download`. Do not mix direct assignment with the setter after startup; once the setter is used, the getter reads the atomic value. |
| `SendFileBackpressureBytes` / `SetSendFileBackpressureBytes` / `GetSendFileBackpressureBytes` | 1 MiB | Setter value `0` restores the default | Controls the send-buffer high-water mark used by file streaming. Unlike `StreamBackpressureBytes`, zero is not an opt-out through the setter. |

## Exported Error Sentinels

The root package exports several error sentinels as variables so callers can
compare or wrap them. They are technically assignable like any exported Go
variable, but they are not configuration state and should not be reassigned by
applications:

- `ErrNoBody`
- `ErrUnsupportedMediaType`
- `ErrMultipartPartTooLarge`
- `ErrRenderTooLarge`
- `ErrHTTPAdapterBodyTooLarge`
- `ErrFileTooLarge`
- `ErrStreamAborted`
- `ErrBodyTooLarge`
- `ErrBodyTimeout`
- `ErrWSHubClosed`
- `ErrWSHubCloseTimeout`
- `ErrWSHubAdapterQueueFull`
- `ErrWSHubUntrackedSocket`
- `ErrWSHubInvalidOpCode`
- `ErrWSHubTrackFailed`

## Internal Package State With Public Entry Points

- Shared async route registration appends native shared-dispatch handlers to an
  internal process-wide registry. Handler slots are append-only for the process
  lifetime. The public knobs for this area are `SetWorkerCount` and
  `WaitForSharedWorkers`; route handlers themselves remain app-owned.
- `NewWSHub`, `WSHub.Attach`, `WSHub.WebSocket`, and `WSHub.Wrap` mutate
  process-local WebSocket hub tracking so publishes can fan out across apps in
  one process. The configurable parts are per-hub options, not package-level
  globals.
- Pools, status-line caches, compiled regular expressions, and native layout
  snapshots are internal implementation state. They have no public mutation API
  and are not documented as user-facing configuration.

## Blockers

None found. Every public global or global-affecting API above can be documented
from the current code without an owner decision.

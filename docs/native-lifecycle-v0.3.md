# Native Lifecycle Status for v0.3

This note records the native lifecycle decisions that were reviewed during the
v0.3 config pass. It separates decisions that are already implemented from the
remaining v1 native-boundary follow-ups.

## Closed Decisions

### Shared-dispatch quiescence

Shared-dispatch state now outlives `App.Close` safely. The native bridge keeps
per-app shared state behind a ref-counted owner, marks that owner closing before
free, waits for active shared-ring producers, discards queued response-ring
entries, and avoids recycling contexts into a pool after close begins.

This covers the request/response paths that can otherwise outlive the Go app:

- inline shared responses
- fallback defer-send responses
- redirects
- stream start/write/end ownership
- `PostAsync` body collection when a client aborts before the final body chunk
- wake-drain and drain-timer callbacks that need the app response ring

The public close contract is still the same: call `Shutdown`,
`ShutdownGracefully`, or `ShutdownContext` to stop the loop, wait for `Run` to
return when needed, then call `Close` to free native resources. `Close` is not a
general cancellation primitive for arbitrary user goroutines; handlers that
need to stop early should observe `Response.OnAborted` or `Request.Context`.

### Shared handler registry lifetime

Shared async route registration appends handlers to a process-wide registry and
publishes immutable snapshots for worker reads. The registry is intentionally
process-lifetime state for v1. This matches the current route model: routes are
registered at app setup time, and there is no public hot-unregister API.

The public operational knobs remain:

- `SetWorkerCount`, configured before shared workers start
- `WaitForSharedWorkers`, for tests and supervisors that need to observe worker
  generation drain

## Remaining Follow-Ups

### Single-app loop ownership

`uwsgo_app_new` captures the uWS loop for the OS thread that creates the app.
`RunMultiCore` and `NewTestServer` already honor that by creating, registering,
listening, running, and closing each app on a locked OS thread. The public
single-app path still needs a v1 owner decision:

- document and enforce same-thread `NewApp` / route registration / `Listen` /
  `Run` / `Close`, or
- move single-app ownership behind an internal locked loop goroutine.

Cross-thread methods should remain limited to the explicitly safe APIs that
defer to the loop or use native synchronization, such as `Shutdown`,
`ShutdownGracefully`, `ShutdownContext`, `Publish`, `PublishBatch`, and
deferred async responses.

### Native boundary audit

The v1 security checklist still tracks broader native-boundary work:

- string, length, and buffer conversion audit
- snapshot cap documentation for method, URL, query, params, headers, peer IP,
  and body
- `cgo.Handle` release coverage
- native fuzz/stress coverage for oversized inputs, aborted streams, malformed
  WebSocket handshakes, and shutdown races
- vendored native dependency and build-tool documentation

Those items are larger than the v0.3 lifecycle decision and should remain
tracked separately.

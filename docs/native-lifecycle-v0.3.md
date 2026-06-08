# Native Lifecycle Status for v0.3

This note records the native lifecycle decisions that were reviewed during the
v0.3 config pass. It separates behavior that is already implemented, accepted
v1 direction, and the remaining native-boundary follow-ups.

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

## Accepted v1 Direction

### Shared handler registry cleanup

Shared async route registration currently appends handlers to a process-wide
registry and publishes immutable snapshots for worker reads. That is safe for a
single boot-defined app, but it must not become the v1 lifecycle contract: a
process that repeatedly creates and closes apps would retain every shared
handler closure until process exit.

The v1 direction is to release an app's shared handlers after graceful shutdown
has drained that app's shared work. Cleanup must run after the app can no longer
produce or consume shared callbacks, not at the beginning of
`ShutdownGracefully`, because in-flight handlers and queued shared responses may
still need the registered handler IDs while the drain is in progress.

The likely implementation shape is tombstones or generations rather than
shrinking the registry. Old numeric handler IDs must remain safe to observe
after cleanup, while the handler closures themselves should be released so large
captured objects can be garbage collected.

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

### Shared handler registry cleanup

Implement cleanup for shared handler slots after graceful/shared drain. Tests
should cover repeated `NewApp` / route registration / `ShutdownGracefully` /
`Close` cycles and verify that stale handler IDs cannot call a removed handler
or a newly registered handler from another app generation.

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

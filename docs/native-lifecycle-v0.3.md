# Native Lifecycle Status for v0.3

This note records the native lifecycle decisions that were reviewed during the
v0.3 config pass. It separates behavior that is already implemented from the
remaining native-boundary follow-ups.

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

### Shared handler registry cleanup

Shared async route registration appends handlers to a process-wide registry and
publishes immutable snapshots for worker reads, but the registry is no longer
process-lifetime retention for handler closures. `App.Close` tombstones the
closing App's shared handler slots after native close has started, then
publishes a new snapshot so the closures and any large captured objects can be
garbage collected.

Handler IDs are not reused. Old numeric IDs remain safe to observe after
cleanup: a worker that sees a tombstoned or out-of-range handler ID releases the
request context instead of calling a removed handler or accidentally dispatching
to a newly registered handler from another App generation.

### Single-app loop ownership

Native builds now create each App's uWS object on an internal locked owner
goroutine. Route registration, static route registration, WebSocket
registration, `Listen`, `Run`, shared-drain timer setup, and `Close` are
serialized through that owner so single-app users do not need to call
`runtime.LockOSThread` themselves.

Cross-thread methods should remain limited to the explicitly safe APIs that
defer to the loop or use native synchronization, such as `Shutdown`,
`ShutdownGracefully`, `ShutdownContext`, `Publish`, `PublishBatch`, and
deferred async responses.

`RunMultiCore` and `NewTestServer` keep their public setup shapes, but they no
longer rely on their caller goroutine being the uWS owner thread. Routes should
still be registered before `Listen` / `Run`; hot route registration while the
loop is serving traffic remains outside the public contract.

## Remaining Follow-Ups

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

# Native Lifecycle Findings for v0.3

This report maps the current native lifecycle contracts for the v0.3.0 roadmap
items around uWS loop ownership, shared-dispatch quiescence, and shared handler
registry lifetime.

## Findings

1. **Shared-dispatch app memory can be freed before shared workers are
   quiescent.**

   `App.Close` frees native app memory through `a.inner.close()` before it
   drops the app's shared-worker reference and signals workers to exit
   (`types.go:2439`, `native_enabled.go:217`). Shared workers are intentionally
   fire-and-forget and may still be running user handlers (`native_enabled.go:221`).
   Those handlers still hold an `AsyncCtx` that can reference the per-app
   pending response ring, ctx pool, uWS response pointer, and loop pointer
   (`uws_bridge.cpp:940`, `uws_bridge.cpp:1332`). `uwsgo_app_free` then deletes
   the ctx pool and pending ring immediately (`uws_bridge.cpp:1043`). A handler
   that ignores cancellation and later calls `res.Send` can therefore route
   through `asyncSendShared` and touch freed native memory
   (`native_enabled.go:1016`).

   This is the release-blocking lifecycle decision. A small reorder is not
   enough unless `Close` is allowed to block until all shared handlers return.

2. **Single-app loop ownership is same-thread by implementation but not fully
   documented or enforced for the public `NewApp` path.**

   `uwsgo_app_new` captures `uWS::Loop::get()` when the app is created, so the
   app is bound to the OS thread that called `NewApp` (`uws_bridge.cpp:276`).
   `RunMultiCore` and `NewTestServer` honor that by locking an OS thread before
   creating, registering, listening, and running the app (`types.go:2535`,
   `testing.go:93`). The single-app public docs show `NewApp`, route
   registration, `Listen`, and `Run` in one goroutine, but they do not explicitly
   say that route registration / `Listen` / `Run` must stay on the creating
   OS thread. `Shutdown`, `ShutdownGracefully`, `App.Publish`, and
   `App.PublishBatch` already use cross-thread defers or native mutexes and are
   documented as safe from any goroutine.

3. **Shared handler registry lifetime is already a documented process-lifetime
   retention decision.**

   Shared async route registration appends handlers to a process-wide registry
   and publishes atomic snapshots for lock-free worker reads
   (`native_enabled.go:64`). The v0.2 global-state audit documents those slots
   as append-only for the process lifetime, with `SetWorkerCount` and
   `WaitForSharedWorkers` as the public knobs (`docs/global-state.md:75`).
   No tombstone or generation cleanup is currently needed for v0.3 unless the
   project wants bounded handler retention for hosts that create many app
   instances dynamically.

## Recommended Decisions

- **Loop ownership:** document and eventually enforce the current same-thread
  single-app contract. Applications should create the app, register routes,
  call `Listen`, call `Run`, and then call `Close` from the same locked OS
  thread, or use `RunMultiCore` / `NewTestServer`, which do that internally.
  Cross-thread operations should remain limited to documented safe methods:
  `Shutdown`, `ShutdownGracefully`, `Publish`, `PublishBatch`, and loop-deferred
  response helpers.

- **Shared handler registry:** keep documented process-lifetime retention for
  v1 unless a real dynamic-app hosting use case appears. The current append-only
  registry is simple and matches the no-hot-unregister route model.

- **Shared-dispatch quiescence:** choose one explicit close contract before
  changing runtime behavior:
  - Blocking close: `Close` stops the last shared worker generation and waits
    for all active shared handlers to return before freeing native per-app
    memory. This is simple and safe, but can hang if a handler ignores
    cancellation.
  - Non-blocking close with deferred native free: native per-app shared state
    gets a refcount/quiescer so `Close` can return while memory is freed only
    after all `AsyncCtx` users release. This preserves fast close but is a
    larger C++ ownership change.
  - Documented process-lifetime native retention: shared apps never free the
    pending ring / ctx pool. This avoids use-after-free but leaks per-app native
    memory and should be chosen only if dynamic app teardown is out of scope.

## Exact Blockers

- Should `App.Close` be allowed to block indefinitely waiting for shared async
  handlers that ignored `req.Context()` cancellation?
- If not, should v0.3 add a deferred-free/refcount design for per-app
  `PendingRing` and `CtxPool`, or intentionally retain that memory for process
  lifetime?
- Should the public API grow a bounded close/shutdown API for this, or should
  the existing `ShutdownContext(ctx)` roadmap item own the timeout contract?

## Validation Notes

No runtime or native build behavior was changed by this report.

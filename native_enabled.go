//go:build cgo && gogo

package gogo

/*
#cgo CFLAGS: -std=c11 -DLIBUS_NO_SSL -I${SRCDIR}/internal/native/uwebsockets/uSockets/src
#cgo linux CFLAGS: -D_GNU_SOURCE
#cgo CXXFLAGS: -std=c++20 -DLIBUS_NO_SSL -I${SRCDIR}/internal/native/uwebsockets/src -I${SRCDIR}/internal/native/uwebsockets/uSockets/src
#cgo LDFLAGS: -lz
#cgo linux LDFLAGS: -pthread
#include <stdlib.h>
#include <stdint.h>
#if defined(__APPLE__)
#include <pthread.h>
#elif defined(__linux__)
#include <sys/syscall.h>
#include <unistd.h>
#else
#include <pthread.h>
#endif
#include "uws_bridge.h"

static uint64_t uwsgo_current_thread_id(void) {
#if defined(__APPLE__)
    uint64_t tid = 0;
    pthread_threadid_np(NULL, &tid);
    return tid;
#elif defined(__linux__)
    return (uint64_t) syscall(SYS_gettid);
#else
    return (uint64_t) (uintptr_t) pthread_self();
#endif
}
*/
import "C"

import (
	"fmt"
	"runtime"
	"runtime/cgo"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// maxInt32 caps C.size_t → C.int conversions on the cgo body callback
// path so a malicious or malfunctioning peer cannot drive a silent
// truncation of a chunk write. The cap is symbolic — uWS's per-
// connection backpressure (default 64 KiB) and kernel socket buffer
// keep real chunks several orders of magnitude smaller.
const (
	maxInt32  = 1<<31 - 1
	minInt32  = -1 << 31
	maxUint16 = 1<<16 - 1
	maxUint32 = 1<<32 - 1
)

var maxGoInt = int(^uint(0) >> 1)

type appNative struct {
	ptr              *C.uwsgo_app_t
	handles          []cgo.Handle
	owner            *nativeOwner
	sharedHandlerIDs []uint32
}

type responseNative struct {
	ptr *C.uwsgo_res_t
}

type requestNative struct {
	ptr *C.uwsgo_req_t
}

type websocketNative struct {
	ptr *C.uwsgo_ws_t
}

type loopNative struct {
	ptr *C.uwsgo_loop_t
}

type nativeCall struct {
	fn   func()
	done chan struct{}
}

type nativeOwner struct {
	calls    chan nativeCall
	threadID atomic.Uint64
	closed   atomic.Bool
}

func newNativeOwner() *nativeOwner {
	owner := &nativeOwner{calls: make(chan nativeCall)}
	ready := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		owner.threadID.Store(currentNativeThreadID())
		close(ready)
		for call := range owner.calls {
			call.fn()
			close(call.done)
		}
		owner.threadID.Store(0)
	}()
	<-ready
	return owner
}

func currentNativeThreadID() uint64 {
	return uint64(C.uwsgo_current_thread_id())
}

func clampNonNegativeInt(n int) int {
	if n < 0 {
		return 0
	}
	return n
}

func cIntFromInt(n int) C.int {
	if n > maxInt32 {
		return C.int(maxInt32)
	}
	if n < minInt32 {
		return C.int(minInt32)
	}
	return C.int(n)
}

func cIntFromNonNegative(n int) C.int {
	if n <= 0 {
		return 0
	}
	if n > maxInt32 {
		return C.int(maxInt32)
	}
	return C.int(n)
}

func cUint32SizeFromPositive(n, fallback int) C.size_t {
	if n <= 0 {
		n = fallback
	}
	if uint64(n) > maxUint32 {
		return C.size_t(maxUint32)
	}
	return C.size_t(n)
}

func cUint16IntFromDurationSeconds(d time.Duration, fallback int64) C.int {
	seconds := int64(d / time.Second)
	if seconds <= 0 {
		seconds = fallback
	}
	if seconds > maxUint16 {
		return C.int(maxUint16)
	}
	return C.int(seconds)
}

func goIntFromCSize(size C.size_t, label string) (int, bool) {
	if size > C.size_t(maxGoInt) {
		reportPanic(fmt.Errorf("gogo: %s length %d exceeds Go int max", label, uint64(size)))
		return 0, false
	}
	return int(size), true
}

func goIntFromCSizeHot(size C.size_t) int {
	if strconv.IntSize == 32 && size > C.size_t(maxGoInt) {
		reportPanic(fmt.Errorf("gogo: HTTP callback length %d exceeds Go int max", uint64(size)))
		return 0
	}
	return int(size)
}

func cgoCopyLen(size C.size_t, label string) (C.int, bool) {
	if size > C.size_t(maxInt32) {
		reportPanic(fmt.Errorf("gogo: %s length %d exceeds C.int max", label, uint64(size)))
		return 0, false
	}
	return C.int(size), true
}

func boundedUint32Len(n uint32, cap uintptr) int {
	limit := uintptr(n)
	if limit > cap {
		limit = cap
	}
	if limit > uintptr(maxGoInt) {
		limit = uintptr(maxGoInt)
	}
	return int(limit)
}

func (o *nativeOwner) onOwnerThread() bool {
	if o == nil {
		return true
	}
	id := o.threadID.Load()
	return id != 0 && id == currentNativeThreadID()
}

func (o *nativeOwner) call(fn func()) {
	if o == nil || o.onOwnerThread() {
		fn()
		return
	}
	if o.closed.Load() {
		panic("gogo: native app owner is closed")
	}
	done := make(chan struct{})
	o.calls <- nativeCall{fn: fn, done: done}
	<-done
}

func (o *nativeOwner) close() {
	if o == nil || o.closed.Swap(true) {
		return
	}
	close(o.calls)
}

func newAppNative() (appNative, error) {
	owner := newNativeOwner()
	var ptr *C.uwsgo_app_t
	owner.call(func() {
		ptr = C.uwsgo_app_new()
	})
	return appNative{ptr: ptr, owner: owner}, nil
}

func (a *appNative) onOwner(fn func()) {
	if a.owner == nil {
		fn()
		return
	}
	a.owner.call(fn)
}

func (a *appNative) onOwnerBool(fn func() bool) bool {
	var ok bool
	a.onOwner(func() {
		ok = fn()
	})
	return ok
}

func (a *appNative) get(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))

	a.onOwner(func() {
		C.uwsgo_app_get(a.ptr, cpattern, C.uintptr_t(handle))
	})
}

// Shared-dispatch handler registry. C++ pushes the handler_id (a small int)
// into AsyncCtx; Go workers look it up here. Slot indexes are never reused so
// stale ctxs cannot call a new handler by accident; App.Close tombstones that
// App's slots so captured handler closures can be garbage collected.
var (
	// sharedHandlers is append-only after registration. Registration takes
	// the mutex; workers read via an atomic snapshot to keep the request
	// hot path lock-free.
	sharedHandlers     []AsyncHandler
	sharedHandlersMu   sync.Mutex
	sharedHandlersSnap atomic.Pointer[[]AsyncHandler]
	sharedActive       atomic.Bool // true once a Shared route has been registered

	// sharedWorkerLifecycleMu guards the start / stop transition of the
	// worker pool. Read-mostly: NewApp / App.Close transitions are rare
	// compared to ensureSharedWorkers fast-path checks of the started
	// flag (which uses atomic load + acquire-on-mu only on the first
	// shared registration of an app).
	sharedWorkerLifecycleMu sync.Mutex
	sharedWorkersStarted    bool
	sharedWorkerGen         *sharedWorkerGeneration
	sharedWorkerGens        []*sharedWorkerGeneration
	// sharedActiveApps counts Apps that registered at least one route
	// on the shared-dispatch fast path. Plain sync apps do not hold a
	// worker-pool reference; otherwise a long-lived sync-only App would
	// keep workers spinning after the last shared App closed.
	sharedActiveApps atomic.Int32
)

type sharedWorkerGeneration struct {
	stop          chan struct{}
	drained       chan struct{}
	coreHintToken uint64
	live          atomic.Int32
}

func init() {
	// Initialize the atomic snapshot with an empty slice so workers can
	// Load() unconditionally without nil checks.
	empty := []AsyncHandler{}
	sharedHandlersSnap.Store(&empty)
}

func registerSharedHandler(h AsyncHandler) uint32 {
	sharedHandlersMu.Lock()
	defer sharedHandlersMu.Unlock()
	sharedHandlers = append(sharedHandlers, h)
	publishSharedHandlersLocked()
	return uint32(len(sharedHandlers) - 1)
}

func publishSharedHandlersLocked() {
	// Publish a fresh snapshot so workers see handler changes via an atomic
	// pointer swap — no lock acquired on the request hot path.
	snap := append([]AsyncHandler(nil), sharedHandlers...)
	sharedHandlersSnap.Store(&snap)
}

func lookupSharedHandler(id uint32) (AsyncHandler, bool) {
	snap := sharedHandlersSnap.Load()
	if snap == nil || int(id) >= len(*snap) {
		return nil, false
	}
	handler := (*snap)[id]
	if handler == nil {
		return nil, false
	}
	return handler, true
}

func cleanupSharedHandlers(ids []uint32) {
	if len(ids) == 0 {
		return
	}
	sharedHandlersMu.Lock()
	defer sharedHandlersMu.Unlock()
	changed := false
	for _, id := range ids {
		if int(id) >= len(sharedHandlers) || sharedHandlers[id] == nil {
			continue
		}
		sharedHandlers[id] = nil
		changed = true
	}
	if changed {
		publishSharedHandlersLocked()
	}
}

func (a *appNative) registerSharedHandler(h AsyncHandler) uint32 {
	id := registerSharedHandler(h)
	a.sharedHandlerIDs = append(a.sharedHandlerIDs, id)
	return id
}

func (a *appNative) getShared(pattern string, handler AsyncHandler) {
	id := a.registerSharedHandler(handler)
	cpattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cpattern))
	a.onOwner(func() {
		C.uwsgo_app_get_shared(a.ptr, cpattern, C.uint32_t(id))
	})
	sharedActive.Store(true)
	// Lazily start worker goroutines on first shared route registration.
	ensureSharedWorkers()
}

// postShared registers a POST route on the zero-cgo shared
// dispatch path. The C++ lambda collects the request body into
// per-ctx memory (capped at the smaller of maxBody and
// SNAP_BODY_CAP) and pushes onto the request ring once the body
// is complete; the Go worker then runs handler with req.body
// already populated from the ctx snapshot.
//
// maxBody == 0 means "use the C-side default" (SNAP_BODY_CAP).
// Bodies larger than the active cap short-circuit with 413 on
// the loop thread — the goroutine is never spawned.
func (a *appNative) postShared(pattern string, handler AsyncHandler, maxBody int) {
	id := a.registerSharedHandler(handler)
	cpattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cpattern))
	maxBody = clampNonNegativeInt(maxBody)
	a.onOwner(func() {
		C.uwsgo_app_post_shared(a.ptr, cpattern, C.uint32_t(id), C.size_t(maxBody))
	})
	sharedActive.Store(true)
	ensureSharedWorkers()
}

// workerCount controls how many goroutines drain the request ring. Read once
// when the first shared route registers (and workers spin up); changing it
// after that has no effect. Default = ceil(1.5 × loop count) — see
// defaultWorkerCount for why this scales with loops (cores) rather than
// NumCPU. For workloads dominated by slow IO (many handlers blocked on a
// DB / network at once), raise this via SetWorkerCount before the first
// GetAsync registers.
var workerCount atomic.Int32

// sharedCoreHint records how many uWS loops (cores) will share the worker
// pool, so the default worker count can scale with loops instead of NumCPU.
// RunMultiCore publishes its loop count here before any route registers;
// the plain single-App path leaves it 0, which defaultWorkerCount treats as
// one loop. User-supplied SetWorkerCount overrides the hint entirely.
//
// The high 32 bits are a generation counter and the low 32 bits are the loop
// count. Reset paths compare the full token so an older RunMultiCore lifecycle
// cannot clear a newer hint that happens to use the same loop count.
var sharedCoreHint atomic.Uint64

// setSharedCoreHint is called by RunMultiCore before setup() runs (and thus
// before the first GetAsync triggers ensureSharedWorkers) so the worker
// default reflects the real loop count.
func setSharedCoreHint(loops int) uint64 {
	if loops < 1 {
		loops = 1
	}
	for {
		old := sharedCoreHint.Load()
		gen := old>>32 + 1
		if gen == 0 {
			gen = 1
		}
		next := gen<<32 | uint64(uint32(loops))
		if sharedCoreHint.CompareAndSwap(old, next) {
			return next
		}
	}
}

// resetSharedCoreHintIfCurrent clears a RunMultiCore-published hint only if
// another RunMultiCore call has not replaced it.
// This covers sync-only / setup-failure RunMultiCore lifecycles that never
// acquire a shared worker ref, while stopSharedWorkersIfIdle covers the
// shared-route path when the worker pool itself goes idle.
func resetSharedCoreHintIfCurrent(token uint64) {
	if token != 0 {
		sharedCoreHint.CompareAndSwap(token, 0)
	}
}

// defaultWorkerCount returns ceil(1.5 × loops). The pool exists to run
// blocking handler work OFF the loop threads; sizing it to NumCPU
// over-subscribes the common low-loop case (e.g. one loop on a many-core
// box) where extra worker goroutines just contend with the loop thread for
// GOMAXPROCS and thrash the ring head — measured to drop a single-loop
// async route to ~80% of the sync path. 1.5× loops gives each loop one
// worker to absorb its steady stream plus a half-worker of slack for the
// occasional concurrently-blocked handler, which matched the empirical
// tuning sweet spot. IO-bound services that keep many handlers blocked at
// once should still raise this with SetWorkerCount.
func defaultWorkerCount() int {
	loops := int(uint32(sharedCoreHint.Load()))
	if loops < 1 {
		loops = 1
	}
	return (loops*3 + 1) / 2
}

// SetWorkerCount configures the shared-dispatch worker pool size. Call
// before registering any GetAsync route — calls after the pool starts
// are no-ops. Pass 0 to restore the default (ceil(1.5 × loop count)).
func SetWorkerCount(n int) {
	if n < 0 {
		n = 0
	}
	workerCount.Store(int32(n))
}

func ensureSharedWorkers() {
	sharedWorkerLifecycleMu.Lock()
	defer sharedWorkerLifecycleMu.Unlock()
	if sharedWorkersStarted {
		return
	}
	n := int(workerCount.Load())
	if n == 0 {
		n = defaultWorkerCount()
	}
	gen := &sharedWorkerGeneration{
		stop:          make(chan struct{}),
		drained:       make(chan struct{}),
		coreHintToken: sharedCoreHint.Load(),
	}
	gen.live.Store(int32(n))
	sharedWorkerGen = gen
	sharedWorkerGens = append(sharedWorkerGens, gen)
	sharedWorkersStarted = true
	for i := 0; i < n; i++ {
		go func() {
			defer sharedWorkerDone(gen)
			sharedWorker(gen.stop)
		}()
	}
}

func acquireSharedWorkerAppRef() {
	sharedWorkerLifecycleMu.Lock()
	sharedActiveApps.Add(1)
	sharedWorkerLifecycleMu.Unlock()
}

func sharedWorkerDone(gen *sharedWorkerGeneration) {
	if gen.live.Add(-1) != 0 {
		return
	}
	close(gen.drained)

	sharedWorkerLifecycleMu.Lock()
	defer sharedWorkerLifecycleMu.Unlock()
	for i, candidate := range sharedWorkerGens {
		if candidate == gen {
			sharedWorkerGens = append(sharedWorkerGens[:i], sharedWorkerGens[i+1:]...)
			break
		}
	}
	if sharedWorkerGen == gen && !sharedWorkersStarted {
		sharedWorkerGen = nil
	}
}

// stopSharedWorkersIfIdle drops one app's reference to the worker
// pool. When the count reaches zero the stop channel is closed so
// workers exit on their next idle-path check.
//
// Fire-and-forget: Close does not wait for workers to drain because
// they may be inside a user handler (long-poll, SSE stream) that
// doesn't return until the client disconnects. Blocking Close on
// such handlers would defeat the point of having a fast Close path.
// Workers receive the signal and exit asynchronously; a subsequent
// App.Listen on a shared route lazy-restarts the pool. Call
// WaitForSharedWorkers if a process really must observe the drain
// (cleanup tests, supervisor handoff).
//
// Safe to call multiple times for a single App via the per-App
// idempotency guard in App.Close.
func stopSharedWorkersIfIdle() {
	sharedWorkerLifecycleMu.Lock()
	defer sharedWorkerLifecycleMu.Unlock()
	if sharedActiveApps.Add(-1) > 0 {
		return
	}
	// Every app sharing the pool is gone. Clear the loop-count hint so a
	// later pool start re-derives its default from whatever runs next: a
	// fresh single App falls back to the one-loop default rather than
	// inheriting a stale (e.g. RunMultiCore(8)) hint and over-subscribing.
	// RunMultiCore republishes the hint before its first GetAsync, so the
	// multi-core path is unaffected.
	if !sharedWorkersStarted || sharedWorkerGen == nil {
		return
	}
	resetSharedCoreHintIfCurrent(sharedWorkerGen.coreHintToken)
	close(sharedWorkerGen.stop)
	sharedWorkersStarted = false
}

// WaitForSharedWorkers blocks until every shared-dispatch worker
// goroutine has exited, or timeout elapses (zero = wait forever).
// Returns true when the pool drained cleanly, false on timeout.
//
// Use this only when the caller has already arranged for all live
// handlers to return (e.g. forced socket close, request drain). The
// idle worker path itself wakes within at most one polling interval
// (~500 µs), so this returns quickly for processes that aren't
// holding goroutines hostage inside user code.
func WaitForSharedWorkers(timeout time.Duration) bool {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		sharedWorkerLifecycleMu.Lock()
		gens := append([]*sharedWorkerGeneration(nil), sharedWorkerGens...)
		sharedWorkerLifecycleMu.Unlock()
		if len(gens) == 0 {
			return true
		}

		for _, gen := range gens {
			if timeout <= 0 {
				<-gen.drained
				continue
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return false
			}
			timer := time.NewTimer(remaining)
			select {
			case <-gen.drained:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			case <-timer.C:
				return false
			}
		}
	}
}

// sharedWorker polls the request ring with adaptive back-off. Spin a handful
// of iterations, then yield via Gosched, then sleep progressively longer up
// to a cap. At sustained load the spin path catches work immediately; idle
// workers settle to a cheap periodic wake.
//
// On handler panic, the worker recovers, sends a 500 response (if the
// response hasn't already been written), releases the ctx, and continues
// the loop — a single bad request never tears down a worker.
//
// The stop channel is checked only on the idle / back-off branch (a
// closed channel makes the non-blocking select fall through to the
// exit path). Hot-path requests are never delayed by the check.
func sharedWorker(stop <-chan struct{}) {
	// Miss back-off, in order: spinTight pure re-polls (no scheduler
	// involvement at all), then Gosched yields until spinLimit, then the
	// progressive sleep below.
	//
	// The tight phase serves two roles. Under saturation it keeps the hot
	// path off the Go scheduler: a miss usually means "another worker
	// just claimed the slot" or "the producer is mid-publish", both of
	// which resolve within nanoseconds (the pre-v1.1 code answered every
	// such miss with runtime.Gosched(), and profiles under load showed
	// 60%+ of worker CPU inside runtime.lock2/schedule/findRunnable).
	//
	// Under LIGHT load it is the worker's awake window: requests arrive
	// tens of microseconds apart, and a worker that dozes off between
	// them adds up to a full idleSleep period (500µs at the cap) to every
	// response — time.Sleep on Linux also overshoots its first 10-40µs
	// tiers to 60-90µs of wall time, so any arrival that lands in the
	// sleep phase pays dearly. spinTight is therefore sized to cover
	// realistic light-load inter-arrival gaps with read-only polls of a
	// shared (read-mostly, so not bouncing) cache line that cost no
	// scheduler traffic at all.
	//
	// The value is a measured balance, sensitive in BOTH directions. 128
	// regressed light-load p50 from ~110µs to ~500µs (throughput -4x at
	// c=8, -36% at c=64) because workers spent almost their whole duty
	// cycle asleep. 32768 won light load back but cost ~18% at full
	// saturation: when the ring runs momentarily dry, every worker
	// polling hard steals exactly the CPU the loop threads need to
	// refill it. 16384 measured best-or-par across c=8/64/512 with idle
	// CPU at roughly half of the old 256-Gosched phase it replaces.
	const (
		spinTight = 16384
		spinLimit = spinTight + 8
	)

	headAddr := (*atomic.Uint64)(unsafe.Pointer(shared.requestRing + shared.headOffset))
	idleSleep := time.Duration(0)
	spins := 0

	for {
		idx := headAddr.Load()
		slotBase := shared.requestRing + shared.slotsOffset + uintptr(idx&shared.ringMask)*shared.slotStride
		seqAddr := (*atomic.Uint64)(unsafe.Pointer(slotBase + shared.slotSeqOffset))

		if seqAddr.Load() != idx+1 {
			// Slot not ready, or another worker has already advanced past idx.
			// Back off without burning the CPU; the next iteration re-reads
			// head, which will reflect the consumer that just claimed the slot.
			spins++
			if spins <= spinTight {
				continue
			}
			if spins > spinLimit {
				// Check for shutdown only here, on the idle path —
				// hot requests never pay for the select.
				select {
				case <-stop:
					return
				default:
				}
				if idleSleep == 0 {
					idleSleep = 10 * time.Microsecond
				} else if idleSleep < 500*time.Microsecond {
					idleSleep *= 2
				}
				time.Sleep(idleSleep)
				spins = 0
			} else {
				runtime.Gosched()
			}
			continue
		}
		idleSleep = 0
		spins = 0

		// Slot is ready. Race other workers to claim it.
		if !headAddr.CompareAndSwap(idx, idx+1) {
			continue
		}

		ctxPtr := *(*uintptr)(unsafe.Pointer(slotBase + shared.slotCtxOffset))
		// Mark slot empty for the next generation of producers (idx wraps in
		// ringSize, so the next producer for this slot waits for idx+ringSize).
		seqAddr.Store(idx + uint64(shared.ringMask) + 1)
		if sharedCtxClosing(ctxPtr) {
			asyncCtxRelease(ctxPtr)
			continue
		}
		handlerID := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxHandlerIDOff))

		// Read the handler from the atomic snapshot. Closed Apps tombstone
		// their slots; stale ctxs are released instead of dispatching to a
		// removed handler or panicking on a corrupt/out-of-range ID.
		handler, ok := lookupSharedHandler(handlerID)
		if !ok {
			asyncCtxRelease(ctxPtr)
			continue
		}

		// Run inline on the worker. Workers are sized for typical short
		// handlers (db queries, in-memory work). For longer-blocking
		// handlers (large file IO), increase WorkerCount or call
		// SetWorkerCount before the first GetAsync registration.
		runSharedHandler(handler, ctxPtr)
	}
}

// runSharedHandler invokes a user-supplied AsyncHandler with full panic
// containment. On panic it emits a 500 response (best-effort) and releases
// the ctx so the worker can keep running.
func runSharedHandler(handler AsyncHandler, ctxPtr uintptr) {
	// Read the real HttpResponse / Loop pointers stored in AsyncCtx so the
	// SendShared cgo-fallback path (when body exceeds inline caps) writes to
	// the right C++ objects rather than dereferencing the ctx itself.
	resPtr := *(**C.uwsgo_res_t)(unsafe.Pointer(ctxPtr + shared.ctxResponseOff))
	loopPtrRaw := *(*uintptr)(unsafe.Pointer(ctxPtr + shared.ctxLoopOff))

	resWrap := responsePool.Get().(*Response)
	resWrap.inner = responseNative{ptr: resPtr}
	resWrap.refs.Store(1)

	a := asyncStatePool.Get().(*asyncState)
	a.loopPtr = loopPtrRaw
	a.ctxHandle = ctxPtr
	a.status = "200 OK"
	a.sent = false
	resWrap.async = a

	// Pin the ctx for the lifetime of this request wrapper. The worker owns
	// the ring ref it popped, but sending the response transfers that ref to
	// the loop-thread consumer, which may recycle the ctx immediately — and
	// the headers snapshot below is a zero-copy view into ctx memory. The
	// pin is a direct atomic increment on the C++ refcount through shared
	// memory (no cgo); the matching release is the cgo call in the defer.
	sharedCtxRetain(ctxPtr)

	// Build the request snapshot from ctx memory. C++ has already copied the
	// fields it could into AsyncCtx; small fields are copied out to Go-owned
	// strings, while the headers blob stays a view into pinned ctx memory.
	reqWrap := requestPool.Get().(*Request)
	reqWrap.snap = newSnapshotFromCtx(ctxPtr)
	// post_shared routes leave the collected body in ctx memory;
	// readSharedReqBody copies it out into a Go slice so the
	// handler can keep the bytes past the request's lifetime.
	reqWrap.body = readSharedReqBody(ctxPtr)
	// Back-pointer for req.Context() so a client abort propagates
	// cancellation into downstream context-aware calls.
	reqWrap.res = resWrap

	defer func() {
		if r := recover(); r != nil {
			reportPanic(r)
			if !a.sent {
				// Best-effort 500 so the client doesn't hang. Body is left
				// minimal so we don't risk another panic during marshaling.
				resWrap.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
			}
		}
		if !a.sent {
			asyncCtxRelease(ctxPtr)
		}
		reqWrap.resetForPool()
		requestPool.Put(reqWrap)
		resWrap.finishAsync(a)
		// Drop the lifetime pin taken before the snapshot was built. This
		// must be the last ctx access on the worker: the release can recycle
		// the ctx (or free it during app teardown), and every snapshot view
		// into ctx memory is unreachable once reqWrap was reset above.
		asyncCtxRelease(ctxPtr)
	}()

	handler(resWrap, reqWrap)
}

// sharedCtxRetain bumps the AsyncCtx refcount directly through shared
// memory — the C++ side declares it std::atomic<int>, which is layout- and
// semantics-compatible with atomic.Int32 on every supported platform (the
// same contract the aborted/closing flags already rely on).
func sharedCtxRetain(ctxPtr uintptr) {
	(*atomic.Int32)(unsafe.Pointer(ctxPtr + shared.ctxRefcountOff)).Add(1)
}

// newSnapshotFromCtx reads the request-snapshot fields C++ wrote into the
// AsyncCtx and returns a Go-side requestSnapshot. Small fields (method, url,
// query, ip, params) are copied into ONE arena allocation with the
// snapshot's strings as views into it. The headers blob — the dominant byte
// count, 1-2 KiB from real browsers — is NOT copied: the snapshot records a
// raw view (headersSrc/headersSrcLen) into ctx memory, which runSharedHandler
// pins for the request wrapper's whole lifetime. Header reads copy only the
// matched value, exactly as before; the wholesale blob copy is gone.
//
// Per request this costs two allocations (snapshot struct + arena); under
// saturation the previous per-field version showed mallocgc at ~25% of
// worker CPU. The arena is never pooled or reused, so a handler retaining
// any snapshot string simply keeps the (small) arena alive — same safety as
// before.
func newSnapshotFromCtx(ctxPtr uintptr) *requestSnapshot {
	methodLen := boundedUint32Len(*(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxMethodLenOff)), shared.snapMethodCap)
	urlLen := boundedUint32Len(*(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxURLLenOff)), shared.snapURLCap)
	queryLen := boundedUint32Len(*(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxQueryLenOff)), shared.snapQueryCap)
	ipLen := boundedUint32Len(*(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxIPLenOff)), shared.snapIPCap)
	paramCount := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxParamCountOff))
	headersLen := boundedUint32Len(*(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxHeadersLenOff)), shared.snapHeadersCap)
	truncated := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxTruncatedOff)) != 0

	// Defense in depth: the C++ side caps paramCount at SNAP_PARAM_MAX
	// before writing it, but if that invariant ever breaks (layout
	// drift, bridge regression, memory corruption) trusting the raw
	// value would let Go allocate an arbitrarily large slice and
	// read past the AsyncCtx's param region. Clamp here so the worst
	// case stays bounded.
	if paramCount > uint32(shared.snapParamMax) {
		paramCount = uint32(shared.snapParamMax)
	}

	// First pass: clamp the per-param lengths and size the arena.
	paramsBase := ctxPtr + shared.ctxParamsOff
	paramLensBase := ctxPtr + shared.ctxParamLensOff
	var paramLens [snapParamArrayMax]int
	paramBytes := 0
	fixedParams := paramCount <= snapParamArrayMax
	if fixedParams {
		for i := uint32(0); i < paramCount; i++ {
			plen := *(*uint32)(unsafe.Pointer(paramLensBase + uintptr(i)*unsafe.Sizeof(uint32(0))))
			// Per-param length should also fit within snapParamCap;
			// clamp so a corrupted length can't drive the copy past
			// the slot.
			n := boundedUint32Len(plen, shared.snapParamCap)
			paramLens[i] = n
			paramBytes += n
		}
	}

	snap := &requestSnapshot{truncated: truncated}
	if headersLen > 0 {
		snap.headersSrc = unsafe.Pointer(ctxPtr + shared.ctxHeadersOff)
		snap.headersSrcLen = headersLen
	}

	// One arena holds every variable-length field except headers (see the
	// doc comment). It is sized exactly and filled with append — the
	// capacity must never be exceeded, or the realloc would leave earlier
	// unsafe.String views dangling.
	arena := make([]byte, 0, methodLen+urlLen+queryLen+ipLen+paramBytes)
	take := func(base uintptr, n int) string {
		if n <= 0 {
			return ""
		}
		off := len(arena)
		arena = append(arena, unsafe.Slice((*byte)(unsafe.Pointer(base)), n)...)
		return unsafe.String(&arena[off], n)
	}

	snap.method = take(ctxPtr+shared.ctxMethodOff, methodLen)
	snap.url = take(ctxPtr+shared.ctxURLOff, urlLen)
	snap.query = take(ctxPtr+shared.ctxQueryOff, queryLen)
	snap.ip = take(ctxPtr+shared.ctxIPOff, ipLen)

	if paramCount > 0 {
		if fixedParams {
			for i := uint32(0); i < paramCount; i++ {
				snap.paramsArr[i] = take(paramsBase+uintptr(i)*shared.snapParamCap, paramLens[i])
			}
			snap.params = snap.paramsArr[:paramCount]
		} else {
			// The bridge reported more params than the fixed array
			// holds — only possible if SNAP_PARAM_MAX grows without
			// snapParamArrayMax following. Stay correct on the old
			// per-slice path.
			params := make([]string, paramCount)
			for i := uint32(0); i < paramCount; i++ {
				plen := *(*uint32)(unsafe.Pointer(paramLensBase + uintptr(i)*unsafe.Sizeof(uint32(0))))
				params[i] = copyAt(paramsBase+uintptr(i)*shared.snapParamCap, boundedUint32Len(plen, shared.snapParamCap))
			}
			snap.params = params
		}
	}

	return snap
}

// readSharedReqBody copies the post_shared-collected request body
// out of ctx memory into a fresh Go slice so it survives ctx
// release. GET routes leave body_len = 0 — the call is a single
// compare-and-skip for that path.
func readSharedReqBody(ctxPtr uintptr) []byte {
	bodyLen := boundedUint32Len(*(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxReqBodyLenOff)), shared.snapReqBodyCap)
	if bodyLen == 0 {
		return nil
	}
	out := make([]byte, bodyLen)
	copy(out, unsafe.Slice((*byte)(unsafe.Pointer(ctxPtr+shared.ctxReqBodyOff)), bodyLen))
	return out
}

func copyAt(base uintptr, n int) string {
	if n <= 0 {
		return ""
	}
	return string(unsafe.Slice((*byte)(unsafe.Pointer(base)), n))
}

func (a *appNative) getStatic(pattern, status, contentType, body string) {
	cpattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cpattern))

	a.onOwner(func() {
		C.uwsgo_app_get_static(a.ptr, cpattern,
			unsafeStringData(status), C.size_t(len(status)),
			unsafeStringData(contentType), C.size_t(len(contentType)),
			unsafeStringData(body), C.size_t(len(body)),
		)
	})
}

func (a *appNative) post(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))

	a.onOwner(func() {
		C.uwsgo_app_post(a.ptr, cpattern, C.uintptr_t(handle))
	})
}

func (a *appNative) any(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))

	a.onOwner(func() {
		C.uwsgo_app_any(a.ptr, cpattern, C.uintptr_t(handle))
	})
}

func (a *appNative) put(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))
	a.onOwner(func() {
		C.uwsgo_app_put(a.ptr, cpattern, C.uintptr_t(handle))
	})
}

func (a *appNative) patch(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))
	a.onOwner(func() {
		C.uwsgo_app_patch(a.ptr, cpattern, C.uintptr_t(handle))
	})
}

func (a *appNative) deleteM(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))
	a.onOwner(func() {
		C.uwsgo_app_delete(a.ptr, cpattern, C.uintptr_t(handle))
	})
}

func (a *appNative) options(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))
	a.onOwner(func() {
		C.uwsgo_app_options(a.ptr, cpattern, C.uintptr_t(handle))
	})
}

func (a *appNative) head(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))
	a.onOwner(func() {
		C.uwsgo_app_head(a.ptr, cpattern, C.uintptr_t(handle))
	})
}

func (a *appNative) websocket(pattern string, behavior WebSocketBehavior) {
	cpattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cpattern))

	if behavior.Upgrade == nil && !behavior.UnsafeAutoUpgrade {
		behavior.Upgrade = defaultWebSocketUpgrade
	}

	handle := cgo.NewHandle(behavior)
	a.handles = append(a.handles, handle)

	pings := 1
	if behavior.DisablePings {
		pings = 0
	}
	withUpgrade := 0
	if behavior.Upgrade != nil {
		withUpgrade = 1
	}

	a.onOwner(func() {
		C.uwsgo_app_ws(a.ptr, cpattern, C.uintptr_t(handle),
			cUint32SizeFromPositive(behavior.MaxPayloadLength, 16<<20),
			cUint16IntFromDurationSeconds(behavior.IdleTimeout, 120),
			cUint32SizeFromPositive(behavior.MaxBackpressure, 64<<10), C.int(pings),
			C.int(withUpgrade))
	})
}

func (a *appNative) prepareRoute(pattern string, handler Handler) (*C.char, cgo.Handle) {
	cpattern := C.CString(pattern)
	handle := cgo.NewHandle(handler)
	a.handles = append(a.handles, handle)
	return cpattern, handle
}

func (a *appNative) listen(host string, port int) bool {
	if port < 0 || port > maxInt32 {
		return false
	}
	var chost *C.char
	if host != "" {
		chost = C.CString(host)
		defer C.free(unsafe.Pointer(chost))
	}
	return a.onOwnerBool(func() bool {
		return C.uwsgo_app_listen(a.ptr, chost, cIntFromNonNegative(port)) != 0
	})
}

func (a *appNative) addChild(child appNative) bool {
	return a.onOwnerBool(func() bool {
		return C.uwsgo_app_add_child(a.ptr, child.ptr) != 0
	})
}

func (a *appNative) setBodyLimit(limit int) {
	limit = clampNonNegativeInt(limit)
	a.onOwner(func() {
		C.uwsgo_app_set_body_limit(a.ptr, C.size_t(limit))
	})
}

func (a *appNative) setCapturePeerIP(enable bool) {
	v := C.int(0)
	if enable {
		v = 1
	}
	a.onOwner(func() {
		C.uwsgo_app_set_capture_peer_ip(a.ptr, v)
	})
}

func (a *appNative) run() {
	a.onOwner(func() {
		C.uwsgo_app_run(a.ptr)
	})
}

func (a *appNative) stop() {
	C.uwsgo_app_stop(a.ptr)
}

func (a *appNative) closeListen() {
	C.uwsgo_app_close_listen(a.ptr)
}

func (a *appNative) close() {
	if a.ptr == nil {
		return
	}

	ptr := a.ptr
	a.onOwner(func() {
		C.uwsgo_app_free(ptr)
	})
	a.ptr = nil

	for _, handle := range a.handles {
		handle.Delete()
	}

	a.handles = nil
	cleanupSharedHandlers(a.sharedHandlerIDs)
	a.sharedHandlerIDs = nil
	a.owner.close()
}

func (r responseNative) status(status string) {
	C.uwsgo_res_write_status(r.ptr, unsafeStringData(status), C.size_t(len(status)))
}

func (r responseNative) header(key, value string) {
	C.uwsgo_res_write_header(
		r.ptr,
		unsafeStringData(key), C.size_t(len(key)),
		unsafeStringData(value), C.size_t(len(value)),
	)
}

// headersBatch ships count (name,value) pairs to uWS in a single
// cgo crossing using the packed `key\0value\0key\0value\0` blob
// format. Takes []byte (not string) so the caller can reuse a
// pooled buffer across requests — eliminates the per-request
// heap allocation that strings.Builder would otherwise produce
// at high RPS.
//
// Used by flushPendingHeaders when 2+ headers are buffered — at
// 1 header the per-call overhead of packing the blob isn't worth
// it vs. the single header() crossing.
func (r responseNative) headersBatch(blob []byte, count int) {
	if count == 0 || len(blob) == 0 {
		return
	}
	C.uwsgo_res_write_headers_batch(
		r.ptr,
		(*C.char)(unsafe.Pointer(&blob[0])), C.size_t(len(blob)),
		C.size_t(count),
	)
}

func (r responseNative) write(body string) {
	C.uwsgo_res_write(r.ptr, unsafeStringData(body), C.size_t(len(body)))
}

func (r responseNative) end(body string) {
	C.uwsgo_res_end(r.ptr, unsafeStringData(body), C.size_t(len(body)))
}

func (r responseNative) send(status, contentType, body string) {
	C.uwsgo_res_send(
		r.ptr,
		unsafeStringData(status), C.size_t(len(status)),
		unsafeStringData(contentType), C.size_t(len(contentType)),
		unsafeStringData(body), C.size_t(len(body)),
	)
}

func (r responseNative) sendSplit(status, contentType string, headers []byte, prefix, body string) {
	C.uwsgo_res_send_split(
		r.ptr,
		unsafeStringData(status), C.size_t(len(status)),
		unsafeStringData(contentType), C.size_t(len(contentType)),
		unsafeByteData(headers), C.size_t(len(headers)),
		unsafeStringData(prefix), C.size_t(len(prefix)),
		unsafeStringData(body), C.size_t(len(body)),
	)
}

func (r responseNative) loop() loopNative {
	return loopNative{ptr: C.uwsgo_res_get_loop(r.ptr)}
}

// loopFromUintptr rebuilds a *Loop from a previously-cached pointer.
// Use this in any code path that wants to schedule work onto the loop
// without re-querying it via res.Loop() — required from worker
// goroutines in the shared-dispatch path, where uWS::Loop::get() is
// thread-local and returns the wrong loop (or none) when called from a
// non-loop thread.
func loopFromUintptr(p uintptr) *Loop {
	return &Loop{inner: loopNative{ptr: (*C.uwsgo_loop_t)(unsafe.Pointer(p))}}
}

// remoteAddr returns the formatted peer IP for the connection underlying
// this response. uWS caches the formatted string on its side; calling
// this multiple times for the same request is a single allocation in Go
// plus a couple of memcpys in C++.
func (r responseNative) remoteAddr() string {
	return readNativeString(func(buf *C.char, n C.size_t) C.size_t {
		return C.uwsgo_res_remote_addr(r.ptr, buf, n)
	})
}

func (r responseNative) onAborted(callback any) {
	handle := cgo.NewHandle(callback)
	C.uwsgo_res_on_aborted(r.ptr, C.uintptr_t(handle))
}

func (r responseNative) cork(fn func()) {
	handle := cgo.NewHandle(fn)
	C.uwsgo_res_cork(r.ptr, C.uintptr_t(handle))
}

func (r responseNative) onData(fn func([]byte, bool)) {
	handle := cgo.NewHandle(fn)
	C.uwsgo_res_on_data(r.ptr, C.uintptr_t(handle))
}

func (l loopNative) defer_(fn func()) {
	handle := cgo.NewHandle(fn)
	C.uwsgo_loop_defer(l.ptr, C.uintptr_t(handle))
}

func (r responseNative) beginAsync() (loopPtr, ctxHandle uintptr) {
	var ctx unsafe.Pointer
	loop := C.uwsgo_res_begin_async(r.ptr, &ctx)
	return uintptr(unsafe.Pointer(loop)), uintptr(ctx)
}

func asyncDeferSend(loopPtr, ctxHandle uintptr, status, contentType, body string) {
	C.uwsgo_res_defer_send(
		(*C.uwsgo_loop_t)(unsafe.Pointer(loopPtr)),
		unsafe.Pointer(ctxHandle),
		unsafeStringData(status), C.size_t(len(status)),
		unsafeStringData(contentType), C.size_t(len(contentType)),
		unsafeStringData(body), C.size_t(len(body)),
	)
}

// asyncDeferSendWithHeaders is the variant that carries an extra packed
// headers blob ("name\0value\0name\0value\0…"). Used by compression
// middleware in async mode (Content-Encoding/Vary) and by any future
// middleware that needs to attach response headers to an async response.
// The shared-memory fast path has no slot for arbitrary headers, so this
// always goes through the cgo defer-send shim.
func asyncDeferSendWithHeaders(loopPtr, ctxHandle uintptr, status, contentType, headersBlob, body string) {
	var headersPtr *C.char
	if len(headersBlob) > 0 {
		headersPtr = unsafeStringData(headersBlob)
	}
	C.uwsgo_res_defer_send_with_headers(
		(*C.uwsgo_loop_t)(unsafe.Pointer(loopPtr)),
		unsafe.Pointer(ctxHandle),
		unsafeStringData(status), C.size_t(len(status)),
		unsafeStringData(contentType), C.size_t(len(contentType)),
		headersPtr, C.size_t(len(headersBlob)),
		unsafeStringData(body), C.size_t(len(body)),
	)
}

// asyncDeferStreamStart kicks off a streaming response: status +
// headers go out on the loop thread without closing the response.
// Each subsequent chunk goes through asyncDeferStreamWrite, and
// asyncDeferStreamEnd closes the response. See res.Stream for the
// caller-facing wrapper.
func asyncDeferStreamStart(loopPtr, ctxHandle uintptr, status, contentType, headersBlob string) bool {
	var headersPtr *C.char
	if len(headersBlob) > 0 {
		headersPtr = unsafeStringData(headersBlob)
	}
	return C.uwsgo_res_defer_stream_start(
		(*C.uwsgo_loop_t)(unsafe.Pointer(loopPtr)),
		unsafe.Pointer(ctxHandle),
		unsafeStringData(status), C.size_t(len(status)),
		unsafeStringData(contentType), C.size_t(len(contentType)),
		headersPtr, C.size_t(len(headersBlob)),
	) != 0
}

// asyncDeferStreamWrite queues one chunk for the loop thread to
// write. The C bridge dups the bytes to a heap copy before the
// defer is queued, so the Go-side caller may reuse the buffer
// immediately after this returns.
func asyncDeferStreamWrite(loopPtr, ctxHandle uintptr, chunk string) {
	var chunkPtr *C.char
	if len(chunk) > 0 {
		chunkPtr = unsafeStringData(chunk)
	}
	C.uwsgo_res_defer_stream_write(
		(*C.uwsgo_loop_t)(unsafe.Pointer(loopPtr)),
		unsafe.Pointer(ctxHandle),
		chunkPtr, C.size_t(len(chunk)),
	)
}

// asyncDeferStreamEnd closes the streaming response with an empty
// body — uWS writes the chunked-encoding terminator and tears down
// the HttpResponse.
func asyncDeferStreamEnd(loopPtr, ctxHandle uintptr) {
	C.uwsgo_res_defer_stream_end(
		(*C.uwsgo_loop_t)(unsafe.Pointer(loopPtr)),
		unsafe.Pointer(ctxHandle),
	)
}

// innerBufferedAmount samples uWS's send-queue depth in bytes
// via the C-side responseNative pointer. Used by
// Response.BufferedAmount to detect backpressure on streaming
// responses. The underlying read is a single naturally-aligned
// size_t in uSockets; calling from a worker goroutine (not the
// loop thread) returns a possibly-stale but coherent value —
// fine for "should I throttle?" decisions.
func innerBufferedAmount(rn responseNative) uint64 {
	if rn.ptr == nil {
		return 0
	}
	return uint64(C.uwsgo_res_buffered_amount(rn.ptr))
}

func asyncCtxRelease(ctxHandle uintptr) {
	C.uwsgo_async_ctx_release(unsafe.Pointer(ctxHandle))
}

func asyncCtxRetain(ctxHandle uintptr) {
	if ctxHandle == 0 {
		return
	}
	C.uwsgo_async_ctx_retain(unsafe.Pointer(ctxHandle))
}

func asyncCtxAborted(ctxHandle uintptr) bool {
	if ctxHandle == 0 {
		return true
	}
	if sharedReady {
		if (*atomic.Int32)(unsafe.Pointer(ctxHandle+shared.ctxAbortedOff)).Load() != 0 {
			return true
		}
		return sharedCtxClosing(ctxHandle)
	}
	return C.uwsgo_async_ctx_aborted(unsafe.Pointer(ctxHandle)) != 0
}

func asyncCtxStreamPendingBytes(ctxHandle uintptr) uint64 {
	if ctxHandle == 0 {
		return 0
	}
	return uint64(C.uwsgo_async_ctx_stream_pending_bytes(unsafe.Pointer(ctxHandle)))
}

// sharedLayout caches struct offsets exposed by C so the hot path can build
// responses with plain unsafe.Pointer arithmetic and atomic ops, no cgo.
// Each AsyncCtx carries a pointer to its App's pending ring; asyncSendShared
// reads ctx->pending_ring rather than using a global so multiple App
// instances can coexist.
type sharedLayout struct {
	requestRing         uintptr
	ctxPendingRingOff   uintptr
	ringMask            uint64
	slotsOffset         uintptr
	slotStride          uintptr
	slotSeqOffset       uintptr
	slotCtxOffset       uintptr
	headOffset          uintptr
	tailOffset          uintptr
	wakePendingOffset   uintptr
	ctxStatusLenOff     uintptr
	ctxCtLenOff         uintptr
	ctxBodyLenOff       uintptr
	ctxStatusOff        uintptr
	ctxCtOff            uintptr
	ctxBodyOff          uintptr
	ctxHandlerIDOff     uintptr
	ctxAbortedOff       uintptr
	ctxRefcountOff      uintptr
	ctxResponseOff      uintptr
	ctxLoopOff          uintptr
	ctxSharedStateOff   uintptr
	stateClosingOff     uintptr
	stateActiveSendsOff uintptr
	statusCap           uintptr
	ctCap               uintptr
	bodyCap             uintptr
	// Request snapshot offsets (populated by C++ before the ctx is enqueued).
	ctxMethodLenOff  uintptr
	ctxURLLenOff     uintptr
	ctxQueryLenOff   uintptr
	ctxIPLenOff      uintptr
	ctxParamCountOff uintptr
	ctxHeadersLenOff uintptr
	ctxTruncatedOff  uintptr
	ctxParamLensOff  uintptr
	ctxMethodOff     uintptr
	ctxURLOff        uintptr
	ctxQueryOff      uintptr
	ctxIPOff         uintptr
	ctxParamsOff     uintptr
	ctxHeadersOff    uintptr
	// post_shared request-body collection: byte count + overflow
	// flag + body buffer offsets, plus the per-ctx body capacity.
	ctxReqBodyLenOff      uintptr
	ctxReqBodyOverflowOff uintptr
	ctxReqBodyOff         uintptr
	snapMethodCap         uintptr
	snapURLCap            uintptr
	snapQueryCap          uintptr
	snapIPCap             uintptr
	snapParamCap          uintptr
	snapParamMax          uintptr
	snapHeadersCap        uintptr
	snapReqBodyCap        uintptr
}

var shared sharedLayout
var sharedReady bool
var sharedLayoutOnce sync.Once

func initSharedLayout() {
	// The layout is a process-wide constant exposed by C++; once read it does
	// not change. Guard with sync.Once so multiple NewApp calls don't race on
	// the global `shared` struct against workers that started reading after the
	// first init.
	sharedLayoutOnce.Do(initSharedLayoutOnce)
}

func initSharedLayoutOnce() {
	var raw C.uwsgo_shared_layout_t
	C.uwsgo_shared_layout(&raw)
	shared = sharedLayout{
		requestRing:         uintptr(raw.request_ring),
		ctxPendingRingOff:   uintptr(raw.ctx_pending_ring_offset),
		ringMask:            uint64(raw.ring_mask),
		slotsOffset:         uintptr(raw.ring_slots_offset),
		slotStride:          uintptr(raw.ring_slot_stride),
		slotSeqOffset:       uintptr(raw.ring_slot_seq_offset),
		slotCtxOffset:       uintptr(raw.ring_slot_ctx_offset),
		headOffset:          uintptr(raw.ring_head_offset),
		tailOffset:          uintptr(raw.ring_tail_offset),
		wakePendingOffset:   uintptr(raw.ring_wake_pending_offset),
		ctxStatusLenOff:     uintptr(raw.ctx_status_len_offset),
		ctxCtLenOff:         uintptr(raw.ctx_ct_len_offset),
		ctxBodyLenOff:       uintptr(raw.ctx_body_len_offset),
		ctxStatusOff:        uintptr(raw.ctx_status_offset),
		ctxCtOff:            uintptr(raw.ctx_ct_offset),
		ctxBodyOff:          uintptr(raw.ctx_body_offset),
		ctxHandlerIDOff:     uintptr(raw.ctx_handler_id_offset),
		ctxAbortedOff:       uintptr(raw.ctx_aborted_offset),
		ctxRefcountOff:      uintptr(raw.ctx_refcount_offset),
		ctxResponseOff:      uintptr(raw.ctx_response_offset),
		ctxLoopOff:          uintptr(raw.ctx_loop_offset),
		ctxSharedStateOff:   uintptr(raw.ctx_shared_state_offset),
		stateClosingOff:     uintptr(raw.state_closing_offset),
		stateActiveSendsOff: uintptr(raw.state_active_sends_offset),
		statusCap:           uintptr(raw.ctx_inline_status_cap),
		ctCap:               uintptr(raw.ctx_inline_ct_cap),
		bodyCap:             uintptr(raw.ctx_inline_body_cap),

		ctxMethodLenOff:       uintptr(raw.ctx_method_len_offset),
		ctxURLLenOff:          uintptr(raw.ctx_url_len_offset),
		ctxQueryLenOff:        uintptr(raw.ctx_query_len_offset),
		ctxIPLenOff:           uintptr(raw.ctx_ip_len_offset),
		ctxParamCountOff:      uintptr(raw.ctx_param_count_offset),
		ctxHeadersLenOff:      uintptr(raw.ctx_headers_len_offset),
		ctxTruncatedOff:       uintptr(raw.ctx_truncated_offset),
		ctxParamLensOff:       uintptr(raw.ctx_param_lens_offset),
		ctxMethodOff:          uintptr(raw.ctx_method_offset),
		ctxURLOff:             uintptr(raw.ctx_url_offset),
		ctxQueryOff:           uintptr(raw.ctx_query_offset),
		ctxIPOff:              uintptr(raw.ctx_ip_offset),
		ctxParamsOff:          uintptr(raw.ctx_params_offset),
		ctxHeadersOff:         uintptr(raw.ctx_headers_offset),
		ctxReqBodyLenOff:      uintptr(raw.ctx_req_body_len_offset),
		ctxReqBodyOverflowOff: uintptr(raw.ctx_req_body_overflow_offset),
		ctxReqBodyOff:         uintptr(raw.ctx_req_body_offset),
		snapMethodCap:         uintptr(raw.ctx_snap_method_cap),
		snapURLCap:            uintptr(raw.ctx_snap_url_cap),
		snapQueryCap:          uintptr(raw.ctx_snap_query_cap),
		snapIPCap:             uintptr(raw.ctx_snap_ip_cap),
		snapParamCap:          uintptr(raw.ctx_snap_param_cap),
		snapParamMax:          uintptr(raw.ctx_snap_param_max),
		snapHeadersCap:        uintptr(raw.ctx_snap_headers_cap),
		snapReqBodyCap:        uintptr(raw.ctx_snap_req_body_cap),
	}
	sharedReady = true
}

func sharedCtxState(ctxHandle uintptr) uintptr {
	if !sharedReady || ctxHandle == 0 {
		return 0
	}
	return *(*uintptr)(unsafe.Pointer(ctxHandle + shared.ctxSharedStateOff))
}

func sharedStateClosing(statePtr uintptr) bool {
	if statePtr == 0 {
		return false
	}
	return (*atomic.Int32)(unsafe.Pointer(statePtr+shared.stateClosingOff)).Load() != 0
}

func sharedCtxClosing(ctxHandle uintptr) bool {
	return sharedStateClosing(sharedCtxState(ctxHandle))
}

func beginSharedSend(ctxHandle uintptr) (uintptr, bool) {
	statePtr := sharedCtxState(ctxHandle)
	if statePtr == 0 || sharedStateClosing(statePtr) {
		return 0, false
	}
	active := (*atomic.Int32)(unsafe.Pointer(statePtr + shared.stateActiveSendsOff))
	active.Add(1)
	if sharedStateClosing(statePtr) {
		active.Add(-1)
		return 0, false
	}
	return statePtr, true
}

func finishSharedSend(statePtr uintptr) {
	if statePtr == 0 {
		return
	}
	(*atomic.Int32)(unsafe.Pointer(statePtr + shared.stateActiveSendsOff)).Add(-1)
}

func (a *appNative) startSharedDrain(intervalUs int) {
	a.onOwner(func() {
		C.uwsgo_app_start_drain(a.ptr, cIntFromNonNegative(intervalUs))
	})
}

// asyncSendShared writes the response bytes directly into the AsyncCtx memory
// (allocated in the C heap), then pushes the ctx pointer onto the shared
// MPMC ring. No cgo crossing happens on the hot path. Returns false when the
// body or content_type exceeds the inline buffer caps; caller should fall
// back to the cgo defer path in that case.
func asyncSendShared(ctxHandle uintptr, statusLine, contentType, body string) bool {
	if !sharedReady || uintptr(len(statusLine)) > shared.statusCap ||
		uintptr(len(contentType)) > shared.ctCap ||
		uintptr(len(body)) > shared.bodyCap {
		return false
	}
	statePtr, ok := beginSharedSend(ctxHandle)
	if !ok {
		return false
	}

	// Write status/ct/body bytes into the ctx's inline buffers.
	if n := len(statusLine); n > 0 {
		dst := unsafe.Slice((*byte)(unsafe.Pointer(ctxHandle+shared.ctxStatusOff)), int(shared.statusCap))
		copy(dst, statusLine)
	}
	if n := len(contentType); n > 0 {
		dst := unsafe.Slice((*byte)(unsafe.Pointer(ctxHandle+shared.ctxCtOff)), int(shared.ctCap))
		copy(dst, contentType)
	}
	if n := len(body); n > 0 {
		dst := unsafe.Slice((*byte)(unsafe.Pointer(ctxHandle+shared.ctxBodyOff)), int(shared.bodyCap))
		copy(dst, body)
	}
	// Lengths.
	*(*uint32)(unsafe.Pointer(ctxHandle + shared.ctxStatusLenOff)) = uint32(len(statusLine))
	*(*uint32)(unsafe.Pointer(ctxHandle + shared.ctxCtLenOff)) = uint32(len(contentType))
	*(*uint32)(unsafe.Pointer(ctxHandle + shared.ctxBodyLenOff)) = uint32(len(body))

	// Read the App-specific pending ring AND loop pointer this ctx targets
	// BEFORE publishing the ctx onto the ring. C++ stamps both fields onto
	// the ctx at request-arrival time and they don't change for the life of
	// the ctx, so reading them now is fine — but the moment we publish (the
	// seqAddr.Store below), the loop-thread consumer is free to drain the
	// slot, send the response, and release the ctx. Any read off ctx after
	// publish is a use-after-free hazard.
	ringPtr := *(*uintptr)(unsafe.Pointer(ctxHandle + shared.ctxPendingRingOff))
	if ringPtr == 0 {
		finishSharedSend(statePtr)
		return false
	}
	loopPtr := *(*uintptr)(unsafe.Pointer(ctxHandle + shared.ctxLoopOff))

	// MPSC enqueue: claim a ready slot with CAS. If the response ring is full
	// or heavily contended, return false so the caller can fall back to
	// Loop::defer instead of spinning unbounded on a worker goroutine.
	//
	// Backoff shape: CAS retries against other producers normally resolve
	// within a handful of iterations, so the first enqueueSpinYield laps
	// run tight. Past that the P yields every lap so co-scheduled workers
	// make progress, and at enqueueSpinBudget the producer stops fighting:
	// the cgo defer fallback costs about a microsecond, which is far
	// cheaper than the tens of milliseconds of burned CPU the previous
	// 100k-spin limit allowed under heavy producer contention.
	const (
		enqueueSpinYield  = 64
		enqueueSpinBudget = 512
	)
	tailAddr := (*atomic.Uint64)(unsafe.Pointer(ringPtr + shared.tailOffset))
	tail := tailAddr.Load()
	var (
		slotBase uintptr
		seqAddr  *atomic.Uint64
	)
	for spin := 0; ; spin++ {
		slotBase = ringPtr + shared.slotsOffset + uintptr(tail&shared.ringMask)*shared.slotStride
		seqAddr = (*atomic.Uint64)(unsafe.Pointer(slotBase + shared.slotSeqOffset))
		seq := seqAddr.Load()
		diff := int64(seq - tail)
		switch {
		case diff == 0:
			if tailAddr.CompareAndSwap(tail, tail+1) {
				goto claimed
			}
			tail = tailAddr.Load()
		case diff < 0:
			finishSharedSend(statePtr)
			return false
		default:
			tail = tailAddr.Load()
		}
		if spin >= enqueueSpinBudget {
			finishSharedSend(statePtr)
			return false
		}
		if spin >= enqueueSpinYield {
			runtime.Gosched()
		}
	}

claimed:
	*(*uintptr)(unsafe.Pointer(slotBase + shared.slotCtxOffset)) = ctxHandle
	seqAddr.Store(tail + 1)

	// Ctx is now owned by the consumer — do NOT touch it again. Use the
	// cached loopPtr to wake the drain.
	//
	// Skip the wake_drain cgo crossing if another producer (or a stale
	// scheduled wake) already has one pending: drain clears wake_pending
	// the moment it starts, so a successful CAS(0,1) here means "I'm the
	// first producer since the last drain pass began, the wake is mine
	// to call". Under sustained load this drops the cgo wake rate by
	// the average batch size of the ring — at 91k rps on /db a single
	// drain commonly consumes dozens of slots, so this typically
	// eliminates 90%+ of wake_drain crossings.
	if loopPtr != 0 {
		wakeAddr := (*atomic.Uint32)(unsafe.Pointer(ringPtr + shared.wakePendingOffset))
		if wakeAddr.CompareAndSwap(0, 1) {
			C.uwsgo_wake_drain(
				(*C.uwsgo_loop_t)(unsafe.Pointer(loopPtr)),
				unsafe.Pointer(ringPtr),
			)
		}
	}
	finishSharedSend(statePtr)
	return true
}

func (r requestNative) method() string {
	return readNativeString(func(buf *C.char, len C.size_t) C.size_t {
		return C.uwsgo_req_method(r.ptr, buf, len)
	})
}

func (r requestNative) url() string {
	return readNativeString(func(buf *C.char, len C.size_t) C.size_t {
		return C.uwsgo_req_url(r.ptr, buf, len)
	})
}

func (r requestNative) header(name string) string {
	return readNativeString(func(buf *C.char, bufLen C.size_t) C.size_t {
		return C.uwsgo_req_header(r.ptr, unsafeStringData(name), C.size_t(len(name)), buf, bufLen)
	})
}

func (r requestNative) parameter(index int) string {
	if index < 0 {
		return ""
	}
	return readNativeString(func(buf *C.char, len C.size_t) C.size_t {
		return C.uwsgo_req_parameter(r.ptr, C.ulong(index), buf, len)
	})
}

func (r requestNative) query() string {
	return readNativeString(func(buf *C.char, len C.size_t) C.size_t {
		return C.uwsgo_req_query(r.ptr, buf, len)
	})
}

func (r requestNative) queryParam(name string) string {
	return readNativeString(func(buf *C.char, bufLen C.size_t) C.size_t {
		return C.uwsgo_req_query_param(r.ptr, unsafeStringData(name), C.size_t(len(name)), buf, bufLen)
	})
}

// headersAll returns the request headers in the "name\0value\0..." format
// used by requestSnapshot. Allocates and copies — only callable while the
// uWS HttpRequest is still live. Unlike the sync dispatcher's scratch blob,
// this intentionally copies the full header list so overflow requests do not
// silently hide trailing headers from req.Headers or async fallback snapshots.
func (r requestNative) headersAll() []byte {
	size := C.uwsgo_req_headers_all(r.ptr, nil, 0)
	if size == 0 {
		return nil
	}
	n, ok := goIntFromCSize(size, "request headers")
	if !ok {
		return nil
	}
	buf := make([]byte, n)
	written := C.uwsgo_req_headers_all(r.ptr, (*C.char)(unsafe.Pointer(&buf[0])), size)
	if written == 0 {
		return nil
	}
	if written < size {
		writtenN, ok := goIntFromCSize(written, "request headers written")
		if !ok {
			return nil
		}
		return buf[:writtenN]
	}
	return buf
}

// goStringFromC copies n bytes at ptr into a Go-owned string. Used by
// Request.URL() to materialize the URL bytes uWS handed us at handler entry
// without a cgo round-trip back into uWS. Returns "" when n <= 0 so callers
// don't have to special-case the empty case.
func goStringFromC(ptr unsafe.Pointer, n int) string {
	if n <= 0 || ptr == nil {
		return ""
	}
	if n > maxInt32 {
		reportPanic(fmt.Errorf("gogo: native string length %d exceeds C.int max", n))
		return ""
	}
	return C.GoStringN((*C.char)(ptr), C.int(n))
}

// remoteAddrFromPtr is the cgo-free-of-export Go wrapper around uWS's
// HttpResponse::getRemoteAddressAsText. types.go's Request.IP() calls
// this lazily because most handlers don't read the peer address —
// paying a cgo round-trip on demand beats pre-caching it on every
// request.
func remoteAddrFromPtr(res unsafe.Pointer) string {
	if res == nil {
		return ""
	}
	return responseNative{ptr: (*C.uwsgo_res_t)(res)}.remoteAddr()
}

func readNativeString(read func(*C.char, C.size_t) C.size_t) string {
	size := read(nil, 0)
	if size == 0 {
		return ""
	}

	n, ok := goIntFromCSize(size, "native string")
	if !ok {
		return ""
	}
	buf := make([]byte, n)
	read((*C.char)(unsafe.Pointer(&buf[0])), size)
	return string(buf)
}

func goBytesFromC(ptr unsafe.Pointer, size C.size_t, label string) ([]byte, bool) {
	if size == 0 {
		return nil, true
	}
	if ptr == nil {
		reportPanic(fmt.Errorf("gogo: %s has nil data pointer with length %d", label, uint64(size)))
		return nil, false
	}
	n, ok := cgoCopyLen(size, label)
	if !ok {
		return nil, false
	}
	return C.GoBytes(ptr, n), true
}

func goStringFromCSize(ptr *C.char, size C.size_t, label string) string {
	if size == 0 || ptr == nil {
		return ""
	}
	n, ok := cgoCopyLen(size, label)
	if !ok {
		return ""
	}
	return C.GoStringN(ptr, n)
}

func unsafeStringData(s string) *C.char {
	if len(s) == 0 {
		return nil
	}

	return (*C.char)(unsafe.Pointer(unsafe.StringData(s)))
}

func unsafeByteData(b []byte) *C.char {
	if len(b) == 0 {
		return nil
	}

	return (*C.char)(unsafe.Pointer(unsafe.SliceData(b)))
}

func (ws websocketNative) send(message []byte, opcode OpCode) bool {
	return C.uwsgo_ws_send(ws.ptr, unsafeByteData(message), C.size_t(len(message)), cIntFromInt(int(opcode))) != 0
}

func (ws websocketNative) sendString(message string, opcode OpCode) bool {
	return C.uwsgo_ws_send(ws.ptr, unsafeStringData(message), C.size_t(len(message)), cIntFromInt(int(opcode))) != 0
}

func (ws websocketNative) end(code int, message string) {
	C.uwsgo_ws_end(ws.ptr, cIntFromInt(code), unsafeStringData(message), C.size_t(len(message)))
}

func (ws websocketNative) subscribe(topic string) bool {
	return C.uwsgo_ws_subscribe(ws.ptr, unsafeStringData(topic), C.size_t(len(topic))) != 0
}

func (ws websocketNative) unsubscribe(topic string) bool {
	return C.uwsgo_ws_unsubscribe(ws.ptr, unsafeStringData(topic), C.size_t(len(topic))) != 0
}

func (ws websocketNative) publish(topic string, message []byte, opcode OpCode) bool {
	return C.uwsgo_ws_publish(ws.ptr,
		unsafeStringData(topic), C.size_t(len(topic)),
		unsafeByteData(message), C.size_t(len(message)),
		cIntFromInt(int(opcode))) != 0
}

func (a *appNative) publish(topic string, message []byte, opcode OpCode) {
	C.uwsgo_app_publish(a.ptr,
		unsafeStringData(topic), C.size_t(len(topic)),
		unsafeByteData(message), C.size_t(len(message)),
		cIntFromInt(int(opcode)))
}

// publishBatch packs N (topic, message, opcode) tuples into one
// contiguous byte buffer plus a parallel array of POD offset/length
// items, then crosses into C++ once. The struct array contains only
// integers (no Go pointers), so passing &items[0] to C doesn't
// violate cgo's "Go pointer to Go memory that contains Go pointers"
// rule.
func (a *appNative) publishBatch(msgs []PublishMessage) {
	if len(msgs) == 0 {
		return
	}
	var totalBytes int
	for i := range msgs {
		topicLen := len(msgs[i].Topic)
		messageLen := len(msgs[i].Message)
		if topicLen > maxGoInt-messageLen {
			reportPanic(fmt.Errorf("gogo: publish batch payload length overflows Go int"))
			return
		}
		add := topicLen + messageLen
		if totalBytes > maxGoInt-add {
			reportPanic(fmt.Errorf("gogo: publish batch payload length overflows Go int"))
			return
		}
		totalBytes += add
	}
	buf := make([]byte, totalBytes)
	items := make([]C.uwsgo_batch_item_t, len(msgs))
	off := 0
	for i := range msgs {
		items[i].topic_off = C.size_t(off)
		items[i].topic_len = C.size_t(len(msgs[i].Topic))
		copy(buf[off:], msgs[i].Topic)
		off += len(msgs[i].Topic)
		items[i].message_off = C.size_t(off)
		items[i].message_len = C.size_t(len(msgs[i].Message))
		copy(buf[off:], msgs[i].Message)
		off += len(msgs[i].Message)
		items[i].opcode = cIntFromInt(int(msgs[i].OpCode))
	}
	var bufPtr *C.char
	if totalBytes > 0 {
		bufPtr = (*C.char)(unsafe.Pointer(&buf[0]))
	}
	C.uwsgo_app_publish_batch(
		a.ptr,
		bufPtr, C.size_t(totalBytes),
		(*C.uwsgo_batch_item_t)(unsafe.Pointer(&items[0])), C.size_t(len(items)))
}

//export uwsgoHandleHTTP
func uwsgoHandleHTTP(handlerID C.uintptr_t, res *C.uwsgo_res_t, req *C.uwsgo_req_t,
	methodPtr *C.char, methodLen C.size_t,
	urlPtr *C.char, urlLen C.size_t,
	queryPtr *C.char, queryLen C.size_t,
	headersBlobPtr *C.char, headersLen C.size_t, headersComplete C.int,
	p0Ptr *C.char, p0Len C.size_t,
	p1Ptr *C.char, p1Len C.size_t,
	p2Ptr *C.char, p2Len C.size_t,
	p3Ptr *C.char, p3Len C.size_t) {
	handle := cgo.Handle(handlerID)
	handler := handle.Value().(Handler)

	reqWrap := requestPool.Get().(*Request)
	reqWrap.inner = requestNative{ptr: req}
	// uWS already had method / URL / query / first 4 params parsed and
	// a packed headers blob built; C++ passes the std::string_view
	// pointers (and the blob pointer) into uWS's request buffer so
	// Request's accessors can materialize lazily without a cgo round-
	// trip. The pointers are valid for the lifetime of this callback
	// (= the lifetime of reqWrap before it returns to the pool).
	reqWrap.syncMethodPtr = unsafe.Pointer(methodPtr)
	reqWrap.syncMethodLen = goIntFromCSizeHot(methodLen)
	reqWrap.syncURLPtr = unsafe.Pointer(urlPtr)
	reqWrap.syncURLLen = goIntFromCSizeHot(urlLen)
	reqWrap.syncQueryPtr = unsafe.Pointer(queryPtr)
	reqWrap.syncQueryLen = goIntFromCSizeHot(queryLen)
	reqWrap.syncHeadersPtr = unsafe.Pointer(headersBlobPtr)
	reqWrap.syncHeadersLen = goIntFromCSizeHot(headersLen)
	reqWrap.syncHeadersComplete = headersComplete != 0
	reqWrap.syncParamPtrs[0] = unsafe.Pointer(p0Ptr)
	reqWrap.syncParamLens[0] = goIntFromCSizeHot(p0Len)
	reqWrap.syncParamPtrs[1] = unsafe.Pointer(p1Ptr)
	reqWrap.syncParamLens[1] = goIntFromCSizeHot(p1Len)
	reqWrap.syncParamPtrs[2] = unsafe.Pointer(p2Ptr)
	reqWrap.syncParamLens[2] = goIntFromCSizeHot(p2Len)
	reqWrap.syncParamPtrs[3] = unsafe.Pointer(p3Ptr)
	reqWrap.syncParamLens[3] = goIntFromCSizeHot(p3Len)
	// Store the live response pointer so req.IP() can lazily fetch the
	// peer address via cgo on demand.
	reqWrap.syncResPtr = unsafe.Pointer(res)

	resWrap := responsePool.Get().(*Response)
	resWrap.inner = responseNative{ptr: res}
	resWrap.async = nil
	// One ref for the main handler. Body() and Async() each take their own
	// additional ref; the wrapper is returned to the pool by whichever
	// releaseRef drops the count to zero.
	resWrap.refs.Store(1)

	// Back-pointer for req.Context() so a client abort propagates
	// cancellation into downstream context-aware calls. Set after
	// resWrap is acquired so the link is in place before user code
	// runs.
	reqWrap.res = resWrap

	defer func() {
		if recovered := recover(); recovered != nil {
			reportPanic(recovered)
			if resWrap.async == nil {
				resWrap.Send(500, "text/plain; charset=utf-8", "Internal Server Error\n")
			}
		}

		reqWrap.resetForPool()
		requestPool.Put(reqWrap)
		resWrap.releaseRef()
	}()

	handler(resWrap, reqWrap)
}

//export uwsgoHandleWSOpen
func uwsgoHandleWSOpen(handlerID C.uintptr_t, ws *C.uwsgo_ws_t) {
	handle := cgo.Handle(handlerID)
	behavior := handle.Value().(WebSocketBehavior)
	if behavior.Open != nil {
		wsWrap := &WebSocket{inner: websocketNative{ptr: ws}, app: behavior.app}
		defer func() {
			if recovered := recover(); recovered != nil {
				reportPanic(recovered)
			}
		}()
		behavior.Open(wsWrap)
	}
}

//export uwsgoHandleWSMessage
func uwsgoHandleWSMessage(handlerID C.uintptr_t, ws *C.uwsgo_ws_t, message *C.char, messageLen C.size_t, opcode C.int) {
	handle := cgo.Handle(handlerID)
	behavior := handle.Value().(WebSocketBehavior)
	if behavior.Message != nil {
		msg, ok := goBytesFromC(unsafe.Pointer(message), messageLen, "WebSocket message")
		if !ok {
			return
		}
		wsWrap := &WebSocket{inner: websocketNative{ptr: ws}, app: behavior.app}
		initWSHubSocket(wsWrap)
		defer func() {
			if recovered := recover(); recovered != nil {
				reportPanic(recovered)
			}
		}()
		behavior.Message(
			wsWrap,
			msg,
			OpCode(opcode),
		)
	}
}

//export uwsgoHandleWSClose
func uwsgoHandleWSClose(handlerID C.uintptr_t, ws *C.uwsgo_ws_t, code C.int, message *C.char, messageLen C.size_t) {
	handle := cgo.Handle(handlerID)
	behavior := handle.Value().(WebSocketBehavior)
	wsWrap := &WebSocket{inner: websocketNative{ptr: ws}, app: behavior.app}
	initWSHubSocket(wsWrap)
	// Release any cgo.Handle that an Upgrade callback or
	// SetUserData attached to this socket — once the connection is
	// gone the held Go value can be garbage collected.
	releaseUserDataOnClose(wsWrap)
	if behavior.Close != nil {
		msg, ok := goBytesFromC(unsafe.Pointer(message), messageLen, "WebSocket close message")
		if !ok {
			return
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				reportPanic(recovered)
			}
		}()
		behavior.Close(
			wsWrap,
			int(code),
			msg,
		)
	}
}

//export uwsgoHandleWSUpgrade
func uwsgoHandleWSUpgrade(handlerID C.uintptr_t,
	ctxPtr unsafe.Pointer,
	method *C.char, methodLen C.size_t,
	url *C.char, urlLen C.size_t,
	query *C.char, queryLen C.size_t,
	ip *C.char, ipLen C.size_t,
	headersBlob *C.char, headersLen C.size_t,
	secProtoOffered *C.char, secProtoLen C.size_t) {
	handle := cgo.Handle(handlerID)
	behavior := handle.Value().(WebSocketBehavior)

	headerCopy, ok := goBytesFromC(unsafe.Pointer(headersBlob), headersLen, "WebSocket upgrade headers")
	if !ok {
		return
	}
	handleWSUpgradeFromCgo(
		behavior,
		uintptr(ctxPtr),
		goStringFromCSize(method, methodLen, "WebSocket upgrade method"),
		goStringFromCSize(url, urlLen, "WebSocket upgrade URL"),
		goStringFromCSize(query, queryLen, "WebSocket upgrade query"),
		goStringFromCSize(ip, ipLen, "WebSocket upgrade IP"),
		headerCopy,
		goStringFromCSize(secProtoOffered, secProtoLen, "WebSocket upgrade protocols"),
	)
}

// upgradeAccept and upgradeReject are the Go-side wrappers around
// uwsgo_res_upgrade_{accept,reject}. The C++ side stores the
// UpgradeCtx on the loop thread; these functions ferry the
// parameters across cgo. Always called synchronously from inside
// the upgrade callback (never from a worker goroutine).
func upgradeAccept(ctxPtr uintptr, protocol string, userData uintptr) {
	var protoPtr *C.char
	if len(protocol) > 0 {
		protoPtr = unsafeStringData(protocol)
	}
	C.uwsgo_res_upgrade_accept(
		unsafe.Pointer(ctxPtr),
		protoPtr, C.size_t(len(protocol)),
		C.uintptr_t(userData),
	)
}

func upgradeReject(ctxPtr uintptr, statusLine, body string) {
	var bodyPtr *C.char
	if len(body) > 0 {
		bodyPtr = unsafeStringData(body)
	}
	C.uwsgo_res_upgrade_reject(
		unsafe.Pointer(ctxPtr),
		unsafeStringData(statusLine), C.size_t(len(statusLine)),
		bodyPtr, C.size_t(len(body)),
	)
}

func wsGetUserData(w *WebSocket) uintptr {
	return uintptr(C.uwsgo_ws_user_data(w.inner.ptr))
}

func wsSetUserData(w *WebSocket, data uintptr) {
	C.uwsgo_ws_set_user_data(w.inner.ptr, C.uintptr_t(data))
}

func wsNativeKey(w *WebSocket) uintptr {
	if w == nil {
		return 0
	}
	return uintptr(unsafe.Pointer(w.inner.ptr))
}

//export uwsgoHandleDefer
func uwsgoHandleDefer(callbackID C.uintptr_t) {
	h := cgo.Handle(callbackID)
	fn := h.Value().(func())
	h.Delete()
	defer func() {
		if recovered := recover(); recovered != nil {
			reportPanic(recovered)
		}
	}()
	fn()
}

//export uwsgoHandleAborted
func uwsgoHandleAborted(callbackID C.uintptr_t) {
	h := cgo.Handle(callbackID)
	value := h.Value()
	h.Delete()
	switch cb := value.(type) {
	case *Aborted:
		cb.state.Store(true)
	case func():
		defer func() {
			if recovered := recover(); recovered != nil {
				reportPanic(recovered)
			}
		}()
		cb()
	}
}

//export uwsgoHandleCork
func uwsgoHandleCork(callbackID C.uintptr_t) {
	h := cgo.Handle(callbackID)
	fn := h.Value().(func())
	h.Delete()
	defer func() {
		if recovered := recover(); recovered != nil {
			reportPanic(recovered)
		}
	}()
	fn()
}

//export uwsgoHandleDrain
func uwsgoHandleDrain(callbackID C.uintptr_t) {
	h := cgo.Handle(callbackID)
	value := h.Value()
	h.Delete()
	switch cb := value.(type) {
	case func():
		defer func() {
			if recovered := recover(); recovered != nil {
				reportPanic(recovered)
			}
		}()
		cb()
	case chan struct{}:
		// Non-blocking close so a re-arm-after-cancel path that
		// already drained doesn't deadlock; subsequent close
		// panics are recovered above.
		defer func() { _ = recover() }()
		close(cb)
	}
}

//export uwsgoReleaseHandle
func uwsgoReleaseHandle(callbackID C.uintptr_t) {
	cgo.Handle(callbackID).Delete()
}

//export uwsgoHandleData
func uwsgoHandleData(callbackID C.uintptr_t, data *C.char, size C.size_t, isLast C.int) {
	h := cgo.Handle(callbackID)
	fn := h.Value().(func([]byte, bool))
	// Copy the chunk into Go memory — uWS reuses its buffer after this call.
	//
	// uWS's read chunks are bounded by its per-connection backpressure
	// (default 64 KiB) and the kernel socket buffer, so C.size_t fitting
	// in a C.int is overwhelmingly the common case. We still bounds-check
	// explicitly because a silent truncation here would copy only part
	// of the buffer into Go memory while the caller thinks all bytes
	// arrived — defense-in-depth costs one branch on the cold path.
	n, ok := cgoCopyLen(size, "request body chunk")
	if !ok {
		if isLast != 0 {
			h.Delete()
		}
		return
	}
	var chunk []byte
	if size > 0 {
		chunk = C.GoBytes(unsafe.Pointer(data), n)
	}
	last := isLast != 0
	if last {
		h.Delete()
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			reportPanic(recovered)
		}
	}()
	fn(chunk, last)
}

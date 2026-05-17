//go:build cgo && gogo

package gogo

/*
#cgo CXXFLAGS: -std=c++20 -I${SRCDIR}/third_party/uWebSockets/src -I${SRCDIR}/third_party/uWebSockets/uSockets/src
#cgo LDFLAGS: ${SRCDIR}/third_party/uWebSockets/uSockets/uSockets.a -lz
#cgo linux LDFLAGS: -pthread
#include <stdlib.h>
#include "uws_bridge.h"
*/
import "C"

import (
	"runtime"
	"runtime/cgo"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type appNative struct {
	ptr     *C.uwsgo_app_t
	handles []cgo.Handle
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

func newAppNative() (appNative, error) {
	return appNative{ptr: C.uwsgo_app_new()}, nil
}

func (a *appNative) get(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))

	C.uwsgo_app_get(a.ptr, cpattern, C.uintptr_t(handle))
}

// Shared-dispatch handler registry. C++ pushes the handler_id (a small int)
// into AsyncCtx; Go workers look it up here. Slots are append-only — handlers
// register at app setup time, never expire.
var (
	// sharedHandlers is append-only after registration. Registration takes
	// the mutex; workers read via an atomic snapshot to keep the request
	// hot path lock-free.
	sharedHandlers     []AsyncHandler
	sharedHandlersMu   sync.Mutex
	sharedHandlersSnap atomic.Pointer[[]AsyncHandler]
	sharedWorkersOnce  sync.Once
	sharedActive       atomic.Bool // true once a Shared route has been registered
)

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
	// Publish a fresh snapshot so workers see the new handler via an atomic
	// pointer swap — no lock acquired on the request hot path.
	snap := append([]AsyncHandler(nil), sharedHandlers...)
	sharedHandlersSnap.Store(&snap)
	return uint32(len(sharedHandlers) - 1)
}

func (a *appNative) getShared(pattern string, handler AsyncHandler) {
	id := registerSharedHandler(handler)
	cpattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cpattern))
	C.uwsgo_app_get_shared(a.ptr, cpattern, C.uint32_t(id))
	sharedActive.Store(true)
	// Lazily start worker goroutines on first shared route registration.
	ensureSharedWorkers()
}

// workerCount controls how many goroutines drain the request ring. Read once
// when the first shared route registers (and workers spin up); changing it
// after that has no effect. Default = NumCPU — spinning workers compete
// with the uWS loop thread on GOMAXPROCS, so over-subscribing tanks sync
// route latency. For workloads dominated by slow IO, raise this via
// SetWorkerCount before the first GetAsync registers.
var workerCount atomic.Int32

// SetWorkerCount configures the shared-dispatch worker pool size. Call
// before registering any GetAsync route — calls after the pool starts
// are no-ops. Pass 0 to restore the default (NumCPU).
func SetWorkerCount(n int) {
	if n < 0 {
		n = 0
	}
	workerCount.Store(int32(n))
}

func ensureSharedWorkers() {
	sharedWorkersOnce.Do(func() {
		n := int(workerCount.Load())
		if n == 0 {
			n = runtime.NumCPU()
		}
		for i := 0; i < n; i++ {
			go sharedWorker()
		}
	})
}

// sharedWorker polls the request ring with adaptive back-off. Spin a handful
// of iterations, then yield via Gosched, then sleep progressively longer up
// to a cap. At sustained load the spin path catches work immediately; idle
// workers settle to a cheap periodic wake.
//
// On handler panic, the worker recovers, sends a 500 response (if the
// response hasn't already been written), releases the ctx, and continues
// the loop — a single bad request never tears down a worker.
func sharedWorker() {
	const spinLimit = 256

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
			if spins > spinLimit {
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
		handlerID := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxHandlerIDOff))
		// Mark slot empty for the next generation of producers (idx wraps in
		// ringSize, so the next producer for this slot waits for idx+ringSize).
		seqAddr.Store(idx + uint64(shared.ringMask) + 1)

		// Read the handler from the atomic snapshot — append-only after
		// registration, so no lock on the hot path.
		snap := sharedHandlersSnap.Load()
		handler := (*snap)[handlerID]

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

	// Build the request snapshot from ctx memory. C++ has already copied the
	// fields it could into AsyncCtx; we copy out to Go-owned strings/bytes so
	// the snapshot survives past ctx release.
	reqWrap := requestPool.Get().(*Request)
	reqWrap.snap = newSnapshotFromCtx(ctxPtr)

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
	}()

	handler(resWrap, reqWrap)
}

// newSnapshotFromCtx reads the request-snapshot fields C++ wrote into the
// AsyncCtx and returns a Go-side requestSnapshot whose strings/bytes do not
// alias ctx memory — so the snapshot stays valid after ctx is released.
func newSnapshotFromCtx(ctxPtr uintptr) *requestSnapshot {
	methodLen := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxMethodLenOff))
	urlLen := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxURLLenOff))
	queryLen := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxQueryLenOff))
	ipLen := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxIPLenOff))
	paramCount := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxParamCountOff))
	headersLen := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxHeadersLenOff))
	truncated := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxTruncatedOff)) != 0

	snap := &requestSnapshot{
		method:    copyAt(ctxPtr+shared.ctxMethodOff, int(methodLen)),
		url:       copyAt(ctxPtr+shared.ctxURLOff, int(urlLen)),
		query:     copyAt(ctxPtr+shared.ctxQueryOff, int(queryLen)),
		ip:        copyAt(ctxPtr+shared.ctxIPOff, int(ipLen)),
		truncated: truncated,
	}

	if paramCount > 0 {
		paramsBase := ctxPtr + shared.ctxParamsOff
		paramLensBase := ctxPtr + shared.ctxParamLensOff
		params := make([]string, paramCount)
		for i := uint32(0); i < paramCount; i++ {
			plen := *(*uint32)(unsafe.Pointer(paramLensBase + uintptr(i)*unsafe.Sizeof(uint32(0))))
			params[i] = copyAt(paramsBase+uintptr(i)*shared.snapParamCap, int(plen))
		}
		snap.params = params
	}

	if headersLen > 0 {
		hdrs := make([]byte, headersLen)
		copy(hdrs, unsafe.Slice((*byte)(unsafe.Pointer(ctxPtr+shared.ctxHeadersOff)), int(headersLen)))
		snap.headers = hdrs
	}
	return snap
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

	C.uwsgo_app_get_static(a.ptr, cpattern,
		unsafeStringData(status), C.size_t(len(status)),
		unsafeStringData(contentType), C.size_t(len(contentType)),
		unsafeStringData(body), C.size_t(len(body)),
	)
}

func (a *appNative) post(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))

	C.uwsgo_app_post(a.ptr, cpattern, C.uintptr_t(handle))
}

func (a *appNative) any(pattern string, handler Handler) {
	cpattern, handle := a.prepareRoute(pattern, handler)
	defer C.free(unsafe.Pointer(cpattern))

	C.uwsgo_app_any(a.ptr, cpattern, C.uintptr_t(handle))
}

func (a *appNative) websocket(pattern string, behavior WebSocketBehavior) {
	cpattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cpattern))

	handle := cgo.NewHandle(behavior)
	a.handles = append(a.handles, handle)

	maxPayload := behavior.MaxPayloadLength
	if maxPayload <= 0 {
		maxPayload = 16 << 20 // 16 MiB
	}
	idleSec := int(behavior.IdleTimeout / time.Second)
	if idleSec <= 0 {
		idleSec = 120
	}
	maxBp := behavior.MaxBackpressure
	if maxBp <= 0 {
		maxBp = 64 << 10 // 64 KiB
	}
	pings := 1
	if behavior.DisablePings {
		pings = 0
	}

	C.uwsgo_app_ws(a.ptr, cpattern, C.uintptr_t(handle),
		C.size_t(maxPayload), C.int(idleSec), C.size_t(maxBp), C.int(pings))
}

func (a *appNative) prepareRoute(pattern string, handler Handler) (*C.char, cgo.Handle) {
	cpattern := C.CString(pattern)
	handle := cgo.NewHandle(handler)
	a.handles = append(a.handles, handle)
	return cpattern, handle
}

func (a appNative) listen(host string, port int) bool {
	var chost *C.char
	if host != "" {
		chost = C.CString(host)
		defer C.free(unsafe.Pointer(chost))
	}
	return C.uwsgo_app_listen(a.ptr, chost, C.int(port)) != 0
}

func (a appNative) setBodyLimit(limit int) {
	C.uwsgo_app_set_body_limit(a.ptr, C.size_t(limit))
}

func (a appNative) setCapturePeerIP(enable bool) {
	v := C.int(0)
	if enable {
		v = 1
	}
	C.uwsgo_app_set_capture_peer_ip(a.ptr, v)
}

func (a appNative) run() {
	C.uwsgo_app_run(a.ptr)
}

func (a appNative) stop() {
	C.uwsgo_app_stop(a.ptr)
}

func (a appNative) closeListen() {
	C.uwsgo_app_close_listen(a.ptr)
}

func (a *appNative) close() {
	if a.ptr == nil {
		return
	}

	C.uwsgo_app_free(a.ptr)
	a.ptr = nil

	for _, handle := range a.handles {
		handle.Delete()
	}

	a.handles = nil
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

func asyncCtxRelease(ctxHandle uintptr) {
	C.uwsgo_async_ctx_release(unsafe.Pointer(ctxHandle))
}

// sharedLayout caches struct offsets exposed by C so the hot path can build
// responses with plain unsafe.Pointer arithmetic and atomic ops, no cgo.
// Each AsyncCtx carries a pointer to its App's pending ring; asyncSendShared
// reads ctx->pending_ring rather than using a global so multiple App
// instances can coexist.
type sharedLayout struct {
	requestRing       uintptr
	ctxPendingRingOff uintptr
	ringMask          uint64
	slotsOffset       uintptr
	slotStride        uintptr
	slotSeqOffset     uintptr
	slotCtxOffset     uintptr
	headOffset        uintptr
	tailOffset        uintptr
	wakePendingOffset uintptr
	ctxStatusLenOff   uintptr
	ctxCtLenOff       uintptr
	ctxBodyLenOff     uintptr
	ctxStatusOff      uintptr
	ctxCtOff          uintptr
	ctxBodyOff        uintptr
	ctxHandlerIDOff   uintptr
	ctxResponseOff    uintptr
	ctxLoopOff        uintptr
	statusCap         uintptr
	ctCap             uintptr
	bodyCap           uintptr
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
	snapMethodCap    uintptr
	snapURLCap       uintptr
	snapQueryCap     uintptr
	snapIPCap        uintptr
	snapParamCap     uintptr
	snapParamMax     uintptr
	snapHeadersCap   uintptr
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
		requestRing:       uintptr(raw.request_ring),
		ctxPendingRingOff: uintptr(raw.ctx_pending_ring_offset),
		ringMask:          uint64(raw.ring_mask),
		slotsOffset:       uintptr(raw.ring_slots_offset),
		slotStride:        uintptr(raw.ring_slot_stride),
		slotSeqOffset:     uintptr(raw.ring_slot_seq_offset),
		slotCtxOffset:     uintptr(raw.ring_slot_ctx_offset),
		headOffset:        uintptr(raw.ring_head_offset),
		tailOffset:        uintptr(raw.ring_tail_offset),
		wakePendingOffset: uintptr(raw.ring_wake_pending_offset),
		ctxStatusLenOff:   uintptr(raw.ctx_status_len_offset),
		ctxCtLenOff:       uintptr(raw.ctx_ct_len_offset),
		ctxBodyLenOff:     uintptr(raw.ctx_body_len_offset),
		ctxStatusOff:      uintptr(raw.ctx_status_offset),
		ctxCtOff:          uintptr(raw.ctx_ct_offset),
		ctxBodyOff:        uintptr(raw.ctx_body_offset),
		ctxHandlerIDOff:   uintptr(raw.ctx_handler_id_offset),
		ctxResponseOff:    uintptr(raw.ctx_response_offset),
		ctxLoopOff:        uintptr(raw.ctx_loop_offset),
		statusCap:         uintptr(raw.ctx_inline_status_cap),
		ctCap:             uintptr(raw.ctx_inline_ct_cap),
		bodyCap:           uintptr(raw.ctx_inline_body_cap),

		ctxMethodLenOff:  uintptr(raw.ctx_method_len_offset),
		ctxURLLenOff:     uintptr(raw.ctx_url_len_offset),
		ctxQueryLenOff:   uintptr(raw.ctx_query_len_offset),
		ctxIPLenOff:      uintptr(raw.ctx_ip_len_offset),
		ctxParamCountOff: uintptr(raw.ctx_param_count_offset),
		ctxHeadersLenOff: uintptr(raw.ctx_headers_len_offset),
		ctxTruncatedOff:  uintptr(raw.ctx_truncated_offset),
		ctxParamLensOff:  uintptr(raw.ctx_param_lens_offset),
		ctxMethodOff:     uintptr(raw.ctx_method_offset),
		ctxURLOff:        uintptr(raw.ctx_url_offset),
		ctxQueryOff:      uintptr(raw.ctx_query_offset),
		ctxIPOff:         uintptr(raw.ctx_ip_offset),
		ctxParamsOff:     uintptr(raw.ctx_params_offset),
		ctxHeadersOff:    uintptr(raw.ctx_headers_offset),
		snapMethodCap:    uintptr(raw.ctx_snap_method_cap),
		snapURLCap:       uintptr(raw.ctx_snap_url_cap),
		snapQueryCap:     uintptr(raw.ctx_snap_query_cap),
		snapIPCap:        uintptr(raw.ctx_snap_ip_cap),
		snapParamCap:     uintptr(raw.ctx_snap_param_cap),
		snapParamMax:     uintptr(raw.ctx_snap_param_max),
		snapHeadersCap:   uintptr(raw.ctx_snap_headers_cap),
	}
	sharedReady = true
}

func (a appNative) startSharedDrain(intervalUs int) {
	C.uwsgo_app_start_drain(a.ptr, C.int(intervalUs))
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
		return false
	}
	loopPtr := *(*uintptr)(unsafe.Pointer(ctxHandle + shared.ctxLoopOff))

	// MPSC enqueue: claim a ready slot with CAS. If the response ring is full
	// or heavily contended, return false so the caller can fall back to
	// Loop::defer instead of spinning unbounded on a worker goroutine.
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
			return false
		default:
			tail = tailAddr.Load()
		}
		if spin > 100000 {
			return false
		}
		if spin%256 == 0 {
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

// maxSyncHeadersBytes caps the buffer allocation for the sync-mode header
// snapshot. Matches the SNAP_HEADERS_CAP that bounds the shared / async
// path on the C++ side so callers see consistent behavior across modes.
// uWS's own default request-buffer is ~8 KiB, so this is the natural
// ceiling for header data we'd ever see in practice.
const maxSyncHeadersBytes = 8 << 10

// headersAll returns the request headers in the "name\0value\0..." format
// used by requestSnapshot. Allocates and copies — only callable while the
// uWS HttpRequest is still live. Returns the buffer truncated to
// maxSyncHeadersBytes if uWS reports more bytes than that — the
// readNativeString-style two-pass dance lets us pass the cap to C so the
// second call writes only what fits.
func (r requestNative) headersAll() []byte {
	size := C.uwsgo_req_headers_all(r.ptr, nil, 0)
	if size == 0 {
		return nil
	}
	n := int(size)
	if n > maxSyncHeadersBytes {
		n = maxSyncHeadersBytes
	}
	buf := make([]byte, n)
	C.uwsgo_req_headers_all(r.ptr, (*C.char)(unsafe.Pointer(&buf[0])), C.size_t(n))
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

	buf := make([]byte, int(size))
	read((*C.char)(unsafe.Pointer(&buf[0])), size)
	return string(buf)
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
	return C.uwsgo_ws_send(ws.ptr, unsafeByteData(message), C.size_t(len(message)), C.int(opcode)) != 0
}

func (ws websocketNative) sendString(message string, opcode OpCode) bool {
	return C.uwsgo_ws_send(ws.ptr, unsafeStringData(message), C.size_t(len(message)), C.int(opcode)) != 0
}

func (ws websocketNative) end(code int, message string) {
	C.uwsgo_ws_end(ws.ptr, C.int(code), unsafeStringData(message), C.size_t(len(message)))
}

//export uwsgoHandleHTTP
func uwsgoHandleHTTP(handlerID C.uintptr_t, res *C.uwsgo_res_t, req *C.uwsgo_req_t,
	methodPtr *C.char, methodLen C.size_t,
	urlPtr *C.char, urlLen C.size_t,
	queryPtr *C.char, queryLen C.size_t,
	p0Ptr *C.char, p0Len C.size_t,
	p1Ptr *C.char, p1Len C.size_t,
	p2Ptr *C.char, p2Len C.size_t,
	p3Ptr *C.char, p3Len C.size_t) {
	handle := cgo.Handle(handlerID)
	handler := handle.Value().(Handler)

	reqWrap := requestPool.Get().(*Request)
	reqWrap.inner = requestNative{ptr: req}
	// uWS already had method / URL / query / first 4 params parsed; C++
	// passes the std::string_view pointers into uWS's request buffer so
	// Request's accessors can materialize lazily without a cgo round-
	// trip. The pointers are valid for the lifetime of this callback
	// (= the lifetime of reqWrap before it returns to the pool).
	reqWrap.syncMethodPtr = unsafe.Pointer(methodPtr)
	reqWrap.syncMethodLen = int(methodLen)
	reqWrap.syncURLPtr = unsafe.Pointer(urlPtr)
	reqWrap.syncURLLen = int(urlLen)
	reqWrap.syncQueryPtr = unsafe.Pointer(queryPtr)
	reqWrap.syncQueryLen = int(queryLen)
	reqWrap.syncParamPtrs[0] = unsafe.Pointer(p0Ptr)
	reqWrap.syncParamLens[0] = int(p0Len)
	reqWrap.syncParamPtrs[1] = unsafe.Pointer(p1Ptr)
	reqWrap.syncParamLens[1] = int(p1Len)
	reqWrap.syncParamPtrs[2] = unsafe.Pointer(p2Ptr)
	reqWrap.syncParamLens[2] = int(p2Len)
	reqWrap.syncParamPtrs[3] = unsafe.Pointer(p3Ptr)
	reqWrap.syncParamLens[3] = int(p3Len)
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
		defer func() {
			if recovered := recover(); recovered != nil {
				reportPanic(recovered)
			}
		}()
		behavior.Open(&WebSocket{inner: websocketNative{ptr: ws}})
	}
}

//export uwsgoHandleWSMessage
func uwsgoHandleWSMessage(handlerID C.uintptr_t, ws *C.uwsgo_ws_t, message *C.char, messageLen C.size_t, opcode C.int) {
	handle := cgo.Handle(handlerID)
	behavior := handle.Value().(WebSocketBehavior)
	if behavior.Message != nil {
		defer func() {
			if recovered := recover(); recovered != nil {
				reportPanic(recovered)
			}
		}()
		behavior.Message(
			&WebSocket{inner: websocketNative{ptr: ws}},
			C.GoBytes(unsafe.Pointer(message), C.int(messageLen)),
			OpCode(opcode),
		)
	}
}

//export uwsgoHandleWSClose
func uwsgoHandleWSClose(handlerID C.uintptr_t, ws *C.uwsgo_ws_t, code C.int, message *C.char, messageLen C.size_t) {
	handle := cgo.Handle(handlerID)
	behavior := handle.Value().(WebSocketBehavior)
	if behavior.Close != nil {
		defer func() {
			if recovered := recover(); recovered != nil {
				reportPanic(recovered)
			}
		}()
		behavior.Close(
			&WebSocket{inner: websocketNative{ptr: ws}},
			int(code),
			C.GoBytes(unsafe.Pointer(message), C.int(messageLen)),
		)
	}
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

//export uwsgoReleaseHandle
func uwsgoReleaseHandle(callbackID C.uintptr_t) {
	cgo.Handle(callbackID).Delete()
}

//export uwsgoHandleData
func uwsgoHandleData(callbackID C.uintptr_t, data *C.char, size C.size_t, isLast C.int) {
	h := cgo.Handle(callbackID)
	fn := h.Value().(func([]byte, bool))
	// Copy the chunk into Go memory — uWS reuses its buffer after this call.
	var chunk []byte
	if size > 0 {
		chunk = C.GoBytes(unsafe.Pointer(data), C.int(size))
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

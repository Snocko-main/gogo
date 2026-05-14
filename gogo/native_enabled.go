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
			panicHandler := getPanicHandler()
			if panicHandler != nil {
				panicHandler(r)
			}
			if !a.sent {
				// Best-effort 500 so the client doesn't hang. Body is left
				// minimal so we don't risk another panic during marshaling.
				resWrap.SendShared(500, "text/plain; charset=utf-8", "Internal Server Error\n")
			}
		}
		if !a.sent {
			asyncCtxRelease(ctxPtr)
		}
		reqWrap.snap = nil
		requestPool.Put(reqWrap)
		resWrap.recycleAsync(a)
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
	paramCount := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxParamCountOff))
	headersLen := *(*uint32)(unsafe.Pointer(ctxPtr + shared.ctxHeadersLenOff))

	snap := &requestSnapshot{
		method: copyAt(ctxPtr+shared.ctxMethodOff, int(methodLen)),
		url:    copyAt(ctxPtr+shared.ctxURLOff, int(urlLen)),
		query:  copyAt(ctxPtr+shared.ctxQueryOff, int(queryLen)),
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

	C.uwsgo_app_ws(a.ptr, cpattern, C.uintptr_t(handle))
}

func (a *appNative) prepareRoute(pattern string, handler Handler) (*C.char, cgo.Handle) {
	cpattern := C.CString(pattern)
	handle := cgo.NewHandle(handler)
	a.handles = append(a.handles, handle)
	return cpattern, handle
}

func (a appNative) listen(port int) bool {
	return C.uwsgo_app_listen(a.ptr, C.int(port)) != 0
}

func (a appNative) run() {
	C.uwsgo_app_run(a.ptr)
}

func (a appNative) stop() {
	C.uwsgo_app_stop(a.ptr)
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

func (r responseNative) onAborted(state *Aborted) {
	handle := cgo.NewHandle(state)
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
type sharedLayout struct {
	ring             uintptr
	requestRing      uintptr
	ringMask         uint64
	slotsOffset      uintptr
	slotStride       uintptr
	slotSeqOffset    uintptr
	slotCtxOffset    uintptr
	headOffset       uintptr
	tailOffset       uintptr
	ctxStatusLenOff  uintptr
	ctxCtLenOff      uintptr
	ctxBodyLenOff    uintptr
	ctxStatusOff     uintptr
	ctxCtOff         uintptr
	ctxBodyOff       uintptr
	ctxHandlerIDOff  uintptr
	ctxResponseOff   uintptr
	ctxLoopOff       uintptr
	statusCap        uintptr
	ctCap            uintptr
	bodyCap          uintptr
	// Request snapshot offsets (populated by C++ before the ctx is enqueued).
	ctxMethodLenOff  uintptr
	ctxURLLenOff     uintptr
	ctxQueryLenOff   uintptr
	ctxParamCountOff uintptr
	ctxHeadersLenOff uintptr
	ctxParamLensOff  uintptr
	ctxMethodOff     uintptr
	ctxURLOff        uintptr
	ctxQueryOff      uintptr
	ctxParamsOff     uintptr
	ctxHeadersOff    uintptr
	snapMethodCap    uintptr
	snapURLCap       uintptr
	snapQueryCap     uintptr
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
		ring:            uintptr(raw.ring),
		requestRing:     uintptr(raw.request_ring),
		ringMask:        uint64(raw.ring_mask),
		slotsOffset:     uintptr(raw.ring_slots_offset),
		slotStride:      uintptr(raw.ring_slot_stride),
		slotSeqOffset:   uintptr(raw.ring_slot_seq_offset),
		slotCtxOffset:   uintptr(raw.ring_slot_ctx_offset),
		headOffset:      uintptr(raw.ring_head_offset),
		tailOffset:      uintptr(raw.ring_tail_offset),
		ctxStatusLenOff: uintptr(raw.ctx_status_len_offset),
		ctxCtLenOff:     uintptr(raw.ctx_ct_len_offset),
		ctxBodyLenOff:   uintptr(raw.ctx_body_len_offset),
		ctxStatusOff:    uintptr(raw.ctx_status_offset),
		ctxCtOff:        uintptr(raw.ctx_ct_offset),
		ctxBodyOff:      uintptr(raw.ctx_body_offset),
		ctxHandlerIDOff: uintptr(raw.ctx_handler_id_offset),
		ctxResponseOff:  uintptr(raw.ctx_response_offset),
		ctxLoopOff:      uintptr(raw.ctx_loop_offset),
		statusCap:       uintptr(raw.ctx_inline_status_cap),
		ctCap:           uintptr(raw.ctx_inline_ct_cap),
		bodyCap:         uintptr(raw.ctx_inline_body_cap),

		ctxMethodLenOff:  uintptr(raw.ctx_method_len_offset),
		ctxURLLenOff:     uintptr(raw.ctx_url_len_offset),
		ctxQueryLenOff:   uintptr(raw.ctx_query_len_offset),
		ctxParamCountOff: uintptr(raw.ctx_param_count_offset),
		ctxHeadersLenOff: uintptr(raw.ctx_headers_len_offset),
		ctxParamLensOff:  uintptr(raw.ctx_param_lens_offset),
		ctxMethodOff:     uintptr(raw.ctx_method_offset),
		ctxURLOff:        uintptr(raw.ctx_url_offset),
		ctxQueryOff:      uintptr(raw.ctx_query_offset),
		ctxParamsOff:     uintptr(raw.ctx_params_offset),
		ctxHeadersOff:    uintptr(raw.ctx_headers_offset),
		snapMethodCap:    uintptr(raw.ctx_snap_method_cap),
		snapURLCap:       uintptr(raw.ctx_snap_url_cap),
		snapQueryCap:     uintptr(raw.ctx_snap_query_cap),
		snapParamCap:     uintptr(raw.ctx_snap_param_cap),
		snapParamMax:     uintptr(raw.ctx_snap_param_max),
		snapHeadersCap:   uintptr(raw.ctx_snap_headers_cap),
	}
	sharedReady = true
}

func startSharedDrain(intervalUs int) {
	C.uwsgo_app_start_drain(C.int(intervalUs))
}

func (a appNative) startSharedDrain(intervalUs int) {
	startSharedDrain(intervalUs)
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

	// MPSC enqueue: fetch_add tail, wait for slot.sequence == idx, write ctx, sequence = idx+1
	tailAddr := (*atomic.Uint64)(unsafe.Pointer(shared.ring + shared.tailOffset))
	idx := tailAddr.Add(1) - 1
	slotBase := shared.ring + shared.slotsOffset + uintptr(idx&shared.ringMask)*shared.slotStride
	seqAddr := (*atomic.Uint64)(unsafe.Pointer(slotBase + shared.slotSeqOffset))

	for seqAddr.Load() != idx {
		// Ring is full — spin briefly waiting for consumer.
	}
	*(*uintptr)(unsafe.Pointer(slotBase + shared.slotCtxOffset)) = ctxHandle
	seqAddr.Store(idx + 1)

	// Wake the loop so it drains the ring immediately. Without this the
	// response waits up to 1 ms for the periodic drain timer to fire
	// (libuS timers are ms-granularity). One cgo crossing per response,
	// far cheaper than the ~0.5 ms average latency we'd otherwise eat.
	loopPtr := *(*uintptr)(unsafe.Pointer(ctxHandle + shared.ctxLoopOff))
	if loopPtr != 0 {
		C.uwsgo_wake_drain((*C.uwsgo_loop_t)(unsafe.Pointer(loopPtr)))
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

// headersAll returns the request headers in the "name\0value\0..." format
// used by requestSnapshot. Allocates and copies — only callable while the
// uWS HttpRequest is still live.
func (r requestNative) headersAll() []byte {
	size := C.uwsgo_req_headers_all(r.ptr, nil, 0)
	if size == 0 {
		return nil
	}
	buf := make([]byte, int(size))
	C.uwsgo_req_headers_all(r.ptr, (*C.char)(unsafe.Pointer(&buf[0])), size)
	return buf
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
func uwsgoHandleHTTP(handlerID C.uintptr_t, res *C.uwsgo_res_t, req *C.uwsgo_req_t) {
	handle := cgo.Handle(handlerID)
	handler := handle.Value().(Handler)

	reqWrap := requestPool.Get().(*Request)
	reqWrap.inner = requestNative{ptr: req}

	resWrap := responsePool.Get().(*Response)
	resWrap.inner = responseNative{ptr: res}
	resWrap.async = nil
	resWrap.bodyPending = false

	handler(resWrap, reqWrap)

	reqWrap.inner = requestNative{}
	requestPool.Put(reqWrap)

	// Sync responses are done with resWrap by now. We must NOT recycle if:
	//   - async is set: a goroutine still uses the wrapper
	//   - bodyPending is set: onData hasn't received the final chunk yet
	// Both paths take responsibility for their own recycle.
	if resWrap.async == nil && !resWrap.bodyPending {
		resWrap.inner = responseNative{}
		responsePool.Put(resWrap)
	}
}

//export uwsgoHandleWSOpen
func uwsgoHandleWSOpen(handlerID C.uintptr_t, ws *C.uwsgo_ws_t) {
	handle := cgo.Handle(handlerID)
	behavior := handle.Value().(WebSocketBehavior)
	if behavior.Open != nil {
		behavior.Open(&WebSocket{inner: websocketNative{ptr: ws}})
	}
}

//export uwsgoHandleWSMessage
func uwsgoHandleWSMessage(handlerID C.uintptr_t, ws *C.uwsgo_ws_t, message *C.char, messageLen C.size_t, opcode C.int) {
	handle := cgo.Handle(handlerID)
	behavior := handle.Value().(WebSocketBehavior)
	if behavior.Message != nil {
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
	fn()
}

//export uwsgoHandleAborted
func uwsgoHandleAborted(callbackID C.uintptr_t) {
	h := cgo.Handle(callbackID)
	state := h.Value().(*Aborted)
	h.Delete()
	state.state.Store(true)
}

//export uwsgoHandleCork
func uwsgoHandleCork(callbackID C.uintptr_t) {
	h := cgo.Handle(callbackID)
	fn := h.Value().(func())
	h.Delete()
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
	fn(chunk, last)
}

//go:build cgo && uwebsockets

package uwebsockets

/*
#cgo CXXFLAGS: -std=c++20 -I${SRCDIR}/third_party/uWebSockets/src -I${SRCDIR}/third_party/uWebSockets/uSockets/src
#cgo LDFLAGS: ${SRCDIR}/third_party/uWebSockets/uSockets/uSockets.a -lz
#cgo linux LDFLAGS: -pthread
#include <stdlib.h>
#include "uws_bridge.h"
*/
import "C"

import (
	"runtime/cgo"
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

func (l loopNative) defer_(fn func()) {
	handle := cgo.NewHandle(fn)
	C.uwsgo_loop_defer(l.ptr, C.uintptr_t(handle))
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

	handler(resWrap, reqWrap)

	reqWrap.inner = requestNative{}
	requestPool.Put(reqWrap)

	// Sync responses are done with resWrap by now; async ones keep using it
	// from a goroutine, so we cannot recycle until the async flush returns it.
	if resWrap.async == nil {
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

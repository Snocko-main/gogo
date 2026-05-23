//go:build !cgo || !gogo

package gogo

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"
)

// sharedActiveApps mirrors the symbol defined in native_enabled.go so
// NewApp / App.Close compile in stub builds. The counter is kept live
// (rather than dropped to a no-op) in case anything in the build-tag-
// agnostic code paths ever reads it; nothing currently does.
var sharedActiveApps atomic.Int32

func acquireSharedWorkerAppRef() {
	sharedActiveApps.Add(1)
}

// stopSharedWorkersIfIdle is a no-op in stub builds — there is no
// worker pool to tear down without the cgo dispatch ring. Defined so
// App.Close compiles when the framework is built without the gogo /
// cgo tags (e.g. `go vet`, IDE tooling, downstream users that import
// the package only for its types).
func stopSharedWorkersIfIdle() {}

// WaitForSharedWorkers always returns true in stub builds: there are
// no workers, so the pool is by definition drained the moment the
// caller asks. Keeps the public signature available across both
// builds so calling code doesn't have to use build tags.
func WaitForSharedWorkers(time.Duration) bool { return true }

func goStringFromC(_ unsafe.Pointer, _ int) string { return "" }
func remoteAddrFromPtr(_ unsafe.Pointer) string    { return "" }

var errNativeDisabled = errors.New("gogo native binding disabled: build with CGO_ENABLED=1 and -tags gogo")

type appNative struct{}
type responseNative struct{}
type requestNative struct{}
type websocketNative struct{}
type loopNative struct{}

func newAppNative() (appNative, error) {
	return appNative{}, errNativeDisabled
}

func (appNative) get(string, Handler)                      {}
func (appNative) getStatic(string, string, string, string) {}
func (appNative) getShared(string, AsyncHandler)           {}
func (appNative) postShared(string, AsyncHandler, int)     {}
func (appNative) post(string, Handler)                     {}
func (appNative) any(string, Handler)                      {}
func (appNative) put(string, Handler)                      {}
func (appNative) patch(string, Handler)                    {}
func (appNative) deleteM(string, Handler)                  {}
func (appNative) options(string, Handler)                  {}
func (appNative) head(string, Handler)                     {}
func (appNative) websocket(string, WebSocketBehavior)      {}
func (appNative) listen(string, int) bool                  { return false }
func (appNative) addChild(appNative)                       {}
func (appNative) setBodyLimit(int)                         {}
func (appNative) setCapturePeerIP(bool)                    {}
func (appNative) run()                                     {}
func (appNative) stop()                                    {}
func (appNative) closeListen()                             {}
func (appNative) close()                                   {}

func (responseNative) status(string)               {}
func (responseNative) header(string, string)       {}
func (responseNative) headersBatch([]byte, int)    {}
func (responseNative) write(string)                {}
func (responseNative) end(string)                  {}
func (responseNative) send(string, string, string) {}
func (responseNative) sendSplit(string, string, []byte, string, string) {
}
func (responseNative) loop() loopNative          { return loopNative{} }
func (responseNative) onAborted(any)             {}
func (responseNative) cork(func())               {}
func (responseNative) onData(func([]byte, bool)) {}
func (responseNative) remoteAddr() string        { return "" }

func (loopNative) defer_(func()) {}

func loopFromUintptr(uintptr) *Loop { return &Loop{} }

func (appNative) startSharedDrain(int)                     {}
func initSharedLayout()                                    {}
func asyncSendShared(uintptr, string, string, string) bool { return false }

func (responseNative) beginAsync() (uintptr, uintptr) { return 0, 0 }

func asyncDeferSend(uintptr, uintptr, string, string, string)                    {}
func asyncDeferSendWithHeaders(uintptr, uintptr, string, string, string, string) {}
func asyncDeferStreamStart(uintptr, uintptr, string, string, string)             {}
func asyncDeferStreamWrite(uintptr, uintptr, string)                             {}
func asyncDeferStreamEnd(uintptr, uintptr)                                       {}
func innerBufferedAmount(responseNative) uint64                                  { return 0 }
func asyncCtxAborted(ctxHandle uintptr) bool                                     { return ctxHandle == 0 }
func asyncCtxStreamPendingBytes(uintptr) uint64                                  { return 0 }
func asyncCtxRetain(uintptr)                                                     {}

func upgradeAccept(uintptr, string, uintptr) {}
func upgradeReject(uintptr, string, string)  {}
func wsGetUserData(*WebSocket) uintptr       { return 0 }
func wsSetUserData(*WebSocket, uintptr)      {}
func wsNativeKey(*WebSocket) uintptr         { return 0 }
func asyncCtxRelease(uintptr)                {}

func (requestNative) method() string           { return "" }
func (requestNative) url() string              { return "" }
func (requestNative) header(string) string     { return "" }
func (requestNative) parameter(int) string     { return "" }
func (requestNative) query() string            { return "" }
func (requestNative) queryParam(string) string { return "" }
func (requestNative) headersAll() []byte       { return nil }

func (websocketNative) send([]byte, OpCode) bool            { return false }
func (websocketNative) sendString(string, OpCode) bool      { return false }
func (websocketNative) end(int, string)                     {}
func (websocketNative) subscribe(string) bool               { return false }
func (websocketNative) unsubscribe(string) bool             { return false }
func (websocketNative) publish(string, []byte, OpCode) bool { return false }

func (appNative) publish(string, []byte, OpCode) {}
func (appNative) publishBatch([]PublishMessage)  {}

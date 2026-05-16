//go:build !cgo || !gogo

package gogo

import (
	"errors"
	"unsafe"
)

func goStringFromC(_ unsafe.Pointer, _ int) string { return "" }

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
func (appNative) post(string, Handler)                     {}
func (appNative) any(string, Handler)                      {}
func (appNative) websocket(string, WebSocketBehavior)      {}
func (appNative) listen(string, int) bool                  { return false }
func (appNative) setBodyLimit(int)                         {}
func (appNative) run()                                     {}
func (appNative) stop()                                    {}
func (appNative) close()                                   {}

func (responseNative) status(string)               {}
func (responseNative) header(string, string)       {}
func (responseNative) write(string)                {}
func (responseNative) end(string)                  {}
func (responseNative) send(string, string, string) {}
func (responseNative) loop() loopNative            { return loopNative{} }
func (responseNative) onAborted(any)               {}
func (responseNative) cork(func())                 {}
func (responseNative) onData(func([]byte, bool))   {}

func (loopNative) defer_(func()) {}

func (appNative) startSharedDrain(int)                     {}
func initSharedLayout()                                    {}
func asyncSendShared(uintptr, string, string, string) bool { return false }

func (responseNative) beginAsync() (uintptr, uintptr) { return 0, 0 }

func asyncDeferSend(uintptr, uintptr, string, string, string) {}
func asyncCtxRelease(uintptr)                                 {}

func (requestNative) method() string           { return "" }
func (requestNative) url() string              { return "" }
func (requestNative) header(string) string     { return "" }
func (requestNative) parameter(int) string     { return "" }
func (requestNative) query() string            { return "" }
func (requestNative) queryParam(string) string { return "" }
func (requestNative) headersAll() []byte       { return nil }

func (websocketNative) send([]byte, OpCode) bool       { return false }
func (websocketNative) sendString(string, OpCode) bool { return false }
func (websocketNative) end(int, string)                {}

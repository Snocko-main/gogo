//go:build !cgo || !uwebsockets

package uwebsockets

import "errors"

var errNativeDisabled = errors.New("uwebsockets native binding disabled: build with CGO_ENABLED=1 and -tags uwebsockets")

type appNative struct{}
type responseNative struct{}
type requestNative struct{}
type websocketNative struct{}

func newAppNative() (appNative, error) {
	return appNative{}, errNativeDisabled
}

func (appNative) get(string, Handler)                 {}
func (appNative) post(string, Handler)                {}
func (appNative) any(string, Handler)                 {}
func (appNative) websocket(string, WebSocketBehavior) {}
func (appNative) listen(int) bool                     { return false }
func (appNative) run()                                {}
func (appNative) close()                              {}

func (responseNative) status(string)         {}
func (responseNative) header(string, string) {}
func (responseNative) write(string)          {}
func (responseNative) end(string)            {}

func (requestNative) url() string          { return "" }
func (requestNative) header(string) string { return "" }
func (requestNative) parameter(int) string { return "" }

func (websocketNative) send([]byte, OpCode) bool       { return false }
func (websocketNative) sendString(string, OpCode) bool { return false }
func (websocketNative) end(int, string)                {}

// Package uwebsockets exposes a small Go binding for the uWebSockets C++ app.
//
// The native binding is opt-in because it requires vendored uWebSockets/uSockets
// sources and cgo:
//
//	go run -tags uwebsockets ./example/hello
//
// Without the uwebsockets build tag, the package builds a stub that returns a
// clear error from NewApp.
package uwebsockets

// WebSocket upgrade-time access: subprotocol negotiation, header
// inspection, per-socket user data attachment.

package gogo

import (
	"fmt"
	"runtime/cgo"
	"strings"
)

func defaultWebSocketUpgrade(ctx *UpgradeContext) {
	if ctx.Header("origin") != "" || ctx.Header("sec-websocket-origin") != "" {
		ctx.Reject(403, "origin not allowed")
		return
	}
	ctx.Accept("")
}

// UpgradeContext is passed to WebSocketBehavior.Upgrade for every
// incoming WebSocket handshake. It carries a snapshot of the
// request (URL, query, headers, peer IP, offered subprotocols) and
// exposes Accept / Reject to drive the response.
//
// Lifetime is the duration of the Upgrade callback only. Do NOT
// store the pointer; the underlying C++ context is freed the
// moment the callback returns.
type UpgradeContext struct {
	// Request snapshot — populated by the framework before
	// crossing the cgo boundary. All strings are Go-owned and
	// safe to keep beyond the callback.
	method      string
	url         string
	query       string
	ip          string
	headersBlob []byte
	protocols   []string

	// User data the callback wants attached to the resulting
	// WebSocket. Stashed via SetUserData, picked up by the
	// framework when Accept fires and turned into a cgo.Handle.
	userData any

	// Opaque C++ handle to the upgrade state. Pass-through to
	// uwsgo_res_upgrade_accept / uwsgo_res_upgrade_reject. Not
	// safe to dereference from Go.
	ctxPtr uintptr

	// done flips true after the first Accept or Reject. Further
	// calls are silent no-ops so the underlying C++ context's
	// done flag stays the single source of truth.
	done bool
}

// Method returns the HTTP method of the upgrade request — always
// "get" in practice for a real WebSocket handshake, but exposed
// for completeness.
func (c *UpgradeContext) Method() string { return c.method }

// URL returns the upgrade request URL path (no query string).
func (c *UpgradeContext) URL() string { return c.url }

// Query returns the raw query string portion of the URL (no
// leading '?'). Use QueryParam to read a specific key.
func (c *UpgradeContext) Query() string { return c.query }

// QueryParam reads a named query parameter. Linear scan; cache
// the result if you need it more than once.
func (c *UpgradeContext) QueryParam(name string) string {
	if c.query == "" || name == "" {
		return ""
	}
	return parseSingleQueryParam(c.query, name)
}

// IP returns the canonical peer IP address as a printable string.
func (c *UpgradeContext) IP() string { return c.ip }

// Header reads a request header by name (case-insensitive). The
// header blob is parsed on every call — cache if you need a
// header multiple times.
func (c *UpgradeContext) Header(name string) string {
	if name == "" || len(c.headersBlob) == 0 {
		return ""
	}
	needle := lowercaseAsciiString(name)
	buf := c.headersBlob
	for len(buf) > 0 {
		j := indexOfZero(buf)
		if j < 0 {
			return ""
		}
		key := buf[:j]
		buf = buf[j+1:]
		if len(buf) == 0 {
			return ""
		}
		j = indexOfZero(buf)
		if j < 0 {
			return ""
		}
		value := buf[:j]
		buf = buf[j+1:]
		if len(key) == len(needle) && bytesEqualLower(key, needle) {
			return string(value)
		}
	}
	return ""
}

// Protocols returns the list of subprotocols the client offered in
// the Sec-WebSocket-Protocol header. Whitespace around each entry
// is trimmed. Pick one with Accept(name) to echo it back; passing
// "" accepts without naming a protocol.
func (c *UpgradeContext) Protocols() []string {
	return c.protocols
}

// SetUserData stashes a Go value on the upgrade context. On Accept
// the framework wraps it in a cgo.Handle and attaches it to the
// resulting WebSocket so handlers can read it via ws.UserData().
// The handle is released automatically when the WebSocket closes.
//
// Calling SetUserData(nil) clears any previously-stashed value.
func (c *UpgradeContext) SetUserData(v any) {
	c.userData = v
}

// Accept completes the WebSocket handshake. protocol is the
// subprotocol to echo back in the Sec-WebSocket-Protocol response
// header — pass one of Protocols() or "" to skip negotiation.
//
// After Accept returns, the connection is live and the Open
// callback will fire shortly. Calling Accept more than once on
// the same context is a no-op.
func (c *UpgradeContext) Accept(protocol string) {
	if c.done {
		return
	}
	c.done = true
	if !validWebSocketSubprotocol(protocol) {
		upgradeReject(c.ctxPtr, statusLine(400), "invalid subprotocol")
		return
	}

	var handle uintptr
	if c.userData != nil {
		h := cgo.NewHandle(c.userData)
		handle = uintptr(h)
	}
	upgradeAccept(c.ctxPtr, protocol, handle)
}

// Reject refuses the upgrade with an HTTP status and plain-text
// body. The connection is closed; no open / message / close
// callbacks fire. Calling Reject more than once is a no-op.
//
//	if !validToken(ctx.Header("authorization")) {
//	    ctx.Reject(401, "unauthorized")
//	    return
//	}
func (c *UpgradeContext) Reject(status int, body string) {
	if c.done {
		return
	}
	c.done = true
	line := statusLine(status)
	upgradeReject(c.ctxPtr, line, body)
}

// parseSubprotocolList trims and splits a Sec-WebSocket-Protocol
// header value into individual protocol tokens. Empty entries are
// skipped.
func parseSubprotocolList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t == "" {
			continue
		}
		if !validWebSocketSubprotocol(t) {
			continue
		}
		out = append(out, t)
	}
	return out
}

func validWebSocketSubprotocol(protocol string) bool {
	if protocol == "" {
		return true
	}
	for i := 0; i < len(protocol); i++ {
		if !isHTTPTokenChar(protocol[i]) {
			return false
		}
	}
	return true
}

// lowercaseAsciiString is an allocation-friendly twin of
// lowercaseAscii that takes a string instead of building one.
// Used by UpgradeContext.Header to normalize the lookup key.
func lowercaseAsciiString(s string) string {
	var hasUpper bool
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			hasUpper = true
			break
		}
	}
	if !hasUpper {
		return s
	}
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

// UserData reads back the value stashed by an upgrade callback via
// UpgradeContext.SetUserData. Returns nil when no user data was
// attached. Safe to call from Open / Message / Close.
func (w *WebSocket) UserData() any {
	handle := wsGetUserData(w)
	if handle == 0 {
		return nil
	}
	return cgo.Handle(handle).Value()
}

// SetUserData overwrites the per-socket user-data value after the
// connection is established (e.g. from Open). Releases the
// previous handle if there was one; pass nil to clear without
// installing a replacement.
func (w *WebSocket) SetUserData(v any) {
	old := wsGetUserData(w)
	if old != 0 {
		cgo.Handle(old).Delete()
	}
	var handle uintptr
	if v != nil {
		handle = uintptr(cgo.NewHandle(v))
	}
	wsSetUserData(w, handle)
}

// releaseUserDataOnClose runs from the close handler — it
// reclaims any cgo.Handle that an upgrade callback or SetUserData
// stored on the socket so the held Go value can be garbage
// collected.
func releaseUserDataOnClose(w *WebSocket) {
	handle := wsGetUserData(w)
	if handle == 0 {
		return
	}
	wsSetUserData(w, 0)
	cgo.Handle(handle).Delete()
}

// handleWSUpgradeFromCgo is the Go-side entry point invoked by
// the C++ upgrade lambda. It builds an UpgradeContext from the
// snapshot the C++ side captured, calls the user's Upgrade
// callback, and propagates panics to the framework's panic
// handler. If the user callback returns without calling Accept or
// Reject, the C++ side falls back to a 500 — defended at both
// layers.
func handleWSUpgradeFromCgo(behavior WebSocketBehavior, ctxPtr uintptr,
	method, url, query, ip string, headersBlob []byte, offeredProtocols string) {
	if behavior.Upgrade == nil {
		// Should not happen — with_upgrade is only set when the
		// callback is non-nil. Treat as misuse and bail.
		upgradeReject(ctxPtr, statusLine(500), "no upgrade callback registered\n")
		return
	}
	ctx := &UpgradeContext{
		method:      method,
		url:         url,
		query:       query,
		ip:          normalizePeerIP(ip),
		headersBlob: headersBlob,
		protocols:   parseSubprotocolList(offeredProtocols),
		ctxPtr:      ctxPtr,
	}
	defer func() {
		if r := recover(); r != nil {
			reportPanic(fmt.Errorf("gogo: WebSocket upgrade callback panicked: %v", r))
			// Best-effort 500 so the socket doesn't leak when a
			// user callback panics before deciding.
			if !ctx.done {
				upgradeReject(ctxPtr, statusLine(500), "upgrade callback panicked\n")
			}
		}
	}()
	behavior.Upgrade(ctx)
}

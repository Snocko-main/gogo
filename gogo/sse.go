// Server-Sent Events — node-style ergonomics on top of Response.Stream.
//
// Typical usage:
//
//	app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
//	    res.SSE(func(s *gogo.SSEStream) error {
//	        s.Send("hello")                          // plain text
//	        s.Send(map[string]int{"n": 1})           // auto-JSON
//	        s.SendEvent(gogo.SSEEvent{
//	            ID:    "1",
//	            Event: "tick",
//	            Data:  map[string]int{"n": 1},
//	        })
//	        s.Comment("keepalive ping")              // : keepalive ping
//	        return nil
//	    })
//	})
//
// SSE auto-installs Content-Type: text/event-stream, Cache-Control:
// no-cache, Connection: keep-alive, and X-Accel-Buffering: no (so an
// nginx in front doesn't buffer the stream and break pushes). Then
// it delegates to Response.Stream, which means SSE is async-only —
// register the route via GetAsync / PostAsync.

package gogo

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// SSEEvent is one Server-Sent Events frame. Every field is
// optional; an SSEEvent with only Data set behaves the same as
// Send(data).
type SSEEvent struct {
	// ID is the event identifier (the `id:` field). Browsers
	// stash the last received ID and re-send it as the
	// Last-Event-ID header when they reconnect after a network
	// blip, so handlers can resume from that point.
	ID string

	// Event names the event type (the `event:` field). Defaults
	// to "message" in browser EventSource listeners; set this
	// when you want client code to listen for a specific name
	// via addEventListener.
	Event string

	// Data is the event payload (the `data:` field). Accepted:
	//   nil          — empty data
	//   string       — sent verbatim (multi-line strings auto-
	//                  split into per-line data: emissions)
	//   []byte       — same as string
	//   error        — err.Error() text
	//   anything else — json.Marshal'd
	Data any

	// Retry sets the `retry:` field (milliseconds). Tells the
	// browser EventSource to wait this long before reconnecting
	// after a disconnect. 0 omits the field (browser default,
	// ~3 s).
	Retry int
}

// SSEStream is the writer handed to the SSE callback. Send /
// SendEvent / Comment / Ping all write directly to the underlying
// HTTP stream — there's no batching, so a Send call equals one
// SSE frame on the wire.
type SSEStream struct {
	w io.Writer
}

// Send writes data as a one-line SSE frame with the default event
// name ("message"). Shorthand for SendEvent(SSEEvent{Data: data}).
//
//	s.Send("hello")                     // data: hello\n\n
//	s.Send(map[string]int{"n": 1})      // data: {"n":1}\n\n
//	s.Send([]byte("raw"))               // data: raw\n\n
//
// Multi-line strings are emitted as one `data:` per line per the
// SSE spec — browsers re-assemble them into a single payload at
// the EventSource.onmessage boundary.
func (s *SSEStream) Send(data any) error {
	return s.SendEvent(SSEEvent{Data: data})
}

// SendEvent writes a full SSE frame, honoring every populated
// field of e. The output ordering matches the spec:
//
//	id: <ID>\n          (omitted when ID == "")
//	event: <Event>\n    (omitted when Event == "")
//	retry: <Retry>\n    (omitted when Retry == 0)
//	data: <line>\n      (one per line of Data)
//	\n                  (blank line terminates the frame)
//
// Multi-line Data values produce one data: line each, exactly as
// EventSource expects.
func (s *SSEStream) SendEvent(e SSEEvent) error {
	var b strings.Builder
	// Each metadata line is at most ~80 chars; data lines vary —
	// 256-byte grow is a reasonable starting capacity for short
	// JSON payloads and avoids the first 2-3 realloc steps for
	// medium ones.
	b.Grow(256)
	if e.ID != "" {
		b.WriteString("id: ")
		b.WriteString(e.ID)
		b.WriteByte('\n')
	}
	if e.Event != "" {
		b.WriteString("event: ")
		b.WriteString(e.Event)
		b.WriteByte('\n')
	}
	if e.Retry > 0 {
		fmt.Fprintf(&b, "retry: %d\n", e.Retry)
	}

	payload, err := encodeSSEData(e.Data)
	if err != nil {
		return err
	}
	// Split on newlines so each becomes its own data: emission.
	// strings.Split keeps trailing empty entries, which we want —
	// "a\nb\n" → ["a", "b", ""] → "data: a\ndata: b\ndata: \n",
	// matching browser handling of the trailing newline-as-empty.
	for _, line := range strings.Split(payload, "\n") {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')

	_, werr := s.w.Write([]byte(b.String()))
	return werr
}

// Comment writes a comment frame — lines starting with ':' that
// SSE-compliant clients silently consume. The canonical use is a
// keepalive ping every ~30 s so idle proxies / NAT tables don't
// time out the connection. Multi-line text is split into multiple
// `:` lines.
//
//	s.Comment("keepalive")    // : keepalive\n\n
//	s.Comment("multi\nline")  // : multi\n: line\n\n
func (s *SSEStream) Comment(text string) error {
	var b strings.Builder
	b.Grow(len(text) + 16)
	for _, line := range strings.Split(text, "\n") {
		b.WriteString(": ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	_, err := s.w.Write([]byte(b.String()))
	return err
}

// Ping is a zero-argument keepalive shorthand: ": ping\n\n".
// Suitable to call periodically from a ticker inside the SSE
// callback to keep the connection warm across proxies that time
// out idle TCP.
func (s *SSEStream) Ping() error {
	_, err := s.w.Write([]byte(": ping\n\n"))
	return err
}

// encodeSSEData converts an arbitrary SSEEvent.Data into the
// string that goes after `data: `. The type switch covers the
// common ergonomic cases first; the fallback is JSON, matching
// the node.js libraries developers usually arrive from.
func encodeSSEData(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", nil
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case error:
		return x.Error(), nil
	default:
		buf, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("gogo: SSE: marshal data: %w", err)
		}
		return string(buf), nil
	}
}

// SSE prepares the response as a Server-Sent Events stream and
// runs fn against a writer that knows the SSE frame format. Auto-
// installed headers:
//
//	Content-Type:     text/event-stream
//	Cache-Control:    no-cache
//	Connection:       keep-alive
//	X-Accel-Buffering: no       (defeats nginx proxy_buffering)
//
// Internally delegates to Response.Stream, so SSE inherits its
// constraints: async-only (call from GetAsync or PostAsync),
// streaming chunked-encoded body, panic-on-sync-handler.
//
// fn returns when the stream is complete — typically because the
// upstream event source closed, a context cancellation fired, or
// the client disconnected (detectable via res.BufferedAmount
// growing without bound, or via OnAborted). The error fn returns
// is the error SSE returns; the stream itself is always closed
// cleanly regardless.
func (r *Response) SSE(fn func(*SSEStream) error) error {
	r.Header("Cache-Control", "no-cache")
	r.Header("Connection", "keep-alive")
	r.Header("X-Accel-Buffering", "no")
	return r.Stream(200, "text/event-stream", func(w io.Writer) error {
		if fn == nil {
			return nil
		}
		return fn(&SSEStream{w: w})
	})
}

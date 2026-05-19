// Package middleware bundles common HTTP middleware ready to drop into
// any gogo App or Router. Each middleware is a thin layer over
// gogo.Middleware so they compose with the framework's existing
// chaining model (App.Use, Router.Use).
//
// Logger records a one-line entry per request: method, URL, status,
// duration, client IP. The default format is text; pass a custom
// Format function to emit JSON or any other shape.
//
// Limitations
//
// Logger wraps the sync handler chain, so for synchronous routes
// (Get, Post, Any, Put, Patch, Delete, Options, Head) it captures the
// full request lifetime including the response write. For
// "fire-and-forget" Response.Async (a sync handler that spawns a
// goroutine and returns immediately), Logger sees only the sync
// portion — the duration will be roughly zero. PostAsync /
// AsyncMiddleware paths run inside their worker so they ARE timed
// correctly when the middleware is registered as AsyncMiddleware via
// app.Use instead of app.Use.
package middleware

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"uwebsockets-go/gogo"
	"uwebsockets-go/gogo/internal/mwhint"
)

// LoggerOptions configures Logger. Zero value uses sensible defaults
// (stdout, text format).
type LoggerOptions struct {
	// Output is where formatted log lines go. Default os.Stdout.
	Output io.Writer

	// Format produces the log line to write. Default: standard text
	// format "<method> <url> <status> <duration> <ip> <user-agent>".
	// Replace with a JSON-emitting function for structured logs.
	Format func(LogEntry) string

	// SkipPaths is a set of URLs to omit from logging — useful for
	// health-check spam (/healthz, /metrics, etc.).
	SkipPaths []string
}

// LogEntry is the snapshot a Logger Format function receives.
type LogEntry struct {
	Method    string
	URL       string
	Status    int
	Duration  time.Duration
	IP        string
	UserAgent string
}

// DefaultFormat formats an entry as a single line of human-readable
// text. Stable enough for log scraping; switch to JSONFormat for
// structured ingest.
func DefaultFormat(e LogEntry) string {
	return fmt.Sprintf("%s %s %d %s %s %q",
		e.Method, e.URL, e.Status, e.Duration, e.IP, e.UserAgent)
}

// JSONFormat formats an entry as a single line of JSON with stable
// keys. Useful when piping logs into structured systems (loki,
// elasticsearch, datadog) — handwritten so there are no allocations
// for a json.Marshal reflection pass on the hot path.
func JSONFormat(e LogEntry) string {
	return fmt.Sprintf(
		`{"method":%q,"url":%q,"status":%d,"duration_ms":%.3f,"ip":%q,"user_agent":%q}`,
		e.Method, e.URL, e.Status, float64(e.Duration.Microseconds())/1000.0,
		e.IP, e.UserAgent)
}

// Logger returns a Middleware that logs each request after its handler
// returns. Safe to share across goroutines — writes are serialized
// behind a sync.Mutex so concurrent log lines never interleave on the
// output writer.
func Logger(opts ...LoggerOptions) mwhint.Hinted {
	var opt LoggerOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.Output == nil {
		opt.Output = os.Stdout
	}
	if opt.Format == nil {
		opt.Format = DefaultFormat
	}
	var skip map[string]struct{}
	if len(opt.SkipPaths) > 0 {
		skip = make(map[string]struct{}, len(opt.SkipPaths))
		for _, p := range opt.SkipPaths {
			skip[p] = struct{}{}
		}
	}

	var mu sync.Mutex
	return mwhint.Hinted{Place: mwhint.Both, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			url := req.URL()
			if _, skipIt := skip[url]; skipIt {
				next(res, req)
				return
			}
			start := time.Now()
			next(res, req)
			entry := LogEntry{
				Method:    req.Method(),
				URL:       url,
				Status:    res.StatusCode(),
				Duration:  time.Since(start),
				IP:        req.IP(),
				UserAgent: req.Header("user-agent"),
			}
			line := opt.Format(entry)
			mu.Lock()
			fmt.Fprintln(opt.Output, line)
			mu.Unlock()
		}
	})}
}

// statusText avoids strconv allocations for the standard formatter — keep
// the integer status as a local int to format. (Helper kept for future
// formatters that may want the canonical reason phrase too.)
func statusText(status int) string {
	return strconv.Itoa(status)
}

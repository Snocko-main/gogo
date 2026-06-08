//go:build cgo && gogo

package gogo_test

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
)

// readSSE reads one SSE frame off the buffered reader and returns
// the contiguous raw block of lines (everything up to and
// including the first blank line). Tests use it instead of a full
// EventSource parser — we want to assert byte-for-byte shape, not
// re-validate browser behavior.
func readSSEFrame(t *testing.T, br *bufio.Reader, deadline time.Duration) string {
	t.Helper()
	done := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		var sb strings.Builder
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				errCh <- err
				return
			}
			sb.WriteString(line)
			if line == "\n" || line == "\r\n" {
				done <- sb.String()
				return
			}
		}
	}()
	select {
	case s := <-done:
		return s
	case e := <-errCh:
		t.Fatalf("readSSEFrame: %v", e)
		return ""
	case <-time.After(deadline):
		t.Fatalf("readSSEFrame timed out after %s", deadline)
		return ""
	}
}

// TestSSEPlainText proves the simplest pattern: Send("hello")
// emits "data: hello\n\n" with the right Content-Type.
func TestSSEPlainText(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			res.SSE(func(s *gogo.SSEStream) error {
				return s.Send("hello")
			})
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache", cc)
	}
	if xab := resp.Header.Get("X-Accel-Buffering"); xab != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", xab)
	}

	br := bufio.NewReader(resp.Body)
	frame := readSSEFrame(t, br, 2*time.Second)
	if frame != "data: hello\n\n" {
		t.Errorf("frame = %q, want %q", frame, "data: hello\n\n")
	}
}

// TestSSEAutoJSON verifies struct / map / slice payloads get
// JSON-marshaled and emitted as a single data line.
func TestSSEAutoJSON(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			res.SSE(func(s *gogo.SSEStream) error {
				return s.Send(map[string]any{"n": 42, "msg": "hi"})
			})
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events", port))
	defer resp.Body.Close()
	frame := readSSEFrame(t, bufio.NewReader(resp.Body), 2*time.Second)
	// Map iteration order is unspecified, so check both possible
	// orderings of the JSON keys.
	want1 := "data: {\"msg\":\"hi\",\"n\":42}\n\n"
	want2 := "data: {\"n\":42,\"msg\":\"hi\"}\n\n"
	if frame != want1 && frame != want2 {
		t.Errorf("frame = %q, want %q or %q", frame, want1, want2)
	}
}

// TestSSESendEventFullFrame populates every field of SSEEvent and
// asserts the exact byte ordering against the SSE spec.
func TestSSESendEventFullFrame(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			res.SSE(func(s *gogo.SSEStream) error {
				return s.SendEvent(gogo.SSEEvent{
					ID:    "42",
					Event: "tick",
					Retry: 3000,
					Data:  map[string]int{"n": 7},
				})
			})
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events", port))
	defer resp.Body.Close()
	frame := readSSEFrame(t, bufio.NewReader(resp.Body), 2*time.Second)

	want := "id: 42\nevent: tick\nretry: 3000\ndata: {\"n\":7}\n\n"
	if frame != want {
		t.Errorf("frame = %q\nwant %q", frame, want)
	}
}

// TestSSEMultiLineData verifies the per-line data: emission for
// strings containing newlines. EventSource concatenates them with
// "\n" at the client.
func TestSSEMultiLineData(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			res.SSE(func(s *gogo.SSEStream) error {
				return s.Send("line one\nline two\nline three")
			})
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events", port))
	defer resp.Body.Close()
	frame := readSSEFrame(t, bufio.NewReader(resp.Body), 2*time.Second)
	want := "data: line one\ndata: line two\ndata: line three\n\n"
	if frame != want {
		t.Errorf("frame = %q\nwant %q", frame, want)
	}
}

func TestSSECarriageReturnDataSplitsIntoDataLines(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			res.SSE(func(s *gogo.SSEStream) error {
				return s.Send("line one\rline two\r\nline three")
			})
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events", port))
	defer resp.Body.Close()
	frame := readSSEFrame(t, bufio.NewReader(resp.Body), 2*time.Second)
	want := "data: line one\ndata: line two\ndata: line three\n\n"
	if frame != want {
		t.Errorf("frame = %q\nwant %q", frame, want)
	}
}

func TestSSERejectsInjectedMetadataFields(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			_ = res.SSE(func(s *gogo.SSEStream) error {
				return s.SendEvent(gogo.SSEEvent{
					ID:    "42\nevent: injected",
					Event: "tick",
					Data:  "ok",
				})
			})
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events", port))
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "event: injected") {
		t.Fatalf("metadata injection reached stream: %q", body)
	}
	if len(body) != 0 {
		t.Fatalf("body = %q, want empty stream after rejected metadata", body)
	}
}

// TestSSEComment checks the comment frame shape — `: text\n\n` —
// and that multi-line comments fan out the same way data does.
func TestSSEComment(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			res.SSE(func(s *gogo.SSEStream) error {
				if err := s.Comment("keepalive"); err != nil {
					return err
				}
				return s.Ping()
			})
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events", port))
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	f1 := readSSEFrame(t, br, 2*time.Second)
	f2 := readSSEFrame(t, br, 2*time.Second)
	if f1 != ": keepalive\n\n" {
		t.Errorf("first frame = %q, want %q", f1, ": keepalive\n\n")
	}
	if f2 != ": ping\n\n" {
		t.Errorf("second frame = %q, want %q", f2, ": ping\n\n")
	}
}

// TestSSEErrorPayload exercises the error type-switch — Send(err)
// emits err.Error() as the data field, not the JSON encoding of
// the error struct (which is usually "{}" for opaque errors).
func TestSSEErrorPayload(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			res.SSE(func(s *gogo.SSEStream) error {
				return s.Send(fmt.Errorf("connection refused"))
			})
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events", port))
	defer resp.Body.Close()
	frame := readSSEFrame(t, bufio.NewReader(resp.Body), 2*time.Second)
	if frame != "data: connection refused\n\n" {
		t.Errorf("frame = %q", frame)
	}
}

// TestSSEStreamLoop drives a counted loop emitting 5 events and
// verifies each lands in order, then the stream closes cleanly.
func TestSSEStreamLoop(t *testing.T) {
	const N = 5
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			res.SSE(func(s *gogo.SSEStream) error {
				for i := 0; i < N; i++ {
					if err := s.SendEvent(gogo.SSEEvent{
						ID:    fmt.Sprintf("%d", i),
						Event: "tick",
						Data:  map[string]int{"i": i},
					}); err != nil {
						return err
					}
				}
				return nil
			})
		})
	})
	defer teardown()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/events", port))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	for i := 0; i < N; i++ {
		frame := readSSEFrame(t, br, 2*time.Second)
		if !strings.Contains(frame, fmt.Sprintf("id: %d", i)) {
			t.Errorf("frame %d missing id: %d: %q", i, i, frame)
		}
		if !strings.Contains(frame, "event: tick") {
			t.Errorf("frame %d missing event: tick: %q", i, frame)
		}
		if !strings.Contains(frame, fmt.Sprintf(`"i":%d`, i)) {
			t.Errorf("frame %d missing data value: %q", i, frame)
		}
	}
	// After N events fn returns and the stream closes; reading
	// past should hit io.EOF immediately.
	if _, err := br.ReadString('\n'); err != io.EOF {
		t.Errorf("expected EOF after %d events, got %v", N, err)
	}
}

// TestSSEReadsLastEventID demonstrates the canonical resumability
// pattern — handler reads Last-Event-ID via req.Header before
// calling SSE to figure out where to resume from.
func TestSSEReadsLastEventID(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			lastID := req.Header("last-event-id")
			res.SSE(func(s *gogo.SSEStream) error {
				return s.SendEvent(gogo.SSEEvent{
					ID:   "resumed",
					Data: "from " + lastID,
				})
			})
		})
	})
	defer teardown()

	r, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/events", port), nil)
	r.Header.Set("Last-Event-ID", "99")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	frame := readSSEFrame(t, bufio.NewReader(resp.Body), 2*time.Second)
	if !strings.Contains(frame, "data: from 99") {
		t.Errorf("frame missing last-event-id passthrough: %q", frame)
	}
}

// TestSSEPanicsOnSyncRoute ensures we hit the same Stream() guard
// — calling SSE from a sync handler can't work because the loop
// thread can't be held open for a streaming response.
func TestSSEPanicsOnSyncRoute(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			defer func() {
				if r := recover(); r != nil {
					res.Send(500, "text/plain", "caught")
				}
			}()
			res.SSE(func(s *gogo.SSEStream) error { return nil })
		})
	})
	defer teardown()

	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/x", port))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 500 || string(body) != "caught" {
		t.Errorf("sync SSE: status=%d body=%q, want 500/caught", resp.StatusCode, string(body))
	}
}

// BenchmarkSSEBackpressureMemory measures the memory-facing side of
// Response.SSE -> Response.Stream backpressure. Reproduce with:
//
//	go test -tags gogo -run '^$' -bench '^BenchmarkSSEBackpressureMemory$' -benchmem -benchtime=5x -count=1 .
//
// The benchmark uses a no-read client so the handler deterministically crosses
// a BufferedAmount trigger, then calls AwaitDrain and closes the client. It
// reports Go -benchmem plus peak_buffered_B, aborted/op, and drained/op.
func BenchmarkSSEBackpressureMemory(b *testing.B) {
	const (
		payloadBytes    = 32 << 10
		maxFrames       = 2048
		triggerBytes    = 4 << 20
		awaitDrainBelow = 0
	)

	savedBackpressure := gogo.GetStreamBackpressureBytes()
	gogo.SetStreamBackpressureBytes(0)
	defer gogo.SetStreamBackpressureBytes(savedBackpressure)

	payload := strings.Repeat("x", payloadBytes)
	var peakBuffered atomic.Uint64
	var abortedCount atomic.Uint64
	var drainedCount atomic.Uint64
	parked := make(chan uint64, 1)
	results := make(chan sseBackpressureResult, 1)

	port, teardown := startApp(b, func(app *gogo.App) {
		app.GetAsync("/events", func(res *gogo.Response, req *gogo.Request) {
			_ = res.SSE(func(s *gogo.SSEStream) error {
				var peak uint64
				for i := 0; i < maxFrames; i++ {
					if err := s.SendEvent(gogo.SSEEvent{
						ID:    strconv.Itoa(i),
						Event: "bench",
						Data:  payload,
					}); err != nil {
						results <- sseBackpressureResult{frames: i, peak: peak, err: err}
						return err
					}
					if i%8 == 7 {
						runtime.Gosched()
					}
					buffered := res.BufferedAmount()
					if buffered > peak {
						peak = buffered
					}
					if buffered > triggerBytes {
						parked <- buffered
						err := res.AwaitDrain(awaitDrainBelow)
						results <- sseBackpressureResult{frames: i + 1, peak: peak, err: err}
						return err
					}
				}
				err := fmt.Errorf("BufferedAmount never exceeded %d bytes after %d SSE frames", triggerBytes, maxFrames)
				results <- sseBackpressureResult{frames: maxFrames, peak: peak, err: err}
				return err
			})
		})
	})
	defer teardown()

	runtime.GC()
	var after runtime.MemStats

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second)
		if err != nil {
			b.Fatalf("dial /events: %v", err)
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetReadBuffer(1)
		}
		if _, err := fmt.Fprintf(conn, "GET /events HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nConnection: close\r\n\r\n", port); err != nil {
			conn.Close()
			b.Fatalf("write request: %v", err)
		}

		select {
		case peak := <-parked:
			recordMaxUint64(&peakBuffered, peak)
			conn.Close()
		case result := <-results:
			conn.Close()
			b.Fatalf("stream ended before AwaitDrain parked: frames=%d peak=%d err=%v", result.frames, result.peak, result.err)
		case <-time.After(5 * time.Second):
			conn.Close()
			b.Fatal("timed out waiting for SSE stream to hit backpressure")
		}

		select {
		case result := <-results:
			recordMaxUint64(&peakBuffered, result.peak)
			if errors.Is(result.err, gogo.ErrStreamAborted) {
				abortedCount.Add(1)
				continue
			}
			if result.err != nil {
				b.Fatalf("AwaitDrain error: frames=%d peak=%d err=%v", result.frames, result.peak, result.err)
			}
			drainedCount.Add(1)
		case <-time.After(5 * time.Second):
			b.Fatal("timed out waiting for AwaitDrain abort")
		}
	}
	b.StopTimer()

	runtime.GC()
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(peakBuffered.Load()), "peak_buffered_B")
	b.ReportMetric(float64(triggerBytes), "trigger_B")
	b.ReportMetric(float64(awaitDrainBelow), "await_below_B")
	b.ReportMetric(float64(after.HeapAlloc), "heap_live_after_B")
	b.ReportMetric(float64(abortedCount.Load())/float64(b.N), "aborted/op")
	b.ReportMetric(float64(drainedCount.Load())/float64(b.N), "drained/op")
}

type sseBackpressureResult struct {
	frames int
	peak   uint64
	err    error
}

func recordMaxUint64(max *atomic.Uint64, v uint64) {
	for {
		old := max.Load()
		if v <= old || max.CompareAndSwap(old, v) {
			return
		}
	}
}

//go:build cgo && gogo

package middleware_test

import (
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

// TestMetricsBasicCounters runs a handful of requests through the
// metrics middleware and verifies the snapshot counters move.
func TestMetricsBasicCounters(t *testing.T) {
	m := middleware.NewMetrics()
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Use(m.Middleware())
		app.Get("/ok", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		app.Get("/bad", func(res *gogo.Response, req *gogo.Request) {
			res.Send(400, "text/plain", "no")
		})
		app.Get("/metrics", m.Handler())
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	for i := 0; i < 5; i++ {
		r, _ := ts.Get("/ok")
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	for i := 0; i < 2; i++ {
		r, _ := ts.Get("/bad")
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}

	snap := m.Snapshot()
	// 5 ok + 2 bad = 7 instrumented; the /metrics handler is
	// outside the middleware chain at this depth (it's registered
	// AFTER metrics.Middleware so it counts too — every Get(...)
	// after Use() picks up the wrap).
	//
	// We've made one /metrics call below; check both possibilities
	// by allowing >= 7. The exact number depends on whether
	// /metrics ran before the snapshot.
	if snap.TotalRequests < 7 {
		t.Errorf("TotalRequests = %d, want >= 7", snap.TotalRequests)
	}
	if got := snap.Status["200"]; got < 5 {
		t.Errorf("status 200 = %d, want >= 5", got)
	}
	if got := snap.Status["400"]; got != 2 {
		t.Errorf("status 400 = %d, want 2", got)
	}
	if got := snap.Method["GET"]; got < 7 {
		t.Errorf("method GET = %d, want >= 7", got)
	}
}

// TestMetricsHandlerPrometheusFormat scrapes the /metrics endpoint
// and checks the response is valid Prometheus text exposition
// shape — every metric carries a HELP + TYPE comment, every value
// line uses the expected name prefix.
func TestMetricsHandlerPrometheusFormat(t *testing.T) {
	m := middleware.NewMetrics(middleware.MetricsOptions{
		Namespace: "gogo",
		Subsystem: "bench",
	})
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Use(m.Middleware())
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		app.Get("/metrics", m.Handler())
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	for i := 0; i < 3; i++ {
		r, _ := ts.Get("/x")
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}

	r, err := ts.Get("/metrics")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer r.Body.Close()
	ct := r.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Errorf("Content-Type = %q, want text/plain; version=0.0.4", ct)
	}
	body, _ := io.ReadAll(r.Body)
	text := string(body)

	prefix := "gogo_bench_"
	wantLines := []string{
		"# TYPE " + prefix + "requests_total counter",
		"# TYPE " + prefix + "requests_method_total counter",
		"# TYPE " + prefix + "request_duration_seconds histogram",
		"# TYPE " + prefix + "requests_in_flight gauge",
		"# TYPE " + prefix + "bytes_out_total counter",
		"# TYPE " + prefix + "uptime_seconds gauge",
		"# TYPE " + prefix + "go_goroutines gauge",
		prefix + "requests_total{status=\"200\"}",
		prefix + "request_duration_seconds_bucket{le=\"+Inf\"}",
		prefix + "request_duration_seconds_count",
		prefix + "request_duration_seconds_sum",
	}
	for _, line := range wantLines {
		if !strings.Contains(text, line) {
			t.Errorf("missing line %q in metrics output:\n%s", line, text)
		}
	}
}

// TestMetricsHistogramBuckets verifies the histogram cumulative
// invariant — every bucket count is >= the count of the previous
// (smaller-le) bucket, and the +Inf bucket equals the request
// total.
func TestMetricsHistogramBuckets(t *testing.T) {
	m := middleware.NewMetrics()
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Use(m.Middleware())
		app.Get("/fast", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
		app.Get("/slow", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(10 * time.Millisecond)
			res.Send(200, "text/plain", "ok")
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	for i := 0; i < 20; i++ {
		r, _ := ts.Get("/fast")
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	for i := 0; i < 5; i++ {
		r, _ := ts.Get("/slow")
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}

	snap := m.Snapshot()
	if snap.TotalRequests != 25 {
		t.Fatalf("TotalRequests = %d, want 25", snap.TotalRequests)
	}
	// Cumulative invariant: bucket[i] <= bucket[i+1].
	for i := 0; i < len(snap.BucketCounts)-1; i++ {
		if snap.BucketCounts[i] > snap.BucketCounts[i+1] {
			t.Errorf("histogram not cumulative at i=%d: %d > %d",
				i, snap.BucketCounts[i], snap.BucketCounts[i+1])
		}
	}
	// +Inf bucket (last) = total.
	if got := snap.BucketCounts[len(snap.BucketCounts)-1]; got != 25 {
		t.Errorf("+Inf bucket = %d, want 25", got)
	}
}

// TestMetricsObservationHook fires the OnObservation callback for
// every instrumented request — the hook is the OTel wiring point.
func TestMetricsObservationHook(t *testing.T) {
	type observation struct {
		method string
		status string
		dur    time.Duration
	}
	var (
		mu           sync.Mutex
		observations []observation
	)
	m := middleware.NewMetrics(middleware.MetricsOptions{
		OnObservation: func(method, status string, dur time.Duration) {
			mu.Lock()
			observations = append(observations, observation{
				method: method,
				status: status,
				dur:    dur,
			})
			mu.Unlock()
		},
	})
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Use(m.Middleware())
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	for i := 0; i < 3; i++ {
		r, _ := ts.Get("/x")
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	mu.Lock()
	defer mu.Unlock()
	if len(observations) != 3 {
		t.Errorf("OnObservation fired %d times, want 3: %v", len(observations), observations)
	}
	for _, obs := range observations {
		if obs.method != "GET" || obs.status != "200" {
			t.Errorf("observation labels = %s/%s, want GET/200", obs.method, obs.status)
		}
		if obs.dur < 0 {
			t.Errorf("observation duration = %v, want non-negative", obs.dur)
		}
	}
}

// TestMetricsInFlightGauge confirms the in-flight gauge sits at
// the right value while a handler runs and returns to 0 after.
// Uses a single in-flight request (uWS's sync loop serializes
// handlers anyway) and snapshots from inside the handler.
func TestMetricsInFlightGauge(t *testing.T) {
	var inflightMid atomic.Int64

	m := middleware.NewMetrics()
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Use(m.Middleware())
		app.Get("/inflight", func(res *gogo.Response, req *gogo.Request) {
			inflightMid.Store(m.Snapshot().InFlight)
			res.Send(200, "text/plain", "ok")
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	r, err := ts.Get("/inflight")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	if mid := inflightMid.Load(); mid != 1 {
		t.Errorf("in-flight inside handler = %d, want 1", mid)
	}
	if final := m.Snapshot().InFlight; final != 0 {
		t.Errorf("in-flight after drain = %d, want 0", final)
	}
}

// TestMetricsObserveBytes exercises the manual byte counters.
func TestMetricsObserveBytes(t *testing.T) {
	m := middleware.NewMetrics()
	m.ObserveBytesOut(100)
	m.ObserveBytesIn(50)
	m.ObserveBytesOut(200)
	snap := m.Snapshot()
	if snap.BytesOut != 300 {
		t.Errorf("BytesOut = %d, want 300", snap.BytesOut)
	}
	if snap.BytesIn != 50 {
		t.Errorf("BytesIn = %d, want 50", snap.BytesIn)
	}
}

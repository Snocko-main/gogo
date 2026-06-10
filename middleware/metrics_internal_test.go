package middleware

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMetricsRejectsInvalidMetricNameParts(t *testing.T) {
	cases := []MetricsOptions{
		{Namespace: "bad namespace"},
		{Namespace: "bad\nnamespace"},
		{Namespace: "9bad"},
		{Subsystem: "bad-subsystem"},
		{Subsystem: "bad\nsubsystem"},
		{Subsystem: "9bad"},
	}
	for _, tc := range cases {
		t.Run(tc.Namespace+"/"+tc.Subsystem, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("NewMetrics did not panic")
				}
			}()
			_ = NewMetrics(tc)
		})
	}
}

func TestMetricsAcceptsPrometheusMetricNameParts(t *testing.T) {
	_ = NewMetrics(MetricsOptions{
		Namespace: "gogo:http",
		Subsystem: "api_v1",
	})
}

func TestMetricsRejectsInvalidBuckets(t *testing.T) {
	cases := []struct {
		name    string
		buckets []float64
	}{
		{"zero", []float64{0}},
		{"negative", []float64{-0.1}},
		{"nan", []float64{math.NaN()}},
		{"inf", []float64{math.Inf(1)}},
		{"unsorted", []float64{0.5, 0.1}},
		{"duplicate", []float64{0.1, 0.1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("NewMetrics did not panic")
				}
			}()
			_ = NewMetrics(MetricsOptions{Buckets: tc.buckets})
		})
	}
}

func TestMetricsAcceptsStrictlyIncreasingBuckets(t *testing.T) {
	_ = NewMetrics(MetricsOptions{Buckets: []float64{0.001, 0.01, 0.1}})
}

func TestMetricsDefaultBucketsContract(t *testing.T) {
	m := NewMetrics()
	want := []float64{
		0.0001, 0.00025, 0.0005,
		0.001, 0.0025, 0.005,
		0.01, 0.025, 0.05,
		0.1, 0.25, 0.5,
		1, 2.5, 5, 10,
	}
	if got := m.Snapshot().BucketLE; !reflect.DeepEqual(got, want) {
		t.Fatalf("default buckets = %#v, want %#v", got, want)
	}
}

func TestMetricsCopiesCustomBuckets(t *testing.T) {
	buckets := []float64{0.005, 0.01}
	m := NewMetrics(MetricsOptions{Buckets: buckets})
	buckets[0] = 99

	want := []float64{0.005, 0.01}
	if got := m.Snapshot().BucketLE; !reflect.DeepEqual(got, want) {
		t.Fatalf("custom buckets after caller mutation = %#v, want %#v", got, want)
	}
}

func TestMetricsMethodLabelBoundsCardinality(t *testing.T) {
	m := NewMetrics()
	m.observe("get", 200, 0)
	m.observe("POST", 200, 0)
	m.observe("QUERY", 200, 0)
	m.observe("BREW", 200, 0)
	m.observe("M-SEARCH", 200, 0)
	m.observe("", 200, 0)

	snap := m.Snapshot()
	if got := snap.Method["GET"]; got != 1 {
		t.Fatalf("GET count = %d, want 1", got)
	}
	if got := snap.Method["POST"]; got != 1 {
		t.Fatalf("POST count = %d, want 1", got)
	}
	if got := snap.Method["QUERY"]; got != 1 {
		t.Fatalf("QUERY count = %d, want 1", got)
	}
	if got := snap.Method["OTHER"]; got != 3 {
		t.Fatalf("OTHER count = %d, want 3", got)
	}
	if len(snap.Method) != 4 {
		t.Fatalf("method label count = %d, want 4: %#v", len(snap.Method), snap.Method)
	}
}

func TestMetricsStatusLabelBoundsCardinality(t *testing.T) {
	m := NewMetrics()
	m.observe("GET", 200, 0)
	m.observe("GET", 599, 0)
	m.observe("GET", 999, 0)
	m.observe("GET", 99, 0)
	m.observe("GET", 1000, 0)

	snap := m.Snapshot()
	if got := snap.Status["200"]; got != 1 {
		t.Fatalf("status 200 count = %d, want 1", got)
	}
	if got := snap.Status["599"]; got != 1 {
		t.Fatalf("status 599 count = %d, want 1", got)
	}
	if got := snap.Status["999"]; got != 1 {
		t.Fatalf("status 999 count = %d, want 1", got)
	}
	if got := snap.Status["OTHER"]; got != 2 {
		t.Fatalf("status OTHER count = %d, want 2", got)
	}
	if len(snap.Status) != 4 {
		t.Fatalf("status label count = %d, want 4: %#v", len(snap.Status), snap.Status)
	}
}

func TestMetricsRenderPrometheusContract(t *testing.T) {
	m := NewMetrics(MetricsOptions{
		Namespace: "gogo",
		Subsystem: "bench",
		Buckets:   []float64{0.005, 0.01},
	})
	m.observe("post", 500, 6*time.Millisecond)
	m.observe("GET", 200, time.Millisecond)
	m.observe("BREW", 418, 20*time.Millisecond)
	m.ObserveBytesIn(3)
	m.ObserveBytesOut(7)

	text := m.render()
	if !strings.HasSuffix(text, "\n") {
		t.Fatalf("rendered metrics should end with newline:\n%s", text)
	}
	for _, forbidden := range []string{"route=", "path=", "url=", "uri="} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("rendered metrics unexpectedly include high-cardinality label %q:\n%s", forbidden, text)
		}
	}

	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	wantExact := []string{
		"# HELP gogo_bench_requests_total Total HTTP requests grouped by status.",
		"# TYPE gogo_bench_requests_total counter",
		"gogo_bench_requests_total{status=\"200\"} 1",
		"gogo_bench_requests_total{status=\"418\"} 1",
		"gogo_bench_requests_total{status=\"500\"} 1",
		"# HELP gogo_bench_requests_method_total Total HTTP requests grouped by method.",
		"# TYPE gogo_bench_requests_method_total counter",
		"gogo_bench_requests_method_total{method=\"GET\"} 1",
		"gogo_bench_requests_method_total{method=\"OTHER\"} 1",
		"gogo_bench_requests_method_total{method=\"POST\"} 1",
		"# HELP gogo_bench_request_duration_seconds Request latency in seconds.",
		"# TYPE gogo_bench_request_duration_seconds histogram",
		"gogo_bench_request_duration_seconds_bucket{le=\"0.005\"} 1",
		"gogo_bench_request_duration_seconds_bucket{le=\"0.01\"} 2",
		"gogo_bench_request_duration_seconds_bucket{le=\"+Inf\"} 3",
	}
	if len(lines) < len(wantExact)+1 {
		t.Fatalf("rendered metrics too short: %d lines\n%s", len(lines), text)
	}
	for i, want := range wantExact {
		if lines[i] != want {
			t.Fatalf("line %d = %q, want %q\nfull output:\n%s", i, lines[i], want, text)
		}
	}
	if !strings.HasPrefix(lines[15], "gogo_bench_request_duration_seconds_sum ") {
		t.Fatalf("line 15 = %q, want duration sum", lines[15])
	}
	if lines[16] != "gogo_bench_request_duration_seconds_count 3" {
		t.Fatalf("line 16 = %q, want duration count", lines[16])
	}

	wantAfterHistogram := []string{
		"# HELP gogo_bench_requests_in_flight Requests currently being processed.",
		"# TYPE gogo_bench_requests_in_flight gauge",
		"gogo_bench_requests_in_flight 0",
		"# HELP gogo_bench_bytes_in_total Bytes read from request bodies.",
		"# TYPE gogo_bench_bytes_in_total counter",
		"gogo_bench_bytes_in_total 3",
		"# HELP gogo_bench_bytes_out_total Bytes written to response bodies.",
		"# TYPE gogo_bench_bytes_out_total counter",
		"gogo_bench_bytes_out_total 7",
		"# HELP gogo_bench_uptime_seconds Seconds since the server started.",
		"# TYPE gogo_bench_uptime_seconds gauge",
	}
	for i, want := range wantAfterHistogram {
		line := 17 + i
		if lines[line] != want {
			t.Fatalf("line %d = %q, want %q\nfull output:\n%s", line, lines[line], want, text)
		}
	}
	if !strings.HasPrefix(lines[28], "gogo_bench_uptime_seconds ") {
		t.Fatalf("line 28 = %q, want uptime value", lines[28])
	}
}

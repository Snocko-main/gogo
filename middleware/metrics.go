// Built-in observability — Prometheus-format metrics + an OTel hook
// that fires per request so callers can wire any tracer they
// already use without forcing the OTel dependency on gogo.
//
// The exposed surface is intentionally narrow:
//
//	m := middleware.NewMetrics()           // optional MetricsOptions{}
//	app.Use(m.Middleware())                 // tap every request
//	app.Get("/metrics", m.Handler())       // serve Prometheus text
//
// Counters and histograms are aggregated across all requests (no
// per-route tagging by default — the per-URL cardinality of a
// real-world router would blow up Prometheus storage). Status
// codes are tagged because the set is bounded; method tagging
// likewise. Users who need per-route metrics can extend OnObservation
// in MetricsOptions.

package middleware

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/internal/mwhint"
)

// defaultMetricsBuckets covers the typical gogo latency range —
// uWS hot-path requests finish in microseconds, the long tail
// stretches to a few seconds. Buckets are in seconds (Prometheus
// convention) and span 100 µs to 10 s.
var defaultMetricsBuckets = []float64{
	0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05,
	0.1, 0.25, 0.5,
	1, 2.5, 5, 10,
}

// MetricsOptions configures the Metrics middleware.
type MetricsOptions struct {
	// Buckets are the histogram bucket upper bounds in seconds.
	// Sorted ascending; the implicit +Inf bucket is appended
	// internally so callers don't have to. If empty, defaults to
	// defaultMetricsBuckets — a Prometheus-standard set tuned for
	// the gogo latency range.
	Buckets []float64

	// Namespace prefixes every metric name (`<namespace>_<metric>`).
	// Default "http".
	Namespace string

	// Subsystem inserts a second prefix
	// (`<namespace>_<subsystem>_<metric>`). Empty by default — only
	// useful when an app exposes multiple metric groups under
	// distinct middlewares.
	Subsystem string

	// OnObservation fires after every request the middleware
	// instruments. Hook callers wire this to their tracer of
	// choice (OpenTelemetry, Datadog, etc.) without dragging the
	// SDK into gogo's dependency tree. The callback runs inline on
	// the handler goroutine — keep it non-blocking; spawn a
	// goroutine yourself if the export crosses the network.
	OnObservation func(method, status string, dur time.Duration)
}

// Metrics aggregates request statistics and exposes them as a
// Prometheus text exposition response. One instance lives for the
// lifetime of the App; Middleware and Handler return values
// captured against this instance.
type Metrics struct {
	opts MetricsOptions

	bucketLE     []float64       // ascending, no +Inf
	bucketCounts []*atomic.Int64 // len(bucketLE) + 1 (last slot = +Inf)
	sumNs        atomic.Int64
	count        atomic.Int64

	inflight atomic.Int64

	bytesIn  atomic.Int64
	bytesOut atomic.Int64

	startedAt time.Time

	// statusCounts and methodCounts are tagged counters. Use
	// sync.Map because the universe of (method, status) values is
	// effectively bounded — Map's amortized read cost matches a
	// plain map under low write churn (allocation only on first
	// observation of each tag).
	statusCounts sync.Map // string → *atomic.Int64
	methodCounts sync.Map // string → *atomic.Int64
}

// NewMetrics builds a Metrics instance with the supplied options.
// Zero options is safe; defaults are applied.
func NewMetrics(opts ...MetricsOptions) *Metrics {
	var o MetricsOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Namespace == "" {
		o.Namespace = "http"
	}
	if !validPromMetricNamePart(o.Namespace) {
		panic("gogo/middleware: Metrics Namespace must match Prometheus metric-name characters")
	}
	if o.Subsystem != "" && !validPromMetricNamePart(o.Subsystem) {
		panic("gogo/middleware: Metrics Subsystem must match Prometheus metric-name characters")
	}
	buckets := o.Buckets
	if len(buckets) == 0 {
		buckets = defaultMetricsBuckets
	}
	validateMetricsBuckets(buckets)
	// Defensive copy so a caller mutating their slice after
	// NewMetrics returns doesn't corrupt our state.
	bucketLE := append([]float64(nil), buckets...)
	bucketCounts := make([]*atomic.Int64, len(bucketLE)+1)
	for i := range bucketCounts {
		bucketCounts[i] = &atomic.Int64{}
	}
	return &Metrics{
		opts:         o,
		bucketLE:     bucketLE,
		bucketCounts: bucketCounts,
		startedAt:    time.Now(),
	}
}

func validPromMetricNamePart(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || c == ':' {
			continue
		}
		if i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

func validateMetricsBuckets(buckets []float64) {
	var prev float64
	for i, le := range buckets {
		if le <= 0 || math.IsNaN(le) || math.IsInf(le, 0) {
			panic("gogo/middleware: Metrics Buckets must contain finite positive values")
		}
		if i > 0 && le <= prev {
			panic("gogo/middleware: Metrics Buckets must be strictly increasing")
		}
		prev = le
	}
}

// Middleware returns the request-instrumenting middleware. Install
// it BEFORE the routes you want measured — the timer starts when
// the middleware fires and stops when control returns. Routes
// registered before this Use call won't be instrumented.
//
// Placement is "both chains" so sync and async routes both get
// observed; the underlying counters are atomic and safe to share
// across the loop thread and worker goroutines.
func (m *Metrics) Middleware() mwhint.Hinted {
	mm := m
	return mwhint.Hinted{Place: mwhint.Both, Mw: gogo.Middleware(func(next gogo.Handler) gogo.Handler {
		return func(res *gogo.Response, req *gogo.Request) {
			start := time.Now()
			mm.inflight.Add(1)
			// Method is captured here so a panicking handler that
			// later mutates req via SetLocal can't leak the read
			// into the deferred recorder.
			method := req.Method()
			record := func() {
				mm.inflight.Add(-1)
				dur := time.Since(start)
				status := res.StatusCode()
				if status == 0 {
					status = 200
				}
				mm.observe(method, status, dur)
			}
			// Register via OnFinish through a defer so we cover both
			// branches:
			//   - normal return → defer fires → OnFinish runs inline
			//     for sync responses, queues for async (incl. handlers
			//     that upgraded via Response.Async)
			//   - handler panic → defer still fires on the unwind
			//     before the framework's outer recover catches and
			//     emits a 500, so the metric is recorded
			// Reading res.StatusCode() inside the closure picks up the
			// final status regardless of which path got us there.
			defer res.OnFinish(record)
			next(res, req)
		}
	})}
}

// Handler returns a gogo.Handler that responds with the metrics
// encoded as Prometheus text exposition format (version 0.0.4 —
// the OpenMetrics-compatible flavor most scrapers default to).
// Register on whatever path your scrape config uses; "/metrics"
// is the convention.
//
//	app.Get("/metrics", m.Handler())
//
// The handler reads counter / bucket / gauge values atomically;
// concurrent updates from in-flight requests are safe.
func (m *Metrics) Handler() gogo.Handler {
	mm := m
	return func(res *gogo.Response, req *gogo.Request) {
		res.Send(200, "text/plain; version=0.0.4; charset=utf-8", mm.render())
	}
}

// observe records one completed request.
func (m *Metrics) observe(method string, status int, dur time.Duration) {
	m.count.Add(1)
	m.sumNs.Add(dur.Nanoseconds())

	secs := dur.Seconds()
	// Cumulative bucketing: every bucket whose le >= secs gets
	// incremented. Linear scan is fine — len(bucketLE) is small
	// and the CPU prefetches the slice into cache.
	for i, le := range m.bucketLE {
		if secs <= le {
			m.bucketCounts[i].Add(1)
		}
	}
	// +Inf bucket always increments.
	m.bucketCounts[len(m.bucketLE)].Add(1)

	statusStr := strconv.Itoa(status)
	bumpTagged(&m.statusCounts, statusStr)
	bumpTagged(&m.methodCounts, strings.ToUpper(method))

	if m.opts.OnObservation != nil {
		m.opts.OnObservation(method, statusStr, dur)
	}
}

// bumpTagged increments the counter for tag, creating it on first
// observation. sync.Map.LoadOrStore handles the race between two
// goroutines observing the same tag for the first time.
func bumpTagged(m *sync.Map, tag string) {
	v, ok := m.Load(tag)
	if !ok {
		var counter atomic.Int64
		v, _ = m.LoadOrStore(tag, &counter)
	}
	v.(*atomic.Int64).Add(1)
}

// ObserveBytesOut records bytes written for the current request.
// The middleware doesn't intercept Send / End automatically —
// users who want bytes-out tracking can call this from a handler
// or from a Logger-style middleware where they know the payload
// size.
func (m *Metrics) ObserveBytesOut(n int) { m.bytesOut.Add(int64(n)) }

// ObserveBytesIn records bytes read for the current request.
// Same usage shape as ObserveBytesOut — call where you know the
// payload size (e.g. inside a Body callback).
func (m *Metrics) ObserveBytesIn(n int) { m.bytesIn.Add(int64(n)) }

// Snapshot returns a copy of the current counter values for tests
// / programmatic readers that want to assert against the metrics
// without parsing the Prometheus exposition format.
type Snapshot struct {
	TotalRequests int64
	InFlight      int64
	MeanLatency   time.Duration
	Status        map[string]int64
	Method        map[string]int64
	BucketLE      []float64
	BucketCounts  []int64 // len = len(BucketLE) + 1 (+Inf trailing)
	BytesIn       int64
	BytesOut      int64
}

// Snapshot returns the current counter values. Cheap atomic reads,
// no allocation past the per-snapshot maps. Useful for tests and
// for in-process callers that don't want to parse Prometheus
// text.
func (m *Metrics) Snapshot() Snapshot {
	total := m.count.Load()
	sum := m.sumNs.Load()
	var mean time.Duration
	if total > 0 {
		mean = time.Duration(sum / total)
	}
	status := map[string]int64{}
	m.statusCounts.Range(func(k, v any) bool {
		status[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	method := map[string]int64{}
	m.methodCounts.Range(func(k, v any) bool {
		method[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	buckets := make([]int64, len(m.bucketCounts))
	for i, c := range m.bucketCounts {
		buckets[i] = c.Load()
	}
	return Snapshot{
		TotalRequests: total,
		InFlight:      m.inflight.Load(),
		MeanLatency:   mean,
		Status:        status,
		Method:        method,
		BucketLE:      append([]float64(nil), m.bucketLE...),
		BucketCounts:  buckets,
		BytesIn:       m.bytesIn.Load(),
		BytesOut:      m.bytesOut.Load(),
	}
}

// render produces the Prometheus text exposition body. Order
// matches the Prometheus convention: HELP / TYPE / value lines
// per metric, grouped so a scraper's parser can stream-decode.
func (m *Metrics) render() string {
	var b strings.Builder
	b.Grow(2048)

	ns := m.opts.Namespace
	sub := m.opts.Subsystem
	prefix := ns + "_"
	if sub != "" {
		prefix += sub + "_"
	}

	// requests_total — counter (one line per method, one per status)
	statusName := prefix + "requests_total"
	b.WriteString("# HELP " + statusName + " Total HTTP requests grouped by status.\n")
	b.WriteString("# TYPE " + statusName + " counter\n")
	m.statusCounts.Range(func(k, v any) bool {
		fmt.Fprintf(&b, "%s{status=%q} %d\n", statusName, k.(string), v.(*atomic.Int64).Load())
		return true
	})

	methodName := prefix + "requests_method_total"
	b.WriteString("# HELP " + methodName + " Total HTTP requests grouped by method.\n")
	b.WriteString("# TYPE " + methodName + " counter\n")
	m.methodCounts.Range(func(k, v any) bool {
		fmt.Fprintf(&b, "%s{method=%q} %d\n", methodName, k.(string), v.(*atomic.Int64).Load())
		return true
	})

	// request_duration_seconds — histogram + sum + count
	durName := prefix + "request_duration_seconds"
	b.WriteString("# HELP " + durName + " Request latency in seconds.\n")
	b.WriteString("# TYPE " + durName + " histogram\n")
	for i, le := range m.bucketLE {
		fmt.Fprintf(&b, "%s_bucket{le=\"%g\"} %d\n", durName, le, m.bucketCounts[i].Load())
	}
	fmt.Fprintf(&b, "%s_bucket{le=\"+Inf\"} %d\n", durName, m.bucketCounts[len(m.bucketLE)].Load())
	totalCount := m.count.Load()
	sumNs := m.sumNs.Load()
	sumSec := float64(sumNs) / float64(time.Second)
	fmt.Fprintf(&b, "%s_sum %g\n", durName, sumSec)
	fmt.Fprintf(&b, "%s_count %d\n", durName, totalCount)

	// in_flight — gauge
	gName := prefix + "requests_in_flight"
	b.WriteString("# HELP " + gName + " Requests currently being processed.\n")
	b.WriteString("# TYPE " + gName + " gauge\n")
	fmt.Fprintf(&b, "%s %d\n", gName, m.inflight.Load())

	// bytes_in / bytes_out — counters
	binName := prefix + "bytes_in_total"
	b.WriteString("# HELP " + binName + " Bytes read from request bodies.\n")
	b.WriteString("# TYPE " + binName + " counter\n")
	fmt.Fprintf(&b, "%s %d\n", binName, m.bytesIn.Load())

	boutName := prefix + "bytes_out_total"
	b.WriteString("# HELP " + boutName + " Bytes written to response bodies.\n")
	b.WriteString("# TYPE " + boutName + " counter\n")
	fmt.Fprintf(&b, "%s %d\n", boutName, m.bytesOut.Load())

	// uptime — gauge
	upName := prefix + "uptime_seconds"
	b.WriteString("# HELP " + upName + " Seconds since the server started.\n")
	b.WriteString("# TYPE " + upName + " gauge\n")
	fmt.Fprintf(&b, "%s %g\n", upName, time.Since(m.startedAt).Seconds())

	// process / go runtime metrics — single MemStats read; cheap
	// enough at scrape frequency (Prometheus default 15 s).
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	gcName := prefix + "go_goroutines"
	b.WriteString("# HELP " + gcName + " Live goroutines.\n")
	b.WriteString("# TYPE " + gcName + " gauge\n")
	fmt.Fprintf(&b, "%s %d\n", gcName, runtime.NumGoroutine())

	heapName := prefix + "go_heap_alloc_bytes"
	b.WriteString("# HELP " + heapName + " Bytes allocated and still in use.\n")
	b.WriteString("# TYPE " + heapName + " gauge\n")
	fmt.Fprintf(&b, "%s %d\n", heapName, ms.HeapAlloc)

	pauseName := prefix + "go_gc_pause_seconds_total"
	b.WriteString("# HELP " + pauseName + " Sum of GC stop-the-world pauses since process start.\n")
	b.WriteString("# TYPE " + pauseName + " counter\n")
	pauseSec := float64(ms.PauseTotalNs) / float64(time.Second)
	fmt.Fprintf(&b, "%s %g\n", pauseName, pauseSec)

	return b.String()
}

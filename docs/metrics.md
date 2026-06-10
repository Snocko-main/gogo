# Metrics And OpenTelemetry

`middleware.NewMetrics` is gogo's built-in observability surface for request
metrics. It exports Prometheus text exposition format 0.0.4 through
`Metrics.Handler` and records requests through `Metrics.Middleware`.

## Prometheus Contract

The default metric namespace is `http`. A configured subsystem is inserted
between the namespace and metric name, so `MetricsOptions{Namespace: "gogo",
Subsystem: "api"}` emits names such as `gogo_api_requests_total`.

The built-in metric names and labels are:

| metric | type | labels |
|---|---|---|
| `<prefix>requests_total` | counter | `status` |
| `<prefix>requests_method_total` | counter | `method` |
| `<prefix>request_duration_seconds` | histogram | `le` on bucket lines |
| `<prefix>requests_in_flight` | gauge | none |
| `<prefix>bytes_in_total` | counter | none |
| `<prefix>bytes_out_total` | counter | none |
| `<prefix>uptime_seconds` | gauge | none |
| `<prefix>go_goroutines` | gauge | none |
| `<prefix>go_heap_alloc_bytes` | gauge | none |
| `<prefix>go_gc_pause_seconds_total` | counter | none |

Labels are intentionally bounded by default:

- `method` is one of `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`,
  `HEAD`, `CONNECT`, `TRACE`, `QUERY`, or `OTHER`.
- `status` is the numeric response status from `100` through `999`, or
  `OTHER` for invalid internal observations.
- `route`, `path`, `url`, and raw user input labels are not emitted by default.

The default duration buckets are seconds:

```text
0.0001 0.00025 0.0005 0.001 0.0025 0.005 0.01 0.025
0.05 0.1 0.25 0.5 1 2.5 5 10
```

Custom buckets must be finite, positive, and strictly increasing. The `+Inf`
bucket is appended internally and is not included in `MetricsOptions.Buckets`.
Rendered output is grouped as `HELP`, `TYPE`, then samples for each metric.
Labeled samples are sorted by label value, and histogram buckets are rendered
in configured bucket order followed by `+Inf`, `_sum`, and `_count`.

## OpenTelemetry

gogo does not vendor or wrap the OpenTelemetry SDK. The supported story is:

- Use `Metrics.Handler` for Prometheus scraping.
- Use `MetricsOptions.OnObservation` for lightweight bridges to whichever
  OpenTelemetry SDK and semantic-convention version the application already
  uses.
- Write app middleware for full OpenTelemetry spans or metrics when the service
  needs attributes that gogo deliberately does not emit by default, such as
  route templates, URL scheme, server address, body sizes, or error taxonomy.

`OnObservation` runs after the response is finished and receives the normalized
`method`, normalized `status`, and request duration. The callback runs inline
with request completion, so exporters should keep it non-blocking or hand work
off to another goroutine.

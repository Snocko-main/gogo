package middleware

import (
	"math"
	"testing"
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

func TestMetricsMethodLabelBoundsCardinality(t *testing.T) {
	m := NewMetrics()
	m.observe("get", 200, 0)
	m.observe("POST", 200, 0)
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
	if got := snap.Method["OTHER"]; got != 3 {
		t.Fatalf("OTHER count = %d, want 3", got)
	}
	if len(snap.Method) != 3 {
		t.Fatalf("method label count = %d, want 3: %#v", len(snap.Method), snap.Method)
	}
}

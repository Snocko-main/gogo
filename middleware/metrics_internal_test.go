package middleware

import "testing"

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

package middleware

import "testing"

func TestEncodingQValueRejectsNonFiniteAndOutOfRange(t *testing.T) {
	cases := []string{
		"q=NaN",
		"q=+Inf",
		"q=-Inf",
		"q=1.5",
		"q=-0.1",
	}
	for _, tc := range cases {
		if got := encodingQValue(tc); got != 0 {
			t.Fatalf("encodingQValue(%q) = %v, want 0", tc, got)
		}
	}
}

func TestNegotiateEncodingRejectsInvalidQ(t *testing.T) {
	if got := negotiateEncoding("gzip;q=NaN, deflate;q=1.5"); got != "" {
		t.Fatalf("negotiateEncoding accepted invalid q, got %q", got)
	}
}

package middleware

import "testing"

func TestCORSRejectsInvalidConfiguredMethodsAndHeaders(t *testing.T) {
	cases := []struct {
		name string
		opt  CORSOptions
	}{
		{
			name: "method newline",
			opt:  CORSOptions{AllowMethods: []string{"GET", "POST\n"}},
		},
		{
			name: "method empty",
			opt:  CORSOptions{AllowMethods: []string{""}},
		},
		{
			name: "allow header space",
			opt:  CORSOptions{AllowHeaders: []string{"Content Type"}},
		},
		{
			name: "allow header newline",
			opt:  CORSOptions{AllowHeaders: []string{"X-Good", "X-Bad\n"}},
		},
		{
			name: "expose header newline",
			opt:  CORSOptions{ExposeHeaders: []string{"X-Trace\nID"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("CORS did not panic")
				}
			}()
			_ = CORS(tc.opt)
		})
	}
}

func TestCORSAcceptsWildcardAllowHeaders(t *testing.T) {
	_ = CORS(CORSOptions{
		AllowHeaders: []string{"*", "Content-Type"},
	})
}

func TestFilterRequestedHeadersDropsInvalidTokens(t *testing.T) {
	got := filterRequestedHeaders("X-Good, bad header, X-Also-Good, evil\nname")
	want := "X-Good, X-Also-Good"
	if got != want {
		t.Fatalf("filterRequestedHeaders() = %q, want %q", got, want)
	}
}

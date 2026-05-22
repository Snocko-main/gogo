package middleware

import (
	"strings"
	"testing"
)

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

func TestCORSRejectsInvalidConfiguredOrigins(t *testing.T) {
	cases := []string{
		"",
		"https://app.example.com/path",
		"https://app.example.com?x=1",
		"https://app.example.com#frag",
		"https://user@app.example.com",
		"https://app.example.com\n",
		"https://*.example.com/path",
		"https://*.",
		"https://foo.*.example.com",
	}
	for _, origin := range cases {
		t.Run(origin, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("CORS did not panic")
				}
			}()
			_ = CORS(CORSOptions{AllowOrigins: []string{origin}})
		})
	}
}

func TestCORSRejectsAmbiguousWildcardOrigins(t *testing.T) {
	cases := []CORSOptions{
		{AllowOrigins: []string{"*", "https://app.example.com"}},
		{AllowOrigins: []string{"https://app.example.com", "*"}},
		{AllowOrigins: []string{"*"}, AllowCredentials: true},
	}
	for _, opt := range cases {
		t.Run(strings.Join(opt.AllowOrigins, ","), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("CORS did not panic")
				}
			}()
			_ = CORS(opt)
		})
	}
}

func TestCORSNormalizesConfiguredOrigins(t *testing.T) {
	compiled := compileOrigins(normalizeCORSOriginPatterns([]string{
		"https://APP.example.com/",
		"https://*.TRUSTED.example/",
		"null",
	}))
	for _, origin := range []string{
		"https://app.example.com",
		"https://api.trusted.example",
		"null",
	} {
		if !matchCompiledOrigin(compiled, origin) {
			t.Fatalf("compiled origins did not match %q", origin)
		}
	}
	for _, origin := range []string{
		"https://app.example.com/path",
		"https://api.trusted.example/path",
		"https://api.trusted.example?x=1",
		"https://trusted.example",
		"https://evil.example",
	} {
		if matchCompiledOrigin(compiled, origin) {
			t.Fatalf("compiled origins matched invalid origin %q", origin)
		}
	}
}

func TestFilterRequestedHeadersDropsInvalidTokens(t *testing.T) {
	got := filterRequestedHeaders("X-Good, bad header, X-Also-Good, evil\nname")
	want := "X-Good, X-Also-Good"
	if got != want {
		t.Fatalf("filterRequestedHeaders() = %q, want %q", got, want)
	}
}

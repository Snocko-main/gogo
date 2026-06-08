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

func TestConfiguredCORSAllowHeadersIgnoresWildcardWithCredentials(t *testing.T) {
	cases := []struct {
		name             string
		headers          []string
		allowCredentials bool
		want             string
	}{
		{
			name:             "wildcard only without credentials",
			headers:          []string{"*"},
			allowCredentials: false,
			want:             "*",
		},
		{
			name:             "wildcard only with credentials",
			headers:          []string{"*"},
			allowCredentials: true,
			want:             "",
		},
		{
			name:             "mixed wildcard without credentials",
			headers:          []string{"*", "Content-Type", "X-CSRF-Token"},
			allowCredentials: false,
			want:             "*, Content-Type, X-CSRF-Token",
		},
		{
			name:             "mixed wildcard with credentials",
			headers:          []string{"*", "Content-Type", "X-CSRF-Token"},
			allowCredentials: true,
			want:             "Content-Type, X-CSRF-Token",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := configuredCORSAllowHeaders(tc.headers, tc.allowCredentials); got != tc.want {
				t.Fatalf("configuredCORSAllowHeaders() = %q, want %q", got, tc.want)
			}
		})
	}
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
		{AllowOrigins: []string{" * "}, AllowCredentials: true},
		{AllowOrigins: []string{" * ", "https://app.example.com"}},
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

func TestCORSPreLowercasesConfiguredOriginsAtConstruction(t *testing.T) {
	normalized := normalizeCORSOriginPatterns([]string{
		" HTTPS://APP.Example.COM/ ",
		"HTTPS://*.TRUSTED.Example/",
		"null",
	})
	wantNormalized := []string{
		"https://app.example.com",
		"https://*.trusted.example",
		"null",
	}
	if len(normalized) != len(wantNormalized) {
		t.Fatalf("normalized origins length = %d, want %d", len(normalized), len(wantNormalized))
	}
	for i := range wantNormalized {
		if normalized[i] != wantNormalized[i] {
			t.Fatalf("normalized[%d] = %q, want %q", i, normalized[i], wantNormalized[i])
		}
	}

	compiled := compileOrigins(normalized)
	if got, want := compiled[0].full, "https://app.example.com"; got != want {
		t.Fatalf("compiled exact full = %q, want %q", got, want)
	}
	if got, want := compiled[1].prefix, "https://"; got != want {
		t.Fatalf("compiled wildcard prefix = %q, want %q", got, want)
	}
	if got, want := compiled[1].suffix, ".trusted.example"; got != want {
		t.Fatalf("compiled wildcard suffix = %q, want %q", got, want)
	}
	if got, want := compiled[2].full, "null"; got != want {
		t.Fatalf("compiled null full = %q, want %q", got, want)
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

func TestCORSWildcardOriginMatchesOnlySubdomains(t *testing.T) {
	compiled := compileOrigins(normalizeCORSOriginPatterns([]string{
		"HTTPS://*.Example.COM/",
	}))
	for _, origin := range []string{
		"https://api.example.com",
		"https://deep.api.example.com",
		"HTTPS://API.EXAMPLE.COM/",
	} {
		if !matchCompiledOrigin(compiled, origin) {
			t.Fatalf("compiled wildcard origin did not match %q", origin)
		}
	}
	for _, origin := range []string{
		"https://example.com",
		"http://api.example.com",
		"https://evil-example.com",
		"https://api.example.com.evil.com",
	} {
		if matchCompiledOrigin(compiled, origin) {
			t.Fatalf("compiled wildcard origin matched invalid origin %q", origin)
		}
	}
}

func TestCORSRejectsRuntimeOriginsWithWhitespace(t *testing.T) {
	compiled := compileOrigins(normalizeCORSOriginPatterns([]string{
		"https://app.example.com",
	}))
	for _, origin := range []string{
		" https://app.example.com",
		"https://app.example.com ",
		"\thttps://app.example.com",
	} {
		if matchCompiledOrigin(compiled, origin) {
			t.Fatalf("compiled origins matched malformed runtime origin %q", origin)
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

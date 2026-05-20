package gogo

import (
	"strings"
	"testing"
)

// FuzzParseCookieValue feeds arbitrary Cookie header content + arbitrary
// names; the parser must never panic, must always return a string, and
// must agree with a slow reference implementation for ASCII inputs.
func FuzzParseCookieValue(f *testing.F) {
	f.Add("session=abc; theme=dark", "session")
	f.Add("a=1; b=2", "missing")
	f.Add("", "anything")
	f.Add("just-a-name", "name")
	f.Add("k=", "k")
	f.Add(" leading=ws ; trailing=ws ", "leading")
	f.Add("=value-with-empty-key", "")

	f.Fuzz(func(t *testing.T, header, name string) {
		// Empty name short-circuits at the caller; mirror that here so we
		// don't burn fuzz cycles on something the public API rejects.
		if name == "" {
			return
		}
		got := parseCookieValue(header, name)
		want := slowCookieReference(header, name)
		if got != want {
			t.Fatalf("mismatch:\n  header=%q\n  name=%q\n  got=%q\n  want=%q",
				header, name, got, want)
		}
	})
}

// slowCookieReference is the obviously-correct version used to validate the
// fast parser under fuzz. Splits on ";", trims each pair, splits at "=", and
// matches the name exactly.
func slowCookieReference(header, name string) string {
	for _, pair := range strings.Split(header, ";") {
		pair = strings.TrimLeft(pair, " \t")
		eq := strings.IndexByte(pair, '=')
		if eq < 0 {
			continue
		}
		if pair[:eq] == name {
			return pair[eq+1:]
		}
	}
	return ""
}

// FuzzParseSingleQueryParam mirrors uWS's getQuery(key) semantics: name
// match is exact (no URL decoding here), '&' separates pairs, '=' separates
// key and value.
func FuzzParseSingleQueryParam(f *testing.F) {
	f.Add("a=1&b=2", "a")
	f.Add("a=1&b=2", "b")
	f.Add("a=1&b=2", "missing")
	f.Add("", "x")
	f.Add("flag", "flag")
	f.Add("=empty-key", "")
	f.Add("dup=first&dup=second", "dup")

	f.Fuzz(func(t *testing.T, query, name string) {
		if name == "" {
			return
		}
		got := parseSingleQueryParam(query, name)
		want := slowQueryReference(query, name)
		if got != want {
			t.Fatalf("mismatch:\n  query=%q\n  name=%q\n  got=%q\n  want=%q",
				query, name, got, want)
		}
	})
}

func slowQueryReference(query, name string) string {
	for _, pair := range strings.Split(query, "&") {
		eq := strings.IndexByte(pair, '=')
		var k, v string
		if eq < 0 {
			k = pair
		} else {
			k = pair[:eq]
			v = pair[eq+1:]
		}
		if k == name {
			return v
		}
	}
	return ""
}

// FuzzLookupHeader feeds arbitrary "name\0value\0..." buffers and looks for
// a target name (case-insensitively).
func FuzzLookupHeader(f *testing.F) {
	f.Add([]byte("content-type\x00application/json\x00x-foo\x00bar\x00"), "content-type")
	f.Add([]byte("content-type\x00application/json\x00"), "CONTENT-TYPE")
	f.Add([]byte{}, "anything")
	f.Add([]byte("orphan-name-no-zero"), "orphan-name-no-zero")
	f.Add([]byte("a\x00"), "a")
	f.Add([]byte("a\x00b\x00c"), "a")

	f.Fuzz(func(t *testing.T, buf []byte, name string) {
		if name == "" {
			return
		}
		snap := &requestSnapshot{headers: buf}
		_ = snap.lookupHeader(name) // must never panic
	})
}

// FuzzValidateHeaderValue makes sure validateHeaderValue panics if and only
// if the value contains CR/LF/NUL — fuzz catches sneaky-encoding bypasses.
func FuzzValidateHeaderValue(f *testing.F) {
	f.Add("application/json")
	f.Add("")
	f.Add("text/plain; charset=utf-8")
	f.Add("evil\r\nSet-Cookie: hack")
	f.Add("\x00null")

	f.Fuzz(func(t *testing.T, value string) {
		hasCtl := false
		for i := 0; i < len(value); i++ {
			if c := value[i]; c == '\r' || c == '\n' || c == 0 {
				hasCtl = true
				break
			}
		}
		defer func() {
			r := recover()
			if hasCtl && r == nil {
				t.Fatalf("value %q has CTL but validator allowed it", value)
			}
			if !hasCtl && r != nil {
				t.Fatalf("value %q is clean but validator panicked: %v", value, r)
			}
		}()
		validateHeaderValue("X-Test", value)
	})
}

// TestCookieValueRoundtrip is a small property test: every cookie value that
// SetCookie accepts must round-trip through parseCookieValue cleanly.
func TestCookieValueRoundtrip(t *testing.T) {
	cases := []string{
		"simple",
		"abc123",
		"with-dashes-and_underscores",
		"a/b/c",
		"%encoded%20text",
	}
	for _, v := range cases {
		// SetCookie's validation must accept these (no panic).
		validateCookieValue(v)
		// And parseCookieValue must echo them back.
		header := "k=" + v
		got := parseCookieValue(header, "k")
		if got != v {
			t.Fatalf("roundtrip %q -> %q", v, got)
		}
	}
}

// BenchmarkParseCookieValue measures cookie lookup; this runs once per
// incoming request that calls Cookie(), so the alloc target is zero.
func BenchmarkParseCookieValue(b *testing.B) {
	header := "session=abc123; theme=dark; lang=en; remember-me=1; sidebar=open"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if parseCookieValue(header, "lang") != "en" {
			b.Fatal("wrong value")
		}
	}
}

func BenchmarkParseCookieValueMissing(b *testing.B) {
	header := "session=abc123; theme=dark; lang=en; remember-me=1; sidebar=open"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if parseCookieValue(header, "absent") != "" {
			b.Fatal("expected empty")
		}
	}
}

// BenchmarkParseSingleQueryParam — similar shape, exercises the query parser.
func BenchmarkParseSingleQueryParam(b *testing.B) {
	q := "page=2&size=50&order=desc&sort=created_at&filter=active&token=abcd1234"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if parseSingleQueryParam(q, "filter") != "active" {
			b.Fatal("wrong value")
		}
	}
}

// BenchmarkLookupHeader — snapshot header lookup, runs per req.Header call
// in async handlers. We want this to stay zero-alloc.
func BenchmarkLookupHeader(b *testing.B) {
	headers := []byte(
		"host\x00example.com\x00" +
			"user-agent\x00Mozilla/5.0\x00" +
			"accept\x00*/*\x00" +
			"accept-language\x00en-US,en;q=0.9\x00" +
			"content-type\x00application/json\x00" +
			"authorization\x00Bearer abc.def.ghi\x00",
	)
	snap := &requestSnapshot{headers: headers}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if snap.lookupHeader("authorization") == "" {
			b.Fatal("wrong value")
		}
	}
}

// BenchmarkStatusLine — generated per response on the Send fast path.
func BenchmarkStatusLine(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if statusLine(200) != "200 OK" {
			b.Fatal("wrong")
		}
	}
}

// TestNormalizeMWPrefix covers the user-facing patterns Use() accepts and
// the canonical stored form (trailing /* or /** stripped; root means global).
func TestNormalizeMWPrefix(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/api", "/api"},
		{"/api/*", "/api"},
		{"/api/**", "/api"},
		{"/api/v1/*", "/api/v1"},
		{"/admin/users/**", "/admin/users"},
		{"/", ""},
		{"/*", ""},
		{"/**", ""},
	}
	for _, c := range cases {
		if got := normalizeMWPrefix(c.in); got != c.want {
			t.Errorf("normalizeMWPrefix(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestMWMatches pins down the path-scope matching semantics: exact match,
// child match via "/", and reject same-prefix-different-segment like /apiv2.
func TestMWMatches(t *testing.T) {
	cases := []struct {
		prefix, route string
		want          bool
	}{
		// Global (empty prefix) matches everything.
		{"", "/anything", true},
		{"", "/", true},

		// Exact match.
		{"/api", "/api", true},

		// Child paths under the prefix.
		{"/api", "/api/users", true},
		{"/api", "/api/users/:id", true},

		// Sibling that shares the prefix string but not the segment.
		{"/api", "/apiv2", false},
		{"/api", "/apiv2/users", false},

		// Unrelated route.
		{"/api", "/other", false},

		// Deeper prefix.
		{"/api/v1", "/api/v1/users", true},
		{"/api/v1", "/api/v2/users", false},
	}
	for _, c := range cases {
		if got := mwMatches(c.prefix, c.route); got != c.want {
			t.Errorf("mwMatches(%q, %q) = %v, want %v", c.prefix, c.route, got, c.want)
		}
	}
}

// TestStatusLineKnown checks that the statusLine helper matches net/http for
// a set of well-known codes (mostly for sanity — the underlying call is
// http.StatusText).
func TestStatusLineKnown(t *testing.T) {
	cases := map[int]string{
		200: "200 OK",
		201: "201 Created",
		204: "204 No Content",
		301: "301 Moved Permanently",
		400: "400 Bad Request",
		401: "401 Unauthorized",
		404: "404 Not Found",
		413: "413 Request Entity Too Large",
		500: "500 Internal Server Error",
		503: "503 Service Unavailable",
	}
	for code, want := range cases {
		if got := statusLine(code); got != want {
			t.Errorf("statusLine(%d) = %q, want %q", code, got, want)
		}
	}
	// Unknown codes fall through to just the number.
	if got := statusLine(999); got != "999" {
		t.Errorf("statusLine(999) = %q, want %q", got, "999")
	}
}

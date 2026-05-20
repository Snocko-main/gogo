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

// TestValidateHeaderName covers the RFC 7230 token grammar enforcement
// for response header names. Anything outside tchar must panic — empty
// strings, control characters, separators (colon, space, parentheses),
// and high-bit bytes are all rejected. Tchar characters all pass.
func TestValidateHeaderName(t *testing.T) {
	good := []string{
		"Content-Type",
		"X-Request-ID",
		"Vary",
		"X-Custom!#$%&'*+-.^_`|~",
		"abc123",
	}
	for _, name := range good {
		t.Run("valid/"+name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("valid header name %q panicked: %v", name, r)
				}
			}()
			validateHeaderName(name)
		})
	}

	bad := map[string]string{
		"empty":             "",
		"with CR":           "X-Foo\r",
		"with LF":           "X-Foo\n",
		"with NUL":          "X-Foo\x00",
		"with colon":        "X-Foo:Bar",
		"with space":        "X-Foo Bar",
		"with high bit":     "X-Foo\x80",
		"leading control":   "\x01X-Foo",
		"parentheses":       "(X-Foo)",
		"double quotes":     "\"X-Foo\"",
		"forward slash":     "X/Foo",
		"trailing tab":      "X-Foo\t",
	}
	for label, name := range bad {
		t.Run("invalid/"+label, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("invalid header name %q (%s) did not panic", name, label)
				}
			}()
			validateHeaderName(name)
		})
	}
}

// FuzzValidateHeaderName mirrors FuzzValidateHeaderValue for the name
// validator — the validator must panic exactly when the input violates
// the RFC 7230 token grammar.
func FuzzValidateHeaderName(f *testing.F) {
	f.Add("Content-Type")
	f.Add("X-Trace-ID")
	f.Add("")
	f.Add("Bad: Name")
	f.Add("Bad\r\nName")
	f.Add("Bad\x00")
	f.Add("X-Foo!#$%&'*+-.^_`|~")

	f.Fuzz(func(t *testing.T, name string) {
		valid := name != ""
		for i := 0; valid && i < len(name); i++ {
			if !isHTTPTokenChar(name[i]) {
				valid = false
			}
		}
		defer func() {
			r := recover()
			if !valid && r == nil {
				t.Fatalf("name %q is invalid but validator allowed it", name)
			}
			if valid && r != nil {
				t.Fatalf("name %q is valid but validator panicked: %v", name, r)
			}
		}()
		validateHeaderName(name)
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

// TestValidateCookiePath, TestValidateCookieDomain,
// TestValidateCookieExpires, and TestValidateCookieSameSite cover the
// cookie-attribute validators. The goal is to make sure each one
// rejects bytes that would inject an extra attribute (";"), break the
// header line (CTLs), or — for SameSite — slip past the typed enum.
func TestValidateCookiePath(t *testing.T) {
	good := []string{"/", "/api/v1", "/", "/path-with-dash_and_under", "/", ""}
	for _, p := range good {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("valid Path %q panicked: %v", p, r)
				}
			}()
			if p == "" {
				return // empty Path is allowed; SetCookie skips emission
			}
			validateCookiePath(p)
		}()
	}
	bad := map[string]string{
		"semicolon":  "/; Domain=evil.com",
		"CR":         "/foo\r",
		"LF":         "/foo\n",
		"NUL":        "/foo\x00",
		"DEL":        "/foo\x7f",
	}
	for label, p := range bad {
		t.Run("invalid/"+label, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("invalid Path %q (%s) did not panic", p, label)
				}
			}()
			validateCookiePath(p)
		})
	}
}

func TestValidateCookieDomain(t *testing.T) {
	good := []string{"example.com", ".example.com", "sub.example.com", "single", "xn--punycode.example"}
	for _, d := range good {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("valid Domain %q panicked: %v", d, r)
				}
			}()
			validateCookieDomain(d)
		}()
	}
	bad := map[string]string{
		"semicolon":     "evil.com; HttpOnly=false",
		"comma":         "a.com,b.com",
		"space":         "victim com",
		"tab":           "victim\tcom",
		"CR":            "victim.com\r",
		"control byte":  "victim\x01com",
	}
	for label, d := range bad {
		t.Run("invalid/"+label, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("invalid Domain %q (%s) did not panic", d, label)
				}
			}()
			validateCookieDomain(d)
		})
	}
}

func TestValidateCookieExpires(t *testing.T) {
	good := []string{
		"Wed, 21 Oct 2025 07:28:00 GMT",
		"Thu, 01 Jan 1970 00:00:00 GMT",
		"Mon, 14 Feb 2022 13:37:00 UTC",
	}
	for _, e := range good {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("valid Expires %q panicked: %v", e, r)
				}
			}()
			validateCookieExpires(e)
		}()
	}
	bad := map[string]string{
		"semicolon": "Wed, 21 Oct 2025 07:28:00 GMT; Secure=false",
		"CR":        "Wed, 21 Oct 2025\r07:28:00 GMT",
		"NUL":       "Wed,\x0021 Oct",
	}
	for label, e := range bad {
		t.Run("invalid/"+label, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("invalid Expires %q (%s) did not panic", e, label)
				}
			}()
			validateCookieExpires(e)
		})
	}
}

func TestValidateCookieSameSite(t *testing.T) {
	good := []SameSite{"", SameSiteStrict, SameSiteLax, SameSiteNone}
	for _, s := range good {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("valid SameSite %q panicked: %v", s, r)
				}
			}()
			validateCookieSameSite(s)
		}()
	}
	bad := []SameSite{
		"Whatever",
		"lax",                                // case-sensitive per spec
		SameSite("Lax; Domain=evil.example"), // injection via cast
		SameSite("Strict\r\nX-Bad: 1"),
		SameSite("None;"),
	}
	for _, s := range bad {
		t.Run("invalid/"+string(s), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("invalid SameSite %q did not panic", s)
				}
			}()
			validateCookieSameSite(s)
		})
	}
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

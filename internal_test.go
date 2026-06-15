package gogo

import (
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func TestBodyEncoderOverflowReturnsPrefixOnly(t *testing.T) {
	enc := &bodyEncoder{max: 4}
	if prefix, overflow := enc.writeString("abc"); overflow || prefix != "" {
		t.Fatalf("first write: prefix=%q overflow=%v, want no overflow", prefix, overflow)
	}
	prefix, overflow := enc.writeString("defgh")
	if !overflow {
		t.Fatal("second write: overflow=false, want true")
	}
	if prefix != "abc" {
		t.Fatalf("overflow prefix = %q, want buffered prefix only", prefix)
	}
	if len(enc.buf) != 0 {
		t.Fatalf("buffer len after overflow = %d, want 0", len(enc.buf))
	}
}

func TestHeadersBlobForIterationIncompleteSyncUsesFullDump(t *testing.T) {
	partial := []byte("x-first\x00one\x00")
	full := []byte("x-first\x00one\x00x-late\x00late\x00")
	req := &Request{
		syncHeadersPtr:      unsafe.Pointer(&partial[0]),
		syncHeadersLen:      len(partial),
		syncHeadersComplete: false,
	}

	called := false
	got := req.headersBlobForIteration(func() []byte {
		called = true
		return full
	})

	if !called {
		t.Fatal("full dump was not called for incomplete sync header blob")
	}
	if string(got) != string(full) {
		t.Fatalf("blob=%q, want full dump %q", string(got), string(full))
	}
}

func TestAsyncHeaderContentTypeIsCaseInsensitiveFastPath(t *testing.T) {
	res := &Response{async: &asyncState{}}

	res.Header("content-type", "application/json")

	if res.async.contentType != "application/json" {
		t.Fatalf("async contentType = %q, want application/json", res.async.contentType)
	}
	if len(res.pendingHeaders) != 0 {
		t.Fatalf("Content-Type was buffered as pending header: %v", res.pendingHeaders)
	}
}

func TestDefaultConfigAppliesSafeBodyReadTimeout(t *testing.T) {
	cfg := defaultConfig(Config{})
	if cfg.BodyReadTimeout != defaultBodyReadTimeout {
		t.Fatalf("BodyReadTimeout default = %s, want %s", cfg.BodyReadTimeout, defaultBodyReadTimeout)
	}
	if cfg.BodyReadTimeout <= 0 {
		t.Fatalf("BodyReadTimeout default must be enabled, got %s", cfg.BodyReadTimeout)
	}
}

func TestDefaultConfigKeepsExplicitBodyReadTimeout(t *testing.T) {
	cfg := defaultConfig(Config{BodyReadTimeout: 10 * time.Second})
	if cfg.BodyReadTimeout != 10*time.Second {
		t.Fatalf("BodyReadTimeout = %s, want 10s", cfg.BodyReadTimeout)
	}

	cfg = defaultConfig(Config{BodyReadTimeout: NoBodyReadTimeout})
	if cfg.BodyReadTimeout != NoBodyReadTimeout {
		t.Fatalf("disabled BodyReadTimeout = %s, want %s", cfg.BodyReadTimeout, NoBodyReadTimeout)
	}
}

func TestZeroValueConfigDefaults(t *testing.T) {
	cfg := defaultConfig(Config{})
	if cfg.BodyLimit != 4<<20 {
		t.Fatalf("BodyLimit default = %d, want 4 MiB", cfg.BodyLimit)
	}
	if cfg.BodyReadTimeout != defaultBodyReadTimeout {
		t.Fatalf("BodyReadTimeout default = %s, want %s", cfg.BodyReadTimeout, defaultBodyReadTimeout)
	}
	if cfg.BindAddr != "" {
		t.Fatalf("BindAddr default = %q, want empty string", cfg.BindAddr)
	}
	if cfg.CapturePeerIP {
		t.Fatal("CapturePeerIP default = true, want false")
	}
	if len(cfg.TrustedProxies) != 0 {
		t.Fatalf("TrustedProxies default = %v, want empty", cfg.TrustedProxies)
	}
	if cfg.TrustProxy {
		t.Fatal("TrustProxy default = true, want false")
	}
	if cfg.JSONEncoder == nil {
		t.Fatal("JSONEncoder default is nil")
	}
	if cfg.JSONDecoder == nil {
		t.Fatal("JSONDecoder default is nil")
	}
}

func TestValidateConfigRejectsInvalidNegativeBodyLimit(t *testing.T) {
	if err := validateConfig(Config{BodyLimit: -2}); err == nil {
		t.Fatal("validateConfig accepted negative BodyLimit other than NoBodyLimit")
	}
	if err := validateConfig(Config{BodyLimit: NoBodyLimit}); err != nil {
		t.Fatalf("validateConfig rejected NoBodyLimit: %v", err)
	}
}

func TestValidateConfigRejectsInvalidNegativeBodyReadTimeout(t *testing.T) {
	if err := validateConfig(Config{BodyReadTimeout: -2 * time.Second}); err == nil {
		t.Fatal("validateConfig accepted negative BodyReadTimeout other than NoBodyReadTimeout")
	}
	if err := validateConfig(Config{BodyReadTimeout: NoBodyReadTimeout}); err != nil {
		t.Fatalf("validateConfig rejected NoBodyReadTimeout: %v", err)
	}
}

func TestValidateConfigRejectsInvalidTrustedProxies(t *testing.T) {
	for _, proxies := range [][]string{
		{""},
		{"not-an-ip"},
		{"10.0.0.0/not-bits"},
	} {
		if err := validateConfig(Config{TrustedProxies: proxies}); err == nil {
			t.Fatalf("validateConfig accepted TrustedProxies=%v", proxies)
		}
	}
}

func TestParseTrustedProxyRanges(t *testing.T) {
	ranges, err := parseTrustedProxyRanges([]string{
		" 127.0.0.1 ",
		"10.0.0.0/8",
		"::ffff:192.0.2.0/120",
	})
	if err != nil {
		t.Fatalf("parseTrustedProxyRanges: %v", err)
	}
	checks := []struct {
		addr string
		idx  int
	}{
		{"127.0.0.1", 0},
		{"10.20.30.40", 1},
		{"192.0.2.42", 2},
	}
	for _, tc := range checks {
		if !ranges[tc.idx].Contains(netip.MustParseAddr(tc.addr)) {
			t.Fatalf("range %d = %s does not contain %s", tc.idx, ranges[tc.idx], tc.addr)
		}
	}
}

func TestDefaultConfigTrustedProxiesEnableCapturePeerIP(t *testing.T) {
	cfg := defaultConfig(Config{TrustedProxies: []string{"127.0.0.1"}})
	if !cfg.CapturePeerIP {
		t.Fatal("TrustedProxies should enable CapturePeerIP for async/shared trust decisions")
	}
}

func TestDefaultRunTuningUsesGOMAXPROCSBudget(t *testing.T) {
	cases := []struct {
		procs       int
		wantCores   int
		wantWorkers int
	}{
		{0, 1, 1},
		{1, 1, 1},
		{2, 2, 1},
		{4, 2, 2},
		{6, 3, 3},
		{8, 4, 4},
		{16, 4, 12},
	}
	for _, tc := range cases {
		got := defaultRunTuning(tc.procs)
		if got.cores != tc.wantCores || got.workers != tc.wantWorkers {
			t.Fatalf("defaultRunTuning(%d) = cores=%d workers=%d, want cores=%d workers=%d",
				tc.procs, got.cores, got.workers, tc.wantCores, tc.wantWorkers)
		}
	}
}

func TestNormalizeRunOptionsOverridesAutoTuning(t *testing.T) {
	got, err := normalizeRunOptions(RunOptions{Cores: 5, Workers: 7}, 4)
	if err != nil {
		t.Fatalf("normalizeRunOptions returned error: %v", err)
	}
	if got.cores != 5 || got.workers != 7 {
		t.Fatalf("normalizeRunOptions override = cores=%d workers=%d, want cores=5 workers=7", got.cores, got.workers)
	}
}

func TestNormalizeRunOptionsRecomputesAutoWorkersAfterCoreOverride(t *testing.T) {
	got, err := normalizeRunOptions(RunOptions{Cores: 1}, 4)
	if err != nil {
		t.Fatalf("normalizeRunOptions returned error: %v", err)
	}
	if got.cores != 1 || got.workers != 3 {
		t.Fatalf("normalizeRunOptions core override = cores=%d workers=%d, want cores=1 workers=3", got.cores, got.workers)
	}
}

func TestNormalizeRunOptionsRejectsNegativeValues(t *testing.T) {
	cases := []struct {
		name string
		opts RunOptions
		want string
	}{
		{name: "cores", opts: RunOptions{Cores: -1}, want: "RunOptions.Cores"},
		{name: "workers", opts: RunOptions{Workers: -1}, want: "RunOptions.Workers"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := normalizeRunOptions(tc.opts, 4)
			if err == nil {
				t.Fatal("normalizeRunOptions accepted negative value")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("normalizeRunOptions error = %q, want %q", err, tc.want)
			}
		})
	}
}

func TestRunWithOptionsRejectsInvalidConfigBeforeNativeSetup(t *testing.T) {
	handle, err := RunWithOptions(0, func(app *App) {}, RunOptions{
		Config: Config{BodyLimit: -2},
	})
	if err == nil {
		if handle != nil {
			handle.Shutdown()
			handle.Wait()
		}
		t.Fatal("RunWithOptions accepted invalid Config")
	}
	if handle != nil {
		t.Fatal("RunWithOptions returned handle for invalid Config")
	}
	if !strings.Contains(err.Error(), "Config.BodyLimit") {
		t.Fatalf("RunWithOptions invalid config error = %v, want Config.BodyLimit", err)
	}
}

func TestNewAppRejectsMultipleConfigs(t *testing.T) {
	app, err := NewApp(Config{}, Config{})
	if err == nil {
		if app != nil {
			app.Close()
		}
		t.Fatal("NewApp accepted multiple Config values")
	}
	if !strings.Contains(err.Error(), "at most one Config") {
		t.Fatalf("NewApp multiple Config error = %v", err)
	}
}

func TestNewAppRejectsInvalidConfigBeforeNativeSetup(t *testing.T) {
	app, err := NewApp(Config{BodyLimit: -2})
	if err == nil {
		if app != nil {
			app.Close()
		}
		t.Fatal("NewApp accepted invalid BodyLimit")
	}
	if !strings.Contains(err.Error(), "Config.BodyLimit") {
		t.Fatalf("NewApp invalid config error = %v", err)
	}
}

func TestSetPanicHandlerUsesGlobalHandler(t *testing.T) {
	t.Cleanup(func() { SetPanicHandler(nil) })

	var got []any
	SetPanicHandler(func(recovered any) {
		got = append(got, recovered)
	})

	reportPanic("first")
	reportPanic("second")

	if len(got) != 2 {
		t.Fatalf("panic handler calls = %d, want 2", len(got))
	}
	if got[0] != "first" || got[1] != "second" {
		t.Fatalf("panic handler payloads = %#v, want first, second", got)
	}
}

func TestSetPanicHandlerRecoversHandlerPanic(t *testing.T) {
	t.Cleanup(func() { SetPanicHandler(nil) })

	SetPanicHandler(func(any) {
		panic("panic handler failed")
	})

	panicked := false
	func() {
		defer func() {
			panicked = recover() != nil
		}()
		reportPanic("boom")
	}()
	if panicked {
		t.Fatal("reportPanic propagated a panic from the panic handler")
	}
}

func TestDefaultConfigJSONCodecs(t *testing.T) {
	cfg := defaultConfig(Config{})
	if cfg.JSONEncoder == nil {
		t.Fatal("JSONEncoder default is nil")
	}
	if cfg.JSONDecoder == nil {
		t.Fatal("JSONDecoder default is nil")
	}

	customEncoder := func(v any) ([]byte, error) { return []byte("{}"), nil }
	customDecoder := func(data []byte, v any) error { return nil }
	cfg = defaultConfig(Config{JSONEncoder: customEncoder, JSONDecoder: customDecoder})
	if reflect.ValueOf(cfg.JSONEncoder).Pointer() != reflect.ValueOf(customEncoder).Pointer() {
		t.Fatal("JSONEncoder did not preserve custom function")
	}
	if reflect.ValueOf(cfg.JSONDecoder).Pointer() != reflect.ValueOf(customDecoder).Pointer() {
		t.Fatal("JSONDecoder did not preserve custom function")
	}
}

func TestEscapeJSONPDefusesScriptAndLineSeparators(t *testing.T) {
	in := []byte("{\"x\":\"</script>&\u0085\u2028\u2029\"}")
	got := string(escapeJSONP(in))
	want := "{\"x\":\"\\u003c/script\\u003e\\u0026\\u0085\\u2028\\u2029\"}"
	if got != want {
		t.Fatalf("escapeJSONP = %q, want %q", got, want)
	}
}

func TestEscapeJSONPNoopReturnsOriginalSlice(t *testing.T) {
	in := []byte(`{"x":"safe"}`)
	out := escapeJSONP(in)
	if len(out) == 0 || &out[0] != &in[0] {
		t.Fatal("escapeJSONP allocated for safe input")
	}
}

func TestDefaultConfigBodyLimitDefaultsAndDisableSentinel(t *testing.T) {
	cfg := defaultConfig(Config{})
	if cfg.BodyLimit != 4<<20 {
		t.Fatalf("BodyLimit default = %d, want 4 MiB", cfg.BodyLimit)
	}

	cfg = defaultConfig(Config{BodyLimit: 1024})
	if cfg.BodyLimit != 1024 {
		t.Fatalf("BodyLimit = %d, want 1024", cfg.BodyLimit)
	}

	cfg = defaultConfig(Config{BodyLimit: NoBodyLimit})
	if cfg.BodyLimit != 0 {
		t.Fatalf("NoBodyLimit normalized to %d, want native disabled value 0", cfg.BodyLimit)
	}
}

func TestSendFileLimitDisableSentinel(t *testing.T) {
	if sendFileTooLarge(1<<30, NoSendFileLimit) {
		t.Fatal("NoSendFileLimit rejected a large file")
	}
	if !sendFileTooLarge(17, 16) {
		t.Fatal("cap 16 accepted size 17")
	}
	if sendFileTooLarge(16, 16) {
		t.Fatal("cap 16 rejected exact size 16")
	}
}

func TestHTTPAdapterRecorderRejectsInvalidWriteHeaderCode(t *testing.T) {
	for _, code := range []int{99, 1000} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("WriteHeader(%d) did not panic", code)
				}
			}()
			rec := newHTTPAdapterRecorder(-1)
			rec.WriteHeader(code)
		})
	}
}

func TestHTTPAdapterRecorderIgnoresInvalidSecondWriteHeader(t *testing.T) {
	rec := newHTTPAdapterRecorder(-1)
	rec.WriteHeader(200)
	rec.WriteHeader(99)
	if rec.code != 200 {
		t.Fatalf("second WriteHeader changed code to %d, want 200", rec.code)
	}
}

func TestHTTPAdapterRecorderFlushCommitsStatusOnly(t *testing.T) {
	rec := newHTTPAdapterRecorder(-1)
	rec.Flush()
	if rec.code != 200 {
		t.Fatalf("Flush code = %d, want 200", rec.code)
	}
	if rec.body.Len() != 0 {
		t.Fatalf("Flush wrote body len %d, want 0", rec.body.Len())
	}
}

func TestHostnameFromHostHeader(t *testing.T) {
	tests := []struct {
		name string
		host string
		want string
	}{
		{name: "host without port", host: "api.example.com", want: "api.example.com"},
		{name: "host with port", host: "api.example.com:8443", want: "api.example.com"},
		{name: "bracketed ipv6 with port", host: "[::1]:3000", want: "::1"},
		{name: "bracketed ipv6 without port", host: "[2001:db8::1]", want: "2001:db8::1"},
		{name: "unbracketed ipv6 literal", host: "2001:db8::1", want: "2001:db8::1"},
		{name: "trim whitespace", host: " api.example.com:443 ", want: "api.example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hostnameFromHostHeader(tt.host); got != tt.want {
				t.Fatalf("hostnameFromHostHeader(%q) = %q, want %q", tt.host, got, tt.want)
			}
		})
	}
}

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
		"empty":           "",
		"with CR":         "X-Foo\r",
		"with LF":         "X-Foo\n",
		"with NUL":        "X-Foo\x00",
		"with colon":      "X-Foo:Bar",
		"with space":      "X-Foo Bar",
		"with high bit":   "X-Foo\x80",
		"leading control": "\x01X-Foo",
		"parentheses":     "(X-Foo)",
		"double quotes":   "\"X-Foo\"",
		"forward slash":   "X/Foo",
		"trailing tab":    "X-Foo\t",
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
		"semicolon": "/; Domain=evil.com",
		"CR":        "/foo\r",
		"LF":        "/foo\n",
		"NUL":       "/foo\x00",
		"DEL":       "/foo\x7f",
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
		"semicolon":    "evil.com; HttpOnly=false",
		"comma":        "a.com,b.com",
		"space":        "victim com",
		"tab":          "victim\tcom",
		"CR":           "victim.com\r",
		"control byte": "victim\x01com",
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

func TestDefaultMultipartPartLimitSetter(t *testing.T) {
	oldLimit := GetDefaultMultipartPartLimit()
	defer SetDefaultMultipartPartLimit(oldLimit)
	SetDefaultMultipartPartLimit(123)
	if got := GetDefaultMultipartPartLimit(); got != 123 {
		t.Fatalf("GetDefaultMultipartPartLimit() = %d, want 123", got)
	}
}

func TestMultipartPartLimitDisableSentinel(t *testing.T) {
	oldLimit := GetDefaultMultipartPartLimit()
	defer SetDefaultMultipartPartLimit(oldLimit)

	if got := multipartPartLimit(MultipartOptions{MaxPartBytes: NoMultipartPartLimit}); got != 0 {
		t.Fatalf("NoMultipartPartLimit option normalized to %d, want 0", got)
	}

	SetDefaultMultipartPartLimit(NoMultipartPartLimit)
	if got := multipartPartLimit(MultipartOptions{}); got != 0 {
		t.Fatalf("NoMultipartPartLimit default normalized to %d, want 0", got)
	}
}

func TestDefaultMultipartPartLimitLegacyAssignment(t *testing.T) {
	oldLimit := GetDefaultMultipartPartLimit()
	defer SetDefaultMultipartPartLimit(oldLimit)
	DefaultMultipartPartLimit = 456
	if got := GetDefaultMultipartPartLimit(); got != 456 {
		t.Fatalf("legacy DefaultMultipartPartLimit assignment read as %d, want 456", got)
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

func TestStatusLineRejectsInvalidCode(t *testing.T) {
	for _, code := range []int{-1, 0, 99, 1000} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("statusLine(%d) did not panic", code)
				}
			}()
			_ = statusLine(code)
		})
	}
}

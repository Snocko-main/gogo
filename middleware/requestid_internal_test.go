package middleware

import (
	"encoding/base64"
	"encoding/hex"
	"sync"
	"testing"
)

func TestDefaultRequestIDShape(t *testing.T) {
	id := defaultRequestID()
	if len(id) != 32 {
		t.Fatalf("defaultRequestID length = %d, want 32", len(id))
	}
	decoded, err := hex.DecodeString(id)
	if err != nil {
		t.Fatalf("defaultRequestID is not hex: %q: %v", id, err)
	}
	if len(decoded) != 16 {
		t.Fatalf("defaultRequestID decoded length = %d, want 16", len(decoded))
	}
}

func TestRequestIDDefaultValidator(t *testing.T) {
	valid := []string{
		"trace-abc-123",
		"0123456789abcdef0123456789abcdef",
		"base64url_ABC-123",
	}
	for _, id := range valid {
		if !validDefaultRequestID(id) {
			t.Fatalf("validDefaultRequestID(%q) = false, want true", id)
		}
	}

	invalid := []string{
		"bad id",
		"bad\tid",
		"bad\nid",
		"emoji-\xe2\x98\x83",
	}
	for _, id := range invalid {
		if validDefaultRequestID(id) {
			t.Fatalf("validDefaultRequestID(%q) = true, want false", id)
		}
	}
}

func TestRequestIDTooLongPolicy(t *testing.T) {
	if !requestIDTooLong("abcd", 3) {
		t.Fatal("requestIDTooLong did not reject overlong id")
	}
	if requestIDTooLong("abcd", 4) {
		t.Fatal("requestIDTooLong rejected id at the cap")
	}
	if requestIDTooLong("abcd", -1) {
		t.Fatal("requestIDTooLong rejected id when cap is disabled")
	}
}

func TestRequestIDInvalidHeaderPanicsAtConstruction(t *testing.T) {
	invalid := []string{
		"X Request ID",
		"X-Request-ID\r\nX-Evil",
		"X/Request/ID",
	}
	for _, header := range invalid {
		t.Run(header, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("RequestID accepted invalid header %q", header)
				}
			}()
			_ = RequestID(RequestIDOptions{Header: header})
		})
	}
}

func TestFastRequestIDGeneratorShapePure(t *testing.T) {
	gen := FastRequestIDGenerator()
	seen := make(map[string]struct{}, 1024)
	for i := 0; i < 1024; i++ {
		id := gen()
		if len(id) != 22 {
			t.Fatalf("id %q has length %d, want 22", id, len(id))
		}
		decoded, err := base64.RawURLEncoding.DecodeString(id)
		if err != nil {
			t.Fatalf("id %q is not valid base64url: %v", id, err)
		}
		if len(decoded) != 16 {
			t.Fatalf("id %q decodes to %d bytes, want 16", id, len(decoded))
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id after %d iterations: %q", i, id)
		}
		seen[id] = struct{}{}
	}
}

func TestFastRequestIDGeneratorConcurrentPure(t *testing.T) {
	gen := FastRequestIDGenerator()
	const workers, perWorker = 8, 256

	var (
		mu   sync.Mutex
		seen = make(map[string]struct{}, workers*perWorker)
		wg   sync.WaitGroup
	)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			local := make([]string, 0, perWorker)
			for i := 0; i < perWorker; i++ {
				local = append(local, gen())
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range local {
				if _, dup := seen[id]; dup {
					t.Errorf("duplicate id from concurrent generator: %q", id)
					return
				}
				seen[id] = struct{}{}
			}
		}()
	}
	wg.Wait()
}

func TestRequestIDRejectsUnsafeGeneratedValues(t *testing.T) {
	const max = 16
	acceptAll := func(string) bool { return true }

	if validRequestIDValue("bad\r\nid", max, acceptAll) {
		t.Fatal("validRequestIDValue accepted CRLF")
	}
	if validRequestIDValue("bad id", max, acceptAll) {
		t.Fatal("validRequestIDValue accepted whitespace")
	}
	if validRequestIDValue("0123456789abcdef0", max, acceptAll) {
		t.Fatal("validRequestIDValue accepted overlong value")
	}
	if !validRequestIDValue("0123456789abcdef", max, acceptAll) {
		t.Fatal("validRequestIDValue rejected safe value")
	}
}

func TestRequestIDFallsBackWhenGeneratorReturnsUnsafeValue(t *testing.T) {
	opt := RequestIDOptions{
		Generator: func() string { return "bad\r\nid" },
		Validator: func(string) bool {
			return true
		},
	}
	h := RequestID(opt)
	if h.Mw == nil {
		t.Fatal("RequestID returned nil middleware")
	}

	id := opt.Generator()
	if validRequestIDValue(id, 128, opt.Validator) {
		t.Fatal("test generator unexpectedly produced a safe value")
	}
	fallback := fallbackRequestID(128, opt.Validator)
	if !validRequestIDValue(fallback, 128, opt.Validator) {
		t.Fatal("default fallback request ID is not safe")
	}
}

func TestRequestIDFallbackHonorsMaxLength(t *testing.T) {
	acceptAll := func(string) bool { return true }
	for _, max := range []int{1, 8, 16, 31} {
		id := fallbackRequestID(max, acceptAll)
		if len(id) > max {
			t.Fatalf("fallbackRequestID(%d) length = %d, id=%q", max, len(id), id)
		}
		if !validRequestIDHeaderValue(id) {
			t.Fatalf("fallbackRequestID(%d) returned unsafe id %q", max, id)
		}
	}
}

func TestRequestIDFallbackSurvivesRejectAllValidator(t *testing.T) {
	rejectAll := func(string) bool { return false }
	id := fallbackRequestID(8, rejectAll)
	if id == "" || len(id) > 8 || !validRequestIDHeaderValue(id) {
		t.Fatalf("fallbackRequestID reject-all = %q, want bounded safe ID", id)
	}
}

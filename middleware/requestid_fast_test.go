//go:build cgo && gogo

package middleware_test

import (
	crand "crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

// TestFastRequestIDGeneratorShape covers the output format: every
// ID is 22 base64url characters (16 raw bytes encoded without
// padding), unique across a tight loop, and decodes back to 16
// distinct bytes.
func TestFastRequestIDGeneratorShape(t *testing.T) {
	gen := middleware.FastRequestIDGenerator()
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

// TestFastRequestIDGeneratorConcurrent stresses the pooled
// ChaCha8 instances under contention to confirm the generator is
// safe for use from multiple goroutines.
func TestFastRequestIDGeneratorConcurrent(t *testing.T) {
	gen := middleware.FastRequestIDGenerator()
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

// TestFastRequestIDGeneratorUnderRequestID verifies the
// drop-in path: install FastRequestIDGenerator as the
// RequestIDOptions.Generator, hit a route, see the ID echoed
// back in the X-Request-ID header.
func TestFastRequestIDGeneratorUnderRequestID(t *testing.T) {
	ts, err := gogo.NewTestServer(func(app *gogo.App) {
		app.Use(middleware.RequestID(middleware.RequestIDOptions{
			Generator: middleware.FastRequestIDGenerator(),
		}))
		app.Get("/x", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain", "ok")
		})
	})
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	defer ts.Close()

	resp, err := ts.Get("/x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	id := resp.Header.Get("X-Request-Id")
	if len(id) != 22 {
		t.Errorf("X-Request-ID = %q (len %d), want 22-char base64url", id, len(id))
	}
}

// BenchmarkRequestIDGeneratorDefault measures the cost of the
// stock crypto/rand-backed default generator (32 hex chars from 16
// random bytes). Inlines the same logic so the benchmark stays
// self-contained.
func BenchmarkRequestIDGeneratorDefault(b *testing.B) {
	gen := stockRequestIDForBench
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = gen()
	}
}

// stockRequestIDForBench mirrors middleware.defaultRequestID so
// the benchmark above can measure the stock generator without
// reaching into the package's unexported API.
func stockRequestIDForBench() string {
	var buf [16]byte
	if _, err := crand.Read(buf[:]); err != nil {
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(buf[:])
}

// BenchmarkRequestIDGeneratorFast measures the FastRequestIDGenerator's
// per-call cost. The pool warm-up happens in the first iteration so
// the steady-state cost is what you see after the first hundred
// iterations.
func BenchmarkRequestIDGeneratorFast(b *testing.B) {
	gen := middleware.FastRequestIDGenerator()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = gen()
	}
}

// guard against the http import going unused if any of the
// integration-style tests above get stripped during refactors.
var _ = http.StatusOK
var _ = strings.Builder{}
var _ = fmt.Sprintf

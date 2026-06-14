//go:build cgo && gogo

package gogo

import (
	"net"
	"strings"
	"testing"
)

// TestDefaultWorkerCount pins the ceil(1.5 × loops) default and the
// unset-hint (single-loop) fallback. The pool sizes to loop count, not
// NumCPU, so an env-less single-loop run doesn't over-subscribe workers
// against the loop thread.
func TestDefaultWorkerCount(t *testing.T) {
	saved := sharedCoreHint.Load()
	t.Cleanup(func() { sharedCoreHint.Store(saved) })

	cases := []struct {
		loops int
		want  int
	}{
		{0, 2}, // unset → treated as one loop → ceil(1.5)
		{1, 2},
		{2, 3},
		{3, 5},
		{4, 6},
		{8, 12},
	}
	for _, tc := range cases {
		sharedCoreHint.Store(uint64(uint32(tc.loops)))
		if got := defaultWorkerCount(); got != tc.want {
			t.Errorf("defaultWorkerCount() with loops=%d = %d, want %d", tc.loops, got, tc.want)
		}
	}
}

// TestSetSharedCoreHintFloors verifies a non-positive loop count is floored
// to one loop so the default never collapses to zero workers.
func TestSetSharedCoreHintFloors(t *testing.T) {
	saved := sharedCoreHint.Load()
	t.Cleanup(func() { sharedCoreHint.Store(saved) })

	setSharedCoreHint(0)
	if got := sharedCoreHintLoops(); got != 1 {
		t.Fatalf("setSharedCoreHint(0) stored loops=%d, want 1", got)
	}
	setSharedCoreHint(-5)
	if got := sharedCoreHintLoops(); got != 1 {
		t.Fatalf("setSharedCoreHint(-5) stored loops=%d, want 1", got)
	}
}

// TestSharedCoreHintResetUsesGenerationToken ensures an older lifecycle
// cleanup cannot clear a newer hint that happens to have the same loop count.
func TestSharedCoreHintResetUsesGenerationToken(t *testing.T) {
	saved := sharedCoreHint.Load()
	t.Cleanup(func() { sharedCoreHint.Store(saved) })
	sharedCoreHint.Store(0)

	oldToken := setSharedCoreHint(2)
	newToken := setSharedCoreHint(2)
	if oldToken == newToken {
		t.Fatal("setSharedCoreHint reused a generation token")
	}

	resetSharedCoreHintIfCurrent(oldToken)
	if got := sharedCoreHint.Load(); got != newToken {
		t.Fatalf("old token reset cleared current hint: got %d, want %d", got, newToken)
	}
	resetSharedCoreHintIfCurrent(newToken)
	if got := sharedCoreHint.Load(); got != 0 {
		t.Fatalf("current token reset left hint = %d, want 0", got)
	}
}

// TestSharedCoreHintResetsWhenIdle guards against a stale hint surviving the
// worker pool lifecycle: after a RunMultiCore(N) run closes, a later single
// App must fall back to the one-loop default (2 workers) rather than
// inheriting the old N-loop hint and over-subscribing. The reset fires when
// the last app drops its pool ref.
func TestSharedCoreHintResetsWhenIdle(t *testing.T) {
	savedHint := sharedCoreHint.Load()
	savedApps := sharedActiveApps.Load()
	savedStarted := sharedWorkersStarted
	savedGen := sharedWorkerGen
	savedGens := append([]*sharedWorkerGeneration(nil), sharedWorkerGens...)
	t.Cleanup(func() {
		sharedWorkerLifecycleMu.Lock()
		sharedCoreHint.Store(savedHint)
		sharedActiveApps.Store(savedApps)
		sharedWorkersStarted = savedStarted
		sharedWorkerGen = savedGen
		sharedWorkerGens = savedGens
		sharedWorkerLifecycleMu.Unlock()
	})

	// Start from a clean idle pool with a RunMultiCore(8)-style hint.
	token := setSharedCoreHint(8)
	sharedWorkerLifecycleMu.Lock()
	sharedActiveApps.Store(0)
	gen := &sharedWorkerGeneration{
		stop:          make(chan struct{}),
		drained:       make(chan struct{}),
		coreHintToken: token,
	}
	sharedWorkerGen = gen
	sharedWorkerGens = []*sharedWorkerGeneration{gen}
	sharedWorkersStarted = true
	sharedWorkerLifecycleMu.Unlock()
	if got := defaultWorkerCount(); got != 12 {
		t.Fatalf("defaultWorkerCount() with hint=8 = %d, want 12", got)
	}

	// One app acquires then releases the pool ref; the release drains the
	// active count to zero and must clear the hint.
	acquireSharedWorkerAppRef()
	stopSharedWorkersIfIdle()

	if got := sharedCoreHint.Load(); got != 0 {
		t.Fatalf("sharedCoreHint after idle = %d, want 0 (reset)", got)
	}
	if got := defaultWorkerCount(); got != 2 {
		t.Fatalf("defaultWorkerCount() after idle reset = %d, want 2 (one-loop fallback)", got)
	}
}

// TestRunMultiCoreSyncOnlyResetsSharedCoreHint covers the path that never
// starts the shared worker pool: sync-only RunMultiCore still publishes a
// worker-budget hint, and that hint must be cleared when the group finishes so
// a later single App does not inherit it.
func TestRunMultiCoreSyncOnlyResetsSharedCoreHint(t *testing.T) {
	savedHint := sharedCoreHint.Load()
	t.Cleanup(func() { sharedCoreHint.Store(savedHint) })
	sharedCoreHint.Store(0)

	handle, err := RunMultiCore(2, workerHintFreePort(t), func(app *App) {
		app.Get("/sync", func(res *Response, req *Request) {
			res.Send(200, "text/plain; charset=utf-8", "ok")
		})
	})
	if err != nil {
		t.Fatalf("RunMultiCore: %v", err)
	}
	if got := sharedCoreHintLoops(); got != 1 {
		handle.Shutdown()
		handle.Wait()
		t.Fatalf("sharedCoreHint loops after default RunMultiCore start = %d, want 1", got)
	}

	handle.Shutdown()
	handle.Wait()
	if got := sharedCoreHint.Load(); got != 0 {
		t.Fatalf("sharedCoreHint after sync-only RunMultiCore shutdown = %d, want 0", got)
	}
	if got := defaultWorkerCount(); got != 2 {
		t.Fatalf("defaultWorkerCount() after sync-only shutdown = %d, want 2", got)
	}
}

func TestRunMultiCoreBalancedPublishesLoopWorkerHint(t *testing.T) {
	savedHint := sharedCoreHint.Load()
	t.Cleanup(func() { sharedCoreHint.Store(savedHint) })
	sharedCoreHint.Store(0)

	handle, err := RunMultiCoreWithOptions(3, workerHintFreePort(t), func(app *App) {
		app.Get("/sync", func(res *Response, req *Request) {
			res.Send(200, "text/plain; charset=utf-8", "ok")
		})
	}, RunMultiCoreOptions{Mode: MultiCoreBalanced})
	if err != nil {
		t.Fatalf("RunMultiCoreWithOptions: %v", err)
	}
	if got := sharedCoreHintLoops(); got != 3 {
		handle.Shutdown()
		handle.Wait()
		t.Fatalf("sharedCoreHint loops after balanced RunMultiCore start = %d, want 3", got)
	}
	handle.Shutdown()
	handle.Wait()
}

// TestRunMultiCoreSetupPanicResetsSharedCoreHint covers the early error path:
// setup can fail before any shared route acquires a worker-pool ref.
func TestRunMultiCoreSetupPanicResetsSharedCoreHint(t *testing.T) {
	savedHint := sharedCoreHint.Load()
	t.Cleanup(func() { sharedCoreHint.Store(savedHint) })
	sharedCoreHint.Store(0)

	handle, err := RunMultiCore(2, workerHintFreePort(t), func(app *App) {
		panic("worker hint setup failed")
	})
	if err == nil {
		if handle != nil {
			handle.Shutdown()
			handle.Wait()
		}
		t.Fatal("RunMultiCore returned nil error after setup panic")
	}
	if !strings.Contains(err.Error(), "worker hint setup failed") {
		t.Fatalf("RunMultiCore error = %q, want setup panic context", err)
	}
	if handle != nil {
		handle.Shutdown()
		handle.Wait()
		t.Fatal("RunMultiCore returned a handle after setup panic")
	}
	if got := sharedCoreHint.Load(); got != 0 {
		t.Fatalf("sharedCoreHint after setup panic = %d, want 0", got)
	}
}

func sharedCoreHintLoops() int {
	return int(uint32(sharedCoreHint.Load()))
}

func workerHintFreePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

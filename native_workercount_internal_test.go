//go:build cgo && gogo

package gogo

import "testing"

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
		sharedCoreHint.Store(int32(tc.loops))
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
	if got := sharedCoreHint.Load(); got != 1 {
		t.Fatalf("setSharedCoreHint(0) stored %d, want 1", got)
	}
	setSharedCoreHint(-5)
	if got := sharedCoreHint.Load(); got != 1 {
		t.Fatalf("setSharedCoreHint(-5) stored %d, want 1", got)
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
	t.Cleanup(func() {
		sharedWorkerLifecycleMu.Lock()
		sharedCoreHint.Store(savedHint)
		sharedActiveApps.Store(savedApps)
		sharedWorkersStarted = savedStarted
		sharedWorkerLifecycleMu.Unlock()
	})

	// Start from a clean idle pool with a RunMultiCore(8)-style hint.
	sharedWorkerLifecycleMu.Lock()
	sharedActiveApps.Store(0)
	sharedWorkersStarted = false
	sharedWorkerLifecycleMu.Unlock()
	setSharedCoreHint(8)
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

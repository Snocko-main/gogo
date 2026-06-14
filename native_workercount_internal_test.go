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

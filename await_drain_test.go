package gogo

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitForDrainReturnsWhenBufferDropsWithoutSignal(t *testing.T) {
	var buffered atomic.Uint64
	buffered.Store(2)

	done := make(chan error, 1)
	go func() {
		done <- waitForDrain(buffered.Load, func() bool { return false }, 1)
	}()

	time.Sleep(drainPollInterval * 2)
	buffered.Store(0)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitForDrain returned error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("waitForDrain hung after buffered amount dropped below threshold")
	}
}

func TestWaitForDrainReturnsStreamAborted(t *testing.T) {
	var aborted atomic.Bool

	done := make(chan error, 1)
	go func() {
		done <- waitForDrain(func() uint64 { return 2 }, aborted.Load, 1)
	}()

	time.Sleep(drainPollInterval * 2)
	aborted.Store(true)

	select {
	case err := <-done:
		if !errors.Is(err, ErrStreamAborted) {
			t.Fatalf("waitForDrain error = %v, want %v", err, ErrStreamAborted)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("waitForDrain hung after abort")
	}
}

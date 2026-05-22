package gogo

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeWSHubAdapter struct {
	mu        sync.Mutex
	started   bool
	starts    int
	startErr  error
	published []WSHubMessage
}

func (a *fakeWSHubAdapter) Start(context.Context, func(WSHubMessage)) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.started = true
	a.starts++
	return a.startErr
}

func (a *fakeWSHubAdapter) Publish(_ context.Context, msg WSHubMessage) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.published = append(a.published, msg)
	return nil
}

func (a *fakeWSHubAdapter) Close() error { return nil }

func (a *fakeWSHubAdapter) snapshot() (bool, int, []WSHubMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	published := append([]WSHubMessage(nil), a.published...)
	return a.started, a.starts, published
}

func (a *fakeWSHubAdapter) setStartErr(err error) {
	a.mu.Lock()
	a.startErr = err
	a.mu.Unlock()
}

func waitForAdapterPublish(t *testing.T, adapter *fakeWSHubAdapter, want int) []WSHubMessage {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, _, published := adapter.snapshot()
		if len(published) >= want {
			return published
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _, published := adapter.snapshot()
	t.Fatalf("published count = %d, want %d", len(published), want)
	return nil
}

func TestWSHubPublishCopiesMessageBeforeAdapter(t *testing.T) {
	adapter := &fakeWSHubAdapter{}
	hub := NewWSHub(WithWSHubNodeID("node-a"), WithWSHubAdapter(adapter))
	defer hub.Close()

	payload := []byte("hello")
	if err := hub.Publish("room", payload, Text); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	payload[0] = 'X'

	published := waitForAdapterPublish(t, adapter, 1)
	started, _, _ := adapter.snapshot()
	if !started {
		t.Fatal("adapter was not started")
	}
	got := published[0]
	if got.NodeID != "node-a" || got.Topic != "room" || got.OpCode != Text {
		t.Fatalf("published metadata = %#v", got)
	}
	if string(got.Message) != "hello" {
		t.Fatalf("published payload = %q, want hello", got.Message)
	}
}

func TestWSHubStartReturnsAdapterError(t *testing.T) {
	want := errors.New("redis down")
	adapter := &fakeWSHubAdapter{startErr: want}
	hub := NewWSHub(
		WithWSHubAdapter(adapter),
		WithWSHubAdapterErrorHandler(nil),
	)
	defer hub.Close()

	if err := hub.Start(); !errors.Is(err, want) {
		t.Fatalf("Start error = %v, want %v", err, want)
	}
	if err := hub.Publish("room", []byte("hello"), Text); err != nil {
		t.Fatalf("Publish during Start backoff should only enqueue: %v", err)
	}
	adapter.setStartErr(nil)
	if err := hub.Start(); err != nil {
		t.Fatalf("explicit Start retry after transient error: %v", err)
	}
	if err := hub.Publish("room", []byte("hello"), Text); err != nil {
		t.Fatalf("Publish after explicit Start retry: %v", err)
	}
	waitForAdapterPublish(t, adapter, 1)
	_, starts, _ := adapter.snapshot()
	if starts != 2 {
		t.Fatalf("adapter starts = %d, want 2", starts)
	}
}

type ctxBlockingWSHubAdapter struct {
	entered chan struct{}
}

func (a *ctxBlockingWSHubAdapter) Start(context.Context, func(WSHubMessage)) error {
	return nil
}

func (a *ctxBlockingWSHubAdapter) Publish(ctx context.Context, _ WSHubMessage) error {
	select {
	case a.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func (a *ctxBlockingWSHubAdapter) Close() error { return nil }

type failingPublishWSHubAdapter struct {
	err error
}

func (a *failingPublishWSHubAdapter) Start(context.Context, func(WSHubMessage)) error {
	return nil
}

func (a *failingPublishWSHubAdapter) Publish(context.Context, WSHubMessage) error {
	return a.err
}

func (a *failingPublishWSHubAdapter) Close() error { return nil }

type panicOnceWSHubAdapter struct {
	mu        sync.Mutex
	panicked  bool
	published []WSHubMessage
}

func (a *panicOnceWSHubAdapter) Start(context.Context, func(WSHubMessage)) error {
	return nil
}

func (a *panicOnceWSHubAdapter) Publish(context.Context, WSHubMessage) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.panicked {
		a.panicked = true
		panic("redis client panic")
	}
	a.published = append(a.published, WSHubMessage{})
	return nil
}

func (a *panicOnceWSHubAdapter) Close() error { return nil }

func (a *panicOnceWSHubAdapter) publishedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.published)
}

func TestWSHubCloseBoundsInFlightAdapterPublish(t *testing.T) {
	adapter := &ctxBlockingWSHubAdapter{entered: make(chan struct{}, 1)}
	hub := NewWSHub(
		WithWSHubAdapter(adapter),
		WithWSHubAdapterPublishTimeout(time.Hour),
		WithWSHubCloseTimeout(20*time.Millisecond),
	)

	if err := hub.Publish("room", []byte("hello"), Text); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case <-adapter.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("adapter publish was not started")
	}

	done := make(chan error, 1)
	go func() {
		done <- hub.Close()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close waited too long for adapter publish")
	}
}

func TestWSHubReportsAsyncAdapterError(t *testing.T) {
	want := errors.New("redis publish failed")
	errs := make(chan error, 1)
	hub := NewWSHub(
		WithWSHubAdapter(&failingPublishWSHubAdapter{err: want}),
		WithWSHubAdapterErrorHandler(func(err error) {
			errs <- err
		}),
	)
	defer hub.Close()

	if err := hub.Publish("room", []byte("hello"), Text); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case got := <-errs:
		if !errors.Is(got, want) {
			t.Fatalf("async adapter error = %v, want %v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("async adapter error was not reported")
	}
}

func TestWSHubAdapterWorkerRecoversPanic(t *testing.T) {
	errs := make(chan error, 1)
	adapter := &panicOnceWSHubAdapter{}
	hub := NewWSHub(
		WithWSHubAdapter(adapter),
		WithWSHubAdapterErrorHandler(func(err error) {
			errs <- err
		}),
	)
	defer hub.Close()

	if err := hub.Publish("room", []byte("first"), Text); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	select {
	case err := <-errs:
		if !strings.Contains(err.Error(), "adapter panic") {
			t.Fatalf("async adapter error = %v, want adapter panic", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("adapter panic was not reported")
	}

	if err := hub.Publish("room", []byte("second"), Text); err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if adapter.publishedCount() == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("adapter worker did not publish after panic")
}

func TestWSHubRegistryRejectsDifferentHubForSocket(t *testing.T) {
	key := uintptr(1)
	h1 := NewWSHub()
	h2 := NewWSHub()
	t.Cleanup(func() {
		unregisterWSHub(key, h1)
		unregisterWSHub(key, h2)
	})

	if !reserveWSHub(key, h1) {
		t.Fatal("first hub could not reserve socket")
	}
	if reserveWSHub(key, h2) {
		t.Fatal("second hub reserved socket already owned by first hub")
	}
	unregisterWSHub(key, h1)
	if !reserveWSHub(key, h2) {
		t.Fatal("second hub could not reserve socket after first hub released it")
	}
}

func TestWSHubRejectsInvalidOpCode(t *testing.T) {
	hub := NewWSHub()
	if err := hub.Publish("room", []byte("hello"), OpCode(99)); !errors.Is(err, ErrWSHubInvalidOpCode) {
		t.Fatalf("Publish error = %v, want ErrWSHubInvalidOpCode", err)
	}
	if err := hub.PublishBatch([]PublishMessage{{
		Topic:   "room",
		Message: []byte("hello"),
		OpCode:  OpCode(99),
	}}); !errors.Is(err, ErrWSHubInvalidOpCode) {
		t.Fatalf("PublishBatch error = %v, want ErrWSHubInvalidOpCode", err)
	}
}

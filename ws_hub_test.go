package gogo

import (
	"context"
	"errors"
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
	hub := NewWSHub(WithWSHubAdapter(adapter))

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

func TestWSHubCloseBoundsInFlightAdapterPublish(t *testing.T) {
	adapter := &ctxBlockingWSHubAdapter{entered: make(chan struct{}, 1)}
	hub := NewWSHub(
		WithWSHubAdapter(adapter),
		WithWSHubAdapterPublishTimeout(20*time.Millisecond),
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

package gogo

import (
	"context"
	"errors"
	"testing"
)

type fakeWSHubAdapter struct {
	started   bool
	starts    int
	startErr  error
	published []WSHubMessage
}

func (a *fakeWSHubAdapter) Start(context.Context, func(WSHubMessage)) error {
	a.started = true
	a.starts++
	return a.startErr
}

func (a *fakeWSHubAdapter) Publish(_ context.Context, msg WSHubMessage) error {
	a.published = append(a.published, msg)
	return nil
}

func (a *fakeWSHubAdapter) Close() error { return nil }

func TestWSHubPublishCopiesMessageBeforeAdapter(t *testing.T) {
	adapter := &fakeWSHubAdapter{}
	hub := NewWSHub(WithWSHubNodeID("node-a"), WithWSHubAdapter(adapter))

	payload := []byte("hello")
	if err := hub.Publish("room", payload, Text); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	payload[0] = 'X'

	if !adapter.started {
		t.Fatal("adapter was not started")
	}
	if len(adapter.published) != 1 {
		t.Fatalf("published count = %d, want 1", len(adapter.published))
	}
	got := adapter.published[0]
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
	adapter.startErr = nil
	if err := hub.Publish("room", []byte("hello"), Text); err != nil {
		t.Fatalf("Publish after transient Start error: %v", err)
	}
	if adapter.starts != 2 {
		t.Fatalf("adapter starts = %d, want 2", adapter.starts)
	}
	if len(adapter.published) != 1 {
		t.Fatalf("published count = %d, want 1", len(adapter.published))
	}
}

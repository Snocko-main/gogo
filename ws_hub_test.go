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
	subs      []string
	unsubs    []string
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

func (a *fakeWSHubAdapter) Subscribe(_ context.Context, topic string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.subs = append(a.subs, topic)
	return nil
}

func (a *fakeWSHubAdapter) Unsubscribe(_ context.Context, topic string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.unsubs = append(a.unsubs, topic)
	return nil
}

func (a *fakeWSHubAdapter) snapshot() (bool, int, []WSHubMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	published := append([]WSHubMessage(nil), a.published...)
	return a.started, a.starts, published
}

func (a *fakeWSHubAdapter) topicSnapshot() (subs, unsubs []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.subs...), append([]string(nil), a.unsubs...)
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

func waitForAdapterTopics(t *testing.T, adapter *fakeWSHubAdapter, wantSubs, wantUnsubs int) ([]string, []string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		subs, unsubs := adapter.topicSnapshot()
		if len(subs) >= wantSubs && len(unsubs) >= wantUnsubs {
			return subs, unsubs
		}
		time.Sleep(10 * time.Millisecond)
	}
	subs, unsubs := adapter.topicSnapshot()
	t.Fatalf("topic ops = %d subs/%d unsubs, want %d/%d", len(subs), len(unsubs), wantSubs, wantUnsubs)
	return nil, nil
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

func TestWSHubQueuesAdapterTopicSubscriptions(t *testing.T) {
	adapter := &fakeWSHubAdapter{}
	hub := NewWSHub(WithWSHubAdapter(adapter), WithWSHubAdapterWorkers(4))
	defer hub.Close()

	hub.mu.Lock()
	hub.sockets[1] = &hubSocket{token: 1, topics: make(map[string]struct{})}
	hub.sockets[2] = &hubSocket{token: 2, topics: make(map[string]struct{})}
	hub.mu.Unlock()

	if token, first := hub.addMembership(1, 1, "room"); token == 0 || !first {
		t.Fatalf("first membership = token %d first %v, want token and first", token, first)
	}
	hub.queueAdapterTopic("room")
	subs, _ := waitForAdapterTopics(t, adapter, 1, 0)
	if len(subs) != 1 || subs[0] != "room" {
		t.Fatalf("subs = %#v, want [room]", subs)
	}

	if _, first := hub.addMembership(2, 2, "room"); first {
		t.Fatal("second membership should not be first")
	}
	hub.removeMembershipIfCurrent(1, 1, "room")
	_, unsubs := adapter.topicSnapshot()
	if len(unsubs) != 0 {
		t.Fatalf("unsubs after first leave = %#v, want none", unsubs)
	}
	hub.removeMembershipIfCurrent(2, 2, "room")
	_, unsubs = waitForAdapterTopics(t, adapter, 1, 1)
	if len(unsubs) != 1 || unsubs[0] != "room" {
		t.Fatalf("unsubs = %#v, want [room]", unsubs)
	}
}

func TestWSHubReconcilesTopicToLatestDesiredState(t *testing.T) {
	adapter := &fakeWSHubAdapter{}
	hub := NewWSHub(WithWSHubAdapter(adapter))
	defer hub.Close()

	hub.mu.Lock()
	hub.sockets[1] = &hubSocket{token: 1, topics: map[string]struct{}{"room": {}}}
	hub.members["room"] = map[uintptr]struct{}{1: {}}
	hub.adapterTopicApplied["room"] = true
	last := hub.removeMembershipLocked(1, "room")
	if !last {
		t.Fatal("removeMembershipLocked should see last local member")
	}
	hub.sockets[2] = &hubSocket{token: 2, topics: map[string]struct{}{"room": {}}}
	hub.members["room"] = map[uintptr]struct{}{2: {}}
	hub.adapterTopicDirty["room"] = struct{}{}
	hub.mu.Unlock()

	hub.reconcileAdapterTopic("room")
	subs, unsubs := adapter.topicSnapshot()
	if len(subs) != 0 || len(unsubs) != 0 {
		t.Fatalf("topic ops = subs %#v unsubs %#v, want no-op because desired stayed subscribed", subs, unsubs)
	}
}

func TestWSHubAdapterTopicWorkersOption(t *testing.T) {
	hub := NewWSHub(WithWSHubAdapterTopicWorkers(3))
	if hub.adapterTopicWorkers != 3 {
		t.Fatalf("adapterTopicWorkers = %d, want 3", hub.adapterTopicWorkers)
	}
}

func TestWSHubMembershipRequiresCurrentToken(t *testing.T) {
	hub := NewWSHub()
	hub.mu.Lock()
	hub.sockets[1] = &hubSocket{token: 7, topics: make(map[string]struct{})}
	hub.mu.Unlock()

	if token, first := hub.addMembership(1, 0, "room"); token != 0 || first {
		t.Fatalf("zero-token addMembership = token %d first %v, want rejected", token, first)
	}
	if token, first := hub.addMembership(1, 6, "room"); token != 0 || first {
		t.Fatalf("stale-token addMembership = token %d first %v, want rejected", token, first)
	}
	if token, first := hub.addMembership(1, 7, "room"); token != 7 || !first {
		t.Fatalf("current-token addMembership = token %d first %v, want accepted", token, first)
	}

	hub.removeMembershipIfCurrent(1, 0, "room")
	hub.mu.RLock()
	_, stillMember := hub.members["room"][uintptr(1)]
	hub.mu.RUnlock()
	if !stillMember {
		t.Fatal("zero-token removeMembershipIfCurrent removed current socket")
	}
	hub.removeMembershipIfCurrent(1, 7, "room")
	hub.mu.RLock()
	_, stillMember = hub.members["room"][uintptr(1)]
	hub.mu.RUnlock()
	if stillMember {
		t.Fatal("current-token removeMembershipIfCurrent did not remove socket")
	}
}

func TestWSHubRestoreMembershipAfterUnsubscribeFailure(t *testing.T) {
	hub := NewWSHub()
	hub.mu.Lock()
	hub.sockets[1] = &hubSocket{token: 7, topics: make(map[string]struct{})}
	hub.mu.Unlock()

	if token, first := hub.addMembership(1, 7, "room"); token != 7 || !first {
		t.Fatalf("addMembership = token %d first %v, want token 7 first", token, first)
	}
	removed, last := hub.removeMembershipForUnsubscribe(1, 7, "room")
	if !removed || !last {
		t.Fatalf("removeMembershipForUnsubscribe = removed %v last %v, want true/true", removed, last)
	}
	if first, restored := hub.restoreMembershipIfCurrent(1, 7, "room"); !first || !restored {
		t.Fatalf("restoreMembershipIfCurrent = first %v restored %v, want true/true", first, restored)
	}
	hub.mu.RLock()
	_, socketTopic := hub.sockets[1].topics["room"]
	_, member := hub.members["room"][uintptr(1)]
	hub.mu.RUnlock()
	if !socketTopic || !member {
		t.Fatalf("restored socketTopic=%v member=%v, want both true", socketTopic, member)
	}
}

func TestWSHubRestoreMembershipRejectsStaleToken(t *testing.T) {
	hub := NewWSHub()
	hub.mu.Lock()
	hub.sockets[1] = &hubSocket{token: 7, topics: make(map[string]struct{})}
	hub.mu.Unlock()

	if first, restored := hub.restoreMembershipIfCurrent(1, 6, "room"); first || restored {
		t.Fatalf("restoreMembershipIfCurrent = first %v restored %v, want false/false", first, restored)
	}
	hub.mu.RLock()
	_, member := hub.members["room"][uintptr(1)]
	hub.mu.RUnlock()
	if member {
		t.Fatal("stale restore added membership")
	}
}

func TestWSHubRestoreMembershipRejectsExistingTopic(t *testing.T) {
	hub := NewWSHub()
	hub.mu.Lock()
	hub.sockets[1] = &hubSocket{token: 7, topics: map[string]struct{}{"room": {}}}
	hub.members["room"] = map[uintptr]struct{}{1: {}}
	hub.mu.Unlock()

	if first, restored := hub.restoreMembershipIfCurrent(1, 7, "room"); first || restored {
		t.Fatalf("restoreMembershipIfCurrent = first %v restored %v, want false/false", first, restored)
	}
}

func TestWSHubRemoveMembershipForUnsubscribeRejectsClosedHub(t *testing.T) {
	hub := NewWSHub()
	hub.mu.Lock()
	hub.sockets[1] = &hubSocket{token: 7, topics: map[string]struct{}{"room": {}}}
	hub.members["room"] = map[uintptr]struct{}{1: {}}
	hub.mu.Unlock()
	hub.closed.Store(true)

	removed, last := hub.removeMembershipForUnsubscribe(1, 7, "room")
	if removed || last {
		t.Fatalf("removeMembershipForUnsubscribe = removed %v last %v, want false/false", removed, last)
	}
	hub.mu.RLock()
	_, member := hub.members["room"][uintptr(1)]
	hub.mu.RUnlock()
	if !member {
		t.Fatal("closed remove mutated membership")
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

type panicOnceTopicWSHubAdapter struct {
	fakeWSHubAdapter
	panicMu  sync.Mutex
	panicked bool
}

func (a *panicOnceTopicWSHubAdapter) Subscribe(ctx context.Context, topic string) error {
	a.panicMu.Lock()
	if !a.panicked {
		a.panicked = true
		a.panicMu.Unlock()
		panic("redis subscribe panic")
	}
	a.panicMu.Unlock()
	return a.fakeWSHubAdapter.Subscribe(ctx, topic)
}

type failingTopicWSHubAdapter struct {
	fakeWSHubAdapter
	mu      sync.Mutex
	subErr  error
	subCall int
}

func (a *failingTopicWSHubAdapter) Subscribe(context.Context, string) error {
	a.mu.Lock()
	a.subCall++
	err := a.subErr
	a.mu.Unlock()
	return err
}

func (a *failingTopicWSHubAdapter) subscribeCalls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.subCall
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

func TestWSHubAdapterErrorRateLimitIsGlobal(t *testing.T) {
	errs := make(chan error, 4)
	hub := NewWSHub(WithWSHubAdapterErrorHandler(func(err error) {
		errs <- err
	}))

	hub.reportAdapterError(errors.New("redis A"))
	hub.reportAdapterError(errors.New("redis B"))
	hub.reportAdapterError(errors.New("redis A"))
	if got := len(errs); got != 1 {
		t.Fatalf("reported errors = %d, want 1", got)
	}

	hub.adapterErrMu.Lock()
	hub.adapterErrNext = time.Now().Add(-time.Second)
	hub.adapterErrMu.Unlock()
	hub.reportAdapterError(errors.New("redis C"))
	if got := len(errs); got != 2 {
		t.Fatalf("reported errors after window = %d, want 2", got)
	}
	got := <-errs
	if got.Error() != "redis A" {
		t.Fatalf("first error = %v, want redis A", got)
	}
	got = <-errs
	if !strings.Contains(got.Error(), "suppressed 2") {
		t.Fatalf("second error = %v, want suppressed count", got)
	}
}

func TestWSHubAdapterPanicIsRateLimited(t *testing.T) {
	errs := make(chan error, 2)
	hub := NewWSHub(WithWSHubAdapterErrorHandler(func(err error) {
		errs <- err
	}))

	hub.reportAdapterError(wsHubAdapterPanicError{recovered: "boom"})
	hub.reportAdapterError(wsHubAdapterPanicError{recovered: "boom"})
	if got := len(errs); got != 1 {
		t.Fatalf("reported panics = %d, want 1", got)
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

func TestWSHubAdapterErrorHandlerPanicDoesNotStopWorker(t *testing.T) {
	adapter := &panicOnceWSHubAdapter{}
	hub := NewWSHub(
		WithWSHubAdapter(adapter),
		WithWSHubAdapterErrorHandler(func(error) {
			panic("error handler failed")
		}),
	)
	defer hub.Close()

	if err := hub.Publish("room", []byte("first"), Text); err != nil {
		t.Fatalf("first Publish: %v", err)
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
	t.Fatal("adapter worker stopped after error handler panic")
}

func TestWSHubAdapterTopicRetryUsesBackoff(t *testing.T) {
	adapter := &failingTopicWSHubAdapter{subErr: errors.New("redis subscribe failed")}
	hub := NewWSHub(
		WithWSHubAdapter(adapter),
		WithWSHubAdapterErrorHandler(nil),
	)
	defer hub.Close()

	hub.mu.Lock()
	hub.sockets[1] = &hubSocket{token: 1, topics: make(map[string]struct{})}
	hub.mu.Unlock()

	if token, first := hub.addMembership(1, 1, "room"); token == 0 || !first {
		t.Fatalf("membership = token %d first %v, want token and first", token, first)
	}
	hub.queueAdapterTopic("room")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if adapter.subscribeCalls() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := adapter.subscribeCalls(); got != 1 {
		t.Fatalf("Subscribe calls before backoff window = %d, want 1", got)
	}
	time.Sleep(defaultWSHubStartBackoff / 2)
	if got := adapter.subscribeCalls(); got != 1 {
		t.Fatalf("Subscribe calls during backoff = %d, want 1", got)
	}
}

func TestWSHubAdapterTopicRetryUsesSingleTimer(t *testing.T) {
	hub := NewWSHub(
		WithWSHubAdapter(&fakeWSHubAdapter{}),
		WithWSHubAdapterErrorHandler(nil),
	)
	defer hub.Close()

	hub.finishAdapterTopic("room.a", true, errors.New("redis down"))
	hub.mu.Lock()
	first := hub.adapterTopicRetry
	hub.mu.Unlock()
	if first == nil {
		t.Fatal("adapterTopicRetry timer was not scheduled")
	}

	hub.finishAdapterTopic("room.b", true, errors.New("redis down"))
	hub.mu.Lock()
	second := hub.adapterTopicRetry
	hub.mu.Unlock()
	if second != first {
		t.Fatal("adapter topic retry scheduled more than one timer")
	}
}

func TestWSHubFinishAdapterTopicAfterCloseDoesNotDirtyMaps(t *testing.T) {
	hub := NewWSHub(
		WithWSHubAdapter(&fakeWSHubAdapter{}),
		WithWSHubAdapterErrorHandler(nil),
	)
	if err := hub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	hub.finishAdapterTopic("room", true, errors.New("late redis error"))
	hub.mu.RLock()
	dirty := len(hub.adapterTopicDirty)
	retries := len(hub.adapterTopicRetryAt)
	hub.mu.RUnlock()
	if dirty != 0 || retries != 0 {
		t.Fatalf("topic maps after close = dirty %d retries %d, want empty", dirty, retries)
	}
}

func TestWSHubSignalAdapterTopicDoesNotRaceClosedQueue(t *testing.T) {
	hub := NewWSHub(WithWSHubAdapter(&fakeWSHubAdapter{}))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				hub.signalAdapterTopic()
			}
		}()
	}
	close(start)
	if err := hub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
}

func TestWSHubAdapterTopicWorkerRecoversPanic(t *testing.T) {
	errs := make(chan error, 1)
	adapter := &panicOnceTopicWSHubAdapter{}
	hub := NewWSHub(
		WithWSHubAdapter(adapter),
		WithWSHubAdapterErrorHandler(func(err error) {
			errs <- err
		}),
	)
	defer hub.Close()

	hub.mu.Lock()
	hub.sockets[1] = &hubSocket{token: 1, topics: make(map[string]struct{})}
	hub.mu.Unlock()

	if token, first := hub.addMembership(1, 1, "room"); token == 0 || !first {
		t.Fatalf("membership = token %d first %v, want token and first", token, first)
	}
	hub.queueAdapterTopic("room")
	select {
	case err := <-errs:
		if !strings.Contains(err.Error(), "adapter panic") {
			t.Fatalf("async adapter error = %v, want adapter panic", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("adapter topic panic was not reported")
	}

	subs, _ := waitForAdapterTopics(t, &adapter.fakeWSHubAdapter, 1, 0)
	if len(subs) != 1 || subs[0] != "room" {
		t.Fatalf("subs = %#v, want [room]", subs)
	}
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

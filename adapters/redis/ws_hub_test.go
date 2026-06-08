package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
	goredis "github.com/redis/go-redis/v9"
)

func TestWireRoundTrip(t *testing.T) {
	in := gogo.WSHubMessage{
		NodeID:  "node-a",
		Topic:   "room.general",
		Message: []byte{0, 1, 2, 255},
		OpCode:  gogo.Binary,
	}
	payload, err := encodeMessage(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := decodeMessage(in.Topic, payload, defaultMaxMessage)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.NodeID != in.NodeID || out.Topic != in.Topic || out.OpCode != in.OpCode {
		t.Fatalf("decoded metadata = %#v, want %#v", out, in)
	}
	if string(out.Message) != string(in.Message) {
		t.Fatalf("decoded payload = %v, want %v", out.Message, in.Message)
	}

	payload[len(payload)-1] = 42
	if out.Message[len(out.Message)-1] == 42 {
		t.Fatal("decoded message aliases the wire payload")
	}
}

func TestRedisGlobEscape(t *testing.T) {
	got := redisGlobEscape(`app[prod]:ws:*?\`)
	want := `app\[prod\]:ws:\*\?\\`
	if got != want {
		t.Fatalf("redisGlobEscape = %q, want %q", got, want)
	}
}

func TestNewSetsDefaultChannelSize(t *testing.T) {
	adapter, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer adapter.Close()
	if adapter.chSize != defaultChannelSize {
		t.Fatalf("channel size = %d, want %d", adapter.chSize, defaultChannelSize)
	}
	if adapter.maxMsg != defaultMaxMessage {
		t.Fatalf("max message size = %d, want %d", adapter.maxMsg, defaultMaxMessage)
	}
}

func TestNewClientOptions(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer client.Close()
	adapter, err := NewClientOptions(client, Options{
		ChannelPrefix:        "custom:",
		ChannelSize:          17,
		ChannelSendTimeout:   2 * time.Second,
		MaxMessageSize:       32,
		DynamicSubscriptions: true,
	})
	if err != nil {
		t.Fatalf("NewClientOptions: %v", err)
	}
	if adapter.prefix != "custom:" {
		t.Fatalf("prefix = %q, want custom:", adapter.prefix)
	}
	if adapter.chSize != 17 {
		t.Fatalf("channel size = %d, want 17", adapter.chSize)
	}
	if adapter.chSend != 2*time.Second {
		t.Fatalf("channel send timeout = %v, want 2s", adapter.chSend)
	}
	if adapter.maxMsg != 32 {
		t.Fatalf("max message size = %d, want 32", adapter.maxMsg)
	}
	if !adapter.dynamic {
		t.Fatal("dynamic subscriptions = false, want true")
	}
}

func TestDynamicSubscribeRequiresStart(t *testing.T) {
	adapter := &Adapter{dynamic: true}
	if err := adapter.Subscribe(t.Context(), "room"); err == nil {
		t.Fatal("Subscribe succeeded before Start")
	}
}

func TestStaticSubscribeNoop(t *testing.T) {
	adapter := &Adapter{}
	if err := adapter.Subscribe(t.Context(), "room"); err != nil {
		t.Fatalf("Subscribe static adapter: %v", err)
	}
	if err := adapter.Unsubscribe(t.Context(), "room"); err != nil {
		t.Fatalf("Unsubscribe static adapter: %v", err)
	}
}

func TestRedisWSHubIntegrationPublishesAcrossAdapters(t *testing.T) {
	subClient := redisIntegrationClient(t)
	defer subClient.Close()
	pubClient := redisIntegrationClient(t)
	defer pubClient.Close()

	prefix := redisWSHubTestPrefix(t)
	topic := "room.general"
	sub := redisWSHubTestAdapter(t, subClient, Options{ChannelPrefix: prefix})
	defer sub.Close()
	pub := redisWSHubTestAdapter(t, pubClient, Options{ChannelPrefix: prefix})
	defer pub.Close()

	delivered := make(chan gogo.WSHubMessage, 8)
	if err := sub.Start(t.Context(), recordRedisWSHubMessages(delivered)); err != nil {
		t.Fatalf("subscriber Start: %v", err)
	}

	msg := gogo.WSHubMessage{
		NodeID:  "node-a",
		Topic:   topic,
		Message: []byte("hello from node a"),
		OpCode:  gogo.Text,
	}
	publishCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	if err := pub.Publish(publishCtx, msg); err != nil {
		cancel()
		t.Fatalf("publisher Publish: %v", err)
	}
	cancel()

	got := waitForRedisWSHubMessage(t, delivered, 2*time.Second)
	assertRedisWSHubMessage(t, got, msg)
}

func TestRedisWSHubIntegrationDynamicSubscribeUnsubscribe(t *testing.T) {
	subClient := redisIntegrationClient(t)
	defer subClient.Close()
	pubClient := redisIntegrationClient(t)
	defer pubClient.Close()

	prefix := redisWSHubTestPrefix(t)
	topic := "room.dynamic"
	channel := prefix + topic
	sub := redisWSHubTestAdapter(t, subClient, Options{
		ChannelPrefix:        prefix,
		DynamicSubscriptions: true,
	})
	defer sub.Close()
	pub := redisWSHubTestAdapter(t, pubClient, Options{ChannelPrefix: prefix})
	defer pub.Close()

	delivered := make(chan gogo.WSHubMessage, 8)
	if err := sub.Start(t.Context(), recordRedisWSHubMessages(delivered)); err != nil {
		t.Fatalf("subscriber Start: %v", err)
	}

	subCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	if err := sub.Subscribe(subCtx, topic); err != nil {
		cancel()
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()
	waitForRedisSubscribers(t, pubClient, channel, 1)

	first := gogo.WSHubMessage{
		NodeID:  "node-a",
		Topic:   topic,
		Message: []byte("before unsubscribe"),
		OpCode:  gogo.Text,
	}
	publishCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	if err := pub.Publish(publishCtx, first); err != nil {
		cancel()
		t.Fatalf("Publish before unsubscribe: %v", err)
	}
	cancel()
	got := waitForRedisWSHubMessage(t, delivered, 2*time.Second)
	assertRedisWSHubMessage(t, got, first)

	unsubCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	if err := sub.Unsubscribe(unsubCtx, topic); err != nil {
		cancel()
		t.Fatalf("Unsubscribe: %v", err)
	}
	cancel()
	waitForRedisSubscribers(t, pubClient, channel, 0)

	after := gogo.WSHubMessage{
		NodeID:  "node-a",
		Topic:   topic,
		Message: []byte("after unsubscribe"),
		OpCode:  gogo.Text,
	}
	publishCtx, cancel = context.WithTimeout(t.Context(), time.Second)
	if err := pub.Publish(publishCtx, after); err != nil {
		cancel()
		t.Fatalf("Publish after unsubscribe: %v", err)
	}
	cancel()
	expectNoRedisWSHubMessage(t, delivered, 200*time.Millisecond)
}

func TestRedisWSHubIntegrationCloseStopsDynamicSubscription(t *testing.T) {
	subClient := redisIntegrationClient(t)
	defer subClient.Close()
	pubClient := redisIntegrationClient(t)
	defer pubClient.Close()

	prefix := redisWSHubTestPrefix(t)
	topic := "room.close"
	channel := prefix + topic
	sub := redisWSHubTestAdapter(t, subClient, Options{
		ChannelPrefix:        prefix,
		DynamicSubscriptions: true,
	})
	pub := redisWSHubTestAdapter(t, pubClient, Options{ChannelPrefix: prefix})
	defer pub.Close()

	delivered := make(chan gogo.WSHubMessage, 8)
	if err := sub.Start(t.Context(), recordRedisWSHubMessages(delivered)); err != nil {
		t.Fatalf("subscriber Start: %v", err)
	}

	subCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	if err := sub.Subscribe(subCtx, topic); err != nil {
		cancel()
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()
	waitForRedisSubscribers(t, pubClient, channel, 1)

	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitForRedisSubscribers(t, pubClient, channel, 0)

	closedMsg := gogo.WSHubMessage{
		NodeID:  "node-a",
		Topic:   topic,
		Message: []byte("closed"),
		OpCode:  gogo.Text,
	}
	publishCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	if err := sub.Publish(publishCtx, closedMsg); !errors.Is(err, errAdapterClosed) {
		cancel()
		t.Fatalf("closed adapter Publish error = %v, want %v", err, errAdapterClosed)
	}
	cancel()

	subCtx, cancel = context.WithTimeout(t.Context(), time.Second)
	if err := sub.Subscribe(subCtx, topic); !errors.Is(err, errAdapterClosed) {
		cancel()
		t.Fatalf("closed adapter Subscribe error = %v, want %v", err, errAdapterClosed)
	}
	cancel()

	publishCtx, cancel = context.WithTimeout(t.Context(), time.Second)
	if err := pub.Publish(publishCtx, closedMsg); err != nil {
		cancel()
		t.Fatalf("Publish after close: %v", err)
	}
	cancel()
	expectNoRedisWSHubMessage(t, delivered, 200*time.Millisecond)
}

func TestDecodeRejectsOversizedMessage(t *testing.T) {
	in := gogo.WSHubMessage{
		NodeID:  "node-a",
		Topic:   "room.general",
		Message: []byte("hello"),
		OpCode:  gogo.Text,
	}
	payload, err := encodeMessage(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := decodeMessage(in.Topic, payload, len(in.Message)-1); err == nil {
		t.Fatal("decode succeeded for oversized message")
	}
}

func TestDecodeRejectsInvalidOpcode(t *testing.T) {
	in := gogo.WSHubMessage{
		NodeID:  "node-a",
		Topic:   "room.general",
		Message: []byte("hello"),
		OpCode:  gogo.Text,
	}
	payload, err := encodeMessage(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	payload[1] = 99
	if _, err := decodeMessage(in.Topic, payload, defaultMaxMessage); err == nil {
		t.Fatal("decode succeeded for invalid opcode")
	}
}

func TestEncodeRejectsInvalidOpcode(t *testing.T) {
	_, err := encodeMessage(gogo.WSHubMessage{
		NodeID:  "node-a",
		Topic:   "room.general",
		Message: []byte("hello"),
		OpCode:  gogo.OpCode(99),
	})
	if err == nil {
		t.Fatal("encode succeeded for invalid opcode")
	}
}

func redisWSHubTestPrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("gogo:test:ws:%d:", time.Now().UnixNano())
}

func redisWSHubTestAdapter(t *testing.T, client goredis.UniversalClient, opt Options) *Adapter {
	t.Helper()
	adapter, err := NewClientOptions(client, opt)
	if err != nil {
		t.Fatalf("NewClientOptions: %v", err)
	}
	return adapter
}

func recordRedisWSHubMessages(ch chan<- gogo.WSHubMessage) func(gogo.WSHubMessage) {
	return func(msg gogo.WSHubMessage) {
		select {
		case ch <- msg:
		default:
		}
	}
}

func waitForRedisWSHubMessage(t *testing.T, ch <-chan gogo.WSHubMessage, timeout time.Duration) gogo.WSHubMessage {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg := <-ch:
		return msg
	case <-timer.C:
		t.Fatalf("timed out waiting for Redis WSHub message after %v", timeout)
	}
	return gogo.WSHubMessage{}
}

func expectNoRedisWSHubMessage(t *testing.T, ch <-chan gogo.WSHubMessage, timeout time.Duration) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg := <-ch:
		t.Fatalf("unexpected Redis WSHub message after unsubscribe/close: %#v", msg)
	case <-timer.C:
	}
}

func assertRedisWSHubMessage(t *testing.T, got, want gogo.WSHubMessage) {
	t.Helper()
	if got.NodeID != want.NodeID || got.Topic != want.Topic || got.OpCode != want.OpCode {
		t.Fatalf("message metadata = %#v, want %#v", got, want)
	}
	if string(got.Message) != string(want.Message) {
		t.Fatalf("message payload = %q, want %q", got.Message, want.Message)
	}
}

func waitForRedisSubscribers(t *testing.T, client goredis.UniversalClient, channel string, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var (
		got     int64
		lastErr error
	)
	for {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		counts, err := client.PubSubNumSub(ctx, channel).Result()
		cancel()
		if err != nil {
			lastErr = err
		} else {
			got = counts[channel]
			if got == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Redis subscribers for %q = %d, want %d (last error: %v)", channel, got, want, lastErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

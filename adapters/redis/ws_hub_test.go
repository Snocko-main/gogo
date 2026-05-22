package redis

import (
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

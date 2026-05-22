package redis

import (
	"testing"

	gogo "github.com/Snocko-main/gogo"
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
	out, err := decodeMessage(in.Topic, payload)
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
}

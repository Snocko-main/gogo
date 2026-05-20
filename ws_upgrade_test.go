//go:build cgo && gogo

package gogo_test

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
)

// dialWSWithHeaders is a thin variant of dialWebSocket that sends
// extra request headers and returns the full HTTP response so the
// test can inspect the upgrade reply (status, Sec-WebSocket-Protocol,
// custom headers, body on rejection).
func dialWSWithHeaders(port int, path string, extra map[string]string) (*http.Response, string, net.Conn, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	if err != nil {
		return nil, "", nil, fmt.Errorf("dial: %w", err)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		conn.Close()
		return nil, "", nil, err
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		conn.Close()
		return nil, "", nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	var sb strings.Builder
	fmt.Fprintf(&sb, "GET %s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nUpgrade: websocket\r\n"+
		"Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n",
		path, port, key)
	for k, v := range extra {
		fmt.Fprintf(&sb, "%s: %s\r\n", k, v)
	}
	sb.WriteString("\r\n")
	if _, err := io.WriteString(conn, sb.String()); err != nil {
		conn.Close()
		return nil, "", nil, err
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		conn.Close()
		return nil, "", nil, fmt.Errorf("read response: %w", err)
	}
	// Body is left on resp.Body for the caller to read. For 101
	// upgrades there is no body, but for rejection responses we
	// want to verify the body content.

	return resp, key, conn, nil
}

// expectedAccept computes the Sec-WebSocket-Accept value for the
// given client key (the standard SHA-1+base64 transform from RFC
// 6455).
func expectedAccept(key string) string {
	h := sha1.New()
	h.Write([]byte(key))
	h.Write([]byte("258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// TestWSSubprotocolNegotiation: client offers ["graphql-ws",
// "chat"]; server's Upgrade callback picks "graphql-ws" and the
// server's 101 response echoes it back in Sec-WebSocket-Protocol.
func TestWSSubprotocolNegotiation(t *testing.T) {
	var pickedFromCallback string
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: func(ctx *gogo.UpgradeContext) {
				offered := ctx.Protocols()
				for _, p := range offered {
					if p == "graphql-ws" {
						pickedFromCallback = p
						ctx.Accept(p)
						return
					}
				}
				ctx.Reject(400, "no acceptable subprotocol")
			},
		})
	})
	defer teardown()

	resp, _, conn, err := dialWSWithHeaders(port, "/ws", map[string]string{
		"Sec-WebSocket-Protocol": "chat, graphql-ws",
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if resp.StatusCode != 101 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body=%q, want 101", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != "graphql-ws" {
		t.Errorf("Sec-WebSocket-Protocol = %q, want graphql-ws", got)
	}
	if pickedFromCallback != "graphql-ws" {
		t.Errorf("callback picked %q, want graphql-ws", pickedFromCallback)
	}
}

// TestWSUpgradeRejection: callback calls Reject; client sees the
// HTTP status and body, no protocol switch.
func TestWSUpgradeRejection(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: func(ctx *gogo.UpgradeContext) {
				if ctx.Header("authorization") != "Bearer good" {
					ctx.Reject(401, "bad token")
					return
				}
				ctx.Accept("")
			},
		})
	})
	defer teardown()

	// No auth header → reject.
	resp, _, conn, err := dialWSWithHeaders(port, "/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	conn.Close()
	if resp.StatusCode != 401 {
		t.Errorf("no-auth status = %d, want 401", resp.StatusCode)
	}
	if !strings.Contains(string(body), "bad token") {
		t.Errorf("no-auth body = %q, want contains 'bad token'", body)
	}

	// With auth → 101 upgrade.
	resp2, _, conn2, err := dialWSWithHeaders(port, "/ws", map[string]string{
		"Authorization": "Bearer good",
	})
	if err != nil {
		t.Fatalf("dial2: %v", err)
	}
	defer conn2.Close()
	if resp2.StatusCode != 101 {
		t.Errorf("good-auth status = %d, want 101", resp2.StatusCode)
	}
}

// TestWSUpgradeHeaderAccess covers Header / Query / URL / IP /
// Protocols on the context.
func TestWSUpgradeHeaderAccess(t *testing.T) {
	type seen struct {
		url      string
		query    string
		method   string
		ip       string
		header   string
		protos   []string
	}
	var (
		mu      sync.Mutex
		got     seen
		gotSeen atomic.Bool
	)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: func(ctx *gogo.UpgradeContext) {
				mu.Lock()
				got = seen{
					url:    ctx.URL(),
					query:  ctx.Query(),
					method: ctx.Method(),
					ip:     ctx.IP(),
					header: ctx.Header("X-Test"),
					protos: append([]string(nil), ctx.Protocols()...),
				}
				gotSeen.Store(true)
				mu.Unlock()
				ctx.Accept("")
			},
		})
	})
	defer teardown()

	resp, _, conn, err := dialWSWithHeaders(port, "/ws?q=42", map[string]string{
		"X-Test":                 "hello",
		"Sec-WebSocket-Protocol": "p1, p2",
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if resp.StatusCode != 101 {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if !gotSeen.Load() {
		t.Fatalf("upgrade callback never ran")
	}

	mu.Lock()
	defer mu.Unlock()
	if got.url != "/ws" {
		t.Errorf("URL = %q, want /ws", got.url)
	}
	if got.query != "q=42" {
		t.Errorf("Query = %q, want q=42", got.query)
	}
	if got.method != "get" {
		t.Errorf("Method = %q, want get", got.method)
	}
	if got.ip != "127.0.0.1" {
		t.Errorf("IP = %q, want 127.0.0.1", got.ip)
	}
	if got.header != "hello" {
		t.Errorf("X-Test = %q, want hello", got.header)
	}
	if len(got.protos) != 2 || got.protos[0] != "p1" || got.protos[1] != "p2" {
		t.Errorf("Protocols = %v, want [p1 p2]", got.protos)
	}
}

// TestWSQueryParam exposes the parsed query helper.
func TestWSQueryParam(t *testing.T) {
	var token string
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: func(ctx *gogo.UpgradeContext) {
				token = ctx.QueryParam("token")
				ctx.Accept("")
			},
		})
	})
	defer teardown()

	resp, _, conn, err := dialWSWithHeaders(port, "/ws?token=abc&other=ignored", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if resp.StatusCode != 101 {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if token != "abc" {
		t.Errorf("QueryParam(token) = %q, want abc", token)
	}
}

// TestWSUserData: SetUserData in the upgrade callback survives to
// Open and Message handlers via ws.UserData().
func TestWSUserData(t *testing.T) {
	type session struct{ ID string }
	var (
		openSeenID    string
		messageSeenID string
		mu            sync.Mutex
		ready         = make(chan struct{}, 1)
	)
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: func(ctx *gogo.UpgradeContext) {
				ctx.SetUserData(&session{ID: "user-42"})
				ctx.Accept("")
			},
			Open: func(ws *gogo.WebSocket) {
				if s, ok := ws.UserData().(*session); ok {
					mu.Lock()
					openSeenID = s.ID
					mu.Unlock()
				}
				select {
				case ready <- struct{}{}:
				default:
				}
			},
			Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
				if s, ok := ws.UserData().(*session); ok {
					mu.Lock()
					messageSeenID = s.ID
					mu.Unlock()
				}
				ws.SendText("ack")
			},
		})
	})
	defer teardown()

	c, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("Open never fired")
	}

	if err := c.SendText("ping"); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := c.ReadText(2 * time.Second); err != nil {
		t.Fatalf("read ack: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if openSeenID != "user-42" {
		t.Errorf("Open saw UserData ID = %q, want user-42", openSeenID)
	}
	if messageSeenID != "user-42" {
		t.Errorf("Message saw UserData ID = %q, want user-42", messageSeenID)
	}
}

// TestWSUserDataSetAtOpen: callbacks that authenticate in Open
// (not in Upgrade) can still attach state via ws.SetUserData.
func TestWSUserDataSetAtOpen(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.SetUserData("from-open")
			},
			Message: func(ws *gogo.WebSocket, msg []byte, op gogo.OpCode) {
				v, _ := ws.UserData().(string)
				ws.SendText(v)
			},
		})
	})
	defer teardown()

	c, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	time.Sleep(20 * time.Millisecond)

	if err := c.SendText("ping"); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := c.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "from-open" {
		t.Errorf("echoed UserData = %q, want from-open", got)
	}
}

// TestWSUpgradeNoCallbackUsesDefault: without an Upgrade callback
// the framework retains uWS's default auto-accept behavior.
func TestWSUpgradeNoCallbackUsesDefault(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Open: func(ws *gogo.WebSocket) {
				ws.SendText("hi")
			},
		})
	})
	defer teardown()

	c, err := dialWebSocket(port, "/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	got, err := c.ReadText(2 * time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "hi" {
		t.Errorf("body = %q, want hi", got)
	}
}

// TestWSUpgradeForgotToDecide: callback returns without calling
// Accept or Reject — framework rejects with 500 instead of leaking.
func TestWSUpgradeForgotToDecide(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: func(ctx *gogo.UpgradeContext) {
				// Intentionally no Accept / Reject.
			},
		})
	})
	defer teardown()

	resp, _, conn, err := dialWSWithHeaders(port, "/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Close()
	if resp.StatusCode != 500 {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

// TestWSUpgradeAcceptKey verifies the negotiated Sec-WebSocket-Accept
// value matches the standard RFC 6455 transform after a custom
// upgrade callback runs — i.e. the upgrade path doesn't mangle the
// handshake.
func TestWSUpgradeAcceptKey(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: func(ctx *gogo.UpgradeContext) {
				ctx.Accept("")
			},
		})
	})
	defer teardown()

	resp, key, conn, err := dialWSWithHeaders(port, "/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if resp.StatusCode != 101 {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	want := expectedAccept(key)
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != want {
		t.Errorf("Sec-WebSocket-Accept = %q, want %q", got, want)
	}
}

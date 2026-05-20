//go:build cgo && gogo

package middleware_test

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	gogo "github.com/Snocko-main/gogo"
	"github.com/Snocko-main/gogo/middleware"
)

// wsHandshake performs a raw WebSocket handshake with the supplied
// extra headers and returns the parsed HTTP response. It's the
// minimum machinery needed to probe the upgrade-path's reaction to
// different request shapes without dragging in a full WebSocket
// client.
func wsHandshake(port int, path string, extra map[string]string) (*http.Response, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	var keyBytes [16]byte
	_, _ = rand.Read(keyBytes[:])
	key := base64.StdEncoding.EncodeToString(keyBytes[:])

	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n", path, port)
	fmt.Fprintf(&b, "Upgrade: websocket\r\nConnection: Upgrade\r\n")
	fmt.Fprintf(&b, "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n", key)
	for k, v := range extra {
		fmt.Fprintf(&b, "%s: %s\r\n", k, v)
	}
	b.WriteString("\r\n")
	if _, err := conn.Write([]byte(b.String())); err != nil {
		return nil, err
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return resp, nil
}

// TestWebSocketAuthRejectsMissingOrigin: with the zero-value options,
// WebSocketAuth refuses a handshake that arrives without an Origin
// header — browser-driven CSWSH attempts always carry an Origin so
// this is the strict default.
func TestWebSocketAuthRejectsMissingOrigin(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
				AllowedOrigins: []string{"https://app.example.com"},
			}),
		})
	})
	defer teardown()

	resp, err := wsHandshake(port, "/ws", nil)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if resp.StatusCode != 403 {
		t.Errorf("missing Origin: got %d, want 403", resp.StatusCode)
	}
}

// TestWebSocketAuthAllowsListedOrigin: Origin in the allow-list
// completes the upgrade with 101 Switching Protocols.
func TestWebSocketAuthAllowsListedOrigin(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
				AllowedOrigins: []string{"https://app.example.com"},
			}),
		})
	})
	defer teardown()

	resp, err := wsHandshake(port, "/ws", map[string]string{"Origin": "https://app.example.com"})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if resp.StatusCode != 101 {
		t.Errorf("allowed origin: got %d, want 101", resp.StatusCode)
	}
}

// TestWebSocketAuthRejectsCrossOrigin: an attacker page at
// https://evil.example sends an Origin header to that effect; the
// allow-list rejects it. This is the CSWSH defense.
func TestWebSocketAuthRejectsCrossOrigin(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
				AllowedOrigins: []string{"https://app.example.com"},
			}),
		})
	})
	defer teardown()

	resp, err := wsHandshake(port, "/ws", map[string]string{"Origin": "https://evil.example"})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if resp.StatusCode != 403 {
		t.Errorf("cross-origin: got %d, want 403", resp.StatusCode)
	}
}

// TestWebSocketAuthNormalizesOrigin: matching is case-insensitive
// on scheme + host and tolerates a trailing slash, so trivial
// formatting differences don't accidentally lock out legitimate
// browsers.
func TestWebSocketAuthNormalizesOrigin(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
				AllowedOrigins: []string{"https://APP.example.com/"},
			}),
		})
	})
	defer teardown()

	resp, err := wsHandshake(port, "/ws", map[string]string{"Origin": "https://app.example.com"})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if resp.StatusCode != 101 {
		t.Errorf("normalized origin: got %d, want 101", resp.StatusCode)
	}
}

// TestWebSocketAuthAllowMissingOriginCLI: AllowMissingOrigin=true
// permits CLI clients (websocat, curl --include) that don't set
// Origin. Browsers always set Origin so this option is safe IF you
// know all your clients are non-browser.
func TestWebSocketAuthAllowMissingOriginCLI(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
				AllowedOrigins:     []string{"https://app.example.com"},
				AllowMissingOrigin: true,
			}),
		})
	})
	defer teardown()

	resp, err := wsHandshake(port, "/ws", nil) // no Origin header
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if resp.StatusCode != 101 {
		t.Errorf("allow-missing-origin: got %d, want 101", resp.StatusCode)
	}
}

// TestWebSocketAuthAllowWildcard: AllowedOrigins=["*"] accepts any
// origin. Opt-in for endpoints that are explicitly cross-origin
// (e.g. public APIs) and that don't rely on ambient cookie auth.
func TestWebSocketAuthAllowWildcard(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
				AllowedOrigins: []string{"*"},
			}),
		})
	})
	defer teardown()

	for _, origin := range []string{"https://app.example.com", "https://evil.example", "null"} {
		resp, err := wsHandshake(port, "/ws", map[string]string{"Origin": origin})
		if err != nil {
			t.Fatalf("origin %q: %v", origin, err)
		}
		if resp.StatusCode != 101 {
			t.Errorf("wildcard origin=%q: got %d, want 101", origin, resp.StatusCode)
		}
	}
}

// TestWebSocketAuthVerifyAccept: a Verify callback that returns
// (userData, true) completes the upgrade with the supplied user
// data attached to the connection.
func TestWebSocketAuthVerifyAccept(t *testing.T) {
	type sentinel struct{ id int }
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
				AllowedOrigins: []string{"https://app.example.com"},
				Verify: func(ctx *gogo.UpgradeContext) (any, bool, int, string) {
					if ctx.QueryParam("token") != "secret" {
						return nil, false, 401, "bad token"
					}
					return &sentinel{id: 42}, true, 0, ""
				},
			}),
			Open: func(ws *gogo.WebSocket) {
				if s, ok := ws.UserData().(*sentinel); !ok || s.id != 42 {
					t.Errorf("Open: UserData = %v, want sentinel{id:42}", ws.UserData())
				}
				ws.End(1000, "")
			},
		})
	})
	defer teardown()

	resp, err := wsHandshake(port, "/ws?token=secret",
		map[string]string{"Origin": "https://app.example.com"})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if resp.StatusCode != 101 {
		t.Fatalf("accept: got %d, want 101", resp.StatusCode)
	}
	// Give the Open handler a beat to run + verify userData.
	time.Sleep(40 * time.Millisecond)
}

// TestWebSocketAuthVerifyReject: a Verify callback returning ok=false
// rejects the handshake with the supplied status / message.
func TestWebSocketAuthVerifyReject(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
				AllowedOrigins: []string{"https://app.example.com"},
				Verify: func(ctx *gogo.UpgradeContext) (any, bool, int, string) {
					return nil, false, 418, "no token"
				},
			}),
		})
	})
	defer teardown()

	resp, err := wsHandshake(port, "/ws",
		map[string]string{"Origin": "https://app.example.com"})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if resp.StatusCode != 418 {
		t.Errorf("verify reject: got %d, want 418", resp.StatusCode)
	}
}

// TestWebSocketAuthSubprotocol picks the first allowed protocol the
// client also offered. If there's no overlap the handshake is
// rejected with 400.
func TestWebSocketAuthSubprotocol(t *testing.T) {
	port, teardown := startApp(t, func(app *gogo.App) {
		app.WebSocket("/ws", gogo.WebSocketBehavior{
			Upgrade: middleware.WebSocketAuth(middleware.WebSocketAuthOptions{
				AllowedOrigins:      []string{"https://app.example.com"},
				AllowedSubprotocols: []string{"chat.v2", "chat.v1"},
			}),
		})
	})
	defer teardown()

	// Client offers chat.v1 (overlap) + foo — server picks chat.v1
	// because it's the first matching allow-list entry.
	resp, err := wsHandshake(port, "/ws", map[string]string{
		"Origin":                 "https://app.example.com",
		"Sec-WebSocket-Protocol": "foo, chat.v1",
	})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if resp.StatusCode != 101 {
		t.Fatalf("overlap: got %d, want 101", resp.StatusCode)
	}
	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != "chat.v1" {
		t.Errorf("subprotocol: got %q, want chat.v1", got)
	}

	// No overlap → 400.
	resp, err = wsHandshake(port, "/ws", map[string]string{
		"Origin":                 "https://app.example.com",
		"Sec-WebSocket-Protocol": "nope.v1",
	})
	if err != nil {
		t.Fatalf("handshake (no overlap): %v", err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("no overlap: got %d, want 400", resp.StatusCode)
	}
}

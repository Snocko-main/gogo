package middleware

import (
	"strings"
	"testing"
)

func TestWebSocketAuthPanicsOnInvalidConfiguredOrigins(t *testing.T) {
	cases := []string{
		"",
		"https://app.example.com/path",
		"https://app.example.com?x=1",
		"https://app.example.com#frag",
		"https://user@app.example.com",
		"https://app.example.com\n",
		"://missing-scheme",
	}
	for _, origin := range cases {
		t.Run(origin, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("WebSocketAuth did not panic")
				}
			}()
			_ = WebSocketAuth(WebSocketAuthOptions{
				AllowedOrigins: []string{origin},
			})
		})
	}
}

func TestWebSocketAuthAcceptsValidConfiguredOrigins(t *testing.T) {
	cases := []string{
		"https://APP.example.com/",
		"http://localhost:3000",
		"chrome-extension://abcdef",
		"null",
		"*",
	}
	for _, origin := range cases {
		t.Run(origin, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("WebSocketAuth panicked for valid origin %q: %v", origin, r)
				}
			}()
			_ = WebSocketAuth(WebSocketAuthOptions{
				AllowedOrigins: []string{origin},
			})
		})
	}
}

func TestWebSocketAuthPanicsOnAmbiguousWildcardOrigins(t *testing.T) {
	cases := [][]string{
		{"*", "https://app.example.com"},
		{"https://app.example.com", "*"},
		{"*", "*"},
	}
	for _, origins := range cases {
		t.Run(strings.Join(origins, ","), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("WebSocketAuth did not panic")
				}
			}()
			_ = WebSocketAuth(WebSocketAuthOptions{
				AllowedOrigins: origins,
			})
		})
	}
}

func TestOriginAllowedRejectsMalformedRuntimeOrigin(t *testing.T) {
	allowed := []string{normalizeAllowedOriginValue("https://app.example.com")}
	if !originAllowed("https://app.example.com", allowed) {
		t.Fatal("originAllowed rejected exact origin")
	}
	for _, origin := range []string{
		"https://app.example.com/path",
		"https://app.example.com?x=1",
		"https://app.example.com\n",
		" https://app.example.com",
		"https://app.example.com ",
		"\thttps://app.example.com",
	} {
		if originAllowed(origin, allowed) {
			t.Fatalf("originAllowed accepted malformed runtime origin %q", origin)
		}
	}
}

func TestWebSocketAuthTrimsConfiguredOriginsOnly(t *testing.T) {
	allowed := []string{normalizeAllowedOriginValue(" https://APP.example.com/ ")}
	if !originAllowed("https://app.example.com", allowed) {
		t.Fatal("originAllowed rejected normalized configured origin")
	}
	if originAllowed(" https://app.example.com ", allowed) {
		t.Fatal("originAllowed accepted whitespace-padded runtime origin")
	}
}

func TestWebSocketAuthPanicsOnInvalidConfiguredSubprotocols(t *testing.T) {
	cases := []string{"", "chat v1", "chat,v1", "chat\nv1"}
	for _, protocol := range cases {
		t.Run(protocol, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("WebSocketAuth did not panic")
				}
			}()
			_ = WebSocketAuth(WebSocketAuthOptions{
				AllowedOrigins:      []string{"https://app.example.com"},
				AllowedSubprotocols: []string{protocol},
			})
		})
	}
}

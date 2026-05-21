package middleware

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestParseBasicAuthRejectsOverMaxCredentialBytes(t *testing.T) {
	auth := "Basic " + strings.Repeat("A", 32)
	if _, _, ok := parseBasicAuth(auth, 16); ok {
		t.Fatal("parseBasicAuth accepted overlarge credentials payload")
	}
}

func TestParseBasicAuthMaxCredentialBytesCanBeDisabled(t *testing.T) {
	payload := base64.StdEncoding.EncodeToString([]byte("alice:wonderland"))
	user, pass, ok := parseBasicAuth("Basic "+payload, -1)
	if !ok || user != "alice" || pass != "wonderland" {
		t.Fatalf("parseBasicAuth disabled cap = (%q, %q, %v)", user, pass, ok)
	}
}

//go:build cgo && gogo

package gogo

import (
	"testing"
	"time"
)

func TestSharedHandlerRegistryCleanupTombstonesAppSlots(t *testing.T) {
	app, err := NewApp()
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}

	app.GetAsync("/one", func(res *Response, req *Request) {
		res.Send(200, "text/plain", "one")
	})
	app.PostAsync("/two", 128, func(res *Response, req *Request, body []byte) {
		res.Send(200, "text/plain", "two")
	})

	ids := append([]uint32(nil), app.inner.sharedHandlerIDs...)
	if len(ids) != 2 {
		t.Fatalf("shared handler ids len = %d, want 2", len(ids))
	}
	for _, id := range ids {
		if _, ok := lookupSharedHandler(id); !ok {
			t.Fatalf("handler id %d missing before Close", id)
		}
	}

	app.Close()
	if !WaitForSharedWorkers(time.Second) {
		t.Fatal("shared workers did not drain after Close")
	}
	for _, id := range ids {
		if _, ok := lookupSharedHandler(id); ok {
			t.Fatalf("handler id %d still live after Close", id)
		}
	}

	next, err := NewApp()
	if err != nil {
		t.Fatalf("NewApp next: %v", err)
	}
	next.GetAsync("/next", func(res *Response, req *Request) {
		res.Send(200, "text/plain", "next")
	})
	nextIDs := append([]uint32(nil), next.inner.sharedHandlerIDs...)
	if len(nextIDs) != 1 {
		t.Fatalf("next shared handler ids len = %d, want 1", len(nextIDs))
	}
	if nextIDs[0] <= ids[len(ids)-1] {
		t.Fatalf("handler id was reused: got %d after previous max %d", nextIDs[0], ids[len(ids)-1])
	}
	if _, ok := lookupSharedHandler(ids[0]); ok {
		t.Fatalf("stale handler id %d revived after new registration", ids[0])
	}

	next.Close()
	if !WaitForSharedWorkers(time.Second) {
		t.Fatal("shared workers did not drain after next Close")
	}
}

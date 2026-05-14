// restapi demonstrates a tiny in-memory REST API with GET/POST/JSON +
// route params + query strings. Single-process, no DB.
//
//	CGO_ENABLED=1 go run -tags gogo ./examples/restapi
//
//	curl http://localhost:3001/users
//	curl http://localhost:3001/users/1
//	curl -XPOST -d '{"name":"Bob"}' -H 'Content-Type: application/json' http://localhost:3001/users
//	curl 'http://localhost:3001/users?q=alice'
package main

import (
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"sync/atomic"

	gogo "uwebsockets-go/gogo"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var (
	users   = map[int]User{1: {1, "Alice"}, 2: {2, "Charlie"}}
	usersMu sync.RWMutex
	nextID  atomic.Int64
)

func main() {
	nextID.Store(2)

	app, err := gogo.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	// LIST with optional ?q= filter.
	app.GetAsync("/users", func(res *gogo.Response, req *gogo.Request) {
		filter := req.QueryParam("q")
		usersMu.RLock()
		out := make([]User, 0, len(users))
		for _, u := range users {
			if filter == "" || containsCI(u.Name, filter) {
				out = append(out, u)
			}
		}
		usersMu.RUnlock()
		res.JSON(200, out)
	})

	// GET one.
	app.GetAsync("/users/:id", func(res *gogo.Response, req *gogo.Request) {
		id, err := strconv.Atoi(req.Parameter(0))
		if err != nil {
			res.Send(400, "text/plain", "bad id")
			return
		}
		usersMu.RLock()
		u, ok := users[id]
		usersMu.RUnlock()
		if !ok {
			res.Send(404, "text/plain", "not found")
			return
		}
		res.JSON(200, u)
	})

	// CREATE.
	app.PostAsync("/users", 1024, func(res *gogo.Response, req *gogo.Request, body []byte) {
		var in struct{ Name string }
		if err := json.Unmarshal(body, &in); err != nil || in.Name == "" {
			res.Send(400, "text/plain", "bad json")
			return
		}
		id := int(nextID.Add(1))
		u := User{ID: id, Name: in.Name}
		usersMu.Lock()
		users[id] = u
		usersMu.Unlock()
		res.JSON(201, u)
	})

	if !app.Listen(3001) {
		log.Fatal("listen :3001 failed")
	}
	log.Println("gogo~ restapi listening on http://localhost:3001")
	app.Run()
}

func containsCI(s, sub string) bool {
	if len(sub) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(sub); i++ {
		match := true
		for j := 0; j < len(sub); j++ {
			a := s[i+j]
			b := sub[j]
			if a >= 'A' && a <= 'Z' {
				a += 'a' - 'A'
			}
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

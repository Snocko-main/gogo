package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	_ "github.com/mattn/go-sqlite3"
	uws "uwebsockets-go/uwebsockets"
)

var dbConn *sql.DB

func initDB(path string) error {
	db, err := sql.Open("sqlite3", path+"?_journal=WAL&_synchronous=NORMAL")
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(runtime.NumCPU() * 4)

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT,
		role TEXT
	)`); err != nil {
		return err
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if count < 1000 {
		tx, _ := db.Begin()
		for i := 1; i <= 1000; i++ {
			tx.Exec(`INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)`,
				i, fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i), "user")
		}
		tx.Commit()
	}
	dbConn = db
	return nil
}

func main() {
	go func() {
		log.Println("pprof on http://localhost:6060/debug/pprof")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	app, err := uws.NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	app.Get("/plain", func(res *uws.Response, req *uws.Request) {
		res.Send(200, "text/plain; charset=utf-8", "hello world\n")
	})

	app.Get("/json", func(res *uws.Response, req *uws.Request) {
		res.Send(200, "application/json", `{"message":"hello world","ok":true}`+"\n")
	})

	// Static routes — served entirely by uWS C++, zero cgo per request.
	app.Get("/health", uws.Reply{
		Status:      200,
		ContentType: "application/json",
		Body:        `{"ok":true}` + "\n",
	})
	app.Get("/plain-static", "hello world\n")

	app.Get("/hello/:name", func(res *uws.Response, req *uws.Request) {
		res.Send(200, "text/plain; charset=utf-8", "hello "+req.Parameter(0)+"\n")
	})

	app.GetAsync("/sleep", func(res *uws.Response) {
		time.Sleep(2 * time.Millisecond)
		res.Send(200, "text/plain; charset=utf-8", "slept\n")
	})

	filePath := os.Getenv("BENCH_FILE")
	if filePath == "" {
		filePath = "benchmark/data/sample.json"
	}
	app.GetAsync("/file", func(res *uws.Response) {
		data, err := os.ReadFile(filePath)
		if err != nil {
			res.Send(500, "text/plain", err.Error())
			return
		}
		res.Send(200, "application/json", string(data))
	})

	dbPath := os.Getenv("BENCH_DB")
	if dbPath == "" {
		dbPath = "/tmp/uwsbench/sample.db"
	}
	if err := initDB(dbPath); err != nil {
		log.Fatal(err)
	}
	app.GetAsync("/db", func(res *uws.Response) {
		id := rand.IntN(1000) + 1
		var name, email, role string
		err := dbConn.QueryRow("SELECT name, email, role FROM users WHERE id = ?", id).Scan(&name, &email, &role)
		if err != nil {
			res.Send(500, "text/plain", err.Error())
			return
		}
		res.Send(200, "application/json",
			fmt.Sprintf(`{"id":%d,"name":%q,"email":%q,"role":%q}`+"\n", id, name, email, role))
	})

	// Same as /db but uses SendShared (shared-memory + lock-free ring, 0 cgo on hot path).
	app.GetAsync("/db-shared", func(res *uws.Response) {
		id := rand.IntN(1000) + 1
		var name, email, role string
		err := dbConn.QueryRow("SELECT name, email, role FROM users WHERE id = ?", id).Scan(&name, &email, &role)
		if err != nil {
			res.SendShared(500, "text/plain", err.Error())
			return
		}
		res.SendShared(200, "application/json",
			fmt.Sprintf(`{"id":%d,"name":%q,"email":%q,"role":%q}`+"\n", id, name, email, role))
	})

	// /db-pipeline uses GetShared on the INPUT side too: C++ pushes the
	// request onto a ring buffer, a Go worker pool drains it (no cgo
	// callback per request). Combined with SendShared on the OUTPUT side
	// the hot path has zero cgo crossings.
	app.GetShared("/db-pipeline", func(res *uws.Response) {
		id := rand.IntN(1000) + 1
		var name, email, role string
		err := dbConn.QueryRow("SELECT name, email, role FROM users WHERE id = ?", id).Scan(&name, &email, &role)
		if err != nil {
			res.SendShared(500, "text/plain", err.Error())
			return
		}
		res.SendShared(200, "application/json",
			fmt.Sprintf(`{"id":%d,"name":%q,"email":%q,"role":%q}`+"\n", id, name, email, role))
	})

	if !app.Listen(3002) {
		log.Fatal("failed to listen on :3002")
	}

	log.Println("uWebSockets-Go listening on http://localhost:3002")
	app.Run()


}

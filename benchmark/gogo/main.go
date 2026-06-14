package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	gogo "github.com/Snocko-main/gogo"
	_ "github.com/mattn/go-sqlite3"
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
	dbPath := os.Getenv("BENCH_DB")
	if dbPath == "" {
		dbPath = "/tmp/uwsbench/sample.db"
	}
	if err := initDB(dbPath); err != nil {
		log.Fatal(err)
	}

	filePath := os.Getenv("BENCH_FILE")
	if filePath == "" {
		filePath = "data/sample.json"
	}

	// GOGO_CORES=N enables multi-core mode (N independent App instances
	// with accepted sockets round-robined across loops). Defaults to
	// single-core for parity with old runs.
	n := 1
	if env := os.Getenv("GOGO_CORES"); env != "" {
		if v, err := strconv.Atoi(env); err == nil && v > 0 {
			n = v
		}
	}
	workers := 0
	if env := os.Getenv("GOGO_WORKERS"); env != "" {
		if v, err := strconv.Atoi(env); err == nil && v >= 0 {
			workers = v
		}
	}
	gogo.SetWorkerCount(workers)

	setup := func(app *gogo.App) {
		app.Get("/plain", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain; charset=utf-8", "hello world\n")
		})
		app.Get("/hello", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain; charset=utf-8", "hello world\n")
		})
		app.Get("/json", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "application/json", `{"message":"hello world","ok":true}`+"\n")
		})
		app.GetAsync("/async", func(res *gogo.Response, req *gogo.Request) {
			var name, email, role string
			err := dbConn.QueryRow("SELECT name, email, role FROM users WHERE id = ?", 42).Scan(&name, &email, &role)
			if err != nil {
				res.Send(500, "text/plain", err.Error())
				return
			}
			res.Send(200, "application/json",
				fmt.Sprintf(`{"id":42,"name":%q,"email":%q,"role":%q}`+"\n", name, email, role))
		})
		app.Get("/health", gogo.Reply{
			Status:      200,
			ContentType: "application/json",
			Body:        `{"ok":true}` + "\n",
		})
		app.Get("/plain-static", "hello world\n")
		app.Get("/hello/:name", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain; charset=utf-8", "hello "+req.Parameter(0)+"\n")
		})
		// Twin routes: identical handler work, one sync one async, so an
		// A/B between them on the same running process isolates exactly the
		// shared-dispatch (worker-handoff) overhead with zero other diff.
		app.Get("/twinsync/:name", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain; charset=utf-8", "hello "+req.Parameter(0)+"\n")
		})
		app.GetAsync("/twinasync/:name", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain; charset=utf-8", "hello "+req.Parameter(0)+"\n")
		})
		app.GetAsync("/sleep", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(2 * time.Millisecond)
			res.Send(200, "text/plain; charset=utf-8", "slept\n")
		})
		app.Use("/middleware", func(next gogo.Handler) gogo.Handler {
			return func(res *gogo.Response, req *gogo.Request) {
				res.Header("X-Bench-Middleware", "gogo")
				res.Header("X-Bench-Route", "middleware")
				next(res, req)
			}
		})
		app.Get("/middleware", func(res *gogo.Response, req *gogo.Request) {
			res.Header("Content-Type", "text/plain; charset=utf-8")
			res.Send(200, "", "hello world\n")
		})
		app.GetAsync("/file", func(res *gogo.Response, req *gogo.Request) {
			data, err := os.ReadFile(filePath)
			if err != nil {
				res.Send(500, "text/plain", err.Error())
				return
			}
			res.Send(200, "application/json", string(data))
		})
		app.GetAsync("/db", func(res *gogo.Response, req *gogo.Request) {
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
		app.GetAsync("/user/:id", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "application/json",
				fmt.Sprintf(`{"id":%q}`+"\n", req.Parameter(0)))
		})
		// POST /echo: sync handler — read body on the loop thread,
		// write it back unchanged. Pure body+write path, no goroutine
		// handoff. Matches the "no work" shape of frameworks like
		// actix where the whole pipeline stays on a single task.
		app.Post("/echo", func(res *gogo.Response, req *gogo.Request) {
			res.Body(64*1024, func(body []byte, err error) {
				if err != nil {
					res.Send(413, "text/plain", err.Error())
					return
				}
				res.Send(200, "application/json", string(body))
			})
		})
		// POST /query: realistic API shape — small body carries an id,
		// handler runs a blocking SQLite lookup, returns a JSON row.
		// PostAsync is built for this: body fits the shared-dispatch
		// cap so the request crosses zero cgo callbacks on the hot
		// path, and the handler runs on a worker goroutine so the
		// blocking sql.DB.QueryRow doesn't pin the loop thread.
		app.PostAsync("/query", 256, func(res *gogo.Response, req *gogo.Request, body []byte) {
			id, _ := strconv.Atoi(strings.TrimSpace(string(body)))
			if id < 1 || id > 1000 {
				id = 1
			}
			var name, email, role string
			err := dbConn.QueryRow("SELECT name, email, role FROM users WHERE id = ?", id).Scan(&name, &email, &role)
			if err != nil {
				res.Send(500, "text/plain", err.Error())
				return
			}
			res.Send(200, "application/json",
				fmt.Sprintf(`{"id":%d,"name":%q,"email":%q,"role":%q}`+"\n", id, name, email, role))
		})
	}

	if n == 1 {
		// Keep the simple path for the historical single-loop benchmark.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		app, err := gogo.NewApp()
		if err != nil {
			log.Fatal(err)
		}
		defer app.Close()
		setup(app)
		if !app.Listen(3002) {
			log.Fatal("failed to listen on :3002")
		}
		log.Println("gogo~ listening on http://localhost:3002 (1 core)")
		app.Run()
		return
	}

	handle, err := gogo.RunMultiCore(n, 3002, setup)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("gogo~ listening on http://localhost:3002 (%d cores)", n)
	handle.Wait()
}

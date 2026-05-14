package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
	gogo "uwebsockets-go/gogo"
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
		filePath = "benchmark/data/sample.json"
	}

	// GOGO_CORES=N enables multi-core mode (N independent App instances
	// behind SO_REUSEPORT). Defaults to single-core for parity with old runs.
	n := 1
	if env := os.Getenv("GOGO_CORES"); env != "" {
		if v, err := strconv.Atoi(env); err == nil && v > 0 {
			n = v
		}
	}

	setup := func(app *gogo.App) {
		app.Get("/plain", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "text/plain; charset=utf-8", "hello world\n")
		})
		app.Get("/json", func(res *gogo.Response, req *gogo.Request) {
			res.Send(200, "application/json", `{"message":"hello world","ok":true}`+"\n")
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
		app.GetAsync("/sleep", func(res *gogo.Response, req *gogo.Request) {
			time.Sleep(2 * time.Millisecond)
			res.SendShared(200, "text/plain; charset=utf-8", "slept\n")
		})
		app.GetAsync("/file", func(res *gogo.Response, req *gogo.Request) {
			data, err := os.ReadFile(filePath)
			if err != nil {
				res.SendShared(500, "text/plain", err.Error())
				return
			}
			// SendShared falls back to Send automatically if body exceeds
			// the 8 KB inline cap, so it's safe to use on any size payload.
			res.SendShared(200, "application/json", string(data))
		})
		app.GetAsync("/db", func(res *gogo.Response, req *gogo.Request) {
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
		app.GetAsync("/user/:id", func(res *gogo.Response, req *gogo.Request) {
			res.SendShared(200, "application/json",
				fmt.Sprintf(`{"id":%q}`+"\n", req.Parameter(0)))
		})
	}

	if n == 1 {
		// Keep the simple path for the historical single-loop benchmark.
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
	log.Printf("gogo~ listening on http://localhost:3002 (%d cores via SO_REUSEPORT)", n)
	handle.Wait()
}

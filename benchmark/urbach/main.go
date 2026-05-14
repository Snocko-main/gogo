package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"runtime"
	"time"

	"git.urbach.dev/go/web"
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
	s := web.NewServer()

	s.Get("/plain", func(ctx web.Context) error {
		ctx.Response().SetHeader("Content-Type", "text/plain; charset=utf-8")
		return ctx.String("hello world\n")
	})

	s.Get("/json", func(ctx web.Context) error {
		ctx.Response().SetHeader("Content-Type", "application/json")
		return ctx.String(`{"message":"hello world","ok":true}` + "\n")
	})

	s.Get("/hello/:name", func(ctx web.Context) error {
		ctx.Response().SetHeader("Content-Type", "text/plain; charset=utf-8")
		return ctx.String("hello " + ctx.Request().Param("name") + "\n")
	})

	s.Get("/sleep", func(ctx web.Context) error {
		time.Sleep(2 * time.Millisecond)
		ctx.Response().SetHeader("Content-Type", "text/plain; charset=utf-8")
		return ctx.String("slept\n")
	})

	filePath := os.Getenv("BENCH_FILE")
	if filePath == "" {
		filePath = "benchmark/data/sample.json"
	}
	s.Get("/file", func(ctx web.Context) error {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return ctx.Status(500).String(err.Error())
		}
		ctx.Response().SetHeader("Content-Type", "application/json")
		return ctx.Bytes(data)
	})

	dbPath := os.Getenv("BENCH_DB")
	if dbPath == "" {
		dbPath = "/tmp/uwsbench/sample.db"
	}
	if err := initDB(dbPath); err != nil {
		log.Fatal(err)
	}
	s.Get("/db", func(ctx web.Context) error {
		id := rand.IntN(1000) + 1
		var name, email, role string
		err := dbConn.QueryRow("SELECT name, email, role FROM users WHERE id = ?", id).Scan(&name, &email, &role)
		if err != nil {
			return ctx.Status(500).String(err.Error())
		}
		ctx.Response().SetHeader("Content-Type", "application/json")
		return ctx.String(fmt.Sprintf(`{"id":%d,"name":%q,"email":%q,"role":%q}`+"\n", id, name, email, role))
	})

	log.Println("urbach/web listening on http://localhost:3006")
	log.Fatal(s.Run(":3006"))
}

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

	"github.com/gofiber/fiber/v2"
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
	prefork := os.Getenv("FIBER_PREFORK") == "1"
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		Prefork:               prefork,
	})

	app.Get("/plain", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/plain; charset=utf-8")
		return c.SendString("hello world\n")
	})

	app.Get("/hello", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/plain; charset=utf-8")
		return c.SendString("hello world\n")
	})

	// /health mirrors gogo's static Reply route — short JSON body.
	app.Get("/health", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "application/json")
		return c.SendString(`{"ok":true}` + "\n")
	})

	app.Get("/json", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "application/json")
		return c.SendString(`{"message":"hello world","ok":true}` + "\n")
	})

	app.Get("/hello/:name", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/plain; charset=utf-8")
		return c.SendString("hello " + c.Params("name") + "\n")
	})

	app.Get("/sleep", func(c *fiber.Ctx) error {
		time.Sleep(2 * time.Millisecond)
		c.Set("Content-Type", "text/plain; charset=utf-8")
		return c.SendString("slept\n")
	})

	filePath := os.Getenv("BENCH_FILE")
	if filePath == "" {
		filePath = "data/sample.json"
	}
	app.Get("/file", func(c *fiber.Ctx) error {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return c.Status(500).SendString(err.Error())
		}
		c.Set("Content-Type", "application/json")
		return c.Send(data)
	})

	dbPath := os.Getenv("BENCH_DB")
	if dbPath == "" {
		dbPath = "/tmp/uwsbench/sample.db"
	}
	if err := initDB(dbPath); err != nil {
		log.Fatal(err)
	}
	app.Get("/db", func(c *fiber.Ctx) error {
		id := rand.IntN(1000) + 1
		var name, email, role string
		err := dbConn.QueryRow("SELECT name, email, role FROM users WHERE id = ?", id).Scan(&name, &email, &role)
		if err != nil {
			return c.Status(500).SendString(err.Error())
		}
		c.Set("Content-Type", "application/json")
		return c.SendString(fmt.Sprintf(`{"id":%d,"name":%q,"email":%q,"role":%q}`+"\n", id, name, email, role))
	})

	// POST /echo echoes the request body back unchanged.
	app.Post("/echo", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "application/json")
		return c.Send(c.Body())
	})

	// POST /query: body carries an integer id; look it up in SQLite
	// and return the row as JSON. Realistic API shape.
	app.Post("/query", func(c *fiber.Ctx) error {
		id, _ := strconv.Atoi(strings.TrimSpace(string(c.Body())))
		if id < 1 || id > 1000 {
			id = 1
		}
		var name, email, role string
		err := dbConn.QueryRow("SELECT name, email, role FROM users WHERE id = ?", id).Scan(&name, &email, &role)
		if err != nil {
			return c.Status(500).SendString(err.Error())
		}
		c.Set("Content-Type", "application/json")
		return c.SendString(fmt.Sprintf(`{"id":%d,"name":%q,"email":%q,"role":%q}`+"\n", id, name, email, role))
	})

	if prefork {
		log.Println("Fiber listening on http://localhost:3004 (prefork)")
	} else {
		log.Println("Fiber listening on http://localhost:3004")
	}
	log.Fatal(app.Listen(":3004"))
}

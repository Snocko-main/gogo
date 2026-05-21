package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime"
	"time"

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
	mux := http.NewServeMux()

	mux.HandleFunc("GET /plain", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("hello world\n"))
	})

	mux.HandleFunc("GET /hello", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("hello world\n"))
	})

	mux.HandleFunc("GET /json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"message":"hello world","ok":true}` + "\n"))
	})

	mux.HandleFunc("GET /hello/{name}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("hello " + r.PathValue("name") + "\n"))
	})

	mux.HandleFunc("GET /sleep", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Millisecond)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("slept\n"))
	})

	dbPath := os.Getenv("BENCH_DB")
	if dbPath == "" {
		dbPath = "/tmp/uwsbench/sample.db"
	}
	if err := initDB(dbPath); err != nil {
		log.Fatal(err)
	}
	mux.HandleFunc("GET /db", func(w http.ResponseWriter, r *http.Request) {
		id := rand.IntN(1000) + 1
		var name, email, role string
		err := dbConn.QueryRow("SELECT name, email, role FROM users WHERE id = ?", id).Scan(&name, &email, &role)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":%d,"name":%q,"email":%q,"role":%q}`+"\n", id, name, email, role)
	})

	// POST /echo: read the body, write it back unchanged.
	mux.HandleFunc("POST /echo", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64*1024))
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	})

	server := &http.Server{
		Addr:    ":3001",
		Handler: mux,
	}

	log.Println("net/http listening on http://localhost:3001")
	log.Fatal(server.ListenAndServe())
}

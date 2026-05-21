// Actix-web HTTP benchmark mirror — same shape as benchmark/gogo:
//   GET  /hello             → "hello world\n"
//   GET  /hello/:name       → "hello <name>\n"
//   GET  /db                → random row from a 1000-row sqlite table
//   POST /echo              → echo the request body back unchanged
//   POST /query             → body carries an integer id; look up row
//
// Single-worker by default to match the gogo single-core run. Set
// ACTIX_WORKERS=N to widen.

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use once_cell::sync::OnceCell;
use rand::Rng;
use rusqlite::{params, Connection};
use std::env;
use std::sync::Mutex;

// One sqlite connection guarded by a mutex. Matches the contention
// profile of the Go benchmark, which routes through a single
// sql.DB connection pool. SQLite isn't multi-reader-safe across
// threads without WAL + per-thread connections; for an apples-to-
// apples comparison with sql.DB (which serializes through the
// same shared pool) one mutex-guarded conn is the fair choice.
static DB: OnceCell<Mutex<Connection>> = OnceCell::new();

fn init_db(path: &str) -> rusqlite::Result<()> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            role TEXT
        )",
        [],
    )?;

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    if count < 1000 {
        let tx = conn.unchecked_transaction()?;
        for i in 1..=1000 {
            tx.execute(
                "INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)",
                params![
                    i,
                    format!("User {}", i),
                    format!("user{}@example.com", i),
                    "user"
                ],
            )?;
        }
        tx.commit()?;
    }
    DB.set(Mutex::new(conn))
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(())
}

#[get("/hello")]
async fn hello() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/plain; charset=utf-8")
        .body("hello world\n")
}

#[get("/hello/{name}")]
async fn hello_name(path: web::Path<String>) -> impl Responder {
    let name = path.into_inner();
    HttpResponse::Ok()
        .content_type("text/plain; charset=utf-8")
        .body(format!("hello {}\n", name))
}

#[get("/db")]
async fn db_row() -> impl Responder {
    // Offload the blocking sqlite call to a thread pool — matches
    // the Go side where the runtime schedules the blocking
    // sql.DB.QueryRow on whatever goroutine is free.
    let body = web::block(|| -> rusqlite::Result<String> {
        let id: i64 = rand::thread_rng().gen_range(1..=1000);
        let conn = DB.get().expect("db not initialized").lock().unwrap();
        let (name, email, role): (String, String, String) = conn.query_row(
            "SELECT name, email, role FROM users WHERE id = ?",
            params![id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        Ok(format!(
            "{{\"id\":{},\"name\":\"{}\",\"email\":\"{}\",\"role\":\"{}\"}}\n",
            id, name, email, role
        ))
    })
    .await;

    match body {
        Ok(Ok(s)) => HttpResponse::Ok().content_type("application/json").body(s),
        _ => HttpResponse::InternalServerError().body("db error"),
    }
}

// POST /echo: read the request body and write it back unchanged.
// Exercises the request-body collection path.
#[post("/echo")]
async fn echo(body: web::Bytes) -> impl Responder {
    HttpResponse::Ok().content_type("application/json").body(body)
}

// POST /query: body carries an integer id; look it up in SQLite and
// return the row as JSON. Offload the blocking sqlite call via
// web::block so the runtime can keep serving other requests on the
// same worker.
#[post("/query")]
async fn db_query_post(body: web::Bytes) -> impl Responder {
    let text = std::str::from_utf8(&body).unwrap_or("0");
    let mut id: i64 = text.trim().parse().unwrap_or(1);
    if !(1..=1000).contains(&id) {
        id = 1;
    }
    let result = web::block(move || -> rusqlite::Result<String> {
        let conn = DB.get().expect("db not initialized").lock().unwrap();
        let (name, email, role): (String, String, String) = conn.query_row(
            "SELECT name, email, role FROM users WHERE id = ?",
            params![id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        Ok(format!(
            "{{\"id\":{},\"name\":\"{}\",\"email\":\"{}\",\"role\":\"{}\"}}\n",
            id, name, email, role
        ))
    })
    .await;
    match result {
        Ok(Ok(s)) => HttpResponse::Ok().content_type("application/json").body(s),
        _ => HttpResponse::InternalServerError().body("db error"),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db_path = env::var("BENCH_DB").unwrap_or_else(|_| "/tmp/uwsbench/sample.db".to_string());
    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    init_db(&db_path).expect("init_db");

    let workers: usize = env::var("ACTIX_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    println!("actix listening on 0.0.0.0:{} (workers={})", port, workers);

    HttpServer::new(|| {
        App::new()
            .service(hello)
            .service(hello_name)
            .service(db_row)
            .service(echo)
            .service(db_query_post)
    })
    .workers(workers)
    .bind(("0.0.0.0", port))?
    .run()
    .await
}

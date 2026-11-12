//! malachi-memory: an MCP server (stdio) that keeps notes across sessions in
//! an embedded Grafeo graph.
//!
//! Usage: malachi-memory --db <path/to/memory.grafeo>  (or MEMORY_DB_PATH)
//!
//! It speaks MCP 2026-07-28 (server/discover, stateless requests) and the
//! 2025 initialize handshake, over newline-delimited JSON-RPC. The database
//! is opened for each call, so every session's server can share it.

mod memory;
mod protocol;

use std::path::PathBuf;

fn main() {
    let mut db = std::env::var_os("MEMORY_DB_PATH").map(PathBuf::from);
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--db" => db = args.next().map(PathBuf::from),
            "--version" => {
                println!("malachi-memory {}", env!("CARGO_PKG_VERSION"));
                return;
            }
            _ => {
                eprintln!("usage: malachi-memory --db <path> (or MEMORY_DB_PATH)");
                std::process::exit(2);
            }
        }
    }
    let Some(db) = db else {
        eprintln!("malachi-memory: no database: pass --db <path> or set MEMORY_DB_PATH");
        std::process::exit(2);
    };
    let db = match memory::Db::new(&db) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("malachi-memory: open {}: {e}", db.display());
            std::process::exit(1);
        }
    };
    let stdin = std::io::stdin().lock();
    let stdout = std::io::stdout().lock();
    protocol::serve(&db, stdin, stdout);
}

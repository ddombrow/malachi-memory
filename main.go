package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ddombrow/malachi-memory/memory"
)

func main() {
	dbPath := os.Getenv("MEMORY_DB_PATH")
	flag.StringVar(&dbPath, "db", dbPath, "path to Pebble database (or MEMORY_DB_PATH)")
	version := flag.Bool("version", false, "print version")
	backup := flag.String("backup", "", "write a consistent copy of the database to this new directory and exit")
	flag.Parse()
	if *version {
		fmt.Println("malachi-memory " + memory.Version)
		return
	}
	if dbPath == "" {
		fmt.Fprintln(os.Stderr, "malachi-memory: no database: pass --db <path> or set MEMORY_DB_PATH")
		os.Exit(2)
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "malachi-memory: create database directory: %v\n", err)
		os.Exit(1)
	}
	db, err := memory.NewDb(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "malachi-memory: open %s: %v\n", dbPath, err)
		os.Exit(1)
	}
	if *backup != "" {
		if err := db.Backup(*backup); err != nil {
			fmt.Fprintf(os.Stderr, "malachi-memory: backup to %s: %v\n", *backup, err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "malachi-memory: backed up %s to %s\n", dbPath, *backup)
		return
	}
	if err := memory.Serve(db, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "malachi-memory: serve MCP input: %v\n", err)
		os.Exit(1)
	}
}

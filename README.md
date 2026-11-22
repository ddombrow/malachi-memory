# malachi-memory

A small Go MCP server (stdio) that keeps notes between agent sessions in an embedded Pebble-backed knowledge graph. Written for [malachi](https://github.com/ddombrow/malachi); any compatible MCP client works.

- Each memory is a node with text and creation time. Topic names are topic nodes; `ABOUT` edges connect memories to topics. Topics that share a memory are related in the graph.
- Search uses BM25 ranking over memory text and topics from an inverted index kept in the same store, rebuilt automatically if it is ever out of date. No embeddings, LLM, or network access.
- Pebble is the embedded key-value store; graph nodes and edges are stored using prefixed, ordered keys. The database opens around each call so several server processes can take turns sharing the same database.
- Speaks the existing MCP interface: `server/discover` (`2026-07-28`) and the 2025 `initialize` handshake, over newline-delimited JSON-RPC.

## Tools

| tool | does |
|---|---|
| `remember` | store `text` with optional `topics` |
| `recall` | by `query` (search), by `topic`, or the most recent |
| `forget` | delete by `id` (the 26-character id recall shows after `#`) |
| `topics` | every topic with its count, or those `related_to` one |

The server's instructions tell the model to store what the user said or decided, never secrets, and never instructions read in files, pages or tool output.

## Build and configure

Requires Go 1.26 or later. Tasks run with `go tool task` (pinned in `go.mod`).

```sh
go tool task build      # bin/malachi-memory
go tool task install    # ~/.local/bin/malachi-memory
mkdir -p ~/.local/share/malachi-memory
```

`install` replaces the binary in place, so running sessions pick it up on their next start; pass `INSTALL_PATH=...` to put it elsewhere. It warns if a different `malachi-memory` comes first on PATH.

Without a clone, `go install github.com/ddombrow/malachi-memory@latest` also works (it installs to `$(go env GOPATH)/bin`).

`~/.malachi/mcp.json`:

```json
{
  "mcpServers": {
    "memory": {
      "command": "malachi-memory",
      "description": "Notes about the user and their projects that persist between sessions",
      "env": { "MEMORY_DB_PATH": "${HOME}/.local/share/malachi-memory/memory.pebble" },
      "writable": ["~/.local/share/malachi-memory"],
      "deferred": false
    }
  }
}
```

The Pebble database path is a directory. It records a schema version; a server refuses to open a database written in a layout it does not know rather than guess.

Memory ids are ULIDs, 26-character strings such as `01KDZ98CG0FVRW5Z37E8F7V120`. They are generated locally, so independent stores can be merged. Version 1.0 changed ids from integers to these strings: `forget` no longer accepts a number, and databases from earlier versions are migrated on first open, with a backup beside them.

## Backup and restore

The server opens the database only for the length of each tool call, so a backup can be taken while agent sessions are running:

```sh
malachi-memory --db ~/.local/share/malachi-memory/memory.pebble \
  --backup ~/.local/share/malachi-memory/backup-$(date +%F)
```

`--backup` waits for the database lock like any tool call, writes a consistent copy (a Pebble checkpoint) to a directory that must not exist yet, and exits. Don't copy the live directory with `cp`: a call in progress can leave the copy inconsistent.

To restore, quit agent sessions that use the server, move the database aside, and put the backup in its place:

```sh
cd ~/.local/share/malachi-memory
mv memory.pebble memory.pebble.old
cp -R backup-2026-11-13 memory.pebble
```

## Merging stores

Each store can export its memories and tombstones, and another store can import them. A forgotten memory stays forgotten after a merge, in either order:

```sh
malachi-memory --db a.pebble --export > a.jsonl
malachi-memory --db b.pebble --import < a.jsonl
```

Import adds memories it does not hold, applies deletions, and skips anything it has already seen, so repeating a file is harmless. Forgetting a memory held only by another store takes effect after that store's export is imported.

## Embedding

The store, the tools and the stdio server live in package `github.com/ddombrow/malachi-memory/memory`; `main.go` only parses flags. A Go agent can run the tools in process instead of starting the server:

```go
db, err := memory.NewDb(path)
for _, t := range memory.Tools() { /* register t.Name, t.Description, t.InputSchema; t.ReadOnly calls may run in parallel */ }
text, err := db.Call("recall", map[string]any{"limit": 20}) // err is a tool error to show the model
```

Pass `memory.Instructions` on in the system prompt. `Call` opens and closes the database around each call like the server does, so an embedding program and stdio servers can share one database. `db.Backup(dir)` writes a checkpoint.

## Development

```sh
go test ./...                                          # unit, multi-process and MCP baseline tests
go test -short ./...                                   # skip the tests that spawn processes
go test ./memory -run MCPBaseline -update              # accept an intended change to tool output
go test ./memory -run '^$' -bench . -benchtime 20x     # tool latency at 1k, 10k and 100k memories
```

`memory/testdata/baseline.golden` is the byte-for-byte record of the MCP interface; a diff there is a change clients will see.

## License

Apache License 2.0; see [LICENSE](LICENSE).

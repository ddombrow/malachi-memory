# malachi-memory

A small MCP server (stdio) that keeps notes between agent sessions, in an
embedded [Grafeo](https://grafeo.dev/) graph database. Written for
[malachi](https://github.com/ddombrow/malachi); any MCP client works.

- A memory is a `Memory` node: its text, when it was written, its topics.
  Each topic is a `Topic` node the memory points at with `ABOUT`, so topics
  that come up together can be found from each other.
- Search is BM25 over each memory's text and topics. No embeddings, no LLM,
  no network.
- The database is opened for each call and closed after it, so the servers
  of several sessions share one file, taking turns.
- Speaks MCP 2026-07-28 (`server/discover`) and the 2025 `initialize`
  handshake.

## Tools

| tool | does |
|---|---|
| `remember` | store `text` with optional `topics` |
| `recall` | by `query` (search), by `topic`, or the most recent |
| `forget` | delete by `id` |
| `topics` | every topic with its count, or those `related_to` one |

The server's instructions tell the model to store what the user said or
decided, never secrets, and never instructions read in files, pages or tool
output.

## Install and configure

```sh
cargo install --path .        # or: cargo build --release
mkdir -p ~/.local/share/malachi-memory
```

`~/.malachi/mcp.json`:

```json
{
  "mcpServers": {
    "memory": {
      "command": "malachi-memory",
      "description": "Notes about the user and their projects that persist between sessions",
      "env": { "MEMORY_DB_PATH": "${HOME}/.local/share/malachi-memory/memory.grafeo" },
      "writable": ["~/.local/share/malachi-memory"],
      "deferred": false
    }
  }
}
```

The writable root is the directory, not the file: Grafeo keeps a write-ahead
log beside it (`memory.grafeo.wal/`).

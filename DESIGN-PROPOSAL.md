# Storage and knowledge graph improvement proposal

## Goals

- Keep the existing MCP tools and their response formats stable: `remember`, `recall`, `forget`, and `topics`.
- Improve graph consistency and search scalability without replacing Pebble prematurely.
- Support multiple agent sessions safely, with a clear path to concurrent use if contention warrants it.
- Keep the graph model focused on actual tool and user needs.

## Principles

1. Pebble remains the embedded storage engine unless measurements or required features show it is unsuitable.
2. Graph node and edge records are authoritative. Search indexes are derived and rebuildable.
3. Keep migrations versioned, tested, and recoverable; never assume an existing database can simply be discarded.
4. Measure performance and lock contention before adding a daemon or more complex graph features.
5. Add typed relationships only alongside a concrete query or workflow that uses them.

## Phase 1: Establish a safe baseline

- Add a schema version and document how to back up and restore the Pebble database.
- Add tests for persistence, deletion, topic recall, and simultaneous server processes.
- Benchmark search and write latency at representative store sizes. Include enough data to expose how the current full scan behaves as the store grows.
- Record the existing MCP behavior as a compatibility baseline.

**Exit criteria:** behavior is covered by tests, a backup/restore procedure is documented, and performance has a reproducible baseline.

### Phase 1 results (2026-11-13)

Done: schema version key (`meta/schema-version` = 1, checked on every open); `--backup` via Pebble checkpoint, with restore documented in the README; tests for persistence, deletion across every view, topic recall order, schema refusal, backup, and four concurrent server processes doing overlapping reads and writes; a golden MCP transcript (`testdata/baseline.golden`); benchmarks in `bench_test.go`.

Bugs found and fixed:
- Lock retry never fired across processes. On macOS a held lock surfaces as `resource temporarily unavailable`, which the old `contains "lock"` check missed, so a second session failed immediately. Now `EAGAIN`/`EACCES` are detected and a timeout returns a clear "busy" error.
- `NewDb` opened without retry, so a server starting while another held the lock exited.
- Pebble's WAL-replay log lines went to stderr on every call; they are now dropped.

Baseline, Apple Silicon laptop, per tool call including open/close (`-benchtime 20x`):

| call | 1k | 10k | 100k |
|---|---|---|---|
| open + close only | 34 ms | 31 ms | 30 ms |
| remember | 48 ms | 46 ms | 96 ms |
| recall by query | 35 ms | 65 ms | 357 ms |
| recall by topic | 34 ms | 31 ms | 45 ms |
| recall recent / topics | 33-35 ms | 37-38 ms | 98-100 ms |

What this says about the later phases:
- The fixed ~30 ms open/close dominates every call below about 10k memories. If latency matters, cutting open cost (or holding the database open between nearby calls) gains more than any index.
- Search's full scan matters only near 100k memories, far beyond expected personal use. Phase 3 should wait until a real store approaches that size.
- `recent` and `topics` also scan every memory and grow at the same rate as search; Phase 2's edge-based topic counts and an ordered by-time key would fix both cheaply.
- Known gap carried into Phase 2: `Forget` leaves `t/<topic>` keys behind when a topic's last memory goes. Nothing reads them yet.

## Phase 2: Make graph records authoritative

Keep the initial graph small:

- **Memory nodes** hold the memory text and creation time.
- **Topic nodes** represent normalized topic names.
- **`ABOUT` edges** link memories to their topics.

Make node and edge records the source of truth instead of duplicating topic membership in memory records.

- Choose key layouts that support efficient topic-to-memory traversal.
- Write or delete a memory and its associated edges in one Pebble batch so graph updates are atomic.
- Add a versioned migration from the current record layout. Make it safe to rerun or resume, and test it using a copy of existing data.
- Keep the original data recoverable until migration has been verified.

**Exit criteria:** graph reads use node and edge records; a migration test demonstrates that existing memories, topics, and relationships are preserved.

### Phase 2 results (2026-11-13)

Done: schema v2 (layout documented at the top of the keys section in `memory.go`). Memory nodes no longer carry topics; ABOUT edges are stored in both directions (`e/<topic>\x00<id>` and `r/<id><topic>`, the latter holding topic order), and topic nodes hold their memory count and are deleted when it reaches zero. Remember and forget update the node, both edges and the count in one batch.

Migration (`migrate.go`) runs on the first open of a v1 or unversioned database: it writes a checkpoint to `<db>.v1-backup-<timestamp>`, rebuilds all edges and counts from the v1 memory records in one batch with the version bump, then reads the result back and compares it with the v1 records. An interrupted migration leaves v1 intact and reruns. It is tested against `testdata/v1.pebble`, a database written by the v1 server itself.

v1 bugs the new layout removes:
- Topics containing `/` collided: `e/ci/` prefix-matched the edges of `ci/cd`. The edge key now ends a topic with `\x00`, and control characters are stripped from topic names.
- Forgotten topics left orphaned `t/` keys; migration drops them.

`recent` and `recall by topic` walk ids backwards with a limit instead of loading and sorting every memory; `topics` reads the counts. At 100k memories (`-benchtime 20x`):

| call | Phase 1 | Phase 2 |
|---|---|---|
| recall recent | 98 ms | 31 ms |
| topics | 100 ms | 33 ms |
| topics related_to | 98 ms | 40 ms |
| recall by topic | 45 ms | 29 ms |
| recall by query | 357 ms | 378 ms (unchanged: Phase 3) |

All but search now cost about one database open (~30 ms). `remember` at 100k (~100 ms) is still slower than at 10k and is worth a look, likely Pebble work on open over the large seeded WAL rather than the graph writes.

### Read-only opens (2026-11-13)

A read-write Pebble open costs ~28 ms and its close ~5 ms, nearly all of it syncing a new WAL, manifest and options file (macOS full syncs). A read-only open writes nothing and takes ~0.4 ms; it still takes the exclusive lock, so it is safe beside other sessions' writes. `recall` and `topics` (the tools marked `readOnlyHint`) now open read-only via `Db.View`; a read-only open that finds a schema needing migration reopens read-write.

| call | 1k | 10k | 100k |
|---|---|---|---|
| recall by topic / recent, topics | 0.2-0.3 ms | 0.3 ms | 0.3 ms |
| topics related_to | 0.3 ms | 1.2 ms | 8.7 ms |
| recall by query | 5 ms | 37 ms | 345 ms |
| remember (read-write) | 45 ms | 47 ms | 103 ms |

Reads now hold the lock for well under a millisecond, except search, whose full scan is now its whole cost and the main reason a read holds the lock. That moves Phase 3 up if stores grow past a few thousand memories; at ~1k it costs 5 ms.

## Phase 3: Add an inverted search index

Replace the current search scan with a derived token-to-memory postings index.

- Normalize tokens consistently and map each token to the memories containing it.
- Store enough per-memory token information to update postings when a memory is forgotten or changed.
- Apply memory and index changes in the same Pebble batch.
- Support rebuilding the index from authoritative graph records. The index must be disposable and recoverable.
- Test matching, ranking, case and Unicode normalization, index rebuilds, and cleanup after deletion.

**Exit criteria:** text search examines matching postings rather than scanning every memory, and rebuilding the index produces equivalent results.

## Phase 4: Support multiple agent sessions

Pebble permits one process to own a database at a time. The current open-per-call pattern lets separate MCP processes take turns, but it does not provide simultaneous database access; requests can wait for the database lock.

### Initial approach

Keep the current approach while contention is low, but make its behavior explicit:

- Use bounded backoff and a configurable lock-wait limit.
- Return a clear busy error when the limit is reached.
- Add process-level tests with overlapping readers and writers, checking that no updates are lost or partially applied.
- Measure lock wait time and request latency so real contention is visible.

### Escalation if contention is frequent

Move Pebble ownership into a single local service process. MCP stdio processes forward tool requests to it over a permission-restricted Unix socket. The service keeps Pebble open and coordinates operations, while clients continue to use the same MCP interface.

The service design should include:

- Socket permissions and local-only access controls.
- Request framing, error handling, and compatibility/versioning for the local protocol.
- A clear service lifecycle and behavior when the service is unavailable.
- Integration tests with several MCP clients issuing reads and writes concurrently.

Do not add this service until benchmarks or real usage show that lock waits are a problem.

**Exit criteria:** parallel-session tests show no lost or partial updates, and lock-wait measurements establish whether the open-per-call design is still adequate.

## Phase 5: Add typed relationships when needed

Keep `ABOUT` as the only edge type until a concrete feature requires more. Examples might include a project using a language or a decision superseding another decision.

When adding a relationship:

- Define its direction and semantics explicitly.
- Add it with the tool behavior or query that needs it.
- Test creation, traversal, deletion, and migration behavior.
- Avoid adding relationship types solely to make the graph appear more general.

**Exit criteria:** every relationship type supports a real user-facing query or workflow.

## Suggested order

1. Establish tests, backups, and performance baselines.
2. Make graph node and edge records authoritative; add and verify migration.
3. Add a rebuildable inverted search index.
4. Measure multi-session lock contention; add a local Pebble-owning service only if needed.
5. Add typed relationships when a concrete feature calls for them.

## Risks and tradeoffs

- Schema and index migrations can lose data if they are not tested against real database copies; preserve backups and make migrations recoverable.
- An inverted index improves search scaling but adds write and migration complexity; keep it derived and rebuildable.
- A local service improves coordination across MCP processes but adds a process to configure, secure, and operate. Prefer the simpler lock-based approach while it meets measured needs.
- A richer graph can make queries more expressive, but unnecessary edge types increase schema and maintenance costs.

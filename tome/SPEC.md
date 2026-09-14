# Tome v1 Specification

**Date:** 2026-03-23
**Status:** Draft for build
**Owner:** Juxtapo + Codex

Tome is the hot local search daemon that replaces Mata's query path while reusing Mata's existing crawl data for first boot.

## Goal

Done means:

- the existing Mata databases can be imported into Tantivy without re-crawling 3.35M files
- one long-lived Rust daemon serves local search over Unix socket
- `tome search`, `tome find`, and `tome status` work without the Mata HTTP server
- common `armaros` behavior still exists through a thin compatibility wrapper
- repeated searches stop paying Python + HTTP cold-path tax

## Current World

### Existing data sources

| Source | Path | Files | Size |
|--------|------|-------|------|
| `linux` | `/home/juxtapo/Server_files/nudger/mata/mata_linux.db` | ~1.55M | ~1.9GB |
| `windows` | `/home/juxtapo/Server_files/nudger/mata/mata.db` | ~1.8M | ~1.0GB |

### Existing Mata schema

Mata already gives us the fields we need for v1 import:

- `path`
- `filename`
- `extension`
- `size_bytes`
- `modified_at`
- `indexed_at`
- `content_preview`
- `file_hash`

### Current operator behavior to preserve

Current `armaros` effectively does this:

- local Linux: `plocate` filename hits + Mata content search
- Windows: direct SQLite query on `mata.db`
- `--win` and `--both` flags matter
- result merge is by `path`, with indexed/content-backed results winning over fallback hits

Tome should preserve that muscle memory where it matters, but the implementation should become:

- Tantivy for indexed search
- optional `plocate` fallback for local Linux filename coverage
- no HTTP dependency

## Non-Goals For v1

- no fresh filesystem crawler
- no live incremental watcher
- no manual shard fan-out unless profiling proves a single index is not enough
- no distributed search
- no MCP-native server inside Tome
- no attempt to replace `knowledge`, memories, or Catalyst jobs search in this phase

## Scope For v1

Ship these subcommands first:

```bash
tome import --source linux --db /path/to/mata_linux.db
tome import --source windows --db /path/to/mata.db
tome daemon
tome search <query> [--source linux|windows|both] [-e .py] [--limit 20] [--json] [--no-plocate]
tome find <name> [--source linux|windows|both] [-e .py] [--limit 20] [--json] [--no-plocate]
tome status [--json]
```

Anything beyond that is phase 2.

## Design Calls

### 1. Keep v1 to two indexes, not six shards

Use one Tantivy index per source:

- `linux`
- `windows`

Reason:

- import/search complexity stays low
- operational reloads are simpler
- we can profile the real workload first
- manual filename sharding can be added later if Tantivy actually needs help

If profiling later proves one index per source is too slow, add sharding in phase 2 with evidence.

### 2. Use Unix socket, not HTTP

Use a local-only Unix socket for daemon requests:

- lower overhead
- local trust boundary
- simpler than maintaining another HTTP service

Default socket path:

```text
/home/juxtapo/Server_files/tome/runtime/tome.sock
```

### 3. Preserve `armaros` behavior through compatibility, not CLI pollution

`tome` gets a clean CLI.

`/usr/local/bin/armaros` can stay as the user-facing legacy wrapper and translate:

- `--win`
- `--both`
- positional `limit`
- `full`

That keeps Tome sane while preserving operator habit.

## Storage Layout

```text
/home/juxtapo/Server_files/tome/
├── Cargo.toml
├── src/
├── index/
│   ├── linux/
│   └── windows/
├── runtime/
│   └── tome.sock
├── meta/
│   ├── linux.json
│   └── windows.json
└── target/release/tome
```

`meta/<source>.json` should contain at least:

- source name
- imported document count
- imported_at timestamp
- source db path
- source db mtime at import time
- schema version

## Tantivy Schema

Use one document per file.

### Required fields

| Field | Type | Indexed | Stored | Notes |
|-------|------|---------|--------|------|
| `path` | text | yes | yes | searchable path |
| `filename` | text | yes | yes | searchable filename |
| `filename_ngram` | text | yes | no | substring-heavy `find` field |
| `extension` | string | yes | yes | exact filter |
| `content_preview` | text | yes | yes | imported snippet/search body |
| `source` | string | yes | yes | `linux` or `windows` |
| `size_bytes` | u64 | no | yes | display only in v1 |
| `modified_at` | f64 | no | yes | unix timestamp |
| `indexed_at` | f64 | no | yes | imported from Mata |
| `file_hash` | string | no | yes | stored for future diff work |

### Query intent

- `search` hits `filename`, `path`, and `content_preview`
- `find` prefers `filename_ngram`, then `filename`
- extension filtering is exact match on `extension`
- source filtering is exact match on `source`

### Ranking intent

For `search`, bias ranking in this order:

1. filename hit
2. path hit
3. content preview hit

Exact scoring can be tuned later, but filename/path should not lose to weak content-preview noise.

## Import Contract

### Command

```bash
tome import --source linux --db /home/juxtapo/Server_files/nudger/mata/mata_linux.db
```

### Behavior

- opens the SQLite database read-only
- streams rows in batches instead of loading the world into RAM
- creates or replaces the target Tantivy index for the requested source
- writes `meta/<source>.json` only after a successful commit
- exits non-zero on schema mismatch, read failure, or index write failure

### Replace semantics

Default behavior: refuse to overwrite an existing source index.

To rebuild, require an explicit flag:

```bash
tome import --source linux --db ... --replace
```

That avoids accidental nukes.

### Import output

Human mode should show:

- source
- input DB path
- imported count
- elapsed time
- output index path

`--json` should return the same facts in machine-readable form.

## Search Contract

### `tome search`

Purpose: full-text search across imported fields.

Rules:

- default source is `linux`
- `--source both` searches `linux` and `windows` in parallel and merges results
- `-e .py` filters by exact extension
- default limit is `20`
- hard cap limit at `100`
- results merge by `path`
- when fallback results and indexed results collide, indexed results win

### `tome find`

Purpose: filename-focused lookup.

Rules:

- case-insensitive intent
- use the ngram-backed filename field for substring-like matching
- same source, extension, limit, and output rules as `search`

### `plocate` fallback

Fallback is allowed only when the requested source includes `linux`.

Default behavior:

- `search` may append `plocate` filename hits after indexed hits
- `find` may append `plocate` filename hits after indexed hits
- fallback hits are labeled `backend: "plocate"`
- `--no-plocate` disables it

Windows never uses `plocate`.

## Daemon Contract

### Lifecycle

`tome daemon`:

- opens available indexes
- binds the Unix socket
- removes a stale socket file on startup if needed
- keeps searchers hot in memory
- exits cleanly on `SIGINT` and `SIGTERM`

If one source index is missing, daemon still starts in degraded mode and reports that in `status`.

### IPC format

Use newline-delimited JSON over the Unix socket.

Request examples:

```json
{"action":"search","query":"roti","source":"linux","limit":20}
{"action":"find","query":"mozart","source":"both","ext":".py","limit":10}
{"action":"status"}
```

Response envelope:

```json
{
  "ok": true,
  "elapsed_ms": 3,
  "total": 42,
  "results": []
}
```

### Result shape

Each result should expose at least:

```json
{
  "path": "/home/juxtapo/example.py",
  "filename": "example.py",
  "extension": ".py",
  "source": "linux",
  "backend": "tantivy",
  "snippet": "matched preview text",
  "size_bytes": 1234,
  "modified_at": 1742700000.0
}
```

## Status Contract

`tome status` should report:

- daemon reachable or not
- socket path
- loaded sources
- per-source document counts
- index paths
- process uptime
- schema version

If daemon is not running, `tome status` should still be able to report local index presence in a degraded local-only mode when practical.

## Build Order

1. create the Rust crate and CLI skeleton
2. implement `import`
3. implement direct in-process `search` and `find` against the built indexes
4. wrap them with `daemon` + Unix socket client path
5. implement `status`
6. repoint `armaros` to Tome compatibility mode
7. measure before deciding whether sharding or recrawling work is justified

## Exit Criteria

Tome v1 is done when all of this is true:

- both Mata databases import successfully into Tantivy
- daemon-backed `tome search roti` works with Mata offline
- daemon-backed `tome find mozart --source both` works
- `armaros roti`, `armaros roti --win`, and `armaros roti --both` still produce sane results through Tome
- repeated local searches are materially faster than the current Python + HTTP path
- `tome status` shows loaded sources and counts

## Verification Plan

### Functional

1. import Linux DB and verify imported count matches `SELECT COUNT(*) FROM files`
2. import Windows DB and verify the same
3. compare a small set of known queries against current Mata behavior:
   - `roti`
   - `cendawan`
   - `mozart`
   - one extension-filtered query such as `-e .py`
4. verify `--source both` merges and dedupes by `path`
5. stop Mata on `127.0.0.1:9874` and confirm Tome still works

### Performance

Measure at least:

- cold CLI query time
- repeated hot daemon query time
- current `armaros` query time

The target is clear direction, not benchmark theatre: Tome should obviously beat the current path on repeated local queries.

## Phase 2, Not v1

Only revisit these after v1 lands and is measured:

- manual sharding
- native recrawler / `tome index`
- live incremental updates
- Body-level direct integration
- richer ranking and snippet generation
- cross-domain unification with `knowledge`, memories, or Catalyst

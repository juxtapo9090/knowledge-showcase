# Tome — Reindex Feature Request

## What's missing

Tome currently has `tome import --db <mata.db>` to ingest from Mata's SQLite databases, but no independent crawler to index files directly from the filesystem.

## What we need

```
tome index --root /home/juxtapo [--incremental]
tome index --reindex
```

### Behavior

- Walk configured root directories (same roots Mata uses)
- Skip dirs: `.git`, `node_modules`, `target`, `__pycache__`, `.cache`, etc.
- Skip binary/large files (same rules as Mata's `should_skip_file`)
- Index: `path`, `filename`, `extension`, `content_preview` (first N chars of text files), `size_bytes`, `modified_at`
- `--incremental`: only index new/changed files (compare mtime + size hash)
- `--reindex`: full rebuild from scratch
- Write directly into existing Tantivy index at `/home/juxtapo/Server_files/tome/index/linux/`

### Reference

- Mata's crawler logic: `/home/juxtapo/Server_files/nudger/mata/mata.py` → functions `crawl_directory`, `process_file`, `should_skip_dir`, `should_skip_file`, `is_text_file`
- Mata's config (roots, skip patterns): check the `config.json` next to `mata.py`
- Existing Tome codebase: `/home/juxtapo/Server_files/tome/src/main.rs`

### Permission note

- Normalize file permissions after indexing (same fix already applied for `import`)
- Should work from both `juxtapo` and `root` users

### Nice to have (not required now)

- `tome index --watch` → inotify-based live indexing (future)
- Daemon-triggered periodic reindex via `troller.sh`

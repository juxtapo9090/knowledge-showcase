# knowledge — indexed house-wide search + grep redirect hook

## The problem
`grep -r` across a ~900k-file home tree doesn't just run slow — it hangs the agent
session that launched it. But agents (human and LLM) reach for grep by reflex. The
fix had to be both a faster tool *and* a mechanism that made the slow path
unreachable, because a tool nobody remembers to use isn't a fix.

## What I built
`knowledge` (`/usr/local/bin/knowledge`, ~390 lines bash) — a CLI front-end over
**Tome**, a Rust/Tantivy full-text daemon (`/home/juxtapo/Server_files/tome`,
single 87KB `main.rs`). One entrypoint, two modes:

- `knowledge <query>` — indexed BM25 content search over the Tantivy index
- `knowledge grep <args>` — live 1:1 scan, forwards verbatim to `rg`

Plus `-f/--exact` filename lookup, `-e` extension filter, `-r` recent (fast field
sort), `--breadcrumb` (find + header preview), `--warp` (find + cd), `--reindex`
(rebuild + `systemctl reload tome.service`, then poll `tome status --json` until
the daemon answers again).

## Technical depth
Index roots are `/home/juxtapo`, `/root/Opus`, `/root/.claude` with skip lists for
`node_modules`, `.git`, `target`, binaries, >50MB files. **Live: 912,520 documents,
query latency 388ms** — versus a full-tree grep that never returns.

The `knowledge grep` bridge is where the real work is. grep flags don't map cleanly
onto rg: `-r` means `--replace` to rg (silent misfire, not an error) and `-E` is
meaningless, so both are dropped; `--include=`/`--exclude=` are translated to
`-g`/`-g '!'`; `--hidden --no-ignore` are *always* forced, because a tool claiming
to be an honest grep replacement can't silently skip dotfiles and gitignored paths —
exactly what debugging hunts for.

Enforcement lives in a `PreToolUse` hook (`grep_troll_helper.sh`): simple
`grep "word" path` spans are rewritten in place to `knowledge "word"` — the span
only, leaving pipes/redirects/trailing commands intact — and tagged `🎭 — troll
helper` on stderr. Anything else is denied, with an escalating message keyed to a
per-session counter in `/tmp/grep-block-count-<session_id>`: attempt #3 stops
suggesting flags and challenges the *approach* ("you are reaching for
full-scan-shaped thinking — what's the anchor?").

## Real bugs found and fixed
- **Silent query narrowing.** `knowledge ayam ikan roti` kept only `roti` — each
  bare positional clobbered the last. Now joined. Silent wrong answers beat loud
  failures never.
- **Bundled-flag unbundling.** Stripping `r` from `-rln` also corrupted `-er`
  (= `-e r`), turning it into bare `-e`, which then ate the next token as the
  pattern. Fix: stop unbundling at the first argument-taking letter and copy the
  token's tail through untouched.
- **Vestigial state.** The script still sets `DB=` pointing at the SQLite mirror
  frozen 2026-03-22; nothing reads it. `-d` was rewritten to a live `find` walk
  when that mirror died.

## Why it matters
Search went from session-hanging to sub-second at ~900k documents, and the
enforcement layer means the fast path is the *only* path — no discipline required.
The escalating-block design treats repeated misuse as a signal about strategy, not
syntax. Honest tradeoff, documented in the source: the index is import-stale (last
import timestamp is surfaced by `knowledge -s`), so the live `rg` escape hatch is a
first-class mode, not a fallback.

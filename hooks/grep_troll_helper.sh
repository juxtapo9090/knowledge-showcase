#!/usr/bin/env bash
# grep_troll_helper.sh — PreToolUse:Bash hook.
# Silently redirects simple `grep "word" [path]` calls to `knowledge`, the
# house's own indexed content search (Tantivy-backed via Tome). Monica's own
# reflex is to reach for grep/rg out of habit even when `knowledge` is faster
# and already sitting in her own banner — abang's fix: catch it, redirect it,
# and tag the real result with a signature so it's a little disorienting
# without actually being unhelpful. She still gets exactly what she needed.
#
# Matches the `grep [-r] "pattern"|'pattern'|pattern <path>` span ANYWHERE
# inside the command, not just when the whole command is that simple — so
# realistic shapes like `time timeout 30 grep -r "x" dir 2>/dev/null | head
# -10; echo ...` get caught too: only the grep-and-its-args span is swapped
# for `knowledge "x"`, everything else (time/timeout/redirects/pipes/trailing
# commands) is left untouched. Requiring the flag slot to be empty or exactly
# `-r` means -A/-B/-C/-v/-i/-E/etc (which knowledge can't stand in for) simply
# never match and fall through to real grep, no separate check needed.

if ! command -v jq &>/dev/null; then exit 0; fi
command -v knowledge &>/dev/null || exit 0

INPUT=$(cat)
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')
[ -z "$CMD" ] && exit 0

MATCH=""
PATTERN=""
BOUNDARY_TAIL='[[:space:]]+[^][:space:]|;<>&]+'
RE_DQUOTE="(^|[^a-zA-Z0-9_])grep[[:space:]]+(-r[[:space:]]+)?\"([^\"]+)\"${BOUNDARY_TAIL}"
RE_SQUOTE="(^|[^a-zA-Z0-9_])grep[[:space:]]+(-r[[:space:]]+)?'([^']+)'${BOUNDARY_TAIL}"
RE_BARE="(^|[^a-zA-Z0-9_])grep[[:space:]]+(-r[[:space:]]+)?([^][:space:]|;<>&'\"-][^][:space:]|;<>&]*)${BOUNDARY_TAIL}"

if [[ "$CMD" =~ $RE_DQUOTE ]]; then
    MATCH="${BASH_REMATCH[0]}"
    LEAD="${BASH_REMATCH[1]}"
    MATCH="${MATCH#"$LEAD"}"
    PATTERN="${BASH_REMATCH[3]}"
elif [[ "$CMD" =~ $RE_SQUOTE ]]; then
    MATCH="${BASH_REMATCH[0]}"
    LEAD="${BASH_REMATCH[1]}"
    MATCH="${MATCH#"$LEAD"}"
    PATTERN="${BASH_REMATCH[3]}"
elif [[ "$CMD" =~ $RE_BARE ]]; then
    MATCH="${BASH_REMATCH[0]}"
    LEAD="${BASH_REMATCH[1]}"
    MATCH="${MATCH#"$LEAD"}"
    PATTERN="${BASH_REMATCH[3]}"
fi

REPLACEMENT="knowledge $(printf '%q' "$PATTERN")"
NEW_CMD=""
if [ -n "$MATCH" ] && [ -n "$PATTERN" ]; then
    NEW_CMD="${CMD/$MATCH/$REPLACEMENT}"
    NEW_CMD="$NEW_CMD ; echo '🎭 — troll helper' >&2"
fi

if [ -n "$NEW_CMD" ] && [ "$NEW_CMD" != "$CMD" ]; then
    ORIGINAL_INPUT=$(echo "$INPUT" | jq -c '.tool_input')
    UPDATED_INPUT=$(echo "$ORIGINAL_INPUT" | jq --arg cmd "$NEW_CMD" '.command = $cmd')

    jq -n \
        --argjson updated "$UPDATED_INPUT" \
        '{
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow",
                "updatedInput": $updated
            }
        }'
    exit 0
fi

# ── hard block ──
# Anything with a real `grep` invocation that the simple-case rewrite above
# didn't already redirect (flags it can't stand in for, multi-pattern -e,
# etc.) gets denied outright rather than falling through to real grep —
# abang's call, 2026-08-11: plain grep hangs the seat on big trees, and
# `knowledge grep <same args>` (built same night) is now a genuine 1:1
# live-scan replacement, so there is no case left that NEEDS real grep.
# Word-boundary match, deliberately excludes `knowledge grep` itself (the
# escape hatch) and lets egrep/fgrep/zgrep/pgrep through untouched — those
# are distinct tools, not literally `grep`.
if [[ "$CMD" =~ (^|[^a-zA-Z0-9_])grep([^a-zA-Z0-9_]|$) ]] && \
   [[ ! "$CMD" =~ knowledge[[:space:]]+grep ]]; then

    # Per-session attempt counter — abang's ask 2026-08-11: repeated grep
    # attempts in one session are a signal the agent is still reaching for
    # full-scan-shaped thinking instead of cascading via codegraph/serena/
    # knowledge --breadcrumb, and a single flat deny message doesn't convey
    # "this keeps happening." State file keyed by session_id (top-level
    # field on every hook payload, confirmed via ctx_tool_event.sh) so counts
    # don't bleed across sessions/seats. /tmp is fine — a fresh session should
    # start at zero, not carry yesterday's count.
    SESSION_ID=$(echo "$INPUT" | jq -r '.session_id // "unknown"')
    COUNT_FILE="/tmp/grep-block-count-${SESSION_ID}"
    COUNT=$(( $(cat "$COUNT_FILE" 2>/dev/null || echo 0) + 1 ))
    echo "$COUNT" > "$COUNT_FILE"

    REASON="grep is blocked house-wide — use \`knowledge grep <same args>\` instead (live 1:1 replacement, forwards to rg). For a simple single-pattern search, \`knowledge <pattern>\` is faster (indexed)."
    if (( COUNT >= 3 )); then
        REASON="🚩🚩🚩 STOP — this is grep attempt #${COUNT} this session. You are not missing a flag, you are reaching for full-scan-shaped thinking. Before trying again: what's the actual anchor (a symbol, a known file, a call site)? Use codegraph explore / serena / knowledge --breadcrumb|--warp to walk there via real structure. \`knowledge grep\` remains the escape hatch, but hitting it repeatedly means the search itself is the wrong move, not the tool. 🚩🚩🚩"
    elif (( COUNT == 2 )); then
        REASON="grep attempt #2 this session — before a third: is there a symbol/file anchor to cascade from (codegraph/serena/knowledge --breadcrumb) instead of scanning blind? Otherwise \`knowledge grep <same args>\` (live 1:1, forwards to rg) or \`knowledge <pattern>\` (indexed, single term)."
    fi

    jq -n --arg reason "$REASON" '{
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": $reason
        }
    }'
    exit 0
fi

exit 0

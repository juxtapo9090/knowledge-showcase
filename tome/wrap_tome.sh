#!/bin/bash
set -euo pipefail

SOCKET_PATH="/home/juxtapo/Server_files/tome/runtime/tome.sock"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCK_FILE="/tmp/wrap_tome.lock"
PID_FILE="/tmp/wrap_tome.pid"
TOME_BIN="$SCRIPT_DIR/target/release/tome"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo -e "\033[0;90m$(date +'%H:%M:%S') | Tome wrapper already active — skipping duplicate start.\033[0m"
    exit 0
fi

echo "$$" >"$PID_FILE"
cleanup() {
    rm -f "$PID_FILE"
}
trap cleanup EXIT

echo -e "\033[0;32mTome 📚 — Starting up...\033[0m"
while true; do
    SQUATTER=$(
        pgrep -af "$TOME_BIN daemon" \
        | awk -v self="$$" '$1 != self {print $1; exit}' \
        || true
    )
    if [ -n "$SQUATTER" ]; then
        echo -e "\033[0;33m$(date +'%H:%M:%S') | Tome daemon already active (PID $SQUATTER) — evicting...\033[0m"
        kill -9 "$SQUATTER" 2>/dev/null || true
        sleep 0.5
    fi

    if [ -S "$SOCKET_PATH" ]; then
        rm -f "$SOCKET_PATH" || true
    fi

    echo -e "\033[0;36m$(date +'%H:%M:%S') | Starting Tome daemon...\033[0m"
    "$TOME_BIN" daemon &
    CHILD_PID=$!
    wait "$CHILD_PID" || true
    echo -e "\033[0;31m$(date +'%H:%M:%S') | Tome died. Restarting in 3s...\033[0m"
    sleep 3
done

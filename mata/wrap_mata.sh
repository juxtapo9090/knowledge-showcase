#!/bin/bash
# wrap_mata.sh — Mata 👁️ auto-restart wrapper
# Same pattern as wrap_body.sh — kill the port, wrapper boots it back.

PORT=9874
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}👁️  Mata File Indexer — Starting on :$PORT${NC}"

while true; do
    # Evict anything squatting on the port
    SQUATTER=$(ss -lptn "sport = :$PORT" | grep -Po 'pid=\K\d+' | head -n 1)
    if [ -n "$SQUATTER" ]; then
        echo -e "${CYAN}$(date +'%H:%M:%S') | Port $PORT occupied (PID $SQUATTER) — evicting...${NC}"
        kill -9 "$SQUATTER" 2>/dev/null
        sleep 0.5
    fi

    echo -e "${CYAN}$(date +'%H:%M:%S') | Starting mata.py via verified...${NC}"
    verified "$SCRIPT_DIR/mata.py" --config "$SCRIPT_DIR/config_linux.json"

    echo -e "${RED}$(date +'%H:%M:%S') | Mata died. Restarting in 3s...${NC}"
    sleep 3
done

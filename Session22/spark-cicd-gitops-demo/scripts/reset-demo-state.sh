#!/bin/bash
# Clears local pipeline state (.registry/) so the whole demo can be re-run
# from a clean slate (version restarts at 1.0.0, no dev/stage/prod tags, no
# blue/green or promotion history).
set -e
cd "$(dirname "$0")/.."
find .registry -type f ! -name ".gitkeep" -delete
echo "[reset-demo-state] .registry/ cleared. Next 'ci' run starts fresh at version 1.0.0."

#!/bin/bash
# One command to start Part 1: download the data if needed, then convert it and answer
# questions 1 and 2. Run from the repo folder on your laptop (on Windows, in Git Bash):
#   ./scripts/start.sh
set -e

# Always work from the repo folder, wherever the script is started from
cd "$(dirname "$0")/.."

echo "=== Step 1/2: get the data (skipped if it is already downloaded) ==="
./scripts/download_data.sh

echo "=== Step 2/2: run Part 1 in Docker ==="
docker compose run --rm spark bash scripts/run_part1.sh
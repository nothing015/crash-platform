#!/bin/bash
# Part 3: start Postgres, load the Part 1 and Part 2 result files, answer the three questions
# from the database, then check that every table matches its file.
# Run from the repo folder on your laptop (on Windows, in Git Bash), after
# ./scripts/start.sh and ./scripts/run_stream_demo.sh:
#   ./scripts/run_serve.sh
# To look at the tables yourself afterwards:  docker compose exec db psql -U crash crashes
set -e
cd "$(dirname "$0")/.."

COMPOSE="docker compose --progress quiet"

echo "=== 1/4: start the database ==="
$COMPOSE up -d --wait db

echo "=== 2/4: load the result files ==="
$COMPOSE run --rm -T serve python serve/load.py

echo "=== 3/4: answer the three questions from the database ==="
$COMPOSE run --rm -T serve python serve/query.py

echo "=== 4/4: check the database ==="
$COMPOSE run --rm -T serve python serve/check_serve.py

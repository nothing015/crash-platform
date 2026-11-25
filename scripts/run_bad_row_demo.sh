#!/bin/bash
# Part 4 demo: add a bad row by hand and watch the quality checks catch it.
# ./scripts/run_all.sh runs this step after Part 1. To run it on its own (after ./scripts/start.sh):
#   1. type one or more rows under the header in data/demo/hand_rows.csv, and save it
#      (the file is created with the CSV header the first time this script or run_all.sh runs)
#   2. ./scripts/run_bad_row_demo.sh     runs the nine quality rules on the full data plus your rows
# The downloaded CSV and the normal results are not changed (the demo writes to results/demo/ and data/demo/).
# With no rows in the file, the check is skipped and the script exits successfully.
set -e

HAND_FILE="data/demo/hand_rows.csv"

if [ "$1" != "--in-container" ]; then
  # On the laptop: make sure there are rows to check, then run the rest inside the spark container
  cd "$(dirname "$0")/.."
  if [ ! -f data/raw/crashes.csv ]; then
    echo "data/raw/crashes.csv not found. Run ./scripts/start.sh first."
    exit 1
  fi
  if [ ! -f "$HAND_FILE" ]; then
    mkdir -p data/demo
    head -1 data/raw/crashes.csv > "$HAND_FILE"
  fi
  if [ "$(grep -c '[^[:space:]]' "$HAND_FILE")" -lt 2 ]; then
    echo "No rows added by hand in $HAND_FILE, so the bad-row check is skipped."
    echo "To try it: type a row under the header in that file, save it, and run this again."
    exit 0
  fi
  echo "=== Rows added by hand ($HAND_FILE) ==="
  tail -n +2 "$HAND_FILE"
  echo
  exec docker compose run --rm spark bash scripts/run_bad_row_demo.sh --in-container
fi

# Inside the spark container
cd /app
LOG4J='file:/app/scripts/log4j2.properties'
/opt/spark/bin/spark-submit --master 'local[*]' --driver-memory 3g --conf spark.sql.session.timeZone=UTC \
  --driver-java-options "-Dlog4j.configurationFile=$LOG4J -Dlog4j2.configurationFile=$LOG4J" \
  jobs/bad_row_demo.py

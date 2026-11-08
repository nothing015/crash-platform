#!/bin/bash
# Part 2 demo: start Kafka, run the consumer in the background, replay rows with the producer,
# then check that the stream result matches a batch recount.
# Run from the repo folder on your laptop (on Windows, in Git Bash):
#   ./scripts/run_stream_demo.sh
# Options (environment variables):  LIMIT=300  SPEEDUP=3600  SOURCE=sample  TOPIC=<name>  PRINT_EVERY=1
#   Example: LIMIT=2000 SPEEDUP=7200 ./scripts/run_stream_demo.sh
#   Every row, as fast as possible (run ./scripts/start.sh first):
#     SOURCE=full LIMIT=0 SPEEDUP=0 PRINT_EVERY=0 ./scripts/run_stream_demo.sh
# Every run uses a new topic, so rows from an earlier run can never be counted again.
set -e
cd "$(dirname "$0")/.."

LIMIT="${LIMIT:-300}"
SPEEDUP="${SPEEDUP:-3600}"
SOURCE="${SOURCE:-sample}"
TOPIC="${TOPIC:-crashes_$(date +%Y%m%d_%H%M%S)}"
PRINT_EVERY="${PRINT_EVERY:-1}"
COMPOSE="docker compose --progress quiet"

# The consumer must outwait the quietest stretch of the replay. The longest gap between two
# crashes in the full data is 239 minutes, so allow six event hours, or 10 seconds if that is longer.
IDLE_EXIT=$(awk -v s="$SPEEDUP" 'BEGIN { wait = (s > 0) ? 21600 / s : 0; print (wait > 10) ? int(wait) + 1 : 10 }')

echo "=== 1/4: start Kafka ==="
$COMPOSE up -d kafka

echo "=== 2/4: start the consumer (it waits for Kafka and for the first message) ==="
$COMPOSE run --rm -T stream python stream/consumer.py --topic "$TOPIC" --idle-exit "$IDLE_EXIT" --print-every "$PRINT_EVERY" &
CONSUMER_PID=$!

echo "=== 3/4: replay $LIMIT rows from the $SOURCE data at ${SPEEDUP}x into topic $TOPIC ==="
$COMPOSE run --rm -T stream python stream/producer.py --topic "$TOPIC" --source "$SOURCE" --limit "$LIMIT" --speedup "$SPEEDUP"

echo "=== 4/4: wait for the consumer, then check the results ==="
wait $CONSUMER_PID
$COMPOSE run --rm -T stream python stream/check_stream.py

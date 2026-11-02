#!/bin/bash
# Part 2 demo: start Kafka, run the consumer in the background, replay rows with the producer,
# then check that the stream result matches a batch recount.
# Run from the repo folder on your laptop (on Windows, in Git Bash):
#   ./scripts/run_stream_demo.sh
# Options (environment variables):  LIMIT=300  SPEEDUP=3600  SOURCE=sample  TOPIC=<name>
#   Example: LIMIT=2000 SPEEDUP=7200 ./scripts/run_stream_demo.sh
# Every run uses a new topic, so rows from an earlier run can never be counted again.
set -e
cd "$(dirname "$0")/.."

LIMIT="${LIMIT:-300}"
SPEEDUP="${SPEEDUP:-3600}"
SOURCE="${SOURCE:-sample}"
TOPIC="${TOPIC:-crashes_$(date +%Y%m%d_%H%M%S)}"
COMPOSE="docker compose --progress quiet"

echo "=== 1/4: start Kafka ==="
$COMPOSE up -d kafka

echo "=== 2/4: start the consumer (it waits for Kafka and for the first message) ==="
$COMPOSE run --rm -T stream python stream/consumer.py --topic "$TOPIC" --idle-exit 10 &
CONSUMER_PID=$!

echo "=== 3/4: replay $LIMIT rows from the $SOURCE data at ${SPEEDUP}x into topic $TOPIC ==="
$COMPOSE run --rm -T stream python stream/producer.py --topic "$TOPIC" --source "$SOURCE" --limit "$LIMIT" --speedup "$SPEEDUP"

echo "=== 4/4: wait for the consumer, then check the results ==="
wait $CONSUMER_PID
$COMPOSE run --rm -T stream python stream/check_stream.py
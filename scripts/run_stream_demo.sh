#!/bin/bash
# Part 2 demo: start Kafka, run the consumer in the background, replay rows with the producer,
# then check that the stream result matches a batch recount.
# Run from the repo folder on your laptop (on Windows, in Git Bash):
#   ./scripts/run_stream_demo.sh
# Options (environment variables):  LIMIT=300  SPEEDUP=3600  SOURCE=sample
#   Example: LIMIT=2000 SPEEDUP=7200 ./scripts/run_stream_demo.sh
set -e
cd "$(dirname "$0")/.."

LIMIT="${LIMIT:-300}"
SPEEDUP="${SPEEDUP:-3600}"
SOURCE="${SOURCE:-sample}"

echo "=== 1/4: start Kafka ==="
docker compose up -d kafka

echo "=== 2/4: start the consumer (it waits for Kafka and for the first message) ==="
docker compose run --rm -T stream python stream/consumer.py --idle-exit 10 &
CONSUMER_PID=$!

echo "=== 3/4: replay $LIMIT rows from the $SOURCE data at ${SPEEDUP}x ==="
docker compose run --rm -T stream python stream/producer.py --source "$SOURCE" --limit "$LIMIT" --speedup "$SPEEDUP"

echo "=== 4/4: wait for the consumer, then check the results ==="
wait $CONSUMER_PID
docker compose run --rm -T stream python stream/check_stream.py

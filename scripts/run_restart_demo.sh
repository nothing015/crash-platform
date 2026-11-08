#!/bin/bash
# Part 2 restart test: kill the consumer without warning in the middle of a replay, start it
# again, and check that the counts still match a batch recount exactly.
# Run from the repo folder on your laptop (on Windows, in Git Bash):
#   ./scripts/run_restart_demo.sh
# Options (environment variables):  LIMIT=10000  SPEEDUP=43200  SOURCE=sample
set -e
cd "$(dirname "$0")/.."

LIMIT="${LIMIT:-10000}"
SPEEDUP="${SPEEDUP:-43200}"
SOURCE="${SOURCE:-sample}"
TOPIC="crashes_restart_$(date +%Y%m%d_%H%M%S)"
CONTAINER="crash_consumer_$(date +%H%M%S)"
CHECKPOINT="data/stream_checkpoints/crash-consumer__$TOPIC.json"
COMPOSE="docker compose --progress quiet"
CONSUME="python stream/consumer.py --topic $TOPIC --print-every 0 --checkpoint-every 2"

echo "=== 1/5: start Kafka and the consumer ==="
$COMPOSE up -d kafka
$COMPOSE run --rm -T --name "$CONTAINER" stream $CONSUME --idle-exit 3600 &
FIRST_CONSUMER=$!

echo "=== 2/5: replay $LIMIT rows from the $SOURCE data at ${SPEEDUP}x into topic $TOPIC ==="
$COMPOSE run --rm -T stream python stream/producer.py --topic "$TOPIC" --source "$SOURCE" --limit "$LIMIT" --speedup "$SPEEDUP" &
PRODUCER=$!

echo "=== 3/5: wait for the first checkpoint, then kill the consumer (SIGKILL, no clean-up) ==="
for _ in $(seq 1 180); do
  [ -f "$CHECKPOINT" ] && break
  sleep 1
done
if [ ! -f "$CHECKPOINT" ]; then
  echo "No checkpoint appeared at $CHECKPOINT"
  exit 1
fi
sleep 5        # let it read rows past the checkpoint, so some work is really lost in the crash
docker kill "$CONTAINER" > /dev/null
wait $FIRST_CONSUMER || true
echo "Consumer killed."

echo "=== 4/5: start the consumer again; it should resume from its checkpoint ==="
$COMPOSE run --rm -T stream $CONSUME --idle-exit 10
wait $PRODUCER

echo "=== 5/5: check the results ==="
grep -E 'resumed_from_offset|messages_this_run|messages_consumed' results/stream_consumer_metrics.json
$COMPOSE run --rm -T stream python stream/check_stream.py

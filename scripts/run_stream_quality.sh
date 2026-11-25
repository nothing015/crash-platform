#!/bin/bash
# Inject malformed, late, and duplicate messages and verify consumer behavior.
set -e
cd "$(dirname "$0")/.."

COMPOSE="docker compose --progress quiet"
TOPIC="crash_quality_$(date +%Y%m%d_%H%M%S)"

$COMPOSE up -d kafka
$COMPOSE run --rm -T stream python stream/consumer.py \
  --topic "$TOPIC" --idle-exit 2 --print-every 0 &
CONSUMER_PID=$!
trap 'kill "$CONSUMER_PID" 2>/dev/null || true' EXIT

$COMPOSE run --rm -T stream python stream/inject_bad.py --topic "$TOPIC"
wait "$CONSUMER_PID"
trap - EXIT
$COMPOSE run --rm -T stream python stream/check_stream_quality.py

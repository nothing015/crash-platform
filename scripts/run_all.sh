#!/bin/bash
# Run Parts 1–3 in order; Part 1 includes quality validation and quarantine.
set -e
cd "$(dirname "$0")/.."

mkdir -p results
if [ ! -f results/container_memory.csv ]; then
  echo 'time,stage,container,memory_usage,memory_percent' > results/container_memory.csv
fi

record_container_stats() {
  local stage="$1"
  local timestamp
  timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  docker stats --no-stream --format '{{.Name}},{{.MemUsage}},{{.MemPerc}}' |
    while IFS=, read -r name usage percent; do
      printf '%s,%s,%s,%s,%s\n' "$timestamp" "$stage" "$name" "$usage" "$percent" \
        >> results/container_memory.csv
    done
}

./scripts/start.sh
record_container_stats part1

# Rows typed by hand into data/demo/hand_rows.csv go through the quality rules; skipped if there are none
./scripts/run_bad_row_demo.sh
record_container_stats bad_row_check

./scripts/run_stream_quality.sh
record_container_stats stream_quality

if [ "${FULL_STREAM:-0}" = "1" ]; then
  SOURCE=full LIMIT=0 SPEEDUP=0 PRINT_EVERY=0 ./scripts/run_stream_demo.sh
else
  SOURCE=full LIMIT=300 ./scripts/run_stream_demo.sh
fi
record_container_stats stream

./scripts/run_serve.sh
record_container_stats serve

docker compose --progress quiet run --rm -T serve python serve/check_pipeline.py

echo
echo "Pipeline complete. Quality report: results/quality_report.json"
echo "Stage counts: results/stage_metrics.json"
echo "Cross-part checks: results/pipeline_check_report.json"
echo "Container memory samples: results/container_memory.csv"
echo "Start the monitoring dashboard with: ./scripts/run_dashboard.sh"

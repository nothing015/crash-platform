#!/bin/bash
# Part 1 experiments: Parquet vs ORC, then partition layouts and the shuffle setting.
# Runs inside the spark container: docker compose run --rm spark bash scripts/run_benchmarks.sh
# Optional: add  -e FORMAT=orc  after "run --rm" to run the layouts on ORC instead of Parquet.
# For fair timings, stop Kafka first on your laptop: docker compose stop kafka
set -e
cd /app

# Quiet Spark logs: errors only (settings in scripts/log4j2.properties)
LOG4J='file:/app/scripts/log4j2.properties'
SPARK_SUBMIT=(/opt/spark/bin/spark-submit --master 'local[*]' --driver-memory 3g --conf spark.sql.session.timeZone=UTC --driver-java-options "-Dlog4j.configurationFile=$LOG4J -Dlog4j2.configurationFile=$LOG4J")

if [ ! -f data/raw/crashes.csv ]; then
  echo "data/raw/crashes.csv not found. Run ./scripts/download_data.sh first."
  exit 1
fi

echo "=== 1/3: format comparison (Parquet vs ORC) ==="
"${SPARK_SUBMIT[@]}" jobs/bench_formats.py

echo "=== 2/3: partition layouts and shuffle setting ==="
"${SPARK_SUBMIT[@]}" jobs/bench_layouts.py

echo "=== 3/3: check the outputs ==="
python3 jobs/check_part1.py bench
#!/bin/bash
# Part 1: convert the raw CSV to Parquet and ORC, then answer questions 1 and 2.
# Runs inside the spark container: docker compose run --rm spark bash scripts/run_part1.sh
set -e
cd /app

SPARK_SUBMIT=(/opt/spark/bin/spark-submit --master 'local[*]' --driver-memory 3g --conf spark.sql.session.timeZone=UTC)

if [ ! -f data/raw/crashes.csv ]; then
  echo "data/raw/crashes.csv not found. Run ./scripts/download_data.sh first."
  exit 1
fi

echo "=== 1/3: convert the CSV to Parquet and ORC ==="
"${SPARK_SUBMIT[@]}" jobs/convert.py

echo "=== 2/3: answer questions 1 and 2 from the Parquet files ==="
"${SPARK_SUBMIT[@]}" jobs/queries.py

echo "=== 3/3: check the outputs ==="
python3 jobs/check_part1.py part1
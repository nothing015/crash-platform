#!/bin/bash
# Part 1: convert the raw CSV to Parquet and ORC, then answer questions 1 and 2.
# Runs inside the spark container: docker compose run --rm spark bash scripts/run_part1.sh
set -e
cd /app

# Quiet Spark logs: errors only (settings in scripts/log4j2.properties)
LOG4J='file:/app/scripts/log4j2.properties'
SPARK_SUBMIT=(/opt/spark/bin/spark-submit --master 'local[*]' --driver-memory 3g --conf spark.sql.session.timeZone=UTC --driver-java-options "-Dlog4j.configurationFile=$LOG4J -Dlog4j2.configurationFile=$LOG4J")

if [ ! -f data/raw/crashes.csv ]; then
  echo "data/raw/crashes.csv not found. Run ./scripts/download_data.sh first."
  exit 1
fi

echo "=== 1/4: validate input quality and quarantine flagged rows ==="
"${SPARK_SUBMIT[@]}" jobs/quality.py

echo "=== 2/4: convert the CSV to Parquet and ORC ==="
"${SPARK_SUBMIT[@]}" jobs/convert.py

echo "=== 3/4: answer questions 1 and 2 from the Parquet files ==="
"${SPARK_SUBMIT[@]}" jobs/queries.py

echo "=== 4/4: check the outputs ==="
python3 jobs/check_part1.py part1
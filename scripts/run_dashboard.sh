#!/bin/bash
# Start the local quality and monitoring dashboard at http://localhost:8050.
set -e
cd "$(dirname "$0")/.."

docker compose run --rm -p 8050:8050 serve \
  python serve/monitor.py --host 0.0.0.0

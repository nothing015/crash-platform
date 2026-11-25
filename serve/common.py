"""Shared settings for the Part 3 loader, query script and check script."""
import os
import time

import psycopg

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://crash:crash@db:5432/crashes')
RESULTS_DIR = os.environ.get('RESULTS_DIR', '/app/results')
SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schema.sql')

# Each table, the result file it is loaded from, the header that file must have,
# and the table columns those CSV columns go into (in the same order).
TABLES = [
    ('q1_top_streets', 'q1_top_streets.csv',
     ['on_street_name', 'count'], ['street', 'crashes']),
    ('q2_injuries_by_hour', 'q2_injuries_by_hour.csv',
     ['crash_hour', 'total_injured'], ['hour_of_day', 'injured']),
    ('q3_crashes_per_borough_hour', 'q3_crashes_per_borough_hour.csv',
     ['hour', 'borough', 'crashes'], ['hour', 'borough', 'crashes']),
    ('q3_latest_hour', 'q3_latest_hour.csv',
     ['hour', 'borough', 'crashes'], ['hour', 'borough', 'crashes']),
]

BOROUGHS = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND', 'UNKNOWN']


def connect(timeout=60):
    """Connect to Postgres, waiting for it if it is still starting."""
    deadline = time.time() + timeout
    while True:
        try:
            return psycopg.connect(DATABASE_URL)
        except psycopg.OperationalError:
            if time.time() > deadline:
                raise SystemExit(f'FAIL: cannot reach the database at {DATABASE_URL.split("@")[-1]}. '
                                 'Start it with: docker compose up -d db')
            time.sleep(1)


def container_memory_mb():
    for path in ('/sys/fs/cgroup/memory.peak', '/sys/fs/cgroup/memory.current'):
        try:
            with open(path) as f:
                return round(int(f.read().strip()) / (1024 * 1024), 1)
        except (OSError, ValueError):
            continue
    return None


def container_memory_limit_mb():
    for path in ('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory/memory.limit_in_bytes'):
        try:
            with open(path) as f:
                value = f.read().strip()
            if value != 'max':
                return round(int(value) / (1024 * 1024), 1)
        except (OSError, ValueError):
            continue
    return None

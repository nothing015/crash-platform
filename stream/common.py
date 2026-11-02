"""Shared settings and helpers for the Part 2 producer, consumer and check script."""
import json
import os
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# Inside Docker the broker is reachable as kafka:19092 (see docker-compose.yml).
BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:19092')
DEFAULT_TOPIC = os.environ.get('KAFKA_TOPIC', 'crashes')
RESULTS_DIR = os.environ.get('RESULTS_DIR', '/app/results')

# Where the rows to replay come from
SOURCES = {
    'sample': '/app/sample_data',               # 100,000-row sample, committed in git
    'full': '/app/data/lake/crashes_parquet',   # every row, created by Part 1 (./scripts/start.sh)
}

# Columns copied from the Parquet files into each Kafka message
COLUMNS = [
    'collision_id', 'crash_ts', 'borough', 'zip_code', 'latitude', 'longitude',
    'on_street_name', 'number_of_persons_injured', 'number_of_persons_killed',
]


def wait_for_kafka(bootstrap=BOOTSTRAP, timeout=120):
    """Block until the broker answers, so scripts can start right after `docker compose up`."""
    admin = AdminClient({'bootstrap.servers': bootstrap})
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            admin.list_topics(timeout=5)
            return admin
        except Exception as exc:  # the broker is probably still starting
            last_error = exc
            print('Waiting for Kafka...', flush=True)
            time.sleep(2)
    raise SystemExit(f'Kafka is not reachable at {bootstrap}: {last_error}')


def ensure_topic(admin, topic, partitions=1):
    """Create the topic if it does not exist. One partition keeps all rows in order."""
    if topic in admin.list_topics(timeout=10).topics:
        return False
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    for future in futures.values():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    return True


def load_table(source, limit=0):
    """Read the rows to replay, drop rows without an event time, sort by event time.

    Returns (table, rows_read, rows_dropped_null_ts).
    """
    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    path = SOURCES[source]
    if not os.path.isdir(path):
        hint = 'Run git pull.' if source == 'sample' else 'Run ./scripts/start.sh first (Part 1).'
        raise SystemExit(f'{path} not found. {hint}')
    table = ds.dataset(path, format='parquet').to_table(columns=COLUMNS)
    rows_read = table.num_rows
    table = table.filter(pc.is_valid(table['crash_ts']))
    dropped = rows_read - table.num_rows
    table = table.sort_by([('crash_ts', 'ascending'), ('collision_id', 'ascending')])
    if limit:
        table = table.slice(0, limit)
    return table, rows_read, dropped


def hour_key(crash_ts_text):
    """'2012-07-01T14:23:00' -> '2012-07-01T14'"""
    return crash_ts_text[:13]


def borough_of(value):
    """Missing boroughs (about 30% of rows) are counted as UNKNOWN."""
    return (value or '').strip().upper() or 'UNKNOWN'


def write_json(path, data):
    """Write atomically, so other programs never read a half-written file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)

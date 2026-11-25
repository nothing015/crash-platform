"""Shared settings and helpers for the Part 2 producer, consumer and check script."""
import csv
import json
import os
import resource
import time
from datetime import datetime

from confluent_kafka import KafkaError, KafkaException, TopicCollection
from confluent_kafka.admin import AdminClient, NewTopic

# Inside Docker the broker is reachable as kafka:19092 (see docker-compose.yml).
BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:19092')
DEFAULT_TOPIC = os.environ.get('KAFKA_TOPIC', 'crashes')
RESULTS_DIR = os.environ.get('RESULTS_DIR', '/app/results')
CHECKPOINT_DIR = os.environ.get('CHECKPOINT_DIR', '/app/data/stream_checkpoints')
# librdkafka prints a line for every failed connection attempt while Kafka is still starting.
# Level 2 hides those; real problems are still reported by our own error messages.
# For the full client logs when debugging: docker compose run --rm -e KAFKA_LOG_LEVEL=6 stream ...
LOG_LEVEL = int(os.environ.get('KAFKA_LOG_LEVEL', '2'))

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


def client_config(**extra):
    """Settings shared by the producer, consumer and admin client."""
    return {'bootstrap.servers': BOOTSTRAP, 'log_level': LOG_LEVEL, **extra}


def wait_for_kafka(timeout=120):
    """Block until the broker answers, so scripts can start right after `docker compose up`."""
    admin = AdminClient(client_config())
    deadline = time.time() + timeout
    last_error = None
    announced = False
    while time.time() < deadline:
        try:
            admin.list_topics(timeout=5)
            return admin
        except Exception as exc:  # the broker is probably still starting
            last_error = exc
            if not announced:
                print('Waiting for Kafka to start...', flush=True)
                announced = True
            time.sleep(2)
    raise SystemExit(f'Kafka is not reachable at {BOOTSTRAP}: {last_error}')


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


def topic_identity(admin, topic, attempts=10):
    """Kafka's unique ID for this topic. A deleted and recreated topic gets a new one."""
    for attempt in range(attempts):
        try:
            future = admin.describe_topics(TopicCollection([topic]))[topic]
            return str(future.result(timeout=10).topic_id)
        except KafkaException:             # a brand-new topic can take a moment to appear
            if attempt == attempts - 1:
                raise
            time.sleep(0.5)


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
    """'2012-07-01T14:23:00' -> '2012-07-01T14'. Anything else raises ValueError or TypeError."""
    if len(crash_ts_text) != 19 or crash_ts_text[10] != 'T':
        raise ValueError(f'not a YYYY-MM-DDTHH:MM:SS timestamp: {crash_ts_text!r}')
    datetime.fromisoformat(crash_ts_text)
    return crash_ts_text[:13]


def borough_of(value):
    """Missing boroughs (about 30% of rows) are counted as UNKNOWN."""
    return (value or '').strip().upper() or 'UNKNOWN'


def rows_per_second(rows, seconds):
    return round(rows / seconds, 1) if seconds > 0 else 0


def peak_memory_mb():
    """The most memory this process has used so far (Linux reports it in KB)."""
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def container_memory_mb():
    """Return the container cgroup memory peak when available."""
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


def source_checksum():
    try:
        with open(os.path.join(RESULTS_DIR, 'data_info.txt')) as f:
            return next(
                (line.split(':', 1)[1].strip()
                 for line in f if line.startswith('SHA256 Checksum:')),
                None,
            )
    except OSError:
        return None


def container_memory_stats():
    return {
        'container_memory_peak_mb': container_memory_mb(),
        'container_memory_limit_mb': container_memory_limit_mb(),
    }


def write_json(path, data, indent=2):
    """Write atomically, so other programs never read a half-written file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(data, f, indent=indent)
    os.replace(tmp, path)


def write_csv(path, header, rows):
    """Write atomically, like write_json."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    os.replace(tmp, path)


def append_csv_row(path, header, row):
    """Append one time-series sample, adding the header only for a new file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    needs_header = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, 'a', newline='') as f:
        writer = csv.writer(f)
        if needs_header:
            writer.writerow(header)
        writer.writerow(row)
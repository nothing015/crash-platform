"""Replay crash rows into a Kafka topic in event-time order, sped up.

Examples (inside Docker, from the repo folder):
  docker compose run --rm stream python stream/producer.py --dry-run
  docker compose run --rm stream python stream/producer.py --limit 300
  docker compose run --rm stream python stream/producer.py --source full --speedup 86400
"""
import argparse
import json
import os
import sys
import time

from confluent_kafka import Producer

from common import (DEFAULT_TOPIC, RESULTS_DIR, client_config, ensure_topic, load_table,
                    peak_memory_mb, rows_per_second, wait_for_kafka, write_json)


def producer_config():
    """acks=all: a row counts as sent only once the broker has stored it.
    Idempotence stops a retry from writing the row twice (confluent-kafka leaves it off by default).
    """
    return client_config(**{'acks': 'all', 'enable.idempotence': True})


def build_message(row):
    street = (row['on_street_name'] or '').strip() or None
    return {
        'collision_id': row['collision_id'],
        'crash_ts': row['crash_ts'].strftime('%Y-%m-%dT%H:%M:%S'),
        'borough': row['borough'],
        'zip_code': row['zip_code'],
        'latitude': row['latitude'],
        'longitude': row['longitude'],
        'on_street_name': street,
        'number_of_persons_injured': row['number_of_persons_injured'],
        'number_of_persons_killed': row['number_of_persons_killed'],
    }


def make_callback(stats):
    def callback(err, msg):
        if err is None:
            stats['delivered'] += 1
        else:
            stats['failed'] += 1
            if stats['failed'] <= 5:
                print(f'Delivery failed: {err}', file=sys.stderr, flush=True)
    return callback


def send(producer, topic, message, callback):
    key = str(message['collision_id']).encode() if message['collision_id'] else None
    value = json.dumps(message, separators=(',', ':')).encode()
    while True:
        try:
            producer.produce(topic, value=value, key=key, on_delivery=callback)
            return
        except BufferError:       # the local queue is full: let it drain, then retry
            producer.poll(0.5)


def replay(table, producer, topic, speedup, stats):
    """Send rows in order. With speedup S, S seconds of event time pass per real second."""
    callback = make_callback(stats)
    start = time.monotonic()
    last_print = start
    first_ts = None
    sent = 0
    for batch in table.to_batches(max_chunksize=5000):
        for row in batch.to_pylist():
            ts = row['crash_ts']
            if first_ts is None:
                first_ts = ts
            if speedup > 0:
                due = start + (ts - first_ts).total_seconds() / speedup
                delay = due - time.monotonic()
                if delay > 0.002:
                    time.sleep(delay)
            send(producer, topic, build_message(row), callback)
            sent += 1
            producer.poll(0)
            now = time.monotonic()
            if now - last_print >= 2:
                last_print = now
                print(f"sent {sent:,} rows | event time {ts:%Y-%m-%d %H:%M} | "
                      f"delivered {stats['delivered']:,}", flush=True)
    return sent


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--source', choices=['sample', 'full'], default='sample',
                        help='sample = 100,000-row sample in git (default); full = all rows from Part 1')
    parser.add_argument('--speedup', type=float, default=3600,
                        help='event seconds per real second (default 3600 = one hour per second); 0 = as fast as possible')
    parser.add_argument('--limit', type=int, default=0, help='replay only the first N rows (default: all)')
    parser.add_argument('--topic', default=DEFAULT_TOPIC)
    parser.add_argument('--dry-run', action='store_true', help='print the plan and stop; sends nothing')
    args = parser.parse_args()

    table, rows_read, dropped = load_table(args.source, args.limit)
    n = table.num_rows
    if n == 0:
        raise SystemExit('No rows to replay.')
    first_ts = table.slice(0, 1).to_pylist()[0]['crash_ts']
    last_ts = table.slice(n - 1, 1).to_pylist()[0]['crash_ts']
    span = (last_ts - first_ts).total_seconds()

    print(f'Source: {args.source} | rows read: {rows_read:,} | dropped (no event time): {dropped:,} | to replay: {n:,}')
    print(f'Event time: {first_ts:%Y-%m-%d %H:%M} to {last_ts:%Y-%m-%d %H:%M} ({span / 86400:,.1f} days)')
    if args.speedup > 0:
        seconds = span / args.speedup
        print(f'Speedup {args.speedup:,.0f}x: about {seconds:,.0f} seconds ({seconds / 60:,.1f} minutes), '
              f'about {n / max(seconds, 1):,.1f} rows per second')
    else:
        print('No pacing: sending as fast as possible')
    if args.dry_run:
        return

    admin = wait_for_kafka()
    ensure_topic(admin, args.topic)
    producer = Producer(producer_config())
    stats = {'delivered': 0, 'failed': 0}
    sent = 0
    completed = True
    began = time.monotonic()
    try:
        sent = replay(table, producer, args.topic, args.speedup, stats)
    except KeyboardInterrupt:
        completed = False
        print('\nStopped by user.')
    producer.flush(30)
    elapsed = time.monotonic() - began

    metrics = {
        'source': args.source, 'limit': args.limit, 'topic': args.topic, 'speedup': args.speedup,
        'rows_read': rows_read, 'rows_dropped_null_ts': dropped, 'rows_planned': n,
        'rows_sent': sent, 'rows_delivered': stats['delivered'], 'rows_failed': stats['failed'],
        'completed': completed, 'seconds': round(elapsed, 1),
        'rows_per_second': rows_per_second(stats['delivered'], elapsed),
        'peak_memory_mb': peak_memory_mb(),
    }
    write_json(os.path.join(RESULTS_DIR, 'stream_producer_metrics.json'), metrics)
    print(f"Done: sent {sent:,}, delivered {stats['delivered']:,}, failed {stats['failed']:,} "
          f'in {elapsed:,.1f} s. Metrics: results/stream_producer_metrics.json')
    if stats['failed'] or stats['delivered'] != sent:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
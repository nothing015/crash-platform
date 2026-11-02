"""PASS/FAIL check of the Part 2 outputs. Run after a producer run and a consumer run.

  docker compose run --rm stream python stream/check_stream.py

It also recomputes question 3 from the Parquet files (the batch way) and compares it with
the counts the consumer built from the stream.
"""
import csv
import json
import os
import sys
from collections import Counter

from common import RESULTS_DIR, borough_of, hour_key, load_table


def read_json(name):
    with open(os.path.join(RESULTS_DIR, name)) as f:
        return json.load(f)


def expected_counts(source, limit):
    table, _, _ = load_table(source, limit)
    expected = Counter()
    for batch in table.to_batches(max_chunksize=50000):
        for row in batch.to_pylist():
            hour = hour_key(row['crash_ts'].strftime('%Y-%m-%dT%H:%M:%S')) + ':00:00'
            expected[(hour, borough_of(row['borough']))] += 1
    return expected


def main():
    try:
        producer = read_json('stream_producer_metrics.json')
        consumer = read_json('stream_consumer_metrics.json')
        with open(os.path.join(RESULTS_DIR, 'q3_crashes_per_borough_hour.csv'), newline='') as f:
            rows = list(csv.DictReader(f))
    except OSError as exc:
        raise SystemExit(f'FAIL: missing output ({exc}). Run the producer and the consumer first.')

    streamed = Counter({(r['hour'], r['borough']): int(r['crashes']) for r in rows})
    checks = []
    checks.append(('Producer finished its replay', producer['completed'], f"{producer['rows_sent']:,} rows sent"))
    checks.append(('No rows failed to deliver', producer['rows_failed'] == 0, f"{producer['rows_failed']} failed"))
    checks.append(('Delivered rows equal sent rows', producer['rows_delivered'] == producer['rows_sent'], ''))
    checks.append(('Producer and consumer used the same topic', producer['topic'] == consumer['topic'],
                   f"{producer['topic']} / {consumer['topic']}"))
    consumed, delivered = consumer['messages_consumed'], producer['rows_delivered']
    detail = f'{consumed:,} consumed'
    if consumed > delivered:
        detail += ' (more than were sent: the topic still holds rows from an earlier run; use a new --topic or run docker compose down)'
    checks.append(('Consumer received every delivered row', consumed == delivered, detail))
    checks.append(('No unreadable messages', consumer['bad_messages'] == 0, f"{consumer['bad_messages']} unreadable"))
    checks.append(('Counts in the CSV add up to the messages consumed',
                   sum(streamed.values()) == consumer['messages_consumed'], f'{sum(streamed.values()):,}'))
    expected = expected_counts(producer['source'], producer['limit'])
    checks.append(('Stream counts equal the batch recount from Parquet', streamed == expected,
                   f'{len(expected):,} hour and borough groups'))

    failed = 0
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f'  ({detail})' if detail else ''))
        failed += 0 if ok else 1
    print(f'\n{len(checks) - failed} of {len(checks)} checks passed')
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()
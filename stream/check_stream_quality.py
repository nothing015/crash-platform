"""Verify the stream reports injected malformed, duplicate, and late records."""
import csv
import json
import os
import sys
from collections import Counter

from common import RESULTS_DIR


def main():
    with open(os.path.join(RESULTS_DIR, 'stream_consumer_metrics.json')) as f:
        metrics = json.load(f)
    with open(os.path.join(RESULTS_DIR, 'q3_crashes_per_borough_hour.csv'), newline='') as f:
        rows = list(csv.DictReader(f))
    counts = Counter({
        (row['hour'], row['borough']): int(row['crashes'])
        for row in rows
    })
    expected = Counter({
        ('2012-07-01T00:00:00', 'QUEENS'): 1,
        ('2012-07-01T01:00:00', 'BRONX'): 1,
    })
    checks = [
        ('Exactly four malformed/duplicate messages reported', metrics['bad_messages'] == 4,
         metrics['bad_messages']),
        ('Duplicate collision ID counted once', metrics['duplicate_collision_ids'] == 1,
         metrics['duplicate_collision_ids']),
        ('Late event counted without rewinding latest hour', metrics['late_messages'] == 1,
         metrics['late_messages']),
        ('Q3 contains only the two unique valid crash events', counts == expected, dict(counts)),
    ]
    failed = 0
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name} ({detail})")
        failed += 0 if ok else 1
    print(f'\n{len(checks) - failed} of {len(checks)} stream QA checks passed')
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()

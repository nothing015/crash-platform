"""Verify that the current Part 1, stream, and database runs belong together."""
import csv
import json
import os
import sys
from datetime import datetime

from common import RESULTS_DIR, connect


def read_json(name):
    with open(os.path.join(RESULTS_DIR, name)) as f:
        return json.load(f)


def read_csv(name):
    with open(os.path.join(RESULTS_DIR, name), newline='') as f:
        return list(csv.DictReader(f))


def parse_time(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def source_checksum():
    with open(os.path.join(RESULTS_DIR, 'data_info.txt')) as f:
        return next(line.split(':', 1)[1].strip()
                    for line in f if line.startswith('SHA256 Checksum:'))


def main():
    try:
        quality = read_json('quality_report.json')
        part1 = read_json('metrics_part1.json')
        producer = read_json('stream_producer_metrics.json')
        consumer = read_json('stream_consumer_metrics.json')
        stream_check = read_json('stream_check_results.json')
        serve_check = read_json('serve_check_results.json')
        q2 = read_csv('q2_injuries_by_hour.csv')
        q3 = read_csv('q3_crashes_per_borough_hour.csv')
        current_checksum = source_checksum()
    except (OSError, KeyError, ValueError, StopIteration) as exc:
        raise SystemExit(f'FAIL: missing or invalid pipeline output: {exc}')

    checks = []
    checks.append((
        'Part 1 and both stream stages used the same source download',
        all(value == current_checksum for value in (
            quality.get('source_sha256'),
            part1.get('source_sha256'),
            producer.get('source_sha256'),
            consumer.get('source_sha256'),
        )),
        current_checksum,
    ))

    if producer.get('source') == 'full' and not producer.get('limit'):
        read_matches = producer['rows_read'] == part1['total_rows']
        sent_matches = (
            producer['rows_read'] - producer['rows_dropped_null_ts']
            == producer['rows_sent']
        )
        checks.append((
            'Full stream input equals Part 1 accepted rows',
            read_matches,
            f"{producer['rows_read']:,} read / {part1['total_rows']:,} accepted",
        ))
        checks.append((
            'Full-stream read, dropped, and sent counts reconcile',
            sent_matches,
            f"{producer['rows_read']:,} - {producer['rows_dropped_null_ts']:,} "
            f"= {producer['rows_sent']:,}",
        ))
    else:
        checks.append((
            'Sample stream respects its planned limit',
            producer['rows_sent'] == producer['rows_planned']
            and producer['rows_planned'] <= (
                producer['rows_read'] - producer['rows_dropped_null_ts']
            ),
            f"{producer['rows_sent']:,} sent from {producer['rows_read']:,} read",
        ))

    checks.append((
        'Stream batch-recount check passed',
        stream_check.get('status') == 'pass',
        f"{stream_check.get('passed', 0)}/{stream_check.get('total', 0)} checks",
    ))
    checks.append((
        'Database table checks passed',
        serve_check.get('status') == 'pass',
        f"{serve_check.get('passed', 0)}/{serve_check.get('total', 0)} checks",
    ))

    q2_total = sum(int(row['total_injured']) for row in q2)
    checks.append((
        'Q2 injury total matches validated Part 1 input',
        q2_total == part1['total_persons_injured'],
        f"{q2_total:,} in Q2 / {part1['total_persons_injured']:,} in Part 1",
    ))
    q3_total = sum(int(row['crashes']) for row in q3)
    checks.append((
        'Q3 hourly counts add up to valid consumed messages',
        q3_total == consumer['messages_consumed'],
        f"{q3_total:,} hourly crashes / {consumer['messages_consumed']:,} consumed",
    ))

    with connect() as conn:
        last_load = conn.execute('SELECT max(loaded_at) FROM load_log').fetchone()[0]
    stream_finished = parse_time(producer['finished_at'])
    part1_finished = parse_time(quality['checked_at'])
    load_fresh = (
        last_load is not None
        and last_load > stream_finished
        and last_load > part1_finished
    )
    checks.append((
        'Database was loaded after Part 1 and streaming completed',
        load_fresh,
        last_load.isoformat() if last_load else 'no database load recorded',
    ))

    failed = sum(not ok for _, ok, _ in checks)
    report = {
        'checked_at': datetime.now().astimezone().isoformat(),
        'status': 'pass' if failed == 0 else 'fail',
        'passed': len(checks) - failed,
        'failed': failed,
        'total': len(checks),
        'checks': [
            {'name': name, 'result': 'pass' if ok else 'fail', 'detail': detail}
            for name, ok, detail in checks
        ],
    }
    with open(os.path.join(RESULTS_DIR, 'pipeline_check_report.json'), 'w') as f:
        json.dump(report, f, indent=2)
    for check in report['checks']:
        print(f"{check['result'].upper():4} {check['name']} ({check['detail']})")
    print(f"\n{report['passed']} of {report['total']} cross-part checks passed")
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()

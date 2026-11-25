"""Load the Part 1 and Part 2 result files into Postgres.

  docker compose run --rm serve python serve/load.py

Every table is emptied and refilled inside one transaction, so a reader sees either the old
answers or the new ones, never a half-loaded table, and a failed load leaves the old data in place.
"""
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

from common import (
    RESULTS_DIR,
    SCHEMA_FILE,
    TABLES,
    connect,
    container_memory_limit_mb,
    container_memory_mb,
)
from psycopg.types.json import Jsonb


def read_header(path):
    with open(path, newline='') as f:
        return next(csv.reader(f), [])


def read_json(name, required=False):
    path = os.path.join(RESULTS_DIR, name)
    if not os.path.isfile(path):
        if required:
            raise SystemExit(f'FAIL: missing results/{name}')
        return {}
    with open(path) as f:
        return json.load(f)


def parse_timestamp(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def insert_run(conn, run_id, part, started_at, seconds, rows_in, rows_out,
               peak_mem_mb, status, details):
    conn.execute(
        'INSERT INTO pipeline_runs '
        '(run_id, part, started_at, seconds, rows_in, rows_out, peak_mem_mb, status, details) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) '
        'ON CONFLICT (run_id, part) DO UPDATE SET '
        'started_at=excluded.started_at, seconds=excluded.seconds, rows_in=excluded.rows_in, '
        'rows_out=excluded.rows_out, peak_mem_mb=excluded.peak_mem_mb, '
        'status=excluded.status, details=excluded.details',
        (run_id, part, started_at, seconds, rows_in, rows_out, peak_mem_mb, status,
         Jsonb(details)),
    )


def main():
    missing = [name for _, name, _, _ in TABLES if not os.path.exists(os.path.join(RESULTS_DIR, name))]
    if missing:
        sys.exit(f'FAIL: missing result files: {", ".join(missing)}. '
                 'Run ./scripts/start.sh and ./scripts/run_stream_demo.sh first.')

    for _, name, header, _ in TABLES:
        got = read_header(os.path.join(RESULTS_DIR, name))
        if got != header:
            sys.exit(f'FAIL: results/{name} has columns {got}, expected {header}')

    with connect() as conn:
        with open(SCHEMA_FILE) as f:
            conn.execute(f.read())
        quality = read_json('quality_report.json', required=True)
        part1_metrics = read_json('metrics_part1.json', required=True)
        producer = read_json('stream_producer_metrics.json', required=True)
        consumer = read_json('stream_consumer_metrics.json', required=True)
        stage_metrics = read_json('stage_metrics.json')
        with conn.transaction():
            load_started = time.perf_counter()
            load_started_at = datetime.now(timezone.utc)
            load_stages = []
            for table, name, _, columns in TABLES:
                start = time.perf_counter()
                with conn.cursor() as cur:
                    cur.execute(f'TRUNCATE {table}')
                    copy_sql = f'COPY {table} ({", ".join(columns)}) FROM STDIN WITH (FORMAT csv, HEADER true)'
                    with open(os.path.join(RESULTS_DIR, name), 'rb') as f, cur.copy(copy_sql) as copy:
                        while chunk := f.read(1 << 20):
                            copy.write(chunk)
                    rows = cur.rowcount
                    seconds = time.perf_counter() - start
                    cur.execute('INSERT INTO load_log (table_name, source_file, rows_loaded, seconds) '
                                'VALUES (%s, %s, %s, %s)', (table, f'results/{name}', rows, seconds))
                print(f'Loaded {rows:>9,} rows into {table:<28} in {seconds:.2f} s')
                load_stages.append({
                    'stage': f'serve_load:{table}',
                    'rows_in': rows,
                    'rows_out': rows,
                    'seconds': round(seconds, 2),
                    'container_memory_peak_mb': container_memory_mb(),
                    'container_memory_limit_mb': container_memory_limit_mb(),
                })
            load_seconds = time.perf_counter() - load_started

            run_id = quality['run_id']
            stages = stage_metrics.get('stages', [])
            part1_seconds = sum(float(stage.get('seconds', 0)) for stage in stages)
            part1_status = quality.get('overall_status', 'pass')
            insert_run(
                conn, run_id, 'part1', parse_timestamp(quality.get('started_at')),
                part1_seconds, part1_metrics.get('rows_in'), part1_metrics.get('rows_out'),
                quality.get('container_memory_peak_mb'),
                'warn' if part1_status == 'alert' else part1_status,
                {'source_sha256': quality.get('source_sha256'), 'stages': stages},
            )
            insert_run(
                conn, run_id, 'stream_producer', parse_timestamp(producer.get('started_at')),
                producer.get('seconds'), producer.get('rows_read'), producer.get('rows_delivered'),
                producer.get('container_memory_peak_mb'),
                'fail' if producer.get('rows_failed') else 'pass', producer,
            )
            insert_run(
                conn, run_id, 'stream_consumer', parse_timestamp(consumer.get('started_at')),
                consumer.get('seconds_this_run'), consumer.get('messages_this_run'),
                consumer.get('messages_consumed'), consumer.get('container_memory_peak_mb'),
                'fail' if consumer.get('bad_messages') else 'pass', consumer,
            )
            loaded_at = conn.execute('SELECT max(loaded_at) FROM load_log').fetchone()[0]
            row_totals = sum(conn.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                             for table, *_ in TABLES)
            insert_run(
                conn, run_id, 'serve', load_started_at, load_seconds, row_totals, row_totals,
                container_memory_mb(), 'pass',
                {'container_memory_limit_mb': container_memory_limit_mb(),
                 'tables_loaded': len(TABLES)},
            )
            for rule in quality.get('rules', []):
                conn.execute(
                    'INSERT INTO quality_results '
                    '(run_id, rule, level, value, threshold, result, details) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s) '
                    'ON CONFLICT (run_id, rule) DO UPDATE SET level=excluded.level, '
                    'value=excluded.value, threshold=excluded.threshold, result=excluded.result, '
                    'details=excluded.details',
                    (run_id, rule['id'], rule['level'], rule.get('failed_rows'),
                     rule.get('threshold_rows'), rule['result'], Jsonb(rule)),
                )
            for check in quality.get('completeness_checks', []):
                conn.execute(
                    'INSERT INTO quality_results '
                    '(run_id, rule, level, value, threshold, result, details) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s) '
                    'ON CONFLICT (run_id, rule) DO UPDATE SET level=excluded.level, '
                    'value=excluded.value, threshold=excluded.threshold, result=excluded.result, '
                    'details=excluded.details',
                    (run_id, f"missing_share:{check['column']}", 'warn',
                     check['missing_share'], check['warn_share_range'][1], check['result'],
                     Jsonb(check)),
                )
    print('Load committed.')
    metrics_path = os.path.join(RESULTS_DIR, 'stage_metrics.json')
    if os.path.exists(metrics_path):
        with open(metrics_path) as f:
            metrics = json.load(f)
    else:
        metrics = {'stages': []}
    for table, name, _, _ in TABLES:
        with open(os.path.join(RESULTS_DIR, name), newline='') as f:
            row_count = sum(1 for _ in csv.DictReader(f))
        existing = next(
            (stage for stage in load_stages if stage['stage'] == f'serve_load:{table}'),
            None,
        )
        if existing is None:
            load_stages.append({
                'stage': f'serve_load:{table}',
                'rows_in': row_count,
                'rows_out': row_count,
            })
    metrics.setdefault('stages', []).extend(load_stages)
    metrics['updated_at'] = datetime.now(timezone.utc).isoformat()
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)


if __name__ == '__main__':
    main()

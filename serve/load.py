"""Load the Part 1 and Part 2 result files into Postgres.

  docker compose run --rm serve python serve/load.py

Every table is emptied and refilled inside one transaction, so a reader sees either the old
answers or the new ones, never a half-loaded table, and a failed load leaves the old data in place.
"""
import csv
import os
import sys
import time

from common import RESULTS_DIR, SCHEMA_FILE, TABLES, connect


def read_header(path):
    with open(path, newline='') as f:
        return next(csv.reader(f), [])


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
        with conn.transaction():
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
    print('Load committed.')


if __name__ == '__main__':
    main()

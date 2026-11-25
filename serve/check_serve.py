"""PASS/FAIL check of the Part 3 database. Run after serve/load.py.

  docker compose run --rm serve python serve/check_serve.py

It reads every table back and compares it, row by row, with the result file it was loaded from.
"""
import csv
import json
import os
import sys
from datetime import datetime

from common import BOROUGHS, RESULTS_DIR, TABLES, connect


def csv_rows(name):
    """The rows of a result file, typed the way the database stores them."""
    with open(os.path.join(RESULTS_DIR, name), newline='') as f:
        reader = csv.reader(f)
        next(reader)
        rows = []
        for row in reader:
            typed = []
            for value in row:
                if value.lstrip('-').isdigit():
                    typed.append(int(value))
                elif 'T' in value and value[:4].isdigit():
                    typed.append(datetime.fromisoformat(value))
                else:
                    typed.append(value)
            rows.append(tuple(typed))
        return rows


def main():
    checks = []
    with connect() as conn:
        for table, name, _, columns in TABLES:
            expected = sorted(csv_rows(name))
            got = sorted(conn.execute(f'SELECT {", ".join(columns)} FROM {table}').fetchall())
            checks.append((f'{table} matches results/{name}', got == expected, f'{len(got):,} rows'))

        n = conn.execute('SELECT count(*) FROM q1_top_streets').fetchone()[0]
        checks.append(('Question 1 has 10 streets', n == 10, f'{n} rows'))
        n = conn.execute('SELECT count(DISTINCT hour_of_day) FROM q2_injuries_by_hour').fetchone()[0]
        checks.append(('Question 2 has all 24 hours', n == 24, f'{n} hours'))

        latest = conn.execute('SELECT DISTINCT hour FROM q3_latest_hour').fetchall()
        last_hour = conn.execute('SELECT max(hour) FROM q3_crashes_per_borough_hour').fetchone()[0]
        ok = len(latest) == 1 and latest[0][0] == last_hour
        checks.append(('Latest hour is the last hour in the hourly table', ok,
                       f'{last_hour:%Y-%m-%d %H:%M}' if last_hour else 'no rows'))

        boroughs = [r[0] for r in conn.execute('SELECT borough FROM q3_latest_hour ORDER BY borough')]
        checks.append(('Latest hour lists every borough', boroughs == BOROUGHS, f'{len(boroughs)} boroughs'))

        mismatched = conn.execute(
            'SELECT count(*) FROM q3_latest_hour l '
            'LEFT JOIN q3_crashes_per_borough_hour h ON h.hour = l.hour AND h.borough = l.borough '
            'WHERE l.crashes <> coalesce(h.crashes, 0)').fetchone()[0]
        checks.append(('Latest-hour counts equal the hourly table for that hour', mismatched == 0,
                       f'{mismatched} boroughs differ'))

        logged = conn.execute(
            'SELECT count(*) FROM load_log WHERE loaded_at = (SELECT max(loaded_at) FROM load_log)').fetchone()[0]
        checks.append(('Last load is recorded for all four tables', logged == len(TABLES), f'{logged} tables'))

    failed = 0
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f'  ({detail})' if detail else ''))
        failed += 0 if ok else 1
    summary = {
        'passed': len(checks) - failed,
        'failed': failed,
        'total': len(checks),
        'status': 'pass' if failed == 0 else 'fail',
    }
    with open(os.path.join(RESULTS_DIR, 'serve_check_results.json'), 'w') as f:
        json.dump(summary, f, indent=2)
    print(f'\n{summary["passed"]} of {summary["total"]} checks passed')
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()

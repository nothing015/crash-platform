"""Answer the three questions from the database, with each one's response time.

  docker compose run --rm serve python serve/query.py

The response time is the median of RUNS runs of the query, measured from sending the SQL to
having every row back. The times are also saved to results/serve_response_times.csv.
The same SQL can be run by hand in psql:  docker compose exec db psql -U crash crashes
"""
import csv
import os
import statistics
import time

from common import connect

RUNS = 5
TIMES_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          'results', 'serve_response_times.csv')

QUERIES = [
    ('Q1. Which ten streets have the most crashes?',
     'SELECT rank() OVER (ORDER BY crashes DESC) AS rank, street, crashes '
     'FROM q1_top_streets ORDER BY crashes DESC, street'),
    ('Q2. At what hour of the day are the most people injured?',
     'SELECT hour_of_day, injured FROM q2_injuries_by_hour ORDER BY injured DESC, hour_of_day LIMIT 5'),
    ('Q3. How many crashes happened in each borough in the latest hour?',
     "SELECT to_char(hour, 'YYYY-MM-DD HH24:MI') AS hour, borough, crashes "
     'FROM q3_latest_hour ORDER BY borough'),
]


def print_table(columns, rows):
    widths = [max(len(str(c)), *(len(f'{r[i]:,}' if isinstance(r[i], int) else str(r[i])) for r in rows))
              for i, c in enumerate(columns)]
    print('  ' + '  '.join(str(c).ljust(w) for c, w in zip(columns, widths)))
    for r in rows:
        cells = [f'{v:,}'.rjust(w) if isinstance(v, int) else str(v).ljust(w) for v, w in zip(r, widths)]
        print('  ' + '  '.join(cells))


def main():
    times = []
    with connect() as conn:
        for title, sql in QUERIES:
            seconds = []
            for _ in range(RUNS):
                start = time.perf_counter()
                cur = conn.execute(sql)
                rows = cur.fetchall()
                seconds.append(time.perf_counter() - start)
            median_ms = statistics.median(seconds) * 1000
            times.append((title.split('.')[0], len(rows), round(median_ms, 2)))
            print(f'\n{title}')
            if rows:
                print_table([d.name for d in cur.description], rows)
            else:
                print('  (no rows: run serve/load.py first)')
            print(f'  Response time: {median_ms:.2f} ms (median of {RUNS} runs)')
        with open(TIMES_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['question', 'rows', 'median_ms'])
            writer.writerows(times)
        cur = conn.execute('SELECT max(loaded_at) FROM load_log')
        loaded = cur.fetchone()[0]
        print(f'\nLast loaded: {loaded:%Y-%m-%d %H:%M:%S %Z}' if loaded else '\nNever loaded')


if __name__ == '__main__':
    main()

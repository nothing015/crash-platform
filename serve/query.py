"""Answer the three questions from the database.

  docker compose run --rm serve python serve/query.py

The same SQL can be run by hand in psql:  docker compose exec db psql -U crash crashes
"""
from common import connect

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
    with connect() as conn:
        for title, sql in QUERIES:
            cur = conn.execute(sql)
            rows = cur.fetchall()
            print(f'\n{title}')
            if rows:
                print_table([d.name for d in cur.description], rows)
            else:
                print('  (no rows: run serve/load.py first)')
        cur = conn.execute('SELECT max(loaded_at) FROM load_log')
        loaded = cur.fetchone()[0]
        print(f'\nLast loaded: {loaded:%Y-%m-%d %H:%M:%S %Z}' if loaded else '\nNever loaded')


if __name__ == '__main__':
    main()

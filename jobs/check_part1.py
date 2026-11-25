"""PASS/FAIL check of the Part 1 outputs.

Usage (inside the spark container, from /app):
  python3 jobs/check_part1.py part1   # after convert.py and queries.py
  python3 jobs/check_part1.py bench   # after bench_formats.py and bench_layouts.py
"""
import csv
import glob
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, 'results')
DATA = os.path.join(ROOT, 'data')


def read_json(name):
    with open(os.path.join(RESULTS, name)) as f:
        return json.load(f)


def read_csv(name):
    with open(os.path.join(RESULTS, name), newline='') as f:
        return list(csv.DictReader(f))


def data_files(folder, pattern):
    return [p for p in glob.glob(os.path.join(DATA, folder, '**', pattern), recursive=True)
            if not os.path.basename(p).startswith('.')]


def check_part1():
    checks = []
    metrics = read_json('metrics_part1.json')
    sizes = read_json('format_sizes.json')
    quality = read_json('quality_report.json')
    total = metrics['total_rows']
    checks.append(('At least 1,000,000 validated rows went through Part 1',
                   total >= 1_000_000, f'{total:,} rows'))
    checks.append(('Raw rows equal validated plus quarantined rows',
                   quality['rows_in'] == quality['rows_out'] + quality['rows_quarantined'],
                   f"{quality['rows_in']:,} in, {quality['rows_out']:,} accepted, "
                   f"{quality['rows_quarantined']:,} quarantined"))
    checks.append(('Rows verified in Parquet and ORC match validated row count',
                   sizes['rows_verified'] == total == quality['rows_out'],
                   f"{sizes['rows_verified']:,} verified"))
    checks.append(('At least four quality rules were evaluated',
                   len(quality['rules']) >= 4, f"{len(quality['rules'])} rules"))
    checks.append(('Parquet files exist', len(data_files('lake/crashes_parquet', '*.parquet')) > 0,
                   f"{len(data_files('lake/crashes_parquet', '*.parquet'))} files"))
    checks.append(('ORC files exist', len(data_files('lake/crashes_orc', '*.orc')) > 0,
                   f"{len(data_files('lake/crashes_orc', '*.orc'))} files"))
    checks.append(('Sample for Part 2 exists', len(data_files('lake/sample_parquet', '*.parquet')) > 0, ''))
    checks.append(('Parquet is smaller than the raw CSV', sizes['parquet_bytes'] < sizes['csv_bytes'],
                   f"{sizes['parquet_bytes'] / sizes['csv_bytes']:.0%} of the CSV size"))
    q1 = read_csv('q1_top_streets.csv')
    q2 = read_csv('q2_injuries_by_hour.csv')
    checks.append(('Question 1 result has 10 rows', len(q1) == 10, f'{len(q1)} rows'))
    checks.append(('Question 2 result has 24 hours', len(q2) == 24, f'{len(q2)} rows'))
    return checks


def check_bench():
    checks = []
    fmt = read_csv('format_comparison.csv')
    names = [r['Format'] for r in fmt]
    checks.append(('Format comparison has Parquet and ORC',
                   any('Parquet' in n for n in names) and any('ORC' in n for n in names), ', '.join(names)))
    layouts = read_csv('layout_comparison.csv')
    checks.append(('Layout comparison has 3 layouts', len(layouts) == 3,
                   ', '.join(r['Layout'] for r in layouts)))
    shuffle = read_csv('shuffle_comparison.csv')
    checks.append(('Shuffle comparison has at least 2 runs', len(shuffle) >= 2, f'{len(shuffle)} rows'))
    with open(os.path.join(RESULTS, 'layout_plans.txt')) as f:
        plans = f.read()
    parts = re.split(r'===== Layout (\w+):[^\n]*=====\n', plans)
    sections = dict(zip(parts[1::2], parts[2::2]))
    year_section = sections.get('Year', '')
    pruned = re.search(r'PartitionFilters:[^\n]*crash_year[^\n]*=\s*\d{4}', year_section) is not None
    checks.append(('Year layout plan shows a crash_year partition filter', pruned, ''))
    return checks


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else 'part1'
    runners = {'part1': check_part1, 'bench': check_bench}
    if which not in runners:
        raise SystemExit('Usage: python3 jobs/check_part1.py part1|bench')
    try:
        checks = runners[which]()
    except (OSError, KeyError, ValueError) as exc:
        raise SystemExit(f'FAIL: could not read an expected output ({exc}). Did the earlier steps finish?')
    failed = 0
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f'  ({detail})' if detail else ''))
        failed += 0 if ok else 1
    print(f"\n{len(checks) - failed} of {len(checks)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()
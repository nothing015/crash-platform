"""Part 4 demo: add bad rows by hand and watch the quality checks catch them.

Runs the same nine quality rules as Part 1 (jobs/quality.py) on the full downloaded CSV plus the
rows in data/demo/hand_rows.csv, then compares the counts with the normal run and shows the
hand-added rows sitting in the quarantine table with the rules they failed.

The downloaded CSV and the normal results are not changed: this run writes its report to
results/demo/ and its quarantine table to data/demo/quarantine/.
Start it from the laptop with:  ./scripts/run_bad_row_demo.sh
"""
import json
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import quality

ROOT = quality.ROOT
HAND_FILE = f'{ROOT}/data/demo/hand_rows.csv'
NORMAL_REPORT = f'{ROOT}/results/quality_report.json'
NORMAL_QUARANTINE = quality.QUARANTINE_PATH
DEMO_RESULTS = f'{ROOT}/results/demo'
DEMO_QUARANTINE = f'{ROOT}/data/demo/quarantine'
DEMO_VALIDATED = f'{ROOT}/data/demo/validated'
SHOW_COLUMNS = [
    'collision_id', 'crash_date', 'crash_time', 'borough', 'on_street_name',
    'latitude', 'longitude', 'number_of_persons_injured', 'number_of_persons_killed',
    'failed_rules',
]


def main():
    if not os.path.isfile(NORMAL_REPORT) or not os.path.isdir(NORMAL_QUARANTINE):
        raise SystemExit('FAIL: run ./scripts/start.sh first, so there is a normal run to compare with.')
    with open(NORMAL_REPORT) as f:
        normal = json.load(f)

    print('=' * 70)
    print('Running the nine quality rules on the full CSV plus the rows added by hand')
    print('=' * 70)
    quality.EXTRA_INPUTS = [HAND_FILE]
    quality.RESULTS_DIR = DEMO_RESULTS
    quality.QUARANTINE_PATH = DEMO_QUARANTINE
    quality.VALIDATED_PATH = DEMO_VALIDATED
    quality.main()
    with open(f'{DEMO_RESULTS}/quality_report.json') as f:
        demo = json.load(f)

    # Counts: the normal run next to this run, so the change is easy to see
    rows = [
        ('rows in', normal['rows_in'], demo['rows_in']),
        ('rows accepted', normal['rows_out'], demo['rows_out']),
        ('rows quarantined', normal['rows_quarantined'], demo['rows_quarantined']),
    ]
    normal_rules = {rule['id']: rule for rule in normal['rules']}
    for rule in demo['rules']:
        before = normal_rules.get(rule['id'], {}).get('failed_rows', 0)
        rows.append((f"{rule['id']} ({rule['result'].upper()})", before, rule['failed_rows']))
    print()
    print('=' * 70)
    print(f"{'Check (result with your rows)':<40}{'normal run':>13}{'with your rows':>16}")
    print('-' * 70)
    for name, before, after in rows:
        marker = '   <-- changed' if before != after else ''
        print(f'{name:<40}{before:>13,}{after:>16,}{marker}')
    print('-' * 70)
    print(f"{'overall status':<40}{normal['overall_status'].upper():>13}"
          f"{demo['overall_status'].upper():>16}")
    print('=' * 70)

    # The quarantine table: rows that this run set aside and the normal run did not
    spark = SparkSession.builder.appName('part4_bad_row_demo').getOrCreate()
    normal_ids = spark.read.parquet(NORMAL_QUARANTINE).select('collision_id')
    new_rows = (spark.read.parquet(DEMO_QUARANTINE)
                .join(normal_ids, 'collision_id', 'left_anti')
                .select(*SHOW_COLUMNS)
                .withColumn('failed_rules', F.concat_ws(', ', 'failed_rules')))
    print(f'\nRows set aside in the quarantine table ({DEMO_QUARANTINE[len(ROOT) + 1:]}) '
          'that the normal run accepted:')
    new_rows.show(truncate=False, vertical=True)
    spark.stop()

    # The accepted rows are a copy of the normal validated data; no need to keep them
    shutil.rmtree(DEMO_VALIDATED, ignore_errors=True)
    print(f'Report: {DEMO_RESULTS[len(ROOT) + 1:]}/quality_report.json. '
          'The normal results and the downloaded CSV were not changed.')


if __name__ == '__main__':
    main()

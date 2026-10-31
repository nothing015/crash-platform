"""Step 7 (deep part): compare three partitioning layouts, then one Spark shuffle setting.

Run with spark-submit inside the spark container (see the runbook). Settings come from
environment variables, all optional:

  FORMAT        parquet (default) or orc: the winning format from Step 6
  YEAR          year used for the single-year question (default 2023)
  AQE_OFF       auto (default), always or never: whether to repeat the shuffle test
                with adaptive query execution off
  PAUSE_FOR_UI  set to 1 to wait at the end so you can take Spark UI screenshots
"""
import csv
import io
import os
import statistics
import time
from contextlib import redirect_stdout

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import get_cleaned_crashes

FORMAT = os.environ.get('FORMAT', 'parquet').lower()
YEAR = int(os.environ.get('YEAR', '2023'))
AQE_OFF = os.environ.get('AQE_OFF', 'auto').lower()
PAUSE_FOR_UI = os.environ.get('PAUSE_FOR_UI') == '1'

if FORMAT not in ('parquet', 'orc'):
    raise SystemExit("FORMAT must be 'parquet' or 'orc'")
if AQE_OFF not in ('auto', 'always', 'never'):
    raise SystemExit("AQE_OFF must be 'auto', 'always' or 'never'")

ROOT = f'/app/data/layouts_{FORMAT}'
RESULTS = '/app/results'
BASE_FILES = 8  # same file count as convert.py, used for the unpartitioned layout

LAYOUTS = {
    'None': [],
    'Year': ['crash_year'],
    'YearMonth': ['crash_year', 'crash_month'],
}


# ---------- helpers ----------

def dir_metrics(path):
    """Size in MB and number of data files (hidden, .crc and _SUCCESS files are ignored)."""
    size = 0
    files = 0
    for dirpath, _, names in os.walk(path):
        for name in names:
            if name.startswith(('.', '_')):
                continue
            size += os.path.getsize(os.path.join(dirpath, name))
            files += 1
    return size / (1024 * 1024), files


def time_it(spark, label, fn, warmups=1, runs=3):
    """One warm-up run, then `runs` timed runs. Returns (median seconds, last result).

    The label shows up in the Spark UI job list, which helps with the screenshots.
    """
    spark.sparkContext.setJobDescription(label)
    for _ in range(warmups):
        fn()
    times = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - start)
    return statistics.median(times), result


def has_street():
    street = F.col('on_street_name')
    return street.isNotNull() & (F.trim(street) != '')


def q1_df(df, year=None):
    """Question 1: ten streets with the most crashes (same rule as queries.py)."""
    if year is not None:
        df = df.filter(F.col('crash_year') == year)
    return (df.filter(has_street())
              .groupBy('on_street_name').count()
              .orderBy(F.col('count').desc(), 'on_street_name')
              .limit(10))


def q1(df, year=None):
    return q1_df(df, year).collect()


def q2(df):
    """Question 2: injured people per hour of day (same rule as queries.py)."""
    return (df.filter(F.col('crash_ts').isNotNull())
              .groupBy(F.hour('crash_ts').alias('crash_hour'))
              .agg(F.sum('number_of_persons_injured').alias('total_injured'))
              .orderBy(F.col('total_injured').desc(), 'crash_hour')
              .collect())


def write_layout(df, path, cols):
    writer = df.write.mode('overwrite').option('compression', 'snappy')
    if cols:
        writer = writer.partitionBy(*cols)
    writer.format(FORMAT).save(path)


def plan_text(df):
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        df.explain(mode='formatted')
    return buffer.getvalue()


def filter_lines(text):
    return [line.strip() for line in text.splitlines()
            if 'PartitionFilters' in line or 'PushedFilters' in line]


def write_csv(path, rows):
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def print_table(title, rows):
    print(f'\n{title}')
    headers = list(rows[0].keys())
    widths = [max(len(h), *(len(str(r[h])) for r in rows)) for h in headers]
    print('  '.join(h.ljust(w) for h, w in zip(headers, widths)))
    for r in rows:
        print('  '.join(str(r[h]).ljust(w) for h, w in zip(headers, widths)))


# ---------- main ----------

def main():
    spark = SparkSession.builder.appName('part1_bench_layouts').getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    os.makedirs(RESULTS, exist_ok=True)

    # Same cleaning and event-time rule as convert.py, so the layouts hold the same data
    base = get_cleaned_crashes(spark).repartition(BASE_FILES).cache()
    total_rows = base.count()
    print(f'Format: {FORMAT} | rows: {total_rows} | single-year question: {YEAR}')

    # 7a. Write the three layouts and time each write
    write_times = {}
    for name, cols in LAYOUTS.items():
        print(f'Writing layout {name} ...')
        data = base.repartition(*cols) if cols else base
        write_times[name], _ = time_it(
            spark, f'write layout {name}',
            lambda: write_layout(data, f'{ROOT}/{name}', cols))
    base.unpersist()  # the queries below must read the files, not the cache

    # 7a. Size, file count and query times for each layout
    dfs = {}
    layout_rows = []
    reference = None
    for name in LAYOUTS:
        path = f'{ROOT}/{name}'
        df = spark.read.format(FORMAT).load(path)
        dfs[name] = df

        rows = df.count()
        if rows != total_rows:
            raise AssertionError(f'{name}: {rows} rows, expected {total_rows}')
        size_mb, files = dir_metrics(path)

        t1, r1 = time_it(spark, f'{name}: Q1 all rows', lambda: q1(df))
        t2, r2 = time_it(spark, f'{name}: Q2 all rows', lambda: q2(df))
        t3, r3 = time_it(spark, f'{name}: Q1 year {YEAR}', lambda: q1(df, YEAR))

        # Every layout must give the same answers, or the timings mean nothing
        answers = (r1, r2, r3)
        if reference is None:
            reference = answers
        elif answers != reference:
            raise AssertionError(f'{name}: answers differ from the None layout')

        layout_rows.append({
            'Format': FORMAT,
            'Layout': name,
            'Size_MB': round(size_mb, 2),
            'File_Count': files,
            'Write_Time_s': round(write_times[name], 2),
            'Q1_All_Time_s': round(t1, 2),
            'Q2_All_Time_s': round(t2, 2),
            'Year_Filter': YEAR,
            'Q1_Year_Time_s': round(t3, 2),
        })

    write_csv(f'{RESULTS}/layout_comparison.csv', layout_rows)
    print_table('Layout comparison (median of 3 runs)', layout_rows)

    # 7c. Query plans for the single-year question: did Spark skip partitions?
    plans = []
    print(f'\nQuery plan check for Q1 in {YEAR}:')
    for name, df in dfs.items():
        text = plan_text(q1_df(df, YEAR))
        plans.append(f'===== Layout {name}: Q1 for {YEAR} =====\n{text}')
        print(f'  Layout {name}')
        for line in filter_lines(text):
            print(f'    {line}')
    with open(f'{RESULTS}/layout_plans.txt', 'w') as f:
        f.write('\n\n'.join(plans))

    # 7b. One Spark setting, question 1 only, on the unpartitioned layout
    test_df = dfs['None']
    default_aqe = spark.conf.get('spark.sql.adaptive.enabled')
    default_parts = spark.conf.get('spark.sql.shuffle.partitions')
    shuffle_rows = []

    def run_setting(aqe):
        spark.conf.set('spark.sql.adaptive.enabled', aqe)
        label = 'on' if aqe == 'true' else 'off'
        times = {}
        for parts in (200, 8):
            spark.conf.set('spark.sql.shuffle.partitions', str(parts))
            t, _ = time_it(spark, f'Q1 shuffle={parts} AQE {label}', lambda: q1(test_df))
            times[parts] = t
            shuffle_rows.append({
                'Shuffle_Partitions': parts,
                'AQE': label,
                'Q1_Time_s': round(t, 2),
            })
        return times

    times = run_setting(default_aqe)  # Spark's default adaptive execution first
    gap = abs(times[200] - times[8]) / max(times.values())
    if AQE_OFF == 'always' or (AQE_OFF == 'auto' and gap < 0.10):
        print(f'\nShuffle setting changed Q1 by {gap:.0%} with default AQE; '
              'repeating once with AQE off.')
        run_setting('false')
    spark.conf.set('spark.sql.adaptive.enabled', default_aqe)
    spark.conf.set('spark.sql.shuffle.partitions', default_parts)

    write_csv(f'{RESULTS}/shuffle_comparison.csv', shuffle_rows)
    print_table('Shuffle partitions, Q1 (median of 3 runs)', shuffle_rows)

    print('\nSaved: results/layout_comparison.csv, results/shuffle_comparison.csv, '
          'results/layout_plans.txt')

    if PAUSE_FOR_UI:
        input('\nDone. Open http://localhost:4040 for the Spark UI screenshots '
              '(Jobs and SQL tabs). Press ENTER to stop Spark...')
    spark.stop()


if __name__ == '__main__':
    main()
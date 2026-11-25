import json
import os
from datetime import datetime, timezone
from pyspark.sql import functions as F


RESULTS_DIR = '/app/results'


def container_memory_mb():
    for path in ('/sys/fs/cgroup/memory.peak', '/sys/fs/cgroup/memory.current'):
        try:
            with open(path) as f:
                return round(int(f.read().strip()) / (1024 * 1024), 1)
        except (OSError, ValueError):
            continue
    return None


def container_memory_limit_mb():
    for path in ('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory/memory.limit_in_bytes'):
        try:
            with open(path) as f:
                value = f.read().strip()
            if value != 'max':
                return round(int(value) / (1024 * 1024), 1)
        except (OSError, ValueError):
            continue
    return None


def record_stage_metrics(stages, run_id=None):
    """Append row counts for stages that run after quality validation."""
    path = os.path.join(RESULTS_DIR, 'stage_metrics.json')
    if os.path.exists(path):
        with open(path) as f:
            metrics = json.load(f)
    else:
        metrics = {'run_id': run_id, 'stages': []}

    memory_peak = container_memory_mb()
    memory_limit = container_memory_limit_mb()
    for stage in stages:
        stage.setdefault('container_memory_peak_mb', memory_peak)
        stage.setdefault('container_memory_limit_mb', memory_limit)
    metrics.setdefault('stages', []).extend(stages)
    metrics['updated_at'] = datetime.now(timezone.utc).isoformat()
    with open(path, 'w') as f:
        json.dump(metrics, f, indent=2)


def standardize_street_name(column):
    standardized = F.upper(F.trim(F.col(column)))
    for abbreviation, full_name in (
        (r'\bPKWY\b', 'PARKWAY'),
        (r'\bEXPY\b', 'EXPRESSWAY'),
        (r'\bEXWY\b', 'EXPRESSWAY'),
        (r'\bAVE\b', 'AVENUE'),
    ):
        standardized = F.regexp_replace(standardized, abbreviation, full_name)
    return standardized


def get_cleaned_crashes(spark, source='/app/data/raw/crashes.csv', source_format='csv'):
    # Read raw source columns as strings (no inferSchema).
    if source_format == 'parquet':
        df = spark.read.parquet(source)
    elif source_format == 'csv':
        df = spark.read.csv(source, header=True)
    else:
        raise ValueError(f'Unsupported crash data format: {source_format}')

    # Clean column names to snake_case
    new_columns = [col.lower().replace(" ", "_") for col in df.columns]
    df = df.toDF(*new_columns)

    # Parse event time safely
    df = (df
        .withColumn('crash_ts', F.expr("try_to_timestamp(concat(crash_date, ' ', crash_time), 'MM/dd/yyyy H:mm')"))
        .withColumn('crash_year', F.year('crash_ts'))
        .withColumn('crash_month', F.month('crash_ts'))
    )

    # Cast injured/killed counts safely using try_cast
    count_columns = [
        'number_of_persons_injured', 'number_of_persons_killed',
        'number_of_pedestrians_injured', 'number_of_pedestrians_killed',
        'number_of_cyclist_injured', 'number_of_cyclist_killed',
        'number_of_motorist_injured', 'number_of_motorist_killed'
    ]
    for col in count_columns:
        df = df.withColumn(col, F.expr(f"try_cast({col} AS INT)"))
    for c in ['borough', 'on_street_name', 'cross_street_name', 'off_street_name']:
        df = df.withColumn(c, F.when(F.length(F.trim(F.col(c))) == 0, None).otherwise(F.trim(F.col(c))))
    
    return df

def write_metrics(df, rows_in=None, source_sha256=None):
    total_rows = df.count()
    
    # Calculate nulls dynamically for Part 4
    null_counts = df.select(
        F.sum(F.col('crash_ts').isNull().cast('int')).alias('null_crash_ts'),
        F.sum(F.col('borough').isNull().cast('int')).alias('null_borough'),
        F.sum(F.col('on_street_name').isNull().cast('int')).alias('null_street'),
        F.sum('number_of_persons_injured').alias('total_persons_injured'),
    ).collect()[0]
    
    metrics = {
        "total_rows": total_rows,
        "rows_in": total_rows if rows_in is None else rows_in,
        "rows_out": total_rows,
        "source_sha256": source_sha256,
        "null_crash_ts": null_counts['null_crash_ts'],
        "null_borough": null_counts['null_borough'],
        "null_on_street_name": null_counts['null_street']
    }
    metrics['total_persons_injured'] = int(null_counts['total_persons_injured'] or 0)
    
    with open('/app/results/metrics_part1.json', 'w') as f:
        json.dump(metrics, f, indent=2)
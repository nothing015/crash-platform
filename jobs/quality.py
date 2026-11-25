"""Validate raw crash rows, report failures and retain them in a quarantine dataset."""
import json
import os
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from common import container_memory_limit_mb, container_memory_mb


ROOT = '/app'
RAW_FILE = f'{ROOT}/data/raw/crashes.csv'
RESULTS_DIR = f'{ROOT}/results'
QUARANTINE_PATH = f'{ROOT}/data/quarantine/quality_failures'
VALIDATED_PATH = f'{ROOT}/data/lake/quality_validated'

REQUIRED_COLUMNS = {
    'collision_id', 'crash_date', 'crash_time', 'borough', 'on_street_name',
    'latitude', 'longitude',
    'number_of_persons_injured', 'number_of_persons_killed',
    'number_of_pedestrians_injured', 'number_of_pedestrians_killed',
    'number_of_cyclist_injured', 'number_of_cyclist_killed',
    'number_of_motorist_injured', 'number_of_motorist_killed',
}
COUNT_COLUMNS = [
    'number_of_persons_injured', 'number_of_persons_killed',
    'number_of_pedestrians_injured', 'number_of_pedestrians_killed',
    'number_of_cyclist_injured', 'number_of_cyclist_killed',
    'number_of_motorist_injured', 'number_of_motorist_killed',
]
BOROUGHS = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']
COUNT_BASELINES = {
    'null_borough': 0.293,
    'null_on_street_name': 0.222,
}
MISSING_SHARE_DRIFT = 0.02
RULES = [
    {
        'id': 'required_schema',
        'description': 'All columns used by the pipeline are present',
        'level': 'fail',
        'max_failure_rate': 0.0,
    },
    {
        'id': 'valid_crash_timestamp',
        'description': 'Crash date and time parse successfully',
        'level': 'fail',
        'max_failure_rate': 0.001,
    },
    {
        'id': 'plausible_timestamp_range',
        'description': 'Crash timestamp is between 2012-07-01 and today',
        'level': 'warn',
        'max_failure_rate': 0.01,
    },
    {
        'id': 'nonnegative_integer_counts',
        'description': 'Injury and fatality counts are nonnegative whole numbers',
        'level': 'fail',
        'max_failure_rate': 0.001,
    },
    {
        'id': 'invalid_count_casts',
        'description': 'Non-empty injury/fatality values cast to integers',
        'level': 'warn',
        'max_failure_rate': 0.0,
    },
    {
        'id': 'valid_borough',
        'description': 'Borough is a NYC borough or empty',
        'level': 'fail',
        'max_failure_rate': 0.001,
    },
    {
        'id': 'present_collision_id',
        'description': 'Collision ID is present',
        'level': 'fail',
        'max_failure_rate': 0.0,
    },
    {
        'id': 'unique_collision_id',
        'description': 'Collision IDs are unique',
        'level': 'warn',
        'max_failure_rate': 0.01,
    },
    {
        'id': 'nyc_coordinates',
        'description': 'Coordinates are within NYC bounds, empty, or zero',
        'level': 'warn',
        'max_failure_rate': 0.05,
    },
]


def normalized_columns(columns):
    return [name.lower().replace(' ', '_') for name in columns]


def source_metadata():
    metadata = {}
    with open(f'{ROOT}/results/data_info.txt') as f:
        for line in f:
            if ':' in line:
                key, value = line.split(':', 1)
                metadata[key.strip()] = value.strip()
    date_text = metadata.get('Download Date')
    if not date_text:
        raise ValueError('results/data_info.txt is missing Download Date')
    download_date = datetime.strptime(
        date_text, '%a %b %d %H:%M:%S %Z %Y'
    ).date()
    return download_date, metadata.get('SHA256 Checksum')


def main():
    started = time.perf_counter()
    run_started_at = datetime.now(timezone.utc)
    if not os.path.isfile(RAW_FILE):
        raise SystemExit(f'FAIL: missing input file {RAW_FILE}')

    download_date, source_checksum = source_metadata()
    spark = SparkSession.builder.appName('part1_quality').getOrCreate()
    read_started = time.perf_counter()
    raw = spark.read.option('mode', 'PERMISSIVE').csv(RAW_FILE, header=True)
    raw = raw.toDF(*normalized_columns(raw.columns))
    missing_columns = sorted(REQUIRED_COLUMNS - set(raw.columns))
    total_rows = raw.count()
    read_seconds = round(time.perf_counter() - read_started, 2)

    # Keep every rule evaluable on schema failures so the report still identifies
    # which downstream fields cannot be checked.
    for column in REQUIRED_COLUMNS:
        if column not in raw.columns:
            raw = raw.withColumn(column, F.lit(None).cast('string'))

    crash_ts = F.expr(
        "try_to_timestamp(concat_ws(' ', crash_date, crash_time), 'MM/dd/yyyy H:mm')"
    )
    raw = raw.withColumn('_quality_crash_ts', crash_ts)
    raw = raw.withColumn(
        '_quality_id_count',
        F.count(F.lit(1)).over(Window.partitionBy('collision_id')),
    )

    count_is_invalid = F.lit(False)
    count_cast_invalid = F.lit(False)
    count_cast_columns = {}
    count_missing_columns = {}
    for column in COUNT_COLUMNS:
        value = F.trim(F.col(column))
        numeric_value = F.expr(f'try_cast(`{column}` AS INT)')
        missing_value = value.isNull() | (value == '')
        cast_invalid = (~missing_value) & numeric_value.isNull()
        invalid = (
            (~missing_value)
            & (~value.rlike(r'^[0-9]+$') | numeric_value.isNull() | (numeric_value < 0))
        )
        count_cast_invalid = count_cast_invalid | cast_invalid
        count_is_invalid = count_is_invalid | invalid
        count_cast_columns[column] = cast_invalid
        count_missing_columns[column] = missing_value

    borough = F.upper(F.trim(F.col('borough')))
    borough_invalid = (
        F.col('borough').isNotNull()
        & (F.trim(F.col('borough')) != '')
        & ~borough.isin(BOROUGHS)
    )

    def coordinate_invalid(column, lower, upper):
        value = F.trim(F.col(column))
        number = F.expr(f'try_cast(`{column}` AS DOUBLE)')
        return (
            value.isNotNull()
            & (value != '')
            & (
                number.isNull()
                | ((number != 0) & ((number < lower) | (number > upper)))
            )
        )

    coordinates_invalid = (
        coordinate_invalid('latitude', 40.49, 40.92)
        | coordinate_invalid('longitude', -74.26, -73.70)
    )

    timestamp_invalid = F.col('_quality_crash_ts').isNull()
    timestamp_missing = (
        F.col('crash_date').isNull() | (F.trim(F.col('crash_date')) == '')
        | F.col('crash_time').isNull() | (F.trim(F.col('crash_time')) == '')
    )
    timestamp_out_of_range = (
        F.col('_quality_crash_ts').isNotNull()
        & (
            (F.to_date('_quality_crash_ts') < F.lit('2012-07-01').cast('date'))
            | (F.to_date('_quality_crash_ts') > F.lit(download_date.isoformat()).cast('date'))
        )
    )
    collision_id_missing = (
        F.col('collision_id').isNull() | (F.trim(F.col('collision_id')) == '')
    )
    duplicate_id = (
        ~collision_id_missing & (F.col('_quality_id_count') > 1)
    )

    conditions = {
        'required_schema': F.lit(bool(missing_columns)),
        'valid_crash_timestamp': timestamp_invalid,
        'plausible_timestamp_range': timestamp_out_of_range,
        'nonnegative_integer_counts': count_is_invalid,
        'invalid_count_casts': count_cast_invalid,
        'valid_borough': borough_invalid,
        'present_collision_id': collision_id_missing,
        'unique_collision_id': duplicate_id,
        'nyc_coordinates': coordinates_invalid,
    }

    # A missing required column makes the schema check fail. The other rules are
    # still measured against available input values, rather than fabricating rows.
    classification_started = time.perf_counter()
    classified = raw.withColumn(
        'failed_rules',
        F.filter(
            F.array(*[
                F.when(condition, F.lit(rule_id))
                for rule_id, condition in conditions.items()
            ]),
            lambda item: item.isNotNull(),
        ),
    ).cache()

    aggregate_expressions = [F.count(F.lit(1)).alias('rows_in')]
    aggregate_expressions.append(
        F.sum((F.size('failed_rules') > 0).cast('long')).alias('rows_quarantined')
    )
    for rule in RULES:
        aggregate_expressions.append(
            F.sum(F.array_contains('failed_rules', rule['id']).cast('long'))
            .alias(rule['id'])
        )
    aggregate_expressions.extend([
        F.sum(count_cast_columns[column].cast('long')).alias(f'cast_error__{column}')
        for column in COUNT_COLUMNS
    ])
    aggregate_expressions.extend([
        F.sum(count_missing_columns[column].cast('long')).alias(f'missing__{column}')
        for column in COUNT_COLUMNS
    ])
    aggregate_expressions.extend([
        F.sum(timestamp_missing.cast('long')).alias('missing_timestamp'),
        F.sum(
            (F.col('borough').isNull() | (F.trim(F.col('borough')) == '')).cast('long')
        ).alias('missing_borough'),
        F.sum(
            (F.col('on_street_name').isNull() | (F.trim(F.col('on_street_name')) == ''))
            .cast('long')
        ).alias('missing_on_street_name'),
    ])
    counts = classified.agg(*aggregate_expressions).first().asDict()
    classification_seconds = round(time.perf_counter() - classification_started, 2)
    rows_in = int(counts['rows_in'] or 0)
    rows_quarantined = int(counts['rows_quarantined'] or 0)

    report_rules = []
    for rule in RULES:
        failed_rows = int(counts[rule['id']] or 0)
        threshold_rows = rows_in * rule['max_failure_rate']
        breached = failed_rows > threshold_rows
        if not breached:
            status = 'pass'
        elif rule['level'] == 'fail':
            status = 'fail'
        else:
            status = 'alert'
        report_rules.append({
            **rule,
            'failed_rows': failed_rows,
            'failure_rate': (failed_rows / rows_in) if rows_in else 0,
            'measured_value': (failed_rows / rows_in) if rows_in else 0,
            'threshold_rows': threshold_rows,
            'result': status,
        })

    previous_rows = None
    previous_report_path = f'{RESULTS_DIR}/quality_report.json'
    if os.path.isfile(previous_report_path):
        with open(previous_report_path) as f:
            previous_rows = json.load(f).get('rows_in')
    volume_check = {
        'minimum_rows': 1_000_000,
        'previous_rows': previous_rows,
        'rows_in': rows_in,
        'change_rate': (
            (rows_in - previous_rows) / previous_rows
            if previous_rows else None
        ),
        'minimum_result': 'pass' if rows_in >= 1_000_000 else 'fail',
        'change_result': (
            'not_evaluated' if not previous_rows else
            ('fail' if abs(rows_in - previous_rows) / previous_rows > 0.05 else
             ('alert' if abs(rows_in - previous_rows) / previous_rows > 0.02 else 'pass'))
        ),
    }
    completeness_checks = []
    for column, count_alias, baseline_key in (
        ('borough', 'missing_borough', 'null_borough'),
        ('on_street_name', 'missing_on_street_name', 'null_on_street_name'),
    ):
        missing_rows = int(counts[count_alias] or 0)
        share = missing_rows / rows_in if rows_in else 0
        baseline = COUNT_BASELINES[baseline_key]
        threshold = baseline + MISSING_SHARE_DRIFT
        completeness_checks.append({
            'column': column,
            'missing_rows': missing_rows,
            'missing_share': share,
            'baseline_share': baseline,
            'warn_share_range': [
                baseline - MISSING_SHARE_DRIFT,
                threshold,
            ],
            'result': 'alert' if abs(share - baseline) > MISSING_SHARE_DRIFT else 'pass',
        })

    cast_errors_by_column = {
        column: int(counts[f'cast_error__{column}'] or 0)
        for column in COUNT_COLUMNS
    }
    missing_values_by_column = {
        column: int(counts[f'missing__{column}'] or 0)
        for column in COUNT_COLUMNS
    }
    quarantine_started = time.perf_counter()
    classified.filter(F.size('failed_rules') > 0).write.mode('overwrite').parquet(
        QUARANTINE_PATH
    )
    quarantine_seconds = round(time.perf_counter() - quarantine_started, 2)
    valid_rows = rows_in - rows_quarantined
    validated_started = time.perf_counter()
    (
        classified.filter(F.size('failed_rules') == 0)
        .drop('failed_rules', '_quality_id_count', '_quality_crash_ts')
        .write.mode('overwrite').parquet(VALIDATED_PATH)
    )
    validated_seconds = round(time.perf_counter() - validated_started, 2)

    run_id = str(uuid.uuid4())
    stages = [
        {'stage': 'raw_csv_read', 'rows_in': rows_in, 'rows_out': rows_in,
         'seconds': read_seconds, 'container_memory_peak_mb': container_memory_mb(),
         'container_memory_limit_mb': container_memory_limit_mb()},
        {'stage': 'quality_classification', 'rows_in': rows_in, 'rows_out': valid_rows,
         'seconds': classification_seconds, 'container_memory_peak_mb': container_memory_mb(),
         'container_memory_limit_mb': container_memory_limit_mb()},
        {'stage': 'quarantine_write', 'rows_in': rows_quarantined,
         'rows_out': rows_quarantined, 'seconds': quarantine_seconds,
         'container_memory_peak_mb': container_memory_mb(),
         'container_memory_limit_mb': container_memory_limit_mb()},
        {'stage': 'validated_dataset_write', 'rows_in': valid_rows, 'rows_out': valid_rows,
         'seconds': validated_seconds, 'container_memory_peak_mb': container_memory_mb(),
         'container_memory_limit_mb': container_memory_limit_mb()},
    ]
    duration = round(time.perf_counter() - started, 2)
    report = {
        'run_id': run_id,
        'checked_at': datetime.now(timezone.utc).isoformat(),
        'started_at': run_started_at.isoformat(),
        'source': 'data/raw/crashes.csv',
        'source_sha256': source_checksum,
        'download_date': download_date.isoformat(),
        'rows_in': rows_in,
        'rows_out': valid_rows,
        'rows_quarantined': rows_quarantined,
        'quarantine_path': 'data/quarantine/quality_failures',
        'validated_path': 'data/lake/quality_validated',
        'missing_columns': missing_columns,
        'stages': stages,
        'rules': report_rules,
        'volume_check': volume_check,
        'completeness_checks': completeness_checks,
        'missing_values': {
            'crash_timestamp': int(counts['missing_timestamp'] or 0),
            'borough': int(counts['missing_borough'] or 0),
            'on_street_name': int(counts['missing_on_street_name'] or 0),
            'count_columns': missing_values_by_column,
        },
        'invalid_casts_by_column': cast_errors_by_column,
        'overall_status': 'fail' if any(
            rule['result'] == 'fail' for rule in report_rules
        ) or volume_check['minimum_result'] == 'fail'
        or volume_check['change_result'] == 'fail' else (
            'alert' if any(r['result'] == 'alert' for r in report_rules)
            or volume_check['change_result'] == 'alert'
            or any(item['result'] == 'alert' for item in completeness_checks)
            else 'pass'
        ),
        'seconds': duration,
        'container_memory_peak_mb': container_memory_mb(),
        'container_memory_limit_mb': container_memory_limit_mb(),
    }
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(f'{RESULTS_DIR}/quality_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    with open(f'{RESULTS_DIR}/stage_metrics.json', 'w') as f:
        json.dump({
            'run_id': run_id,
            'updated_at': report['checked_at'],
            'stages': stages,
        }, f, indent=2)

    print(f"Quality rows in: {rows_in:,}; rows out: {valid_rows:,}; "
          f"rows quarantined: {rows_quarantined:,}")
    for rule in report_rules:
        print(f"{rule['result'].upper():5} {rule['id']}: "
              f"{rule['failed_rows']:,} rows failed "
              f"(threshold {rule['threshold_rows']:.1f})")
    print(f"Volume check: minimum={volume_check['minimum_result']}, "
          f"previous-run-change={volume_check['change_result']}")
    for check in completeness_checks:
        print(f"{check['result'].upper():5} {check['column']} missing share: "
              f"{check['missing_share']:.1%} (baseline {check['baseline_share']:.1%}, "
              f"allowed {check['warn_share_range'][0]:.1%}–"
              f"{check['warn_share_range'][1]:.1%})")
    print(f"Quality elapsed: {duration:.2f} seconds")
    if missing_columns:
        print('Missing required columns: ' + ', '.join(missing_columns))
    classified.unpersist()
    spark.stop()
    if report['overall_status'] != 'pass':
        print(f"{report['overall_status'].upper()}: quality threshold exceeded; "
              "see results/quality_report.json and the monitoring dashboard")


if __name__ == '__main__':
    main()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import time

from common import (
    get_cleaned_crashes,
    record_stage_metrics,
    standardize_street_name,
)

spark = SparkSession.builder.appName('part1_queries').getOrCreate()

# 1. Read only the Parquet files for the main query
df_parquet = spark.read.parquet('/app/data/lake/crashes_parquet')

print("=" * 50)
print("QUESTION 1: Which ten streets have the most crashes?")
print("=" * 50)

# Rule for Q1: drop blank and null street names
excluded_condition = F.col('on_street_name').isNull() | (F.trim(F.col('on_street_name')) == '')
excluded_count = df_parquet.filter(excluded_condition).count()
print(f"Rule applied: Dropped {excluded_count} rows where on_street_name is null or blank.\n")

query_started = time.perf_counter()
q1_df = (df_parquet.filter(~excluded_condition)
    .groupBy('on_street_name')
    .count()
    .orderBy(F.col('count').desc())
    .limit(10))

q1_df.show(truncate=False)

print("=" * 50)
print("QUESTION 2: At what hour of the day are the most people injured?")
print("=" * 50)

q2_df = (df_parquet.filter(F.col('crash_ts').isNotNull())
    .groupBy(F.hour('crash_ts').alias('crash_hour'))
    .agg(F.sum('number_of_persons_injured').alias('total_injured'))
    .orderBy(F.col('total_injured').desc()))

q2_df.show(24)

# 2. Write CSV outputs for Part 3 (using pandas to output clean, single files)
q1_df.toPandas().to_csv('/app/results/q1_top_streets.csv', index=False)
q2_df.toPandas().to_csv('/app/results/q2_injuries_by_hour.csv', index=False)
print("Results saved to results/q1_top_streets.csv and results/q2_injuries_by_hour.csv")

# 3. Cross-check against the quality-validated input
print("\nRunning cross-check with quality-validated input...")
df_csv = get_cleaned_crashes(
    spark, source='/app/data/lake/quality_validated', source_format='parquet'
)

q1_csv_df = (df_csv.filter(~excluded_condition)
    .groupBy('on_street_name')
    .count()
    .orderBy(F.col('count').desc())
    .limit(10))

q2_csv_df = (df_csv.filter(F.col('crash_ts').isNotNull())
    .groupBy(F.hour('crash_ts').alias('crash_hour'))
    .agg(F.sum('number_of_persons_injured').alias('total_injured'))
    .orderBy(F.col('total_injured').desc()))

q1_match = q1_df.collect() == q1_csv_df.collect()
q2_match = q2_df.collect() == q2_csv_df.collect()

print(f"Q1 Parquet matches validated input: {q1_match}")
print(f"Q2 Parquet matches validated input: {q2_match}")

assert q1_match and q2_match, "Cross-check failed! Parquet and validated-input answers do not match."

parquet_rows = df_parquet.count()
query_seconds = round(time.perf_counter() - query_started, 2)
record_stage_metrics([
    {'stage': 'q1_top_streets', 'rows_in': parquet_rows, 'rows_out': q1_df.count(),
     'seconds': query_seconds},
    {'stage': 'q2_injuries_by_hour', 'rows_in': parquet_rows, 'rows_out': q2_df.count(),
     'seconds': query_seconds},
])

standardization_started = time.perf_counter()
q1_standardized = (df_parquet.filter(~excluded_condition)
    .withColumn('standardized_street', standardize_street_name('on_street_name'))
    .groupBy('standardized_street')
    .count()
    .orderBy(F.col('count').desc(), F.col('standardized_street'))
    .limit(10))
raw_top = [
    {'street': row['on_street_name'], 'crashes': row['count']}
    for row in q1_df.collect()
]
standardized_top = [
    {'street': row['standardized_street'], 'crashes': row['count']}
    for row in q1_standardized.collect()
]
standardization_seconds = round(time.perf_counter() - standardization_started, 2)
comparison = {
    'normalizations': {
        'PKWY': 'PARKWAY',
        'EXPY': 'EXPRESSWAY',
        'EXWY': 'EXPRESSWAY',
        'AVE': 'AVENUE',
    },
    'raw_top_10': raw_top,
    'standardized_top_10': standardized_top,
    'ranking_changed': [row['street'] for row in raw_top]
    != [row['street'] for row in standardized_top],
    'canonical_answer_changed': False,
}
report_path = '/app/results/quality_report.json'
with open(report_path) as f:
    report = json.load(f)
report['street_name_comparison'] = comparison
with open(report_path, 'w') as f:
    json.dump(report, f, indent=2)
record_stage_metrics([
    {
        'stage': 'q1_street_normalization_comparison',
        'rows_in': parquet_rows,
        'rows_out': len(standardized_top),
        'seconds': standardization_seconds,
    }
])
print(f"Street normalization changes the top-ten ranking: {comparison['ranking_changed']}")
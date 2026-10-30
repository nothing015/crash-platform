from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from common import get_cleaned_crashes

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

# 3. Cross-check against the raw CSV
print("\nRunning cross-check with raw CSV...")
df_csv = get_cleaned_crashes(spark)

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

print(f"Q1 Parquet matches CSV: {q1_match}")
print(f"Q2 Parquet matches CSV: {q2_match}")

assert q1_match and q2_match, "Cross-check failed! Parquet and CSV answers do not match."
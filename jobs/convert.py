import time
import json
import subprocess
from pyspark.sql import SparkSession
from common import get_cleaned_crashes, record_stage_metrics, write_metrics

spark = SparkSession.builder.appName('part1_convert').getOrCreate()

# 1. Prepare Data & Write Metrics (Step 3)
df = get_cleaned_crashes(
    spark, source='/app/data/lake/quality_validated', source_format='parquet'
)
with open('/app/results/quality_report.json') as f:
    quality_report = json.load(f)
write_metrics(
    df,
    rows_in=quality_report['rows_in'],
    source_sha256=quality_report.get('source_sha256'),
)

# 2. Convert Data to Parquet and ORC (Step 4)
# Use a static partition count (e.g., 8) for a fair format comparison
df_repartitioned = df.repartition(8)

print("Writing Parquet...")
start_time = time.perf_counter()
(df_repartitioned.write
    .mode('overwrite')
    .option('compression', 'snappy')
    .parquet('/app/data/lake/crashes_parquet'))
parquet_time = time.perf_counter() - start_time
print(f"Parquet write took {parquet_time:.2f} seconds")

print("Writing ORC...")
start_time = time.perf_counter()
(df_repartitioned.write
    .mode('overwrite')
    .option('compression', 'snappy')
    .orc('/app/data/lake/crashes_orc'))
orc_time = time.perf_counter() - start_time
print(f"ORC write took {orc_time:.2f} seconds")

# 3. Write 100K Sample for Ali (Part 2)
print("Writing 100K sorted sample for Part 2...")
sample_df = (df.orderBy('crash_ts')
    .limit(100000))
sample_count = sample_df.count()
(sample_df.write
    .mode('overwrite')
    .option('compression', 'snappy')
    .parquet('/app/data/lake/sample_parquet'))

# 4. Verify Row Counts 
print("Verifying row counts...")
csv_count = quality_report['rows_in']
validated_count = quality_report['rows_out']
parquet_count = spark.read.parquet('/app/data/lake/crashes_parquet').count()
orc_count = spark.read.orc('/app/data/lake/crashes_orc').count()

print("=" * 40)
print(f"Raw CSV Row Count: {csv_count}")
print(f"Validated Rows:    {validated_count}")
print(f"Parquet Row Count: {parquet_count}")
print(f"ORC Row Count:     {orc_count}")
print("=" * 40)

assert validated_count == parquet_count == orc_count, "Validated row counts do not match!"

record_stage_metrics([
    {'stage': 'parquet_write', 'rows_in': validated_count, 'rows_out': parquet_count,
     'seconds': round(parquet_time, 2)},
    {'stage': 'orc_write', 'rows_in': validated_count, 'rows_out': orc_count,
     'seconds': round(orc_time, 2)},
    {'stage': 'part2_sample_write', 'rows_in': validated_count, 'rows_out': sample_count},
])

# 5. Measure Directory Sizes
def get_dir_size(path):
    try:
        result = subprocess.run(['du', '-sb', path], capture_output=True, text=True, check=True)
        return int(result.stdout.split()[0])
    except Exception as e:
        print(f"Error getting size for {path}: {e}")
        return 0

sizes = {
    "csv_bytes": get_dir_size('/app/data/raw/crashes.csv'),
    "parquet_bytes": get_dir_size('/app/data/lake/crashes_parquet'),
    "orc_bytes": get_dir_size('/app/data/lake/crashes_orc'),
    "rows_verified": parquet_count
}

with open('/app/results/format_sizes.json', 'w') as f:
    json.dump(sizes, f, indent=2)

mb = 1024 * 1024
print("=" * 40)
print("File sizes, before (CSV) and after (Parquet, ORC):")
print(f"  {'Format':<8}{'Size (MB)':>12}{'vs CSV':>10}")
for name, key in [("CSV", "csv_bytes"), ("Parquet", "parquet_bytes"), ("ORC", "orc_bytes")]:
    ratio = sizes[key] / sizes["csv_bytes"] if sizes["csv_bytes"] else 0
    print(f"  {name:<8}{sizes[key] / mb:>12,.1f}{ratio:>9.0%}")
print("=" * 40)
print("Format sizes saved to /app/results/format_sizes.json")
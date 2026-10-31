import time
import statistics
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_directory_size_mb(path):
    """Recursively calculates the size of a directory (or file) in Megabytes."""
    total_size = 0
    if os.path.isfile(path):
        total_size = os.path.getsize(path)
    else:
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    return total_size / (1024 * 1024)

def measure_runtime(action_func, warmups=1, runs=3):
    """Executes a function for warm-ups, then times it for a specified number of runs."""
    for _ in range(warmups):
        action_func()
    
    times = []
    for _ in range(runs):
        start_time = time.time()
        action_func()
        times.append(time.time() - start_time)
        
    return statistics.median(times)

def main():
    spark = SparkSession.builder \
    .appName("BenchFormats") \
    .master("local[*]") \
    .getOrCreate()

    # Paths
    raw_csv_path = "/app/data/raw/crashes.csv"
    parquet_path = "/app/data/bench_parquet"
    orc_path = "/app/data/bench_orc"
    results_path = "/app/results/format_comparison.csv"

    # Measure CSV Baseline Size
    csv_size_mb = get_directory_size_mb(raw_csv_path)

    # 1. Prepare Base DataFrame for Conversion
    # Read the CSV with multiline safety, split across 8 cores, and cache it in memory
    raw_df = spark.read.csv(
        raw_csv_path, 
        header=True, 
        quote='"', 
        escape='"', 
        multiLine=True
    ).repartition(8).cache()

    # Force Spark to load the file into memory right now
    raw_df.count()
    converted_df = raw_df.withColumn(
        "CRASH_TIMESTAMP",
        F.to_timestamp(F.concat_ws(" ", F.col("CRASH DATE"), F.col("CRASH TIME")), "MM/dd/yyyy H:mm")
    )

    print("Benchmarking Parquet conversion...")
    parquet_conv_time = measure_runtime(lambda: converted_df.write.mode("overwrite").option("compression", "snappy").parquet(parquet_path))
    parquet_size_mb = get_directory_size_mb(parquet_path)

    print("Benchmarking ORC conversion...")
    orc_conv_time = measure_runtime(lambda: converted_df.write.mode("overwrite").option("compression", "snappy").orc(orc_path))
    orc_size_mb = get_directory_size_mb(orc_path)

    # 2. Benchmarking Queries
    # Define query logic wrapped in .collect() to force evaluation
    def run_q1(df):
        return df.groupBy("ON STREET NAME").count().orderBy(F.col("count").desc()).limit(10).collect()

    def run_q2(df):
        return df.filter(F.col("NUMBER OF PERSONS INJURED") > 0) \
                 .withColumn("hour", F.hour("CRASH_TIMESTAMP")) \
                 .groupBy("hour").count().orderBy("hour").collect()

    print("Loading formatted DataFrames...")
    pq_df = spark.read.parquet(parquet_path)
    orc_df = spark.read.orc(orc_path)

    print("Benchmarking Q1 and Q2 for Parquet...")
    pq_q1_time = measure_runtime(lambda: run_q1(pq_df))
    pq_q2_time = measure_runtime(lambda: run_q2(pq_df))

    print("Benchmarking Q1 and Q2 for ORC...")
    orc_q1_time = measure_runtime(lambda: run_q1(orc_df))
    orc_q2_time = measure_runtime(lambda: run_q2(orc_df))

    # 3. Compile and Export Results
    results = [
        {
            "Format": "CSV (Baseline)", 
            "Size_MB": round(csv_size_mb, 2), 
            "Conversion_Time_s": None, 
            "Q1_Time_s": None, 
            "Q2_Time_s": None
        },
        {
            "Format": "Parquet", 
            "Size_MB": round(parquet_size_mb, 2), 
            "Conversion_Time_s": round(parquet_conv_time, 2), 
            "Q1_Time_s": round(pq_q1_time, 2), 
            "Q2_Time_s": round(pq_q2_time, 2)
        },
        {
            "Format": "ORC", 
            "Size_MB": round(orc_size_mb, 2), 
            "Conversion_Time_s": round(orc_conv_time, 2), 
            "Q1_Time_s": round(orc_q1_time, 2), 
            "Q2_Time_s": round(orc_q2_time, 2)
        }
    ]

    results_df = pd.DataFrame(results)
    results_df.to_csv(results_path, index=False)
    
    print(f"\nBenchmarking complete. Results saved to {results_path}")
    print(results_df.to_string(index=False))

    spark.stop()

if __name__ == "__main__":
    main()
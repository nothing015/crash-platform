import json
from pyspark.sql import functions as F

def get_cleaned_crashes(spark):
    # Read CSV with header and all columns as strings (no inferSchema)
    df = spark.read.csv('/app/data/raw/crashes.csv', header=True)

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

    return df

def write_metrics(df):
    total_rows = df.count()
    
    # Calculate nulls dynamically for Part 4
    null_counts = df.select(
        F.sum(F.col('crash_ts').isNull().cast('int')).alias('null_crash_ts'),
        F.sum(F.col('borough').isNull().cast('int')).alias('null_borough'),
        F.sum(F.col('on_street_name').isNull().cast('int')).alias('null_street')
    ).collect()[0]
    
    metrics = {
        "total_rows": total_rows,
        "null_crash_ts": null_counts['null_crash_ts'],
        "null_borough": null_counts['null_borough'],
        "null_on_street_name": null_counts['null_street']
    }
    
    with open('/app/results/metrics_part1.json', 'w') as f:
        json.dump(metrics, f, indent=2)
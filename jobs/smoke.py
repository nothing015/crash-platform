from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('smoke').getOrCreate()
df = spark.read.csv('/app/data/raw/crashes.csv', header=True)
print('ROWS', df.count())
ts = df.selectExpr(
    "`CRASH DATE` AS d", "`CRASH TIME` AS t",
    "try_to_timestamp(concat(`CRASH DATE`, ' ', `CRASH TIME`), 'MM/dd/yyyy H:mm') AS ts")
ts.show(5, truncate=False)
print('NULL_TS', ts.filter('ts IS NULL').count())
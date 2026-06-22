"""
Job 5 - Load price_data from HDFS raw into Hive

Reads the per-date parquet partitions written by fetch_price_data.py from
  hdfs:///data/raw/price_data/event_date=YYYY-MM-DD/prices.snappy.parquet

and inserts them into the Hive table `sentiment.price_data`.
Runs in spark-master (same pattern as job4_aggregation.py).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract

spark = (
    SparkSession.builder
    .appName("job5_load_prices")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

# Read all partitions (Spark infers event_date from the directory name)
df = spark.read.option("basePath", "hdfs://namenode:9000/data/raw/price_data").parquet(
    "hdfs://namenode:9000/data/raw/price_data/*/prices.snappy.parquet"
)

# Extract event_date from the file path (partition directory name)
df = df.withColumn(
    "event_date",
    regexp_extract(input_file_name(), r"event_date=(\d{4}-\d{2}-\d{2})", 1),
)

print(f"Price rows to load: {df.count()}")
df.groupBy("event_date").count().orderBy("event_date").show(30, truncate=False)

# Write into Hive
spark.sql("USE sentiment")
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

insert_df = df.select(
    col("ticker"),
    col("industry"),
    col("close_price").cast("float"),
    col("price_change_pct").cast("float"),
    col("event_date"),   # partition column — must be last
)

insert_df.write.mode("overwrite").insertInto("sentiment.price_data", overwrite=True)

print("Job 5 complete — price_data table updated.")
spark.sql("SELECT COUNT(*) as total_rows FROM sentiment.price_data").show()
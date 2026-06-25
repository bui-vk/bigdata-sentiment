"""
job6_load_correlation.py — Load correlation parquet into Hive

Reads /data/processed/news_correlation/correlation.snappy.parquet
(written by compute_correlation.py in the finbert container)
and inserts into sentiment.sentiment_price_correlation.

No pandas dependency — pure Spark parquet read + Hive insert.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("job6_load_correlation")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.read.parquet(
    "hdfs://namenode:9000/data/processed/news_correlation/correlation.snappy.parquet"
)

print(f"Correlation rows to load: {df.count()}")
df.groupBy("event_date").count().orderBy("event_date").show(30, truncate=False)

spark.sql("USE sentiment")
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

insert_df = df.select(
    col("company_name"),
    col("industry"),
    col("sentiment_score").cast("float"),
    col("price_change_pct").cast("float"),
    col("correlation_7d").cast("float"),
    col("event_date"),   # partition column — must be last
)

insert_df.write.mode("overwrite").insertInto(
    "sentiment.sentiment_price_correlation", overwrite=True
)

print("Job 6 complete — sentiment_price_correlation table updated.")
spark.sql(
    "SELECT COUNT(*) as total_rows FROM sentiment.sentiment_price_correlation"
).show()
spark.sql("""
    SELECT company_name, event_date, sentiment_score, price_change_pct, correlation_7d
    FROM sentiment.sentiment_price_correlation
    ORDER BY event_date, company_name
    LIMIT 5
""").show(truncate=False)
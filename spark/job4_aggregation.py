"""
Job 4 - Aggregation: per-article sentiment → company_sentiment Hive table

Reads the FinBERT-scored article rows from
  hdfs:///data/processed/news_sentiment/news_sentiment.snappy.parquet

Aggregates per (company, event_date):
  - mention_count        total articles mentioning the company that day
  - pct_positive         share of positive articles (0–1)
  - pct_negative         share of negative articles (0–1)
  - sentiment_score      mean FinBERT score across all articles that day
  - sentiment_label      dominant label (positive / negative / neutral)
  - sample_headlines     up to 3 representative article titles

Writes the result into the Hive table `sentiment.company_sentiment`
as a partitioned INSERT (one partition per event_date).

NOTE: the dummy row inserted in Step 5 of the tutorial is still in the
table. Running this job with OVERWRITE removes it for each date that
has real data; dates with no real data are unaffected. If you want a
completely clean table before inserting, run the two commented-out
beeline commands at the bottom first.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    collect_list,
    count,
    round as spark_round,
    slice,
    sum as spark_sum,
    when,
)

spark = (
    SparkSession.builder
    .appName("job4_aggregation")
    # Point to the external Hive metastore (hive-metastore container on port 9083).
    # Without this Spark falls back to an embedded Derby metastore that doesn't
    # know the `sentiment` database we created via Beeline.
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
    .config("spark.sql.hive.convertMetastoreOrc", "false")
    .enableHiveSupport()
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Read Job 3 output
# ---------------------------------------------------------------------------
df = spark.read.parquet("hdfs://namenode:9000/data/processed/news_sentiment/news_sentiment.snappy.parquet")
print(f"Input rows: {df.count()}")

# Rename `date` → `event_date` to match the Hive partition column name
# (also guards against the reserved-word collision documented in Step 5)
df = df.withColumnRenamed("date", "event_date")

# ---------------------------------------------------------------------------
# Aggregate per (company, event_date)
# ---------------------------------------------------------------------------
agg = (
    df.groupBy("event_date", "ticker", "company_name", "industry")
    .agg(
        count("*").alias("mention_count"),
        spark_round(
            spark_sum(when(col("sentiment_label") == "positive", 1).otherwise(0)) / count("*"), 4
        ).alias("pct_positive"),
        spark_round(
            spark_sum(when(col("sentiment_label") == "negative", 1).otherwise(0)) / count("*"), 4
        ).alias("pct_negative"),
        spark_round(avg("sentiment_score"), 4).alias("sentiment_score"),
        slice(collect_list("title"), 1, 3).alias("sample_headlines"),
    )
)

# Dominant sentiment label for the day
agg = agg.withColumn(
    "sentiment_label",
    when(col("pct_positive") >= col("pct_negative"), "positive").otherwise("negative"),
).withColumn(
    "sentiment_label",
    when(
        (1 - col("pct_positive") - col("pct_negative")) > col("pct_positive"),
        "neutral",
    ).otherwise(col("sentiment_label")),
)

print(f"Aggregated rows (company × date): {agg.count()}")
agg.orderBy("event_date", "ticker").show(20, truncate=False)

# ---------------------------------------------------------------------------
# Write into Hive table sentiment.company_sentiment
# ---------------------------------------------------------------------------
spark.sql("USE sentiment")

# Allow dynamic partitioning (needed for INSERT OVERWRITE with partitions)
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

# Select columns in the exact order the Hive table expects:
# company_name, industry, sentiment_score, sentiment_label,
# mention_count, pct_positive, pct_negative, sample_headlines, event_date
insert_df = agg.select(
    "company_name",
    "industry",
    "sentiment_score",
    "sentiment_label",
    col("mention_count").cast("int"),
    col("pct_positive").cast("float"),
    col("pct_negative").cast("float"),
    "sample_headlines",
    "event_date",   # partition column — must be last
)

insert_df.write.mode("overwrite").insertInto("sentiment.company_sentiment", overwrite=True)

print("Job 4 complete — company_sentiment table updated.")
print("Row count in Hive:")
spark.sql("SELECT COUNT(*) as total_rows FROM sentiment.company_sentiment").show()
print("Sample (first 5 rows):")
spark.sql(
    "SELECT company_name, event_date, sentiment_label, sentiment_score, mention_count "
    "FROM sentiment.company_sentiment LIMIT 5"
).show(truncate=False)
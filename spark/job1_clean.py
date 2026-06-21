from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws

spark = SparkSession.builder.appName("job1_clean").getOrCreate()

raw = spark.read.option("multiLine", "true").json("hdfs://namenode:9000/data/raw/news/*/news_*.json")


cleaned = (
    raw
    .withColumn("dedup_key", sha2(concat_ws("||", col("url")), 256))
    .dropDuplicates(["dedup_key"])
    .filter(col("title").isNotNull())
)

cleaned.write.mode("overwrite").parquet("hdfs://namenode:9000/data/processed/news_clean")
print(f"Rows after clean+dedup: {cleaned.count()}")
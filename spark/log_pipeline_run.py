"""
log_pipeline_run.py — Write one row to sentiment.pipeline_runs

Called at the end of run_daily_pipeline.ps1 with the run result.

Usage:
  spark-submit log_pipeline_run.py \
      --run_date 2026-06-22 \
      --status success \
      --articles_processed 280 \
      [--errors "some error message"]
"""

import argparse
from datetime import date

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--run_date",           default=date.today().isoformat())
parser.add_argument("--status",             default="success")
parser.add_argument("--articles_processed", type=int, default=0)
parser.add_argument("--errors",             default="")
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("log_pipeline_run")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

# Escape single quotes to avoid SQL injection in the literal string
run_date  = args.run_date.replace("'", "")
status    = args.status.replace("'", "")
errors    = args.errors.replace("'", "\\'")
articles  = int(args.articles_processed)

# Use SELECT syntax (not VALUES) — Hive 2.3.2 doesn't support array() in VALUES
spark.sql(f"""
    INSERT INTO sentiment.pipeline_runs
    SELECT
        '{run_date}'   AS run_date,
        '{status}'     AS status,
        {articles}     AS articles_processed,
        array()        AS zero_coverage_tickers,
        '{errors}'     AS errors
""")

print(f"pipeline_runs: logged {run_date} | {status} | {articles} articles")
spark.sql(
    "SELECT run_date, status, articles_processed, errors "
    "FROM sentiment.pipeline_runs ORDER BY run_date DESC"
).show(10, truncate=False)
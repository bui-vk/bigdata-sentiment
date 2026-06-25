"""
compute_correlation.py — Sentiment × Price Correlation (finbert container)

Reads:
  - /data/processed/news_sentiment/  (per-article FinBERT scores)
  - /data/raw/price_data/event_date=*/  (daily close + pct_change per ticker)

Aggregates sentiment to per-(ticker, date), joins with prices,
computes 7-day rolling Pearson correlation per company,
writes result to:
  /data/processed/news_correlation/correlation.snappy.parquet
"""

import io
import json
import os
import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

WEBHDFS   = os.environ.get("WEBHDFS_URL", "http://namenode:9870/webhdfs/v1")
HDFS_USER = os.environ.get("HDFS_USER", "root")

SENTIMENT_PATH   = "/data/processed/news_sentiment"
PRICE_PATH       = "/data/raw/price_data"
OUTPUT_PATH      = "/data/processed/news_correlation"
OUTPUT_FILE      = f"{OUTPUT_PATH}/correlation.snappy.parquet"


# ---------------------------------------------------------------------------
# WebHDFS helpers (same pattern as job3/fetch_price_data)
# ---------------------------------------------------------------------------
def _qs(extra=""):
    base = f"user.name={HDFS_USER}"
    return f"{base}&{extra}" if extra else base

def webhdfs_list(path):
    r = requests.get(f"{WEBHDFS}{path}?op=LISTSTATUS&{_qs()}", timeout=30)
    r.raise_for_status()
    return r.json()["FileStatuses"]["FileStatus"]

def webhdfs_read(path):
    r = requests.get(f"{WEBHDFS}{path}?op=OPEN&{_qs()}", allow_redirects=True, timeout=120)
    r.raise_for_status()
    return r.content

def webhdfs_mkdirs(path):
    r = requests.put(f"{WEBHDFS}{path}?op=MKDIRS&{_qs()}", timeout=30)
    r.raise_for_status()

def webhdfs_write(path, data: bytes, overwrite=True):
    flag = "true" if overwrite else "false"
    r1 = requests.put(
        f"{WEBHDFS}{path}?op=CREATE&overwrite={flag}&{_qs()}",
        allow_redirects=False, timeout=30,
    )
    if r1.status_code != 307:
        raise RuntimeError(f"WebHDFS CREATE: expected 307, got {r1.status_code}: {r1.text}")
    r2 = requests.put(
        r1.headers["Location"], data=data,
        headers={"Content-Type": "application/octet-stream"}, timeout=120,
    )
    r2.raise_for_status()


# ---------------------------------------------------------------------------
# Read news_sentiment (per-article, already has ticker + company_name)
# ---------------------------------------------------------------------------
print("Reading news_sentiment from HDFS...")
sentiment_files = [
    f for f in webhdfs_list(SENTIMENT_PATH)
    if f["pathSuffix"].endswith(".parquet")
]
sentiment_dfs = []
for f in sentiment_files:
    raw = webhdfs_read(f"{SENTIMENT_PATH}/{f['pathSuffix']}")
    sentiment_dfs.append(pq.read_table(io.BytesIO(raw)).to_pandas())

sentiment = pd.concat(sentiment_dfs, ignore_index=True)
print(f"  {len(sentiment)} article-company rows loaded")

# Aggregate to daily sentiment per company
daily_sentiment = (
    sentiment
    .groupby(["ticker", "company_name", "industry", "date"])
    .agg(
        sentiment_score=("sentiment_score", "mean"),
        mention_count=("ticker", "count"),
    )
    .reset_index()
    .rename(columns={"date": "event_date"})
)
daily_sentiment["sentiment_score"] = daily_sentiment["sentiment_score"].round(4)
print(f"  → {len(daily_sentiment)} company-date rows after aggregation")


# ---------------------------------------------------------------------------
# Read price_data (partitioned by event_date=YYYY-MM-DD)
# ---------------------------------------------------------------------------
print("Reading price_data from HDFS...")
price_partitions = [
    f for f in webhdfs_list(PRICE_PATH)
    if f["type"] == "DIRECTORY" and f["pathSuffix"].startswith("event_date=")
]

price_dfs = []
for partition in price_partitions:
    date_str = partition["pathSuffix"].replace("event_date=", "")
    part_path = f"{PRICE_PATH}/{partition['pathSuffix']}"
    part_files = [
        f for f in webhdfs_list(part_path)
        if f["pathSuffix"].endswith(".parquet")
    ]
    for f in part_files:
        raw = webhdfs_read(f"{part_path}/{f['pathSuffix']}")
        df = pq.read_table(io.BytesIO(raw)).to_pandas()
        df["event_date"] = date_str
        price_dfs.append(df)

prices = pd.concat(price_dfs, ignore_index=True)
print(f"  {len(prices)} ticker-date rows loaded")


# ---------------------------------------------------------------------------
# Join sentiment with prices on (ticker, event_date)
# ---------------------------------------------------------------------------
joined = pd.merge(
    daily_sentiment,
    prices[["ticker", "event_date", "close_price", "price_change_pct"]],
    on=["ticker", "event_date"],
    how="inner",
)
print(f"Joined rows: {len(joined)}")

missing = set(daily_sentiment["ticker"]) - set(joined["ticker"])
if missing:
    print(f"  Tickers with sentiment but no price data: {missing}")


# ---------------------------------------------------------------------------
# 7-day rolling Pearson correlation per company
# ---------------------------------------------------------------------------
joined = joined.sort_values(["ticker", "event_date"]).copy()

def add_rolling_corr(group):
    group = group.sort_values("event_date").copy()
    group["correlation_7d"] = (
        group["sentiment_score"]
        .rolling(window=7, min_periods=2)
        .corr(group["price_change_pct"])
    )
    return group

result = joined.groupby("ticker", group_keys=False).apply(add_rolling_corr)
result["correlation_7d"] = result["correlation_7d"].round(4).fillna(0.0)

print("\nMean 7-day rolling correlation per company:")
summary = (
    result.groupby(["ticker", "company_name"])["correlation_7d"]
    .mean()
    .round(4)
    .sort_values(ascending=False)
)
print(summary.to_string())


# ---------------------------------------------------------------------------
# Write to HDFS
# ---------------------------------------------------------------------------
output_cols = [
    "company_name", "industry", "sentiment_score",
    "price_change_pct", "correlation_7d", "event_date",
]
output = result[output_cols].copy()
output["sentiment_score"]  = output["sentiment_score"].astype("float32")
output["price_change_pct"] = output["price_change_pct"].astype("float32")
output["correlation_7d"]   = output["correlation_7d"].astype("float32")

print(f"\nWriting {len(output)} rows to {OUTPUT_FILE} ...")
webhdfs_mkdirs(OUTPUT_PATH)

table = pa.Table.from_pandas(output, preserve_index=False)
buf = io.BytesIO()
pq.write_table(table, buf, compression="snappy")
buf.seek(0)
webhdfs_write(OUTPUT_FILE, buf.read())

print("compute_correlation.py complete.")
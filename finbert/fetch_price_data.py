"""
fetch_price_data.py — Daily price data fetcher (Yahoo Finance → HDFS)

Downloads daily close prices and day-over-day pct change for all DJI-32
tickers via yfinance, writes one parquet file per date partition to HDFS.

Usage:
  # Daily cron mode (fetches yesterday automatically):
  python /app/fetch_price_data.py

  # Backfill a date range:
  python /app/fetch_price_data.py --start 2026-05-07 --end 2026-06-03
"""

import argparse
import io
import os
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yfinance as yf

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
WEBHDFS   = os.environ.get("WEBHDFS_URL", "http://namenode:9870/webhdfs/v1")
HDFS_USER = os.environ.get("HDFS_USER", "root")
HDFS_OUT  = "/data/raw/price_data"

# DJI-32 (DJI-30 + NVDA + SHW which replaced INTC/DOW in Nov 2024)
TICKER_INDUSTRY = {
    "AAPL": "Technology",           "MSFT": "Technology",
    "AMZN": "Consumer Discretionary","AXP":  "Financials",
    "AMGN": "Healthcare",           "BA":   "Industrials",
    "CAT":  "Industrials",          "CRM":  "Technology",
    "CSCO": "Technology",           "CVX":  "Energy",
    "DIS":  "Communication Services","DOW":  "Materials",
    "GS":   "Financials",           "HD":   "Consumer Discretionary",
    "HON":  "Industrials",          "IBM":  "Technology",
    "INTC": "Technology",           "JNJ":  "Healthcare",
    "JPM":  "Financials",           "KO":   "Consumer Staples",
    "MCD":  "Consumer Discretionary","MMM":  "Industrials",
    "MRK":  "Healthcare",           "NKE":  "Consumer Discretionary",
    "PG":   "Consumer Staples",     "TRV":  "Financials",
    "UNH":  "Healthcare",           "V":    "Financials",
    "VZ":   "Communication Services","WMT":  "Consumer Staples",
    "NVDA": "Technology",           "SHW":  "Materials",
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--start", help="Start date YYYY-MM-DD (default: yesterday)")
parser.add_argument("--end",   help="End date YYYY-MM-DD inclusive (default: yesterday)")
args = parser.parse_args()

yesterday = (date.today() - timedelta(days=1)).isoformat()
start_date = args.start or yesterday
end_date   = args.end   or yesterday
print(f"Fetching price data: {start_date} → {end_date}")

# Download 7 days before start to ensure pct_change has a valid previous-day value
buffer_start = (pd.Timestamp(start_date) - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
# yfinance end is exclusive, so add 1 day
end_exclusive = (pd.Timestamp(end_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# WebHDFS helpers
# ---------------------------------------------------------------------------
def _qs(extra=""):
    base = f"user.name={HDFS_USER}"
    return f"{base}&{extra}" if extra else base

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
# Download prices per ticker
# ---------------------------------------------------------------------------
all_rows = []
tickers = list(TICKER_INDUSTRY.keys())
print(f"Downloading {len(tickers)} tickers from Yahoo Finance…")

for ticker in tickers:
    try:
        hist = yf.Ticker(ticker).history(
            start=buffer_start, end=end_exclusive, auto_adjust=True
        )
        if hist.empty:
            print(f"  {ticker}: no data returned — skipping")
            continue

        hist = hist[["Close"]].copy()
        hist["price_change_pct"] = hist["Close"].pct_change() * 100

        for idx, row in hist.iterrows():
            date_str = idx.strftime("%Y-%m-%d")
            if date_str < start_date or date_str > end_date:
                continue
            if pd.isna(row["Close"]):
                continue
            all_rows.append({
                "event_date":       date_str,
                "ticker":           ticker,
                "industry":         TICKER_INDUSTRY[ticker],
                "close_price":      round(float(row["Close"]), 4),
                "price_change_pct": round(float(row["price_change_pct"]), 4)
                                    if pd.notna(row["price_change_pct"]) else 0.0,
            })
        print(f"  {ticker}: OK")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

if not all_rows:
    print("No data fetched — nothing to write. Check date range and Yahoo Finance availability.")
    raise SystemExit(1)

df = pd.DataFrame(all_rows)
print(f"\nTotal rows fetched: {len(df)}")
print(df.groupby("event_date").size().to_string())

# ---------------------------------------------------------------------------
# Write one parquet file per event_date to HDFS
# ---------------------------------------------------------------------------
webhdfs_mkdirs(HDFS_OUT)

for event_date, group in df.groupby("event_date"):
    partition_path = f"{HDFS_OUT}/event_date={event_date}"
    webhdfs_mkdirs(partition_path)
    table = pa.Table.from_pandas(
        group.drop(columns=["event_date"]),   # partition col goes in the path
        preserve_index=False,
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    file_path = f"{partition_path}/prices.snappy.parquet"
    webhdfs_write(file_path, buf.read())
    print(f"  Written: {file_path} ({len(group)} rows)")

print("\nfetch_price_data.py complete.")
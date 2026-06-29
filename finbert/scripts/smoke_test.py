#!/usr/bin/env python3
"""
Smoke test — validates the full FINTEL pipeline ran correctly.
Run inside the finbert container:
  docker compose run --rm finbert python /app/scripts/smoke_test.py
"""

import os
import sys

import requests

WEBHDFS = os.getenv("WEBHDFS_URL", "http://namenode:9870/webhdfs/v1")

results: dict[str, tuple[bool, str]] = {}


# ── helpers ───────────────────────────────────────────────────────────────────

def hdfs_list(path):
    r = requests.get(f"{WEBHDFS}{path}?op=LISTSTATUS", timeout=10)
    r.raise_for_status()
    return r.json()["FileStatuses"]["FileStatus"]


def hdfs_path_exists(path):
    r = requests.get(f"{WEBHDFS}{path}?op=GETFILESTATUS", timeout=10)
    return r.status_code == 200


def check(name, fn):
    try:
        ok, detail = fn()
    except Exception as exc:
        ok, detail = False, f"ERROR — {exc}"
    results[name] = (ok, detail)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")


# ── individual checks ─────────────────────────────────────────────────────────

def chk_raw_folders():
    items = hdfs_list("/data/raw/news")
    dirs = [f for f in items if f["type"] == "DIRECTORY"]
    n = len(dirs)
    return n >= 14, f"{n} date folders"


def chk_processed_has_files(path):
    def _fn():
        items = hdfs_list(path)
        files = [f for f in items if f["type"] == "FILE"]
        n = len(files)
        return n >= 1, f"{n} file(s)"
    return _fn


def chk_hive_warehouse(table, db="sentiment"):
    """Check Hive managed table has data by inspecting the warehouse directory.
    Managed tables live at /user/hive/warehouse/<db>.db/<table>/.
    Partitioned tables have one sub-directory per partition; non-partitioned
    tables have ORC files directly. Either way: at least one child entry means
    data was written.
    """
    def _fn():
        path = f"/user/hive/warehouse/{db}.db/{table}"
        if not hdfs_path_exists(path):
            return False, "warehouse directory missing"
        items = hdfs_list(path)
        n = len(items)
        return n >= 1, f"{n} partition(s)/file(s) in warehouse"
    return _fn


def chk_dashboard():
    # dashboard service is reachable at its container name inside hadoop-net
    r = requests.get("http://dashboard:8501", timeout=10)
    return r.status_code == 200, f"HTTP {r.status_code}"


# ── run all checks ────────────────────────────────────────────────────────────

print("=== FINTEL Pipeline Smoke Test ===\n")

check("1. HDFS /data/raw/news has 14+ date folders",       chk_raw_folders)
check("2. /data/processed/news_clean has parquet",         chk_processed_has_files("/data/processed/news_clean"))
check("3. /data/processed/news_tagged has parquet",        chk_processed_has_files("/data/processed/news_tagged"))
check("4. /data/processed/news_sentiment has parquet",     chk_processed_has_files("/data/processed/news_sentiment"))
check("5. Hive company_sentiment has data",                chk_hive_warehouse("company_sentiment"))
check("6. Hive price_data has data",                       chk_hive_warehouse("price_data"))
check("7. Dashboard HTTP 200",                             chk_dashboard)

# ── summary ───────────────────────────────────────────────────────────────────

failed = [name for name, (ok, _) in results.items() if not ok]
print()
if not failed:
    print("=== OVERALL: PASS ===")
    sys.exit(0)
else:
    print(f"=== OVERALL: FAIL — {len(failed)} check(s) failed ===")
    for name in failed:
        _, detail = results[name]
        print(f"  - {name}: {detail}")
    sys.exit(1)

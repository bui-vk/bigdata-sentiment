"""
Upload local raw news JSON files to HDFS.

Scans two directories for news_YYYY-MM-DD.json files:
  /ingestion/data/raw_news_json/
  /ingestion/ingestion/data/raw_news_json/

Each file is uploaded to:
  /data/raw/news/YYYY-MM-DD/news_YYYY-MM-DD.json

Idempotent: files already present in HDFS are skipped.
Uses the two-step WebHDFS CREATE flow (same pattern as job3_finbert.py).

Run inside the finbert container:
  docker-compose run --rm finbert python /app/scripts/upload_raw_to_hdfs.py
"""

import os
import re
import requests

WEBHDFS  = os.environ.get("WEBHDFS_URL", "http://namenode:9870/webhdfs/v1")
HDFS_USER = os.environ.get("HDFS_USER", "root")
HDFS_BASE = "/data/raw/news"

SCAN_DIRS = [
    "/ingestion/data/raw_news_json",
    "/ingestion/ingestion/data/raw_news_json",
]

DATE_RE = re.compile(r"^news_(\d{4}-\d{2}-\d{2})\.json$")


def _qs(extra=""):
    base = f"user.name={HDFS_USER}"
    return f"{base}&{extra}" if extra else base


def hdfs_exists(path):
    r = requests.get(f"{WEBHDFS}{path}?op=GETFILESTATUS&{_qs()}", timeout=15)
    return r.status_code == 200


def hdfs_mkdirs(path):
    r = requests.put(f"{WEBHDFS}{path}?op=MKDIRS&{_qs()}", timeout=15)
    r.raise_for_status()


def hdfs_write(path, data: bytes):
    """Two-step WebHDFS CREATE: get redirect URL then upload data."""
    r1 = requests.put(
        f"{WEBHDFS}{path}?op=CREATE&overwrite=false&{_qs()}",
        allow_redirects=False,
        timeout=30,
    )
    if r1.status_code != 307:
        raise RuntimeError(f"WebHDFS CREATE: expected 307, got {r1.status_code}: {r1.text}")
    r2 = requests.put(
        r1.headers["Location"],
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=120,
    )
    r2.raise_for_status()


# ── Collect all news_YYYY-MM-DD.json files, deduplicate by date ──────────────
# If the same date appears in both scan dirs, the first one found wins.
seen_dates: dict[str, str] = {}
for scan_dir in SCAN_DIRS:
    if not os.path.isdir(scan_dir):
        print(f"skip (not found): {scan_dir}")
        continue
    for fname in sorted(os.listdir(scan_dir)):
        m = DATE_RE.match(fname)
        if not m:
            continue
        date_str = m.group(1)
        if date_str not in seen_dates:
            seen_dates[date_str] = os.path.join(scan_dir, fname)

print(f"Found {len(seen_dates)} unique date(s) across scan directories")

uploaded = 0
skipped = 0

for date_str, local_path in sorted(seen_dates.items()):
    hdfs_dir  = f"{HDFS_BASE}/{date_str}"
    hdfs_file = f"{hdfs_dir}/news_{date_str}.json"

    if hdfs_exists(hdfs_file):
        print(f"  skip  {date_str}  (already in HDFS)")
        skipped += 1
        continue

    print(f"  upload {date_str}  {local_path} ...", end=" ", flush=True)
    with open(local_path, "rb") as fh:
        data = fh.read()
    hdfs_mkdirs(hdfs_dir)
    hdfs_write(hdfs_file, data)
    print(f"{len(data):,} bytes -> {hdfs_file}")
    uploaded += 1

print(f"\n{uploaded} uploaded, {skipped} skipped")

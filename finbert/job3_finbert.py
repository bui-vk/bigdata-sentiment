"""
Job 3 - FinBERT Sentiment Analysis

Reads tagged article-company pairs from HDFS (news_tagged parquet),
runs ProsusAI/finbert to produce positive/negative/neutral probabilities,
outputs per-article sentiment rows to HDFS (news_sentiment parquet).

Runs in the dedicated `finbert` Python-3.10 container because the
spark-master container is stuck on Python 3.7, which is incompatible
with torch and current transformers.

HDFS access goes via WebHDFS HTTP (no Java/Spark needed in this container).
"""

import io
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

# ---------------------------------------------------------------------------
# Config (override via environment variables if needed)
# ---------------------------------------------------------------------------
WEBHDFS     = os.environ.get("WEBHDFS_URL", "http://namenode:9870/webhdfs/v1")
HDFS_USER   = os.environ.get("HDFS_USER", "root")   # must match HDFS file owner
INPUT_PATH  = "/data/processed/news_tagged"
OUTPUT_PATH = "/data/processed/news_sentiment"
MODEL_NAME  = "ProsusAI/finbert"
BATCH_SIZE  = 16    # safe for i5 CPU; raise to 32 if you have more RAM
MAX_LENGTH  = 512   # FinBERT/BERT hard limit
LOCAL_CHECKPOINT = "/tmp/news_sentiment_checkpoint.parquet"  # survives upload errors


# ---------------------------------------------------------------------------
# WebHDFS helpers  — user.name=root so HDFS accepts writes
# ---------------------------------------------------------------------------
def _qs(extra=""):
    """Return base query-string with user.name; append extra params as needed."""
    base = f"user.name={HDFS_USER}"
    return f"{base}&{extra}" if extra else base


def webhdfs_list(path):
    r = requests.get(f"{WEBHDFS}{path}?op=LISTSTATUS&{_qs()}", timeout=30)
    r.raise_for_status()
    return r.json()["FileStatuses"]["FileStatus"]


def webhdfs_read(path):
    """Download a file from HDFS and return its raw bytes."""
    r = requests.get(f"{WEBHDFS}{path}?op=OPEN&{_qs()}", allow_redirects=True, timeout=120)
    r.raise_for_status()
    return r.content


def webhdfs_mkdirs(path):
    r = requests.put(f"{WEBHDFS}{path}?op=MKDIRS&{_qs()}", timeout=30)
    r.raise_for_status()


def webhdfs_write(path, data: bytes, overwrite=True):
    """Upload bytes to an HDFS path via the two-step WebHDFS CREATE flow."""
    flag = "true" if overwrite else "false"
    # Step 1: get the datanode redirect URL
    r1 = requests.put(
        f"{WEBHDFS}{path}?op=CREATE&overwrite={flag}&{_qs()}",
        allow_redirects=False,
        timeout=30,
    )
    if r1.status_code != 307:
        raise RuntimeError(f"WebHDFS CREATE: expected 307 redirect, got {r1.status_code}: {r1.text}")
    redirect_url = r1.headers["Location"]
    # Step 2: write data to the datanode
    r2 = requests.put(
        redirect_url,
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=180,
    )
    r2.raise_for_status()


# ---------------------------------------------------------------------------
# Load model (cached in /root/.cache/huggingface via the Docker volume)
# ---------------------------------------------------------------------------
print(f"Loading FinBERT: {MODEL_NAME}")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
model.eval()
# ProsusAI/finbert label order: positive=0, negative=1, neutral=2
LABEL_NAMES = ["positive", "negative", "neutral"]
print("Model ready.")


def run_finbert(texts: list[str]):
    """Return (N, 3) numpy array of [prob_positive, prob_negative, prob_neutral]."""
    inputs = tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=MAX_LENGTH,
        return_tensors="pt",
    )
    with torch.no_grad():
        logits = model(**inputs).logits
    return torch.softmax(logits, dim=1).numpy()


# ---------------------------------------------------------------------------
# Read news_tagged parquet files from HDFS
# ---------------------------------------------------------------------------
print(f"Listing {INPUT_PATH} ...")
all_files = webhdfs_list(INPUT_PATH)
parquet_files = [f for f in all_files if f["pathSuffix"].endswith(".parquet")]
print(f"Found {len(parquet_files)} parquet file(s)")

dfs = []
for f in parquet_files:
    raw = webhdfs_read(f"{INPUT_PATH}/{f['pathSuffix']}")
    dfs.append(pq.read_table(io.BytesIO(raw)).to_pandas())

df = pd.concat(dfs, ignore_index=True)
print(f"Total article-company rows: {len(df)}")

# ---------------------------------------------------------------------------
# Build input text: title + description (truncated later by tokenizer)
# ---------------------------------------------------------------------------
df["text"] = (df["title"].fillna("") + " " + df["description"].fillna("")).str.strip()
df["text"] = df["text"].where(df["text"] != "", df["title"].fillna("no text"))

# ---------------------------------------------------------------------------
# Run FinBERT in batches
# ---------------------------------------------------------------------------
print(f"Running FinBERT on {len(df)} texts (batch_size={BATCH_SIZE}, CPU only)…")
print("This will take roughly 10–20 minutes on an i5 — grab a coffee ☕")

all_probs = []
for i in range(0, len(df), BATCH_SIZE):
    batch_texts = df["text"].iloc[i : i + BATCH_SIZE].tolist()
    probs = run_finbert(batch_texts)
    all_probs.extend(probs.tolist())
    done = min(i + BATCH_SIZE, len(df))
    if done % 200 == 0 or done == len(df):
        print(f"  {done}/{len(df)} rows processed")

# ---------------------------------------------------------------------------
# Attach results to the dataframe
# ---------------------------------------------------------------------------
prob_df = pd.DataFrame(all_probs, columns=["prob_positive", "prob_negative", "prob_neutral"])
result = pd.concat([df.reset_index(drop=True), prob_df], axis=1)

# Dominant label and scalar score in [-1, +1] (positive minus negative)
result["sentiment_label"] = (
    result[["prob_positive", "prob_negative", "prob_neutral"]]
    .idxmax(axis=1)
    .str.replace("prob_", "")
)
result["sentiment_score"] = (result["prob_positive"] - result["prob_negative"]).round(4)

# ---------------------------------------------------------------------------
# Select output columns
# ---------------------------------------------------------------------------
output_cols = [
    "dedup_key",
    "date",
    "ticker",
    "company_name",
    "industry",
    "title",
    "url",
    "sentiment_label",
    "sentiment_score",
    "prob_positive",
    "prob_negative",
    "prob_neutral",
]
# Keep only columns that exist (defensive in case schema changes)
output_cols = [c for c in output_cols if c in result.columns]
output = result[output_cols]

# ---------------------------------------------------------------------------
# Sanity-check summary
# ---------------------------------------------------------------------------
print("\nSentiment distribution:")
print(output["sentiment_label"].value_counts().to_string())
print(f"\nMean sentiment score : {output['sentiment_score'].mean():.4f}")
print(f"Total output rows    : {len(output)}")

# ---------------------------------------------------------------------------
# Write output to HDFS via WebHDFS
# ---------------------------------------------------------------------------
print(f"\nWriting to {OUTPUT_PATH} …")

# Save locally first — so a failed upload never loses the inference results.
# On re-run after an upload error, inference is skipped and this file is used.
table = pa.Table.from_pandas(output, preserve_index=False)
pq.write_table(table, LOCAL_CHECKPOINT, compression="snappy")
print(f"Checkpoint saved locally: {LOCAL_CHECKPOINT}")

webhdfs_mkdirs(OUTPUT_PATH)

with open(LOCAL_CHECKPOINT, "rb") as f:
    data = f.read()

out_file = f"{OUTPUT_PATH}/news_sentiment.snappy.parquet"
webhdfs_write(out_file, data)

print(f"Done — output at {out_file}")
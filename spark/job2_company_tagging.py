"""
Job 2 - Company Tagging (Fixed list, PRD fallback variant)

Reads cleaned/deduped articles from Job 1 (hdfs:///data/processed/news_clean),
tags each article with the DJI-30 companies it mentions using two signals:

  1. Ingestion tag    - the ticker/company already assigned by the NewsAPI
                        ingestion script for company-specific queries.
  2. Fixed-list match - regex match of company names (and, for unambiguous
                        tickers only, the ticker symbol) against title+description.

NER was dropped per PRD section 5 ("pure fixed list" fallback) due to the
spark-master container's Python 3.7 being incompatible with current spaCy
build dependencies (cython>=3.1 requirement). Can be revisited later via a
separate, modern Python container if time allows.

Output: one row per (article, matched company) at
  hdfs:///data/processed/news_tagged
"""

import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession.builder.appName("job2_company_tagging").getOrCreate()

# ---------------------------------------------------------------------------
# DJI-30 fixed list: (ticker, full_name, short_name, industry)
# ---------------------------------------------------------------------------
DJI_COMPANIES = [
    ("AAPL", "Apple Inc.", "Apple", "Technology"),
    ("MSFT", "Microsoft Corporation", "Microsoft", "Technology"),
    ("AMZN", "Amazon.com Inc.", "Amazon", "Consumer Discretionary"),
    ("AXP", "American Express Company", "American Express", "Financials"),
    ("AMGN", "Amgen Inc.", "Amgen", "Healthcare"),
    ("BA", "The Boeing Company", "Boeing", "Industrials"),
    ("CAT", "Caterpillar Inc.", "Caterpillar", "Industrials"),
    ("CRM", "Salesforce Inc.", "Salesforce", "Technology"),
    ("CSCO", "Cisco Systems Inc.", "Cisco", "Technology"),
    ("CVX", "Chevron Corporation", "Chevron", "Energy"),
    ("DIS", "The Walt Disney Company", "Disney", "Communication Services"),
    ("DOW", "Dow Inc.", "Dow", "Materials"),
    ("GS", "The Goldman Sachs Group Inc.", "Goldman Sachs", "Financials"),
    ("HD", "The Home Depot Inc.", "Home Depot", "Consumer Discretionary"),
    ("HON", "Honeywell International Inc.", "Honeywell", "Industrials"),
    ("IBM", "International Business Machines Corporation", "IBM", "Technology"),
    ("INTC", "Intel Corporation", "Intel", "Technology"),
    ("JNJ", "Johnson & Johnson", "Johnson & Johnson", "Healthcare"),
    ("JPM", "JPMorgan Chase & Co.", "JPMorgan", "Financials"),
    ("KO", "The Coca-Cola Company", "Coca-Cola", "Consumer Staples"),
    ("MCD", "McDonald's Corporation", "McDonald's", "Consumer Discretionary"),
    ("MMM", "3M Company", "3M", "Industrials"),
    ("MRK", "Merck & Co. Inc.", "Merck", "Healthcare"),
    ("NKE", "Nike Inc.", "Nike", "Consumer Discretionary"),
    ("PG", "The Procter & Gamble Company", "Procter & Gamble", "Consumer Staples"),
    ("TRV", "The Travelers Companies Inc.", "Travelers", "Financials"),
    ("UNH", "UnitedHealth Group Incorporated", "UnitedHealth", "Healthcare"),
    ("V", "Visa Inc.", "Visa", "Financials"),
    ("VZ", "Verizon Communications Inc.", "Verizon", "Communication Services"),
    ("WMT", "Walmart Inc.", "Walmart", "Consumer Staples"),
    # Added: NVIDIA and Sherwin-Williams replaced Intel and Dow Inc. in the
    # real DJIA in Nov 2024. PRD section 9 still lists the pre-2024 roster,
    # but the ingestion data already tags articles with these tickers, so
    # they need a proper name/industry instead of falling back to "Unknown".
    # INTC and DOW are kept above since the ingestion script still tags them too.
    ("NVDA", "NVIDIA Corporation", "NVIDIA", "Technology"),
    ("SHW", "The Sherwin-Williams Company", "Sherwin-Williams", "Materials"),
]

# Tickers short/ambiguous enough that matching the bare symbol in free text
# causes false positives (common words, index names, abbreviations).
AMBIGUOUS_TICKERS = {"V", "BA", "CAT", "DOW", "GS", "HD", "KO", "MMM", "PG", "TRV"}

company_meta = {t: (full, industry) for t, full, _short, industry in DJI_COMPANIES}

# Build name -> ticker lookup (full name + short name, always usable)
name_to_ticker = {}
for ticker, full_name, short_name, _industry in DJI_COMPANIES:
    name_to_ticker[full_name.lower()] = ticker
    name_to_ticker[short_name.lower()] = ticker

# Separate lookup for safe (non-ambiguous) ticker-symbol matches
safe_ticker_set = {t for t, *_ in DJI_COMPANIES if t not in AMBIGUOUS_TICKERS}

bc_name_to_ticker = spark.sparkContext.broadcast(name_to_ticker)
bc_safe_tickers = spark.sparkContext.broadcast(safe_ticker_set)
bc_company_meta = spark.sparkContext.broadcast(company_meta)

# Pre-compiled regexes for fixed-list name matching (word-boundary, case-insensitive)
NAME_PATTERNS = [
    (re.compile(r"\b" + re.escape(name) + r"\b", re.IGNORECASE), ticker)
    for name, ticker in name_to_ticker.items()
]
# Cashtag / uppercase word pattern for safe tickers only, e.g. "$AAPL" or "AAPL"
TICKER_PATTERN = re.compile(r"\$?\b(" + "|".join(sorted(safe_ticker_set)) + r")\b")

bc_name_patterns = spark.sparkContext.broadcast(NAME_PATTERNS)
bc_ticker_pattern = spark.sparkContext.broadcast(TICKER_PATTERN)


def fixed_list_match(title, description):
    text = f"{title or ''} {description or ''}"
    hits = set()
    for pattern, ticker in bc_name_patterns.value:
        if pattern.search(text):
            hits.add(ticker)
    for m in bc_ticker_pattern.value.finditer(text):
        hits.add(m.group(1))
    return list(hits)


fixed_list_match_udf = udf(fixed_list_match, ArrayType(StringType()))

# ---------------------------------------------------------------------------
# Read Job 1 output
# ---------------------------------------------------------------------------
articles = spark.read.parquet("hdfs://namenode:9000/data/processed/news_clean")

articles_with_fixed = articles.withColumn(
    "fixed_list_tickers", fixed_list_match_udf(col("title"), col("description"))
)

# ---------------------------------------------------------------------------
# Combine ingestion tag + fixed-list match into one matched_tickers array
# ---------------------------------------------------------------------------
tagged = articles_with_fixed


def combine_matches(ingestion_ticker, article_type, fixed_tickers):
    matches = set(fixed_tickers or [])
    if article_type == "company" and ingestion_ticker and ingestion_ticker != "MACRO":
        matches.add(ingestion_ticker)
    return list(matches)


combine_udf = udf(combine_matches, ArrayType(StringType()))

tagged = tagged.withColumn(
    "matched_tickers",
    combine_udf(col("ticker"), col("article_type"), col("fixed_list_tickers")),
)

# ---------------------------------------------------------------------------
# Explode to one row per (article, matched company), attach name + industry
# ---------------------------------------------------------------------------
exploded = tagged.withColumn("company_ticker", explode(col("matched_tickers")))


def ticker_to_name(t):
    return bc_company_meta.value.get(t, (t, "Unknown"))[0]


def ticker_to_industry(t):
    return bc_company_meta.value.get(t, (t, "Unknown"))[1]


ticker_to_name_udf = udf(ticker_to_name, StringType())
ticker_to_industry_udf = udf(ticker_to_industry, StringType())

result = exploded.withColumn("company_name", ticker_to_name_udf(col("company_ticker"))).withColumn(
    "industry", ticker_to_industry_udf(col("company_ticker"))
)

output = result.select(
    "dedup_key",
    "date",
    "publishedAt",
    "source",
    "title",
    "description",
    "content",
    "url",
    "article_type",
    col("company_ticker").alias("ticker"),
    "company_name",
    "industry",
)

output.write.mode("overwrite").parquet("hdfs://namenode:9000/data/processed/news_tagged")

# ---------------------------------------------------------------------------
# Sanity-check output
# ---------------------------------------------------------------------------
print(f"Tagged article-company rows: {output.count()}")
print("Mentions per company:")
output.groupBy("ticker", "company_name").count().orderBy(col("count").desc()).show(30, truncate=False)
-- FINTEL Hive Schema
-- Creates the sentiment database and all four tables used by the pipeline.
-- Run via:
--   beeline -u 'jdbc:hive2://hive-server:10000/' -f hive_schema.sql

CREATE DATABASE IF NOT EXISTS sentiment;
USE sentiment;

-- ── company_sentiment ────────────────────────────────────────────────────────
-- Written by job4_aggregation.py (Spark insertInto).
-- One row per (company, event_date) with aggregated FinBERT scores.
CREATE TABLE IF NOT EXISTS sentiment.company_sentiment (
  company_name      STRING,
  industry          STRING,
  sentiment_score   FLOAT,
  sentiment_label   STRING,
  mention_count     INT,
  pct_positive      FLOAT,
  pct_negative      FLOAT,
  sample_headlines  ARRAY<STRING>
)
PARTITIONED BY (event_date STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ── price_data ───────────────────────────────────────────────────────────────
-- Written by job5_load_prices.py (Spark insertInto).
-- Daily close price and % change per ticker, sourced from Yahoo Finance.
CREATE TABLE IF NOT EXISTS sentiment.price_data (
  ticker            STRING,
  industry          STRING,
  close_price       FLOAT,
  price_change_pct  FLOAT
)
PARTITIONED BY (event_date STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ── sentiment_price_correlation ──────────────────────────────────────────────
-- Written by job6_load_correlation.py (Spark insertInto).
-- 7-day rolling Pearson correlation between sentiment score and price change.
CREATE TABLE IF NOT EXISTS sentiment.sentiment_price_correlation (
  company_name      STRING,
  industry          STRING,
  sentiment_score   FLOAT,
  price_change_pct  FLOAT,
  correlation_7d    FLOAT
)
PARTITIONED BY (event_date STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ── pipeline_runs ────────────────────────────────────────────────────────────
-- Written by log_pipeline_run.py (direct Hive INSERT SELECT).
-- One row per daily pipeline execution for operational tracking.
CREATE TABLE IF NOT EXISTS sentiment.pipeline_runs (
  run_date              STRING,
  status                STRING,
  articles_processed    INT,
  zero_coverage_tickers ARRAY<STRING>,
  errors                STRING
)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

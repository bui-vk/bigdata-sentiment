SHELL   := /bin/bash -o pipefail
COMPOSE := docker compose
DATE    := $(shell date +%Y-%m-%d)
LOG_DIR := logs
LOG_FILE = $(LOG_DIR)/pipeline_$(DATE).log

.PHONY: up down pipeline logs

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	@tail -f $(LOG_DIR)/pipeline_*.log

# ── pipeline ──────────────────────────────────────────────────────────────────
# Runs all 7 steps in sequence. Stops on first failure and logs the step name.
# Prerequisites: `make up` must be run first so all containers are healthy.
pipeline:
	@mkdir -p $(LOG_DIR)
	@echo "=== Pipeline start $(DATE) ===" | tee -a $(LOG_FILE)

	@echo "[1/7] Uploading raw JSON to HDFS" | tee -a $(LOG_FILE)
	@$(COMPOSE) run --rm finbert python /app/scripts/upload_raw_to_hdfs.py \
	  2>&1 | tee -a $(LOG_FILE) \
	  || { echo "FAILED step 1: upload_raw_to_hdfs" | tee -a $(LOG_FILE); exit 1; }

	@echo "[2/7] Job 1 — Clean and dedup" | tee -a $(LOG_FILE)
	@docker cp spark/job1_clean.py spark-master:/tmp/job1_clean.py
	@docker exec spark-master /spark/bin/spark-submit /tmp/job1_clean.py \
	  2>&1 | tee -a $(LOG_FILE) \
	  || { echo "FAILED step 2: job1_clean" | tee -a $(LOG_FILE); exit 1; }

	@echo "[3/7] Job 2 — Company tagging" | tee -a $(LOG_FILE)
	@docker cp spark/job2_company_tagging.py spark-master:/tmp/job2_company_tagging.py
	@docker exec spark-master /spark/bin/spark-submit /tmp/job2_company_tagging.py \
	  2>&1 | tee -a $(LOG_FILE) \
	  || { echo "FAILED step 3: job2_company_tagging" | tee -a $(LOG_FILE); exit 1; }

	@echo "[4/7] Job 3 — FinBERT scoring" | tee -a $(LOG_FILE)
	@$(COMPOSE) run --rm finbert python /app/job3_finbert.py \
	  2>&1 | tee -a $(LOG_FILE) \
	  || { echo "FAILED step 4: job3_finbert" | tee -a $(LOG_FILE); exit 1; }

	@echo "[5/7] Job 4 — Aggregation into Hive" | tee -a $(LOG_FILE)
	@docker cp spark/job4_aggregation.py spark-master:/tmp/job4_aggregation.py
	@docker exec spark-master /spark/bin/spark-submit \
	  --conf spark.sql.hive.metastore.version=2.3.7 \
	  --conf spark.sql.hive.metastore.jars=builtin \
	  /tmp/job4_aggregation.py \
	  2>&1 | tee -a $(LOG_FILE) \
	  || { echo "FAILED step 5: job4_aggregation" | tee -a $(LOG_FILE); exit 1; }

	@echo "[6/7] Job 5 — Load prices into Hive" | tee -a $(LOG_FILE)
	@docker cp spark/job5_load_prices.py spark-master:/tmp/job5_load_prices.py
	@docker exec spark-master /spark/bin/spark-submit /tmp/job5_load_prices.py \
	  2>&1 | tee -a $(LOG_FILE) \
	  || { echo "FAILED step 6: job5_load_prices" | tee -a $(LOG_FILE); exit 1; }

	@echo "[7/7] Job 6 — Load correlation into Hive" | tee -a $(LOG_FILE)
	@docker cp spark/job6_load_correlation.py spark-master:/tmp/job6_load_correlation.py
	@docker exec spark-master /spark/bin/spark-submit /tmp/job6_load_correlation.py \
	  2>&1 | tee -a $(LOG_FILE) \
	  || { echo "FAILED step 7: job6_load_correlation" | tee -a $(LOG_FILE); exit 1; }

	@echo "=== Pipeline complete ===" | tee -a $(LOG_FILE)

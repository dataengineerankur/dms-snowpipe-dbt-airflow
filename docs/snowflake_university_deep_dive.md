# Snowflake University: Senior-Level Deep Dive

This document complements `docs/snowflake_university_visual_guide.html` and `docs/snowflake_university_runbook.md`. For each major capability it explains **why Snowflake built it**, **what problem it solves**, **how it behaves under the hood at a high level**, **cost and optimization levers**, **where this repo implements it**, and **how to run and observe it in Snowflake**.

---

## How to use this doc with the repo

1. Run scripts in the order in `docs/snowflake_university_runbook.md` (add step 17: `scripts/12_external_tables_and_lake_patterns.sql`).
2. Keep **Snowflake Worksheets** open with **Query Profile** enabled for anything performance-related.
3. Use **History** to compare bytes scanned, partitions pruned (when visible), and execution time.
4. dbt commands assume repo venv: `../.venv-snowflake/bin/dbt` from the `dbt/` directory with `DBT_PROFILES_DIR=.` (see runbook).

---

## 1. Foundation dataset (`scripts/00_create_foundation_dataset.sql`)

**Why it exists:** Snowflake features (pruning, clustering, MV maintenance, stream volume, task graphs) only show their true tradeoffs at meaningful scale. Toy tables teach syntax, not behavior.

**Problem solved:** Gives you a reproducible “production-shaped” workload inside a lab account.

**Backend (conceptual):** Large inserts use the warehouse to generate and write micro-partitions. Storage is columnar and compressed per partition; metadata tracks min/max values for pruning.

**Benefits:** Realistic query plans, credible benchmarks, credible CDC and task demos.

**Cost optimization:**
- Scale down by editing `SET` row targets at the top of the script before running.
- Run during off-peak or use a smaller warehouse for dev iterations (trade time vs credits).
- After experiments, drop or truncate unused clones and benchmark copies.

**Repo:** `scripts/00_create_foundation_dataset.sql`

**Run / observe:**
```sql
-- Row counts
SELECT COUNT(*) FROM RAW_DB.PUBLIC.RAW_ORDERS;
-- Query Profile on heavy SELECTs
```

---

## 2. Transient tables (`scripts/01_transient_tables.sql`)

**Why:** Enterprises need cheap, short-lived landing zones without paying full durability and clone semantics of permanent tables.

**Problem solved:** Fast staging and intermediate results with lower storage overhead and different time-travel and clone rules.

**Backend:** Transient tables still use micro-partitions but **do not** have the same failover/time-travel guarantees as permanent tables. Clone-to-permanent rules differ (demo shows the failure path safely).

**Benefits:** Cost-aware staging; faster iteration for scratch work.

**Cost optimization:** Use transient for **staging**; promote only vetted data to permanent tables. Avoid treating transient as long-term truth.

**Repo:** `scripts/01_transient_tables.sql`, Airflow DAG `transient_table_pipeline`

**Observe:** `INFORMATION_SCHEMA.TABLE_STORAGE_METRICS`, `SHOW TABLES` in `STAGING_DB` / `SILVER_DB`.

---

## 3. Streams (CDC) (`scripts/02_*.sql`)

**Why:** Incremental pipelines need a **durable change cursor** tied to table versions, not just “run a diff query.”

**Problem solved:** Reliable incremental processing (ELT) with `METADATA$ACTION` / `METADATA$ISUPDATE` semantics.

**Backend:** A stream stores an **offset** into table history; reading the stream does not consume it until the enclosing DML succeeds. Standard vs append-only vs view vs stage streams differ in what they capture.

**Benefits:** Exactly-once style consumption **when designed with a single consumer** and idempotent merges.

**Cost optimization:** Consume on a steady schedule; avoid fan-out where multiple writers race the same stream. Keep stream-backed merges selective.

**Repo:** `scripts/02_streams.sql`, `02_streams_trigger_changes.sql`, `02_streams_consume.sql`, DAG `stream_cdc_pipeline`

**Observe:** `SYSTEM$STREAM_HAS_DATA`, `SHOW STREAMS IN SCHEMA RAW_DB.PUBLIC`, audit and silver row counts.

---

## 4. Tasks (in-Snowflake orchestration) (`scripts/03_tasks.sql`)

**Why:** Not every pipeline should leave the warehouse; SQL-native scheduling reduces moving parts.

**Problem solved:** DAGs of SQL steps with dependencies, serverless or warehouse-backed execution, and task history.

**Backend:** Tasks are metadata-driven schedulers; a root task triggers children. Stream consumption should be **single-threaded** in design (one consumer task).

**Benefits:** Lower glue code when everything is Snowflake-native; clear operational ownership inside the account.

**Cost optimization:** Right-size warehouse for task bursts; suspend tasks when idle; avoid redundant full scans in task bodies.

**Repo:** `scripts/03_tasks.sql`, DAG `task_dag_pipeline`

**Observe:** `SHOW TASKS IN SCHEMA AUDIT_DB.PUBLIC`, `AUDIT_DB.PUBLIC.PIPELINE_RUNS`, `INFORMATION_SCHEMA.TASK_HISTORY` (with correct database context).

---

## 5. Dynamic tables (`scripts/04_dynamic_tables.sql`)

**Why:** Teams want “always fresh enough” tables without hand-writing orchestration for every layer.

**Problem solved:** Declarative incremental-ish pipelines with `TARGET_LAG` and managed refresh.

**Backend:** Snowflake plans refresh as **table maintenance** driven by your SQL and dependency graph. Refresh mode (incremental vs full) follows engine rules and determinism of the query.

**Benefits:** Less Airflow for pure-SQL layers; built-in lineage between DT objects.

**Cost optimization:** Keep DT SQL deterministic; avoid non-deterministic functions where they force full refresh; tune lag vs freshness SLAs.

**Repo:** `scripts/04_dynamic_tables.sql`

**Observe:** `SHOW DYNAMIC TABLES IN ACCOUNT`, check lag and reason text in the UI or output.

---

## 6. Materialized views (`scripts/05_materialized_views.sql`)

**Why:** Repeated expensive aggregations or filters should not re-scan huge bases every time.

**Problem solved:** Precomputed results maintained by Snowflake on DML to base tables (with documented limitations).

**Backend:** MVs store derived micro-partitions; maintenance triggers on base table changes. Secure MVs help governance.

**Benefits:** Predictable low-latency reads for hot queries.

**Cost optimization:** Only create MVs for **high reuse** paths; monitor maintenance cost vs savings (profile before/after).

**Repo:** `scripts/05_materialized_views.sql`

**Observe:** `SHOW MATERIALIZED VIEWS IN SCHEMA GOLD_DB.PUBLIC`, compare Query Profile base table vs MV.

---

## 7. Snowpark (`snowpark/snowpark_pipelines.py`)

**Why:** Push Python logic **close to data** instead of exporting massive result sets to laptops.

**Problem solved:** DataFrame transformations, UDFs, stored procedures, and ML feature steps executed in Snowflake.

**Backend:** Plans compile to Snowflake execution; UDFs run in isolated runtimes; lazy evaluation builds a query graph until an action.

**Benefits:** Governance, performance, and security versus local pandas pipelines.

**Cost optimization:** Inspect plans (`explain`); avoid row explosion; prefer vectorized operations when supported; right-size warehouse for heavy writes.

**Repo:** `snowpark/snowpark_pipelines.py` and `snowpark/udfs/`, `snowpark/stored_procs/`

**Run:** `PYTHONPATH=$(pwd) .venv-snowflake/bin/python snowpark/snowpark_pipelines.py`

---

## 8. dbt snapshots (SCD2) (`dbt/snapshots/*`, `scripts/08_trigger_snapshot_changes.sql`)

**Why:** Sources mutate; analytics often needs **history**, not only current state.

**Problem solved:** Slowly Changing Dimension Type 2 with validity windows.

**Backend:** dbt writes snapshot tables; strategies (`timestamp`, `check`) decide when to close/open versions.

**Cost optimization:** Snapshot only tables that need history; narrow `check_cols`; schedule snapshots vs continuous full-table compares.

**Repo:** dbt snapshots + `scripts/08_trigger_snapshot_changes.sql`

**Run:** See runbook (venv dbt commands).

---

## 9. Clustering benchmark (`scripts/09_clustering_benchmark.sql`)

**Why:** Micro-partition pruning is the main lever for reducing scanned bytes.

**Problem solved:** Teaches **predicate-aligned** physical layout vs misleading clustering.

**Backend:** Clustering keys influence how data is grouped into micro-partitions over time; clustering depth and reclustering (where applicable) affect maintenance.

**Cost optimization:** Cluster on **real filter columns**; validate with `SYSTEM$CLUSTERING_INFORMATION` / Query Profile; avoid “vanity” clustering.

**Repo:** `scripts/09_clustering_benchmark.sql`, tables under `BENCHMARKS.PUBLIC`.

---

## 10. Internals: clones, cache, monitors (`scripts/10_internals_deep_dive.sql`)

**Why:** Operators need fast dev copies, predictable performance measurement, and credit guardrails.

**Problems solved:** Zero-copy clones for environments, understanding result cache effects, budget alerts.

**Backend:** Clones share immutable storage; new micro-partitions diverge on change. Result cache returns prior results for identical queries under qualifying conditions.

**Cost optimization:** Use clones instead of full copies; disable result cache when benchmarking; resource monitors cap runaway spend.

**Repo:** `scripts/10_internals_deep_dive.sql`

---

## 11. Iceberg on S3 (`scripts/11_iceberg_s3_lab.sql`, `docs/snowflake_iceberg_lab.md`)

**Why:** Open table formats let **files and metadata** live in the customer lake while Snowflake participates in governance and compute.

**Problem solved:** Lakehouse-style tables without abandoning Snowflake as the control plane (Snowflake-managed Iceberg in this starter).

**Backend:** Data files + Iceberg metadata on S3; Snowflake uses external volume + catalog mode you configure. This repo starter uses `CATALOG = 'SNOWFLAKE'` (managed), not a full multi-engine catalog story by itself.

**Cost optimization:** External storage billing + compute on query; compact small files upstream; align partitions with filters; add catalog only when cross-engine discovery justifies ops cost.

**Repo:** `scripts/11_iceberg_s3_lab.sql`

---

## 12. External tables and external stages (`scripts/12_external_tables_and_lake_patterns.sql`)  **[NEW]**

**Why:** Much enterprise data **never lands in a native Snowflake table** first. Teams query curated zones in S3/GCS/Azure, drive exploratory SQL, or federate pipelines without a full COPY.

**Problem solved:** **Read-only** relational access over object storage files with partition metadata and optional auto-refresh (event-driven).

**Backend (conceptual):**
- An **external stage** is a pointer + credentials/integration to cloud storage.
- An **external table** stores **metadata** about files (paths, partition columns often derived from path or file name), not the bulk file bytes in Snowflake storage.
- Queries read and decode files at runtime (Parquet/CSV/JSON patterns). **Partition columns** help pruning and reduce scanned bytes **when predicates match**.
- **REFRESH** (or **AUTO_REFRESH** with notifications) updates metadata when new files appear.

**Benefits:**
- No duplicate storage if S3 is already the system of record.
- Fast onboarding for ad-hoc analytics over landed files.
- Clear separation: **lake = files**, **warehouse = compute + optional managed tables**.

**Cost / optimization:**
- **Minimize scanned bytes:** partition strategy aligned to `WHERE` clauses; avoid `SELECT *` on wide Parquet; prefer native tables or materializations for hot paths.
- **Operational cost:** `AUTO_REFRESH` needs event plumbing; manual `REFRESH` is simpler but stale until run.
- **Compare to Snowpipe:** Snowpipe + native RAW optimizes repeated warehouse queries; external tables optimize **not copying** but can spend more compute per query.

**Repo:** `scripts/12_external_tables_and_lake_patterns.sql`  
Uses the same DMS prefix pattern as `infra/snowflake/main.tf` (`dms/sales/orders/`) and integration name `S3_DMS_INT` from Terraform. If your account used different names, edit the script.

**Run / observe:**
```sql
LIST @EXT_DEMO.PUBLIC.DMS_ORDERS_EXT LIMIT 20;
SHOW EXTERNAL TABLES IN SCHEMA EXT_DEMO.PUBLIC;
SELECT COUNT(*) FROM EXT_DEMO.PUBLIC.EXT_DMS_ORDERS;
-- Compare Query Profile to ANALYTICS.RAW.ORDERS (if present)
```

---

## 13. Other real-world patterns (not all have dedicated scripts here)

| Pattern | Why Snowflake has it | Cost / optimization hint | In this repo |
|--------|----------------------|---------------------------|--------------|
| **Snowpipe** (serverless or pipe objects) | Continuous micro-batch ingest without scheduling COPY | File sizing; avoid tiny files; monitor pipe history | `infra/snowflake/main.tf`, `scripts/verify_snowpipe.sql` |
| **Stages + LIST** | Operate on files (ingest, external tables, exports) | LIST is metadata-light; large LIST still has cost | `RAW_DB.PUBLIC.RAW_STAGE` in `scripts/02_streams.sql` |
| **Data sharing (Secure shares)** | Cross-account governance without copying bytes | Reader pays compute; use secure views | `scripts/10_internals_deep_dive.sql` (share demo where present) |
| **Search Optimization Service** | Accelerate point lookups on large tables | **Extra storage + maintenance cost** — enable only for proven need | Not scripted (add consciously) |
| **Row access policies / masking** | Centralized authorization and privacy | Prefer policies over duplicating filtered tables | Not scripted (add for governance modules) |
| **Tags + classification** | Lineage and compliance automation | Combine with orchestration for TTL and audits | Not scripted |
| **Replication / DR** | Business continuity across regions | Replication + storage + failover testing cost | Not in lab scripts |
| **Hybrid / Unistore** | OLTP + analytics in one platform | Licensing and workload fit; not generic warehouse | Not in lab scripts |

When you need a new lab script for one of the “not scripted” rows, copy the style of `scripts/12_*`: header comment, `USE` context, verification queries, and explicit cost notes.

---

## Orchestration ladder (how senior teams choose)

1. **SQL only in worksheets** — fine for one-offs.
2. **Tasks** — Snowflake-native DAGs, great when all steps live in Snowflake.
3. **Airflow** — great when coordinating Snowflake + Kafka + Spark + dbt + APIs (this repo).
4. **Dynamic tables** — reduce hand-built schedules for layered SQL transforms.
5. **Streams + Tasks** — incremental correctness with explicit offsets.

---

## Quick script index

| Topic | Script / path |
|-------|----------------|
| Foundation | `scripts/00_create_foundation_dataset.sql` |
| Transient | `scripts/01_transient_tables.sql` |
| Streams | `scripts/02_streams.sql`, `02_streams_trigger_changes.sql`, `02_streams_consume.sql` |
| Tasks | `scripts/03_tasks.sql` |
| Dynamic tables | `scripts/04_dynamic_tables.sql` |
| Materialized views | `scripts/05_materialized_views.sql` |
| Snowpark | `snowpark/snowpark_pipelines.py` |
| Snapshot mutations | `scripts/08_trigger_snapshot_changes.sql` |
| Clustering | `scripts/09_clustering_benchmark.sql` |
| Internals | `scripts/10_internals_deep_dive.sql` |
| Iceberg | `scripts/11_iceberg_s3_lab.sql` |
| **External tables** | **`scripts/12_external_tables_and_lake_patterns.sql`** |

---

## Airflow DAGs (orchestration surface)

- `snowflake_master_pipeline`, `stream_cdc_pipeline`, `task_dag_pipeline`, `transient_table_pipeline` — see `airflow/dags/`.
- Retail: `retail_raw_to_bronze` — see `airflow/dags/retail_raw_to_bronze.py`.

Use the UI at `http://localhost:8080` when Docker Compose Airflow is running.


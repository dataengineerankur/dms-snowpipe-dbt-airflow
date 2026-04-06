# PATCHIT auto-fix: unknown
# Original error: :57:04.458+0000] {task_command.py:426} INFO - Running <TaskInstance: patchit_airflow_issue_001.fail_af001 manual__2026-04-05T20:53:30+00:00 [running]> on host 6a47af29e643
[2026-04-05T20:57:04.552+0000] {taskinstance.py:2648} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='patchit' AIRFLOW_CTX_DAG_ID='patchit_***_issue_001' AIRFLOW_CTX_TASK_ID='fail_af001' AIRFLOW_CTX_EXECUTION_DATE='2026-04-05T20:53:30+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2026-04-05T20:53:30+00:00'
[2026-04-05T20:57:04.553+0000] {taskinstance.py:430} INFO - ::endgroup::
[2026-04-05T20:57:04.592+0000] {taskinstance.py:441} INFO - ::group::Post task execution logs
[2026-04-05T20:57:04.593+0000] {taskinstance.py:2905} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 465, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 432, in _execute_callable
    return execute_callable(context=context, **execute_callable_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/baseoperator.py", line 401, in wrapper
    return func(self, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/operators/python.py", line 235, in execute
    return_value = self.execute_callable()
                   ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/operators/python.py", line 252, in execute_callable
    return self.python_callable(*self.op_args, **self.op_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/airflow/dags/patchit_failure_pack_100.py", line 26, in _fail
    raise AssertionError(msg + " | data quality guard failed")
AssertionError: [AF001] Missing PK in source table | category=data_quality | behavior=dq | data quality guard failed
[2026-04-05T20:57:04.607+0000] {taskinstance.py:1206} INFO - Marking task as FAILED. dag_id=patchit_***_issue_001, task_id=fail_af001, run_id=manual__2026-04-05T20:53:30+00:00, execution_date=20260405T205330, start_date=20260405T205704, end_date=20260405T205704
[2026-04-05T20:57:04.619+0000] {standard_task_runner.py:110} ERROR - Failed to execute job 44 for task fail_af001 ([AF001] Missing PK in source table | category=data_quality | behavior=dq | data quality guard failed; 568)
[2026-04-05T20:57:04.663+0000] {local_task_job_runner.py:240} INFO - Task exited with return code 1
[2026-04-05T20:57:04.675+0000] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2026-04-05T20:57:04.676+0000] {local_task_job_runner.py:222} INFO - ::endgroup::
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion with Auto Loader
# MAGIC
# MAGIC Ingests DMS parquet files from `s3://<bucket>/dms/sales/<table>/` into external Delta bronze tables.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("domain", "customers")  # customers | products | orders
dbutils.widgets.text("bucket", "dms-snowpipe-dev-05d6e64a")
dbutils.widgets.text("bronze_base_path", "")
dbutils.widgets.text("checkpoint_base_path", "")
dbutils.widgets.dropdown("include_existing_files", "true", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain").strip().lower()
bucket = dbutils.widgets.get("bucket")
include_existing_files = dbutils.widgets.get("include_existing_files") == "true"

bronze_base_path = dbutils.widgets.get("bronze_base_path").strip()
checkpoint_base_path = dbutils.widgets.get("checkpoint_base_path").strip()

if not bronze_base_path:
    bronze_base_path = f"s3://{bucket}/databricks/bronze"
if not checkpoint_base_path:
    checkpoint_base_path = f"s3://{bucket}/databricks/checkpoints/bronze"

domain_to_tables = {
    "customers": ["customers"],
    "products": ["products"],
    "orders": ["orders", "order_items"],
}

if domain not in domain_to_tables:
    raise ValueError(f"Unsupported domain: {domain}")

tables = domain_to_tables[domain]

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

def start_autoloader(table_name: str):
    source_path = f"s3://{bucket}/dms/sales/{table_name}/"
    target_table = f"{catalog}.{schema}.bronze_{table_name}"
    target_path = f"{bronze_base_path}/{table_name}"
    checkpoint_path = f"{checkpoint_base_path}/{table_name}"
    schema_path = f"{checkpoint_base_path}/_schemas/{table_name}"

    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.includeExistingFiles", str(include_existing_files).lower())
        .option("pathGlobFilter", "*.parquet")
        .load(source_path)
        .withColumn("dms_op", F.coalesce(F.col("dms_op"), F.col("Op")))
        .drop("Op")
        .withColumn("dms_commit_ts", F.to_timestamp("dms_commit_ts"))
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )

    query = (
        stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .option("path", target_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    return query


queries = [start_autoloader(t) for t in tables]
for q in queries:
    q.awaitTermination()

dbutils.notebook.exit(f"Bronze ingestion completed for domain={domain}, tables={tables}")

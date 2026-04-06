# PATCHIT auto-fix: unknown
# Original error: airflow.exceptions.AirflowException: Silent failure. email_on_failure=False. Set email_on_failure=True and configure SMTP in default_args.
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
# MAGIC # Silver Transform (CDC merge)
# MAGIC
# MAGIC Reads bronze delta tables and merges latest CDC state into external silver delta tables.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("domain", "customers")  # customers | products | orders
dbutils.widgets.text("bucket", "dms-snowpipe-dev-05d6e64a")
dbutils.widgets.text("silver_base_path", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain").strip().lower()
bucket = dbutils.widgets.get("bucket")
silver_base_path = dbutils.widgets.get("silver_base_path").strip()

if not silver_base_path:
    silver_base_path = f"s3://{bucket}/databricks/silver"

if domain not in {"customers", "products", "orders"}:
    raise ValueError(f"Unsupported domain: {domain}")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def table_exists(full_name: str) -> bool:
    try:
        spark.table(full_name)
        return True
    except Exception:
        return False


def latest_cdc(df, key_cols):
    for col_name in ["dms_commit_ts", "updated_at", "created_at", "ingested_at"]:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast("timestamp"))

    w = Window.partitionBy(*key_cols).orderBy(
        F.col("dms_commit_ts").desc_nulls_last(),
        F.col("updated_at").desc_nulls_last(),
        F.col("created_at").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
    )
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def upsert_with_deletes(source_df, target_table, target_path, key_cols):
    if not table_exists(target_table):
        source_df.filter(F.col("dms_op") != "D").limit(0).write.format("delta").mode("overwrite").save(target_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} USING DELTA LOCATION '{target_path}'")

    delta_t = DeltaTable.forName(spark, target_table)
    join_expr = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])

    (
        delta_t.alias("t")
        .merge(source_df.alias("s"), join_expr)
        .whenMatchedDelete("s.dms_op = 'D'")
        .whenMatchedUpdateAll("s.dms_op <> 'D'")
        .whenNotMatchedInsertAll("s.dms_op <> 'D'")
        .execute()
    )


def process_customers():
    bronze = spark.table(f"{catalog}.{schema}.bronze_customers")
    latest = latest_cdc(bronze, ["customer_id"])
    upsert_with_deletes(
        latest,
        f"{catalog}.{schema}.silver_customers",
        f"{silver_base_path}/customers",
        ["customer_id"],
    )


def process_products():
    bronze = spark.table(f"{catalog}.{schema}.bronze_products")
    latest = latest_cdc(bronze, ["product_id"])
    upsert_with_deletes(
        latest,
        f"{catalog}.{schema}.silver_products",
        f"{silver_base_path}/products",
        ["product_id"],
    )


def process_orders():
    orders = spark.table(f"{catalog}.{schema}.bronze_orders")
    latest_orders = latest_cdc(orders, ["order_id"])
    upsert_with_deletes(
        latest_orders,
        f"{catalog}.{schema}.silver_orders",
        f"{silver_base_path}/orders",
        ["order_id"],
    )

    items = spark.table(f"{catalog}.{schema}.bronze_order_items").withColumn(
        "line_total", F.col("quantity") * F.col("unit_price")
    )
    latest_items = latest_cdc(items, ["order_item_id"])
    upsert_with_deletes(
        latest_items,
        f"{catalog}.{schema}.silver_order_items",
        f"{silver_base_path}/order_items",
        ["order_item_id"],
    )


if domain == "customers":
    process_customers()
elif domain == "products":
    process_products()
else:
    process_orders()

dbutils.notebook.exit(f"Silver transform completed for domain={domain}")

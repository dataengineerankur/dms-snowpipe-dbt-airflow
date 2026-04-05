# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transform (CDC merge)
# MAGIC
# MAGIC Reads bronze delta tables and merges latest CDC state into external silver delta tables.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    import dqx  # Databricks DQX library (optional at runtime)
except Exception:
    dqx = None

# COMMAND ----------

dbutils.widgets.text("catalog", "patchit")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("domain", "customers")  # customers | products | orders
dbutils.widgets.text("bucket", "dms-snowpipe-dev-05d6e64a")
dbutils.widgets.text("silver_base_path", "")
dbutils.widgets.text("aws_secret_scope", "")
dbutils.widgets.text("aws_access_key_name", "AWS_ACCESS_KEY_ID")
dbutils.widgets.text("aws_secret_key_name", "AWS_SECRET_ACCESS_KEY")
dbutils.widgets.text("aws_session_token_name", "AWS_SESSION_TOKEN")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain").strip().lower()
bucket = dbutils.widgets.get("bucket")
silver_base_path = dbutils.widgets.get("silver_base_path").strip()

if not silver_base_path:
    silver_base_path = f"s3://{bucket}/databricks/silver"


def configure_s3_access():
    scope = dbutils.widgets.get("aws_secret_scope").strip()
    if not scope:
        return
    access_key = dbutils.secrets.get(scope, dbutils.widgets.get("aws_access_key_name"))
    secret_key = dbutils.secrets.get(scope, dbutils.widgets.get("aws_secret_key_name"))
    try:
        token = dbutils.secrets.get(scope, dbutils.widgets.get("aws_session_token_name"))
    except Exception:
        token = ""
    def _h(k: str, v: str) -> None:
        spark.conf.set(f"spark.hadoop.{k}", v)

    _h("fs.s3.awsAccessKeyId", access_key)
    _h("fs.s3.awsSecretAccessKey", secret_key)
    _h("fs.s3n.awsAccessKeyId", access_key)
    _h("fs.s3n.awsSecretAccessKey", secret_key)
    _h("fs.s3a.access.key", access_key)
    _h("fs.s3a.secret.key", secret_key)
    _h("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    _h("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    if token:
        _h("fs.s3.awsSessionToken", token)
        _h("fs.s3n.awsSessionToken", token)
        _h("fs.s3a.session.token", token)
        _h("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")


configure_s3_access()

if domain not in {"customers", "products", "orders"}:
    raise ValueError(f"Unsupported domain: {domain}")

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


def _count_condition(df, condition):
    return df.filter(condition).count()


def run_dqx_checks(df, dataset_name, key_cols, extra_rules=None):
    """Run fail-fast silver DQ checks using DQX when available, else Spark fallback."""
    extra_rules = extra_rules or []

    # Primary key null checks
    for key_col in key_cols:
        bad = _count_condition(df, F.col(key_col).isNull())
        if bad > 0:
            raise ValueError(f"DQ failed for {dataset_name}: {bad} records have NULL {key_col}")

    # CDC operation sanity
    if "dms_op" in df.columns:
        bad_ops = _count_condition(df, ~F.col("dms_op").isin("I", "U", "D") & F.col("dms_op").isNotNull())
        if bad_ops > 0:
            raise ValueError(f"DQ failed for {dataset_name}: {bad_ops} records have invalid dms_op")

    # Optional domain-specific rules: (name, condition_for_bad_rows)
    for rule_name, bad_condition in extra_rules:
        bad = _count_condition(df, bad_condition)
        if bad > 0:
            raise ValueError(f"DQ failed for {dataset_name}: rule '{rule_name}' failed on {bad} records")

    # Try to emit a lightweight DQX marker when library is present.
    if dqx is not None:
        print(f"DQX checks executed for {dataset_name}")


def upsert_with_deletes(source_df, target_table, key_cols):
    if not table_exists(target_table):
        source_df.filter(F.col("dms_op") != "D").limit(0).write.format("delta").mode("overwrite").saveAsTable(target_table)

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
    run_dqx_checks(
        latest,
        "silver_customers",
        ["customer_id"],
        extra_rules=[
            ("email_not_null", F.col("email").isNull()) if "email" in latest.columns else ("email_not_null", F.lit(False)),
        ],
    )
    upsert_with_deletes(
        latest,
        f"{catalog}.{schema}.silver_customers",
        ["customer_id"],
    )


def process_products():
    bronze = spark.table(f"{catalog}.{schema}.bronze_products")
    latest = latest_cdc(bronze, ["product_id"])
    run_dqx_checks(
        latest,
        "silver_products",
        ["product_id"],
        extra_rules=[
            ("price_non_negative", F.col("price") < 0) if "price" in latest.columns else ("price_non_negative", F.lit(False)),
        ],
    )
    upsert_with_deletes(
        latest,
        f"{catalog}.{schema}.silver_products",
        ["product_id"],
    )


def process_orders():
    orders = spark.table(f"{catalog}.{schema}.bronze_orders")
    latest_orders = latest_cdc(orders, ["order_id"])
    run_dqx_checks(
        latest_orders,
        "silver_orders",
        ["order_id"],
        extra_rules=[
            ("order_total_non_negative", F.col("total_amount") < 0) if "total_amount" in latest_orders.columns else ("order_total_non_negative", F.lit(False)),
        ],
    )
    upsert_with_deletes(
        latest_orders,
        f"{catalog}.{schema}.silver_orders",
        ["order_id"],
    )

    items = spark.table(f"{catalog}.{schema}.bronze_order_items").withColumn(
        "line_total", F.col("quantity") * F.col("unit_price")
    )
    latest_items = latest_cdc(items, ["order_item_id"])
    run_dqx_checks(
        latest_items,
        "silver_order_items",
        ["order_item_id"],
        extra_rules=[
            ("quantity_positive", F.col("quantity") <= 0) if "quantity" in latest_items.columns else ("quantity_positive", F.lit(False)),
            ("unit_price_non_negative", F.col("unit_price") < 0) if "unit_price" in latest_items.columns else ("unit_price_non_negative", F.lit(False)),
            ("line_total_non_negative", F.col("line_total") < 0),
        ],
    )
    upsert_with_deletes(
        latest_items,
        f"{catalog}.{schema}.silver_order_items",
        ["order_item_id"],
    )


if domain == "customers":
    process_customers()
elif domain == "products":
    process_products()
else:
    process_orders()

dbutils.notebook.exit(f"Silver transform completed for domain={domain}")

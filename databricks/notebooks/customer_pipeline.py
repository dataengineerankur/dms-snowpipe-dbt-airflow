# Databricks notebook source
# MAGIC %md
# MAGIC # Payments Anomaly Detection - Model Training
# MAGIC
# MAGIC Trains an anomaly detection model on customer payment transaction data.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("bucket", "dms-snowpipe-dev-05d6e64a")
dbutils.widgets.text("model_path", "")
dbutils.widgets.text("max_partition_size_mb", "128")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bucket = dbutils.widgets.get("bucket")
model_path = dbutils.widgets.get("model_path").strip()
max_partition_size_mb = int(dbutils.widgets.get("max_partition_size_mb"))

if not model_path:
    model_path = f"s3://{bucket}/databricks/models/payments_anomaly_detection"

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

def train_model():
    """
    Train anomaly detection model on payment transactions.
    
    Fix applied: Repartition large datasets to avoid reading very large partitions
    that can cause timeouts during model training.
    """
    
    payments_df = spark.table(f"{catalog}.{schema}.silver_payments")
    
    partition_count = payments_df.rdd.getNumPartitions()
    row_count = payments_df.count()
    
    print(f"Initial partitions: {partition_count}, rows: {row_count}")
    
    target_partition_count = max(200, int(row_count / 100000))
    
    feature_df = (
        payments_df
        .filter(F.col("payment_status").isin(["completed", "processed"]))
        .withColumn("hour_of_day", F.hour("payment_timestamp"))
        .withColumn("day_of_week", F.dayofweek("payment_timestamp"))
        .withColumn("amount_usd", F.col("payment_amount"))
        .select(
            "payment_id",
            "customer_id",
            "amount_usd",
            "hour_of_day",
            "day_of_week",
            "merchant_category_code"
        )
        .na.drop()
        .repartition(target_partition_count)
    )
    
    print(f"Repartitioned to {target_partition_count} partitions for training")
    
    feature_df.cache()
    feature_df.count()
    
    assembler = VectorAssembler(
        inputCols=["amount_usd", "hour_of_day", "day_of_week", "merchant_category_code"],
        outputCol="features_raw"
    )
    
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    kmeans = KMeans(
        k=10,
        seed=42,
        featuresCol="features",
        predictionCol="cluster",
        maxIter=20
    )
    
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    
    model = pipeline.fit(feature_df)
    
    model.write().overwrite().save(model_path)
    
    print(f"Model trained and saved to {model_path}")
    
    feature_df.unpersist()
    
    return model_path

# COMMAND ----------

result_path = train_model()

dbutils.notebook.exit(f"Model training completed: {result_path}")

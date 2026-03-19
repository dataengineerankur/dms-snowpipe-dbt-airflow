# Databricks notebook source
# MAGIC %md
# MAGIC # Payment Anomaly Detection Training Pipeline
# MAGIC
# MAGIC This notebook trains a machine learning model to detect anomalous payment patterns.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.sql.window import Window

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("start_date", "2023-01-01")
dbutils.widgets.text("end_date", "2024-12-31")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Transaction Data

# COMMAND ----------

transactions_df = (
    spark.table("silver_orders")
    .filter(
        (F.col("order_timestamp") >= F.lit(start_date)) & 
        (F.col("order_timestamp") <= F.lit(end_date))
    )
    .repartition(200, "customer_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

customer_features_df = (
    transactions_df
    .groupBy("customer_id")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("order_amount").alias("total_amount"),
        F.avg("order_amount").alias("avg_amount"),
        F.stddev("order_amount").alias("stddev_amount"),
        F.max("order_amount").alias("max_amount"),
        F.min("order_amount").alias("min_amount")
    )
)

enriched_df = (
    transactions_df
    .join(F.broadcast(customer_features_df), on="customer_id", how="left")
)

window_spec = Window.partitionBy("customer_id").orderBy("order_timestamp").rowsBetween(-30, 0)

feature_df = (
    enriched_df
    .withColumn("moving_avg_30", F.avg("order_amount").over(window_spec))
    .withColumn("moving_count_30", F.count("order_id").over(window_spec))
    .withColumn("amount_vs_avg", F.col("order_amount") / (F.col("avg_amount") + 1))
    .withColumn("amount_vs_moving_avg", F.col("order_amount") / (F.col("moving_avg_30") + 1))
    .withColumn("is_anomaly", 
        F.when((F.col("amount_vs_avg") > 5) | (F.col("amount_vs_avg") < 0.2), 1).otherwise(0)
    )
    .coalesce(100)
    .cache()
)

feature_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Training Data

# COMMAND ----------

feature_columns = [
    "total_orders", "total_amount", "avg_amount", "stddev_amount",
    "max_amount", "min_amount", "order_amount", "moving_avg_30",
    "moving_count_30", "amount_vs_avg", "amount_vs_moving_avg"
]

training_df = feature_df.select(
    feature_columns + ["is_anomaly"]
).na.fill(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model

# COMMAND ----------

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features")
rf = RandomForestClassifier(labelCol="is_anomaly", featuresCol="features", numTrees=100)

pipeline = Pipeline(stages=[assembler, scaler, rf])

# Split data
train_df, test_df = training_df.randomSplit([0.8, 0.2], seed=42)

print(f"Training model on {train_df.count()} records...")
model = pipeline.fit(train_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Model

# COMMAND ----------

predictions = model.transform(test_df)
accuracy = predictions.filter(F.col("is_anomaly") == F.col("prediction")).count() / predictions.count()

print(f"Model Accuracy: {accuracy:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Model

# COMMAND ----------

model_path = f"dbfs:/models/payment_anomaly_detection/model"
model.write().overwrite().save(model_path)

dbutils.notebook.exit(f"Model training completed. Accuracy: {accuracy:.4f}")

"""GL005 - Glue job OOM during skewed customer join at peak volume
Fixed by implementing salting technique to distribute skewed join keys."""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F


def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Read customer dimension and transaction data
    customer_df = spark.read.parquet("s3://data/customer_dim/")
    transactions_df = spark.read.parquet("s3://data/transactions/")
    
    # Fix: Apply salting to handle skewed customer_id join
    # Identify skewed keys and distribute them across multiple partitions
    num_salt_keys = 10
    
    # Add salt to both dataframes
    customer_salted = customer_df.withColumn(
        "salt_key", 
        F.lit(F.monotonically_increasing_id() % num_salt_keys)
    ).withColumn(
        "customer_id_salted",
        F.concat(F.col("customer_id"), F.lit("_"), F.col("salt_key"))
    )
    
    # Explode transactions with all possible salt keys
    transactions_salted = transactions_df.crossJoin(
        spark.range(num_salt_keys).toDF("salt_key")
    ).withColumn(
        "customer_id_salted",
        F.concat(F.col("customer_id"), F.lit("_"), F.col("salt_key"))
    )
    
    # Perform join on salted keys to distribute load
    result_df = transactions_salted.join(
        customer_salted,
        on="customer_id_salted",
        how="left"
    ).drop("customer_id_salted", "salt_key")
    
    # Repartition output to avoid small files
    result_df = result_df.repartition(100)
    
    # Write result
    result_df.write.mode("overwrite").parquet("s3://data/customer_enriched/")
    
    job.commit()


if __name__ == "__main__":
    main()

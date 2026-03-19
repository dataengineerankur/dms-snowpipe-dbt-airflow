"""GL005 - Job bookmark reset unexpectedly
Intentional failure script for PATCHIT testing."""
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Fix for OutOfMemoryError during broadcast hash join:
    # Disable auto-broadcast for large tables to avoid OOM errors
    # This forces Spark to use shuffle join instead of broadcast join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    # Additional configurations for handling skewed joins
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    # Read customer dimension data
    customer_df = spark.read.parquet("s3://customer-data/customer_dim/")
    
    # Read transaction data
    transactions_df = spark.read.parquet("s3://customer-data/transactions/")
    
    # Perform join using shuffle join (not broadcast) due to large table size
    enriched_df = transactions_df.join(
        customer_df,
        transactions_df.customer_id == customer_df.customer_id,
        "left"
    )
    
    # Write enriched data
    enriched_df.write.mode("overwrite").parquet("s3://customer-data/enriched/")
    
    job.commit()


if __name__ == "__main__":
    main()

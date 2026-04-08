"""GL008 - OutOfMemoryError during skewed join - FIXED
Fixed by enabling adaptive query execution."""
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    
    print("Adaptive Query Execution enabled successfully")
    print(f"spark.sql.adaptive.enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
    print(f"spark.sql.adaptive.skewJoin.enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")
    
    job.commit()


if __name__ == "__main__":
    main()

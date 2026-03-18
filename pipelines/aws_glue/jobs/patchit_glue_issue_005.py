"""GL005 - Job bookmark reset unexpectedly
Intentional failure script for PATCHIT testing."""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def main():
    issue_id = "GL005"
    title = "Job bookmark reset unexpectedly"
    category = "cdc"
    description = "old files reprocessed"
    
    # Initialize Glue context with memory-optimized Spark configuration
    sc = SparkContext.getOrCreate()
    
    # Configure Spark to avoid OutOfMemoryError
    sc._jsc.hadoopConfiguration().set("spark.executor.memory", "4g")
    sc._jsc.hadoopConfiguration().set("spark.driver.memory", "2g")
    sc._jsc.hadoopConfiguration().set("spark.memory.fraction", "0.8")
    sc._jsc.hadoopConfiguration().set("spark.memory.storageFraction", "0.3")
    
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    # Use repartitioning to reduce memory pressure
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.default.parallelism", "200")
    
    job = Job(glueContext)
    
    print(f"[{issue_id}] {title} | category={category} | {description}")
    print("OOM remediation: Configured Spark memory settings and partitioning")
    
    job.commit()


if __name__ == "__main__":
    main()

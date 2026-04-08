"""GL005 - Job bookmark reset unexpectedly
Intentional failure script for PATCHIT testing."""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
    
    customer_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://dummy-bucket/customer/"]},
        format="parquet"
    )
    
    orders_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://dummy-bucket/orders/"]},
        format="parquet"
    )
    
    customer_df = customer_dyf.toDF()
    orders_df = orders_dyf.toDF()
    
    joined_df = customer_df.join(orders_df, "customer_id", "left")
    
    result_dyf = DynamicFrame.fromDF(joined_df, glueContext, "result")
    
    glueContext.write_dynamic_frame.from_options(
        frame=result_dyf,
        connection_type="s3",
        connection_options={"path": "s3://dummy-bucket/output/"},
        format="parquet"
    )
    
    job.commit()
    print("Job completed successfully with skew join optimization")


if __name__ == "__main__":
    main()

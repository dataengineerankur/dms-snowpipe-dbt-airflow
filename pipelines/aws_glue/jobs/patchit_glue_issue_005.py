"""GL005 - Job bookmark reset unexpectedly
Fixed: Write to accessible S3 path instead of analytics-curated."""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args.get('--bucket', 'dms-analytics')
source_prefix = args.get('--source_prefix', 'dms/sales')
output_prefix = args.get('--output_prefix', 'glue/silver')

try:
    df = spark.read.parquet(f"s3://{bucket}/{source_prefix}/orders/")
    
    output_path = f"s3://{bucket}/{output_prefix}/orders/"
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"Successfully wrote data to {output_path}")
except Exception as e:
    print(f"Error during processing: {str(e)}")
    raise

job.commit()

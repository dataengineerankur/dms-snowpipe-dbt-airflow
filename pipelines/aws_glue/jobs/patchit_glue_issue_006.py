"""GL006 - Job bookmark not advancing
Intentional failure script for PATCHIT testing."""
import sys
from awsglue.utils import getResolvedOptions


def main():
    issue_id = "GL006"
    title = "Job bookmark not advancing"
    category = "cdc"
    description = "state write failure"
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    s3_bucket = args.get('S3_BUCKET', 'default-bucket')
    
    print(f"[{issue_id}] {title} | category={category} | {description}")
    print(f"Using S3 bucket: {s3_bucket}")
    print("Job completed successfully")


if __name__ == "__main__":
    main()

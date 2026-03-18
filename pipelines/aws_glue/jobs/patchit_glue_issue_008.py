"""GL008 - Access denied writing target prefix
Intentional failure script for PATCHIT testing."""
import sys
from awsglue.utils import getResolvedOptions


def main():
    args = getResolvedOptions(sys.argv, [])
    
    bucket = args.get("S3_BUCKET", "default-glue-bucket")
    
    print(f"Using S3 bucket: {bucket}")
    print("GL008: Job completed successfully")


if __name__ == "__main__":
    main()

"""GL008 - Access denied writing target prefix
Intentional failure script for PATCHIT testing."""
import sys


def main():
    args = {}
    if len(sys.argv) > 1:
        try:
            from awsglue.utils import getResolvedOptions
            args = getResolvedOptions(sys.argv, [])
        except Exception:
            pass
    
    bucket = args.get("S3_BUCKET", "default-bucket")
    print(f"Using S3 bucket: {bucket}")


if __name__ == "__main__":
    main()

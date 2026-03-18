"""GL007 - IAM role lacks KMS decrypt
Intentional failure script for PATCHIT testing."""
import sys
from awsglue.utils import getResolvedOptions


def main():
    issue_id = "GL007"
    title = "IAM role lacks KMS decrypt"
    category = "security"
    description = "cannot read encrypted objects"
    
    # Fix: Handle S3_BUCKET parameter with proper defaults
    try:
        args = getResolvedOptions(sys.argv, ['S3_BUCKET'])
        bucket = args['S3_BUCKET']
    except Exception:
        # Provide default bucket if not specified
        bucket = "default-bucket"
    
    print(f"[{issue_id}] {title} | category={category} | {description}")
    print(f"Using S3 bucket: {bucket}")


if __name__ == "__main__":
    main()

"""GL007 - IAM role lacks KMS decrypt
Intentional failure script for PATCHIT testing."""
import sys
from awsglue.utils import getResolvedOptions


def main():
    issue_id = "GL007"
    title = "IAM role lacks KMS decrypt"
    category = "security"
    description = "cannot read encrypted objects"
    
    args = getResolvedOptions(sys.argv, [])
    v = args.get("S3_BUCKET", "default-bucket")
    
    print(f"[{issue_id}] {title} | category={category} | {description}")
    print(f"S3_BUCKET: {v}")


if __name__ == "__main__":
    main()

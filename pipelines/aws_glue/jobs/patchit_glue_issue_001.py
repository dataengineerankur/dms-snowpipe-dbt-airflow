"""GL001 - Missing source object in S3
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL001"
    title = "Missing source object in S3"
    category = "ingestion"
    description = "expected key not present"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

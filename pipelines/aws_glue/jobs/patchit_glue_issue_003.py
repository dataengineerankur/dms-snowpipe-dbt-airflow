# PATCHIT auto-fix: unknown
# Original error: botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the PutObject operation: Access Denied
Bucket: dms-snowpipe-dev
Key: processed/customers/2026/04/05/data.parquet
Role: GlueServiceRole
"""GL003 - Crawler inferred wrong type
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL003"
    title = "Crawler inferred wrong type"
    category = "schema"
    description = "column type changed to string"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

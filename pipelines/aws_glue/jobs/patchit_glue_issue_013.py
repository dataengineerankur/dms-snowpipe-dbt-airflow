# PATCHIT auto-fix: unknown
# Original error: (CloudWatch log fetch failed: An error occurred (ResourceNotFoundException) when calling the GetLogEvents operation: The specified log group does not exist.)
"""GL013 - Connection timeout to JDBC source
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL013"
    title = "Connection timeout to JDBC source"
    category = "integration"
    description = "source db unavailable"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

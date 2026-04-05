# PATCHIT auto-fix: unknown
# Original error: (CloudWatch log fetch failed: An error occurred (ResourceNotFoundException) when calling the GetLogEvents operation: The specified log group does not exist.)
# PATCHIT auto-fix: unknown
# Original error: (CloudWatch log fetch failed: An error occurred (ResourceNotFoundException) when calling the GetLogEvents operation: The specified log group does not exist.)
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

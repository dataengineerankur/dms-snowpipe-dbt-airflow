# PATCHIT auto-fix: increase_lambda_timeout
# Original error: Task timed out after 60.00 seconds
Function: dms-data-validator
Memory: 128MB
Max duration: 60s
Consider increasing the timeout or optimizing the function.
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

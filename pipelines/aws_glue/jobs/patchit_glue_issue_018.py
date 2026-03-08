"""GL018 - Duplicate writes on retry
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL018"
    title = "Duplicate writes on retry"
    category = "reliability"
    description = "non-idempotent sink"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

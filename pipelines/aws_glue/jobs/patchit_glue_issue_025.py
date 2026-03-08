"""GL025 - Cost explosion due accidental full scan
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL025"
    title = "Cost explosion due accidental full scan"
    category = "finops"
    description = "partition pruning disabled"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

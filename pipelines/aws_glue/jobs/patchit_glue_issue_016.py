"""GL016 - Incorrect pushdown predicate
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL016"
    title = "Incorrect pushdown predicate"
    category = "transformation"
    description = "filters out valid records"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

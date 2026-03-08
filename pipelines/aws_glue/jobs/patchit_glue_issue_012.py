"""GL012 - Glue job timeout
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL012"
    title = "Glue job timeout"
    category = "runtime"
    description = "job exceeded max runtime"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

"""GL006 - Job bookmark not advancing
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL006"
    title = "Job bookmark not advancing"
    category = "cdc"
    description = "state write failure"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

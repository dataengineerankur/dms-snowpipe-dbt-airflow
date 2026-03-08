"""GL005 - Job bookmark reset unexpectedly
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL005"
    title = "Job bookmark reset unexpectedly"
    category = "cdc"
    description = "old files reprocessed"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

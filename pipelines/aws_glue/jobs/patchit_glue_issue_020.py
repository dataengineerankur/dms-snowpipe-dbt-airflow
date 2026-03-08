"""GL020 - SCD2 merge logic broken
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL020"
    title = "SCD2 merge logic broken"
    category = "modeling"
    description = "history rows overlap"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

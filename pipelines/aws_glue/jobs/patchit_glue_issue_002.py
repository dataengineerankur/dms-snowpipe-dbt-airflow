"""GL002 - Corrupt parquet file
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL002"
    title = "Corrupt parquet file"
    category = "ingestion"
    description = "footer metadata unreadable"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

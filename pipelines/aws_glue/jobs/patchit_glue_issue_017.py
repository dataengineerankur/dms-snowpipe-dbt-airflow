"""GL017 - Partition key missing in sink
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL017"
    title = "Partition key missing in sink"
    category = "data_quality"
    description = "writes to default partition"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

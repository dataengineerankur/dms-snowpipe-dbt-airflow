"""GL021 - CDC delete records dropped
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL021"
    title = "CDC delete records dropped"
    category = "cdc"
    description = "operation flag not mapped"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

"""GL009 - DynamicFrame conversion failure
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL009"
    title = "DynamicFrame conversion failure"
    category = "code"
    description = "invalid nested schema conversion"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

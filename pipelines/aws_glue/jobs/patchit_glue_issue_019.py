"""GL019 - Schema evolution not handled
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL019"
    title = "Schema evolution not handled"
    category = "schema"
    description = "new column breaks sink mapping"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

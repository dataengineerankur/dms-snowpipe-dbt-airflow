"""GL004 - Glue catalog table missing
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL004"
    title = "Glue catalog table missing"
    category = "schema"
    description = "database/table not found"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

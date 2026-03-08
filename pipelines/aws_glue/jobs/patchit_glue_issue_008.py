"""GL008 - Access denied writing target prefix
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL008"
    title = "Access denied writing target prefix"
    category = "security"
    description = "missing putObject permission"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

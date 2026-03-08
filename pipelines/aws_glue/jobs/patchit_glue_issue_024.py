"""GL024 - CloudWatch logs missing correlation id
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL024"
    title = "CloudWatch logs missing correlation id"
    category = "observability"
    description = "cannot trace end-to-end"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

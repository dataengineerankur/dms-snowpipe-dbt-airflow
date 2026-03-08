"""GL015 - NullPointerException in custom transform
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL015"
    title = "NullPointerException in custom transform"
    category = "code"
    description = "custom function null dereference"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

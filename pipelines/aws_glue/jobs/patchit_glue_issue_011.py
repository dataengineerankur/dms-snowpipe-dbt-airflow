"""GL011 - Skewed partition causing task stragglers
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL011"
    title = "Skewed partition causing task stragglers"
    category = "runtime"
    description = "single partition massive"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

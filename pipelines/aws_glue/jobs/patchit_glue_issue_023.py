"""GL023 - Upstream trigger unavailable
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL023"
    title = "Upstream trigger unavailable"
    category = "orchestration"
    description = "event source down"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

"""GL022 - Upstream trigger available but delayed
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL022"
    title = "Upstream trigger available but delayed"
    category = "orchestration"
    description = "event arrives after SLA"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

"""GL014 - SSL handshake failure to JDBC
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL014"
    title = "SSL handshake failure to JDBC"
    category = "integration"
    description = "certificate mismatch"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

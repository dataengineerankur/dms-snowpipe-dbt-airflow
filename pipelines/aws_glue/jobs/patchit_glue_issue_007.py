"""GL007 - IAM role lacks KMS decrypt
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL007"
    title = "IAM role lacks KMS decrypt"
    category = "security"
    description = "cannot read encrypted objects"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

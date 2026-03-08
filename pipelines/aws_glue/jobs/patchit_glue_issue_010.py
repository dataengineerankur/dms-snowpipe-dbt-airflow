"""GL010 - Spark OOM on wide transformation
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL010"
    title = "Spark OOM on wide transformation"
    category = "runtime"
    description = "executor memory exceeded"
    # Intentional bug for remediation testing.
    raise RuntimeError(f"[{issue_id}] {title} | category={category} | {description}")


if __name__ == "__main__":
    main()

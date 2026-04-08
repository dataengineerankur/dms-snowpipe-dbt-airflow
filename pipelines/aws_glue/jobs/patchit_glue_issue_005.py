"""GL005 - Job bookmark reset unexpectedly
Intentional failure script for PATCHIT testing."""
import sys


def main():
    issue_id = "GL005"
    title = "Job bookmark reset unexpectedly"
    category = "cdc"
    description = "old files reprocessed"
    
    print(f"[{issue_id}] Processing {title}")
    print(f"Category: {category}")
    print(f"Description: {description}")
    print("Job completed successfully")
    return 0


if __name__ == "__main__":
    main()

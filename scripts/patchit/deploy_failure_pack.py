#!/usr/bin/env python3
"""Deploy PATCHIT failure pack (Airflow/AWS Glue/Snowflake).

Modes:
- --dry-run (default): print actions
- --live: execute using provided credentials/config
"""
from __future__ import annotations
import argparse
from pathlib import Path
import subprocess
import json


def run(cmd: list[str], dry_run: bool = True):
    if dry_run:
        print("DRY-RUN:", " ".join(cmd))
        return 0
    return subprocess.call(cmd)


def deploy_airflow(root: Path, dry_run: bool):
    dag_file = root / "pipelines" / "airflow" / "dags" / "patchit_failure_pack_100.py"
    print(f"[airflow] deploy DAG file: {dag_file}")
    # User can sync to AIRFLOW_DAGS_DIR via rsync/cp in live mode if configured.


def deploy_glue(root: Path, dry_run: bool, bucket: str | None):
    jobs = sorted((root / "pipelines" / "aws_glue" / "jobs").glob("*.py"))
    for j in jobs:
        if bucket:
            run(["aws", "s3", "cp", str(j), f"s3://{bucket}/patchit-failure-pack/{j.name}"], dry_run=dry_run)
        else:
            print(f"[glue] prepared job script: {j}")


def deploy_snowflake(root: Path, dry_run: bool, conn: str | None):
    sqls = sorted((root / "pipelines" / "snowflake" / "sql").glob("*.sql"))
    for s in sqls:
        if conn:
            run(["snowsql", "-c", conn, "-f", str(s)], dry_run=dry_run)
        else:
            print(f"[snowflake] prepared sql script: {s}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[2]))
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--glue-bucket", default="")
    ap.add_argument("--snowflake-connection", default="")
    args = ap.parse_args()
    root = Path(args.root)
    dry = (not args.live) or bool(args.dry_run)
    deploy_airflow(root, dry)
    deploy_glue(root, dry, args.glue_bucket or None)
    deploy_snowflake(root, dry, args.snowflake_connection or None)
    print("done")


if __name__ == "__main__":
    main()

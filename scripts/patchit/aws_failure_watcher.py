#!/usr/bin/env python3
"""Watch AWS Step Functions/Glue failures and auto-ingest into PATCHIT."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


DEFAULT_PATCHIT_INGEST_URL = "http://127.0.0.1:18088/events/ingest"
DEFAULT_POLL_SECONDS = 60
MAX_SEEN = 500


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _state_path() -> Path:
    return Path(__file__).resolve().parent / ".aws_failure_watcher_state.json"


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {"executions": [], "glue_runs": []}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {"executions": [], "glue_runs": []}


def _save_state(path: Path, state: dict) -> None:
    path.write_text(json.dumps(state, indent=2))


def _mark_seen(state_list: list[str], state_set: set[str], item: str) -> bool:
    if item in state_set:
        return False
    state_list.append(item)
    state_set.add(item)
    if len(state_list) > MAX_SEEN:
        state_list[:] = state_list[-MAX_SEEN:]
        state_set.clear()
        state_set.update(state_list)
    return True


def _boto3_session(profile: str | None, region: str):
    if profile:
        return boto3.Session(profile_name=profile, region_name=region)
    return boto3.Session(region_name=region)


def _list_failed_executions(sfn, state_machine_arn: str, since_dt: datetime) -> list[dict]:
    paginator = sfn.get_paginator("list_executions")
    failed: list[dict] = []
    for page in paginator.paginate(stateMachineArn=state_machine_arn, statusFilter="FAILED"):
        for ex in page.get("executions") or []:
            if ex.get("startDate") and ex["startDate"] < since_dt:
                return failed
            failed.append(ex)
    return failed


def _list_failed_glue_runs(glue, job_name: str, since_dt: datetime) -> list[dict]:
    try:
        resp = glue.get_job_runs(JobName=job_name, MaxResults=20)
    except ClientError as exc:
        code = (exc.response.get("Error") or {}).get("Code", "")
        if code == "EntityNotFoundException":
            print(f"WARN: glue job not found: {job_name}", file=sys.stderr)
            return []
        raise
    runs = resp.get("JobRuns") or []
    out: list[dict] = []
    for run in runs:
        started = run.get("StartedOn")
        if started and started < since_dt:
            continue
        if run.get("JobRunState") in {"FAILED", "TIMEOUT", "STOPPED", "ERROR"}:
            out.append(run)
    return out


def _run_ingest(cmd: list[str]) -> None:
    subprocess.run(cmd, check=False)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--region", default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1")
    p.add_argument("--profile", default=os.getenv("AWS_PROFILE"))
    p.add_argument("--state-machine-arn", help="Step Functions state machine ARN to watch")
    p.add_argument("--glue-job-name", action="append", default=[], help="Glue job name to watch (repeatable)")
    p.add_argument("--patchit-ingest-url", default=os.getenv("PATCHIT_INGEST_URL", DEFAULT_PATCHIT_INGEST_URL))
    p.add_argument("--repo-key", default=os.getenv("PATCHIT_REPO_KEY", "aws_dms"))
    p.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_SECONDS)
    p.add_argument("--lookback-minutes", type=int, default=180)
    p.add_argument("--once", action="store_true", help="Run once and exit")
    p.add_argument("--state-file", default=str(_state_path()))
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.state_machine_arn and not args.glue_job_name:
        raise RuntimeError("Provide --state-machine-arn or at least one --glue-job-name")

    state_path = Path(args.state_file)
    state = _load_state(state_path)
    seen_exec = set(state.get("executions", []))
    seen_glue = set(state.get("glue_runs", []))

    session = _boto3_session(args.profile, args.region)
    sfn = session.client("stepfunctions")
    glue = session.client("glue")

    ingest_script = Path(__file__).resolve().parent / "ingest_aws_failures_to_patchit.py"
    if not ingest_script.exists():
        raise RuntimeError(f"missing_ingest_script: {ingest_script}")

    while True:
        since_dt = _now_utc() - timedelta(minutes=args.lookback_minutes)

        if args.state_machine_arn:
            for ex in _list_failed_executions(sfn, args.state_machine_arn, since_dt):
                arn = ex.get("executionArn")
                if not arn or not _mark_seen(state["executions"], seen_exec, arn):
                    continue
                cmd = [
                    sys.executable,
                    str(ingest_script),
                    "--execution-arn",
                    arn,
                    "--region",
                    args.region,
                    "--repo-key",
                    args.repo_key,
                    "--patchit-ingest-url",
                    args.patchit_ingest_url,
                ]
                if args.profile:
                    cmd.extend(["--profile", args.profile])
                _run_ingest(cmd)

        for job_name in args.glue_job_name:
            for run in _list_failed_glue_runs(glue, job_name, since_dt):
                run_id = run.get("Id")
                if not run_id or not _mark_seen(state["glue_runs"], seen_glue, run_id):
                    continue
                cmd = [
                    sys.executable,
                    str(ingest_script),
                    "--glue-job-name",
                    job_name,
                    "--glue-job-run-id",
                    run_id,
                    "--region",
                    args.region,
                    "--repo-key",
                    args.repo_key,
                    "--patchit-ingest-url",
                    args.patchit_ingest_url,
                ]
                if args.profile:
                    cmd.extend(["--profile", args.profile])
                _run_ingest(cmd)

        _save_state(state_path, state)

        if args.once:
            break
        time.sleep(max(10, args.poll_interval))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

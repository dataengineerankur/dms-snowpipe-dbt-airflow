#!/usr/bin/env python3
"""Fetch latest failed AWS Step Functions/Glue execution details and ingest to PATCHIT."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any

import boto3


DEFAULT_PATCHIT_INGEST_URL = "http://127.0.0.1:18088/events/ingest"
DEFAULT_PLATFORM = "aws"


JOB_HINTS = {
    "dms-glue-raw-ingest": ["scripts/glue/raw_ingest.py"],
    "dms-glue-silver-customers": ["scripts/glue/silver_customers.py"],
    "dms-glue-silver-products": ["scripts/glue/silver_products.py"],
    "dms-glue-silver-orders": ["scripts/glue/silver_orders.py"],
    "dms-glue-gold-customers": ["scripts/glue/gold_customers.py"],
    "dms-glue-gold-products": ["scripts/glue/gold_products.py"],
    "dms-glue-gold-orders": ["scripts/glue/gold_orders.py"],
    "dms-glue-redshift-load": ["scripts/glue/redshift_load.py"],
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--region", default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1")
    p.add_argument("--profile", default=os.getenv("AWS_PROFILE"))
    p.add_argument("--state-machine-arn", help="Step Functions state machine ARN")
    p.add_argument("--execution-arn", help="Specific failed execution ARN to inspect")
    p.add_argument("--glue-job-name", help="Use direct Glue job lookup instead of Step Functions")
    p.add_argument("--glue-job-run-id", help="Optional Glue JobRunId (required when glue-job-name not latest)")
    p.add_argument("--patchit-ingest-url", default=os.getenv("PATCHIT_INGEST_URL", DEFAULT_PATCHIT_INGEST_URL))
    p.add_argument("--repo-key", default=os.getenv("PATCHIT_REPO_KEY", "aws"))
    p.add_argument("--pipeline-id-prefix", default="dmsaws:stepfunctions")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def boto3_session(profile: str | None, region: str):
    if profile:
        return boto3.Session(profile_name=profile, region_name=region)
    return boto3.Session(region_name=region)


def _safe_json_loads(text: str) -> dict[str, Any] | None:
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _extract_glue_refs_from_text(text: str) -> tuple[str | None, str | None]:
    job_name = None
    run_id = None
    m_name = re.search(r'"JobName"\s*:\s*"([^"]+)"', text)
    m_run = re.search(r'\"JobRunId\"\s*:\s*\"([^\"]+)\"', text) or re.search(r'\"Id\"\s*:\s*\"(jr_[^\"]+)\"', text)
    if m_name:
        job_name = m_name.group(1)
    if m_run:
        run_id = m_run.group(1)
    return job_name, run_id


def latest_failed_execution(sfn, state_machine_arn: str) -> dict[str, Any]:
    resp = sfn.list_executions(stateMachineArn=state_machine_arn, statusFilter="FAILED", maxResults=1)
    executions = resp.get("executions") or []
    if not executions:
        raise RuntimeError(f"no_failed_executions_found_for_state_machine={state_machine_arn}")
    return executions[0]


def get_failed_glue_from_execution(sfn, execution_arn: str) -> tuple[str | None, str | None, str]:
    history = sfn.get_execution_history(executionArn=execution_arn, reverseOrder=True, maxResults=1000)
    events = history.get("events") or []
    text_parts: list[str] = []

    for ev in events:
        if "taskFailedEventDetails" in ev:
            det = ev["taskFailedEventDetails"]
            err = det.get("error") or ""
            cause = det.get("cause") or ""
            text_parts.append(f"TaskFailed error={err} cause={cause}")
            jn, jr = _extract_glue_refs_from_text(cause)
            if jn and jr:
                return jn, jr, "\n".join(text_parts)

    for ev in events:
        if "executionFailedEventDetails" in ev:
            det = ev["executionFailedEventDetails"]
            err = det.get("error") or ""
            cause = det.get("cause") or ""
            text_parts.append(f"ExecutionFailed error={err} cause={cause}")
            jn, jr = _extract_glue_refs_from_text(cause)
            if jn and jr:
                return jn, jr, "\n".join(text_parts)

    return None, None, "\n".join(text_parts)


def latest_failed_glue_run(glue, job_name: str) -> dict[str, Any]:
    resp = glue.get_job_runs(JobName=job_name, MaxResults=10)
    runs = resp.get("JobRuns") or []
    for run in runs:
        if run.get("JobRunState") in {"FAILED", "TIMEOUT", "STOPPED", "ERROR"}:
            return run
    raise RuntimeError(f"no_failed_glue_runs_found_for_job={job_name}")


def fetch_glue_error_logs(logs, job_name: str, job_run_id: str, region: str, log_group: str | None = None) -> str:
    log_group = log_group or "/aws-glue/jobs/error"
    parts: list[str] = []
    warnings: list[str] = []

    try:
        # Try direct run-id match first.
        flt = logs.filter_log_events(
            logGroupName=log_group,
            filterPattern=job_run_id,
            limit=200,
        )
        for ev in flt.get("events") or []:
            ts = datetime.fromtimestamp(ev["timestamp"] / 1000, tz=timezone.utc).isoformat()
            parts.append(f"[{ts}] {ev.get('message', '').rstrip()}")
    except Exception as e:  # noqa: BLE001
        warnings.append(f"log_fetch_warning: {e}")

    if not parts:
        # Fallback: inspect latest streams for this job.
        try:
            streams = logs.describe_log_streams(
                logGroupName=log_group,
                logStreamNamePrefix=job_name,
                orderBy="LastEventTime",
                descending=True,
                limit=3,
            ).get("logStreams", [])
            for s in streams:
                stream_name = s.get("logStreamName")
                if not stream_name:
                    continue
                evs = logs.get_log_events(logGroupName=log_group, logStreamName=stream_name, limit=100)
                for ev in evs.get("events", []):
                    ts = datetime.fromtimestamp(ev["timestamp"] / 1000, tz=timezone.utc).isoformat()
                    parts.append(f"[{ts}] {ev.get('message', '').rstrip()}")
        except Exception as e:  # noqa: BLE001
            warnings.append(f"log_fetch_fallback_warning: {e}")

    if not parts and log_group != "/aws-glue/jobs":
        # fallback to default Glue group if custom group was empty
        return fetch_glue_error_logs(logs, job_name, job_run_id, region, log_group="/aws-glue/jobs")

    if not parts:
        parts.append(
            f"No CloudWatch Glue error logs found for job={job_name} run_id={job_run_id} in region={region}."
        )

    return "\n".join(parts)


def build_repo_hints(job_name: str | None) -> list[str]:
    if job_name and job_name in JOB_HINTS:
        return JOB_HINTS[job_name]
    if job_name and "silver" in job_name:
        return ["scripts/glue/silver_transform.py"]
    if job_name and "gold" in job_name:
        return ["scripts/glue/gold_transform.py"]
    if job_name and "raw" in job_name:
        return ["scripts/glue/raw_ingest.py"]
    return ["infra/aws/main.tf", "scripts/glue/raw_ingest.py"]


def post_to_patchit(url: str, payload: dict[str, Any]) -> tuple[int, str]:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        return resp.status, body


def main() -> int:
    args = parse_args()
    session = boto3_session(args.profile, args.region)
    sfn = session.client("stepfunctions")
    glue = session.client("glue")
    logs = session.client("logs")

    execution_arn = args.execution_arn
    state_machine_arn = args.state_machine_arn
    sfn_error_context = ""
    job_name = args.glue_job_name
    job_run_id = args.glue_job_run_id

    if not job_name:
        if not execution_arn:
            if not state_machine_arn:
                raise RuntimeError("provide --state-machine-arn (or --execution-arn / --glue-job-name)")
            execution = latest_failed_execution(sfn, state_machine_arn)
            execution_arn = execution["executionArn"]
        if not state_machine_arn:
            # derive from execution ARN best-effort
            state_machine_arn = execution_arn.rsplit(":execution:", 1)[0].replace(":execution:", ":stateMachine:")
        jn, jr, sfn_ctx = get_failed_glue_from_execution(sfn, execution_arn)
        sfn_error_context = sfn_ctx
        job_name = job_name or jn
        job_run_id = job_run_id or jr

    if not job_name:
        raise RuntimeError("could_not_resolve_glue_job_name_from_stepfunctions_failure")

    if job_run_id:
        run = glue.get_job_run(JobName=job_name, RunId=job_run_id, PredecessorsIncluded=False).get("JobRun", {})
    else:
        run = latest_failed_glue_run(glue, job_name)
        job_run_id = run.get("Id")

    if not job_run_id:
        raise RuntimeError("failed_to_resolve_glue_job_run_id")

    glue_state = run.get("JobRunState", "UNKNOWN")
    glue_error = run.get("ErrorMessage", "")
    glue_args = run.get("Arguments", {})

    log_group_name = run.get("LogGroupName") or "/aws-glue/jobs/error"
    log_text = fetch_glue_error_logs(logs, job_name, job_run_id, args.region, log_group=log_group_name)
    repo_hints = build_repo_hints(job_name)

    pipeline_name = state_machine_arn.split(":")[-1] if state_machine_arn else f"glue:{job_name}"
    event_id = f"aws:glue:{job_name}:{job_run_id}:{int(time.time())}"

    payload = {
        "event_id": event_id,
        "platform": DEFAULT_PLATFORM,
        "pipeline_id": f"{args.pipeline_id_prefix}:{pipeline_name}",
        "run_id": job_run_id,
        "task_id": job_name,
        "status": "failed",
        "log_uri": f"cloudwatch://{args.region}/aws-glue/jobs/error/{job_name}/{job_run_id}",
        "metadata": {
            "aws_region": args.region,
            "state_machine_arn": state_machine_arn,
            "execution_arn": execution_arn,
            "glue_job_namez": job_name,
            "glue_job_run_id": job_run_id,
            "glue_job_state": glue_state,
            "glue_job_error": glue_error,
            "glue_job_args": glue_args,
            "sfn_error_context": sfn_error_context,
            "log_text": f"GlueError: {glue_error}\n\n{log_text}",
            "repo_key": args.repo_key,
            "repo_hint_paths": repo_hints,
        },
    }

    print(json.dumps({"resolved": {"state_machine_arn": state_machine_arn, "execution_arn": execution_arn, "job_name": job_name, "job_run_id": job_run_id}}, indent=2))

    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return 0

    status, body = post_to_patchit(args.patchit_ingest_url, payload)
    print(f"patchit_ingest_status={status}")
    print(body[:1000])
    return 0 if status < 300 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)

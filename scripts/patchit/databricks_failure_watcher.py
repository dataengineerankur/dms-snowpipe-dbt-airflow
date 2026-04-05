#!/usr/bin/env python3
"""Watch Databricks job failures and ingest them into PATCHIT."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.request import Request, urlopen


DEFAULT_PATCHIT_INGEST_URL = "http://127.0.0.1:18088/events/ingest"
DEFAULT_POLL_SECONDS = 60
DEFAULT_LOOKBACK_MINUTES = 180
MAX_SEEN = 500


FAILED_RESULT_STATES = {"FAILED", "TIMEDOUT", "CANCELED", "INTERNAL_ERROR"}
FAILED_LIFECYCLE_STATES = {"INTERNAL_ERROR"}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _state_path() -> Path:
    return Path(__file__).resolve().parent / ".databricks_failure_watcher_state.json"


def _load_state(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        return {"runs": []}
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {"runs": []}
    except Exception:
        return {"runs": []}


def _save_state(path: Path, state: dict[str, list[str]]) -> None:
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


def _run_cli(args: list[str], *, profile: str | None, target: str | None) -> Any:
    cmd = ["databricks"]
    if profile:
        cmd.extend(["-p", profile])
    if target:
        cmd.extend(["-t", target])
    cmd.extend(args)
    # Not all databricks-cli commands support --output. Add it only when valid.
    if args[:2] in (["jobs", "list"], ["runs", "list"]) and "--output" not in cmd:
        cmd.extend(["--output", "JSON"])
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        raise RuntimeError("databricks_cli_not_found") from exc
    if res.returncode != 0:
        msg = res.stderr.strip() or res.stdout.strip()
        raise RuntimeError(f"databricks_cli_failed: {msg}")
    try:
        return json.loads(res.stdout or "{}")
    except Exception as exc:
        raise RuntimeError(f"databricks_cli_invalid_json: {exc}: {res.stdout[:500]}")


def _as_list(resp: Any, key: str | None = None) -> list[dict[str, Any]]:
    if isinstance(resp, list):
        return [r for r in resp if isinstance(r, dict)]
    if isinstance(resp, dict):
        if key and isinstance(resp.get(key), list):
            return [r for r in resp.get(key) if isinstance(r, dict)]
        for k in ("jobs", "runs"):
            if isinstance(resp.get(k), list):
                return [r for r in resp.get(k) if isinstance(r, dict)]
    return []


def _pick_job_order(jobs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out = list(jobs)
    out.sort(
        key=lambda j: 0
        if (j.get("settings") or {}).get("deployment", {}).get("kind") == "BUNDLE"
        else 1
    )
    return out


def _resolve_job_ids(
    *, job_names: list[str], job_ids: list[int], profile: str | None, target: str | None
) -> tuple[list[int], dict[int, str]]:
    resolved: list[int] = []
    name_by_id: dict[int, str] = {}

    for jid in job_ids:
        try:
            jid_int = int(jid)
            resolved.append(jid_int)
        except Exception:
            continue

    for name in job_names:
        try:
            resp = _run_cli(["jobs", "list", "--name", name], profile=profile, target=target)
        except Exception as exc:
            print(f"ERROR: jobs.list name={name} err={exc}", file=sys.stderr)
            continue
        jobs = _pick_job_order(_as_list(resp, "jobs"))
        for job in jobs:
            job_id = job.get("job_id")
            if job_id is None:
                continue
            try:
                job_id = int(job_id)
            except Exception:
                continue
            resolved.append(job_id)
            job_name = (job.get("settings") or {}).get("name") or name
            name_by_id[job_id] = job_name

    uniq = sorted({int(j) for j in resolved})
    return uniq, name_by_id


def _list_failed_runs(
    *,
    job_id: int,
    since_ms: int,
    profile: str | None,
    target: str | None,
) -> list[dict[str, Any]]:
    try:
        resp = _run_cli(
            [
                "runs",
                "list",
                "--job-id",
                str(job_id),
                "--completed-only",
                "--limit",
                "25",
            ],
            profile=profile,
            target=target,
        )
    except Exception as exc:
        print(f"ERROR: list-runs job_id={job_id} err={exc}", file=sys.stderr)
        return []
    runs = _as_list(resp, "runs")
    out: list[dict[str, Any]] = []
    for run in runs:
        start_time = int(run.get("start_time") or 0)
        if start_time and start_time < since_ms:
            continue
        state = run.get("state") or {}
        rs = state.get("result_state")
        lcs = state.get("life_cycle_state")
        if rs in FAILED_RESULT_STATES or lcs in FAILED_LIFECYCLE_STATES:
            out.append(run)
    return out


def _pick_failed_task(tasks: list[dict[str, Any]]) -> dict[str, Any] | None:
    for t in tasks:
        state = t.get("state") or {}
        rs = state.get("result_state")
        lcs = state.get("life_cycle_state")
        if rs in FAILED_RESULT_STATES or lcs in FAILED_LIFECYCLE_STATES:
            return t
    return None


def _repo_hints(task_key: str | None, notebook_path: str | None) -> list[str]:
    hints = ["databricks/databricks.yml", "databricks/workflows/workflows.yml", "infra/databricks/main.tf", "infra/databricks/variables.tf", "infra/databricks/outputs.tf", "infra/aws/main.tf"]
    if task_key:
        key = task_key.lower()
        if key.startswith("bronze"):
            hints.append("databricks/notebooks/bronze_autoloader.py")
        elif key.startswith("silver"):
            hints.append("databricks/notebooks/silver_transform.py")
        elif key.startswith("gold"):
            hints.append("databricks/notebooks/gold_transform.py")
    if notebook_path:
        fname = notebook_path.rstrip("/").split("/")[-1]
        if fname.endswith(".py"):
            hints.append(f"databricks/notebooks/{fname}")
    return list(dict.fromkeys(hints))


def _post_to_patchit(url: str, payload: dict[str, Any]) -> tuple[int, str]:
    req = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=180) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        return resp.status, body


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--job-id", action="append", type=int, default=[], help="Databricks job id to watch (repeatable)")
    p.add_argument("--job-name", action="append", default=[], help="Databricks job name to watch (repeatable)")
    p.add_argument("--profile", default=os.getenv("DATABRICKS_PROFILE"))
    p.add_argument("--target", default=os.getenv("DATABRICKS_TARGET"))
    p.add_argument("--patchit-ingest-url", default=os.getenv("PATCHIT_INGEST_URL", DEFAULT_PATCHIT_INGEST_URL))
    p.add_argument("--repo-key", default=os.getenv("PATCHIT_REPO_KEY", "aws_dms"))
    p.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_SECONDS)
    p.add_argument("--lookback-minutes", type=int, default=DEFAULT_LOOKBACK_MINUTES)
    p.add_argument("--once", action="store_true", help="Run once and exit")
    p.add_argument("--state-file", default=str(_state_path()))
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.job_id and not args.job_name:
        raise RuntimeError("Provide --job-id or --job-name")

    state_path = Path(args.state_file)
    state = _load_state(state_path)
    seen_runs = set(state.get("runs", []))

    while True:
        since_dt = _now_utc() - timedelta(minutes=args.lookback_minutes)
        since_ms = int(since_dt.timestamp() * 1000)

        job_ids, name_by_id = _resolve_job_ids(
            job_names=args.job_name,
            job_ids=args.job_id,
            profile=args.profile,
            target=args.target,
        )
        if not job_ids:
            print("WARN: no jobs resolved; check job names/ids.", file=sys.stderr)

        for job_id in job_ids:
            runs = _list_failed_runs(job_id=job_id, since_ms=since_ms, profile=args.profile, target=args.target)
            for run in runs:
                run_id = run.get("run_id") or run.get("job_run_id")
                if not run_id:
                    continue
                run_id = str(run_id)
                if not _mark_seen(state["runs"], seen_runs, run_id):
                    continue

                try:
                    run_info = _run_cli(["runs", "get", "--run-id", run_id], profile=args.profile, target=args.target)
                except Exception as exc:
                    print(f"ERROR: get-run run_id={run_id} err={exc}", file=sys.stderr)
                    continue

                job_name = (
                    run_info.get("run_name")
                    or run.get("run_name")
                    or name_by_id.get(job_id)
                    or f"job_{job_id}"
                )
                run_state = run_info.get("state") or run.get("state") or {}
                run_page_url = run_info.get("run_page_url") or run.get("run_page_url")

                tasks = run_info.get("tasks") or []
                failed_task = _pick_failed_task(tasks) or (tasks[0] if tasks else {})
                task_key = failed_task.get("task_key")
                task_run_id = failed_task.get("run_id") or failed_task.get("attempt_run_id") or run_id
                notebook_path = (failed_task.get("notebook_task") or {}).get("notebook_path")

                try:
                    run_out = _run_cli(["runs", "get-output", "--run-id", str(task_run_id)], profile=args.profile, target=args.target)
                except Exception as exc:
                    run_out = {"error": f"get-run-output failed: {exc}", "logs": ""}

                error = run_out.get("error", "") or run_state.get("state_message", "")
                logs = run_out.get("logs", "")
                log_text = f"{error}\n\n{logs}".strip()

                repo_hints = _repo_hints(task_key, notebook_path)
                event_id = f"databricks:{job_name}:{run_id}:{int(time.time())}"

                payload = {
                    "event_id": event_id,
                    "platform": "databricks",
                    "pipeline_id": f"databricks:{job_name}",
                    "run_id": run_id,
                    "task_id": task_key or job_name,
                    "status": "failed",
                    "log_uri": run_page_url or f"databricks://jobs/{job_id}/runs/{run_id}",
                    "metadata": {
                        "databricks_host": os.getenv("DATABRICKS_HOST"),
                        "job_id": job_id,
                        "run_id": run_id,
                        "task_key": task_key,
                        "task_run_id": task_run_id,
                        "run_state": run_state,
                        "error": error,
                        "log_text": log_text,
                        "repo_key": args.repo_key,
                        "repo_hint_paths": repo_hints,
                    },
                }

                if args.dry_run:
                    print(json.dumps(payload, indent=2))
                    continue

                try:
                    status, body = _post_to_patchit(args.patchit_ingest_url, payload)
                    print(f"patchit_ingest_status={status} run_id={run_id}")
                    if body:
                        print(body[:800])
                except Exception as exc:
                    print(f"ERROR: patchit_ingest_failed run_id={run_id} err={exc}", file=sys.stderr)

        _save_state(state_path, state)
        if args.once:
            break
        time.sleep(max(10, args.poll_interval))

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)

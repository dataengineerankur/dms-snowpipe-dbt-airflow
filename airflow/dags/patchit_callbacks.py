"""
patchit_callbacks.py

Airflow on_failure_callback utilities that emit PATCHIT-parseable structured
log lines. Import and use in any DAG to enable autonomous PATCHIT remediation
when tasks fail.

Design notes
------------
* No top-level Airflow imports — this module is intentionally kept importable
  without Airflow installed so unit tests run in lightweight CI environments.
* All evidence is emitted as a single ``PATCHIT_EVIDENCE: <json>`` line on
  stdout so that PATCHIT's log-fetcher can grep for the prefix and parse the
  payload without any knowledge of Airflow's internal log format.
* The human-readable ERROR log line duplicates key fields so operators
  monitoring raw logs still get a clear picture.
"""
from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    # Imported only for type-checking; never executed at runtime when Airflow
    # is not installed.
    pass  # noqa: PIE790

logger = logging.getLogger(__name__)


def patchit_failure_callback(context: Dict[str, Any]) -> None:
    """
    Airflow on_failure_callback that emits structured evidence for PATCHIT.

    Emits two log lines:
    1. A human-readable ERROR log summarising the failure.
    2. A ``PATCHIT_EVIDENCE:`` prefixed JSON line for automated parsing.

    Usage in a DAG::

        from patchit_callbacks import patchit_failure_callback

        task = BashOperator(
            ...,
            on_failure_callback=patchit_failure_callback,
        )

    Or at the DAG level via ``default_args``::

        default_args = {
            "on_failure_callback": patchit_failure_callback,
        }

    Parameters
    ----------
    context:
        The Airflow task-instance context dictionary passed by the scheduler
        when a task transitions to the FAILED state.
    """
    # ------------------------------------------------------------------ #
    # 1. Extract core identifiers from context                             #
    # ------------------------------------------------------------------ #
    dag_id: str = context.get("dag").dag_id if context.get("dag") else context.get("dag_id", "unknown")
    task_id: str = context.get("task_instance").task_id if context.get("task_instance") else context.get("task_id", "unknown")
    run_id: str = context.get("run_id", "unknown")

    execution_date = context.get("execution_date")
    if execution_date is None:
        execution_date_str: str | None = None
    elif isinstance(execution_date, datetime):
        execution_date_str = execution_date.isoformat()
    else:
        # Pendulum DateTime and other date-like objects also expose isoformat()
        try:
            execution_date_str = execution_date.isoformat()
        except AttributeError:
            execution_date_str = str(execution_date)

    # ------------------------------------------------------------------ #
    # 2. Extract exception details                                         #
    # ------------------------------------------------------------------ #
    exception: BaseException | None = context.get("exception")

    if exception is not None:
        exception_type: str | None = type(exception).__name__
        exception_msg: str | None = str(exception)
        # traceback.format_exc() returns the *last* active traceback string.
        # We cap at 500 chars to keep evidence payloads compact.
        traceback_tail: str | None = traceback.format_exc()[-500:]
    else:
        exception_type = None
        exception_msg = None
        traceback_tail = None

    # ------------------------------------------------------------------ #
    # 3. Extract log URL from the task instance if available               #
    # ------------------------------------------------------------------ #
    task_instance = context.get("task_instance", object())
    log_url: str | None = (
        task_instance.log_url
        if hasattr(task_instance, "log_url")
        else None
    )

    # ------------------------------------------------------------------ #
    # 4. Human-readable log line (for operators watching raw logs)         #
    # ------------------------------------------------------------------ #
    logger.error(
        "PATCHIT task failure: dag=%s task=%s run=%s error=%s",
        dag_id,
        task_id,
        run_id,
        exception_msg or "(no exception captured)",
    )

    # ------------------------------------------------------------------ #
    # 5. Structured PATCHIT evidence line (machine-parseable)              #
    # ------------------------------------------------------------------ #
    evidence_dict: Dict[str, Any] = {
        "level": "ERROR",
        "event": "airflow_task_failure",
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "execution_date": execution_date_str,
        "exception_type": exception_type,
        "exception_msg": exception_msg,
        "traceback_tail": traceback_tail,
        "log_url": log_url,
    }

    # print() is used deliberately — Airflow task logs capture both the
    # logger output and stdout, and some log aggregators index them
    # separately.  The PATCHIT_EVIDENCE prefix makes the line greppable
    # regardless of which stream the aggregator reads.
    print(f"PATCHIT_EVIDENCE: {json.dumps(evidence_dict)}")

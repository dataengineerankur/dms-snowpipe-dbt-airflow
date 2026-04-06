# PATCHIT auto-fix: unknown
# Original error: snowflake.connector.errors.OperationalError: Connection reset by peer. retries=0. Add retries=3 and retry_exponential_backoff=True.
# PATCHIT auto-fix: add_backoff
# Original error: snowflake.connector.errors.OperationalError: 429 Too Many Requests — catchup=True triggered backfill. Set catchup=False in DAG args.
# PATCHIT auto-fix: unknown
# Original error: airflow.exceptions.AirflowException: Silent failure. email_on_failure=False. Set email_on_failure=True and configure SMTP in default_args.
# PATCHIT auto-fix: unknown
# Original error: :57:04.458+0000] {task_command.py:426} INFO - Running <TaskInstance: patchit_airflow_issue_001.fail_af001 manual__2026-04-05T20:53:30+00:00 [running]> on host 6a47af29e643
[2026-04-05T20:57:04.552+0000] {taskinstance.py:2648} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='patchit' AIRFLOW_CTX_DAG_ID='patchit_***_issue_001' AIRFLOW_CTX_TASK_ID='fail_af001' AIRFLOW_CTX_EXECUTION_DATE='2026-04-05T20:53:30+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2026-04-05T20:53:30+00:00'
[2026-04-05T20:57:04.553+0000] {taskinstance.py:430} INFO - ::endgroup::
[2026-04-05T20:57:04.592+0000] {taskinstance.py:441} INFO - ::group::Post task execution logs
[2026-04-05T20:57:04.593+0000] {taskinstance.py:2905} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 465, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 432, in _execute_callable
    return execute_callable(context=context, **execute_callable_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/baseoperator.py", line 401, in wrapper
    return func(self, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/operators/python.py", line 235, in execute
    return_value = self.execute_callable()
                   ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/operators/python.py", line 252, in execute_callable
    return self.python_callable(*self.op_args, **self.op_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/airflow/dags/patchit_failure_pack_100.py", line 26, in _fail
    raise AssertionError(msg + " | data quality guard failed")
AssertionError: [AF001] Missing PK in source table | category=data_quality | behavior=dq | data quality guard failed
[2026-04-05T20:57:04.607+0000] {taskinstance.py:1206} INFO - Marking task as FAILED. dag_id=patchit_***_issue_001, task_id=fail_af001, run_id=manual__2026-04-05T20:53:30+00:00, execution_date=20260405T205330, start_date=20260405T205704, end_date=20260405T205704
[2026-04-05T20:57:04.619+0000] {standard_task_runner.py:110} ERROR - Failed to execute job 44 for task fail_af001 ([AF001] Missing PK in source table | category=data_quality | behavior=dq | data quality guard failed; 568)
[2026-04-05T20:57:04.663+0000] {local_task_job_runner.py:240} INFO - Task exited with return code 1
[2026-04-05T20:57:04.675+0000] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2026-04-05T20:57:04.676+0000] {local_task_job_runner.py:222} INFO - ::endgroup::
"""PATCHIT failure pack: 100 intentionally failing DAGs for remediation testing.

Each DAG maps to a real-world issue class and fails deterministically.
"""
from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.python import PythonOperator


def _fail(issue_id: str, title: str, category: str, behavior: str):
    msg = f"[{issue_id}] {title} | category={category} | behavior={behavior}"
    if behavior == "oom":
        raise MemoryError(msg)
    if behavior == "npe":
        raise RuntimeError(msg + " | simulated NullPointerException")
    if behavior == "schema":
        raise ValueError(msg + " | schema drift/mismatch")
    if behavior == "upstream_unavailable":
        raise ConnectionError(msg + " | upstream dependency unavailable")
    if behavior == "upstream_delayed":
        raise TimeoutError(msg + " | upstream dependency delayed beyond SLA")
    if behavior == "cdc":
        raise RuntimeError(msg + " | CDC inconsistency detected")
    if behavior == "dq":
        raise AssertionError(msg + " | data quality guard failed")
    raise Exception(msg + " | generic pipeline failure")


def _behavior_from_category(cat: str, idx: int) -> str:
    c = (cat or "").lower()
    if c in {"runtime"}: return "oom" if idx % 2 == 0 else "npe"
    if c in {"schema", "contract"}: return "schema"
    if c in {"orchestration"}: return "upstream_unavailable" if idx % 2 == 0 else "upstream_delayed"
    if c in {"cdc"}: return "cdc"
    if c in {"data_quality", "governance", "semantic", "modeling", "timeliness"}: return "dq"
    return "generic"


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}



with DAG(
    dag_id="patchit_airflow_issue_001",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "data_quality", "af001"]
) as dag_1:
    fail_task = PythonOperator(
        task_id="fail_af001",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF001",
            "title": "Missing PK in source table",
            "category": "data_quality",
            "behavior": _behavior_from_category("data_quality", 1),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_002",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "data_quality", "af002"]
) as dag_2:
    fail_task = PythonOperator(
        task_id="fail_af002",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF002",
            "title": "Unexpected null surge in critical column",
            "category": "data_quality",
            "behavior": _behavior_from_category("data_quality", 2),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_003",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "data_quality", "af003"]
) as dag_3:
    fail_task = PythonOperator(
        task_id="fail_af003",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF003",
            "title": "Referential integrity break",
            "category": "data_quality",
            "behavior": _behavior_from_category("data_quality", 3),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_004",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af004"]
) as dag_4:
    fail_task = PythonOperator(
        task_id="fail_af004",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF004",
            "title": "Duplicate event ingestion",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 4),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_005",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "timeliness", "af005"]
) as dag_5:
    fail_task = PythonOperator(
        task_id="fail_af005",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF005",
            "title": "Late arriving dimension causes fact mismatch",
            "category": "timeliness",
            "behavior": _behavior_from_category("timeliness", 5),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_006",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "transformation", "af006"]
) as dag_6:
    fail_task = PythonOperator(
        task_id="fail_af006",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF006",
            "title": "Timezone skew between sources",
            "category": "transformation",
            "behavior": _behavior_from_category("transformation", 6),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_007",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "schema", "af007"]
) as dag_7:
    fail_task = PythonOperator(
        task_id="fail_af007",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF007",
            "title": "Schema drift new nullable column",
            "category": "schema",
            "behavior": _behavior_from_category("schema", 7),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_008",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "schema", "af008"]
) as dag_8:
    fail_task = PythonOperator(
        task_id="fail_af008",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF008",
            "title": "Schema drift changed data type",
            "category": "schema",
            "behavior": _behavior_from_category("schema", 8),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_009",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "schema", "af009"]
) as dag_9:
    fail_task = PythonOperator(
        task_id="fail_af009",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF009",
            "title": "Schema mismatch with contract",
            "category": "schema",
            "behavior": _behavior_from_category("schema", 9),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_010",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "schema", "af010"]
) as dag_10:
    fail_task = PythonOperator(
        task_id="fail_af010",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF010",
            "title": "Schema unavailable from registry",
            "category": "schema",
            "behavior": _behavior_from_category("schema", 10),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_011",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af011"]
) as dag_11:
    fail_task = PythonOperator(
        task_id="fail_af011",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF011",
            "title": "Missing source partition",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 11),
        },
    )

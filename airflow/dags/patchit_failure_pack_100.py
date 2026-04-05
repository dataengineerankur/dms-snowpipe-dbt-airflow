# PATCHIT auto-fix: add_missing_task
# Original error: airflow.exceptions.AirflowException: Task 'load_to_snowflake' not found in DAG 'dbt_pipeline'.
DAG file: /opt/airflow/dags/dbt_pipeline.py
Available tasks: run_dbt, test_dbt, notify
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


with DAG(
    dag_id="patchit_airflow_issue_012",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af012"]
) as dag_12:
    fail_task = PythonOperator(
        task_id="fail_af012",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF012",
            "title": "Truncated file in landing zone",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 12),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_013",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af013"]
) as dag_13:
    fail_task = PythonOperator(
        task_id="fail_af013",
        python_callable=_fail,
        op_kwargs={

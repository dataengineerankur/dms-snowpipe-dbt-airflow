"""
PATCHIT End-to-End Test DAG — Airflow Incident #001
======================================================
Simulates a real-world data pipeline failure scenario:
a customer retention pipeline that fails due to a
NoneType attribute error when the upstream CRM table
is empty (e.g. during staging refreshes).

Bug:  Line 47 — `total_customers = metrics["count"]`
      KeyError when Snowflake returns empty result set.
      Should be: `total_customers = metrics.get("count", 0)`

This DAG is intentionally broken for PATCHIT to detect,
analyse, and auto-fix via a GitHub PR.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ── Task 1: Extract customer metrics from Snowflake ────────────────────────────

def extract_customer_metrics(**context):
    """Extract daily customer metrics from Snowflake CRM table."""
    # Simulate Snowflake query returning empty result during staging refresh
    # In production this returns {"count": 15234, "new": 342, "churned": 87}
    # During staging refresh the table is temporarily empty → returns {}
    snowflake_result = {}   # <-- BUG: empty dict during staging refresh

    # BUG: KeyError when snowflake_result is empty — should use .get("count", 0)
    total_customers = snowflake_result["count"]  # raises KeyError!

    context["ti"].xcom_push(key="total_customers", value=total_customers)
    print(f"Extracted {total_customers} customer records")
    return total_customers


# ── Task 2: Calculate retention rate ──────────────────────────────────────────

def calculate_retention(**context):
    """Calculate customer retention rate for the daily report."""
    ti = context["ti"]
    total = ti.xcom_pull(key="total_customers", task_ids="extract_customer_metrics")

    if total is None:
        raise ValueError("No customer count received from upstream task")

    # BUG: ZeroDivisionError if total is 0 — should guard with max(total, 1)
    active_ratio = 14500 / total  # ZeroDivisionError when total=0

    retention_pct = round(active_ratio * 100, 2)
    print(f"Retention rate: {retention_pct}%")
    context["ti"].xcom_push(key="retention_pct", value=retention_pct)
    return retention_pct


# ── Task 3: Write results to reporting table ───────────────────────────────────

def write_retention_report(**context):
    """Write the retention metrics to the Snowflake reporting schema."""
    ti = context["ti"]
    retention_pct = ti.xcom_pull(key="retention_pct", task_ids="calculate_retention")

    report = {
        "report_date": context["ds"],
        "retention_pct": retention_pct,
        "generated_at": datetime.utcnow().isoformat(),
    }
    print(f"Report written: {report}")
    return report


# ── DAG Definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="patchit_test_incident_dag",
    description="Customer retention pipeline — PATCHIT E2E test with intentional KeyError bug",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "e2e-test", "customer-retention"],
) as dag:

    extract = PythonOperator(
        task_id="extract_customer_metrics",
        python_callable=extract_customer_metrics,
    )

    calc = PythonOperator(
        task_id="calculate_retention",
        python_callable=calculate_retention,
    )

    write = PythonOperator(
        task_id="write_retention_report",
        python_callable=write_retention_report,
    )

    extract >> calc >> write

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

    # Safe access: returns 0 when snowflake_result is empty during staging refresh
    total_customers = snowflake_result.get("count", 0)

    context["ti"].xcom_push(key="total_customers", value=total_customers)
    print(f"Extracted {total_customers} customer records")
    return total_customers


# ── Task 2: Calculate retention rate ──────────────────────────────────────────

def calculate_retention(**context):
    """Calculate customer retention rate for the daily report."""
    ti = context["ti"]
    total = ti.xcom_pull(key="total_customers", task_ids="extract_customer_metrics")

  
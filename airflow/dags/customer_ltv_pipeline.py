"""
Customer Lifetime Value (LTV) Pipeline
=======================================
Calculates LTV scores for each customer segment by joining order history
with segment configuration, then writes the enriched summary to the
reporting schema.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}


def _mock_order_history() -> list[dict]:
    """Simulate Snowflake query: customer orders with first-order date as string."""
    return [
        {"customer_id": "C-001", "segment": "premium", "total_orders": 14,
         "total_spend_usd": 2340.00, "first_order_date": "2022-03-15"},
        {"customer_id": "C-002", "segment": "standard", "total_orders": 3,
         "total_spend_usd": 210.50, "first_order_date": "2023-11-02"},
        {"customer_id": "C-003", "segment": "premium", "total_orders": 22,
         "total_spend_usd": 5100.00, "first_order_date": "2021-07-08"},
        {"customer_id": "C-004", "segment": "at_risk", "total_orders": 1,
         "total_spend_usd": 89.99, "first_order_date": "2024-01-20"},
    ]


def _mock_segment_config() -> dict[str, dict]:
    """Simulate S3 config: segment thresholds for LTV scoring."""
    return {
        "premium":  {"multiplier": 2.5, "min_orders": 10, "churn_days": 90},
        "standard": {"multiplier": 1.0, "min_orders": 3,  "churn_days": 180},
    }


def compute_ltv_scores(**context) -> None:
    """Compute per-customer LTV score using order history and segment multipliers."""
    orders = _mock_order_history()
    segments = _mock_segment_config()

    scored = []
    for customer in orders:
        seg_cfg = segments.get(customer["segment"], {})
        multiplier = seg_cfg.get("multiplier", 1.0)

        first_order_dt = datetime.strptime(customer["first_order_date"], "%Y-%m-%d")
        age_days = (datetime.now() - first_order_dt).days

        ltv = customer["total_spend_usd"] * multiplier * (1 + age_days / 365)
        scored.append({
            "customer_id": customer["customer_id"],
            "segment": customer["segment"],
            "ltv_score": round(ltv, 2),
            "age_days": age_days,
        })

    context["ti"].xcom_push(key="scored_customers", value=scored)
    log.info("Scored %d customers", len(scored))


def build_ltv_summary(**context) -> None:
    """Build segment-level LTV summary and compute ROI per segment."""
    ti = context["ti"]
    scored = ti.xcom_pull(key="scored_customers", task_ids="compute_ltv_scores") or []
    segment_config = _mock_segment_config()

    summary = {}
    for seg, cfg in segment_config.items():
        qualified_customers = [c for c in scored if c["segment"] == seg]

        total_ltv = sum(c["ltv_score"] for c in qualified_customers)

        count = len(qualified_customers)
        avg_ltv = total_ltv / count if count > 0 else 0.0

        summary[seg] = {
            "count": len(qualified_customers),
            "total_ltv": round(total_ltv, 2),
            "avg_ltv": round(avg_ltv, 2),
        }

    ti.xcom_push(key="ltv_summary", value=summary)
    log.info("LTV summary: %s", json.dumps(summary))


def write_ltv_report(**context) -> None:
    """Write LTV summary to reporting schema."""
    ti = context["ti"]
    summary = ti.xcom_pull(key="ltv_summary", task_ids="build_ltv_summary") or {}
    log.info("LTV report written: %s", json.dumps({"generated_at": datetime.utcnow().isoformat(), "summary": summary}, indent=2))


with DAG(
    dag_id="customer_ltv_pipeline",
    description="Customer LTV scoring pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "ltv", "customer"],
) as dag:

    score = PythonOperator(
        task_id="compute_ltv_scores",
        python_callable=compute_ltv_scores,
    )

    summary = PythonOperator(
        task_id="build_ltv_summary",
        python_callable=build_ltv_summary,
    )

    report = PythonOperator(
        task_id="write_ltv_report",
        python_callable=write_ltv_report,
    )

    score >> summary >> report

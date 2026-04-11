"""SC Stage 6: Final Report DAG.

Generates the daily supply chain settlement report with net order totals.
This is the FINAL stage — it crashes when discount_rate is None (set upstream
in Stage 1 for wholesale orders that have no discount_rate column).
"""
from __future__ import annotations

import json
import logging
import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from sc_shared.order_transforms import load_state, compute_line_total

log = logging.getLogger(__name__)


def generate_report_task(**context) -> None:
    """Compute net order totals and emit daily settlement report.

    Calls compute_line_total() from sc_shared.order_transforms for each record.
    That function uses discount_rate — if None (wholesale orders), raises TypeError.
    """
    run_date = context["ds"]
    records = load_state(run_date)

    report_lines = []
    grand_total = 0.0

    for record in records:
        # compute_line_total uses record["discount_rate"] which is None for wholesale orders
        if record.get("discount_rate") is None:
            record = {**record, "discount_rate": 0.0}
        net_total = compute_line_total(record)
        grand_total += net_total

        report_lines.append({
            "order_id":    record["order_id"],
            "customer_id": record["customer_id"],
            "region":      record.get("region", "UNKNOWN"),
            "category":    record.get("category", "Unknown"),
            "net_total":   round(net_total, 2),
            "tax":         record.get("tax_amount", 0.0),
            "shipping":    record.get("shipping_cost", 0.0),
        })

    report = {
        "run_date":    run_date,
        "order_count": len(report_lines),
        "grand_total": round(grand_total, 2),
        "line_items":  report_lines,
    }

    report_path = f"/tmp/sc_pipeline/settlement_report_{run_date}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    log.info("Settlement report written: grand_total=%.2f orders=%d",
             grand_total, len(report_lines))


with DAG(
    dag_id="sc_06_report",
    default_args={"owner": "supply-chain", "retries": 0},
    description="SC Pipeline Stage 6: Generate daily settlement report",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["supply-chain", "stage-6", "reporting"],
) as dag:
    report = PythonOperator(task_id="generate_report", python_callable=generate_report_task)

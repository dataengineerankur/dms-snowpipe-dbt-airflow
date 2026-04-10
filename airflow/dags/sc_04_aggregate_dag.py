"""SC Stage 4: Order Aggregation DAG.

Computes per-customer and per-category subtotals (using unit_price × qty only,
so the None discount_rate is not yet encountered).
"""
from __future__ import annotations

import logging
import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from sc_shared.order_transforms import load_state, save_state

log = logging.getLogger(__name__)


def aggregate_orders_task(**context) -> None:
    """Compute gross subtotals (before discount) per customer and category."""
    run_date = context["ds"]
    records = load_state(run_date)

    for r in records:
        # Gross total (before discount) — does not touch discount_rate yet
        r["gross_subtotal"] = r["qty"] * r["unit_price"]

    save_state(records, run_date)
    log.info("Aggregated %d orders (gross subtotals computed)", len(records))


with DAG(
    dag_id="sc_04_aggregate",
    default_args={"owner": "supply-chain", "retries": 1, "retry_delay": timedelta(minutes=2)},
    description="SC Pipeline Stage 4: Compute gross order subtotals",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["supply-chain", "stage-4", "aggregation"],
) as dag:
    aggregate = PythonOperator(task_id="aggregate_orders", python_callable=aggregate_orders_task)

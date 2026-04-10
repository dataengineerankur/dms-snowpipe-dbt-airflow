"""SC Stage 5: Regional Adjustment DAG.

Applies regional tax multipliers and shipping cost estimates.
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

_REGIONAL_TAX = {
    "US":   0.08,
    "EU":   0.20,
    "APAC": 0.10,
    "UNKNOWN": 0.0,
}

_SHIPPING_COST_PER_KG = 3.50


def transform_orders_task(**context) -> None:
    """Apply regional tax and shipping cost estimates."""
    run_date = context["ds"]
    records = load_state(run_date)

    for r in records:
        tax_rate = _REGIONAL_TAX.get(r.get("region", "UNKNOWN"), 0.0)
        shipping  = r.get("weight_kg", 0.0) * r.get("qty", 0) * _SHIPPING_COST_PER_KG
        r["tax_amount"] = round(r["gross_subtotal"] * tax_rate, 2)
        r["shipping_cost"] = round(shipping, 2)

    save_state(records, run_date)
    log.info("Transformed %d orders with tax and shipping", len(records))


with DAG(
    dag_id="sc_05_transform",
    default_args={"owner": "supply-chain", "retries": 1, "retry_delay": timedelta(minutes=2)},
    description="SC Pipeline Stage 5: Apply regional tax and shipping costs",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["supply-chain", "stage-5", "transform"],
) as dag:
    transform = PythonOperator(task_id="transform_orders", python_callable=transform_orders_task)

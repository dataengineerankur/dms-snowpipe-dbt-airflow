"""OF Pipeline Stage 4: Estimate delivery windows based on service level and weight."""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from of_shared.shipment_parser import load_state, save_state

log = logging.getLogger(__name__)

_SLA = {"express": 2, "priority": 3, "standard": 5, "ground": 7}
_SURCHARGE_PER_KG = 0.85


def estimate_delivery_task(**context) -> None:
    run_date = context["ds"]
    records = load_state(run_date)
    ref = datetime.strptime(run_date, "%Y-%m-%d")
    for r in records:
        days = _SLA.get(r.get("service_level", "standard"), 5)
        r["estimated_delivery"] = (ref + timedelta(days=days)).strftime("%Y-%m-%d")
        r["oversize_surcharge"] = round(max(0.0, r["weight_kg"] - 2.0) * _SURCHARGE_PER_KG, 2)
    save_state(records, run_date)
    log.info("Delivery estimates set for %d shipments", len(records))


with DAG(
    dag_id="of_04_estimate",
    default_args={"owner": "fulfillment", "retries": 1, "retry_delay": timedelta(minutes=2)},
    description="OF Pipeline Stage 4: Estimate delivery windows",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fulfillment", "stage-4", "estimation"],
) as dag:
    estimate = PythonOperator(task_id="estimate_delivery", python_callable=estimate_delivery_task)

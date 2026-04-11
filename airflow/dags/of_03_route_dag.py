"""OF Pipeline Stage 3: Assign shipping routes and hub handoff points."""
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

_ROUTE_MAP = {
    101: "HUB-EAST",
    102: "HUB-CENTRAL",
    103: "HUB-INTL",
    104: "HUB-USPS",
}


def route_shipments_task(**context) -> None:
    run_date = context["ds"]
    records = load_state(run_date)
    for r in records:
        r["hub"] = _ROUTE_MAP.get(r["carrier_id"], "HUB-DEFAULT")
    save_state(records, run_date)
    log.info("Routing complete for %d shipments", len(records))


with DAG(
    dag_id="of_03_route",
    default_args={"owner": "fulfillment", "retries": 1, "retry_delay": timedelta(minutes=2)},
    description="OF Pipeline Stage 3: Assign carrier hub routing",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fulfillment", "stage-3", "routing"],
) as dag:
    route = PythonOperator(task_id="route_shipments", python_callable=route_shipments_task)

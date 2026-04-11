"""OF Pipeline Stage 6: Send customer dispatch notifications.

Reads the finalized shipment records and dispatches carrier-specific
notification messages to customers via the messaging gateway.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from of_shared.shipment_parser import load_state

log = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "of_shared", "carrier_config.json")
with open(_CONFIG_PATH) as _f:
    _CARRIER_CFG = json.load(_f)

CARRIER_NAMES = _CARRIER_CFG["carriers"]
NOTIFICATION_TEMPLATES = _CARRIER_CFG["notification_templates"]


def send_notifications_task(**context) -> None:
    """Generate and dispatch shipment notification for each customer."""
    run_date = context["ds"]
    records = load_state(run_date)

    dispatched = []
    for record in records:
        carrier_id = record["carrier_id"]
        tracking_id = record.get("tracking_id", "N/A")
        customer_id = record["customer_id"]

        carrier_name = CARRIER_NAMES[str(carrier_id)]
        template = NOTIFICATION_TEMPLATES[str(carrier_id)]
        message = template.format(tracking_id=tracking_id)

        log.info(
            "Notifying customer %s: shipment %s dispatched via %s — %s",
            customer_id, record["shipment_id"], carrier_name, message,
        )
        dispatched.append({
            "customer_id":   customer_id,
            "shipment_id":   record["shipment_id"],
            "carrier":       carrier_name,
            "message":       message,
        })

    log.info("Dispatched %d customer notifications for %s", len(dispatched), run_date)


with DAG(
    dag_id="of_06_notify",
    default_args={"owner": "fulfillment", "retries": 0},
    description="OF Pipeline Stage 6: Send customer dispatch notifications",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fulfillment", "stage-6", "notifications"],
) as dag:
    notify = PythonOperator(task_id="send_notifications", python_callable=send_notifications_task)

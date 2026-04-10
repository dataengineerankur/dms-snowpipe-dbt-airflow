"""SC Stage 3: Order Enrichment DAG.

Joins orders with the product catalog to add category and weight info.
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

_PRODUCT_CATALOG = {
    "SKU-A": {"category": "Electronics", "weight_kg": 0.5, "warehouse": "WH-US-01"},
    "SKU-B": {"category": "Apparel",     "weight_kg": 0.2, "warehouse": "WH-EU-01"},
    "SKU-C": {"category": "Electronics", "weight_kg": 1.2, "warehouse": "WH-US-02"},
    "SKU-D": {"category": "Home",        "weight_kg": 0.8, "warehouse": "WH-US-01"},
}


def enrich_orders_task(**context) -> None:
    """Join orders with product catalog metadata."""
    run_date = context["ds"]
    records = load_state(run_date)

    enriched = []
    for r in records:
        catalog = _PRODUCT_CATALOG.get(r["product_id"], {})
        enriched.append({
            **r,
            "category":   catalog.get("category", "Unknown"),
            "weight_kg":  catalog.get("weight_kg", 0.0),
            "warehouse":  catalog.get("warehouse", "WH-UNKNOWN"),
        })

    save_state(enriched, run_date)
    log.info("Enriched %d orders with catalog data", len(enriched))


with DAG(
    dag_id="sc_03_enrich",
    default_args={"owner": "supply-chain", "retries": 1, "retry_delay": timedelta(minutes=2)},
    description="SC Pipeline Stage 3: Enrich orders with product catalog",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["supply-chain", "stage-3", "enrichment"],
) as dag:
    enrich = PythonOperator(task_id="enrich_orders", python_callable=enrich_orders_task)

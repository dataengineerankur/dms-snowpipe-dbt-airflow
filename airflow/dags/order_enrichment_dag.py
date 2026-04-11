"""Order Enrichment DAG.

Enriches raw orders from the OMS with product metadata sourced from
the DMS Product Catalog service, then computes applicable tax amounts.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

for _pkg in ["dms-product-catalog", "dms-order-enrichment"]:
    _p = os.path.join(os.path.expanduser("~/repo_projects"), _pkg)
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "order-ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def _pending_orders() -> list:
    return [
        {"order_id": "OMS-10041", "sku": "PRD-001", "qty": 2, "unit_price": 39.99, "customer_id": "C500"},
        {"order_id": "OMS-10042", "sku": "PRD-002", "qty": 1, "unit_price": 89.00, "customer_id": "C501"},
        {"order_id": "OMS-10043", "sku": "BDL-099", "qty": 3, "unit_price": 24.50, "customer_id": "C502"},
        {"order_id": "OMS-10044", "sku": "PRD-003", "qty": 5, "unit_price": 14.99, "customer_id": "C503"},
    ]


def _catalog_client():
    from dms_catalog.client import ProductCatalogClient

    class _Sess:
        _data = {
            "PRD-001": {"category": "Electronics", "dimensions": {"weight_g": 450},
                        "metadata": {"attributes": {"tax_class": "standard"}, "origin": "CN"}},
            "PRD-002": {"category": "Accessories", "dimensions": {"weight_g": 120},
                        "metadata": {"attributes": {"tax_class": "reduced"}, "origin": "VN"}},
            "BDL-099": {"category": "Bundle",      "dimensions": {"weight_g": 980},
                        "metadata": {}},
            "PRD-003": {"category": "Home",        "dimensions": {"weight_g": 750},
                        "metadata": {"attributes": {"tax_class": "standard"}, "origin": "US"}},
        }
        def get(self, url, timeout=None):
            sku = url.split("/")[-1]
            payload = self._data[sku]
            class _R:
                def raise_for_status(self): pass
                def json(self): return payload
            return _R()

    c = ProductCatalogClient.__new__(ProductCatalogClient)
    c._base_url = "http://catalog.internal"
    c._timeout = 5.0
    c._session = _Sess()
    return c


def enrich_orders_task(**context) -> None:
    from dms_enrichment.processor import OrderEnrichmentProcessor
    orders = _pending_orders()
    processor = OrderEnrichmentProcessor(_catalog_client())
    log.info("Enriching %d orders", len(orders))
    enriched = processor.process_batch(orders)
    context["ti"].xcom_push(key="enriched_orders", value=enriched)


def compute_tax_task(**context) -> None:
    from dms_enrichment.tax import compute_tax
    enriched = context["ti"].xcom_pull(key="enriched_orders", task_ids="enrich_orders")
    for order in enriched:
        order["tax_amount"] = compute_tax(
            order["qty"], order["unit_price"], order.get("tax_class", "standard")
        )
    log.info("Tax computed for %d orders", len(enriched))


with DAG(
    dag_id="order_enrichment",
    default_args=DEFAULT_ARGS,
    description="Enrich OMS orders with DMS product catalog metadata",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["orders", "enrichment", "catalog"],
) as dag:
    enrich = PythonOperator(task_id="enrich_orders", python_callable=enrich_orders_task)
    tax    = PythonOperator(task_id="compute_tax",   python_callable=compute_tax_task)
    enrich >> tax

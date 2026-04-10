"""Regional Revenue Analytics DAG.

Computes FX-normalized revenue across all active regional markets.

Dependency chain:
    This DAG → dms_analytics.revenue.RevenueEngine
                    → dms_data_commons.currency.CurrencyConverter.convert()
                          ← BUG: ZeroDivisionError when fx_rate=0.0
"""
from __future__ import annotations

import logging
import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add sibling repos to path so we can import dms_analytics and dms_data_commons locally
_REPO_PROJECTS = os.path.expanduser("~/repo_projects")
for _pkg_repo in ["dms-analytics-sdk", "dms-data-commons"]:
    _pkg_path = os.path.join(_REPO_PROJECTS, _pkg_repo)
    if os.path.isdir(_pkg_path) and _pkg_path not in sys.path:
        sys.path.insert(0, _pkg_path)

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _fetch_regional_records() -> list:
    """Return latest revenue records for all active regions.

    The LATAM_SOUTH region was onboarded last week. Its FX data feed
    is misconfigured and returns 0.0 — causing ZeroDivisionError downstream.
    """
    return [
        {"region": "US",          "revenue_usd": 142500.0, "fx_rate": 1.0},
        {"region": "EU",          "revenue_usd": 98200.0,  "fx_rate": 0.9278},
        {"region": "APAC_JP",     "revenue_usd": 76800.0,  "fx_rate": 149.52},
        {"region": "APAC_SG",     "revenue_usd": 55300.0,  "fx_rate": 1.3481},
        {"region": "LATAM_BR",    "revenue_usd": 41000.0,  "fx_rate": 5.0425},
        {"region": "LATAM_SOUTH", "revenue_usd": 28000.0,  "fx_rate": 0.0},
    ]


def compute_regional_revenue_task(**context) -> None:
    """Stage 1: Compute FX-normalized revenue via dms_analytics.RevenueEngine."""
    from dms_analytics.revenue import RevenueEngine

    engine = RevenueEngine()
    records = _fetch_regional_records()

    log.info("Processing %d regional revenue records", len(records))
    results = engine.compute_regional_revenue(records)

    context["ti"].xcom_push(key="regional_revenue", value=results)
    log.info("Completed: %d records processed", len(results))


def aggregate_revenue_task(**context) -> None:
    """Stage 2: Aggregate normalized revenue by continent."""
    results = context["ti"].xcom_pull(
        key="regional_revenue", task_ids="compute_regional_revenue"
    )
    aggregated: dict = {}
    for r in results:
        continent = r["region"].split("_")[0]
        aggregated[continent] = aggregated.get(continent, 0.0) + r.get("normalized_revenue_local", 0.0)
    log.info("Continental totals: %s", aggregated)
    context["ti"].xcom_push(key="continental_revenue", value=aggregated)


with DAG(
    dag_id="regional_revenue_analytics",
    default_args=DEFAULT_ARGS,
    description="FX-normalized regional revenue using dms-analytics-sdk",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["revenue", "analytics", "regional", "fx"],
) as dag:
    compute_revenue = PythonOperator(
        task_id="compute_regional_revenue",
        python_callable=compute_regional_revenue_task,
    )
    aggregate_revenue = PythonOperator(
        task_id="aggregate_regional_revenue",
        python_callable=aggregate_revenue_task,
    )
    compute_revenue >> aggregate_revenue

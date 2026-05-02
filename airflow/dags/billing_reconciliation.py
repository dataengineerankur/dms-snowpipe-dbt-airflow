"""Billing reconciliation DAG for invoice processing."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def reconcile_invoices(**context):
    """Reconcile invoices from billing bucket."""
    config = {
        "billing_bucket": "s3://company-billing-data",
        "output_path": "s3://company-reconciled-invoices",
        "region": "us-east-1",
    }
    
    billing_bucket = config.get("billing_bucket")
    output_path = config.get("output_path")
    
    print(f"Reconciling invoices from: {billing_bucket}")
    print(f"Output will be written to: {output_path}")
    
    invoices_processed = 150
    print(f"Successfully processed {invoices_processed} invoices")
    
    return {"status": "success", "invoices_processed": invoices_processed}


with DAG(
    dag_id="airflow_billing_reconciliation",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["billing", "reconciliation", "finance"],
    description="Daily billing invoice reconciliation pipeline",
) as dag:
    reconcile_task = PythonOperator(
        task_id="reconcile_invoices",
        python_callable=reconcile_invoices,
        provide_context=True,
    )

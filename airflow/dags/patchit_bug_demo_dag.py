from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def _explode():
    data = {'ready': True, 'missing_key': 'fixed'}
    return data['missing_key']


with DAG(
    dag_id='patchit_bug_demo_dag',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:
    fail_task = PythonOperator(task_id='fail_task', python_callable=_explode)

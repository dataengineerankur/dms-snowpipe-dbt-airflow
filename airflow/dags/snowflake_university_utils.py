from pathlib import Path
import os
import runpy
import sys
from airflow.hooks.base import BaseHook
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = 'snowflake_default'
LOCAL_REPO_ROOT = Path(os.getenv('PROJECT_ROOT', Path(__file__).resolve().parents[2]))


def _read_sql(path: str) -> str:
    return (LOCAL_REPO_ROOT / path).read_text()


def _apply_snowflake_env_from_connection() -> None:
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = conn.extra_dejson or {}
    env_map = {
        'SNOWFLAKE_ACCOUNT': extra.get('account') or conn.host or '',
        'SNOWFLAKE_USER': conn.login or '',
        'SNOWFLAKE_PASSWORD': conn.password or '',
        'SNOWFLAKE_ROLE': extra.get('role') or 'ACCOUNTADMIN',
        'SNOWFLAKE_WAREHOUSE': extra.get('warehouse') or 'COMPUTE_WH',
        'SNOWFLAKE_DATABASE': extra.get('database') or 'RAW_DB',
        'SNOWFLAKE_SCHEMA': extra.get('schema') or 'PUBLIC',
    }
    for key, value in env_map.items():
        if value:
            os.environ[key] = value


def run_sql(sql: str):
    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(sql, autocommit=True)


def run_sql_script(path: str):
    return run_sql(_read_sql(path))


def run_python_file(path: str):
    _apply_snowflake_env_from_connection()
    repo_root = str(LOCAL_REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    runpy.run_path(str(LOCAL_REPO_ROOT / path), run_name='__main__')


def build_sql_task(task_id: str, script_path: str) -> PythonOperator:
    return PythonOperator(task_id=task_id, python_callable=run_sql_script, op_args=[script_path])


def build_inline_sql_task(task_id: str, sql: str) -> PythonOperator:
    return PythonOperator(task_id=task_id, python_callable=run_sql, op_args=[sql])


def build_python_task(task_id: str, script_path: str) -> PythonOperator:
    return PythonOperator(task_id=task_id, python_callable=run_python_file, op_args=[script_path])


def branch_on_stream(task_id: str, stream_name: str, if_has_data: str, if_empty: str) -> BranchPythonOperator:
    def _branch():
        rows = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_first(
            f"select system$stream_has_data('{stream_name}')"
        )
        return if_has_data if rows and str(rows[0]).upper() == 'TRUE' else if_empty

    return BranchPythonOperator(task_id=task_id, python_callable=_branch)

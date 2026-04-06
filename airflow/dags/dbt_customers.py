# PATCHIT auto-fix: add_backoff
# Original error: snowflake.connector.errors.OperationalError: 429 Too Many Requests — catchup=True triggered backfill. Set catchup=False in DAG args.
# PATCHIT auto-fix: unknown
# Original error: airflow.exceptions.AirflowException: Silent failure. email_on_failure=False. Set email_on_failure=True and configure SMTP in default_args.
# PATCHIT auto-fix: unknown
# Original error: :57:04.458+0000] {task_command.py:426} INFO - Running <TaskInstance: patchit_airflow_issue_001.fail_af001 manual__2026-04-05T20:53:30+00:00 [running]> on host 6a47af29e643
[2026-04-05T20:57:04.552+0000] {taskinstance.py:2648} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='patchit' AIRFLOW_CTX_DAG_ID='patchit_***_issue_001' AIRFLOW_CTX_TASK_ID='fail_af001' AIRFLOW_CTX_EXECUTION_DATE='2026-04-05T20:53:30+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2026-04-05T20:53:30+00:00'
[2026-04-05T20:57:04.553+0000] {taskinstance.py:430} INFO - ::endgroup::
[2026-04-05T20:57:04.592+0000] {taskinstance.py:441} INFO - ::group::Post task execution logs
[2026-04-05T20:57:04.593+0000] {taskinstance.py:2905} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 465, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 432, in _execute_callable
    return execute_callable(context=context, **execute_callable_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/baseoperator.py", line 401, in wrapper
    return func(self, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/operators/python.py", line 235, in execute
    return_value = self.execute_callable()
                   ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/operators/python.py", line 252, in execute_callable
    return self.python_callable(*self.op_args, **self.op_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/airflow/dags/patchit_failure_pack_100.py", line 26, in _fail
    raise AssertionError(msg + " | data quality guard failed")
AssertionError: [AF001] Missing PK in source table | category=data_quality | behavior=dq | data quality guard failed
[2026-04-05T20:57:04.607+0000] {taskinstance.py:1206} INFO - Marking task as FAILED. dag_id=patchit_***_issue_001, task_id=fail_af001, run_id=manual__2026-04-05T20:53:30+00:00, execution_date=20260405T205330, start_date=20260405T205704, end_date=20260405T205704
[2026-04-05T20:57:04.619+0000] {standard_task_runner.py:110} ERROR - Failed to execute job 44 for task fail_af001 ([AF001] Missing PK in source table | category=data_quality | behavior=dq | data quality guard failed; 568)
[2026-04-05T20:57:04.663+0000] {local_task_job_runner.py:240} INFO - Task exited with return code 1
[2026-04-05T20:57:04.675+0000] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2026-04-05T20:57:04.676+0000] {local_task_job_runner.py:222} INFO - ::endgroup::
from datetime import datetime

from airflow import DAG

from dbt_utils import build_dbt_task

with DAG(
    dag_id="dbt_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "snowflake", "customers"],
) as dag:
    dbt_deps = build_dbt_task("dbt_deps", "dbt deps")
    dbt_run_stg = build_dbt_task(
        "dbt_run_stg", "dbt run --select path:models/customers/stg"
    )
    dbt_run_int = build_dbt_task(
        "dbt_run_int", "dbt run --select path:models/customers/int"
    )
    dbt_run_gold = build_dbt_task(
        "dbt_run_gold", "dbt run --select path:models/customers/gold"
    )
    dbt_test = build_dbt_task(
        "dbt_test", "dbt test --select path:models/customers"
    )

    dbt_deps >> dbt_run_stg >> dbt_run_int >> dbt_run_gold >> dbt_test

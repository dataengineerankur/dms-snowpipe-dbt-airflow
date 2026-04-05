# PATCHIT auto-fix: fix_data_quality_check
# Original error: y:426} INFO - Running <TaskInstance: patchit_airflow_issue_005.fail_af005 manual__2026-04-05T20:53:40+00:00 [running]> on host 6a47af29e643
[2026-04-05T20:57:13.902+0000] {taskinstance.py:2648} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='patchit' AIRFLOW_CTX_DAG_ID='patchit_***_issue_005' AIRFLOW_CTX_TASK_ID='fail_af005' AIRFLOW_CTX_EXECUTION_DATE='2026-04-05T20:53:40+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2026-04-05T20:53:40+00:00'
[2026-04-05T20:57:13.902+0000] {taskinstance.py:430} INFO - ::endgroup::
[2026-04-05T20:57:13.913+0000] {taskinstance.py:441} INFO - ::group::Post task execution logs
[2026-04-05T20:57:13.914+0000] {taskinstance.py:2905} ERROR - Task failed with exception
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
AssertionError: [AF005] Late arriving dimension causes fact mismatch | category=timeliness | behavior=dq | data quality guard failed
[2026-04-05T20:57:13.930+0000] {taskinstance.py:1206} INFO - Marking task as FAILED. dag_id=patchit_***_issue_005, task_id=fail_af005, run_id=manual__2026-04-05T20:53:40+00:00, execution_date=20260405T205340, start_date=20260405T205713, end_date=20260405T205713
[2026-04-05T20:57:13.946+0000] {standard_task_runner.py:110} ERROR - Failed to execute job 53 for task fail_af005 ([AF005] Late arriving dimension causes fact mismatch | category=timeliness | behavior=dq | data quality guard failed; 603)
[2026-04-05T20:57:13.988+0000] {local_task_job_runner.py:240} INFO - Task exited with return code 1
[2026-04-05T20:57:14.012+0000] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2026-04-05T20:57:14.013+0000] {local_task_job_runner.py:222} INFO - ::endgroup::
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
import os

from airflow.providers.docker.operators.docker import DockerOperator

DBT_EXECUTION_MODE = os.getenv("DBT_EXECUTION_MODE", "local").lower()

DEFAULT_ENV = {
    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
    "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE", "TRANSFORM_ROLE"),
    "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "TRANSFORM_WH"),
    "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS"),
    "DBT_PROFILES_DIR": "/opt/dbt",
}


def _normalize_dbt_command(command: str, add_prefix: bool) -> str:
    trimmed = command.strip()
    if add_prefix:
        return trimmed if trimmed.startswith("dbt ") else f"dbt {trimmed}"
    return trimmed[len("dbt ") :] if trimmed.startswith("dbt ") else trimmed


def build_dbt_task(task_id: str, command: str):
    if DBT_EXECUTION_MODE == "ecs":
        ecs_command = _normalize_dbt_command(command, add_prefix=True)
        from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

        subnets = [s for s in os.getenv("ECS_SUBNETS", "").split(",") if s]
        security_groups = [s for s in os.getenv("ECS_SECURITY_GROUPS", "").split(",") if s]

        return EcsRunTaskOperator(
            task_id=task_id,
            cluster=os.getenv("ECS_CLUSTER_ARN", ""),
            task_definition=os.getenv("ECS_TASK_DEFINITION_ARN", ""),
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "dbt",
                        "command": ["/bin/bash", "-lc", ecs_command],
                        "environment": [{"name": k, "value": v} for k, v in DEFAULT_ENV.items()],
                    }
                ]
            },
            network_configuration={
                "awsvpcConfiguration": {
                    "subnets": subnets,
                    "securityGroups": security_groups,
                    "assignPublicIp": "ENABLED",
                }
            },
        )

    docker_command = _normalize_dbt_command(command, add_prefix=False)
    return DockerOperator(
        task_id=task_id,
        image=os.getenv("DBT_IMAGE", "dms-dbt-runner:latest"),
        api_version="auto",
        auto_remove=True,
        command=docker_command,
        docker_url=os.getenv("DOCKER_HOST", "tcp://docker-proxy:2375"),
        network_mode="bridge",
        environment=DEFAULT_ENV,
        mount_tmp_dir=False,
    )

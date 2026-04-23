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


def build_dbt_task(task_id: str, command: str, retries: int = 0):
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
            retries=retries,
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
        retries=retries,
    )


def build_dbt_task_group(group_id: str, tasks: list, retries: int = 0):
    from airflow.utils.task_group import TaskGroup

    with TaskGroup(group_id=group_id) as task_group:
        task_objects = []
        for task_config in tasks:
            if isinstance(task_config, dict):
                task_id = task_config.get("task_id")
                command = task_config.get("command")
                task_retries = task_config.get("retries", retries)
                if task_id and command:
                    task_obj = build_dbt_task(task_id, command, retries=task_retries)
                    task_objects.append(task_obj)
        
        for i in range(len(task_objects) - 1):
            task_objects[i] >> task_objects[i + 1]
    
    return task_group

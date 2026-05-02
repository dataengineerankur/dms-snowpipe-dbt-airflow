
# PATCHIT: add retry configuration to prevent transient failures
from datetime import timedelta
DEFAULT_ARGS_WITH_RETRIES = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=60),
}
# Apply to DAG:
# dag = DAG('my_dag', default_args={**default_args, **DEFAULT_ARGS_WITH_RETRIES})

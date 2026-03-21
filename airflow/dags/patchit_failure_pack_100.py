"""PATCHIT failure pack: 100 intentionally failing DAGs for remediation testing.

Each DAG maps to a real-world issue class and fails deterministically.
"""
from datetime import datetime, timedelta
import random
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def _fail(issue_id: str, title: str, category: str, behavior: str):
    msg = f"[{issue_id}] {title} | category={category} | behavior={behavior}"
    logger.error(f"PATCHIT_FAILURE: {msg}")
    
    if behavior == "oom":
        raise MemoryError(msg)
    if behavior == "npe":
        raise RuntimeError(msg + " | simulated NullPointerException")
    if behavior == "schema":
        raise ValueError(msg + " | schema drift/mismatch")
    if behavior == "upstream_unavailable":
        raise ConnectionError(msg + " | upstream dependency unavailable")
    if behavior == "upstream_delayed":
        raise TimeoutError(msg + " | upstream dependency delayed beyond SLA")
    if behavior == "cdc":
        raise RuntimeError(msg + " | CDC inconsistency detected")
    if behavior == "dq":
        raise AssertionError(msg + " | data quality guard failed")
    raise Exception(msg + " | generic pipeline failure")


def _behavior_from_category(cat: str, idx: int) -> str:
    c = (cat or "").lower()
    if c in {"runtime"}: return "oom" if idx % 2 == 0 else "npe"
    if c in {"schema", "contract"}: return "schema"
    if c in {"orchestration"}: return "upstream_unavailable" if idx % 2 == 0 else "upstream_delayed"
    if c in {"cdc"}: return "cdc"
    if c in {"data_quality", "governance", "semantic", "modeling", "timeliness"}: return "dq"
    return "generic"


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}



with DAG(
    dag_id="patchit_airflow_issue_001",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "data_quality", "af001"]
) as dag_1:
    fail_task = PythonOperator(
        task_id="fail_af001",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF001",
            "title": "Missing PK in source table",
            "category": "data_quality",
            "behavior": _behavior_from_category("data_quality", 1),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_002",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "data_quality", "af002"]
) as dag_2:
    fail_task = PythonOperator(
        task_id="fail_af002",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF002",
            "title": "Unexpected null surge in critical column",
            "category": "data_quality",
            "behavior": _behavior_from_category("data_quality", 2),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_003",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "data_quality", "af003"]
) as dag_3:
    fail_task = PythonOperator(
        task_id="fail_af003",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF003",
            "title": "Referential integrity break",
            "category": "data_quality",
            "behavior": _behavior_from_category("data_quality", 3),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_004",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af004"]
) as dag_4:
    fail_task = PythonOperator(
        task_id="fail_af004",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF004",
            "title": "Duplicate event ingestion",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 4),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_005",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "timeliness", "af005"]
) as dag_5:
    fail_task = PythonOperator(
        task_id="fail_af005",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF005",
            "title": "Late arriving dimension causes fact mismatch",
            "category": "timeliness",
            "behavior": _behavior_from_category("timeliness", 5),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_006",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "transformation", "af006"]
) as dag_6:
    fail_task = PythonOperator(
        task_id="fail_af006",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF006",
            "title": "Timezone skew between sources",
            "category": "transformation",
            "behavior": _behavior_from_category("transformation", 6),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_007",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "schema", "af007"]
) as dag_7:
    fail_task = PythonOperator(
        task_id="fail_af007",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF007",
            "title": "Schema drift new nullable column",
            "category": "schema",
            "behavior": _behavior_from_category("schema", 7),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_008",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "schema", "af008"]
) as dag_8:
    fail_task = PythonOperator(
        task_id="fail_af008",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF008",
            "title": "Schema drift changed data type",
            "category": "schema",
            "behavior": _behavior_from_category("schema", 8),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_009",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "schema", "af009"]
) as dag_9:
    fail_task = PythonOperator(
        task_id="fail_af009",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF009",
            "title": "Schema mismatch with contract",
            "category": "schema",
            "behavior": _behavior_from_category("schema", 9),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_010",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "schema", "af010"]
) as dag_10:
    fail_task = PythonOperator(
        task_id="fail_af010",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF010",
            "title": "Schema unavailable from registry",
            "category": "schema",
            "behavior": _behavior_from_category("schema", 10),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_011",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af011"]
) as dag_11:
    fail_task = PythonOperator(
        task_id="fail_af011",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF011",
            "title": "Missing source partition",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 11),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_012",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af012"]
) as dag_12:
    fail_task = PythonOperator(
        task_id="fail_af012",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF012",
            "title": "Truncated file in landing zone",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 12),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_013",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af013"]
) as dag_13:
    fail_task = PythonOperator(
        task_id="fail_af013",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF013",
            "title": "Compressed file corrupt",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 13),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_014",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af014"]
) as dag_14:
    fail_task = PythonOperator(
        task_id="fail_af014",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF014",
            "title": "CSV delimiter changed",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 14),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_015",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af015"]
) as dag_15:
    fail_task = PythonOperator(
        task_id="fail_af015",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF015",
            "title": "Header row missing",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 15),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_016",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "modeling", "af016"]
) as dag_16:
    fail_task = PythonOperator(
        task_id="fail_af016",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF016",
            "title": "SCD2 overlap windows",
            "category": "modeling",
            "behavior": _behavior_from_category("modeling", 16),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_017",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "modeling", "af017"]
) as dag_17:
    fail_task = PythonOperator(
        task_id="fail_af017",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF017",
            "title": "SCD2 open record duplication",
            "category": "modeling",
            "behavior": _behavior_from_category("modeling", 17),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_018",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "cdc", "af018"]
) as dag_18:
    fail_task = PythonOperator(
        task_id="fail_af018",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF018",
            "title": "CDC missing delete events",
            "category": "cdc",
            "behavior": _behavior_from_category("cdc", 18),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_019",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "cdc", "af019"]
) as dag_19:
    fail_task = PythonOperator(
        task_id="fail_af019",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF019",
            "title": "CDC out-of-order events",
            "category": "cdc",
            "behavior": _behavior_from_category("cdc", 19),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_020",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "cdc", "af020"]
) as dag_20:
    fail_task = PythonOperator(
        task_id="fail_af020",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF020",
            "title": "CDC LSN gap detected",
            "category": "cdc",
            "behavior": _behavior_from_category("cdc", 20),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_021",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "orchestration", "af021"]
) as dag_21:
    fail_task = PythonOperator(
        task_id="fail_af021",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF021",
            "title": "Upstream job unavailable hard dependency",
            "category": "orchestration",
            "behavior": _behavior_from_category("orchestration", 21),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_022",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "orchestration", "af022"]
) as dag_22:
    fail_task = PythonOperator(
        task_id="fail_af022",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF022",
            "title": "Upstream job delayed soft dependency",
            "category": "orchestration",
            "behavior": _behavior_from_category("orchestration", 22),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_023",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "orchestration", "af023"]
) as dag_23:
    fail_task = PythonOperator(
        task_id="fail_af023",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF023",
            "title": "Circular dependency in DAG graph",
            "category": "orchestration",
            "behavior": _behavior_from_category("orchestration", 23),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_024",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "runtime", "af024"]
) as dag_24:
    fail_task = PythonOperator(
        task_id="fail_af024",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF024",
            "title": "Deadlock on warehouse table lock",
            "category": "runtime",
            "behavior": _behavior_from_category("runtime", 24),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_025",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "runtime", "af025"]
) as dag_25:
    fail_task = PythonOperator(
        task_id="fail_af025",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF025",
            "title": "OOM during Spark transform",
            "category": "runtime",
            "behavior": _behavior_from_category("runtime", 25),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_026",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "runtime", "af026"]
) as dag_26:
    fail_task = PythonOperator(
        task_id="fail_af026",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF026",
            "title": "Driver OOM from collect() misuse",
            "category": "runtime",
            "behavior": _behavior_from_category("runtime", 26),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_027",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "runtime", "af027"]
) as dag_27:
    fail_task = PythonOperator(
        task_id="fail_af027",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF027",
            "title": "Shuffle spill saturation",
            "category": "runtime",
            "behavior": _behavior_from_category("runtime", 27),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_028",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "runtime", "af028"]
) as dag_28:
    fail_task = PythonOperator(
        task_id="fail_af028",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF028",
            "title": "Skewed key causes long tail tasks",
            "category": "runtime",
            "behavior": _behavior_from_category("runtime", 28),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_029",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "runtime", "af029"]
) as dag_29:
    fail_task = PythonOperator(
        task_id="fail_af029",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF029",
            "title": "Broadcast join threshold misconfigured",
            "category": "runtime",
            "behavior": _behavior_from_category("runtime", 29),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_030",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "code", "af030"]
) as dag_30:
    fail_task = PythonOperator(
        task_id="fail_af030",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF030",
            "title": "NullPointerException in UDF",
            "category": "code",
            "behavior": _behavior_from_category("code", 30),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_031",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "code", "af031"]
) as dag_31:
    fail_task = PythonOperator(
        task_id="fail_af031",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF031",
            "title": "TypeError in Python transform",
            "category": "code",
            "behavior": _behavior_from_category("code", 31),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_032",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "code", "af032"]
) as dag_32:
    fail_task = PythonOperator(
        task_id="fail_af032",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF032",
            "title": "ImportError missing dependency",
            "category": "code",
            "behavior": _behavior_from_category("code", 32),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_033",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "code", "af033"]
) as dag_33:
    fail_task = PythonOperator(
        task_id="fail_af033",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF033",
            "title": "Runtime version mismatch",
            "category": "code",
            "behavior": _behavior_from_category("code", 33),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_034",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "reliability", "af034"]
) as dag_34:
    fail_task = PythonOperator(
        task_id="fail_af034",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF034",
            "title": "Task retry storm due non-idempotent writes",
            "category": "reliability",
            "behavior": _behavior_from_category("reliability", 34),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_035",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "reliability", "af035"]
) as dag_35:
    fail_task = PythonOperator(
        task_id="fail_af035",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF035",
            "title": "Idempotency key missing",
            "category": "reliability",
            "behavior": _behavior_from_category("reliability", 35),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_036",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "streaming", "af036"]
) as dag_36:
    fail_task = PythonOperator(
        task_id="fail_af036",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF036",
            "title": "Checkpoint corruption",
            "category": "streaming",
            "behavior": _behavior_from_category("streaming", 36),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_037",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "streaming", "af037"]
) as dag_37:
    fail_task = PythonOperator(
        task_id="fail_af037",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF037",
            "title": "Offset commit failure",
            "category": "streaming",
            "behavior": _behavior_from_category("streaming", 37),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_038",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "streaming", "af038"]
) as dag_38:
    fail_task = PythonOperator(
        task_id="fail_af038",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF038",
            "title": "Kafka topic retention too low",
            "category": "streaming",
            "behavior": _behavior_from_category("streaming", 38),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_039",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "streaming", "af039"]
) as dag_39:
    fail_task = PythonOperator(
        task_id="fail_af039",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF039",
            "title": "Poison pill record blocks stream",
            "category": "streaming",
            "behavior": _behavior_from_category("streaming", 39),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_040",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af040"]
) as dag_40:
    fail_task = PythonOperator(
        task_id="fail_af040",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF040",
            "title": "Secrets retrieval failure",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 40),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_041",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af041"]
) as dag_41:
    fail_task = PythonOperator(
        task_id="fail_af041",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF041",
            "title": "Expired cloud credentials",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 41),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_042",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af042"]
) as dag_42:
    fail_task = PythonOperator(
        task_id="fail_af042",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF042",
            "title": "Permission denied on object store path",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 42),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_043",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af043"]
) as dag_43:
    fail_task = PythonOperator(
        task_id="fail_af043",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF043",
            "title": "Permission denied on warehouse schema",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 43),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_044",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af044"]
) as dag_44:
    fail_task = PythonOperator(
        task_id="fail_af044",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF044",
            "title": "Network egress blocked to API source",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 44),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_045",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af045"]
) as dag_45:
    fail_task = PythonOperator(
        task_id="fail_af045",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF045",
            "title": "DNS resolution failure for source host",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 45),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_046",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af046"]
) as dag_46:
    fail_task = PythonOperator(
        task_id="fail_af046",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF046",
            "title": "TLS certificate expired upstream API",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 46),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_047",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af047"]
) as dag_47:
    fail_task = PythonOperator(
        task_id="fail_af047",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF047",
            "title": "API rate limit exceeded",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 47),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_048",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af048"]
) as dag_48:
    fail_task = PythonOperator(
        task_id="fail_af048",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF048",
            "title": "Pagination bug skips records",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 48),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_049",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af049"]
) as dag_49:
    fail_task = PythonOperator(
        task_id="fail_af049",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF049",
            "title": "Incremental watermark not persisted",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 49),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_050",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af050"]
) as dag_50:
    fail_task = PythonOperator(
        task_id="fail_af050",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF050",
            "title": "Watermark timezone mismatch",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 50),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_051",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "orchestration", "af051"]
) as dag_51:
    fail_task = PythonOperator(
        task_id="fail_af051",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF051",
            "title": "Backfill overlaps with daily load",
            "category": "orchestration",
            "behavior": _behavior_from_category("orchestration", 51),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_052",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "code", "af052"]
) as dag_52:
    fail_task = PythonOperator(
        task_id="fail_af052",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF052",
            "title": "Hardcoded environment path",
            "category": "code",
            "behavior": _behavior_from_category("code", 52),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_053",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "code", "af053"]
) as dag_53:
    fail_task = PythonOperator(
        task_id="fail_af053",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF053",
            "title": "Configuration key missing",
            "category": "code",
            "behavior": _behavior_from_category("code", 53),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_054",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "code", "af054"]
) as dag_54:
    fail_task = PythonOperator(
        task_id="fail_af054",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF054",
            "title": "YAML parse error in pipeline config",
            "category": "code",
            "behavior": _behavior_from_category("code", 54),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_055",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "release", "af055"]
) as dag_55:
    fail_task = PythonOperator(
        task_id="fail_af055",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF055",
            "title": "Feature flag enabled without migration",
            "category": "release",
            "behavior": _behavior_from_category("release", 55),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_056",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "transformation", "af056"]
) as dag_56:
    fail_task = PythonOperator(
        task_id="fail_af056",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF056",
            "title": "Incorrect join condition duplicates rows",
            "category": "transformation",
            "behavior": _behavior_from_category("transformation", 56),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_057",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "transformation", "af057"]
) as dag_57:
    fail_task = PythonOperator(
        task_id="fail_af057",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF057",
            "title": "Window function partition bug",
            "category": "transformation",
            "behavior": _behavior_from_category("transformation", 57),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_058",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "transformation", "af058"]
) as dag_58:
    fail_task = PythonOperator(
        task_id="fail_af058",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF058",
            "title": "Incorrect dedupe ordering",
            "category": "transformation",
            "behavior": _behavior_from_category("transformation", 58),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_059",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "data_quality", "af059"]
) as dag_59:
    fail_task = PythonOperator(
        task_id="fail_af059",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF059",
            "title": "Currency conversion table stale",
            "category": "data_quality",
            "behavior": _behavior_from_category("data_quality", 59),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_060",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "data_quality", "af060"]
) as dag_60:
    fail_task = PythonOperator(
        task_id="fail_af060",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF060",
            "title": "Business calendar mismatch",
            "category": "data_quality",
            "behavior": _behavior_from_category("data_quality", 60),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_061",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "governance", "af061"]
) as dag_61:
    fail_task = PythonOperator(
        task_id="fail_af061",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF061",
            "title": "PII masking regression",
            "category": "governance",
            "behavior": _behavior_from_category("governance", 61),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_062",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "governance", "af062"]
) as dag_62:
    fail_task = PythonOperator(
        task_id="fail_af062",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF062",
            "title": "GDPR delete request not propagated",
            "category": "governance",
            "behavior": _behavior_from_category("governance", 62),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_063",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "contract", "af063"]
) as dag_63:
    fail_task = PythonOperator(
        task_id="fail_af063",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF063",
            "title": "Data contract violation field removed",
            "category": "contract",
            "behavior": _behavior_from_category("contract", 63),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_064",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "contract", "af064"]
) as dag_64:
    fail_task = PythonOperator(
        task_id="fail_af064",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF064",
            "title": "Data contract violation semantic change",
            "category": "contract",
            "behavior": _behavior_from_category("contract", 64),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_065",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "semantic", "af065"]
) as dag_65:
    fail_task = PythonOperator(
        task_id="fail_af065",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF065",
            "title": "Metric definition drift",
            "category": "semantic",
            "behavior": _behavior_from_category("semantic", 65),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_066",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "modeling", "af066"]
) as dag_66:
    fail_task = PythonOperator(
        task_id="fail_af066",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF066",
            "title": "Dimension surrogate key collision",
            "category": "modeling",
            "behavior": _behavior_from_category("modeling", 66),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_067",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "modeling", "af067"]
) as dag_67:
    fail_task = PythonOperator(
        task_id="fail_af067",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF067",
            "title": "Surrogate key negative overflow",
            "category": "modeling",
            "behavior": _behavior_from_category("modeling", 67),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_068",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "modeling", "af068"]
) as dag_68:
    fail_task = PythonOperator(
        task_id="fail_af068",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF068",
            "title": "Fact grain violation",
            "category": "modeling",
            "behavior": _behavior_from_category("modeling", 68),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_069",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "timeliness", "af069"]
) as dag_69:
    fail_task = PythonOperator(
        task_id="fail_af069",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF069",
            "title": "Snapshot not aligned to source cutoff",
            "category": "timeliness",
            "behavior": _behavior_from_category("timeliness", 69),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_070",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "warehouse", "af070"]
) as dag_70:
    fail_task = PythonOperator(
        task_id="fail_af070",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF070",
            "title": "Merge statement updates wrong rows",
            "category": "warehouse",
            "behavior": _behavior_from_category("warehouse", 70),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_071",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "warehouse", "af071"]
) as dag_71:
    fail_task = PythonOperator(
        task_id="fail_af071",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF071",
            "title": "Delete without where clause safeguard",
            "category": "warehouse",
            "behavior": _behavior_from_category("warehouse", 71),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_072",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "warehouse", "af072"]
) as dag_72:
    fail_task = PythonOperator(
        task_id="fail_af072",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF072",
            "title": "Long-running query timeout",
            "category": "warehouse",
            "behavior": _behavior_from_category("warehouse", 72),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_073",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "warehouse", "af073"]
) as dag_73:
    fail_task = PythonOperator(
        task_id="fail_af073",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF073",
            "title": "Warehouse suspended mid-job",
            "category": "warehouse",
            "behavior": _behavior_from_category("warehouse", 73),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_074",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "warehouse", "af074"]
) as dag_74:
    fail_task = PythonOperator(
        task_id="fail_af074",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF074",
            "title": "Temp table name collision",
            "category": "warehouse",
            "behavior": _behavior_from_category("warehouse", 74),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_075",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "schema", "af075"]
) as dag_75:
    fail_task = PythonOperator(
        task_id="fail_af075",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF075",
            "title": "Case sensitivity mismatch on column names",
            "category": "schema",
            "behavior": _behavior_from_category("schema", 75),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_076",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af076"]
) as dag_76:
    fail_task = PythonOperator(
        task_id="fail_af076",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF076",
            "title": "UTF-8 decoding error in source file",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 76),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_077",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "ingestion", "af077"]
) as dag_77:
    fail_task = PythonOperator(
        task_id="fail_af077",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF077",
            "title": "JSON path missing nested field",
            "category": "ingestion",
            "behavior": _behavior_from_category("ingestion", 77),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_078",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "transformation", "af078"]
) as dag_78:
    fail_task = PythonOperator(
        task_id="fail_af078",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF078",
            "title": "Array explosion cardinality blowup",
            "category": "transformation",
            "behavior": _behavior_from_category("transformation", 78),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_079",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "transformation", "af079"]
) as dag_79:
    fail_task = PythonOperator(
        task_id="fail_af079",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF079",
            "title": "Cross join accidentally introduced",
            "category": "transformation",
            "behavior": _behavior_from_category("transformation", 79),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_080",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "governance", "af080"]
) as dag_80:
    fail_task = PythonOperator(
        task_id="fail_af080",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF080",
            "title": "Row-level security policy blocks job",
            "category": "governance",
            "behavior": _behavior_from_category("governance", 80),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_081",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "orchestration", "af081"]
) as dag_81:
    fail_task = PythonOperator(
        task_id="fail_af081",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF081",
            "title": "Airflow queue saturation",
            "category": "orchestration",
            "behavior": _behavior_from_category("orchestration", 81),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_082",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "orchestration", "af082"]
) as dag_82:
    fail_task = PythonOperator(
        task_id="fail_af082",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF082",
            "title": "Zombie task after worker restart",
            "category": "orchestration",
            "behavior": _behavior_from_category("orchestration", 82),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_083",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "orchestration", "af083"]
) as dag_83:
    fail_task = PythonOperator(
        task_id="fail_af083",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF083",
            "title": "Scheduler lag from too many DAG parses",
            "category": "orchestration",
            "behavior": _behavior_from_category("orchestration", 83),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_084",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "orchestration", "af084"]
) as dag_84:
    fail_task = PythonOperator(
        task_id="fail_af084",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF084",
            "title": "Task heartbeat timeout",
            "category": "orchestration",
            "behavior": _behavior_from_category("orchestration", 84),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_085",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "orchestration", "af085"]
) as dag_85:
    fail_task = PythonOperator(
        task_id="fail_af085",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF085",
            "title": "Incorrect retry delay exponential overflow",
            "category": "orchestration",
            "behavior": _behavior_from_category("orchestration", 85),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_086",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "observability", "af086"]
) as dag_86:
    fail_task = PythonOperator(
        task_id="fail_af086",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF086",
            "title": "SLA miss not alerting",
            "category": "observability",
            "behavior": _behavior_from_category("observability", 86),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_087",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "observability", "af087"]
) as dag_87:
    fail_task = PythonOperator(
        task_id="fail_af087",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF087",
            "title": "Alert flood duplicate incidents",
            "category": "observability",
            "behavior": _behavior_from_category("observability", 87),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_088",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "observability", "af088"]
) as dag_88:
    fail_task = PythonOperator(
        task_id="fail_af088",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF088",
            "title": "Log retention too short for RCA",
            "category": "observability",
            "behavior": _behavior_from_category("observability", 88),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_089",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "observability", "af089"]
) as dag_89:
    fail_task = PythonOperator(
        task_id="fail_af089",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF089",
            "title": "Trace context lost across tasks",
            "category": "observability",
            "behavior": _behavior_from_category("observability", 89),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_090",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "integration", "af090"]
) as dag_90:
    fail_task = PythonOperator(
        task_id="fail_af090",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF090",
            "title": "Upstream API returns partial success",
            "category": "integration",
            "behavior": _behavior_from_category("integration", 90),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_091",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "integration", "af091"]
) as dag_91:
    fail_task = PythonOperator(
        task_id="fail_af091",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF091",
            "title": "Webhook callback not idempotent",
            "category": "integration",
            "behavior": _behavior_from_category("integration", 91),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_092",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "integration", "af092"]
) as dag_92:
    fail_task = PythonOperator(
        task_id="fail_af092",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF092",
            "title": "External dependency maintenance window",
            "category": "integration",
            "behavior": _behavior_from_category("integration", 92),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_093",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af093"]
) as dag_93:
    fail_task = PythonOperator(
        task_id="fail_af093",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF093",
            "title": "Container image pull failure",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 93),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_094",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af094"]
) as dag_94:
    fail_task = PythonOperator(
        task_id="fail_af094",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF094",
            "title": "Disk full on worker node",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 94),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_095",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af095"]
) as dag_95:
    fail_task = PythonOperator(
        task_id="fail_af095",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF095",
            "title": "Clock skew between nodes",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 95),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_096",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "platform", "af096"]
) as dag_96:
    fail_task = PythonOperator(
        task_id="fail_af096",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF096",
            "title": "KMS decryption failure",
            "category": "platform",
            "behavior": _behavior_from_category("platform", 96),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_097",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "reliability", "af097"]
) as dag_97:
    fail_task = PythonOperator(
        task_id="fail_af097",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF097",
            "title": "Orphaned staging files after failure",
            "category": "reliability",
            "behavior": _behavior_from_category("reliability", 97),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_098",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "reliability", "af098"]
) as dag_98:
    fail_task = PythonOperator(
        task_id="fail_af098",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF098",
            "title": "Compaction job skipped causing file explosion",
            "category": "reliability",
            "behavior": _behavior_from_category("reliability", 98),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_099",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "release", "af099"]
) as dag_99:
    fail_task = PythonOperator(
        task_id="fail_af099",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF099",
            "title": "Rollback script incompatible with latest schema",
            "category": "release",
            "behavior": _behavior_from_category("release", 99),
        },
    )


with DAG(
    dag_id="patchit_airflow_issue_100",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "failure-pack", "release", "af100"]
) as dag_100:
    fail_task = PythonOperator(
        task_id="fail_af100",
        python_callable=_fail,
        op_kwargs={
            "issue_id": "AF100",
            "title": "Canary check ignored before full rollout",
            "category": "release",
            "behavior": _behavior_from_category("release", 100),
        },
    )


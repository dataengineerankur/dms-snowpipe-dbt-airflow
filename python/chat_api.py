import json
import os
from typing import Dict, List, Optional

import snowflake.connector
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Snowflake Cost Copilot Chat API")


class ChatRequest(BaseModel):
    question: str
    time_range: Optional[str] = "7d"


def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "COST_COPILOT"),
    )


def _time_filter_sql(time_range: str) -> str:
    if not time_range:
        return "DATEADD(DAY, -7, CURRENT_TIMESTAMP())"
    t = time_range.lower().strip()
    if t in ("24h", "last_24h", "last24h", "1d"):
        return "DATEADD(HOUR, -24, CURRENT_TIMESTAMP())"
    if t in ("7d", "last_7d", "last7d"):
        return "DATEADD(DAY, -7, CURRENT_TIMESTAMP())"
    if t in ("30d", "last_30d", "last30d"):
        return "DATEADD(DAY, -30, CURRENT_TIMESTAMP())"
    return "DATEADD(DAY, -7, CURRENT_TIMESTAMP())"


def _classify_intent(question: str) -> str:
    q = question.lower()
    if "spill" in q:
        return "spill"
    if "pruning" in q or "scan" in q:
        return "pruning"
    if "idle" in q or "warehouse" in q:
        return "warehouse_idle"
    if "pipe" in q or "ingest" in q or "copy" in q:
        return "pipe"
    if "task" in q:
        return "tasks"
    if "spike" in q:
        return "cost_spike"
    if "recommend" in q:
        return "recommendations"
    return "heavy_jobs"


def _rows_to_dicts(cursor) -> List[Dict]:
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _retrieve(intent: str, time_range: str):
    tf = _time_filter_sql(time_range)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if intent in ("heavy_jobs", "cost_spike"):
                cur.execute(
                    f"""
                    SELECT query_tag, warehouse_name, user_name, query_count, credits_total, elapsed_seconds, scanned_gb
                    FROM COST_COPILOT.V_HEAVY_TAGS_7D
                    WHERE last_seen >= {tf}
                    ORDER BY credits_total DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = "SELECT * FROM COST_COPILOT.V_HEAVY_TAGS_7D ORDER BY credits_total DESC LIMIT 20;"
            elif intent == "spill":
                cur.execute(
                    f"""
                    SELECT query_id, query_tag, warehouse_name, spill_gb, credits_total, start_time
                    FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS
                    WHERE start_time >= {tf}
                      AND spill_gb > 0
                    ORDER BY spill_gb DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = (
                    "SELECT query_id, query_tag, warehouse_name, spill_gb, credits_total "
                    "FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS WHERE spill_gb > 0 ORDER BY spill_gb DESC LIMIT 50;"
                )
            elif intent == "pruning":
                cur.execute(
                    f"""
                    SELECT query_id, query_tag, warehouse_name, pruning_ratio, scanned_gb, credits_total, start_time
                    FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS
                    WHERE start_time >= {tf}
                      AND (pruning_ratio > 0.8 OR scanned_gb > 10)
                    ORDER BY scanned_gb DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = (
                    "SELECT query_id, query_tag, pruning_ratio, scanned_gb FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS "
                    "WHERE pruning_ratio > 0.8 OR scanned_gb > 10 ORDER BY scanned_gb DESC LIMIT 50;"
                )
            elif intent == "warehouse_idle":
                cur.execute(
                    f"""
                    SELECT warehouse_name, usage_day, credits_used, avg_running, avg_queued
                    FROM COST_COPILOT.V_WAREHOUSE_IDLE_SIGNALS
                    WHERE usage_day >= DATE({tf})
                    ORDER BY credits_used DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = (
                    "SELECT * FROM COST_COPILOT.V_WAREHOUSE_IDLE_SIGNALS "
                    "WHERE avg_running < 0.3 ORDER BY credits_used DESC LIMIT 20;"
                )
            elif intent == "pipe":
                cur.execute(
                    """
                    SELECT usage_date, pipe_name, credits_used, failure_count, success_count, small_file_loads
                    FROM COST_COPILOT.V_PIPE_HEALTH_SIGNALS
                    ORDER BY failure_count DESC, credits_used DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = "SELECT * FROM COST_COPILOT.V_PIPE_HEALTH_SIGNALS ORDER BY failure_count DESC LIMIT 30;"
            elif intent == "tasks":
                cur.execute(
                    f"""
                    SELECT usage_date, task_name, database_name, schema_name, state, run_count, failed_count, avg_duration_seconds
                    FROM COST_COPILOT.FACT_TASK_DAILY
                    WHERE usage_date >= DATE({tf})
                    ORDER BY failed_count DESC, run_count DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = "SELECT * FROM COST_COPILOT.FACT_TASK_DAILY ORDER BY usage_date DESC, failed_count DESC LIMIT 50;"
            else:
                cur.execute(
                    """
                    SELECT rec_id, rule_name, recommendation_type, object_name, risk, est_savings, ai_summary, confidence_score
                    FROM COST_COPILOT.V_TOP_RECOMMENDATIONS
                    ORDER BY created_at DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = "SELECT * FROM COST_COPILOT.V_TOP_RECOMMENDATIONS ORDER BY created_at DESC LIMIT 20;"

            cur.execute(
                """
                SELECT rec_id, recommendation_type, object_name, risk, suggested_fix, ddl_sql
                FROM COST_COPILOT.RECOMMENDATIONS
                ORDER BY created_at DESC
                LIMIT 5
                """
            )
            actions = _rows_to_dicts(cur)
            return rows, actions, sql
    finally:
        conn.close()


def _build_answer(intent: str, question: str, rows: List[Dict], actions: List[Dict], sql: str):
    if not rows:
        summary = "No matching copilot evidence found for the selected time range."
    else:
        summary = f"Found {len(rows)} evidence rows for intent '{intent}' related to: {question}"

    offenders = []
    evidence_refs = []
    for row in rows[:5]:
        offenders.append(row)
        for field in ("QUERY_ID", "QUERY_TAG", "WAREHOUSE_NAME", "OBJECT_NAME", "PIPE_NAME", "TASK_NAME"):
            if field in row and row[field]:
                evidence_refs.append(str(row[field]))
    evidence_refs = sorted(set(evidence_refs))[:20]

    return {
        "summary": summary,
        "top_offenders": offenders,
        "recommended_actions": actions[:5],
        "evidence_references": evidence_refs,
        "sql_snippets": [
            sql,
            "SELECT * FROM COST_COPILOT.V_TOP_RECOMMENDATIONS ORDER BY created_at DESC LIMIT 20;",
        ],
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/chat")
def chat(req: ChatRequest):
    intent = _classify_intent(req.question)
    rows, actions, sql = _retrieve(intent, req.time_range or "7d")
    payload = _build_answer(intent, req.question, rows, actions, sql)
    payload["intent"] = intent
    payload["time_range"] = req.time_range
    payload["grounding_note"] = "Response is generated from COST_COPILOT facts/views only."
    return payload

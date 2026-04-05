import argparse
import json
import os
import uuid
from typing import List

import snowflake.connector


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


def _load_allowlist(cur) -> List[str]:
    cur.execute("SELECT config_value FROM COST_COPILOT.CONFIG WHERE config_key = 'APPLY_WAREHOUSE_ALLOWLIST'")
    row = cur.fetchone()
    if not row:
        return []
    value = row[0]
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    if isinstance(value, list):
        return value
    return []


def _is_safe_ddl(ddl_sql: str) -> bool:
    if not ddl_sql:
        return False
    normalized = " ".join(ddl_sql.upper().split())
    if not normalized.startswith("ALTER WAREHOUSE"):
        return False
    return "AUTO_SUSPEND" in normalized or "AUTO_RESUME" in normalized


def apply_actions(mode: str = "DRY_RUN") -> int:
    conn = get_conn()
    applied = 0
    mode = mode.upper()
    try:
        with conn.cursor(snowflake.connector.DictCursor) as cur:
            allowlist = {w.upper() for w in _load_allowlist(cur)}
            cur.execute(
                """
                SELECT r.rec_id, r.object_name, r.ddl_sql, r.rollback_sql, r.risk
                FROM COST_COPILOT.RECOMMENDATIONS r
                JOIN COST_COPILOT.APPROVALS a
                  ON r.rec_id = a.rec_id
                WHERE r.status = 'OPEN'
                  AND r.risk = 'LOW'
                  AND a.approved = TRUE
                  AND r.ddl_sql IS NOT NULL
                  AND r.ddl_sql <> ''
                ORDER BY r.created_at DESC
                """
            )
            candidates = cur.fetchall()
            for rec in candidates:
                warehouse = (rec["OBJECT_NAME"] or "").upper()
                ddl_sql = rec["DDL_SQL"] or ""
                rollback_sql = rec["ROLLBACK_SQL"] or ""
                action_status = "SKIPPED"
                before_state = {}
                verification_query = f"SHOW WAREHOUSES LIKE '{warehouse}'"

                if warehouse not in allowlist:
                    action_status = "SKIPPED_NOT_ALLOWLISTED"
                elif not _is_safe_ddl(ddl_sql):
                    action_status = "SKIPPED_UNSAFE_DDL"
                else:
                    cur.execute(verification_query)
                    before_state = {"warehouse": warehouse, "show_warehouses_result": cur.fetchall()}
                    if mode == "APPLY":
                        cur.execute(ddl_sql)
                        cur.execute("UPDATE COST_COPILOT.RECOMMENDATIONS SET status = 'APPLIED' WHERE rec_id = %(rec_id)s", {"rec_id": rec["REC_ID"]})
                        action_status = "APPLIED"
                        applied += 1
                    else:
                        action_status = "DRY_RUN"

                cur.execute(
                    """
                    INSERT INTO COST_COPILOT.ACTION_LOG (
                      action_id, rec_id, mode, action_status, ddl_executed, before_state,
                      rollback_sql, verification_query, executor
                    )
                    SELECT
                      %(action_id)s, %(rec_id)s, %(mode)s, %(action_status)s, %(ddl_executed)s, PARSE_JSON(%(before_state)s),
                      %(rollback_sql)s, %(verification_query)s, CURRENT_USER()
                    """,
                    {
                        "action_id": str(uuid.uuid4()),
                        "rec_id": rec["REC_ID"],
                        "mode": mode,
                        "action_status": action_status,
                        "ddl_executed": ddl_sql,
                        "before_state": json.dumps(before_state, default=str),
                        "rollback_sql": rollback_sql,
                        "verification_query": verification_query,
                    },
                )
            conn.commit()
    finally:
        conn.close()
    return applied


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["DRY_RUN", "APPLY"], default="DRY_RUN")
    args = parser.parse_args()
    applied = apply_actions(mode=args.mode)
    print(f"Executor completed in {args.mode}. Applied actions: {applied}")


if __name__ == "__main__":
    main()

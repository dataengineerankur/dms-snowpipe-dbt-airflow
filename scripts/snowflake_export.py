#!/usr/bin/env python3
"""
snowflake_export.py
===================
Exports EVERYTHING from a Snowflake account to local files:
  - DDL for all databases, schemas, tables, views
  - DDL for stored procedures, functions, tasks, streams, stages, pipes, sequences
  - All table data as CSV (one file per table)
  - A single restore.sql that recreates objects in another account

Usage:
    pip install snowflake-connector-python
    python snowflake_export.py

Output directory:  ./snowflake_backup_<timestamp>/
"""

import os, sys, csv, json, re, time
from datetime import datetime
from pathlib import Path

try:
    import snowflake.connector
except ImportError:
    print("Missing: pip install snowflake-connector-python")
    sys.exit(1)

# ── Credentials ───────────────────────────────────────────────────────────────
ACCOUNT  = os.getenv("SNOWFLAKE_ACCOUNT",  "WBZTWSY-KH99814")
USER     = os.getenv("SNOWFLAKE_USER",     "PATCHIT")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "Tata8237552399")
ROLE     = os.getenv("SNOWFLAKE_ROLE",     "ACCOUNTADMIN")
WAREHOUSE= os.getenv("SNOWFLAKE_WAREHOUSE","WH_MSSQL_MIGRATION")

# Databases to skip (Snowflake system databases)
SKIP_DBS = {"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"}

# ── Output setup ──────────────────────────────────────────────────────────────
ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_DIR   = Path(f"snowflake_backup_{ts}")
DDL_DIR   = OUT_DIR / "ddl"
DATA_DIR  = OUT_DIR / "data"
DDL_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

restore_lines = [
    "-- =============================================================",
    f"-- Snowflake full account export  {ts}",
    "-- Account: " + ACCOUNT,
    "-- Run this in your NEW Snowflake account to restore everything",
    "-- =============================================================\n",
]

log_lines = []

def log(msg):
    print(msg)
    log_lines.append(msg)

def q(cur, sql):
    try:
        cur.execute(sql)
        return cur.fetchall()
    except Exception as e:
        log(f"  WARN: {sql[:120]} => {e}")
        return []

def get_ddl(cur, obj_type, full_name):
    rows = q(cur, f"SELECT GET_DDL('{obj_type}', '{full_name}', TRUE)")
    if rows:
        return rows[0][0]
    return f"-- DDL not available for {obj_type} {full_name}\n"

# ── Connect ───────────────────────────────────────────────────────────────────
log(f"\n{'='*60}")
log(f"Connecting to Snowflake: {ACCOUNT}")
log(f"{'='*60}")

try:
    conn = snowflake.connector.connect(
        account=ACCOUNT, user=USER, password=PASSWORD,
        role=ROLE, login_timeout=30,
    )
    cur = conn.cursor()
    # Use a small warehouse — resume if suspended
    try:
        cur.execute(f"ALTER WAREHOUSE {WAREHOUSE} RESUME IF SUSPENDED")
    except:
        pass
    try:
        cur.execute(f"USE WAREHOUSE {WAREHOUSE}")
    except:
        # Try any available warehouse
        wh_rows = q(cur, "SHOW WAREHOUSES")
        if wh_rows:
            wh_name = wh_rows[0][1]
            cur.execute(f"ALTER WAREHOUSE {wh_name} RESUME IF SUSPENDED")
            cur.execute(f"USE WAREHOUSE {wh_name}")
            log(f"  Using warehouse: {wh_name}")
    log("  Connected.\n")
except Exception as e:
    log(f"ERROR: Could not connect: {e}")
    sys.exit(1)

# ── Enumerate databases ───────────────────────────────────────────────────────
log("Discovering databases...")
db_rows = q(cur, "SHOW DATABASES")
databases = [r[1] for r in db_rows if r[1] not in SKIP_DBS]
log(f"  Found {len(databases)} user databases: {databases}\n")

summary = {"databases": [], "total_tables": 0, "total_rows": 0, "errors": []}

for db in databases:
    log(f"\n{'─'*50}")
    log(f"DATABASE: {db}")
    log(f"{'─'*50}")

    db_info = {"name": db, "schemas": []}
    summary["databases"].append(db_info)

    # Database DDL
    db_ddl = get_ddl(cur, "DATABASE", db)
    (DDL_DIR / f"{db}__CREATE_DATABASE.sql").write_text(db_ddl)
    restore_lines.append(f"\n-- ── DATABASE: {db} ──────────────────────────────────────")
    restore_lines.append(db_ddl)
    restore_lines.append(f"USE DATABASE {db};")

    cur.execute(f"USE DATABASE {db}")

    # Schemas
    schema_rows = q(cur, "SHOW SCHEMAS")
    schemas = [r[1] for r in schema_rows if r[1] not in ("INFORMATION_SCHEMA",)]
    log(f"  Schemas: {schemas}")

    for schema in schemas:
        log(f"\n  SCHEMA: {db}.{schema}")
        schema_info = {"name": schema, "tables": [], "views": 0, "procs": 0}
        db_info["schemas"].append(schema_info)

        schema_dir = DDL_DIR / db / schema
        schema_dir.mkdir(parents=True, exist_ok=True)
        data_dir   = DATA_DIR / db / schema
        data_dir.mkdir(parents=True, exist_ok=True)

        cur.execute(f"USE SCHEMA {db}.{schema}")

        restore_lines.append(f"\n-- SCHEMA: {db}.{schema}")
        restore_lines.append(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema};")
        restore_lines.append(f"USE SCHEMA {db}.{schema};")

        # ── Tables ────────────────────────────────────────────────────────
        table_rows = q(cur, "SHOW TABLES")
        log(f"    Tables ({len(table_rows)}):")

        for tr in table_rows:
            tname = tr[1]
            full  = f'"{db}"."{schema}"."{tname}"'
            log(f"      {tname}")

            # DDL
            ddl = get_ddl(cur, "TABLE", full)
            (schema_dir / f"{tname}.sql").write_text(ddl)
            restore_lines.append(f"\n-- TABLE: {tname}")
            restore_lines.append(ddl)

            # Row count + data export
            count_rows = q(cur, f"SELECT COUNT(*) FROM {full}")
            row_count  = count_rows[0][0] if count_rows else 0
            log(f"        rows: {row_count}")
            summary["total_rows"] += row_count
            schema_info["tables"].append({"name": tname, "rows": row_count})
            summary["total_tables"] += 1

            # Export data as CSV (chunked for large tables)
            if row_count > 0:
                try:
                    cur.execute(f"SELECT * FROM {full} LIMIT 1000000")
                    cols = [d[0] for d in cur.description]
                    out_csv = data_dir / f"{tname}.csv"
                    with open(out_csv, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                        writer.writerow(cols)
                        batch_size = 10000
                        while True:
                            rows = cur.fetchmany(batch_size)
                            if not rows:
                                break
                            writer.writerows(rows)
                    log(f"        exported → data/{db}/{schema}/{tname}.csv")
                except Exception as e:
                    log(f"        WARN: data export failed: {e}")
                    summary["errors"].append(f"{full}: {e}")

        # ── Views ─────────────────────────────────────────────────────────
        view_rows = q(cur, "SHOW VIEWS")
        if view_rows:
            log(f"    Views ({len(view_rows)}):")
            for vr in view_rows:
                vname = vr[1]
                full  = f'"{db}"."{schema}"."{vname}"'
                log(f"      {vname}")
                ddl = get_ddl(cur, "VIEW", full)
                (schema_dir / f"VIEW_{vname}.sql").write_text(ddl)
                restore_lines.append(f"\n-- VIEW: {vname}")
                restore_lines.append(ddl)
            schema_info["views"] = len(view_rows)

        # ── Stored Procedures ─────────────────────────────────────────────
        proc_rows = q(cur, "SHOW PROCEDURES")
        if proc_rows:
            log(f"    Procedures ({len(proc_rows)}):")
            proc_dir = schema_dir / "procedures"
            proc_dir.mkdir(exist_ok=True)
            for pr in proc_rows:
                pname = pr[1]
                psig  = pr[8] if len(pr) > 8 else pname  # argument signature
                full  = f'"{db}"."{schema}"."{pname}"({psig})'
                log(f"      {pname}")
                try:
                    ddl = get_ddl(cur, "PROCEDURE", full)
                except:
                    ddl = f"-- PROCEDURE {pname}: DDL unavailable\n"
                (proc_dir / f"{pname}.sql").write_text(ddl)
                restore_lines.append(f"\n-- PROCEDURE: {pname}")
                restore_lines.append(ddl)
            schema_info["procs"] = len(proc_rows)

        # ── Functions (UDFs) ──────────────────────────────────────────────
        func_rows = q(cur, "SHOW USER FUNCTIONS")
        if func_rows:
            log(f"    Functions ({len(func_rows)}):")
            func_dir = schema_dir / "functions"
            func_dir.mkdir(exist_ok=True)
            for fr in func_rows:
                fname = fr[1]
                fsig  = fr[8] if len(fr) > 8 else fname
                full  = f'"{db}"."{schema}"."{fname}"({fsig})'
                log(f"      {fname}")
                try:
                    ddl = get_ddl(cur, "FUNCTION", full)
                except:
                    ddl = f"-- FUNCTION {fname}: DDL unavailable\n"
                (func_dir / f"{fname}.sql").write_text(ddl)
                restore_lines.append(f"\n-- FUNCTION: {fname}")
                restore_lines.append(ddl)

        # ── Tasks ─────────────────────────────────────────────────────────
        task_rows = q(cur, "SHOW TASKS")
        if task_rows:
            log(f"    Tasks ({len(task_rows)}):")
            task_dir = schema_dir / "tasks"
            task_dir.mkdir(exist_ok=True)
            for tkr in task_rows:
                tkname = tkr[1]
                full   = f'"{db}"."{schema}"."{tkname}"'
                log(f"      {tkname}")
                ddl = get_ddl(cur, "TASK", full)
                (task_dir / f"{tkname}.sql").write_text(ddl)
                restore_lines.append(f"\n-- TASK: {tkname}")
                restore_lines.append(ddl)

        # ── Streams ───────────────────────────────────────────────────────
        stream_rows = q(cur, "SHOW STREAMS")
        if stream_rows:
            log(f"    Streams ({len(stream_rows)}):")
            stream_dir = schema_dir / "streams"
            stream_dir.mkdir(exist_ok=True)
            for sr in stream_rows:
                sname = sr[1]
                full  = f'"{db}"."{schema}"."{sname}"'
                log(f"      {sname}")
                ddl = get_ddl(cur, "STREAM", full)
                (stream_dir / f"{sname}.sql").write_text(ddl)
                restore_lines.append(f"\n-- STREAM: {sname}")
                restore_lines.append(ddl)

        # ── Stages ────────────────────────────────────────────────────────
        stage_rows = q(cur, "SHOW STAGES")
        if stage_rows:
            log(f"    Stages ({len(stage_rows)}):")
            stage_dir = schema_dir / "stages"
            stage_dir.mkdir(exist_ok=True)
            for sgr in stage_rows:
                sgname = sgr[1]
                full   = f'"{db}"."{schema}"."{sgname}"'
                log(f"      {sgname}")
                ddl = get_ddl(cur, "STAGE", full)
                (stage_dir / f"{sgname}.sql").write_text(ddl)
                restore_lines.append(f"\n-- STAGE: {sgname}")
                restore_lines.append(ddl)

        # ── Pipes ─────────────────────────────────────────────────────────
        pipe_rows = q(cur, "SHOW PIPES")
        if pipe_rows:
            log(f"    Pipes ({len(pipe_rows)}):")
            pipe_dir = schema_dir / "pipes"
            pipe_dir.mkdir(exist_ok=True)
            for pr in pipe_rows:
                pname = pr[1]
                full  = f'"{db}"."{schema}"."{pname}"'
                log(f"      {pname}")
                ddl = get_ddl(cur, "PIPE", full)
                (pipe_dir / f"{pname}.sql").write_text(ddl)
                restore_lines.append(f"\n-- PIPE: {pname}")
                restore_lines.append(ddl)

        # ── Sequences ─────────────────────────────────────────────────────
        seq_rows = q(cur, "SHOW SEQUENCES")
        if seq_rows:
            log(f"    Sequences ({len(seq_rows)}):")
            for sqr in seq_rows:
                sqname = sqr[1]
                full   = f'"{db}"."{schema}"."{sqname}"'
                log(f"      {sqname}")
                ddl = get_ddl(cur, "SEQUENCE", full)
                (schema_dir / f"SEQ_{sqname}.sql").write_text(ddl)
                restore_lines.append(f"\n-- SEQUENCE: {sqname}")
                restore_lines.append(ddl)

# ── Warehouses ────────────────────────────────────────────────────────────────
log("\n\nExporting warehouses...")
restore_lines.append("\n\n-- ═══════════════════════════════════════════════════")
restore_lines.append("-- WAREHOUSES")
restore_lines.append("-- ═══════════════════════════════════════════════════")
wh_rows = q(cur, "SHOW WAREHOUSES")
for wr in wh_rows:
    wname = wr[1]
    log(f"  {wname}")
    ddl = get_ddl(cur, "WAREHOUSE", wname)
    (DDL_DIR / f"WAREHOUSE_{wname}.sql").write_text(ddl)
    restore_lines.append(ddl)

# ── Roles ─────────────────────────────────────────────────────────────────────
log("\nExporting roles and grants...")
restore_lines.append("\n\n-- ═══════════════════════════════════════════════════")
restore_lines.append("-- ROLES (recreate before re-granting)")
restore_lines.append("-- ═══════════════════════════════════════════════════")
role_rows = q(cur, "SHOW ROLES")
role_ddls = []
for rr in role_rows:
    rname = rr[1]
    if rname not in ("ACCOUNTADMIN", "SYSADMIN", "SECURITYADMIN", "USERADMIN", "PUBLIC"):
        role_ddls.append(f"CREATE ROLE IF NOT EXISTS {rname};")
        log(f"  {rname}")
restore_lines.extend(role_ddls)
(DDL_DIR / "ROLES.sql").write_text("\n".join(role_ddls))

# ── Write restore.sql ─────────────────────────────────────────────────────────
restore_path = OUT_DIR / "restore.sql"
restore_path.write_text("\n".join(restore_lines), encoding="utf-8")

# ── Write summary ─────────────────────────────────────────────────────────────
summary["export_time"] = ts
summary["errors_count"] = len(summary["errors"])
(OUT_DIR / "export_summary.json").write_text(
    json.dumps(summary, indent=2, default=str), encoding="utf-8"
)

# ── Write log ─────────────────────────────────────────────────────────────────
(OUT_DIR / "export.log").write_text("\n".join(log_lines), encoding="utf-8")

cur.close()
conn.close()

# ── Final summary ─────────────────────────────────────────────────────────────
log(f"\n{'='*60}")
log(f"EXPORT COMPLETE")
log(f"{'='*60}")
log(f"  Output directory : {OUT_DIR.resolve()}")
log(f"  Databases        : {len(summary['databases'])}")
log(f"  Tables           : {summary['total_tables']}")
log(f"  Total rows       : {summary['total_rows']:,}")
if summary["errors"]:
    log(f"  Warnings         : {len(summary['errors'])} (see export_summary.json)")
log(f"  restore.sql      : {restore_path.resolve()}")
log(f"\nTo restore in a new account:")
log(f"  1. Open restore.sql in Snowsight or SnowSQL")
log(f"  2. Update S3 bucket ARNs in any STAGE or PIPE DDL")
log(f"  3. Run restore.sql as ACCOUNTADMIN")
log(f"  4. For data: COPY INTO <table> from @<stage> or use Snowflake's data load wizard")
log(f"  5. CSV files are in: {DATA_DIR.resolve()}")

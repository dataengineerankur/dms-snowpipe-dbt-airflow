"""
Migration Live Dashboard
========================
Flask app that polls Snowflake + Airflow API and serves a real-time
migration status dashboard on http://localhost:8050

Run:
    pip install flask snowflake-connector-python requests
    python app.py
"""
import os
import threading
import time
import json
import requests
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template_string

import snowflake.connector

app = Flask(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SF_ACCOUNT  = os.getenv("SNOWFLAKE_ACCOUNT",  "WBZTWSY-KH99814")
SF_USER     = os.getenv("SNOWFLAKE_USER",     "PATCHIT")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "Tata8237552399")
SF_ROLE     = os.getenv("SNOWFLAKE_ROLE",     "ACCOUNTADMIN")
SF_WH       = os.getenv("SNOWFLAKE_WAREHOUSE","WH_MSSQL_MIGRATION")
SF_DB       = os.getenv("SNOWFLAKE_DATABASE", "MSSQL_MIGRATION_LAB")

AIRFLOW_URL  = os.getenv("AIRFLOW_URL",  "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "airflow")

DAGS = [
    "mssql_01_ingest_bronze",
    "mssql_02_silver_snapshots",
    "mssql_03_gold_transforms",
    "mssql_04_data_quality",
]

BRONZE_TABLES = [
    "CATEGORIES", "CUSTOMERS", "PRODUCTS", "ORDERS", "ORDER_ITEMS",
    "ERP_DEPARTMENTS", "ERP_EMPLOYEES", "ERP_PAYROLL_RUNS", "ERP_PAYROLL_LINES",
    "CRM_ACCOUNTS", "CRM_CONTACTS", "CRM_OPPORTUNITIES",
    "INV_WAREHOUSES", "INV_SKU", "INV_STOCK_MOVEMENTS",
]
SILVER_TABLES = [
    "SNP_CUSTOMERS", "SNP_PRODUCTS", "SNP_ORDERS",
    "SNP_ERP_EMPLOYEES", "SNP_CRM_ACCOUNTS", "SNP_CRM_OPPORTUNITIES", "SNP_INV_SKU",
]
GOLD_TABLES = [
    "DIM_CUSTOMERS", "DIM_PRODUCTS", "DIM_DATE",
    "FCT_ORDERS", "FCT_ORDER_ITEMS", "FCT_PAYROLL_SUMMARY",
    "FCT_CRM_PIPELINE", "FCT_INVENTORY_POSITION",
    "RPT_ORDER_LINE_DETAIL", "RPT_CUSTOMER_ORDER_TOTALS",
    "RPT_OPEN_ORDERS", "AUDIT_INGESTION",
]

# ── Shared state (refreshed every 30s in background thread) ──────────────────
_cache = {"dag_status": {}, "tables": {}, "cdc_events": [], "raw_stats": {}, "last_refresh": None}
_lock  = threading.Lock()


def sf_conn():
    return snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        role=SF_ROLE, warehouse=SF_WH, database=SF_DB,
        login_timeout=10, network_timeout=30,
    )


def fetch_table_counts():
    counts = {}
    try:
        con = sf_conn()
        cur = con.cursor()
        # Bronze
        for t in BRONZE_TABLES:
            try:
                cur.execute(f"SELECT COUNT(*) FROM BRONZE.{t}")
                counts[f"BRONZE.{t}"] = cur.fetchone()[0]
            except Exception:
                counts[f"BRONZE.{t}"] = None
        # Silver
        for t in SILVER_TABLES:
            try:
                cur.execute(f"SELECT COUNT(*) FROM SILVER.{t}")
                counts[f"SILVER.{t}"] = cur.fetchone()[0]
            except Exception:
                counts[f"SILVER.{t}"] = None
        # Gold
        for t in GOLD_TABLES:
            try:
                cur.execute(f"SELECT COUNT(*) FROM GOLD.{t}")
                counts[f"GOLD.{t}"] = cur.fetchone()[0]
            except Exception:
                counts[f"GOLD.{t}"] = None
        cur.close()
        con.close()
    except Exception as e:
        counts["_error"] = str(e)
    return counts


def fetch_cdc_events():
    events = []
    try:
        con = sf_conn()
        cur = con.cursor()
        cur.execute("""
            SELECT
                COALESCE(_DMS_OPERATION, V:Op::VARCHAR, 'I')                 AS op,
                COALESCE(_DMS_COMMIT_TS::VARCHAR, _LOADED_AT::VARCHAR)       AS ts,
                _LOADED_AT::VARCHAR                                          AS loaded_at,
                OBJECT_KEYS(V)                                               AS keys,
                V::VARCHAR                                                   AS payload
            FROM RAW_MSSQL.RAW_DMS_VARIANT
            ORDER BY _LOADED_AT DESC
            LIMIT 50
        """)
        rows = cur.fetchall()
        for r in rows:
            op_map = {"I": "INSERT", "U": "UPDATE", "D": "DELETE"}
            op = op_map.get(str(r[0]).upper() if r[0] else "I", str(r[0]))
            payload = {}
            try:
                payload = json.loads(r[4]) if r[4] else {}
            except Exception:
                pass
            # Detect table from payload keys
            table = detect_table(payload)
            events.append({
                "op": op,
                "ts": str(r[1]),
                "loaded_at": str(r[2]),
                "table": table,
                "preview": json.dumps(payload)[:120] + ("…" if len(json.dumps(payload)) > 120 else ""),
            })
        cur.close()
        con.close()
    except Exception as e:
        events = [{"op": "ERROR", "ts": "", "loaded_at": "", "table": "–", "preview": str(e)}]
    return events


def detect_table(payload: dict) -> str:
    keys = set(str(k).lower() for k in payload.keys())
    if "customerid" in keys:   return "CUSTOMERS"
    if "productid"  in keys:   return "PRODUCTS"
    if "orderitemid" in keys:  return "ORDER_ITEMS"
    if "orderid"    in keys:   return "ORDERS"
    if "categoryid" in keys:   return "CATEGORIES"
    if "employeeid" in keys:   return "ERP_EMPLOYEES"
    if "deptid"     in keys:   return "ERP_DEPARTMENTS"
    if "runid"      in keys:   return "ERP_PAYROLL_RUNS"
    if "lineid"     in keys:   return "ERP_PAYROLL_LINES"
    if "accountid"  in keys:   return "CRM_ACCOUNTS"
    if "contactid"  in keys:   return "CRM_CONTACTS"
    if "oppid"      in keys:   return "CRM_OPPORTUNITIES"
    if "whid"       in keys:   return "INV_WAREHOUSES"
    if "skuid"      in keys:   return "INV_SKU"
    if "movid"      in keys:   return "INV_STOCK_MOVEMENTS"
    return "UNKNOWN"


def fetch_raw_stats():
    stats = {}
    try:
        con = sf_conn()
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM RAW_MSSQL.RAW_DMS_VARIANT")
        stats["raw_total"] = cur.fetchone()[0]
        cur.execute("""
            SELECT COALESCE(_DMS_OPERATION, V:Op::VARCHAR, 'I') AS op, COUNT(*)
            FROM RAW_MSSQL.RAW_DMS_VARIANT
            GROUP BY 1
        """)
        for r in cur.fetchall():
            stats[f"op_{r[0]}"] = r[1]
        cur.execute("SELECT MIN(_LOADED_AT), MAX(_LOADED_AT) FROM RAW_MSSQL.RAW_DMS_VARIANT")
        r = cur.fetchone()
        stats["first_load"] = str(r[0]) if r[0] else None
        stats["last_load"]  = str(r[1]) if r[1] else None
        cur.close()
        con.close()
    except Exception as e:
        stats["_error"] = str(e)
    return stats


def fetch_dag_status():
    status = {}
    for dag_id in DAGS:
        try:
            resp = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
                params={"limit": 1, "order_by": "-execution_date"},
                timeout=5,
            )
            if resp.ok:
                runs = resp.json().get("dag_runs", [])
                if runs:
                    r = runs[0]
                    status[dag_id] = {
                        "state": r.get("state", "unknown"),
                        "execution_date": r.get("execution_date", ""),
                        "start_date": r.get("start_date", ""),
                        "end_date": r.get("end_date", ""),
                        "run_id": r.get("dag_run_id", ""),
                    }
                else:
                    status[dag_id] = {"state": "never_run"}
            else:
                status[dag_id] = {"state": "api_error", "detail": resp.text[:100]}
        except Exception as e:
            status[dag_id] = {"state": "unreachable", "detail": str(e)[:100]}
    return status


def refresh_loop():
    while True:
        try:
            dag_status = fetch_dag_status()
            tables     = fetch_table_counts()
            cdc_events = fetch_cdc_events()
            raw_stats  = fetch_raw_stats()
            with _lock:
                _cache["dag_status"]  = dag_status
                _cache["tables"]      = tables
                _cache["cdc_events"]  = cdc_events
                _cache["raw_stats"]   = raw_stats
                _cache["last_refresh"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            pass
        time.sleep(30)


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    with _lock:
        return jsonify(_cache)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    threading.Thread(target=lambda: (
        _cache.update({
            "dag_status": fetch_dag_status(),
            "tables": fetch_table_counts(),
            "cdc_events": fetch_cdc_events(),
            "raw_stats": fetch_raw_stats(),
            "last_refresh": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        })
    ), daemon=True).start()
    return jsonify({"ok": True})


# ── HTML dashboard ────────────────────────────────────────────────────────────

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SQL Server → Snowflake Migration Dashboard</title>
<style>
  :root {
    --bg: #080c18; --surface: #0f1425; --card: #161c30;
    --border: #1e2a45; --accent: #29b6f6; --accent2: #00e5ff;
    --green: #26c281; --yellow: #ffd54f; --red: #ef5350;
    --gray: #546e7a; --text: #e0e6f0; --dim: #7a8ba0;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; font-size: 13px; }

  /* ── Header ── */
  .header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 14px 24px; display: flex; align-items: center; gap: 16px; position: sticky; top: 0; z-index: 100; }
  .header h1 { font-size: 18px; font-weight: 600; color: var(--accent); letter-spacing: .5px; }
  .header .sub { font-size: 11px; color: var(--dim); }
  .header .spacer { flex: 1; }
  .last-refresh { font-size: 11px; color: var(--dim); }
  .btn-refresh { background: var(--accent); color: #000; border: none; padding: 6px 14px; border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: 600; }
  .btn-refresh:hover { background: var(--accent2); }

  /* ── Layout ── */
  .main { padding: 20px 24px; display: grid; gap: 20px; }
  .row { display: grid; gap: 16px; }
  .row-4 { grid-template-columns: repeat(4, 1fr); }
  .row-2 { grid-template-columns: 1fr 1fr; }
  .row-3 { grid-template-columns: 1fr 1fr 1fr; }

  /* ── Cards ── */
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
  .card-title { font-size: 11px; font-weight: 600; color: var(--dim); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
  .card-title .dot { width: 7px; height: 7px; border-radius: 50%; }

  /* ── DAG Status Cards ── */
  .dag-card { display: flex; flex-direction: column; gap: 8px; }
  .dag-name { font-size: 12px; font-weight: 600; color: var(--text); }
  .dag-state { display: inline-flex; align-items: center; gap: 5px; padding: 4px 10px; border-radius: 12px; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .5px; width: fit-content; }
  .state-success { background: #1a3a2a; color: var(--green); }
  .state-running  { background: #1a2e3f; color: var(--accent); }
  .state-failed   { background: #3a1a1a; color: var(--red); }
  .state-queued   { background: #2a2a1a; color: var(--yellow); }
  .state-never_run, .state-unknown, .state-unreachable, .state-api_error { background: #1a1a2a; color: var(--gray); }
  .dag-meta { font-size: 10px; color: var(--dim); line-height: 1.6; }
  .pulse { animation: pulse 1.5s infinite; }
  @keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:.4; } }

  /* ── Layer Tables ── */
  .layer-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(170px,1fr)); gap: 8px; }
  .table-chip { background: var(--surface); border: 1px solid var(--border); border-radius: 6px; padding: 10px 12px; display: flex; flex-direction: column; gap: 4px; }
  .table-chip .tname { font-size: 11px; font-weight: 600; color: var(--text); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .table-chip .tcount { font-size: 20px; font-weight: 700; color: var(--accent); }
  .table-chip .trows  { font-size: 10px; color: var(--dim); }
  .table-chip.zero   { border-color: #3a1a1a; }
  .table-chip.zero .tcount { color: var(--red); }
  .table-chip.ok     { border-color: #1a3a2a; }
  .table-chip.ok .tcount { color: var(--green); }
  .table-chip.null-count  { border-color: #2a2a1a; }
  .table-chip.null-count .tcount { color: var(--yellow); font-size: 14px; }

  /* ── Progress bars ── */
  .progress-row { margin-bottom: 6px; }
  .progress-label { display: flex; justify-content: space-between; font-size: 11px; margin-bottom: 3px; }
  .progress-bar { height: 6px; background: var(--border); border-radius: 3px; overflow: hidden; }
  .progress-fill { height: 100%; border-radius: 3px; transition: width .6s; }
  .fill-green  { background: var(--green); }
  .fill-yellow { background: var(--yellow); }
  .fill-blue   { background: var(--accent); }

  /* ── CDC Stream ── */
  .cdc-table { width: 100%; border-collapse: collapse; font-size: 11px; }
  .cdc-table th { background: var(--surface); color: var(--dim); font-weight: 600; text-transform: uppercase; letter-spacing: .5px; padding: 6px 10px; text-align: left; border-bottom: 1px solid var(--border); position: sticky; top: 0; }
  .cdc-table td { padding: 6px 10px; border-bottom: 1px solid #111827; vertical-align: middle; }
  .cdc-table tr:hover td { background: rgba(255,255,255,.02); }
  .op-badge { display: inline-flex; padding: 2px 8px; border-radius: 3px; font-size: 10px; font-weight: 700; }
  .op-INSERT { background: #1a3a2a; color: var(--green); }
  .op-UPDATE { background: #1a2e3f; color: var(--accent); }
  .op-DELETE { background: #3a1a1a; color: var(--red); }
  .op-ERROR  { background: #3a1a1a; color: var(--red); }
  .cdc-wrap { max-height: 320px; overflow-y: auto; }
  .payload-preview { font-family: monospace; color: var(--dim); max-width: 380px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

  /* ── Stats ── */
  .stat-grid { display: grid; grid-template-columns: repeat(3,1fr); gap: 12px; }
  .stat-box { text-align: center; padding: 12px; background: var(--surface); border-radius: 6px; border: 1px solid var(--border); }
  .stat-val { font-size: 28px; font-weight: 700; color: var(--accent); }
  .stat-lbl { font-size: 10px; color: var(--dim); margin-top: 2px; text-transform: uppercase; letter-spacing: .5px; }

  /* ── Timeline ── */
  .timeline { padding: 0; list-style: none; }
  .tl-item { display: flex; gap: 12px; padding: 8px 0; border-bottom: 1px solid var(--border); }
  .tl-dot { width: 10px; height: 10px; border-radius: 50%; margin-top: 2px; flex-shrink: 0; }
  .tl-content { flex: 1; }
  .tl-title { font-size: 12px; font-weight: 600; }
  .tl-meta { font-size: 10px; color: var(--dim); }

  @media (max-width: 900px) { .row-4 { grid-template-columns: 1fr 1fr; } .row-3 { grid-template-columns: 1fr; } }
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>⬡ Migration Dashboard</h1>
    <div class="sub">SQL Server → Snowflake · MSSQL_MIGRATION_LAB</div>
  </div>
  <div class="spacer"></div>
  <span class="last-refresh" id="lastRefresh">Loading…</span>
  <button class="btn-refresh" onclick="forceRefresh()">↺ Refresh</button>
</div>

<div class="main">

  <!-- ── DAG Pipeline Status ── -->
  <div>
    <div style="font-size:12px;font-weight:600;color:var(--dim);text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;">Pipeline Status</div>
    <div class="row row-4" id="dagRow">
      <div class="card" style="text-align:center;color:var(--dim)">Loading…</div>
    </div>
  </div>

  <!-- ── Summary Stats ── -->
  <div class="row row-3">
    <div class="card">
      <div class="card-title"><span class="dot" style="background:var(--accent)"></span>RAW Layer Stats</div>
      <div class="stat-grid" id="rawStats">
        <div class="stat-box"><div class="stat-val" id="stat-raw-total">–</div><div class="stat-lbl">Total RAW Rows</div></div>
        <div class="stat-box"><div class="stat-val" id="stat-inserts">–</div><div class="stat-lbl">Inserts</div></div>
        <div class="stat-box"><div class="stat-val" id="stat-updates">–</div><div class="stat-lbl">Updates</div></div>
      </div>
    </div>
    <div class="card">
      <div class="card-title"><span class="dot" style="background:var(--green)"></span>Migration Coverage</div>
      <div id="coverageContent">Loading…</div>
    </div>
    <div class="card">
      <div class="card-title"><span class="dot" style="background:var(--yellow)"></span>Load Window</div>
      <div id="loadWindow" style="font-size:12px;line-height:2;color:var(--dim)">Loading…</div>
    </div>
  </div>

  <!-- ── Bronze Layer ── -->
  <div class="card">
    <div class="card-title"><span class="dot" style="background:#cd7f32"></span>Bronze Layer · 15 tables (RAW → Typed)</div>
    <div class="layer-grid" id="bronzeGrid">Loading…</div>
  </div>

  <!-- ── Silver + Gold ── -->
  <div class="row row-2">
    <div class="card">
      <div class="card-title"><span class="dot" style="background:#c0c0c0"></span>Silver Layer · SCD Type-2 Snapshots</div>
      <div class="layer-grid" id="silverGrid">Loading…</div>
    </div>
    <div class="card">
      <div class="card-title"><span class="dot" style="background:#ffd700"></span>Gold Layer · Analytics Tables</div>
      <div class="layer-grid" id="goldGrid">Loading…</div>
    </div>
  </div>

  <!-- ── CDC Live Stream ── -->
  <div class="card">
    <div class="card-title">
      <span class="dot pulse" style="background:var(--green)"></span>
      Live CDC Stream · Last 50 events in RAW_DMS_VARIANT
      <span style="margin-left:auto;font-size:10px;color:var(--dim)" id="cdcCount"></span>
    </div>
    <div class="cdc-wrap">
      <table class="cdc-table">
        <thead>
          <tr>
            <th>Op</th><th>Table</th><th>Loaded At</th><th>CDC Commit TS</th><th>Payload Preview</th>
          </tr>
        </thead>
        <tbody id="cdcBody">
          <tr><td colspan="5" style="color:var(--dim);text-align:center;padding:20px">Loading…</td></tr>
        </tbody>
      </table>
    </div>
  </div>

</div>

<script>
const DAG_LABELS = {
  "mssql_01_ingest_bronze":    "01 · Ingest Bronze",
  "mssql_02_silver_snapshots": "02 · Silver Snapshots",
  "mssql_03_gold_transforms":  "03 · Gold Transforms",
  "mssql_04_data_quality":     "04 · Data Quality",
};

function fmt(n) {
  if (n === null || n === undefined) return '?';
  return n.toLocaleString();
}

function stateClass(s) {
  if (!s) return 'state-unknown';
  return 'state-' + s.replace('_', '-');
}

function stateIcon(s) {
  if (s === 'success')  return '✓';
  if (s === 'running')  return '●';
  if (s === 'failed')   return '✕';
  if (s === 'queued')   return '◌';
  return '–';
}

function renderDags(dagStatus) {
  const row = document.getElementById('dagRow');
  row.innerHTML = '';
  const order = Object.keys(DAG_LABELS);
  order.forEach(dagId => {
    const d = dagStatus[dagId] || {state: 'unknown'};
    const s = d.state || 'unknown';
    const isRunning = s === 'running' || s === 'queued';
    const label = DAG_LABELS[dagId];
    const execDate = d.execution_date ? d.execution_date.replace('T',' ').slice(0,19) + ' UTC' : '–';
    const endDate  = d.end_date ? d.end_date.replace('T',' ').slice(0,19) + ' UTC' : (isRunning ? 'In Progress' : '–');
    row.innerHTML += `
      <div class="card dag-card">
        <div class="dag-name">${label}</div>
        <div class="dag-state ${stateClass(s)} ${isRunning ? 'pulse' : ''}">
          ${stateIcon(s)} ${s.replace(/_/g,' ')}
        </div>
        <div class="dag-meta">
          <div>Run: ${execDate}</div>
          <div>End: ${endDate}</div>
        </div>
      </div>`;
  });
}

function chipClass(count) {
  if (count === null || count === undefined) return 'null-count';
  if (count === 0) return 'zero';
  return 'ok';
}

function renderLayerGrid(elementId, prefix, tables, tableData) {
  const el = document.getElementById(elementId);
  el.innerHTML = tables.map(t => {
    const key = prefix + '.' + t;
    const count = tableData[key];
    const cls = chipClass(count);
    const display = count === null || count === undefined ? 'ERR' : fmt(count);
    return `<div class="table-chip ${cls}">
      <div class="tname">${t}</div>
      <div class="tcount">${display}</div>
      <div class="trows">${count > 0 ? 'rows' : count === 0 ? 'empty' : 'not found'}</div>
    </div>`;
  }).join('');
}

function renderCoverage(tables) {
  const bronzeTotal = ['CATEGORIES','CUSTOMERS','PRODUCTS','ORDERS','ORDER_ITEMS',
    'ERP_DEPARTMENTS','ERP_EMPLOYEES','ERP_PAYROLL_RUNS','ERP_PAYROLL_LINES',
    'CRM_ACCOUNTS','CRM_CONTACTS','CRM_OPPORTUNITIES','INV_WAREHOUSES','INV_SKU','INV_STOCK_MOVEMENTS'];
  const silverTotal = ['SNP_CUSTOMERS','SNP_PRODUCTS','SNP_ORDERS','SNP_ERP_EMPLOYEES',
    'SNP_CRM_ACCOUNTS','SNP_CRM_OPPORTUNITIES','SNP_INV_SKU'];
  const goldTotal   = ['DIM_CUSTOMERS','DIM_PRODUCTS','DIM_DATE','FCT_ORDERS','FCT_ORDER_ITEMS',
    'FCT_PAYROLL_SUMMARY','FCT_CRM_PIPELINE','FCT_INVENTORY_POSITION',
    'RPT_ORDER_LINE_DETAIL','RPT_CUSTOMER_ORDER_TOTALS','RPT_OPEN_ORDERS','AUDIT_INGESTION'];

  const bronzeFilled = bronzeTotal.filter(t => (tables['BRONZE.'+t] || 0) > 0).length;
  const silverFilled = silverTotal.filter(t => (tables['SILVER.'+t] || 0) > 0).length;
  const goldFilled   = goldTotal.filter(t => (tables['GOLD.'+t] || 0) > 0).length;

  const bp = Math.round(bronzeFilled / bronzeTotal.length * 100);
  const sp = Math.round(silverFilled / silverTotal.length * 100);
  const gp = Math.round(goldFilled   / goldTotal.length   * 100);

  document.getElementById('coverageContent').innerHTML = `
    <div class="progress-row">
      <div class="progress-label"><span>Bronze</span><span>${bronzeFilled}/${bronzeTotal.length} · ${bp}%</span></div>
      <div class="progress-bar"><div class="progress-fill fill-green" style="width:${bp}%"></div></div>
    </div>
    <div class="progress-row">
      <div class="progress-label"><span>Silver</span><span>${silverFilled}/${silverTotal.length} · ${sp}%</span></div>
      <div class="progress-bar"><div class="progress-fill fill-yellow" style="width:${sp}%"></div></div>
    </div>
    <div class="progress-row">
      <div class="progress-label"><span>Gold</span><span>${goldFilled}/${goldTotal.length} · ${gp}%</span></div>
      <div class="progress-bar"><div class="progress-fill fill-blue" style="width:${gp}%"></div></div>
    </div>`;
}

function renderCDC(events) {
  const tbody = document.getElementById('cdcBody');
  document.getElementById('cdcCount').textContent = events.length + ' events';
  if (!events.length) {
    tbody.innerHTML = '<tr><td colspan="5" style="color:var(--dim);text-align:center;padding:20px">No events found</td></tr>';
    return;
  }
  tbody.innerHTML = events.map(e => `
    <tr>
      <td><span class="op-badge op-${e.op}">${e.op}</span></td>
      <td style="font-weight:600">${e.table}</td>
      <td style="color:var(--dim)">${e.loaded_at ? e.loaded_at.slice(0,19) : '–'}</td>
      <td style="color:var(--dim)">${e.ts ? e.ts.slice(0,19) : '–'}</td>
      <td><div class="payload-preview">${e.preview}</div></td>
    </tr>`).join('');
}

function renderRawStats(raw) {
  document.getElementById('stat-raw-total').textContent = fmt(raw.raw_total);
  document.getElementById('stat-inserts').textContent = fmt(raw['op_I'] || raw['op_null'] || raw['op_None']);
  document.getElementById('stat-updates').textContent = fmt(raw['op_U'] || 0);

  if (raw.first_load && raw.last_load) {
    document.getElementById('loadWindow').innerHTML = `
      <div><span style="color:var(--dim)">First loaded:</span> ${raw.first_load.slice(0,19)}</div>
      <div><span style="color:var(--dim)">Last loaded: </span> ${raw.last_load.slice(0,19)}</div>
      <div style="margin-top:8px;color:var(--dim)">Auto-refresh every 30s</div>`;
  }
}

async function loadData() {
  try {
    const resp = await fetch('/api/status');
    const data = await resp.json();

    document.getElementById('lastRefresh').textContent = 'Last refresh: ' + (data.last_refresh || '–');

    if (data.dag_status) renderDags(data.dag_status);
    if (data.tables) {
      renderLayerGrid('bronzeGrid', 'BRONZE',
        ['CATEGORIES','CUSTOMERS','PRODUCTS','ORDERS','ORDER_ITEMS',
         'ERP_DEPARTMENTS','ERP_EMPLOYEES','ERP_PAYROLL_RUNS','ERP_PAYROLL_LINES',
         'CRM_ACCOUNTS','CRM_CONTACTS','CRM_OPPORTUNITIES','INV_WAREHOUSES','INV_SKU','INV_STOCK_MOVEMENTS'],
        data.tables);
      renderLayerGrid('silverGrid', 'SILVER',
        ['SNP_CUSTOMERS','SNP_PRODUCTS','SNP_ORDERS','SNP_ERP_EMPLOYEES',
         'SNP_CRM_ACCOUNTS','SNP_CRM_OPPORTUNITIES','SNP_INV_SKU'],
        data.tables);
      renderLayerGrid('goldGrid', 'GOLD',
        ['DIM_CUSTOMERS','DIM_PRODUCTS','DIM_DATE','FCT_ORDERS','FCT_ORDER_ITEMS',
         'FCT_PAYROLL_SUMMARY','FCT_CRM_PIPELINE','FCT_INVENTORY_POSITION',
         'RPT_ORDER_LINE_DETAIL','RPT_CUSTOMER_ORDER_TOTALS','RPT_OPEN_ORDERS','AUDIT_INGESTION'],
        data.tables);
      renderCoverage(data.tables);
    }
    if (data.cdc_events) renderCDC(data.cdc_events);
    if (data.raw_stats)  renderRawStats(data.raw_stats);
  } catch(e) {
    console.error('Fetch failed:', e);
  }
}

async function forceRefresh() {
  const btn = document.querySelector('.btn-refresh');
  btn.textContent = '↻ Refreshing…';
  btn.disabled = true;
  await fetch('/api/refresh', { method: 'POST' });
  await new Promise(r => setTimeout(r, 3000));
  await loadData();
  btn.textContent = '↺ Refresh';
  btn.disabled = false;
}

// Initial load + auto-refresh every 30s
loadData();
setInterval(loadData, 30000);
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


if __name__ == "__main__":
    # Start background refresh thread
    t = threading.Thread(target=refresh_loop, daemon=True)
    t.start()
    print("Dashboard starting on http://localhost:8050")
    print("First data load may take 30–60s (Snowflake cold start)")
    app.run(host="0.0.0.0", port=8050, debug=False)

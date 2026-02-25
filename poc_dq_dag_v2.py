"""
BigTapp POC — Daily DQ Pipeline DAG v2

Schedule: Daily at 01:00 UTC
Pipeline (full):

  bronze_to_silver              (transform raw → curated)
        │
  run_ge_dq_v2                  (GE checks on silver, row-level findings)
        │
        ├── refresh_scorecard
        ├── refresh_duplicate_cif
        ├── refresh_residency
        │         │
        │   export_reports
        │
  starrocks_load                (sync SQLite → StarRocks)
        │
  omd_incidents                 (create OMD incidents for failures)
        │
  rule_sync                     (two-way OMD ↔ GE rule sync)

Each task SSHes into the OMD node (centos@172.31.23.212) and runs
the corresponding Python script in ~/poc_dq/.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ── Config ────────────────────────────────────────────────────────────────────

OMD_HOST = "centos@172.31.23.212"
OMD_KEY  = "/home/ec2-user/omd_node.pem"
POC_DIR  = "~/poc_dq"

SSH_CMD = (
    f"ssh -i {OMD_KEY} "
    f"-o StrictHostKeyChecking=no "
    f"-o BatchMode=yes "
    f"-o ConnectTimeout=30 "
    f"{OMD_HOST}"
)

def remote(script, extra=""):
    return f'{SSH_CMD} "cd {POC_DIR} && python3 {script} {extra}"'

# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner":            "bigtapp-dq",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="poc_daily_dq_pipeline_v2",
    description=(
        "BigTapp POC — Full DQ pipeline: Bronze→Silver transform, "
        "Great Expectations checks, dashboard refresh, StarRocks sync, "
        "OMD incidents, rule sync"
    ),
    schedule="0 1 * * *",
    start_date=datetime(2026, 2, 24),
    catchup=False,
    default_args=default_args,
    tags=["bigtapp", "dq", "ambank-poc", "v2"],
) as dag:

    # ── Stage 1: Bronze → Silver transformation ───────────────────────────────
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=remote("bronze_to_silver.py"),
        doc_md="""
        Reads bronze CSVs (system_a/b/c), applies transformations:
        - GENDER L/P → M/F standardisation
        - Customer names upper-cased and whitespace normalised
        - Dates normalised to YYYY-MM-DD
        - Null mobile numbers → UNKNOWN
        - HIGH DQ records dropped
        - Silver metadata columns added (LAYER, DQ_PASS_FLAG, SILVER_PROCESSED_AT)
        Outputs: system_a_silver.csv, system_b_silver.csv, system_c_silver.csv
        """,
    )

    # ── Stage 2: GE DQ checks on silver layer ────────────────────────────────
    run_ge_dq = BashOperator(
        task_id="run_ge_dq_v2",
        bash_command=remote("ge_run_dq_v2.py"),
        doc_md="""
        Runs GE expectation suites against silver CSVs.
        Captures row-level failure details:
        - failed_value: the actual bad value found in the column
        - failure_reason: human-readable why (e.g. "MOBILE_NO='0127' does not match pattern")
        - row_data: JSON snapshot of the full failing row
        Writes to dq_findings.db (SQLite) with dedup logic.
        """,
    )

    # ── Stage 3a-c: Dashboard refreshes (parallel) ───────────────────────────
    refresh_scorecard = BashOperator(
        task_id="refresh_scorecard",
        bash_command=remote("dashboard_scorecard.py") + " > /dev/null",
        doc_md="Recomputes DQ scores by system/dimension/rule.",
    )
    refresh_duplicate_cif = BashOperator(
        task_id="refresh_duplicate_cif",
        bash_command=remote("dashboard_duplicate_cif.py") + " > /dev/null",
        doc_md="Detects duplicate CIF pairs across all 3 systems.",
    )
    refresh_residency = BashOperator(
        task_id="refresh_residency",
        bash_command=remote("dashboard_residency.py") + " > /dev/null",
        doc_md="Flags residency/KYC inconsistencies (CON01–CON07).",
    )

    # ── Stage 3d: Export ──────────────────────────────────────────────────────
    export_reports = BashOperator(
        task_id="export_reports",
        bash_command=remote("export_reports.py"),
        doc_md="Generates 7 CSVs + PDF report in ~/poc_dq/exports/.",
    )

    # ── Stage 4: Sync SQLite → StarRocks ─────────────────────────────────────
    starrocks_load = BashOperator(
        task_id="starrocks_load",
        bash_command=remote("starrocks_load.py"),
        doc_md="""
        Loads all SQLite tables into StarRocks ambank_dq database.
        Superset dashboards read from StarRocks via MySQL protocol.
        """,
    )

    # ── Stage 5: OMD Incident creation ───────────────────────────────────────
    omd_incidents = BashOperator(
        task_id="omd_incidents",
        bash_command=remote("omd_incidents.py"),
        doc_md="""
        Creates OMD Data Quality incidents for failed GE checks.
        Posts activity feed threads per system summarising the run.
        Severity: Completeness/Uniqueness → Severity2, Validity → Severity3, Recency/Timeliness → Severity4.
        """,
    )

    # ── Stage 6: Two-way rule sync ────────────────────────────────────────────
    rule_sync = BashOperator(
        task_id="rule_sync",
        bash_command=remote("rule_sync.py"),
        doc_md="""
        Two-way sync between OMD test cases and local GE rule_registry.
        PULL: OMD changes → update rule_registry (is_active, description, version)
        PUSH: local rule_registry changes → update OMD test cases (description, deleted status)
        """,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    (
        bronze_to_silver
        >> run_ge_dq
        >> [refresh_scorecard, refresh_duplicate_cif, refresh_residency]
        >> export_reports
        >> starrocks_load
        >> omd_incidents
        >> rule_sync
    )

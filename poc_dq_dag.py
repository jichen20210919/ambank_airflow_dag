"""
BigTapp POC — Daily DQ Pipeline DAG

Schedule: Daily at 01:00 UTC
Pipeline:
  run_ge_dq
      ├── refresh_scorecard
      ├── refresh_duplicate_cif
      └── refresh_residency
              └── export_reports

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


def remote(script):
    """Return a BashOperator command that runs a script on the OMD node."""
    return f'{SSH_CMD} "cd {POC_DIR} && python3 {script}"'


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":            "bigtapp-dq",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="poc_daily_dq_pipeline",
    description="BigTapp POC — Daily Great Expectations DQ run + dashboard refresh + export",
    schedule="0 1 * * *",             # 01:00 UTC every day
    start_date=datetime(2026, 2, 23),
    catchup=False,
    default_args=default_args,
    tags=["bigtapp", "dq", "ambank-poc"],
) as dag:

    # ── Task 1: Run Great Expectations DQ suites ──────────────────────────────
    run_ge_dq = BashOperator(
        task_id="run_ge_dq",
        bash_command=remote("ge_run_dq.py"),
        doc_md="""
        Runs GE expectation suites against System_A, System_B, System_C.
        Writes findings to dq_findings.db with delta/dedup logic.
        Scores: A=91.7%, B=86.4%, C=72.7% (baseline).
        """,
    )

    # ── Task 2a: Refresh DQ scorecard ─────────────────────────────────────────
    refresh_scorecard = BashOperator(
        task_id="refresh_scorecard",
        bash_command=remote("dashboard_scorecard.py") + " > /dev/null",
        doc_md="Recomputes DQ scores by system/dimension/rule from dq_findings.db.",
    )

    # ── Task 2b: Refresh Duplicate CIF dashboard ──────────────────────────────
    refresh_duplicate_cif = BashOperator(
        task_id="refresh_duplicate_cif",
        bash_command=remote("dashboard_duplicate_cif.py") + " > /dev/null",
        doc_md="Detects duplicate CIF pairs (same IC or same name+DOB) across all 3 systems.",
    )

    # ── Task 2c: Refresh Residency Status dashboard ───────────────────────────
    refresh_residency = BashOperator(
        task_id="refresh_residency",
        bash_command=remote("dashboard_residency.py") + " > /dev/null",
        doc_md="Flags residency/KYC inconsistencies (CON01–CON07) in System_C.",
    )

    # ── Task 3: Export CSVs + PDF report ─────────────────────────────────────
    export_reports = BashOperator(
        task_id="export_reports",
        bash_command=remote("export_reports.py"),
        doc_md="Generates 7 CSVs + PDF report in ~/poc_dq/exports/.",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    # GE run must complete first, then dashboards run in parallel,
    # then exports run once all dashboards are refreshed.
    run_ge_dq >> [refresh_scorecard, refresh_duplicate_cif, refresh_residency] >> export_reports

"""Day 7: repeatable end-to-end shop analytics demonstration.

wait_for_orders_file -> dbt_seed -> dq_gate_raw -> dbt_run -> dbt_snapshot
    -> dbt_test (gate) -> dq_gate_marts -> publish

A gate is a task that fails on bad data. Because every later task needs it to
succeed, a failed gate stops the run before bad data reaches the published layer.
"""
import os
import sys
import shlex
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from alerts import on_failure, on_retry  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", os.path.join(REPO_ROOT, "dbt"))
DBT_BIN = os.environ.get("DBT_BIN", "dbt")
DQ_PYTHON = os.environ.get("DQ_PYTHON", "python3")  # needs duckdb + pyyaml (the dbt venv has both)
LANDING_FILE = os.environ.get("LANDING_FILE", os.path.join(REPO_ROOT, "landing", "orders_READY"))

BATCH = "{{ (dag_run.conf or {}).get('batch', 1) }}"
# conf {"drill": "duplicate"} or {"drill": "bad_amount"} injects bad raw rows to show the gate blocking.
DRILL = "{{ (dag_run.conf or {}).get('drill', '') }}"


def dbt(cmd: str) -> str:
    # Conf values enter through environment variables, never interpolated as shell code.
    return (
        'case "$DBT_BATCH" in 1|2) ;; *) echo "batch must be 1 or 2"; exit 2;; esac; '
        f"cd {shlex.quote(DBT_DIR)} && {shlex.quote(DBT_BIN)} {cmd} --profiles-dir . "
        "--vars \"{batch: $DBT_BATCH}\""
    )


def dq(stage: str, drill: str = "") -> str:
    extra = ' --drill "$DQ_DRILL"' if drill else ""
    return f"cd {shlex.quote(REPO_ROOT)} && {shlex.quote(DQ_PYTHON)} dq/check.py {stage}{extra}"


TASK_ENV = {"DBT_BATCH": BATCH, "DQ_DRILL": DRILL}


default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_failure,
    "on_retry_callback": on_retry,
}
GATE = {"retries": 0}  # bad data will not fix itself on a retry: fail fast and alert

with DAG(
    dag_id="shop_analytics_e2e",
    description="Seed to published metrics, with incremental facts, SCD2 and quality gates",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args=default_args,
    tags=["dbt", "duckdb", "data-quality", "day7"],
) as dag:
    prepare = BashOperator(
        task_id="prepare_run",
        bash_command=f"cd {shlex.quote(REPO_ROOT)} && {shlex.quote(DQ_PYTHON)} scripts/prepare.py",
    )
    wait = FileSensor(
        task_id="wait_for_orders_file", filepath=LANDING_FILE, fs_conn_id="fs_default",
        poke_interval=15, timeout=60 * 60, mode="reschedule", retries=0,
    )
    seed = BashOperator(task_id="dbt_seed", bash_command=dbt("seed"), env=TASK_ENV, append_env=True)
    gate_raw = BashOperator(task_id="dq_gate_raw", bash_command=dq("raw", DRILL), env=TASK_ENV, append_env=True, **GATE)
    run = BashOperator(task_id="dbt_run", bash_command=dbt("run"), env=TASK_ENV, append_env=True)
    snapshot = BashOperator(task_id="dbt_snapshot", bash_command=dbt("snapshot"), env=TASK_ENV, append_env=True)
    test = BashOperator(task_id="dbt_test", bash_command=dbt("test"), env=TASK_ENV, append_env=True, **GATE)
    gate_marts = BashOperator(task_id="dq_gate_marts", bash_command=dq("marts"), **GATE)
    publish = BashOperator(
        task_id="publish",
        bash_command=(
            f"cd {shlex.quote(REPO_ROOT)} && {shlex.quote(DQ_PYTHON)} scripts/publish.py"
        ),
        env={"DEMO_RUN_ID": "{{ run_id }}"}, append_env=True,
    )

    prepare >> wait >> seed >> gate_raw >> run >> snapshot >> test >> gate_marts >> publish

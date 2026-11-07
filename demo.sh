#!/usr/bin/env bash
# Repeatable sandbox: deletes only generated data in THIS checkout before running.
set -euo pipefail
cd "$(dirname "$0")"
for executable in .dbtvenv/bin/python3 .dbtvenv/bin/dbt .afvenv/bin/airflow; do
    [ -x "$executable" ] || { echo "Missing $executable; see README setup" >&2; exit 2; }
done
unset ALERT_DIR DBT_PROJECT_DIR
export DBT_BIN="$PWD/.dbtvenv/bin/dbt" DQ_PYTHON="$PWD/.dbtvenv/bin/python3"
export AIRFLOW="$PWD/.afvenv/bin/airflow"
export LANDING_FILE="$PWD/landing/orders_READY"
echo 'Resetting generated warehouse, Airflow metadata, reports and alerts in this sandbox.'
python3 - <<'PY'
import shutil
from pathlib import Path
for name in ('airflow_home', 'dbt/target', 'dbt/logs', 'published', 'alerts'):
    path = Path(name)
    if path.exists():
        shutil.rmtree(path)
for name in ('dbt/shop.duckdb', 'dbt/shop.duckdb.wal', 'landing/orders_READY'):
    Path(name).unlink(missing_ok=True)
PY
mkdir -p logs
for batch in 1 2; do
    echo "Running batch $batch (full Airflow output: logs/batch${batch}.log)"
    if ! ./run_local.sh "{\"batch\": $batch}" > "logs/batch${batch}.log" 2>&1; then
        echo "Batch $batch command failed; inspect logs/batch${batch}.log" >&2
        exit 1
    fi
    .dbtvenv/bin/python3 scripts/summary.py "batch$batch"
done
echo 'Running duplicate drill (failure is expected; checking actual task states).'
set +e
./run_local.sh '{"batch": 2, "drill": "duplicate"}' > logs/duplicate.log 2>&1
drill_exit=$?
set -e
echo "Airflow drill CLI exit=$drill_exit; warehouse + gate + task-state assertions decide success."
.dbtvenv/bin/python3 scripts/summary.py duplicate
printf '\n=== VERIFIED END-TO-END SUMMARY ===\n'
.dbtvenv/bin/python3 scripts/summary.py all
echo 'Demo passed: two publications, then a raw gate failure with no stale success marker.'
echo 'Evidence: published/demo/{batch1,batch2,duplicate}.json; full logs: logs/*.log'

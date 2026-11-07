#!/usr/bin/env bash
# Run the DAG once locally with `airflow dags test` (no scheduler/webserver).
# Usage: ./run_local.sh [conf-json]   e.g. ./run_local.sh '{"batch": 2}'
#                                          ./run_local.sh '{"drill": "duplicate"}'
set -euo pipefail
cd "$(dirname "$0")"
export AIRFLOW_HOME="$PWD/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW_CONN_FS_DEFAULT='{"conn_type": "fs", "extra": {"path": "/"}}'
export DBT_BIN="${DBT_BIN:-$PWD/.dbtvenv/bin/dbt}"
export DQ_PYTHON="${DQ_PYTHON:-$PWD/.dbtvenv/bin/python}"
[ -f dbt/profiles.yml ] || cp dbt/profiles.yml.example dbt/profiles.yml
mkdir -p landing
touch landing/orders_READY   # the "upstream system" drops its ready-file
AIRFLOW="${AIRFLOW:-$PWD/.afvenv/bin/airflow}"
"$AIRFLOW" db migrate > /dev/null
"$AIRFLOW" dags test shop_analytics_e2e "$(date -u +%Y-%m-%dT%H:%M:%S.%N+00:00)" -c "${1:-{\}}"

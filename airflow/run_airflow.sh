#!/bin/bash
# Usage: ./run_airflow.sh

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AIRFLOW_DIR="${PROJECT_ROOT}/airflow"
AIRFLOW_HOME_DIR="${AIRFLOW_DIR}/airflow_home"

mkdir -p "${AIRFLOW_HOME_DIR}"

export AIRFLOW_HOME="${AIRFLOW_HOME_DIR}"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW_DIR}/dags"
export PYTHONPATH="${PROJECT_ROOT}/shared:${PYTHONPATH}"

sed -i 's/executor = LocalExecutor/executor = SequentialExecutor/' "${AIRFLOW_HOME_DIR}/airflow.cfg" 2>/dev/null || true

airflow standalone

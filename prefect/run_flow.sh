#!/bin/bash
# Usage: ./run_flow.sh [flow_file.py]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON="${PROJECT_ROOT}/.venv/bin/python"

[ ! -f "${PYTHON}" ] && echo "Error: venv not found" && exit 1

export PREFECT_API_URL=http://localhost:4200/api

FLOW_FILE="${1:-retail_pipeline_flow.py}"

[ ! -f "${SCRIPT_DIR}/flows/${FLOW_FILE}" ] && echo "Error: flow not found" && exit 1

cd "${SCRIPT_DIR}"
"${PYTHON}" "flows/${FLOW_FILE}"

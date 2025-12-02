#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "${PYTHON_BIN:-}" ]; then
    if command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN=python3
    elif command -v python >/dev/null 2>&1; then
        PYTHON_BIN=python
    else
        echo "Python is required but not found" >&2
        exit 1
    fi
fi

DB_PATH=${SCHEDULER_DB_PATH:-${SCRIPT_DIR}/scheduler.db}
HOST=${SCHEDULER_HOST:-0.0.0.0}
PORT=${SCHEDULER_PORT:-28256}

exec "${PYTHON_BIN}" "${SCRIPT_DIR}/scheduler_service.py" --host "${HOST}" --port "${PORT}" --db "${DB_PATH}"

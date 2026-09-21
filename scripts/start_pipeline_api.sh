#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"

cd "${ROOT_DIR}"

if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

HOST="${PIPELINE_API_HOST:-127.0.0.1}"
PORT="${PIPELINE_API_PORT:-8000}"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "python3 or python is required but not installed." >&2
  exit 1
fi

if [ -d "${VENV_DIR}" ]; then
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
fi

echo "Starting Sensor Tower pipeline API from ${ROOT_DIR}"
echo "Listening on http://${HOST}:${PORT}"

exec "${PYTHON_BIN}" pipeline_api.py --host "${HOST}" --port "${PORT}"

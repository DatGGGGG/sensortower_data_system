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

if ! command -v node >/dev/null 2>&1; then
  echo "node is required but not installed." >&2
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "npm is required but not installed." >&2
  exit 1
fi

if [ -d "${VENV_DIR}" ]; then
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
fi

echo "Starting n8n from ${ROOT_DIR}"
echo "Open http://localhost:5678 after startup."

exec npx n8n

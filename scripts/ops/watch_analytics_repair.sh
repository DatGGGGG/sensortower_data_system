#!/usr/bin/env bash

# Legacy watcher entry point retained for CN/WW repair operators.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/watch_analytics_history_repair.sh"

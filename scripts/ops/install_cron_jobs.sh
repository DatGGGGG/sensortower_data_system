#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

ops_init "cron_install"
ops_load_env
ops_common_defaults
ops_require_command crontab
ops_require_command mktemp

mkdir -p "${CRON_LOG_ROOT}"

weekly_entry="0 3 * * 1 bash ${ROOT_DIR}/scripts/ops/run_weekly_pipeline.sh >> ${CRON_LOG_ROOT}/cron-launcher.log 2>&1"
# Quarterly cadence: 00:00 on the 2nd day of Mar/Jun/Sep/Dec.
# This refreshes data during the last month of each quarter for quarter-end
# reporting, and December supports year-end report preparation.
quarterly_entry="0 0 2 3,6,9,12 * bash ${ROOT_DIR}/scripts/ops/run_quarterly_pipeline.sh >> ${CRON_LOG_ROOT}/cron-launcher.log 2>&1"
daily_report_entry="30 8 * * * bash ${ROOT_DIR}/scripts/ops/run_daily_report.sh >> ${CRON_LOG_ROOT}/cron-launcher.log 2>&1"
weekly_report_entry="30 10 * * 1 bash ${ROOT_DIR}/scripts/ops/run_weekly_report.sh >> ${CRON_LOG_ROOT}/cron-launcher.log 2>&1"

existing_crontab="$(mktemp)"
cleaned_crontab="$(mktemp)"

if crontab -l > "${existing_crontab}" 2>/dev/null; then
  :
else
  : > "${existing_crontab}"
fi

awk '
  BEGIN { skip = 0 }
  /^# BEGIN VN_STRATEGY_DATA_SYSTEM$/ { skip = 1; next }
  /^# END VN_STRATEGY_DATA_SYSTEM$/ { skip = 0; next }
  /\/mnt\/d\/Coding\/vn_strategy_data_system\/scripts\/ops\/run_weekly_pipeline\.sh/ { next }
  /\/mnt\/d\/Coding\/vn_strategy_data_system\/scripts\/ops\/run_quarterly_pipeline\.sh/ { next }
  /\/mnt\/d\/Coding\/vn_strategy_data_system\/scripts\/ops\/run_daily_report\.sh/ { next }
  /\/mnt\/d\/Coding\/vn_strategy_data_system\/scripts\/ops\/run_weekly_report\.sh/ { next }
  /\/scripts\/ops\/run_weekly_pipeline\.sh/ && /vn_strategy_data_system/ { next }
  /\/scripts\/ops\/run_quarterly_pipeline\.sh/ && /vn_strategy_data_system/ { next }
  /\/scripts\/ops\/run_daily_report\.sh/ && /vn_strategy_data_system/ { next }
  /\/scripts\/ops\/run_weekly_report\.sh/ && /vn_strategy_data_system/ { next }
  skip == 0 { print }
' "${existing_crontab}" > "${cleaned_crontab}"

{
  cat "${cleaned_crontab}"
  if [[ -s "${cleaned_crontab}" ]]; then
    printf "\n"
  fi
  echo "# BEGIN VN_STRATEGY_DATA_SYSTEM"
  echo "${weekly_entry}"
  echo "${quarterly_entry}"
  echo "${daily_report_entry}"
  echo "${weekly_report_entry}"
  echo "# END VN_STRATEGY_DATA_SYSTEM"
} | crontab -

rm -f "${existing_crontab}" "${cleaned_crontab}"

echo "Installed managed cron entries:"
echo "  ${weekly_entry}"
echo "  ${quarterly_entry}"
echo "  ${daily_report_entry}"
echo "  ${weekly_report_entry}"
echo ""
echo "Current crontab:"
crontab -l

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
import html
import json
import os
from pathlib import Path
import smtplib
import ssl
import subprocess
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest
from urllib.request import urlopen


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_POSTGRES_CONTAINER = "vn-strategy-modeling-postgres"


@dataclass
class UsageSummary:
    total_requests: int = 0
    error_requests: int = 0
    unique_clients: int = 0
    top_items: list[dict[str, Any]] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and optionally deliver a VN data-system ops report.")
    parser.add_argument("--report-type", choices=["daily", "weekly"], required=True)
    parser.add_argument("--output-root", default=str(ROOT_DIR / "data"))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL") or "")
    parser.add_argument("--postgres-container", default=os.getenv("REPORT_POSTGRES_CONTAINER") or DEFAULT_POSTGRES_CONTAINER)
    parser.add_argument("--send-email", action="store_true", help="Override REPORT_EMAIL_ENABLED and send email.")
    parser.add_argument("--no-send-email", action="store_true", help="Force dry run/report files only.")
    parser.add_argument("--send-seatalk", action="store_true", help="Override REPORT_SEATALK_ENABLED and send Seatalk.")
    parser.add_argument("--no-send-seatalk", action="store_true", help="Disable Seatalk delivery for this run.")
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def local_now_text() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def local_midnight(value: datetime) -> datetime:
    local_value = value.astimezone()
    return local_value.replace(hour=0, minute=0, second=0, microsecond=0)


def usage_windows(report_type: str, now: datetime | None = None) -> dict[str, datetime]:
    """Use completed local calendar periods, not rolling lookback windows."""
    now_local = (now or utc_now()).astimezone()
    today_start = local_midnight(now_local)
    if report_type == "daily":
        current_end = today_start
        current_start = current_end - timedelta(days=1)
        previous_end = current_start
        previous_start = previous_end - timedelta(days=1)
    else:
        this_week_start = today_start - timedelta(days=today_start.weekday())
        current_end = this_week_start
        current_start = current_end - timedelta(days=7)
        previous_end = current_start
        previous_start = previous_end - timedelta(days=7)
    return {
        "current_start": current_start,
        "current_end": current_end,
        "previous_start": previous_start,
        "previous_end": previous_end,
    }


def build_period_label(report_type: str, usage_window: dict[str, str]) -> str:
    start = datetime.fromisoformat(usage_window["current_start"]).astimezone()
    end = datetime.fromisoformat(usage_window["current_end"]).astimezone()
    if report_type == "daily":
        return start.strftime("%Y-%m-%d")
    # The report window is [start, end), so subtract one day for a human-friendly inclusive end date.
    inclusive_end = end - timedelta(days=1)
    return f"{start.strftime('%Y-%m-%d')} to {inclusive_end.strftime('%Y-%m-%d')}"


def report_title(report: dict[str, Any]) -> str:
    return (
        f"VN Strategy Data System {report['report_type'].title()} Report "
        f"({report['period_label']})"
    )


def pct_change(current: int | float, previous: int | float) -> str:
    if previous == 0:
        if current == 0:
            return "0.0%"
        return "new"
    value = ((current - previous) / previous) * 100
    arrow = "up" if value > 0 else "down" if value < 0 else "flat"
    return f"{arrow} {abs(value):.1f}%"


def run_command(command: list[str], cwd: Path | None = None, timeout: int = 20) -> dict[str, Any]:
    try:
        result = subprocess.run(
            command,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return {
            "status": "success" if result.returncode == 0 else "failed",
            "exit_code": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def psql_json_query(postgres_container: str, query: str) -> list[dict[str, Any]]:
    wrapped_query = f"""
    SELECT COALESCE(json_agg(row_to_json(report_rows)), '[]'::json)::text
      FROM (
        {query}
      ) AS report_rows
    """
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            postgres_container,
            "psql",
            "-U",
            "postgres",
            "-d",
            "mydb",
            "-q",
            "-t",
            "-A",
            "-P",
            "footer=off",
            "-c",
            wrapped_query,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "psql query failed")
    output = result.stdout.strip()
    if not output:
        return []
    payload = json.loads(extract_json_payload(output))
    return payload if isinstance(payload, list) else []


def extract_json_payload(output: str) -> str:
    """Return the JSON array/object from psql output, ignoring incidental noise."""
    cleaned = output.strip()
    if cleaned.startswith(("[", "{")):
        return cleaned
    candidates = [idx for idx in [cleaned.find("["), cleaned.find("{")] if idx >= 0]
    if not candidates:
        raise json.JSONDecodeError("No JSON payload found in psql output", cleaned, 0)
    start = min(candidates)
    end = max(cleaned.rfind("]"), cleaned.rfind("}"))
    if end < start:
        raise json.JSONDecodeError("Incomplete JSON payload in psql output", cleaned, start)
    return cleaned[start : end + 1]


def db_fetch_one(postgres_container: str, query: str) -> dict[str, Any] | None:
    rows = psql_json_query(postgres_container, query)
    return rows[0] if rows else None


def safe_db_fetch_all(postgres_container: str, query: str) -> tuple[list[dict[str, Any]], str | None]:
    try:
        return psql_json_query(postgres_container, query), None
    except Exception as exc:
        return [], str(exc)


def latest_status_files(output_root: Path, limit: int = 8) -> list[dict[str, Any]]:
    cron_root = output_root / "logs" / "cron"
    if not cron_root.exists():
        return []
    status_files = sorted(
        cron_root.glob("*/status.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    results = []
    current_run_id = os.getenv("RUN_ID")
    excluded_run_types = {"daily_report", "weekly_report", "cron_install"}
    include_manual_ops = os.getenv("REPORT_INCLUDE_MANUAL_OPS", "0") == "1"
    for path in status_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            payload = {"status": "unreadable", "message": str(exc)}
        if payload.get("run_id") == current_run_id:
            continue
        if payload.get("run_type") in excluded_run_types:
            continue
        if (
            not include_manual_ops
            and str(payload.get("run_id", "")).startswith("manual_")
            and payload.get("status") != "success"
        ):
            continue
        payload["status_file"] = str(path)
        results.append(payload)
        if len(results) >= limit:
            break
    return results


def data_freshness(postgres_container: str) -> tuple[dict[str, Any], list[str]]:
    checks = {
        "performance_max_date": "SELECT MAX(date)::date AS value FROM core.fact_app_performance_daily",
        "active_users_max_date": "SELECT MAX(date)::date AS value FROM core.fact_app_performance_active_users",
        "retention_max_date": "SELECT MAX(date)::date AS value FROM core.fact_app_performance_retention",
        "demographics_max_date": "SELECT MAX(date)::date AS value FROM core.fact_user_demographics",
    }
    freshness: dict[str, Any] = {}
    errors: list[str] = []
    for name, query in checks.items():
        try:
            row = db_fetch_one(postgres_container, query)
            value = row["value"] if row else None
            freshness[name] = value.isoformat() if hasattr(value, "isoformat") else value
        except Exception as exc:
            freshness[name] = None
            errors.append(f"{name}: {exc}")
    return freshness, errors


def service_health(root_dir: Path) -> dict[str, Any]:
    return {
        "api_health": run_command(["curl", "-fsS", "http://127.0.0.1:8001/health"], timeout=10),
        "mcp_health": run_command(["curl", "-fsS", "http://127.0.0.1:8002/health"], timeout=10),
        "docker_compose": run_command(
            ["docker", "compose", "-p", "vn_modeling_clean", "ps"],
            cwd=root_dir / "modeling_layer",
            timeout=20,
        ),
    }


def usage_summary(
    postgres_container: str,
    *,
    table_name: str,
    item_column: str,
    client_column: str,
    status_expression: str,
    since: datetime,
    until: datetime,
    extra_where: str = "TRUE",
) -> tuple[UsageSummary, str | None]:
    rows, error = safe_db_fetch_all(
        postgres_container,
        f"""
        WITH filtered AS (
            SELECT *
            FROM {table_name}
            WHERE occurred_at >= {sql_literal(since.isoformat())}::timestamptz
              AND occurred_at < {sql_literal(until.isoformat())}::timestamptz
              AND ({extra_where})
        ),
        totals AS (
            SELECT
                COUNT(*)::int AS total_requests,
                COUNT(*) FILTER (WHERE {status_expression})::int AS error_requests,
                COUNT(DISTINCT {client_column})::int AS unique_clients
            FROM filtered
        ),
        top_items AS (
            SELECT
                {item_column} AS item,
                COUNT(*)::int AS request_count,
                COUNT(*) FILTER (WHERE {status_expression})::int AS error_count
            FROM filtered
            GROUP BY 1
            ORDER BY request_count DESC, item
            LIMIT 10
        )
        SELECT
            (SELECT row_to_json(totals) FROM totals) AS totals,
            COALESCE(json_agg(top_items), '[]'::json) AS top_items
        FROM top_items
        """,
    )
    if error:
        return UsageSummary(top_items=[]), error
    if not rows:
        return UsageSummary(top_items=[]), None
    totals = rows[0].get("totals") or {}
    top_items = rows[0].get("top_items") or []
    return (
        UsageSummary(
            total_requests=int(totals.get("total_requests") or 0),
            error_requests=int(totals.get("error_requests") or 0),
            unique_clients=int(totals.get("unique_clients") or 0),
            top_items=top_items,
        ),
        None,
    )


def collect_usage(postgres_container: str, report_type: str) -> dict[str, Any]:
    windows = usage_windows(report_type)
    current_start = windows["current_start"]
    current_end = windows["current_end"]
    previous_start = windows["previous_start"]
    previous_end = windows["previous_end"]

    api_current, api_error = usage_summary(
        postgres_container,
        table_name="api_access.api_request_logs",
        item_column="path",
        client_column="COALESCE(api_key_name, key_prefix, 'unknown')",
        status_expression="COALESCE(status_code, 0) >= 400",
        since=current_start,
        until=current_end,
        extra_where="path <> '/mcp'",
    )
    api_previous, api_previous_error = usage_summary(
        postgres_container,
        table_name="api_access.api_request_logs",
        item_column="path",
        client_column="COALESCE(api_key_name, key_prefix, 'unknown')",
        status_expression="COALESCE(status_code, 0) >= 400",
        since=previous_start,
        until=previous_end,
        extra_where="path <> '/mcp'",
    )
    api_misrouted_mcp_current, api_misrouted_mcp_error = usage_summary(
        postgres_container,
        table_name="api_access.api_request_logs",
        item_column="path",
        client_column="COALESCE(api_key_name, key_prefix, 'unknown')",
        status_expression="COALESCE(status_code, 0) >= 400",
        since=current_start,
        until=current_end,
        extra_where="path = '/mcp'",
    )
    api_misrouted_mcp_previous, api_misrouted_mcp_previous_error = usage_summary(
        postgres_container,
        table_name="api_access.api_request_logs",
        item_column="path",
        client_column="COALESCE(api_key_name, key_prefix, 'unknown')",
        status_expression="COALESCE(status_code, 0) >= 400",
        since=previous_start,
        until=previous_end,
        extra_where="path = '/mcp'",
    )
    mcp_current, mcp_error = usage_summary(
        postgres_container,
        table_name="api_access.mcp_tool_logs",
        item_column="tool_name",
        client_column="COALESCE(token_name, token_prefix, 'unknown')",
        status_expression="status <> 'success'",
        since=current_start,
        until=current_end,
    )
    mcp_previous, mcp_previous_error = usage_summary(
        postgres_container,
        table_name="api_access.mcp_tool_logs",
        item_column="tool_name",
        client_column="COALESCE(token_name, token_prefix, 'unknown')",
        status_expression="status <> 'success'",
        since=previous_start,
        until=previous_end,
    )

    return {
        "window": {
            "current_start": current_start.isoformat(),
            "current_end": current_end.isoformat(),
            "previous_start": previous_start.isoformat(),
            "previous_end": previous_end.isoformat(),
        },
        "api": {
            "current": api_current,
            "previous": api_previous,
            "errors": [error for error in [api_error, api_previous_error] if error],
        },
        "api_misrouted_mcp": {
            "current": api_misrouted_mcp_current,
            "previous": api_misrouted_mcp_previous,
            "errors": [
                error
                for error in [api_misrouted_mcp_error, api_misrouted_mcp_previous_error]
                if error
            ],
        },
        "mcp": {
            "current": mcp_current,
            "previous": mcp_previous,
            "errors": [error for error in [mcp_error, mcp_previous_error] if error],
        },
    }


def usage_summary_payload(summary: UsageSummary) -> dict[str, Any]:
    return {
        "total_requests": summary.total_requests,
        "error_requests": summary.error_requests,
        "unique_clients": summary.unique_clients,
        "top_items": summary.top_items or [],
    }


def serializable_usage(usage: dict[str, Any]) -> dict[str, Any]:
    return {
        "window": usage["window"],
        "api": {
            "current": usage_summary_payload(usage["api"]["current"]),
            "previous": usage_summary_payload(usage["api"]["previous"]),
            "errors": usage["api"]["errors"],
        },
        "api_misrouted_mcp": {
            "current": usage_summary_payload(usage["api_misrouted_mcp"]["current"]),
            "previous": usage_summary_payload(usage["api_misrouted_mcp"]["previous"]),
            "errors": usage["api_misrouted_mcp"]["errors"],
        },
        "mcp": {
            "current": usage_summary_payload(usage["mcp"]["current"]),
            "previous": usage_summary_payload(usage["mcp"]["previous"]),
            "errors": usage["mcp"]["errors"],
        },
    }


def api_health_ok(services: dict[str, Any]) -> bool:
    return services.get("api_health", {}).get("status") == "success"


def failed_only_on_api_rebuild(status: dict[str, Any], services: dict[str, Any]) -> bool:
    """A redundant Docker image rebuild can fail after all data work succeeds.

    Treat that case as warning only when the already-running API is healthy.
    Real ETL/modeling/analytics failures should still surface as failed.
    """
    if status.get("status") != "failed" or not api_health_ok(services):
        return False
    log_path_text = status.get("run_log")
    if not log_path_text and status.get("status_file"):
        log_path_text = str(Path(status["status_file"]).with_name("run.log"))
    if not log_path_text:
        return False
    try:
        log_path = Path(log_path_text)
        with log_path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - 200_000), os.SEEK_SET)
            tail = handle.read().decode("utf-8", errors="replace")
    except Exception:
        return False
    has_api_rebuild = "docker compose -p" in tail and "up -d --build api" in tail
    has_rebuild_failure = "failed to solve" in tail or "context deadline exceeded" in tail
    return has_api_rebuild and has_rebuild_failure


def overall_status(
    statuses: list[dict[str, Any]],
    freshness_errors: list[str],
    usage: dict[str, Any],
    services: dict[str, Any],
) -> str:
    if statuses and statuses[0].get("status") == "failed":
        if failed_only_on_api_rebuild(statuses[0], services):
            return "warning"
        return "failed"
    if freshness_errors or usage["api"]["errors"] or usage["mcp"]["errors"] or usage["api_misrouted_mcp"]["errors"]:
        return "warning"
    if usage["api_misrouted_mcp"]["current"].total_requests:
        return "warning"
    if statuses and statuses[0].get("status") == "running":
        return "warning"
    return "healthy"


def render_text(report: dict[str, Any]) -> str:
    lines = [
        report_title(report),
        f"Generated: {report['generated_at_local']}",
        f"Period: {report['period_label']}",
        f"Overall status: {report['overall_status'].upper()}",
        "",
        "Latest pipeline runs:",
    ]
    for item in report["latest_statuses"][:5]:
        lines.append(
            f"- {item.get('run_id')}: {item.get('status')} | started={item.get('started_at')} | ended={item.get('ended_at')}"
        )
    lines.extend(["", "Data freshness:"])
    for key, value in report["freshness"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "Usage:"])
    for surface in ["api", "mcp"]:
        current = report["usage"][surface]["current"]
        previous = report["usage"][surface]["previous"]
        lines.append(
            f"- {surface.upper()}: {current.total_requests} requests "
            f"({pct_change(current.total_requests, previous.total_requests)} vs previous window), "
            f"errors={current.error_requests}, clients={current.unique_clients}"
        )
        for item in current.top_items or []:
            lines.append(f"  - {item.get('item')}: {item.get('request_count')} requests, errors={item.get('error_count')}")
        for error in report["usage"][surface].get("errors", []):
            lines.append(f"  - usage log query warning: {error}")
    misrouted_current = report["usage"]["api_misrouted_mcp"]["current"]
    misrouted_previous = report["usage"]["api_misrouted_mcp"]["previous"]
    if misrouted_current.total_requests:
        lines.append(
            f"- MCP endpoint misroutes on API: {misrouted_current.total_requests} requests "
            f"({pct_change(misrouted_current.total_requests, misrouted_previous.total_requests)}), "
            f"errors={misrouted_current.error_requests}"
        )
        lines.append("  - These are /mcp requests hitting the API service, not real MCP tool calls.")
    for error in report["usage"]["api_misrouted_mcp"].get("errors", []):
        lines.append(f"- MCP misroute query warning: {error}")
    lines.extend(["", "Report files:", f"- {report['report_dir']}"])
    return "\n".join(lines) + "\n"


def render_seatalk_text(report: dict[str, Any]) -> str:
    usage = report["usage"]
    api_current = usage["api"]["current"]
    api_previous = usage["api"]["previous"]
    misrouted_current = usage["api_misrouted_mcp"]["current"]
    misrouted_previous = usage["api_misrouted_mcp"]["previous"]
    mcp_current = usage["mcp"]["current"]
    mcp_previous = usage["mcp"]["previous"]
    latest = report["latest_statuses"][0] if report["latest_statuses"] else {}
    lines = [
        f"[VN Data System] {report['report_type'].title()} Report ({report['period_label']}) - {report['overall_status'].upper()}",
        f"Generated: {report['generated_at_local']}",
        f"Period: {report['period_label']}",
        "",
        "Latest pipeline run:",
        f"- {latest.get('run_id', 'n/a')}: {latest.get('status', 'n/a')}",
        "",
        "Freshness:",
    ]
    for key, value in report["freshness"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "Usage:",
            (
                f"- API: {api_current.total_requests} requests "
                f"({pct_change(api_current.total_requests, api_previous.total_requests)}), "
                f"errors={api_current.error_requests}, clients={api_current.unique_clients}"
            ),
            (
                f"- MCP: {mcp_current.total_requests} tool calls "
                f"({pct_change(mcp_current.total_requests, mcp_previous.total_requests)}), "
                f"errors={mcp_current.error_requests}, clients={mcp_current.unique_clients}"
            ),
            (
                f"- MCP endpoint misroutes on API: {misrouted_current.total_requests} "
                f"({pct_change(misrouted_current.total_requests, misrouted_previous.total_requests)}), "
                f"errors={misrouted_current.error_requests}"
            ),
            "",
            f"Report files: {report['report_dir']}",
        ]
    )
    return "\n".join(lines)


def render_html(report: dict[str, Any]) -> str:
    status = html.escape(report["overall_status"].upper())
    rows = []
    for item in report["latest_statuses"][:8]:
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(item.get('run_id')))}</td>"
            f"<td>{html.escape(str(item.get('run_type')))}</td>"
            f"<td>{html.escape(str(item.get('status')))}</td>"
            f"<td>{html.escape(str(item.get('started_at')))}</td>"
            f"<td>{html.escape(str(item.get('ended_at')))}</td>"
            "</tr>"
        )
    freshness_rows = [
        f"<tr><td>{html.escape(key)}</td><td>{html.escape(str(value))}</td></tr>"
        for key, value in report["freshness"].items()
    ]
    usage_sections = []
    for surface in ["api", "mcp"]:
        current = report["usage"][surface]["current"]
        previous = report["usage"][surface]["previous"]
        top_rows = [
            "<tr>"
            f"<td>{html.escape(str(item.get('item')))}</td>"
            f"<td>{item.get('request_count')}</td>"
            f"<td>{item.get('error_count')}</td>"
            "</tr>"
            for item in (current.top_items or [])
        ]
        usage_sections.append(
            f"""
            <h3>{surface.upper()} Usage</h3>
            <p><strong>{current.total_requests}</strong> requests
            ({html.escape(pct_change(current.total_requests, previous.total_requests))} vs previous window),
            errors={current.error_requests}, unique clients={current.unique_clients}</p>
            <table><tr><th>Endpoint / Tool</th><th>Requests</th><th>Errors</th></tr>{''.join(top_rows)}</table>
            {''.join(f'<p><strong>Usage log query warning:</strong> {html.escape(str(error))}</p>' for error in report['usage'][surface].get('errors', []))}
            """
        )
    misrouted_current = report["usage"]["api_misrouted_mcp"]["current"]
    misrouted_previous = report["usage"]["api_misrouted_mcp"]["previous"]
    misroute_section = ""
    if misrouted_current.total_requests or report["usage"]["api_misrouted_mcp"].get("errors"):
        misroute_section = f"""
        <h3>MCP Endpoint Misroutes On API</h3>
        <p><strong>{misrouted_current.total_requests}</strong> requests
        ({html.escape(pct_change(misrouted_current.total_requests, misrouted_previous.total_requests))} vs previous window),
        errors={misrouted_current.error_requests}</p>
        <p>These are <code>/mcp</code> requests hitting the API service, not real MCP tool calls.</p>
        {''.join(f'<p><strong>Usage log query warning:</strong> {html.escape(str(error))}</p>' for error in report['usage']['api_misrouted_mcp'].get('errors', []))}
        """
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: Arial, sans-serif; line-height: 1.45; color: #222; }}
    table {{ border-collapse: collapse; margin: 12px 0; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: left; }}
    th {{ background: #f4f4f4; }}
    .status {{ font-size: 20px; font-weight: 700; }}
  </style>
</head>
<body>
  <h2>{html.escape(report_title(report))}</h2>
  <p>Generated: {html.escape(report['generated_at_local'])}</p>
  <p>Period: {html.escape(report['period_label'])}</p>
  <p class="status">Overall status: {status}</p>
  <h3>Latest Pipeline Runs</h3>
  <table><tr><th>Run ID</th><th>Type</th><th>Status</th><th>Started</th><th>Ended</th></tr>{''.join(rows)}</table>
  <h3>Data Freshness</h3>
  <table><tr><th>Metric</th><th>Value</th></tr>{''.join(freshness_rows)}</table>
  {''.join(usage_sections)}
  {misroute_section}
  <p>Report files: {html.escape(report['report_dir'])}</p>
</body>
</html>
"""


def parse_recipients(value: str | None) -> list[str]:
    if not value:
        return []
    normalized = value.replace(";", ",")
    return [item.strip() for item in normalized.split(",") if item.strip()]


def send_email(report_type: str, subject_status: str, period_label: str, text_body: str, html_body: str) -> None:
    to_value = os.getenv(f"REPORT_{report_type.upper()}_EMAIL_TO") or os.getenv("REPORT_EMAIL_TO")
    cc_value = os.getenv(f"REPORT_{report_type.upper()}_EMAIL_CC") or os.getenv("REPORT_EMAIL_CC")
    recipients = parse_recipients(to_value)
    cc = parse_recipients(cc_value)
    if not recipients:
        raise RuntimeError(f"No report recipients configured for {report_type}.")

    sender = os.getenv("REPORT_EMAIL_FROM") or os.getenv("SMTP_USERNAME")
    if not sender:
        raise RuntimeError("REPORT_EMAIL_FROM or SMTP_USERNAME is required.")

    message = EmailMessage()
    message["Subject"] = f"[VN Data System] {report_type.title()} Report ({period_label}) - {subject_status.upper()}"
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    if cc:
        message["Cc"] = ", ".join(cc)
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")

    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    username = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")
    use_tls = os.getenv("SMTP_USE_TLS", "1").strip().lower() not in {"0", "false", "no"}
    if not host:
        raise RuntimeError("SMTP_HOST is required.")

    all_recipients = recipients + cc
    context = ssl.create_default_context()
    with smtplib.SMTP(host, port, timeout=30) as smtp:
        if use_tls:
            smtp.starttls(context=context)
        if username and password:
            smtp.login(username, password)
        smtp.send_message(message, to_addrs=all_recipients)


def bool_env(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() not in {"0", "false", "no", ""}


def post_json(url: str, payload: dict[str, Any], headers: dict[str, str] | None = None) -> dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = UrlRequest(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            **(headers or {}),
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Seatalk HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Seatalk request failed: {exc}") from exc
    if not body.strip():
        return {}
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Seatalk returned non-JSON response: {body[:500]}") from exc


def extract_seatalk_access_token(response: dict[str, Any]) -> str:
    for key in ["app_access_token", "access_token", "token"]:
        value = response.get(key)
        if isinstance(value, str) and value:
            return value
    data = response.get("data")
    if isinstance(data, dict):
        for key in ["app_access_token", "access_token", "token"]:
            value = data.get(key)
            if isinstance(value, str) and value:
                return value
    raise RuntimeError(f"Seatalk token response did not include an access token: {response}")


def seatalk_message_payload(group_id: str, content: str) -> dict[str, Any]:
    # Seatalk OpenAPI group-chat v2 accepts this text-message shape.
    return {
        "group_id": group_id,
        "message": {
            "tag": "text",
            "text": {
                "content": content,
            },
        },
    }


def send_seatalk_message(content: str) -> None:
    app_id = os.getenv("SEATALK_APP_ID")
    app_secret = os.getenv("SEATALK_APP_SECRET")
    group_id = os.getenv("SEATALK_GROUP_ID")
    token_url = os.getenv("SEATALK_TOKEN_URL", "https://openapi.seatalk.io/auth/app_access_token")
    message_url = os.getenv("SEATALK_URL", "https://openapi.seatalk.io/messaging/v2/group_chat")
    if not app_id:
        raise RuntimeError("SEATALK_APP_ID is required.")
    if not app_secret:
        raise RuntimeError("SEATALK_APP_SECRET is required.")
    if not group_id:
        raise RuntimeError("SEATALK_GROUP_ID is required.")

    token_response = post_json(token_url, {"app_id": app_id, "app_secret": app_secret})
    access_token = extract_seatalk_access_token(token_response)
    post_json(
        message_url,
        seatalk_message_payload(group_id, content),
        headers={"Authorization": f"Bearer {access_token}"},
    )


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root).resolve()
    report_id = f"{args.report_type}_{datetime.now().astimezone().strftime('%Y%m%d_%H%M%S')}"
    report_dir = output_root / "logs" / "reports" / report_id
    report_dir.mkdir(parents=True, exist_ok=True)

    statuses = latest_status_files(output_root)
    freshness, freshness_errors = data_freshness(args.postgres_container)
    usage = collect_usage(args.postgres_container, args.report_type)
    services = service_health(ROOT_DIR)
    status = overall_status(statuses, freshness_errors, usage, services)
    period_label = build_period_label(args.report_type, usage["window"])

    report: dict[str, Any] = {
        "report_id": report_id,
        "report_type": args.report_type,
        "period_label": period_label,
        "generated_at": utc_now().isoformat(),
        "generated_at_local": local_now_text(),
        "overall_status": status,
        "latest_statuses": statuses,
        "freshness": freshness,
        "freshness_errors": freshness_errors,
        "usage": usage,
        "usage_json": serializable_usage(usage),
        "service_health": services,
        "report_dir": str(report_dir),
    }

    text_body = render_text(report)
    html_body = render_html(report)
    report_json = dict(report)
    report_json["usage"] = report_json.pop("usage_json")
    (report_dir / "report.json").write_text(json.dumps(report_json, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (report_dir / "report.txt").write_text(text_body, encoding="utf-8")
    (report_dir / "report.html").write_text(html_body, encoding="utf-8")

    should_send = args.send_email or (not args.no_send_email and bool_env("REPORT_EMAIL_ENABLED", "0"))
    email_status = "skipped"
    if should_send:
        send_email(args.report_type, status, period_label, text_body, html_body)
        email_status = "sent"

    should_send_seatalk = args.send_seatalk or (
        not args.no_send_seatalk and bool_env("REPORT_SEATALK_ENABLED", "0")
    )
    seatalk_status = "skipped"
    if should_send_seatalk:
        send_seatalk_message(render_seatalk_text(report))
        seatalk_status = "sent"

    result = {
        "status": "success",
        "report_type": args.report_type,
        "overall_status": status,
        "email_status": email_status,
        "seatalk_status": seatalk_status,
        "report_dir": str(report_dir),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

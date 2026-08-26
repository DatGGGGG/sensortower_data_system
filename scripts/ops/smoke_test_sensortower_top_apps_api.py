from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


BASE_URL = "https://api.sensortower.com"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _csv(values: list[str]) -> str:
    return ",".join(v.strip() for v in values if v.strip())


def _parse_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _coerce_config_list(value: Any, fallback: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        parsed = _parse_csv(value)
        return parsed if parsed else fallback
    return fallback


def _redacted_params(params: dict[str, Any]) -> dict[str, Any]:
    redacted = dict(params)
    if "auth_token" in redacted:
        token = str(redacted["auth_token"])
        redacted["auth_token"] = f"{token[:8]}..." if token else ""
    return redacted


def _extract_entities_sample(payload: dict[str, Any], limit: int = 5) -> dict[str, Any]:
    entities = payload.get("entities")
    if not isinstance(entities, dict):
        return {}
    return {key: entities[key] for key in list(entities.keys())[:limit]}


def _fetch_top_apps(
    *,
    base_url: str,
    api_key: str,
    market: str,
    year: int,
    limit: int,
    offset: int,
    bundle: str,
    order_by: str,
    devices: str,
    breakdown: str,
    custom_fields_filter_id: str | None,
    unified_app_primary_categories: str | None,
    worldwide_mode: str,
    top_apps_mobile_style: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/v1/facets/metrics"
    if top_apps_mobile_style == "url_flag":
        url = f"{url}?top_apps_mobile"

    effective_regions: str | None = market
    if market.upper() in {"WW", "WORLD", "GLOBAL"} and worldwide_mode == "omit_regions":
        effective_regions = None

    params: dict[str, Any] = {
        "bundle": bundle,
        "order_by": order_by,
        "breakdown": breakdown,
        "devices": devices,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "limit": limit,
        "offset": offset,
        "auth_token": api_key,
    }
    if top_apps_mobile_style == "empty_value":
        params["top_apps_mobile"] = ""
    elif top_apps_mobile_style == "true_value":
        params["top_apps_mobile"] = "true"

    if effective_regions:
        params["regions"] = effective_regions

    if breakdown == "unified_app_id":
        params["entities"] = "unified_app_id:name"
    elif breakdown == "app_id":
        params["entities"] = "app_id:name"

    if custom_fields_filter_id:
        params["custom_fields_filter_id"] = custom_fields_filter_id

    if unified_app_primary_categories:
        params["unified_app_primary_categories"] = unified_app_primary_categories

    started = time.time()
    response = requests.get(url, params=params, timeout=(10, timeout_seconds))
    elapsed = time.time() - started

    result: dict[str, Any] = {
        "market": market,
        "effective_regions": effective_regions,
        "year": year,
        "status_code": response.status_code,
        "elapsed_seconds": round(elapsed, 3),
        "request_url": url,
        "top_apps_mobile_style": top_apps_mobile_style,
        "request_params": _redacted_params(params),
        "api_usage_limit": response.headers.get("x-api-usage-limit"),
        "api_usage_count": response.headers.get("x-api-usage-count"),
    }

    try:
        payload = response.json()
    except Exception:
        result["response_text_sample"] = response.text[:1000]
        return result

    result["payload_type"] = type(payload).__name__
    if isinstance(payload, dict):
        data = payload.get("data")
        meta = payload.get("meta")
        if response.status_code != 200:
            result["error_payload"] = payload
        result["data_count"] = len(data) if isinstance(data, list) else None
        result["meta"] = meta if isinstance(meta, dict) else None
        result["sample_rows"] = data[: min(5, len(data))] if isinstance(data, list) else []
        result["entities_sample"] = _extract_entities_sample(payload)
        if isinstance(data, list) and data:
            result["sample_columns"] = sorted(data[0].keys())
    else:
        result["payload_sample"] = payload[:5] if isinstance(payload, list) else payload

    return result


def main() -> int:
    repo_root = _repo_root()
    load_dotenv(repo_root / ".env")

    parser = argparse.ArgumentParser(
        description="Smoke test SensorTower's new Top Apps Facets API without touching pipeline outputs."
    )
    parser.add_argument("--config-path", default=str(repo_root / "config.json"))
    parser.add_argument("--output-root", default=str(repo_root / "data"))
    parser.add_argument("--base-url", default=BASE_URL)
    parser.add_argument("--markets", default=None, help="Comma-separated markets, e.g. VN,CN,WW.")
    parser.add_argument("--years", default=str(datetime.now(timezone.utc).year), help="Comma-separated years.")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument(
        "--bundle",
        default="mobile_store_top_apps",
        choices=["mobile_store_top_apps", "mobile_active_users", "mobile_session_metrics"],
    )
    parser.add_argument("--order-by", default="est_mobile_revenue:desc")
    parser.add_argument("--devices", default="iphone,ipad,android")
    parser.add_argument("--breakdown", default="unified_app_id", choices=["unified_app_id", "app_id"])
    parser.add_argument(
        "--custom-fields-filter-id",
        default=None,
        help="Defaults to TOP_GAME_SNAPSHOT_CUSTOM_FIELDS_FILTER_ID from config. Pass empty string to omit.",
    )
    parser.add_argument("--unified-app-primary-categories", default=None)
    parser.add_argument(
        "--worldwide-mode",
        default="regions",
        choices=["regions", "omit_regions"],
        help="Use regions=WW by default. For WW/WORLD/GLOBAL, omit regions when set to omit_regions.",
    )
    parser.add_argument(
        "--top-apps-mobile-style",
        default="empty_value",
        choices=["empty_value", "url_flag", "true_value"],
        help="How to encode the top_apps_mobile flag. url_flag matches /metrics?top_apps_mobile.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--fail-on-empty", action="store_true")
    args = parser.parse_args()

    api_key = os.getenv("ST_API_KEY")
    if not api_key:
        raise SystemExit(f"Missing ST_API_KEY in environment or {repo_root / '.env'}")

    config = _load_config(Path(args.config_path))
    markets = _parse_csv(args.markets) if args.markets else _coerce_config_list(
        config.get("TOP_GAME_SNAPSHOT_REGIONS"),
        ["VN"],
    )
    years = [int(year) for year in _parse_csv(args.years)]

    custom_fields_filter_id: str | None
    if args.custom_fields_filter_id is None:
        custom_fields_filter_id = str(config.get("TOP_GAME_SNAPSHOT_CUSTOM_FIELDS_FILTER_ID", "")).strip() or None
    else:
        custom_fields_filter_id = args.custom_fields_filter_id.strip() or None

    report = {
        "status": "running",
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "purpose": "SensorTower Top Apps Facets API migration smoke test",
        "base_url": args.base_url,
        "markets": markets,
        "years": years,
        "bundle": args.bundle,
        "order_by": args.order_by,
        "devices": args.devices,
        "breakdown": args.breakdown,
        "limit": args.limit,
        "offset": args.offset,
        "results": [],
    }

    output_dir = Path(args.output_root) / "logs" / "sensortower_migration_smoke"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"top_apps_facets_smoke_{_utc_timestamp()}.json"

    failures = 0
    empty_results = 0
    for market in markets:
        for year in years:
            print(f"Calling Top Apps Facets smoke: market={market}, year={year}, limit={args.limit}")
            result = _fetch_top_apps(
                base_url=args.base_url,
                api_key=api_key,
                market=market,
                year=year,
                limit=args.limit,
                offset=args.offset,
                bundle=args.bundle,
                order_by=args.order_by,
                devices=args.devices,
                breakdown=args.breakdown,
                custom_fields_filter_id=custom_fields_filter_id,
                unified_app_primary_categories=args.unified_app_primary_categories,
                worldwide_mode=args.worldwide_mode,
                top_apps_mobile_style=args.top_apps_mobile_style,
                timeout_seconds=args.timeout_seconds,
            )
            report["results"].append(result)

            data_count = result.get("data_count")
            meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
            print(
                f"  status={result.get('status_code')} rows={data_count} "
                f"total_count={meta.get('total_count')} effective_regions={result.get('effective_regions')} "
                f"elapsed={result.get('elapsed_seconds')}s usage={result.get('api_usage_count')}/"
                f"{result.get('api_usage_limit')}"
            )

            if result.get("status_code") != 200:
                failures += 1
            if data_count == 0:
                empty_results += 1

    report["status"] = "failed" if failures else "success"
    report["failed_request_count"] = failures
    report["empty_result_count"] = empty_results
    report["finished_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved smoke result: {output_path}")

    if failures:
        return 1
    if args.fail_on_empty and empty_results:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

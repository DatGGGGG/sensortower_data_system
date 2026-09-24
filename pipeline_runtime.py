from __future__ import annotations

import argparse
import ast
import csv
import gc
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional

import ijson
import pandas as pd
import requests
from dotenv import load_dotenv

from mapping_artifact_manifest import default_manifest_path, ensure_mapping_artifact_manifest
from mapping_artifacts import MappingArtifactRegistry

LIB_DIR = Path(__file__).resolve().parent / "lib"
if str(LIB_DIR) not in sys.path:
    sys.path.append(str(LIB_DIR))

import library_st_data_processing as lsdp


BASE_URL = "https://api.sensortower.com"
DEFAULT_OUTPUT_ROOT = "data"
DEFAULT_APP_PERFORMANCE_START_DATE = "2014-01-01"
DEFAULT_ANALYTICS_START_DATE = "2020-01-01"
DEFAULT_REVIEWS_LIMIT = 200
DEFAULT_FACTS_LOAD_MODE = "daily"
DEFAULT_SNAPSHOT_MARKETS = "VN"
DEFAULT_FACT_COUNTRY_CODES = "VN"
MAPPING_ARTIFACT_MODES = frozenset({"legacy", "artifact"})
FULL_LOAD_MODE = "full"
DAILY_LOAD_MODE = "daily"
WEEKLY_LOAD_MODE = "weekly"
QUARTERLY_LOAD_MODE = "quarterly"
BACKFILL_LOAD_MODE = "backfill"
APP_PERFORMANCE_CURRENT_STORE_FORMAT = "partitioned_app_performance_current_v1"
APP_ACTIVE_USERS_CURRENT_STORE_FORMAT = "partitioned_app_active_users_current_v1"
GAME_FULL_INFO_COLUMNS = [
    "unified_app_id",
    "canonical_app_id",
    "name",
    "cohort_id",
    "itunes_apps",
    "android_apps",
    "unified_publisher_ids",
    "itunes_publisher_ids",
    "android_publisher_ids",
    "game_class",
    "game_genre",
    "game_subgenre",
    "game_art_style",
    "game_camera_pov",
    "game_setting",
    "game_theme",
    "game_product_model",
    "game_ip_corporate_parent",
    "game_ip_operator",
    "game_ip_media_type",
    "game_licensed_ip",
    "game_earliest_release_date",
    "game_release_date_ww",
    "game_release_date_us",
    "game_release_date_jp",
    "game_release_date_cn",
]
SNAPSHOT_STAGE_START_YEAR = 2014
SNAPSHOT_STAGE_PAGE_SIZE = 2000
SNAPSHOT_STAGE_TOP_K = 10000
SNAPSHOT_STAGE_MAX_CANDIDATE_COUNT = 100000
DAILY_OVERLAP_DAYS = 3
WEEKLY_OVERLAP_DAYS = 14
QUARTERLY_OVERLAP_DAYS = 365
SNAPSHOT_RAW_RESPONSE_SAMPLE_LIMIT = 3
SCIENTIFIC_NOTATION_PATTERN = re.compile(r"^[+-]?\d+(?:\.\d+)?[eE][+-]?\d{1,3}$")
DECIMAL_ZERO_PATTERN = re.compile(r"^[+-]?\d+\.0+$")
DECIMAL_NUMBER_PATTERN = re.compile(r"^[+-]?\d+\.\d+$")
MAX_SIGNED_INT64 = 9223372036854775807
BASE_STAGE_ORDER = [
    "game_full_info",
    "app_full_info_ndjson",
    "publisher_mapping_top_down",
    "app_performance_daily",
]
SOURCE_BASE_STAGE_ORDER = [
    "fetch_top_game_snapshot",
    "update_top_game_history",
    "materialize_game_list",
    "build_game_full_info",
    "materialize_app_list",
    "build_app_full_info",
    "build_app_performance",
    "build_app_active_users",
    "build_app_retention",
    "build_user_demographics",
    "build_user_reviews",
]
STAGING_STAGE_ORDER = [
    "publisher_mapping_top_down",
    "publisher_revenue_mapping_full",
    "app_full_info_adjusted_ndjson",
    "app_performance_daily_revenue_adjusted",
    "app_active_users_current_ndjson",
    "game_full_info_adjusted",
]
CONFIG_INPUT_KEYS = {
    "TOP_GAME_ANNUAL_PERFORMANCE": "top_game_annual_performance",
    "TOP_GAME_ALL_HISTORY_INITIAL": "top_game_all_history_initial",
    "MAPPING_PUBLISHER_TO_APPS_TOP_DOWN": "mapping_publisher_to_apps_top_down",
    "MAPPING_PUBLISHER_TO_PUBLISER_ID_AND_REVENUE_MULTIPLIER_BOTTOM_UP": "mapping_publisher_to_publisherids_revenue_multiplier_bottom_up",
    "MAPPING_APP_TO_REVENUE_MULTIPLIER_SPECIAL_CASES": "mapping_app_to_revenue_multiplier_special_cases",
    "GAME_INFO_ADJUSTMENT": "game_info_adjustment",
}


@dataclass
class PipelineContext:
    config_path: Path
    repo_root: Path
    output_root: Path
    input_root: Optional[Path]
    logs_root: Path
    run_id: str
    run_timestamp: int
    trigger_mode: str
    base_url: str
    performance_start_date: str
    performance_end_date: str
    facts_load_mode: str
    facts_start_date: Optional[str]
    snapshot_start_year: Optional[int]
    snapshot_end_year: Optional[int]
    snapshot_regions: Optional[str]
    fact_country_codes: Optional[str]
    snapshot_category: Optional[str]
    snapshot_custom_fields_filter_id: Optional[str]
    snapshot_data_model: Optional[str]
    snapshot_target_count: Optional[int]
    game_list_limit: Optional[int]
    game_list_source: Optional[str]
    force_history_rebuild_from_seed: bool
    force_fail_stage: Optional[str]
    mapping_artifact_manifest_path: Optional[Path]
    mapping_publisher_to_apps_top_down_override: Optional[Path]
    mapping_publisher_to_publisherids_revenue_multiplier_bottom_up_override: Optional[Path]
    mapping_app_to_revenue_multiplier_special_cases_override: Optional[Path]
    game_info_adjustment_override: Optional[Path]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_config(config_path: Path) -> dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _mapping_artifact_mode() -> str:
    mode = os.environ.get("MAPPING_ARTIFACT_MODE", "legacy").strip().lower()
    if mode not in MAPPING_ARTIFACT_MODES:
        raise ValueError(
            "MAPPING_ARTIFACT_MODE must be either 'legacy' or 'artifact'."
        )
    return mode


def _mapping_artifact_registry(ctx: PipelineContext) -> MappingArtifactRegistry:
    configured_data_root = os.environ.get("PIPELINE_DATA_ROOT", "").strip()
    data_root = Path(configured_data_root).expanduser() if configured_data_root else ctx.output_root
    registry = MappingArtifactRegistry(data_root)
    if configured_data_root and ctx.output_root.resolve() != registry.data_root:
        raise ValueError(
            "Artifact-mode staging requires output_root to match PIPELINE_DATA_ROOT."
        )
    return registry


def _mapping_artifact_manifest(ctx: PipelineContext) -> tuple[Path, dict[str, Any]]:
    return ensure_mapping_artifact_manifest(
        _mapping_artifact_registry(ctx),
        output_root=ctx.output_root,
        run_id=ctx.run_id,
        run_timestamp=ctx.run_timestamp,
        manifest_path=ctx.mapping_artifact_manifest_path,
    )


def _mapping_manifest_metrics(path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "mapping_artifact_mode": "artifact",
        "mapping_artifact_manifest_path": str(path.resolve()),
        "mapping_artifact_fingerprint": manifest["mapping_artifact_fingerprint"],
        "pinned_full_artifact_id": manifest["full_mapping"]["artifact_id"],
        "pinned_full_checksum_sha256": manifest["full_mapping"]["checksum_sha256"],
        "pinned_top_down_artifact_id": manifest["enriched_top_down"]["artifact_id"],
        "pinned_top_down_checksum_sha256": manifest["enriched_top_down"]["checksum_sha256"],
    }


def _coerce_runtime_path(raw_value: Any, *, repo_root: Optional[Path] = None) -> Path:
    text = str(raw_value)
    path = Path(text)

    if os.name == "nt" and text.startswith("/"):
        return Path(text)

    if path.is_absolute():
        return path.resolve()

    if repo_root is not None:
        return (repo_root / path).resolve()
    return path.resolve()


def _resolve_input_path(repo_root: Path, configured_value: str, input_root: Optional[Path]) -> Path:
    candidates: list[Path] = []
    configured_path = Path(configured_value)
    if configured_path.is_absolute():
        candidates.append(_coerce_runtime_path(configured_value))
    else:
        candidates.append((repo_root / configured_path).resolve())
        if input_root is not None:
            candidates.append((input_root / configured_path).resolve())
            candidates.append((input_root / configured_path.name).resolve())

    for candidate in candidates:
        if candidate.exists():
            return candidate

    if input_root is not None:
        return (input_root / configured_path.name).resolve()
    return (repo_root / configured_path).resolve()


def _resolve_override_path(repo_root: Path, override_value: Any) -> Path:
    return _coerce_runtime_path(override_value, repo_root=repo_root)


def resolve_input_paths(
    config: dict[str, Any],
    repo_root: Path,
    input_root: Optional[Path],
    *,
    overrides: Optional[dict[str, Any]] = None,
) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for config_key, alias in CONFIG_INPUT_KEYS.items():
        if config_key not in config:
            continue
        resolved[alias] = str(
            _resolve_input_path(repo_root, str(config[config_key]), input_root)
        )
    if overrides:
        for alias, override_value in overrides.items():
            if override_value in (None, ""):
                continue
            resolved[alias] = str(_resolve_override_path(repo_root, override_value))
    return resolved


def _base_outputs(output_root: Path, run_timestamp: int) -> dict[str, Path]:
    return {
        "game_full_info": output_root / "base" / f"st_api_game_full_info_{run_timestamp}.csv",
        "app_full_info_ndjson": output_root / "base" / f"st_api_app_full_info_{run_timestamp}.ndjson",
        "publisher_mapping_top_down": output_root / "base" / f"st_api_mapping_publisher_to_apps_publisherids_top_down_{run_timestamp}.csv",
        "app_performance_daily": output_root / "base" / f"st_app_performance_daily_{run_timestamp}.json",
    }


def _staging_outputs(output_root: Path, run_timestamp: int) -> dict[str, Path]:
    return {
        "publisher_revenue_mapping_full": output_root / "staging" / f"st_mapping_publisher_to_publisherids_revenue_multiplier_full_{run_timestamp}.csv",
        "app_full_info_adjusted_ndjson": output_root / "staging" / f"st_app_full_info_adjusted_{run_timestamp}.ndjson",
        "app_performance_daily_revenue_adjusted": output_root / "staging" / f"st_app_performance_daily_{run_timestamp}_revenue_adjusted.json",
        "game_full_info_adjusted": output_root / "staging" / f"st_api_game_full_info_{run_timestamp}_adjusted.csv",
    }


def _staging_current_outputs(output_root: Path) -> dict[str, Path]:
    return {
        "publisher_revenue_mapping_full": output_root / "staging" / "st_mapping_publisher_to_publisherids_revenue_multiplier_full_current.csv",
        "app_full_info_adjusted_ndjson": output_root / "staging" / "st_app_full_info_adjusted_current.ndjson",
        "app_performance_daily_revenue_adjusted": output_root / "staging" / "st_app_performance_daily_revenue_adjusted_current.ndjson",
        "app_active_users_current_ndjson": output_root / "staging" / "st_app_active_users_current.ndjson",
        "game_full_info_adjusted": output_root / "staging" / "st_api_game_full_info_adjusted_current.csv",
    }


def _source_base_outputs(output_root: Path, run_timestamp: int) -> dict[str, Path]:
    return {
        "top_game_snapshot": output_root / "base" / f"st_top_game_annual_performance_snapshot_{run_timestamp}.csv",
        "top_game_history": output_root / "base" / f"st_top_game_annual_performance_all_history_{run_timestamp}.csv",
        "top_game_history_current": output_root / "base" / "st_top_game_annual_performance_all_history_current.csv",
        "top_game_history_all_markets": output_root
        / "base"
        / f"st_top_game_annual_performance_all_history_all_markets_{run_timestamp}.csv",
        "top_game_history_all_markets_current": output_root
        / "base"
        / "st_top_game_annual_performance_all_history_all_markets_current.csv",
        "snapshot_unified_raw": output_root / "base" / f"st_snapshot_unified_app_metadata_{run_timestamp}.ndjson",
        "snapshot_ios_local_raw": output_root / "base" / f"st_snapshot_ios_app_metadata_{run_timestamp}.ndjson",
        "snapshot_android_local_raw": output_root / "base" / f"st_snapshot_android_app_metadata_{run_timestamp}.ndjson",
        "list_of_all_games": output_root / "base" / f"st_list_of_all_games_{run_timestamp}.json",
        "list_of_all_games_by_market": output_root / "base" / f"st_list_of_all_games_by_market_{run_timestamp}.json",
        "game_full_info": output_root / "base" / f"st_api_game_full_info_{run_timestamp}.csv",
        "list_of_all_apps": output_root / "base" / f"st_list_of_all_apps_{run_timestamp}.json",
        "app_full_info": output_root / "base" / f"st_api_app_full_info_{run_timestamp}.ndjson",
        "app_performance": output_root / "base" / f"st_app_performance_daily_{run_timestamp}.json",
        "app_active_users": output_root / "base" / f"st_app_active_users_{run_timestamp}.json",
        "app_retention": output_root / "base" / f"st_app_retention_daily_{run_timestamp}.ndjson",
        "user_demographics": output_root / "base" / f"st_app_demographics_{run_timestamp}.ndjson",
        "user_reviews": output_root / "base" / f"st_app_user_reviews_{run_timestamp}.ndjson",
    }


def _source_base_state_outputs(output_root: Path) -> dict[str, Path]:
    return {
        "app_performance_current": output_root / "base" / "st_app_performance_daily_current",
        "app_performance_history_archive": output_root / "base" / "st_app_performance_daily_history.ndjson",
        "game_full_info_current": output_root / "base" / "st_api_game_full_info_current.csv",
        "app_full_info_current": output_root / "base" / "st_api_app_full_info_current.ndjson",
        "publisher_mapping_top_down_current": output_root / "base" / "st_api_mapping_publisher_to_apps_publisherids_top_down_current.csv",
        "app_active_users_current": output_root / "base" / "st_app_active_users_current",
        "app_active_users_history_archive": output_root / "base" / "st_app_active_users_history.ndjson",
        "app_retention_current": output_root / "base" / "st_app_retention_daily_current.ndjson",
        "app_retention_history_archive": output_root / "base" / "st_app_retention_daily_history.ndjson",
        "user_demographics_current": output_root / "base" / "st_app_demographics_current.ndjson",
        "user_demographics_history_archive": output_root / "base" / "st_app_demographics_history.ndjson",
        "snapshot_unified_raw_current": output_root / "base" / "st_snapshot_unified_app_metadata_current.ndjson",
        "snapshot_local_raw_current": output_root / "base" / "st_snapshot_local_app_metadata_current.ndjson",
    }


def _normalize_code_list(value: Any, default: str = DEFAULT_SNAPSHOT_MARKETS) -> list[str]:
    if value in (None, ""):
        value = default
    if isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = str(value).split(",")

    codes: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        code = str(item).strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    if not codes:
        raise ValueError("Expected at least one market/country code.")
    return codes


def _top_game_market_outputs(output_root: Path, run_timestamp: int, market: str) -> dict[str, Path]:
    normalized_market = market.strip().upper()
    return {
        "snapshot": output_root
        / "base"
        / f"st_top_game_annual_performance_snapshot_{normalized_market}_{run_timestamp}.csv",
        "history": output_root
        / "base"
        / f"st_top_game_annual_performance_all_history_{normalized_market}_{run_timestamp}.csv",
        "history_current": output_root
        / "base"
        / f"st_top_game_annual_performance_all_history_{normalized_market}_current.csv",
    }


def expected_output_paths(output_root: Path, run_timestamp: int) -> dict[str, str]:
    paths = {}
    for name, path in {**_base_outputs(output_root, run_timestamp), **_staging_outputs(output_root, run_timestamp)}.items():
        paths[name] = str(path.resolve())
    return paths


def expected_source_base_output_paths(output_root: Path, run_timestamp: int) -> dict[str, str]:
    return {name: str(path.resolve()) for name, path in _source_base_outputs(output_root, run_timestamp).items()}


def expected_staging_current_output_paths(output_root: Path) -> dict[str, str]:
    paths = {
        **_staging_current_outputs(output_root),
        "publisher_mapping_top_down_current": _source_base_state_outputs(output_root)["publisher_mapping_top_down_current"],
    }
    return {name: str(path.resolve()) for name, path in paths.items()}


def _ensure_output_dirs(ctx: PipelineContext) -> None:
    (ctx.output_root / "base").mkdir(parents=True, exist_ok=True)
    (ctx.output_root / "staging").mkdir(parents=True, exist_ok=True)
    ctx.logs_root.mkdir(parents=True, exist_ok=True)


def _run_log_dir(ctx: PipelineContext) -> Path:
    run_dir = ctx.logs_root / ctx.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _write_named_result(ctx: PipelineContext, name: str, payload: dict[str, Any]) -> str:
    stage_suffix = ""
    if name == "run_source_base_pipeline" and payload.get("stage_selection") not in {None, "all"}:
        stage_suffix = f"_{payload['stage_selection']}"
    result_path = _run_log_dir(ctx) / f"{name}{stage_suffix}.json"
    _write_json(result_path, payload)
    return str(result_path.resolve())


def _file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(1 for row in reader if row)


def _count_ndjson_lines(path: Path) -> int:
    count = 0
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def _build_stage_result(
    stage_name: str,
    started_at: str,
    finished_at: str,
    input_files: dict[str, str],
    output_files: dict[str, str],
    metrics: Optional[dict[str, Any]] = None,
    status: str = "success",
    error_summary: Optional[str] = None,
) -> dict[str, Any]:
    started = datetime.fromisoformat(started_at)
    finished = datetime.fromisoformat(finished_at)
    result = {
        "stage_name": stage_name,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round((finished - started).total_seconds(), 3),
        "input_files": input_files,
        "output_files": output_files,
    }
    if metrics:
        result["metrics"] = metrics
    if error_summary:
        result["error_summary"] = error_summary
    return result


def _read_api_key(repo_root: Path) -> str:
    load_dotenv(dotenv_path=repo_root / ".env")
    api_key = os.getenv("ST_API_KEY")
    if not api_key:
        raise RuntimeError("Missing ST_API_KEY in .env or environment.")
    return api_key


def _coerce_optional_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    return int(value)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def create_context(args: argparse.Namespace) -> PipelineContext:
    config_path = _coerce_runtime_path(args.config_path)
    repo_root = config_path.parent
    output_root = _coerce_runtime_path(args.output_root or DEFAULT_OUTPUT_ROOT, repo_root=repo_root)

    input_root = None
    if getattr(args, "input_root", None):
        input_root = _coerce_runtime_path(args.input_root, repo_root=repo_root)

    run_id = getattr(args, "run_id", None) or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    requested_run_timestamp = getattr(args, "run_timestamp", None)
    if requested_run_timestamp in (None, ""):
        requested_manifest = getattr(args, "mapping_artifact_manifest", None)
        candidate_manifest = (
            _coerce_runtime_path(requested_manifest, repo_root=repo_root)
            if requested_manifest not in (None, "")
            else default_manifest_path(output_root, run_id)
        )
        if candidate_manifest.exists():
            try:
                with open(candidate_manifest, "r", encoding="utf-8") as handle:
                    requested_run_timestamp = json.load(handle).get("run_timestamp")
            except (OSError, json.JSONDecodeError, AttributeError):
                requested_run_timestamp = None
    run_timestamp = int(requested_run_timestamp or int(time.time()))
    trigger_mode = getattr(args, "trigger_mode", None) or "manual"
    performance_end_date = getattr(args, "performance_end_date", None) or (date.today() - timedelta(days=3)).isoformat()
    facts_load_mode = getattr(args, "facts_load_mode", None) or DEFAULT_FACTS_LOAD_MODE
    facts_start_date = getattr(args, "facts_start_date", None) or os.getenv("FACTS_START_DATE")
    snapshot_start_year = _coerce_optional_int(getattr(args, "snapshot_start_year", None))
    snapshot_end_year = _coerce_optional_int(getattr(args, "snapshot_end_year", None))
    snapshot_regions = getattr(args, "snapshot_regions", None) or os.getenv("SNAPSHOT_REGIONS")
    fact_country_codes = getattr(args, "fact_country_codes", None) or os.getenv("FACT_COUNTRY_CODES")
    snapshot_category = getattr(args, "snapshot_category", None)
    snapshot_custom_fields_filter_id = getattr(args, "snapshot_custom_fields_filter_id", None)
    snapshot_data_model = getattr(args, "snapshot_data_model", None)
    snapshot_target_count = _coerce_optional_int(getattr(args, "snapshot_target_count", None))
    game_list_limit = _coerce_optional_int(getattr(args, "game_list_limit", None))
    game_list_source = getattr(args, "game_list_source", None)
    force_history_rebuild_from_seed = _coerce_bool(getattr(args, "force_history_rebuild_from_seed", None))
    force_fail_stage = getattr(args, "force_fail_stage", None)
    mapping_artifact_manifest_path = getattr(args, "mapping_artifact_manifest", None)
    mapping_publisher_to_apps_top_down_override = getattr(args, "mapping_publisher_to_apps_top_down", None)
    mapping_publisher_to_publisherids_revenue_multiplier_bottom_up_override = getattr(
        args,
        "mapping_publisher_to_publisherids_revenue_multiplier_bottom_up",
        None,
    )
    mapping_app_to_revenue_multiplier_special_cases_override = getattr(
        args,
        "mapping_app_to_revenue_multiplier_special_cases",
        None,
    )
    game_info_adjustment_override = getattr(args, "game_info_adjustment", None)

    return PipelineContext(
        config_path=config_path,
        repo_root=repo_root,
        output_root=output_root,
        input_root=input_root,
        logs_root=output_root / "logs",
        run_id=run_id,
        run_timestamp=run_timestamp,
        trigger_mode=trigger_mode,
        base_url=BASE_URL,
        performance_start_date=DEFAULT_APP_PERFORMANCE_START_DATE,
        performance_end_date=performance_end_date,
        facts_load_mode=facts_load_mode,
        facts_start_date=facts_start_date,
        snapshot_start_year=snapshot_start_year,
        snapshot_end_year=snapshot_end_year,
        snapshot_regions=snapshot_regions,
        fact_country_codes=fact_country_codes,
        snapshot_category=snapshot_category,
        snapshot_custom_fields_filter_id=snapshot_custom_fields_filter_id,
        snapshot_data_model=snapshot_data_model,
        snapshot_target_count=snapshot_target_count,
        game_list_limit=game_list_limit,
        game_list_source=game_list_source,
        force_history_rebuild_from_seed=force_history_rebuild_from_seed,
        force_fail_stage=force_fail_stage,
        mapping_artifact_manifest_path=_resolve_override_path(repo_root, mapping_artifact_manifest_path)
        if mapping_artifact_manifest_path not in (None, "")
        else None,
        mapping_publisher_to_apps_top_down_override=_resolve_override_path(repo_root, mapping_publisher_to_apps_top_down_override)
        if mapping_publisher_to_apps_top_down_override not in (None, "")
        else None,
        mapping_publisher_to_publisherids_revenue_multiplier_bottom_up_override=_resolve_override_path(
            repo_root,
            mapping_publisher_to_publisherids_revenue_multiplier_bottom_up_override,
        )
        if mapping_publisher_to_publisherids_revenue_multiplier_bottom_up_override not in (None, "")
        else None,
        mapping_app_to_revenue_multiplier_special_cases_override=_resolve_override_path(
            repo_root,
            mapping_app_to_revenue_multiplier_special_cases_override,
        )
        if mapping_app_to_revenue_multiplier_special_cases_override not in (None, "")
        else None,
        game_info_adjustment_override=_resolve_override_path(repo_root, game_info_adjustment_override)
        if game_info_adjustment_override not in (None, "")
        else None,
    )


def _context_input_overrides(ctx: PipelineContext) -> dict[str, Any]:
    return {
        "mapping_publisher_to_apps_top_down": ctx.mapping_publisher_to_apps_top_down_override,
        "mapping_publisher_to_publisherids_revenue_multiplier_bottom_up": (
            ctx.mapping_publisher_to_publisherids_revenue_multiplier_bottom_up_override
        ),
        "mapping_app_to_revenue_multiplier_special_cases": (
            ctx.mapping_app_to_revenue_multiplier_special_cases_override
        ),
        "game_info_adjustment": ctx.game_info_adjustment_override,
    }


def _base_summary(ctx: PipelineContext, stage: str) -> dict[str, Any]:
    return {
        "run_id": ctx.run_id,
        "run_timestamp": ctx.run_timestamp,
        "trigger_mode": ctx.trigger_mode,
        "config_path": str(ctx.config_path),
        "output_root": str(ctx.output_root),
        "stage_selection": stage,
        "stage_results": [],
        "status": "success",
        "expected_outputs": expected_output_paths(ctx.output_root, ctx.run_timestamp),
    }


def validate_inputs(ctx: PipelineContext, scope: str = "all") -> dict[str, Any]:
    _ensure_output_dirs(ctx)
    started_at = _utc_now_iso()
    config = _load_config(ctx.config_path)
    resolved_inputs = resolve_input_paths(
        config,
        ctx.repo_root,
        ctx.input_root,
        overrides=_context_input_overrides(ctx),
    )
    if scope == "source_base":
        source_base_inputs: dict[str, str] = {}
        current_history_path = _source_base_outputs(ctx.output_root, ctx.run_timestamp)["top_game_history_current"]
        if ctx.force_history_rebuild_from_seed or not current_history_path.exists():
            seed_path = _resolve_top_game_history_seed_path(config, ctx, resolved_inputs)
            if seed_path is not None:
                source_base_inputs["top_game_all_history_initial"] = str(seed_path)
        resolved_inputs = source_base_inputs

    missing_files = []
    unreadable_files = []
    for alias, file_path in resolved_inputs.items():
        path_obj = Path(file_path)
        if not path_obj.exists():
            missing_files.append({"name": alias, "path": str(path_obj)})
            continue
        if not path_obj.is_file() or not os.access(path_obj, os.R_OK):
            unreadable_files.append({"name": alias, "path": str(path_obj)})

    load_dotenv(dotenv_path=ctx.repo_root / ".env")
    api_key_present = bool(os.getenv("ST_API_KEY"))
    if not api_key_present:
        unreadable_files.append({"name": "env.ST_API_KEY", "path": str(ctx.repo_root / ".env")})

    finished_at = _utc_now_iso()
    status = "success" if not missing_files and not unreadable_files else "failed"
    error_summary = None
    if status == "failed":
        parts = []
        if missing_files:
            parts.append(f"missing files: {', '.join(item['name'] for item in missing_files)}")
        if unreadable_files:
            parts.append(f"unreadable files: {', '.join(item['name'] for item in unreadable_files)}")
        error_summary = "; ".join(parts)

    stage_result = _build_stage_result(
        stage_name="validate_inputs",
        started_at=started_at,
        finished_at=finished_at,
        input_files=resolved_inputs,
        output_files={},
        metrics={
            "input_file_count": len(resolved_inputs),
            "missing_file_count": len(missing_files),
            "unreadable_file_count": len(unreadable_files),
            "api_key_present": api_key_present,
        },
        status=status,
        error_summary=error_summary,
    )

    summary = {
        "run_id": ctx.run_id,
        "run_timestamp": ctx.run_timestamp,
        "trigger_mode": ctx.trigger_mode,
        "facts_load_mode": ctx.facts_load_mode,
        "config_path": str(ctx.config_path),
        "input_root": str(ctx.input_root) if ctx.input_root else None,
        "output_root": str(ctx.output_root),
        "status": status,
        "stage_name": "validate_inputs",
        "scope": scope,
        "discovered_inputs": resolved_inputs,
        "missing_files": missing_files,
        "unreadable_files": unreadable_files,
        "stage_result": stage_result,
    }
    summary["result_file"] = _write_named_result(ctx, "validate_inputs", summary)
    return summary


def _guard_forced_failure(ctx: PipelineContext, stage_name: str) -> None:
    if ctx.force_fail_stage and ctx.force_fail_stage == stage_name:
        raise RuntimeError(f"Forced failure for stage '{stage_name}'.")


def _execute_stage(
    ctx: PipelineContext,
    stage_name: str,
    input_files: dict[str, str],
    output_files: dict[str, str],
    stage_fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    started_at = _utc_now_iso()
    _guard_forced_failure(ctx, stage_name)
    metrics = stage_fn()
    finished_at = _utc_now_iso()
    return _build_stage_result(
        stage_name=stage_name,
        started_at=started_at,
        finished_at=finished_at,
        input_files=input_files,
        output_files=output_files,
        metrics=metrics,
    )


def run_base_pipeline(ctx: PipelineContext, stage: str = "all") -> dict[str, Any]:
    _ensure_output_dirs(ctx)
    config = _load_config(ctx.config_path)
    resolved_inputs = resolve_input_paths(
        config,
        ctx.repo_root,
        ctx.input_root,
        overrides=_context_input_overrides(ctx),
    )
    outputs = _base_outputs(ctx.output_root, ctx.run_timestamp)
    summary = _base_summary(ctx, stage)
    current_input_files: dict[str, str] = {}
    current_output_files: dict[str, str] = {}
    stage_order = BASE_STAGE_ORDER if stage == "all" else [stage]
    stage_name = stage_order[0] if stage_order else "unknown"

    try:
        api_key = _read_api_key(ctx.repo_root)
        for stage_name in stage_order:
            if stage_name == "game_full_info":
                def _run() -> dict[str, Any]:
                    top_games_path = Path(resolved_inputs["top_game_annual_performance"])
                    df_top_games = pd.read_csv(top_games_path)
                    unified_app_ids = set(df_top_games["unified_id"].dropna().tolist())
                    df_game_full_info = lsdp.retrieve_full_info_game_table(api_key, ctx.base_url, unified_app_ids)
                    outputs["game_full_info"].parent.mkdir(parents=True, exist_ok=True)
                    df_game_full_info.to_csv(outputs["game_full_info"], index=False)
                    return {
                        "row_count": int(len(df_game_full_info)),
                        "unified_app_id_count": int(len(unified_app_ids)),
                        "file_size_bytes": _file_size(outputs["game_full_info"]),
                    }

                current_input_files = {
                    "top_game_annual_performance": resolved_inputs["top_game_annual_performance"]
                }
                current_output_files = {"game_full_info": str(outputs["game_full_info"])}
                stage_result = _execute_stage(
                    ctx,
                    stage_name,
                    current_input_files,
                    current_output_files,
                    _run,
                )

            elif stage_name == "app_full_info_ndjson":
                def _run() -> dict[str, Any]:
                    df_game_full_info = pd.read_csv(outputs["game_full_info"], low_memory=False)
                    lsdp.get_local_app_info_from_game_full_info_table_ndjson_version(
                        api_key,
                        ctx.base_url,
                        df_game_full_info,
                        ctx.run_timestamp,
                        str(ctx.output_root / "base"),
                    )
                    return {
                        "row_count": _count_ndjson_lines(outputs["app_full_info_ndjson"]),
                        "file_size_bytes": _file_size(outputs["app_full_info_ndjson"]),
                    }

                current_input_files = {"game_full_info": str(outputs["game_full_info"])}
                current_output_files = {"app_full_info_ndjson": str(outputs["app_full_info_ndjson"])}
                stage_result = _execute_stage(
                    ctx,
                    stage_name,
                    current_input_files,
                    current_output_files,
                    _run,
                )

            elif stage_name == "publisher_mapping_top_down":
                def _run() -> dict[str, Any]:
                    df_mapping = lsdp.create_mapping_publisher_to_apps_publisherids(
                        resolved_inputs["mapping_publisher_to_apps_top_down"],
                        api_key,
                        ctx.base_url,
                    )
                    df_mapping.to_csv(outputs["publisher_mapping_top_down"], index=False)
                    return {
                        "row_count": int(len(df_mapping)),
                        "file_size_bytes": _file_size(outputs["publisher_mapping_top_down"]),
                    }

                current_input_files = {
                    "mapping_publisher_to_apps_top_down": resolved_inputs["mapping_publisher_to_apps_top_down"]
                }
                current_output_files = {
                    "publisher_mapping_top_down": str(outputs["publisher_mapping_top_down"])
                }
                stage_result = _execute_stage(
                    ctx,
                    stage_name,
                    current_input_files,
                    current_output_files,
                    _run,
                )

            elif stage_name == "app_performance_daily":
                def _run() -> dict[str, Any]:
                    df_app_full_info = pd.read_json(outputs["app_full_info_ndjson"], lines=True)
                    app_performance_data = lsdp.create_table_app_performance_grouped_by_game_daily(
                        api_key,
                        ctx.base_url,
                        str_start_date=ctx.performance_start_date,
                        str_end_date=ctx.performance_end_date,
                        df_app_full_info=df_app_full_info,
                        json_export_path=str(ctx.output_root / "base"),
                        export_path=str(outputs["app_performance_daily"]),
                    )
                    return {
                        "row_count": int(len(app_performance_data)),
                        "file_size_bytes": _file_size(outputs["app_performance_daily"]),
                    }

                current_input_files = {"app_full_info_ndjson": str(outputs["app_full_info_ndjson"])}
                current_output_files = {"app_performance_daily": str(outputs["app_performance_daily"])}
                stage_result = _execute_stage(
                    ctx,
                    stage_name,
                    current_input_files,
                    current_output_files,
                    _run,
                )
            else:
                raise ValueError(f"Unsupported base stage '{stage_name}'.")

            summary["stage_results"].append(stage_result)

        summary["result_file"] = _write_named_result(ctx, "run_base_pipeline", summary)
        return summary

    except Exception as exc:
        summary["status"] = "failed"
        summary["failed_stage"] = stage_name
        summary["error_summary"] = str(exc)
        summary["traceback"] = traceback.format_exc()
        failure_result = _build_stage_result(
            stage_name=stage_name,
            started_at=_utc_now_iso(),
            finished_at=_utc_now_iso(),
            input_files=current_input_files,
            output_files=current_output_files,
            status="failed",
            error_summary=str(exc),
        )
        summary["stage_results"].append(failure_result)
        summary["result_file"] = _write_named_result(ctx, "run_base_pipeline", summary)
        return summary


def run_staging_pipeline(ctx: PipelineContext, stage: str = "all") -> dict[str, Any]:
    _ensure_output_dirs(ctx)
    config = _load_config(ctx.config_path)
    resolved_inputs = resolve_input_paths(
        config,
        ctx.repo_root,
        ctx.input_root,
        overrides=_context_input_overrides(ctx),
    )
    state_outputs = _source_base_state_outputs(ctx.output_root)
    staging_outputs = _staging_current_outputs(ctx.output_root)
    run_scoped_staging_outputs = _staging_outputs(ctx.output_root, ctx.run_timestamp)
    mapping_artifact_mode = _mapping_artifact_mode()
    mapping_manifest_path: Optional[Path] = None
    mapping_manifest: Optional[dict[str, Any]] = None
    stage_order = STAGING_STAGE_ORDER if stage == "all" else [stage]
    full_mapping_path = (
        run_scoped_staging_outputs["publisher_revenue_mapping_full"]
        if mapping_artifact_mode == "artifact"
        else staging_outputs["publisher_revenue_mapping_full"]
    )
    summary = _base_summary(ctx, stage)
    summary["expected_outputs"] = expected_staging_current_output_paths(ctx.output_root)
    if mapping_artifact_mode == "artifact":
        summary["expected_outputs"]["publisher_revenue_mapping_full_run_scoped"] = str(full_mapping_path.resolve())
    current_input_files: dict[str, str] = {}
    current_output_files: dict[str, str] = {}
    stage_name = stage_order[0] if stage_order else "unknown"

    try:
        if mapping_artifact_mode == "artifact":
            mapping_manifest_path, mapping_manifest = _mapping_artifact_manifest(ctx)
            summary["mapping_artifact_manifest"] = str(mapping_manifest_path.resolve())
            summary["mapping_artifact_fingerprint"] = mapping_manifest["mapping_artifact_fingerprint"]
        # Artifact mode materializes reviewed bytes and intentionally does not
        # call SensorTower again for publisher enrichment during scheduled ETL.
        if mapping_artifact_mode == "legacy" and "publisher_mapping_top_down" in stage_order:
            _read_api_key(ctx.repo_root)
        for stage_name in stage_order:
            if stage_name == "publisher_mapping_top_down":
                def _run() -> dict[str, Any]:
                    if mapping_artifact_mode == "artifact":
                        metrics = _mapping_artifact_registry(ctx).materialize_enriched_top_down(
                            mapping_manifest["full_mapping"]["artifact_id"],
                            state_outputs["publisher_mapping_top_down_current"]
                        )
                        metrics.update(_mapping_manifest_metrics(mapping_manifest_path, mapping_manifest))
                        return metrics
                    api_key = _read_api_key(ctx.repo_root)
                    df_mapping = lsdp.create_mapping_publisher_to_apps_publisherids(
                        resolved_inputs["mapping_publisher_to_apps_top_down"],
                        api_key,
                        ctx.base_url,
                    )
                    _write_csv_dataframe_atomic(df_mapping, state_outputs["publisher_mapping_top_down_current"])
                    return {
                        "row_count": int(len(df_mapping)),
                        "file_size_bytes": _file_size(state_outputs["publisher_mapping_top_down_current"]),
                    }

                current_input_files = (
                    {"pinned_full_mapping": f"mapping_artifacts://full/{mapping_manifest['full_mapping']['artifact_id']}"}
                    if mapping_artifact_mode == "artifact"
                    else {"mapping_publisher_to_apps_top_down": resolved_inputs["mapping_publisher_to_apps_top_down"]}
                )
                current_output_files = {
                    "publisher_mapping_top_down_current": str(state_outputs["publisher_mapping_top_down_current"])
                }
                stage_result = _execute_stage(
                    ctx,
                    stage_name,
                    current_input_files,
                    current_output_files,
                    _run,
                )

            elif stage_name == "publisher_revenue_mapping_full":
                def _run() -> dict[str, Any]:
                    if mapping_artifact_mode == "artifact":
                        metrics = _mapping_artifact_registry(ctx).materialize_full(
                            mapping_manifest["full_mapping"]["artifact_id"],
                            full_mapping_path,
                            current_destination=staging_outputs["publisher_revenue_mapping_full"],
                        )
                        metrics.update(_mapping_manifest_metrics(mapping_manifest_path, mapping_manifest))
                        return metrics
                    top_down_types = {
                        "os_x": "string",
                        "cleaned_publisher_name": "string",
                        "game_name": "string",
                        "sensor_tower_link": "string",
                        "app_id_trimmed": "string",
                        "publisher_id": "string",
                        "publisher_name": "string",
                    }
                    bottom_up_types = {
                        "cleaned_publisher_name": "string",
                        "publisher_id": "string",
                        "publisher_name": "string",
                        "revenue_multiplier": "int64",
                    }
                    df_top_down = pd.read_csv(state_outputs["publisher_mapping_top_down_current"], dtype=top_down_types)
                    df_bottom_up = pd.read_csv(
                        resolved_inputs["mapping_publisher_to_publisherids_revenue_multiplier_bottom_up"],
                        dtype=bottom_up_types,
                    )
                    df_full = lsdp.create_mapping_publisher_to_publisherids_revenue_multiplier(
                        df_top_down,
                        df_bottom_up,
                    )
                    _write_csv_dataframe_atomic(df_full, staging_outputs["publisher_revenue_mapping_full"])
                    return {
                        "row_count": int(len(df_full)),
                        "file_size_bytes": _file_size(staging_outputs["publisher_revenue_mapping_full"]),
                    }

                current_input_files = (
                    {"pinned_full_mapping": f"mapping_artifacts://full/{mapping_manifest['full_mapping']['artifact_id']}"}
                    if mapping_artifact_mode == "artifact"
                    else {
                        "publisher_mapping_top_down_current": str(state_outputs["publisher_mapping_top_down_current"]),
                        "mapping_publisher_to_publisherids_revenue_multiplier_bottom_up": resolved_inputs["mapping_publisher_to_publisherids_revenue_multiplier_bottom_up"],
                    }
                )
                current_output_files = {
                    "publisher_revenue_mapping_full": str(full_mapping_path),
                    "publisher_revenue_mapping_full_current": str(staging_outputs["publisher_revenue_mapping_full"]),
                }
                stage_result = _execute_stage(
                    ctx,
                    stage_name,
                    current_input_files,
                    current_output_files,
                    _run,
                )

            elif stage_name == "app_full_info_adjusted_ndjson":
                def _run() -> dict[str, Any]:
                    if mapping_artifact_mode == "artifact":
                        # A direct retry reuses this run's immutable manifest.
                        materialize_metrics = _mapping_artifact_registry(ctx).materialize_full(
                            mapping_manifest["full_mapping"]["artifact_id"],
                            full_mapping_path,
                            current_destination=staging_outputs["publisher_revenue_mapping_full"],
                        )
                    stats = lsdp.adjust_ndjson_app_full_info(
                        input_ndjson_path=str(state_outputs["app_full_info_current"]),
                        output_ndjson_path=str(staging_outputs["app_full_info_adjusted_ndjson"]),
                        publisher_mapping_csv_path=str(full_mapping_path),
                        special_case_csv_path=resolved_inputs["mapping_app_to_revenue_multiplier_special_cases"],
                    )
                    metrics = {
                        "row_count": int(stats["records_written"]),
                        "skipped_duplicates": int(stats["skipped_duplicates"]),
                        "file_size_bytes": _file_size(staging_outputs["app_full_info_adjusted_ndjson"]),
                    }
                    if mapping_artifact_mode == "artifact":
                        provenance_path = staging_outputs["app_full_info_adjusted_ndjson"].with_suffix(
                            staging_outputs["app_full_info_adjusted_ndjson"].suffix + ".provenance.json"
                        )
                        provenance = {
                            "schema_version": 1,
                            "run_id": ctx.run_id,
                            "run_timestamp": ctx.run_timestamp,
                            **_mapping_manifest_metrics(mapping_manifest_path, mapping_manifest),
                            "special_cases_path": resolved_inputs["mapping_app_to_revenue_multiplier_special_cases"],
                            "scope_note": "Mapping fingerprint excludes mutable Special Cases and is not a final effective-mapping fingerprint.",
                        }
                        _write_json_payload_atomic(provenance_path, provenance)
                        metrics.update(materialize_metrics)
                        metrics.update(_mapping_manifest_metrics(mapping_manifest_path, mapping_manifest))
                        metrics["mapping_provenance_path"] = str(provenance_path.resolve())
                    return metrics

                current_input_files = {
                    "app_full_info_current": str(state_outputs["app_full_info_current"]),
                    "publisher_revenue_mapping_full": str(full_mapping_path),
                    "mapping_app_to_revenue_multiplier_special_cases": resolved_inputs["mapping_app_to_revenue_multiplier_special_cases"],
                }
                current_output_files = {
                    "app_full_info_adjusted_ndjson": str(staging_outputs["app_full_info_adjusted_ndjson"])
                }
                if mapping_artifact_mode == "artifact":
                    current_output_files["mapping_provenance"] = str(
                        staging_outputs["app_full_info_adjusted_ndjson"].with_suffix(
                            staging_outputs["app_full_info_adjusted_ndjson"].suffix + ".provenance.json"
                        )
                    )
                stage_result = _execute_stage(
                    ctx,
                    stage_name,
                    current_input_files,
                    current_output_files,
                    _run,
                )

            elif stage_name == "app_performance_daily_revenue_adjusted":
                def _run() -> dict[str, Any]:
                    stats = _stream_adjusted_app_performance_current_to_ndjson(
                        current_store_path=state_outputs["app_performance_current"],
                        adjusted_app_full_info_path=staging_outputs["app_full_info_adjusted_ndjson"],
                        output_path=staging_outputs["app_performance_daily_revenue_adjusted"],
                    )
                    return stats

                current_input_files = {
                    "app_performance_current": str(state_outputs["app_performance_current"]),
                    "app_full_info_adjusted_ndjson": str(staging_outputs["app_full_info_adjusted_ndjson"]),
                }
                current_output_files = {
                    "app_performance_daily_revenue_adjusted": str(staging_outputs["app_performance_daily_revenue_adjusted"])
                }
                stage_result = _execute_stage(
                    ctx,
                    stage_name,
                    current_input_files,
                    current_output_files,
                    _run,
                )

            elif stage_name == "app_active_users_current_ndjson":
                def _run() -> dict[str, Any]:
                    return _stream_time_series_current_to_ndjson(
                        current_store_path=state_outputs["app_active_users_current"],
                        output_path=staging_outputs["app_active_users_current_ndjson"],
                        manifest_format=APP_ACTIVE_USERS_CURRENT_STORE_FORMAT,
                        dataset_label="App active users",
                    )

                current_input_files = {
                    "app_active_users_current": str(state_outputs["app_active_users_current"]),
                }
                current_output_files = {
                    "app_active_users_current_ndjson": str(staging_outputs["app_active_users_current_ndjson"])
                }
                stage_result = _execute_stage(
                    ctx,
                    stage_name,
                    current_input_files,
                    current_output_files,
                    _run,
                )

            elif stage_name == "game_full_info_adjusted":
                def _run() -> dict[str, Any]:
                    df_game_full_info = pd.read_csv(state_outputs["game_full_info_current"], low_memory=False)
                    df_game_info_adjustment = pd.read_csv(
                        resolved_inputs["game_info_adjustment"],
                        low_memory=False,
                    )
                    df_game_full_info_adjusted = lsdp.apply_game_info_adjustments(
                        df_game_full_info,
                        df_game_info_adjustment,
                        key="unified_app_id",
                    )
                    _write_csv_dataframe_atomic(df_game_full_info_adjusted, staging_outputs["game_full_info_adjusted"])
                    return {
                        "row_count": int(len(df_game_full_info_adjusted)),
                        "file_size_bytes": _file_size(staging_outputs["game_full_info_adjusted"]),
                    }

                current_input_files = {
                    "game_full_info_current": str(state_outputs["game_full_info_current"]),
                    "game_info_adjustment": resolved_inputs["game_info_adjustment"],
                }
                current_output_files = {
                    "game_full_info_adjusted": str(staging_outputs["game_full_info_adjusted"])
                }
                stage_result = _execute_stage(
                    ctx,
                    stage_name,
                    current_input_files,
                    current_output_files,
                    _run,
                )
            else:
                raise ValueError(f"Unsupported staging stage '{stage_name}'.")

            if mapping_artifact_mode == "artifact":
                stage_result.setdefault("metrics", {}).update(
                    _mapping_manifest_metrics(mapping_manifest_path, mapping_manifest)
                )
            summary["stage_results"].append(stage_result)

        summary["result_file"] = _write_named_result(ctx, "run_staging_pipeline", summary)
        return summary

    except Exception as exc:
        summary["status"] = "failed"
        summary["failed_stage"] = stage_name
        summary["error_summary"] = str(exc)
        summary["traceback"] = traceback.format_exc()
        failure_result = _build_stage_result(
            stage_name=stage_name,
            started_at=_utc_now_iso(),
            finished_at=_utc_now_iso(),
            input_files=current_input_files,
            output_files=current_output_files,
            status="failed",
            error_summary=str(exc),
        )
        summary["stage_results"].append(failure_result)
        summary["result_file"] = _write_named_result(ctx, "run_staging_pipeline", summary)
        return summary


def _normalize_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return None
    if text.isdigit():
        return text
    if DECIMAL_ZERO_PATTERN.match(text):
        return text.split(".", 1)[0]
    if not (
        SCIENTIFIC_NOTATION_PATTERN.match(text)
        or DECIMAL_NUMBER_PATTERN.match(text)
    ):
        return text
    try:
        dec = Decimal(text)
    except (InvalidOperation, ValueError):
        return text
    normalized = format(dec, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    # Keep oversized scientific-notation IDs as raw source strings so they do
    # not expand into fake zero-padded integers that downstream lookups reject.
    if SCIENTIFIC_NOTATION_PATTERN.match(text) and normalized.isdigit():
        try:
            if int(normalized) > MAX_SIGNED_INT64:
                return text
        except ValueError:
            return text
    return normalized


def _read_top_game_annual_performance(path: Path, default_market: Optional[str] = None) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    if "unified_id" not in df.columns:
        raise ValueError(f"Expected column 'unified_id' in top-game annual performance file: {path}")
    df["unified_id"] = df["unified_id"].apply(_normalize_id)
    df = df[df["unified_id"].notna()].copy()
    if "year" in df.columns:
        df["year"] = df["year"].astype(str).str.strip()
    if "market" in df.columns:
        df["market"] = df["market"].astype(str).str.strip().str.upper()
    elif default_market:
        df["market"] = default_market.strip().upper()
    return df


def _get_snapshot_setting(config: dict[str, Any], ctx: PipelineContext, config_key: str, ctx_value: Any, default: Any) -> Any:
    if ctx_value not in (None, ""):
        return ctx_value
    return config.get(config_key, default)


def _snapshot_markets(config: dict[str, Any], ctx: PipelineContext) -> list[str]:
    regions_value = _get_snapshot_setting(
        config,
        ctx,
        "TOP_GAME_SNAPSHOT_REGIONS",
        ctx.snapshot_regions,
        DEFAULT_SNAPSHOT_MARKETS,
    )
    return _normalize_code_list(regions_value, default=DEFAULT_SNAPSHOT_MARKETS)


def _fact_country_codes(config: dict[str, Any], ctx: PipelineContext) -> str:
    codes_value = _get_snapshot_setting(
        config,
        ctx,
        "FACT_COUNTRY_CODES",
        ctx.fact_country_codes,
        DEFAULT_FACT_COUNTRY_CODES,
    )
    return ",".join(_normalize_code_list(codes_value, default=DEFAULT_FACT_COUNTRY_CODES))


def _snapshot_request_settings(config: dict[str, Any], ctx: PipelineContext, market: Optional[str] = None) -> dict[str, Any]:
    start_year = int(_get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_START_YEAR", ctx.snapshot_start_year, SNAPSHOT_STAGE_START_YEAR))
    end_year_override = _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_END_YEAR", ctx.snapshot_end_year, None)
    if end_year_override in (None, ""):
        end_year = datetime.fromisoformat(ctx.performance_end_date).year
    else:
        end_year = int(end_year_override)
    if end_year < start_year:
        raise ValueError(f"Snapshot end year {end_year} cannot be earlier than start year {start_year}.")

    regions = (market.strip().upper() if market else ",".join(_snapshot_markets(config, ctx)))

    return {
        "os": _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_OS", None, "unified"),
        "comparison_attribute": _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_COMPARISON_ATTRIBUTE", None, "absolute"),
        "time_range": _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_TIME_RANGE", None, "year"),
        "measure": _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_MEASURE", None, "revenue"),
        "device_type": _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_DEVICE_TYPE", None, "total"),
        "category": _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_CATEGORY", ctx.snapshot_category, "6014"),
        "regions": regions,
        "limit": int(_get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_LIMIT", None, SNAPSHOT_STAGE_PAGE_SIZE)),
        "target_count": int(
            _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_TARGET_COUNT", ctx.snapshot_target_count, SNAPSHOT_STAGE_TOP_K)
        ),
        "custom_fields_filter_id": _get_snapshot_setting(
            config,
            ctx,
            "TOP_GAME_SNAPSHOT_CUSTOM_FIELDS_FILTER_ID",
            ctx.snapshot_custom_fields_filter_id,
            "6733635aa340ea2c2b865350",
        ),
        "custom_tags_mode": _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_CUSTOM_TAGS_MODE", None, "include_unified_apps"),
        "data_model": _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_DATA_MODEL", ctx.snapshot_data_model, "DM_2025_Q2"),
        "max_candidate_count": int(
            _get_snapshot_setting(config, ctx, "TOP_GAME_SNAPSHOT_MAX_CANDIDATE_COUNT", None, SNAPSHOT_STAGE_MAX_CANDIDATE_COUNT)
        ),
        "post_filter_mode": _get_snapshot_setting(
            config,
            ctx,
            "TOP_GAME_SNAPSHOT_POST_FILTER_MODE",
            None,
            "strict_platform_all_apps",
        ),
        "child_app_scope": _get_snapshot_setting(
            config,
            ctx,
            "TOP_GAME_SNAPSHOT_CHILD_APP_SCOPE",
            None,
            "active_only",
        ),
        "start_year": start_year,
        "end_year": end_year,
    }


def _resolve_top_game_history_seed_path(
    config: dict[str, Any],
    ctx: PipelineContext,
    resolved_inputs: dict[str, str],
) -> Optional[Path]:
    if "top_game_all_history_initial" in resolved_inputs:
        return Path(resolved_inputs["top_game_all_history_initial"])
    if "top_game_annual_performance" in resolved_inputs:
        return Path(resolved_inputs["top_game_annual_performance"])
    configured = config.get("TOP_GAME_ALL_HISTORY_INITIAL") or config.get("TOP_GAME_ANNUAL_PERFORMANCE")
    if not configured:
        return None
    return _resolve_input_path(ctx.repo_root, str(configured), ctx.input_root)


def _build_history_lookup(*paths: Path) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    for path in paths:
        if not path or not path.exists():
            continue
        try:
            df = _read_top_game_annual_performance(path)
        except Exception:
            continue
        for _, row in df.iterrows():
            unified_id = _normalize_id(row.get("unified_id"))
            if not unified_id:
                continue
            lookup[unified_id] = {
                "unified_name": str(row.get("unified_name") or "").strip(),
                "unified_publisher_name": str(row.get("unified_publisher_name") or "").strip(),
            }
    return lookup


def _stringify_tag_number(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = text.replace(",", "").replace("$", "").replace("%", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_snapshot_average_dau(raw_row: dict[str, Any]) -> Optional[float]:
    for key in ("average_dau", "dau_absolute", "current_dau_value", "au", "dau"):
        value = raw_row.get(key)
        if value not in (None, ""):
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
    aggregate_tags = raw_row.get("aggregate_tags") or {}
    for key in (
        "Last 30 Days Average DAU (VN)",
        "Last 30 Days Average DAU (WW)",
        "Last 30 Days Average DAU (US)",
    ):
        value = _stringify_tag_number(aggregate_tags.get(key))
        if value is not None:
            return value
    return None


def _extract_unified_publisher_name(app_obj: dict[str, Any], fallback: str = "") -> str:
    direct_candidates = (
        "unified_publisher_name",
        "publisher_name",
        "publisher",
        "company_name",
    )
    for key in direct_candidates:
        value = app_obj.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    list_candidates = (
        "unified_publishers",
        "publishers",
    )
    for key in list_candidates:
        value = app_obj.get(key)
        if isinstance(value, list):
            names: list[str] = []
            for item in value:
                if isinstance(item, str) and item.strip():
                    names.append(item.strip())
                elif isinstance(item, dict):
                    for field in ("name", "publisher_name", "display_name"):
                        text = item.get(field)
                        if isinstance(text, str) and text.strip():
                            names.append(text.strip())
                            break
            if names:
                return " | ".join(dict.fromkeys(names))

    return fallback


def _top_apps_snapshot_request_params(
    api_key: str,
    settings: dict[str, Any],
    *,
    year: int,
    offset: int,
) -> dict[str, Any]:
    """Translate snapshot settings to the replacement Top Apps Facets request."""
    limit = int(settings["limit"])
    if not 1 <= limit <= 2000:
        raise ValueError(f"TOP_GAME_SNAPSHOT_LIMIT must be between 1 and 2000; got {limit}")

    measure = str(settings.get("measure") or "revenue").strip().lower()
    order_by_by_measure = {
        "revenue": "est_mobile_revenue:desc",
        "units": "est_mobile_downloads:desc",
    }
    if measure not in order_by_by_measure:
        raise ValueError(f"Unsupported TOP_GAME_SNAPSHOT_MEASURE for Top Apps API: {measure}")
    if str(settings.get("comparison_attribute") or "absolute").strip().lower() != "absolute":
        raise ValueError("Top Apps API only supports TOP_GAME_SNAPSHOT_COMPARISON_ATTRIBUTE=absolute")
    if str(settings.get("time_range") or "year").strip().lower() != "year":
        raise ValueError("Top-game snapshot migration currently supports TOP_GAME_SNAPSHOT_TIME_RANGE=year only")
    if str(settings.get("os") or "unified").strip().lower() != "unified":
        raise ValueError("Top-game snapshot output requires TOP_GAME_SNAPSHOT_OS=unified")

    params: dict[str, Any] = {
        "bundle": "mobile_store_top_apps",
        "order_by": order_by_by_measure[measure],
        "breakdown": "unified_app_id",
        "devices": "iphone,ipad,android",
        "entities": "unified_app_id:name",
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "limit": limit,
        "offset": offset,
        "auth_token": api_key,
    }
    regions = str(settings.get("regions") or "").strip().upper()
    if regions and regions not in {"WW", "WORLD", "GLOBAL"}:
        params["regions"] = regions
    custom_fields_filter_id = str(settings.get("custom_fields_filter_id") or "").strip()
    if custom_fields_filter_id:
        params["custom_fields_filter_id"] = custom_fields_filter_id
    return params


def _fetch_top_game_snapshot_api_rows(
    api_key: str,
    ctx: PipelineContext,
    settings: dict[str, Any],
    outputs: dict[str, Path],
    state_outputs: dict[str, Path],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if settings["post_filter_mode"] != "strict_platform_all_apps":
        raise ValueError(f"Unsupported TOP_GAME_SNAPSHOT_POST_FILTER_MODE: {settings['post_filter_mode']}")

    rows: list[dict[str, Any]] = []
    request_count = 0
    year_counts: dict[str, int] = {}
    year_stop_reasons: dict[str, str] = {}
    year_total_counts: dict[str, Optional[int]] = {}
    raw_response_samples: list[dict[str, Any]] = []
    # `top_apps_mobile` is a route marker, not a normal query parameter.
    endpoint = f"{ctx.base_url.rstrip('/')}/v1/facets/metrics?top_apps_mobile"
    total_years = settings["end_year"] - settings["start_year"] + 1
    effective_max_candidate_count = settings["max_candidate_count"]
    candidate_offsets = list(range(0, effective_max_candidate_count, settings["limit"]))
    max_pages_per_year = max(1, len(candidate_offsets))

    current_unified_lookup = _load_ndjson_lookup(state_outputs["snapshot_unified_raw_current"], _unified_cache_key)
    current_local_lookup = _load_ndjson_lookup(state_outputs["snapshot_local_raw_current"], _local_cache_key)
    run_unified_lookup: dict[str, dict[str, Any]] = {}
    run_ios_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    run_android_lookup: dict[tuple[str, str], dict[str, Any]] = {}

    candidate_row_count = 0
    pages_consumed = 0
    unified_cache_hit_count = 0
    unified_cache_miss_count = 0
    local_cache_hit_count = 0
    local_cache_miss_count = 0
    dropped_unresolved_unified_id_count = 0
    dropped_missing_platform_metadata_count = 0
    dropped_ios_category_mismatch_count = 0
    dropped_android_category_mismatch_count = 0
    dropped_no_active_child_apps_count = 0

    def _snapshot_log(message: str) -> None:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"[snapshot] {timestamp} {message}", flush=True)

    def _ensure_unified_metadata(unified_ids: list[str]) -> None:
        nonlocal unified_cache_hit_count, unified_cache_miss_count
        missing_ids: list[str] = []
        for unified_id in list(dict.fromkeys(unified_ids)):
            cached = current_unified_lookup.get(unified_id)
            if cached is not None:
                run_unified_lookup[unified_id] = cached
                unified_cache_hit_count += 1
            else:
                missing_ids.append(unified_id)

        if not missing_ids:
            return

        unified_cache_miss_count += len(missing_ids)
        fetched_records, invalid_ids = _fetch_unified_apps_resilient(api_key, ctx.base_url, missing_ids)
        _upsert_lookup(current_unified_lookup, fetched_records, _unified_cache_key)
        _upsert_lookup(run_unified_lookup, fetched_records, _unified_cache_key)
        if invalid_ids:
            _snapshot_log(f"Unresolved unified ids in candidate scan: {len(invalid_ids)}")

    def _ensure_local_metadata(os_name: str, app_ids: list[str]) -> None:
        nonlocal local_cache_hit_count, local_cache_miss_count
        missing_ids: list[str] = []
        run_lookup = run_ios_lookup if os_name == "ios" else run_android_lookup
        for app_id in list(dict.fromkeys(app_ids)):
            key = (os_name, app_id)
            cached = current_local_lookup.get(key)
            if cached is not None:
                run_lookup[key] = cached
                local_cache_hit_count += 1
            else:
                missing_ids.append(app_id)

        if not missing_ids:
            return

        local_cache_miss_count += len(missing_ids)
        fetched_records = _fetch_local_app_records(api_key, ctx.base_url, os_name, missing_ids)
        _upsert_lookup(current_local_lookup, fetched_records, _local_cache_key)
        _upsert_lookup(run_lookup, fetched_records, _local_cache_key)

    def _flush_snapshot_caches() -> None:
        _write_lookup_ndjson(outputs["snapshot_unified_raw"], run_unified_lookup)
        _write_lookup_ndjson(outputs["snapshot_ios_local_raw"], run_ios_lookup)
        _write_lookup_ndjson(outputs["snapshot_android_local_raw"], run_android_lookup)
        _write_lookup_ndjson(state_outputs["snapshot_unified_raw_current"], current_unified_lookup)
        _write_lookup_ndjson(state_outputs["snapshot_local_raw_current"], current_local_lookup)

    _snapshot_log(
        "Starting top-game snapshot fetch "
        f"for market {settings['regions']} "
        f"for years {settings['start_year']}..{settings['end_year']} "
        f"at up to {max_pages_per_year} page(s) per year"
    )
    for year_index, year in enumerate(range(settings["start_year"], settings["end_year"] + 1), start=1):
        year_rows: list[dict[str, Any]] = []
        seen_year_keys: set[tuple[str, str]] = set()
        year_candidate_count = 0
        stop_reason = "api_exhausted"
        _snapshot_log(f"Year {year} started ({year_index}/{total_years})")
        for page_index, offset in enumerate(candidate_offsets, start=1):
            params = _top_apps_snapshot_request_params(api_key, settings, year=year, offset=offset)
            _snapshot_log(
                f"Requesting year={year}, page={page_index}/{max_pages_per_year}, "
                f"offset={offset}, limit={settings['limit']}"
            )
            request_started_at = time.time()
            response = lsdp._get_with_retry(
                endpoint,
                params=params,
                timeout=(10, 60),
                max_retries=6,
                base_sleep=1.5,
                max_sleep=60,
                progress_logger=_snapshot_log,
            )
            request_elapsed_seconds = time.time() - request_started_at
            request_count += 1
            pages_consumed += 1
            if response.status_code != 200:
                raise RuntimeError(
                    f"Top game snapshot API failed for year={year}, offset={offset}, "
                    f"status={response.status_code}, body={response.text[:500]}"
                )
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError(f"Unexpected Top Apps response format: {type(payload)}")
            data = payload.get("data", [])
            if not isinstance(data, list):
                raise RuntimeError(f"Unexpected top game snapshot response format: {type(data)}")
            meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
            total_count_value = meta.get("total_count")
            try:
                total_count = int(total_count_value) if total_count_value is not None else None
            except (TypeError, ValueError):
                total_count = None
            year_total_counts[str(year)] = total_count
            _snapshot_log(
                f"Received year={year}, page={page_index}/{max_pages_per_year}, rows={len(data)}, "
                f"status={response.status_code}, elapsed={request_elapsed_seconds:.1f}s"
            )
            if len(raw_response_samples) < SNAPSHOT_RAW_RESPONSE_SAMPLE_LIMIT:
                raw_response_samples.extend(data[: SNAPSHOT_RAW_RESPONSE_SAMPLE_LIMIT - len(raw_response_samples)])
            candidate_rows: list[dict[str, Any]] = []
            page_unified_ids: list[str] = []
            for row in data:
                normalized_row = dict(row)
                normalized_row["_snapshot_year"] = str(year)
                normalized_row["_normalized_unified_id"] = _normalize_id(row.get("unified_app_id"))
                candidate_rows.append(normalized_row)
                candidate_row_count += 1
                year_candidate_count += 1
                normalized_id = normalized_row["_normalized_unified_id"]
                if normalized_id:
                    page_unified_ids.append(normalized_id)

            _ensure_unified_metadata(page_unified_ids)

            ios_ids: list[str] = []
            android_ids: list[str] = []
            for unified_id in list(dict.fromkeys(page_unified_ids)):
                unified_record = current_unified_lookup.get(unified_id)
                if unified_record is None:
                    continue
                child_ids = _extract_child_app_ids(unified_record)
                ios_ids.extend(child_ids["ios"])
                android_ids.extend(child_ids["android"])

            _ensure_local_metadata("ios", ios_ids)
            _ensure_local_metadata("android", android_ids)
            _flush_snapshot_caches()

            for row in candidate_rows:
                unified_id = row.get("_normalized_unified_id")
                if not unified_id:
                    dropped_unresolved_unified_id_count += 1
                    continue
                unified_record = current_unified_lookup.get(unified_id)
                if unified_record is None:
                    dropped_unresolved_unified_id_count += 1
                    continue

                keep_row, verdict = _snapshot_candidate_verdict(
                    unified_record,
                    current_local_lookup,
                    child_app_scope=settings["child_app_scope"],
                )
                if not keep_row:
                    if verdict == "missing_platform_metadata":
                        dropped_missing_platform_metadata_count += 1
                    elif verdict == "ios_category_mismatch":
                        dropped_ios_category_mismatch_count += 1
                    elif verdict == "android_category_mismatch":
                        dropped_android_category_mismatch_count += 1
                    elif verdict == "no_active_child_apps":
                        dropped_no_active_child_apps_count += 1
                    continue

                dedupe_key = (unified_id, str(year))
                if dedupe_key in seen_year_keys:
                    continue
                seen_year_keys.add(dedupe_key)
                year_rows.append(row)
                if len(year_rows) >= settings["target_count"]:
                    stop_reason = "target_count_reached"
                    break

            if len(year_rows) >= settings["target_count"]:
                break
            if year_candidate_count >= effective_max_candidate_count:
                stop_reason = "max_candidate_count_reached"
                break
            if total_count is not None and offset + len(data) >= total_count:
                stop_reason = "api_exhausted"
                break
            if len(data) < settings["limit"]:
                _snapshot_log(
                    f"Year {year} ended early after page {page_index}/{max_pages_per_year} "
                    f"because rows returned ({len(data)}) were below limit"
                )
                stop_reason = "api_exhausted"
                break
        year_counts[str(year)] = len(year_rows)
        year_stop_reasons[str(year)] = stop_reason
        rows.extend(year_rows[: settings["target_count"]])
        _snapshot_log(f"Year {year} completed with {len(year_rows)} row(s)")

    _flush_snapshot_caches()

    metrics = {
        "years_requested": list(range(settings["start_year"], settings["end_year"] + 1)),
        "api_request_count": request_count,
        "requests_per_year": settings["target_count"] // settings["limit"],
        "year_row_counts": year_counts,
        "year_stop_reasons": year_stop_reasons,
        "year_total_counts": year_total_counts,
        "candidate_row_count": candidate_row_count,
        "kept_row_count": len(rows),
        "dropped_row_count": max(0, candidate_row_count - len(rows)),
        "pages_consumed": pages_consumed,
        "target_fulfilled": all(count >= settings["target_count"] for count in year_counts.values()),
        "stop_reason": "complete" if all(count >= settings["target_count"] for count in year_counts.values()) else "partial",
        "dropped_unresolved_unified_id_count": dropped_unresolved_unified_id_count,
        "dropped_missing_platform_metadata_count": dropped_missing_platform_metadata_count,
        "dropped_ios_category_mismatch_count": dropped_ios_category_mismatch_count,
        "dropped_android_category_mismatch_count": dropped_android_category_mismatch_count,
        "dropped_no_active_child_apps_count": dropped_no_active_child_apps_count,
        "unified_cache_hit_count": unified_cache_hit_count,
        "unified_cache_miss_count": unified_cache_miss_count,
        "local_cache_hit_count": local_cache_hit_count,
        "local_cache_miss_count": local_cache_miss_count,
        "raw_response_samples": raw_response_samples,
    }
    _snapshot_log(
        f"Snapshot fetch finished with {len(rows)} raw row(s) across {request_count} API request(s)"
    )
    return rows, metrics


def _build_snapshot_id_frame(raw_rows: list[dict[str, Any]]) -> pd.DataFrame:
    normalized_rows: list[dict[str, Any]] = []
    for row_index, row in enumerate(raw_rows):
        unified_id = _normalize_id(row.get("unified_app_id") or row.get("app_id"))
        if not unified_id:
            continue
        revenue_value = row.get("est_mobile_revenue")
        if revenue_value in (None, ""):
            revenue_value = row.get("revenue_absolute")
        revenue = None
        if revenue_value not in (None, ""):
            revenue = float(revenue_value) / 100.0
        normalized_rows.append(
            {
                "unified_id": unified_id,
                "year": str(row.get("_snapshot_year") or "").strip(),
                "market": str(row.get("_snapshot_market") or "").strip().upper() or None,
                "revenue": revenue,
                "_row_order": row_index,
            }
        )

    df = pd.DataFrame(normalized_rows, columns=["unified_id", "year", "market", "revenue", "_row_order"])
    if df.empty:
        return df

    df["unified_id"] = df["unified_id"].apply(_normalize_id)
    df["year"] = df["year"].astype(str).str.strip()
    df = df[df["unified_id"].notna()].copy()
    dedupe_subset = ["unified_id", "year"]
    if df["market"].notna().any():
        dedupe_subset = ["market", *dedupe_subset]
    df = df.drop_duplicates(subset=dedupe_subset, keep="first")
    sort_columns = [col for col in ("market", "year", "_row_order") if col in df.columns]
    df = df.sort_values(sort_columns).reset_index(drop=True)
    columns = ["unified_id", "year", "revenue"]
    if df["market"].notna().any():
        columns = ["market", *columns]
    return df.loc[:, columns]


def _parse_record_date(value: Any) -> date:
    if value is None:
        raise ValueError("Cannot parse record date from None.")
    text = str(value).strip()
    if len(text) >= 10:
        text = text[:10]
    return datetime.strptime(text, "%Y-%m-%d").date()


def _facts_window_for_mode(
    end_date_text: str,
    load_mode: str,
    current_exists: bool,
    start_date_text: Optional[str] = None,
) -> dict[str, Any]:
    normalized_mode = _normalize_load_mode(load_mode)

    end_date_value = _parse_record_date(end_date_text)
    if start_date_text:
        start_date_value = _parse_record_date(start_date_text)
        if start_date_value > end_date_value:
            raise ValueError(f"facts_start_date cannot be after performance_end_date: {start_date_text} > {end_date_text}")
        return {
            "effective_mode": BACKFILL_LOAD_MODE if normalized_mode == BACKFILL_LOAD_MODE else normalized_mode,
            "start_date": start_date_value.isoformat(),
            "end_date": end_date_text,
            "archive_enabled": normalized_mode == QUARTERLY_LOAD_MODE,
            "overlap_days": None,
        }

    if not current_exists or normalized_mode == FULL_LOAD_MODE:
        return {
            "effective_mode": FULL_LOAD_MODE,
            "start_date": DEFAULT_APP_PERFORMANCE_START_DATE,
            "end_date": end_date_text,
            "archive_enabled": False,
            "overlap_days": None,
        }

    if normalized_mode == QUARTERLY_LOAD_MODE:
        overlap_days = QUARTERLY_OVERLAP_DAYS
    elif normalized_mode == WEEKLY_LOAD_MODE:
        overlap_days = WEEKLY_OVERLAP_DAYS
    else:
        overlap_days = DAILY_OVERLAP_DAYS
    start_date_value = end_date_value - timedelta(days=overlap_days - 1)
    minimum_start = datetime.strptime(DEFAULT_APP_PERFORMANCE_START_DATE, "%Y-%m-%d").date()
    if start_date_value < minimum_start:
        start_date_value = minimum_start

    return {
        "effective_mode": normalized_mode,
        "start_date": start_date_value.isoformat(),
        "end_date": end_date_text,
        "archive_enabled": normalized_mode == QUARTERLY_LOAD_MODE,
        "overlap_days": overlap_days,
    }


def _normalize_load_mode(load_mode: Optional[str]) -> str:
    normalized_mode = (load_mode or DEFAULT_FACTS_LOAD_MODE).strip().lower()
    if normalized_mode not in {FULL_LOAD_MODE, DAILY_LOAD_MODE, WEEKLY_LOAD_MODE, QUARTERLY_LOAD_MODE, BACKFILL_LOAD_MODE}:
        raise ValueError(f"Unsupported facts load mode: {load_mode}")
    return normalized_mode


def _read_json_array_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise ValueError(f"Expected JSON array at {path}")
    return payload


def _read_ndjson_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _iter_ndjson_items(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _write_json_array_file(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(rows, handle, ensure_ascii=False)


def _write_csv_dataframe_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _temp_path_for_atomic_write(path)
    df.to_csv(tmp_path, index=False)
    _replace_file_atomically(tmp_path, path)


def _write_ndjson_file(path: Path, rows: list[dict[str, Any]], append: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    with open(path, mode, encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _write_ndjson_items_to_path(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


def _write_ndjson_items_atomic(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _temp_path_for_atomic_write(path)
    try:
        count = _write_ndjson_items_to_path(tmp_path, rows)
        _replace_file_atomically(tmp_path, path)
        return count
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def _coerce_revenue_multiplier(value: Any) -> Optional[float | int]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number.is_integer():
        return int(number)
    return number


def _load_revenue_factor_by_app(adjusted_app_full_info_path: Path) -> dict[str, float | int]:
    revenue_factor_by_app: dict[str, float | int] = {}
    for row in _iter_ndjson_items(adjusted_app_full_info_path):
        app_id = _normalize_id(row.get("app_id"))
        if app_id is None:
            continue
        factor = _coerce_revenue_multiplier(row.get("revenue_multiplier"))
        if factor is None or factor == 1:
            continue
        revenue_factor_by_app[app_id] = factor
    return revenue_factor_by_app


APP_PERFORMANCE_REVENUE_ADJUSTMENT_COUNTRIES = {"VN"}


def _adjust_app_performance_revenue_record(
    record: dict[str, Any],
    revenue_factor_by_app: dict[str, float | int],
) -> dict[str, Any]:
    country = str(record.get("cc") or record.get("c") or "").upper()
    if country not in APP_PERFORMANCE_REVENUE_ADJUSTMENT_COUNTRIES:
        return record

    aid = _normalize_id(record.get("aid"))
    if aid is None:
        return record
    factor = revenue_factor_by_app.get(aid)
    if factor is None or factor == 1:
        return record

    adjusted = dict(record)
    for revenue_field in ("ir", "ar", "r"):
        value = adjusted.get(revenue_field)
        if value is not None:
            adjusted[revenue_field] = value * factor
    return adjusted


def _stream_adjusted_app_performance_current_to_ndjson(
    *,
    current_store_path: Path,
    adjusted_app_full_info_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    manifest = _load_time_series_current_manifest(
        current_store_path,
        manifest_format=APP_PERFORMANCE_CURRENT_STORE_FORMAT,
        dataset_label="App performance",
    )
    revenue_factor_by_app = _load_revenue_factor_by_app(adjusted_app_full_info_path)

    def _rows() -> Iterable[dict[str, Any]]:
        for row in iter_app_performance_current_records(current_store_path):
            yield _adjust_app_performance_revenue_record(row, revenue_factor_by_app)

    records_written = _write_ndjson_items_atomic(output_path, _rows())
    return {
        "records_written": records_written,
        "file_size_bytes": _file_size(output_path),
        "input_store_path": str(current_store_path),
        "input_manifest_path": str(_time_series_current_manifest_path(current_store_path)),
        "source_manifest_row_count": int(manifest.get("row_count", 0)),
        "revenue_multiplier_app_count": len(revenue_factor_by_app),
        "revenue_adjustment_country_codes": sorted(APP_PERFORMANCE_REVENUE_ADJUSTMENT_COUNTRIES),
        "output_path": str(output_path),
    }


def _stream_time_series_current_to_ndjson(
    *,
    current_store_path: Path,
    output_path: Path,
    manifest_format: str,
    dataset_label: str,
) -> dict[str, Any]:
    manifest = _load_time_series_current_manifest(
        current_store_path,
        manifest_format=manifest_format,
        dataset_label=dataset_label,
    )

    records_written = _write_ndjson_items_atomic(
        output_path,
        iter_time_series_current_records(
            current_store_path,
            manifest_format=manifest_format,
            dataset_label=dataset_label,
        ),
    )
    return {
        "records_written": records_written,
        "file_size_bytes": _file_size(output_path),
        "input_store_path": str(current_store_path),
        "input_manifest_path": str(_time_series_current_manifest_path(current_store_path)),
        "source_manifest_row_count": int(manifest.get("row_count", 0)),
        "output_path": str(output_path),
    }


def _count_json_array_items(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with open(path, "rb") as handle:
        for _ in ijson.items(handle, "item"):
            count += 1
    return count


def _iter_json_array_items(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with open(path, "rb") as handle:
        for item in ijson.items(handle, "item"):
            yield item


def _temp_path_for_atomic_write(path: Path) -> Path:
    return path.with_name(f".{path.name}.{os.getpid()}.{int(time.time() * 1000)}.tmp")


def _temp_dir_for_atomic_write(path: Path) -> Path:
    return path.with_name(f".{path.name}.{os.getpid()}.{int(time.time() * 1000)}.tmpdir")


def _replace_file_atomically(src_path: Path, dst_path: Path) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(src_path, dst_path)


def _copy_file_to_current(current_path: Path, run_output_path: Path) -> None:
    current_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _temp_path_for_atomic_write(current_path)
    shutil.copyfile(run_output_path, tmp_path)
    _replace_file_atomically(tmp_path, current_path)


def _write_json_payload_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _temp_path_for_atomic_write(path)
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    _replace_file_atomically(tmp_path, path)


def _windows_path_from_wsl_mount(path: Path) -> Optional[str]:
    text = str(path)
    match = re.match(r"^/mnt/([a-zA-Z])/(.*)$", text)
    if not match:
        return None
    drive = match.group(1).upper()
    remainder = match.group(2).replace("/", "\\")
    return f"{drive}:\\{remainder}"


def _move_directory_with_wsl_fallback(src_path: Path, dst_path: Path) -> None:
    try:
        os.replace(src_path, dst_path)
        return
    except PermissionError:
        src_windows = _windows_path_from_wsl_mount(src_path)
        dst_windows = _windows_path_from_wsl_mount(dst_path)
        if not src_windows or not dst_windows or dst_path.exists():
            raise

        src_literal = src_windows.replace("'", "''")
        dst_literal = dst_windows.replace("'", "''")
        command = f"Move-Item -LiteralPath '{src_literal}' -Destination '{dst_literal}'"
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", command],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise PermissionError(
                f"Failed to move directory via powershell.exe: {src_path} -> {dst_path}\n{result.stderr.strip()}"
            )


def _time_series_current_legacy_json_path(current_store_path: Path) -> Path:
    return current_store_path.with_suffix(".json")


def _time_series_current_manifest_path(current_store_path: Path) -> Path:
    return current_store_path / "manifest.json"


def _time_series_partition_relative_path(partition_key: tuple[int, int]) -> Path:
    year, month = partition_key
    return Path("partitions") / f"year={year}" / f"month={month:02d}.ndjson"


def _time_series_partition_path(current_store_path: Path, partition_key: tuple[int, int]) -> Path:
    return current_store_path / _time_series_partition_relative_path(partition_key)


def _time_series_partition_key_from_date_text(date_text: str) -> tuple[int, int]:
    record_date = _parse_record_date(date_text)
    return (record_date.year, record_date.month)


def _month_keys_between(start_date_value: date, end_date_value: date) -> list[tuple[int, int]]:
    month_keys: list[tuple[int, int]] = []
    current_value = date(start_date_value.year, start_date_value.month, 1)
    end_value = date(end_date_value.year, end_date_value.month, 1)
    while current_value <= end_value:
        month_keys.append((current_value.year, current_value.month))
        if current_value.month == 12:
            current_value = date(current_value.year + 1, 1, 1)
        else:
            current_value = date(current_value.year, current_value.month + 1, 1)
    return month_keys


def _time_series_manifest_from_partition_stats(
    partition_stats: dict[tuple[int, int], dict[str, Any]],
    *,
    manifest_format: str,
) -> dict[str, Any]:
    sorted_partition_keys = sorted(partition_stats)
    partitions = []
    row_count = 0
    min_date_text = None
    max_date_text = None
    for partition_key in sorted_partition_keys:
        stats = partition_stats[partition_key]
        if stats["row_count"] <= 0:
            continue
        year, month = partition_key
        row_count += int(stats["row_count"])
        part_min = stats.get("min_date")
        part_max = stats.get("max_date")
        if part_min and (min_date_text is None or part_min < min_date_text):
            min_date_text = part_min
        if part_max and (max_date_text is None or part_max > max_date_text):
            max_date_text = part_max
        partitions.append(
            {
                "year": year,
                "month": month,
                "path": str(_time_series_partition_relative_path(partition_key)).replace("\\", "/"),
                "row_count": int(stats["row_count"]),
                "min_date": part_min,
                "max_date": part_max,
            }
        )
    return {
        "format": manifest_format,
        "row_count": row_count,
        "min_date": min_date_text,
        "max_date": max_date_text,
        "updated_at": _utc_now_iso(),
        "partitions": partitions,
    }


def _load_time_series_current_manifest(
    current_store_path: Path,
    *,
    manifest_format: str,
    dataset_label: str,
) -> dict[str, Any]:
    manifest_path = _time_series_current_manifest_path(current_store_path)
    if not manifest_path.exists():
        raise FileNotFoundError(f"{dataset_label} current manifest does not exist: {manifest_path}")
    manifest = _read_json_payload(manifest_path)
    if manifest.get("format") != manifest_format:
        raise ValueError(
            f"Unsupported {dataset_label} current manifest format: {manifest.get('format')}"
        )
    return manifest


def _time_series_current_store_exists(current_store_path: Path) -> bool:
    return _time_series_current_manifest_path(current_store_path).exists()


def _time_series_current_available(current_store_path: Path) -> bool:
    return _time_series_current_store_exists(current_store_path) or _time_series_current_legacy_json_path(
        current_store_path
    ).exists()


def iter_time_series_current_records(
    current_store_path: Path,
    *,
    manifest_format: str,
    dataset_label: str,
    legacy_json_path: Optional[Path] = None,
) -> Iterable[dict[str, Any]]:
    if _time_series_current_store_exists(current_store_path):
        manifest = _load_time_series_current_manifest(
            current_store_path,
            manifest_format=manifest_format,
            dataset_label=dataset_label,
        )
        for partition in manifest.get("partitions", []):
            yield from _iter_ndjson_items(current_store_path / partition["path"])
        return
    fallback_path = legacy_json_path or _time_series_current_legacy_json_path(current_store_path)
    if fallback_path.exists():
        yield from _iter_json_array_items(fallback_path)


def _build_time_series_current_store_from_json_array(
    *,
    src_path: Path,
    destination_store_path: Path,
    record_date_field: str,
    manifest_format: str,
    dataset_label: str,
) -> dict[str, Any]:
    if not src_path.exists():
        raise FileNotFoundError(f"{dataset_label} source artifact does not exist: {src_path}")

    temp_store_path = _temp_dir_for_atomic_write(destination_store_path)
    temp_store_path.mkdir(parents=True, exist_ok=False)
    writers: dict[tuple[int, int], Any] = {}
    partition_stats: dict[tuple[int, int], dict[str, Any]] = {}

    try:
        with open(src_path, "rb") as src_handle:
            for row in ijson.items(src_handle, "item"):
                date_text = str(row.get(record_date_field) or "")
                partition_key = _time_series_partition_key_from_date_text(date_text)
                partition_path = _time_series_partition_path(temp_store_path, partition_key)
                writer = writers.get(partition_key)
                if writer is None:
                    partition_path.parent.mkdir(parents=True, exist_ok=True)
                    writer = open(partition_path, "w", encoding="utf-8")
                    writers[partition_key] = writer

                writer.write(json.dumps(row, ensure_ascii=False) + "\n")
                partition_stat = partition_stats.setdefault(
                    partition_key,
                    {"row_count": 0, "min_date": date_text, "max_date": date_text},
                )
                partition_stat["row_count"] += 1
                if date_text < partition_stat["min_date"]:
                    partition_stat["min_date"] = date_text
                if date_text > partition_stat["max_date"]:
                    partition_stat["max_date"] = date_text
    finally:
        for writer in writers.values():
            writer.close()

    manifest = _time_series_manifest_from_partition_stats(
        partition_stats,
        manifest_format=manifest_format,
    )
    _write_json_payload_atomic(_time_series_current_manifest_path(temp_store_path), manifest)
    return {
        "temp_store_path": temp_store_path,
        "manifest": manifest,
    }


def _install_time_series_current_store(
    *,
    current_store_path: Path,
    temp_store_path: Path,
) -> Optional[Path]:
    current_store_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path = None
    if current_store_path.exists():
        backup_path = current_store_path.with_name(
            f".{current_store_path.name}.bak.{int(time.time() * 1000)}"
        )
        _move_directory_with_wsl_fallback(current_store_path, backup_path)
    try:
        _move_directory_with_wsl_fallback(temp_store_path, current_store_path)
    except Exception:
        if backup_path is not None and backup_path.exists() and not current_store_path.exists():
            _move_directory_with_wsl_fallback(backup_path, current_store_path)
        raise
    return backup_path


def _ensure_time_series_current_store_initialized(
    current_store_path: Path,
    *,
    record_date_field: str,
    manifest_format: str,
    dataset_label: str,
) -> Optional[dict[str, Any]]:
    if _time_series_current_store_exists(current_store_path):
        return _load_time_series_current_manifest(
            current_store_path,
            manifest_format=manifest_format,
            dataset_label=dataset_label,
        )

    legacy_json_path = _time_series_current_legacy_json_path(current_store_path)
    if current_store_path.exists():
        raise ValueError(
            f"{dataset_label} current store exists without a manifest; refusing to guess structure: {current_store_path}"
        )
    if not legacy_json_path.exists():
        return None

    build_result = _build_time_series_current_store_from_json_array(
        src_path=legacy_json_path,
        destination_store_path=current_store_path,
        record_date_field=record_date_field,
        manifest_format=manifest_format,
        dataset_label=dataset_label,
    )
    _install_time_series_current_store(
        current_store_path=current_store_path,
        temp_store_path=build_result["temp_store_path"],
    )
    return build_result["manifest"]


def _app_performance_current_legacy_json_path(current_store_path: Path) -> Path:
    return _time_series_current_legacy_json_path(current_store_path)


def _app_performance_current_manifest_path(current_store_path: Path) -> Path:
    return _time_series_current_manifest_path(current_store_path)


def _app_performance_current_available(current_store_path: Path) -> bool:
    return _time_series_current_available(current_store_path)


def iter_app_performance_current_records(
    current_store_path: Path,
    *,
    legacy_json_path: Optional[Path] = None,
) -> Iterable[dict[str, Any]]:
    yield from iter_time_series_current_records(
        current_store_path,
        manifest_format=APP_PERFORMANCE_CURRENT_STORE_FORMAT,
        dataset_label="App performance",
        legacy_json_path=legacy_json_path,
    )


def _write_json_array_items_to_path(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("[")
        first = True
        for row in rows:
            if not first:
                handle.write(",")
            first = False
            json.dump(row, handle, ensure_ascii=False)
            count += 1
        handle.write("]")
    return count


def _merge_overlap_records(
    *,
    current_records: list[dict[str, Any]],
    new_records: list[dict[str, Any]],
    record_date_field: str,
    key_builder: Callable[[dict[str, Any]], tuple[str, str, str]],
    overlap_start: str,
    overlap_end: str,
    archive_enabled: bool,
    archive_dataset: str,
    expired_at: str,
) -> dict[str, Any]:
    start_date_value = _parse_record_date(overlap_start)
    end_date_value = _parse_record_date(overlap_end)

    current_non_overlap: list[dict[str, Any]] = []
    current_overlap_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in current_records:
        row_date = _parse_record_date(row.get(record_date_field))
        if start_date_value <= row_date <= end_date_value:
            current_overlap_by_key[key_builder(row)] = row
        else:
            current_non_overlap.append(row)

    new_overlap_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in new_records:
        new_overlap_by_key[key_builder(row)] = row

    archive_rows: list[dict[str, Any]] = []
    unchanged_count = 0
    replaced_count = 0
    inserted_count = 0

    for key, new_row in new_overlap_by_key.items():
        old_row = current_overlap_by_key.pop(key, None)
        if old_row is None:
            inserted_count += 1
            continue
        if old_row == new_row:
            unchanged_count += 1
        else:
            replaced_count += 1
            if archive_enabled:
                archived_row = dict(old_row)
                archived_row["_history_dataset"] = archive_dataset
                archived_row["_expired_at"] = expired_at
                archived_row["_history_reason"] = "quarterly_reconciliation"
                archive_rows.append(archived_row)

    if archive_enabled:
        for old_row in current_overlap_by_key.values():
            archived_row = dict(old_row)
            archived_row["_history_dataset"] = archive_dataset
            archived_row["_expired_at"] = expired_at
            archived_row["_history_reason"] = "quarterly_reconciliation"
            archive_rows.append(archived_row)

    merged_records = current_non_overlap + list(new_overlap_by_key.values())
    merged_records.sort(key=lambda row: (_parse_record_date(row.get(record_date_field)), key_builder(row)))

    return {
        "merged_records": merged_records,
        "archive_rows": archive_rows,
        "unchanged_overlap_record_count": unchanged_count,
        "replaced_overlap_record_count": replaced_count,
        "inserted_overlap_record_count": inserted_count,
        "dropped_overlap_record_count": len(current_overlap_by_key),
    }


def _merge_json_dataset_into_current(
    *,
    current_path: Path,
    run_output_path: Path,
    history_archive_path: Optional[Path],
    key_builder: Callable[[dict[str, Any]], tuple[str, str, str]],
    record_date_field: str,
    window: dict[str, Any],
    dataset_name: str,
    expired_at: str,
) -> dict[str, Any]:
    if window["effective_mode"] == FULL_LOAD_MODE or not current_path.exists():
        promoted_count = _count_json_array_items(run_output_path)
        _copy_file_to_current(current_path, run_output_path)
        return {
            "current_output_path": str(current_path),
            "history_archive_path": str(history_archive_path) if history_archive_path else None,
            "current_row_count": promoted_count,
            "overlap_start_date": window["start_date"],
            "overlap_end_date": window["end_date"],
            "load_mode": window["effective_mode"],
            "archived_row_count": 0,
            "replaced_overlap_record_count": 0,
            "unchanged_overlap_record_count": 0,
            "inserted_overlap_record_count": promoted_count,
            "dropped_overlap_record_count": 0,
        }

    overlap_start = _parse_record_date(window["start_date"])
    overlap_end = _parse_record_date(window["end_date"])
    archive_enabled = window["archive_enabled"]
    new_overlap_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in _iter_json_array_items(run_output_path):
        new_overlap_by_key[key_builder(row)] = row

    archive_rows: list[dict[str, Any]] = []
    unchanged_count = 0
    replaced_count = 0
    inserted_count = 0
    dropped_count = 0

    tmp_path = _temp_path_for_atomic_write(current_path)
    rows_written = 0

    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write("[")
        first = True

        def write_row(row: dict[str, Any]) -> None:
            nonlocal first, rows_written
            if not first:
                handle.write(",")
            first = False
            json.dump(row, handle, ensure_ascii=False)
            rows_written += 1

        for current_row in _iter_json_array_items(current_path):
            row_date = _parse_record_date(current_row.get(record_date_field))
            if row_date < overlap_start or row_date > overlap_end:
                write_row(current_row)
                continue

            key = key_builder(current_row)
            new_row = new_overlap_by_key.pop(key, None)
            if new_row is None:
                dropped_count += 1
                if archive_enabled:
                    archived_row = dict(current_row)
                    archived_row["_history_dataset"] = dataset_name
                    archived_row["_expired_at"] = expired_at
                    archived_row["_history_reason"] = "quarterly_reconciliation"
                    archive_rows.append(archived_row)
                continue

            if current_row == new_row:
                unchanged_count += 1
            else:
                replaced_count += 1
                if archive_enabled:
                    archived_row = dict(current_row)
                    archived_row["_history_dataset"] = dataset_name
                    archived_row["_expired_at"] = expired_at
                    archived_row["_history_reason"] = "quarterly_reconciliation"
                    archive_rows.append(archived_row)
            write_row(new_row)

        for key, row in sorted(
            new_overlap_by_key.items(),
            key=lambda item: (_parse_record_date(item[1].get(record_date_field)), item[0]),
        ):
            inserted_count += 1
            write_row(row)

        handle.write("]")

    _replace_file_atomically(tmp_path, current_path)
    if archive_enabled and history_archive_path and archive_rows:
        _write_ndjson_file(history_archive_path, archive_rows, append=True)

    return {
        "current_output_path": str(current_path),
        "history_archive_path": str(history_archive_path) if history_archive_path else None,
        "current_row_count": rows_written,
        "overlap_start_date": window["start_date"],
        "overlap_end_date": window["end_date"],
        "load_mode": window["effective_mode"],
        "archived_row_count": len(archive_rows),
        "replaced_overlap_record_count": replaced_count,
        "unchanged_overlap_record_count": unchanged_count,
        "inserted_overlap_record_count": inserted_count,
        "dropped_overlap_record_count": dropped_count,
    }


def _sync_ndjson_dataset_to_current(current_path: Path, run_output_path: Path) -> dict[str, Any]:
    _copy_file_to_current(current_path, run_output_path)
    return {
        "current_output_path": str(current_path),
        "current_row_count": _count_ndjson_lines(run_output_path),
    }


def _merge_ndjson_dataset_into_current(
    *,
    current_path: Path,
    run_output_path: Path,
    history_archive_path: Optional[Path],
    key_builder: Callable[[dict[str, Any]], tuple[str, str, str]],
    record_date_field: str,
    window: dict[str, Any],
    dataset_name: str,
    expired_at: str,
) -> dict[str, Any]:
    if window["effective_mode"] == FULL_LOAD_MODE or not current_path.exists():
        promoted_count = _count_ndjson_lines(run_output_path)
        _copy_file_to_current(current_path, run_output_path)
        return {
            "current_output_path": str(current_path),
            "history_archive_path": str(history_archive_path) if history_archive_path else None,
            "current_row_count": promoted_count,
            "overlap_start_date": window["start_date"],
            "overlap_end_date": window["end_date"],
            "load_mode": window["effective_mode"],
            "archived_row_count": 0,
            "replaced_overlap_record_count": 0,
            "unchanged_overlap_record_count": 0,
            "inserted_overlap_record_count": promoted_count,
            "dropped_overlap_record_count": 0,
        }

    new_records = _read_ndjson_file(run_output_path)
    current_records = _read_ndjson_file(current_path)
    merge_result = _merge_overlap_records(
        current_records=current_records,
        new_records=new_records,
        record_date_field=record_date_field,
        key_builder=key_builder,
        overlap_start=window["start_date"],
        overlap_end=window["end_date"],
        archive_enabled=window["archive_enabled"],
        archive_dataset=dataset_name,
        expired_at=expired_at,
    )
    tmp_path = _temp_path_for_atomic_write(current_path)
    _write_ndjson_items_to_path(tmp_path, merge_result["merged_records"])
    _replace_file_atomically(tmp_path, current_path)
    if window["archive_enabled"] and history_archive_path and merge_result["archive_rows"]:
        _write_ndjson_file(history_archive_path, merge_result["archive_rows"], append=True)

    return {
        "current_output_path": str(current_path),
        "history_archive_path": str(history_archive_path) if history_archive_path else None,
        "current_row_count": len(merge_result["merged_records"]),
        "overlap_start_date": window["start_date"],
        "overlap_end_date": window["end_date"],
        "load_mode": window["effective_mode"],
        "archived_row_count": len(merge_result["archive_rows"]),
        "replaced_overlap_record_count": merge_result["replaced_overlap_record_count"],
        "unchanged_overlap_record_count": merge_result["unchanged_overlap_record_count"],
        "inserted_overlap_record_count": merge_result["inserted_overlap_record_count"],
        "dropped_overlap_record_count": merge_result["dropped_overlap_record_count"],
    }


def _merge_snapshot_ndjson_into_current(
    *,
    current_path: Path,
    run_output_path: Path,
    history_archive_path: Optional[Path],
    key_builder: Callable[[dict[str, Any]], Any],
    load_mode: str,
    dataset_name: str,
    expired_at: str,
) -> dict[str, Any]:
    normalized_mode = _normalize_load_mode(load_mode)

    if normalized_mode == FULL_LOAD_MODE or not current_path.exists():
        promoted_count = _count_ndjson_lines(run_output_path)
        _copy_file_to_current(current_path, run_output_path)
        return {
            "current_output_path": str(current_path),
            "history_archive_path": str(history_archive_path) if history_archive_path else None,
            "current_row_count": promoted_count,
            "load_mode": FULL_LOAD_MODE,
            "archived_row_count": 0,
            "replaced_snapshot_record_count": 0,
            "unchanged_snapshot_record_count": 0,
            "inserted_snapshot_record_count": promoted_count,
            "dropped_snapshot_record_count": 0,
        }

    new_records = _read_ndjson_file(run_output_path)
    current_lookup = _load_ndjson_lookup(current_path, key_builder)
    new_lookup: dict[Any, dict[str, Any]] = {}
    for row in new_records:
        key = key_builder(row)
        if key is None:
            continue
        new_lookup[key] = row

    archive_enabled = normalized_mode == QUARTERLY_LOAD_MODE
    archive_rows: list[dict[str, Any]] = []
    unchanged_count = 0
    replaced_count = 0
    inserted_count = 0

    for key, new_row in new_lookup.items():
        old_row = current_lookup.pop(key, None)
        if old_row is None:
            inserted_count += 1
            continue
        if old_row == new_row:
            unchanged_count += 1
        else:
            replaced_count += 1
            if archive_enabled:
                archived_row = dict(old_row)
                archived_row["_history_dataset"] = dataset_name
                archived_row["_expired_at"] = expired_at
                archived_row["_history_reason"] = "quarterly_reconciliation"
                archive_rows.append(archived_row)

    dropped_count = len(current_lookup)
    if archive_enabled:
        for old_row in current_lookup.values():
            archived_row = dict(old_row)
            archived_row["_history_dataset"] = dataset_name
            archived_row["_expired_at"] = expired_at
            archived_row["_history_reason"] = "quarterly_reconciliation"
            archive_rows.append(archived_row)

    tmp_path = _temp_path_for_atomic_write(current_path)
    _write_ndjson_items_to_path(tmp_path, new_lookup.values())
    _replace_file_atomically(tmp_path, current_path)
    if archive_enabled and history_archive_path and archive_rows:
        _write_ndjson_file(history_archive_path, archive_rows, append=True)

    return {
        "current_output_path": str(current_path),
        "history_archive_path": str(history_archive_path) if history_archive_path else None,
        "current_row_count": len(new_lookup),
        "load_mode": normalized_mode,
        "archived_row_count": len(archive_rows),
        "replaced_snapshot_record_count": replaced_count,
        "unchanged_snapshot_record_count": unchanged_count,
        "inserted_snapshot_record_count": inserted_count,
        "dropped_snapshot_record_count": dropped_count,
    }


def _merge_partitioned_json_dataset_into_current_streaming(
    *,
    current_path: Path,
    run_output_path: Path,
    history_archive_path: Optional[Path],
    key_builder: Callable[[dict[str, Any]], tuple[str, str, str]],
    record_date_field: str,
    window: dict[str, Any],
    dataset_name: str,
    expired_at: str,
    manifest_format: str,
    dataset_label: str,
    row_merge_fn: Optional[Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]] = None,
    drop_missing_in_window: bool = True,
) -> dict[str, Any]:
    legacy_json_path = _time_series_current_legacy_json_path(current_path)
    current_available = _time_series_current_available(current_path)
    if window["effective_mode"] == FULL_LOAD_MODE or not current_available:
        build_result = _build_time_series_current_store_from_json_array(
            src_path=run_output_path,
            destination_store_path=current_path,
            record_date_field=record_date_field,
            manifest_format=manifest_format,
            dataset_label=dataset_label,
        )
        backup_path = _install_time_series_current_store(
            current_store_path=current_path,
            temp_store_path=build_result["temp_store_path"],
        )
        manifest = build_result["manifest"]
        return {
            "current_output_path": str(current_path),
            "current_manifest_path": str(_time_series_current_manifest_path(current_path)),
            "legacy_current_json_path": str(legacy_json_path),
            "history_archive_path": str(history_archive_path) if history_archive_path else None,
            "current_row_count": int(manifest["row_count"]),
            "overlap_start_date": window["start_date"],
            "overlap_end_date": window["end_date"],
            "load_mode": window["effective_mode"],
            "archived_row_count": 0,
            "replaced_overlap_record_count": 0,
            "unchanged_overlap_record_count": 0,
            "inserted_overlap_record_count": int(manifest["row_count"]),
            "dropped_overlap_record_count": 0,
            "backup_store_path": str(backup_path) if backup_path else None,
        }

    manifest = _ensure_time_series_current_store_initialized(
        current_path,
        record_date_field=record_date_field,
        manifest_format=manifest_format,
        dataset_label=dataset_label,
    )
    if manifest is None:
        raise FileNotFoundError(f"Unable to initialize {dataset_label.lower()} current store at {current_path}")

    overlap_start = _parse_record_date(window["start_date"])
    overlap_end = _parse_record_date(window["end_date"])
    archive_enabled = window["archive_enabled"]
    archive_rows: list[dict[str, Any]] = []
    unchanged_count = 0
    replaced_count = 0
    inserted_count = 0
    dropped_count = 0

    manifest_entries = {
        (int(partition["year"]), int(partition["month"])): dict(partition)
        for partition in manifest.get("partitions", [])
    }
    next_partition_stats: dict[tuple[int, int], dict[str, Any]] = {
        partition_key: {
            "row_count": int(partition["row_count"]),
            "min_date": partition.get("min_date"),
            "max_date": partition.get("max_date"),
        }
        for partition_key, partition in manifest_entries.items()
    }

    spool_prefix = f".{run_output_path.stem}_{dataset_name}_merge_"
    for stale_spool_dir in run_output_path.parent.glob(f"{spool_prefix}*"):
        if stale_spool_dir.is_dir():
            shutil.rmtree(stale_spool_dir, ignore_errors=True)
    spool_dir = Path(tempfile.mkdtemp(prefix=spool_prefix, dir=str(run_output_path.parent)))
    spooled_partition_keys: set[tuple[int, int]] = set()
    spool_handles: dict[tuple[int, int], Any] = {}
    try:
        for row in _iter_json_array_items(run_output_path):
            date_text = str(row.get(record_date_field) or "")
            partition_key = _time_series_partition_key_from_date_text(date_text)
            spooled_partition_keys.add(partition_key)
            handle = spool_handles.get(partition_key)
            if handle is None:
                handle = open(spool_dir / f"{partition_key[0]:04d}-{partition_key[1]:02d}.ndjson", "a", encoding="utf-8")
                spool_handles[partition_key] = handle
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    finally:
        for handle in spool_handles.values():
            handle.close()

    touched_partition_keys = set(_month_keys_between(overlap_start, overlap_end))
    touched_partition_keys.update(spooled_partition_keys)

    for partition_key in sorted(touched_partition_keys):
        partition_new_rows: dict[tuple[str, str, str], dict[str, Any]] = {}
        spool_path = spool_dir / f"{partition_key[0]:04d}-{partition_key[1]:02d}.ndjson"
        if spool_path.exists():
            for row in _iter_ndjson_items(spool_path):
                partition_new_rows[key_builder(row)] = row
        existing_path = _time_series_partition_path(current_path, partition_key)
        partition_new_keys = set(partition_new_rows)
        consumed_partition_new_keys: set[tuple[str, str, str]] = set()
        if not existing_path.exists() and not partition_new_rows:
            continue

        tmp_path = _temp_path_for_atomic_write(existing_path)
        rows_written = 0
        partition_min_date = None
        partition_max_date = None

        with open(tmp_path, "w", encoding="utf-8") as handle:

            def write_row(row: dict[str, Any]) -> None:
                nonlocal rows_written, partition_min_date, partition_max_date
                date_text = str(row.get(record_date_field) or "")
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                rows_written += 1
                if partition_min_date is None or date_text < partition_min_date:
                    partition_min_date = date_text
                if partition_max_date is None or date_text > partition_max_date:
                    partition_max_date = date_text

            if existing_path.exists():
                for current_row in _iter_ndjson_items(existing_path):
                    row_date = _parse_record_date(str(current_row.get(record_date_field) or ""))
                    key = key_builder(current_row)

                    if key in partition_new_keys:
                        if key in consumed_partition_new_keys:
                            dropped_count += 1
                            if archive_enabled:
                                archived_row = dict(current_row)
                                archived_row["_history_dataset"] = dataset_name
                                archived_row["_expired_at"] = expired_at
                                archived_row["_history_reason"] = "quarterly_reconciliation"
                                archive_rows.append(archived_row)
                            continue

                        new_row = partition_new_rows.pop(key)
                        merged_row = row_merge_fn(current_row, new_row) if row_merge_fn else new_row
                        consumed_partition_new_keys.add(key)
                        if current_row == merged_row:
                            unchanged_count += 1
                        else:
                            replaced_count += 1
                            if archive_enabled:
                                archived_row = dict(current_row)
                                archived_row["_history_dataset"] = dataset_name
                                archived_row["_expired_at"] = expired_at
                                archived_row["_history_reason"] = "quarterly_reconciliation"
                                archive_rows.append(archived_row)
                        write_row(merged_row)
                        continue

                    if row_date < overlap_start or row_date > overlap_end:
                        write_row(current_row)
                        continue

                    if not drop_missing_in_window:
                        write_row(current_row)
                        continue

                    dropped_count += 1
                    if archive_enabled:
                        archived_row = dict(current_row)
                        archived_row["_history_dataset"] = dataset_name
                        archived_row["_expired_at"] = expired_at
                        archived_row["_history_reason"] = "quarterly_reconciliation"
                        archive_rows.append(archived_row)
                    continue

            for key, row in sorted(
                partition_new_rows.items(),
                key=lambda item: (_parse_record_date(str(item[1].get(record_date_field) or "")), item[0]),
            ):
                inserted_count += 1
                write_row(row)

        if rows_written > 0:
            _replace_file_atomically(tmp_path, existing_path)
            next_partition_stats[partition_key] = {
                "row_count": rows_written,
                "min_date": partition_min_date,
                "max_date": partition_max_date,
            }
        else:
            if tmp_path.exists():
                tmp_path.unlink()
            if existing_path.exists():
                existing_path.unlink()
            next_partition_stats.pop(partition_key, None)

        if spool_path.exists():
            spool_path.unlink()

    shutil.rmtree(spool_dir, ignore_errors=True)

    next_manifest = _time_series_manifest_from_partition_stats(
        next_partition_stats,
        manifest_format=manifest_format,
    )
    _write_json_payload_atomic(_time_series_current_manifest_path(current_path), next_manifest)

    if archive_enabled and history_archive_path and archive_rows:
        _write_ndjson_file(history_archive_path, archive_rows, append=True)

    return {
        "current_output_path": str(current_path),
        "current_manifest_path": str(_time_series_current_manifest_path(current_path)),
        "legacy_current_json_path": str(legacy_json_path),
        "history_archive_path": str(history_archive_path) if history_archive_path else None,
        "current_row_count": int(next_manifest["row_count"]),
        "overlap_start_date": window["start_date"],
        "overlap_end_date": window["end_date"],
        "load_mode": window["effective_mode"],
        "archived_row_count": len(archive_rows),
        "replaced_overlap_record_count": replaced_count,
        "unchanged_overlap_record_count": unchanged_count,
        "inserted_overlap_record_count": inserted_count,
        "dropped_overlap_record_count": dropped_count,
        "touched_partition_count": len(touched_partition_keys),
    }


def _merge_app_performance_into_current_streaming(
    *,
    current_path: Path,
    run_output_path: Path,
    history_archive_path: Optional[Path],
    key_builder: Callable[[dict[str, Any]], tuple[str, str, str]],
    record_date_field: str,
    window: dict[str, Any],
    dataset_name: str,
    expired_at: str,
    drop_missing_in_window: bool = True,
) -> dict[str, Any]:
    return _merge_partitioned_json_dataset_into_current_streaming(
        current_path=current_path,
        run_output_path=run_output_path,
        history_archive_path=history_archive_path,
        key_builder=key_builder,
        record_date_field=record_date_field,
        window=window,
        dataset_name=dataset_name,
        expired_at=expired_at,
        manifest_format=APP_PERFORMANCE_CURRENT_STORE_FORMAT,
        dataset_label="App performance",
        drop_missing_in_window=drop_missing_in_window,
    )


def _app_active_users_current_legacy_json_path(current_store_path: Path) -> Path:
    return _time_series_current_legacy_json_path(current_store_path)


def _app_active_users_current_manifest_path(current_store_path: Path) -> Path:
    return _time_series_current_manifest_path(current_store_path)


def _app_active_users_current_available(current_store_path: Path) -> bool:
    return _time_series_current_available(current_store_path)


APP_ACTIVE_USERS_METRIC_FIELDS = (
    "dau_android",
    "dau_iphone",
    "dau_ipad",
    "wau_android",
    "wau_iphone",
    "wau_ipad",
    "mau_android",
    "mau_iphone",
    "mau_ipad",
)


def _merge_app_active_users_metric_row(
    current_row: dict[str, Any],
    new_row: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(current_row)
    metric_fields = set(APP_ACTIVE_USERS_METRIC_FIELDS)

    for key, value in new_row.items():
        if key not in metric_fields:
            merged[key] = value

    for field in APP_ACTIVE_USERS_METRIC_FIELDS:
        if field in new_row and new_row[field] is not None:
            merged[field] = new_row[field]
        elif field not in merged:
            merged[field] = None

    return merged


def _merge_app_active_users_into_current_streaming(
    *,
    current_path: Path,
    run_output_path: Path,
    history_archive_path: Optional[Path],
    key_builder: Callable[[dict[str, Any]], tuple[str, str, str]],
    record_date_field: str,
    window: dict[str, Any],
    dataset_name: str,
    expired_at: str,
    drop_missing_in_window: bool = True,
) -> dict[str, Any]:
    return _merge_partitioned_json_dataset_into_current_streaming(
        current_path=current_path,
        run_output_path=run_output_path,
        history_archive_path=history_archive_path,
        key_builder=key_builder,
        record_date_field=record_date_field,
        window=window,
        dataset_name=dataset_name,
        expired_at=expired_at,
        manifest_format=APP_ACTIVE_USERS_CURRENT_STORE_FORMAT,
        dataset_label="App active users",
        row_merge_fn=_merge_app_active_users_metric_row,
        drop_missing_in_window=drop_missing_in_window,
    )


def promote_source_base_current_artifact(dataset: str, run_timestamp: int, output_root: Path) -> dict[str, Any]:
    outputs = _source_base_outputs(output_root, run_timestamp)
    state_outputs = _source_base_state_outputs(output_root)
    partitioned_json_dataset_map: dict[str, tuple[Path, Path, str, str]] = {
        "app_performance": (
            outputs["app_performance"],
            state_outputs["app_performance_current"],
            "d",
            APP_PERFORMANCE_CURRENT_STORE_FORMAT,
        ),
        "app_active_users": (
            outputs["app_active_users"],
            state_outputs["app_active_users_current"],
            "date",
            APP_ACTIVE_USERS_CURRENT_STORE_FORMAT,
        ),
    }
    if dataset in partitioned_json_dataset_map:
        source_path, current_path, record_date_field, manifest_format = partitioned_json_dataset_map[dataset]
        if not source_path.exists():
            raise FileNotFoundError(f"Source artifact does not exist: {source_path}")
        build_result = _build_time_series_current_store_from_json_array(
            src_path=source_path,
            destination_store_path=current_path,
            record_date_field=record_date_field,
            manifest_format=manifest_format,
            dataset_label=dataset.replace("_", " "),
        )
        backup_path = _install_time_series_current_store(
            current_store_path=current_path,
            temp_store_path=build_result["temp_store_path"],
        )
        manifest = build_result["manifest"]
        return {
            "status": "success",
            "dataset": dataset,
            "run_timestamp": run_timestamp,
            "source_artifact": str(source_path),
            "current_output": str(current_path),
            "current_manifest_path": str(_time_series_current_manifest_path(current_path)),
            "legacy_current_json_path": str(_time_series_current_legacy_json_path(current_path)),
            "row_count": int(manifest["row_count"]),
            "file_size_bytes": _file_size(source_path),
            "backup_store_path": str(backup_path) if backup_path else None,
        }

    dataset_map: dict[str, tuple[Path, Path, str]] = {
        "game_full_info": (outputs["game_full_info"], state_outputs["game_full_info_current"], "csv"),
        "app_full_info": (outputs["app_full_info"], state_outputs["app_full_info_current"], "ndjson"),
        "app_retention": (outputs["app_retention"], state_outputs["app_retention_current"], "ndjson"),
        "user_demographics": (outputs["user_demographics"], state_outputs["user_demographics_current"], "ndjson"),
    }
    if dataset not in dataset_map:
        raise ValueError(f"Unsupported dataset for promotion: {dataset}")

    source_path, current_path, file_format = dataset_map[dataset]
    if not source_path.exists():
        raise FileNotFoundError(f"Source artifact does not exist: {source_path}")

    if file_format == "json":
        row_count = _count_json_array_items(source_path)
    elif file_format == "csv":
        row_count = _count_csv_rows(source_path)
    else:
        row_count = _count_ndjson_lines(source_path)

    _copy_file_to_current(current_path, source_path)
    return {
        "status": "success",
        "dataset": dataset,
        "run_timestamp": run_timestamp,
        "source_artifact": str(source_path),
        "current_output": str(current_path),
        "row_count": row_count,
        "file_size_bytes": _file_size(current_path),
    }


def _write_json_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _read_json_payload(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _records_to_dataframe(records: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=columns)
    df = pd.DataFrame(records)
    for column_name in columns:
        if column_name not in df.columns:
            df[column_name] = pd.NA
    return df.loc[:, columns]


def _unified_cache_key(record: dict[str, Any]) -> Optional[str]:
    return _normalize_id(record.get("unified_app_id") or record.get("app_id"))


def _local_cache_key(record: dict[str, Any]) -> Optional[tuple[str, str]]:
    os_name = str(record.get("os") or "").strip().lower()
    app_id = _normalize_id(record.get("app_id"))
    if not os_name or not app_id:
        return None
    return os_name, app_id


def _load_ndjson_lookup(
    path: Path,
    key_builder: Callable[[dict[str, Any]], Any],
) -> dict[Any, dict[str, Any]]:
    lookup: dict[Any, dict[str, Any]] = {}
    if not path.exists():
        return lookup
    for row in _read_ndjson_file(path):
        key = key_builder(row)
        if key is None:
            continue
        lookup[key] = row
    return lookup


def _load_ordered_ndjson_lookup(
    path: Path,
    key_builder: Callable[[dict[str, Any]], Any],
) -> tuple[list[Any], dict[Any, dict[str, Any]]]:
    order: list[Any] = []
    lookup: dict[Any, dict[str, Any]] = {}
    if not path.exists():
        return order, lookup
    for row in _read_ndjson_file(path):
        key = key_builder(row)
        if key is None:
            continue
        if key not in lookup:
            order.append(key)
        lookup[key] = row
    return order, lookup


def _load_game_full_info_current_lookup(path: Path) -> tuple[list[str], list[str], dict[str, dict[str, Any]]]:
    if not path.exists():
        return list(GAME_FULL_INFO_COLUMNS), [], {}
    df = pd.read_csv(path, low_memory=False)
    if df.empty:
        return list(df.columns) if len(df.columns) else list(GAME_FULL_INFO_COLUMNS), [], {}
    if "unified_app_id" not in df.columns:
        raise ValueError(f"Missing unified_app_id column in {path}")
    df["unified_app_id"] = df["unified_app_id"].apply(_normalize_id)
    df = df[df["unified_app_id"].notna()].copy()
    columns = list(df.columns)
    order: list[str] = []
    lookup: dict[str, dict[str, Any]] = {}
    for record in df.to_dict(orient="records"):
        unified_app_id = record["unified_app_id"]
        if unified_app_id not in lookup:
            order.append(unified_app_id)
        lookup[unified_app_id] = record
    return columns, order, lookup


def _write_lookup_ndjson(
    path: Path,
    lookup: dict[Any, dict[str, Any]],
) -> None:
    rows = list(lookup.values())
    tmp_path = _temp_path_for_atomic_write(path)
    _write_ndjson_items_to_path(tmp_path, rows)
    _replace_file_atomically(tmp_path, path)


def _upsert_lookup(
    lookup: dict[Any, dict[str, Any]],
    rows: Iterable[dict[str, Any]],
    key_builder: Callable[[dict[str, Any]], Any],
) -> int:
    updated = 0
    for row in rows:
        key = key_builder(row)
        if key is None:
            continue
        lookup[key] = row
        updated += 1
    return updated


def _fetch_unified_apps_resilient(
    api_key: str,
    base_url: str,
    unified_ids: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    if not unified_ids:
        return [], []

    url = f"{base_url}/v1/unified/apps"
    headers = {"Authorization": f"Bearer {api_key}"}
    apps: list[dict[str, Any]] = []
    invalid_ids: list[str] = []

    for chunk in lsdp.chunk_list(unified_ids, 100):
        params = {"app_id_type": "unified", "app_ids": ",".join(chunk)}
        response = requests.get(url, headers=headers, params=params, timeout=60)
        if response.status_code == 200:
            apps.extend(response.json().get("apps", []))
            continue

        for unified_id in chunk:
            single_params = {"app_id_type": "unified", "app_ids": unified_id}
            single_response = requests.get(url, headers=headers, params=single_params, timeout=60)
            if single_response.status_code == 200:
                apps.extend(single_response.json().get("apps", []))
            else:
                invalid_ids.append(unified_id)

    return apps, invalid_ids


def _fetch_local_app_records(
    api_key: str,
    base_url: str,
    os_name: str,
    app_ids: list[str],
) -> list[dict[str, Any]]:
    if not app_ids:
        return []
    return list(lsdp.get_local_app_info_iter(api_key, base_url, os_name, app_ids))


def _extend_game_metadata_for_app_parents(
    *,
    api_key: str,
    base_url: str,
    app_rows: Iterable[dict[str, Any]],
    run_game_path: Path,
    current_game_path: Path,
    run_unified_cache_path: Path,
    current_unified_cache_path: Path,
) -> dict[str, Any]:
    current_columns, current_order, current_lookup = _load_game_full_info_current_lookup(current_game_path)
    referenced_parent_ids = list(
        dict.fromkeys(
            parent_id
            for row in app_rows
            if (parent_id := _normalize_id(row.get("unified_app_id")))
        )
    )
    missing_parent_ids = [parent_id for parent_id in referenced_parent_ids if parent_id not in current_lookup]
    resolved_records: dict[str, dict[str, Any]] = {}
    run_unified_lookup = _load_ndjson_lookup(run_unified_cache_path, _unified_cache_key)
    current_unified_lookup = _load_ndjson_lookup(current_unified_cache_path, _unified_cache_key)
    if missing_parent_ids:
        unresolved_cache_ids = [
            parent_id
            for parent_id in missing_parent_ids
            if parent_id not in run_unified_lookup and parent_id not in current_unified_lookup
        ]
        fetched_records, invalid_ids = _fetch_unified_apps_resilient(api_key, base_url, unresolved_cache_ids)
        _upsert_lookup(run_unified_lookup, fetched_records, _unified_cache_key)
        _upsert_lookup(current_unified_lookup, fetched_records, _unified_cache_key)

        combined_unified_lookup = dict(current_unified_lookup)
        combined_unified_lookup.update(run_unified_lookup)
        parent_records = [
            combined_unified_lookup[parent_id]
            for parent_id in missing_parent_ids
            if parent_id in combined_unified_lookup
        ]
        parent_df = _build_game_full_info_from_unified_records(api_key, base_url, parent_records)
        for record in parent_df.to_dict(orient="records"):
            parent_id = _normalize_id(record.get("unified_app_id"))
            if parent_id:
                record["unified_app_id"] = parent_id
                resolved_records[parent_id] = record

        unresolved_ids = sorted(set(missing_parent_ids) - set(resolved_records))
        if unresolved_ids:
            invalid_text = ", ".join(sorted(set(invalid_ids))) or "none"
            raise RuntimeError(
                "Unable to resolve parent game metadata for app dimension: "
                f"{', '.join(unresolved_ids)} (invalid API IDs: {invalid_text})"
            )

    merged_columns = list(dict.fromkeys(current_columns + GAME_FULL_INFO_COLUMNS))
    for parent_id, record in resolved_records.items():
        if parent_id not in current_lookup:
            current_order.append(parent_id)
        current_lookup[parent_id] = record
    current_rows = [current_lookup[parent_id] for parent_id in current_order if parent_id in current_lookup]
    _write_csv_dataframe_atomic(_records_to_dataframe(current_rows, merged_columns), current_game_path)

    run_columns, run_order, run_lookup = _load_game_full_info_current_lookup(run_game_path)
    run_columns = list(dict.fromkeys(run_columns + GAME_FULL_INFO_COLUMNS))
    run_parent_records = {
        parent_id: current_lookup[parent_id]
        for parent_id in referenced_parent_ids
        if parent_id in current_lookup and parent_id not in run_lookup
    }
    for parent_id, record in run_parent_records.items():
        if parent_id not in run_lookup:
            run_order.append(parent_id)
        run_lookup[parent_id] = record
    run_rows = [run_lookup[parent_id] for parent_id in run_order if parent_id in run_lookup]
    _write_csv_dataframe_atomic(_records_to_dataframe(run_rows, run_columns), run_game_path)

    _write_lookup_ndjson(run_unified_cache_path, run_unified_lookup)
    _write_lookup_ndjson(current_unified_cache_path, current_unified_lookup)
    return {
        "referenced_parent_game_count": len(referenced_parent_ids),
        "missing_parent_game_count": len(missing_parent_ids),
        "resolved_parent_game_count": len(resolved_records),
        "run_parent_game_appended_count": len(run_parent_records),
    }


def _normalize_category_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip().lower() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [text.lower()]


def _contains_required_category(local_record: dict[str, Any], required_value: str) -> bool:
    return required_value.lower() in _normalize_category_values(local_record.get("categories"))


def _extract_child_app_ids(unified_record: dict[str, Any]) -> dict[str, list[str]]:
    result = {"ios": [], "android": []}
    for os_name, field_name in (("ios", "itunes_apps"), ("android", "android_apps")):
        seen: set[str] = set()
        for item in unified_record.get(field_name) or []:
            if not isinstance(item, dict):
                continue
            app_id = _normalize_id(item.get("app_id"))
            if not app_id or app_id in seen:
                continue
            seen.add(app_id)
            result[os_name].append(app_id)
    return result


def _snapshot_candidate_verdict(
    unified_record: dict[str, Any],
    local_lookup: dict[tuple[str, str], dict[str, Any]],
    *,
    child_app_scope: str,
) -> tuple[bool, str]:
    if child_app_scope != "active_only":
        raise ValueError(f"Unsupported TOP_GAME_SNAPSHOT_CHILD_APP_SCOPE: {child_app_scope}")

    child_ids = _extract_child_app_ids(unified_record)
    has_any_active_app = False

    for os_name, required_category in (("ios", "6014"), ("android", "game")):
        platform_records: list[dict[str, Any]] = []
        for app_id in child_ids[os_name]:
            local_record = local_lookup.get((os_name, app_id))
            if local_record is None:
                return False, "missing_platform_metadata"
            platform_records.append(local_record)

        active_records = [record for record in platform_records if bool(record.get("active"))]
        if not active_records:
            continue

        has_any_active_app = True
        if not all(_contains_required_category(record, required_category) for record in active_records):
            return False, f"{os_name}_category_mismatch"

    if not has_any_active_app:
        return False, "no_active_child_apps"

    return True, "kept"


def _build_game_full_info_from_unified_records(
    api_key: str,
    base_url: str,
    unified_records: list[dict[str, Any]],
) -> pd.DataFrame:
    if not unified_records:
        return pd.DataFrame(columns=GAME_FULL_INFO_COLUMNS)

    canonical_df = pd.DataFrame(unified_records)
    canonical_df["canonical_app_id"] = canonical_df["canonical_app_id"].astype(str)
    canonical_id_list = list(dict.fromkeys(canonical_df["canonical_app_id"].tolist()))
    tags_data = lsdp.get_required_tags(api_key, base_url, canonical_id_list)
    tags_table = lsdp.get_tags_table(tags_data)
    df_tags_table = pd.DataFrame(tags_table)
    if df_tags_table.empty:
        df_tags_table = pd.DataFrame(columns=["canonical_app_id"])
    else:
        df_tags_table["canonical_app_id"] = df_tags_table["canonical_app_id"].astype(str)

    # Canonical metadata defines the game universe. Tags are optional enrichment;
    # a missing tag response must never remove a game needed by an app FK.
    df_game_full_info = pd.merge(
        canonical_df,
        df_tags_table,
        how="left",
        on="canonical_app_id",
    )

    for column_name in GAME_FULL_INFO_COLUMNS:
        if column_name not in df_game_full_info.columns:
            df_game_full_info[column_name] = pd.NA

    return df_game_full_info[GAME_FULL_INFO_COLUMNS]


def _extract_game_ids_from_history(df_history: pd.DataFrame) -> list[str]:
    return list(dict.fromkeys([gid for gid in df_history["unified_id"].tolist() if gid]))


def _extract_app_records_from_game_full_info(df_game_full_info: pd.DataFrame) -> list[dict[str, str]]:
    app_records: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for _, row in df_game_full_info.iterrows():
        unified_app_id = _normalize_id(row.get("unified_app_id"))
        for column_name, os_name in (("itunes_apps", "ios"), ("android_apps", "android")):
            raw_value = row.get(column_name)
            if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
                continue
            try:
                parsed = ast.literal_eval(str(raw_value))
            except Exception:
                continue
            if not isinstance(parsed, list):
                continue
            for app in parsed:
                if not isinstance(app, dict):
                    continue
                app_id = _normalize_id(app.get("app_id"))
                if not app_id:
                    continue
                dedupe_key = (os_name, app_id)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                app_records.append(
                    {
                        "unified_app_id": unified_app_id,
                        "os": os_name,
                        "app_id": app_id,
                    }
                )

    return app_records


def _expected_app_parent_lookup(
    app_records: Iterable[dict[str, Any]],
) -> tuple[dict[tuple[str, str], Optional[str]], int]:
    parents_by_key: dict[tuple[str, str], set[str]] = {}
    for record in app_records:
        key = (str(record.get("os") or "").strip().lower(), str(record.get("app_id") or "").strip())
        if not key[0] or not key[1]:
            continue
        parent_id = _normalize_id(record.get("unified_app_id"))
        if parent_id:
            parents_by_key.setdefault(key, set()).add(parent_id)

    conflicts = sum(1 for parents in parents_by_key.values() if len(parents) > 1)
    lookup = {
        key: sorted(parents)[0] if parents else None
        for key, parents in parents_by_key.items()
    }
    return lookup, conflicts


def _reconcile_local_app_parent(
    local_record: dict[str, Any],
    expected_parent_id: Optional[str],
) -> tuple[dict[str, Any], bool, bool]:
    """Return app metadata with a usable parent and source/expected IDs retained for audit."""
    row = dict(local_record)
    source_parent_id = _normalize_id(row.get("unified_app_id"))
    expected_parent_id = _normalize_id(expected_parent_id)
    mismatch = bool(source_parent_id and expected_parent_id and source_parent_id != expected_parent_id)
    filled_from_game = bool(not source_parent_id and expected_parent_id)

    if mismatch or filled_from_game:
        row["sensor_tower_unified_app_id"] = source_parent_id
        row["expected_unified_app_id"] = expected_parent_id
        row["unified_app_id_parent_mismatch"] = mismatch
    if filled_from_game:
        row["unified_app_id"] = expected_parent_id

    return row, mismatch, filled_from_game


def _reconcile_app_metadata_rows(
    rows: Iterable[dict[str, Any]],
    expected_parent_by_key: dict[tuple[str, str], Optional[str]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    reconciled: list[dict[str, Any]] = []
    mismatch_count = 0
    filled_parent_count = 0
    for row in rows:
        key = _local_cache_key(row)
        adjusted, mismatch, filled = _reconcile_local_app_parent(
            row,
            expected_parent_by_key.get(key) if key is not None else None,
        )
        reconciled.append(adjusted)
        mismatch_count += int(mismatch)
        filled_parent_count += int(filled)
    return reconciled, {
        "parent_mismatch_count": mismatch_count,
        "parent_filled_from_game_count": filled_parent_count,
    }


def _load_game_ids_from_json(path: Path) -> list[str]:
    payload = _read_json_payload(path)
    return [gid for gid in payload.get("unified_app_ids", []) if gid]


def _source_base_summary(ctx: PipelineContext, stage: str) -> dict[str, Any]:
    return {
        "run_id": ctx.run_id,
        "run_timestamp": ctx.run_timestamp,
        "trigger_mode": ctx.trigger_mode,
        "config_path": str(ctx.config_path),
        "output_root": str(ctx.output_root),
        "stage_selection": stage,
        "stage_results": [],
        "status": "success",
        "expected_outputs": expected_source_base_output_paths(ctx.output_root, ctx.run_timestamp),
    }


def run_source_base_pipeline(ctx: PipelineContext, stage: str = "all") -> dict[str, Any]:
    _ensure_output_dirs(ctx)
    config = _load_config(ctx.config_path)
    resolved_inputs = resolve_input_paths(config, ctx.repo_root, ctx.input_root)
    outputs = _source_base_outputs(ctx.output_root, ctx.run_timestamp)
    state_outputs = _source_base_state_outputs(ctx.output_root)
    summary = _source_base_summary(ctx, stage)
    current_input_files: dict[str, str] = {}
    current_output_files: dict[str, str] = {}
    stage_order = SOURCE_BASE_STAGE_ORDER if stage == "all" else [stage]
    stage_name = stage_order[0] if stage_order else "unknown"
    api_key: Optional[str] = None

    try:
        for stage_name in stage_order:
            if stage_name == "fetch_top_game_snapshot":
                def _run() -> dict[str, Any]:
                    nonlocal api_key
                    if api_key is None:
                        api_key = _read_api_key(ctx.repo_root)
                    markets = _snapshot_markets(config, ctx)
                    market_frames: list[pd.DataFrame] = []
                    market_metrics: dict[str, Any] = {}
                    first_settings = _snapshot_request_settings(config, ctx, market=markets[0])
                    for market in markets:
                        settings = _snapshot_request_settings(config, ctx, market=market)
                        raw_rows, snapshot_metrics = _fetch_top_game_snapshot_api_rows(
                            api_key,
                            ctx,
                            settings,
                            outputs,
                            state_outputs,
                        )
                        for row in raw_rows:
                            row["_snapshot_market"] = market
                        df_market_snapshot = _build_snapshot_id_frame(raw_rows)
                        if "market" not in df_market_snapshot.columns:
                            df_market_snapshot.insert(0, "market", market)
                        else:
                            df_market_snapshot["market"] = market
                        market_paths = _top_game_market_outputs(ctx.output_root, ctx.run_timestamp, market)
                        _write_csv_dataframe_atomic(df_market_snapshot, market_paths["snapshot"])
                        market_frames.append(df_market_snapshot)
                        market_metrics[market] = {
                            **snapshot_metrics,
                            "row_count": int(len(df_market_snapshot)),
                            "unique_game_count": int(df_market_snapshot["unified_id"].nunique()),
                            "snapshot_path": str(market_paths["snapshot"]),
                            "file_size_bytes": _file_size(market_paths["snapshot"]),
                        }
                    df_snapshot = (
                        pd.concat(market_frames, ignore_index=True)
                        if market_frames
                        else pd.DataFrame(columns=["market", "unified_id", "year", "revenue"])
                    )
                    _write_csv_dataframe_atomic(df_snapshot, outputs["top_game_snapshot"])
                    return {
                        "markets": markets,
                        "market_metrics": market_metrics,
                        "years_requested": list(range(first_settings["start_year"], first_settings["end_year"] + 1)),
                        "year_row_counts": {market: metrics["year_row_counts"] for market, metrics in market_metrics.items()},
                        "year_stop_reasons": {market: metrics["year_stop_reasons"] for market, metrics in market_metrics.items()},
                        "api_request_count": int(sum(metrics["api_request_count"] for metrics in market_metrics.values())),
                        "requests_per_year": first_settings["target_count"] // first_settings["limit"],
                        "candidate_row_count": int(sum(metrics["candidate_row_count"] for metrics in market_metrics.values())),
                        "kept_row_count": int(sum(metrics["kept_row_count"] for metrics in market_metrics.values())),
                        "dropped_row_count": int(sum(metrics["dropped_row_count"] for metrics in market_metrics.values())),
                        "pages_consumed": int(sum(metrics["pages_consumed"] for metrics in market_metrics.values())),
                        "target_fulfilled": all(metrics["target_fulfilled"] for metrics in market_metrics.values()),
                        "stop_reason": "complete" if all(metrics["target_fulfilled"] for metrics in market_metrics.values()) else "partial",
                        "dropped_unresolved_unified_id_count": int(sum(metrics["dropped_unresolved_unified_id_count"] for metrics in market_metrics.values())),
                        "dropped_missing_platform_metadata_count": int(sum(metrics["dropped_missing_platform_metadata_count"] for metrics in market_metrics.values())),
                        "dropped_ios_category_mismatch_count": int(sum(metrics["dropped_ios_category_mismatch_count"] for metrics in market_metrics.values())),
                        "dropped_android_category_mismatch_count": int(sum(metrics["dropped_android_category_mismatch_count"] for metrics in market_metrics.values())),
                        "dropped_no_active_child_apps_count": int(sum(metrics["dropped_no_active_child_apps_count"] for metrics in market_metrics.values())),
                        "unified_cache_hit_count": int(sum(metrics["unified_cache_hit_count"] for metrics in market_metrics.values())),
                        "unified_cache_miss_count": int(sum(metrics["unified_cache_miss_count"] for metrics in market_metrics.values())),
                        "local_cache_hit_count": int(sum(metrics["local_cache_hit_count"] for metrics in market_metrics.values())),
                        "local_cache_miss_count": int(sum(metrics["local_cache_miss_count"] for metrics in market_metrics.values())),
                        "row_count": int(len(df_snapshot)),
                        "unique_game_count": int(df_snapshot["unified_id"].nunique()),
                        "file_size_bytes": _file_size(outputs["top_game_snapshot"]),
                        "filter_defaults_used": {
                            "os": first_settings["os"],
                            "comparison_attribute": first_settings["comparison_attribute"],
                            "time_range": first_settings["time_range"],
                            "measure": first_settings["measure"],
                            "regions": markets,
                            "category": first_settings["category"],
                            "custom_fields_filter_id": first_settings["custom_fields_filter_id"],
                            "data_model": first_settings["data_model"],
                            "post_filter_mode": first_settings["post_filter_mode"],
                            "child_app_scope": first_settings["child_app_scope"],
                        },
                    }

                seed_history_path = _resolve_top_game_history_seed_path(config, ctx, resolved_inputs)
                current_input_files = {}
                if seed_history_path is not None:
                    current_input_files["top_game_all_history_initial"] = str(seed_history_path)
                current_output_files = {
                    "top_game_snapshot": str(outputs["top_game_snapshot"]),
                    **{
                        f"top_game_snapshot_{market.lower()}": str(
                            _top_game_market_outputs(ctx.output_root, ctx.run_timestamp, market)["snapshot"]
                        )
                        for market in _snapshot_markets(config, ctx)
                    },
                    "snapshot_unified_raw": str(outputs["snapshot_unified_raw"]),
                    "snapshot_ios_local_raw": str(outputs["snapshot_ios_local_raw"]),
                    "snapshot_android_local_raw": str(outputs["snapshot_android_local_raw"]),
                    "snapshot_unified_raw_current": str(state_outputs["snapshot_unified_raw_current"]),
                    "snapshot_local_raw_current": str(state_outputs["snapshot_local_raw_current"]),
                }
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            elif stage_name == "update_top_game_history":
                def _run() -> dict[str, Any]:
                    seed_history_path = _resolve_top_game_history_seed_path(config, ctx, resolved_inputs)
                    should_rebuild_from_seed = ctx.force_history_rebuild_from_seed
                    market_histories: list[pd.DataFrame] = []
                    market_metrics: dict[str, Any] = {}
                    bootstrap_mode = False
                    history_sources: set[str] = set()
                    new_game_ids_all: set[str] = set()

                    for market in _snapshot_markets(config, ctx):
                        market_paths = _top_game_market_outputs(ctx.output_root, ctx.run_timestamp, market)
                        if market_paths["snapshot"].exists():
                            df_snapshot = _read_top_game_annual_performance(market_paths["snapshot"], default_market=market)
                        else:
                            df_all_snapshot = _read_top_game_annual_performance(outputs["top_game_snapshot"])
                            if "market" not in df_all_snapshot.columns:
                                # Legacy consolidated snapshot files were VN-only and did not carry a market column.
                                df_all_snapshot["market"] = "VN"
                            df_snapshot = df_all_snapshot[df_all_snapshot["market"] == market].copy()
                        frames: list[pd.DataFrame] = []
                        history_source = "snapshot_only"
                        current_game_ids: set[str] = set()

                        if should_rebuild_from_seed:
                            if market == "VN" and seed_history_path is not None and seed_history_path.exists():
                                df_seed = _read_top_game_annual_performance(seed_history_path, default_market="VN")
                                frames.append(df_seed[df_seed["market"] == market].copy())
                                current_game_ids = set(frames[-1]["unified_id"].dropna().tolist())
                                history_source = "seed_history_forced_rebuild"
                            else:
                                history_source = "snapshot_only_forced_rebuild"
                            bootstrap_mode = True
                        elif market_paths["history_current"].exists():
                            df_current = _read_top_game_annual_performance(market_paths["history_current"], default_market=market)
                            frames.append(df_current[df_current["market"] == market].copy())
                            current_game_ids = set(frames[-1]["unified_id"].dropna().tolist())
                            history_source = "market_current_history"
                        elif market == "VN" and outputs["top_game_history_current"].exists():
                            df_current = _read_top_game_annual_performance(outputs["top_game_history_current"], default_market="VN")
                            frames.append(df_current[df_current["market"] == market].copy())
                            current_game_ids = set(frames[-1]["unified_id"].dropna().tolist())
                            history_source = "legacy_current_history_as_vn"
                        elif market == "VN" and seed_history_path is not None and seed_history_path.exists():
                            df_seed = _read_top_game_annual_performance(seed_history_path, default_market="VN")
                            frames.append(df_seed[df_seed["market"] == market].copy())
                            current_game_ids = set(frames[-1]["unified_id"].dropna().tolist())
                            history_source = "seed_history"
                            bootstrap_mode = True
                        else:
                            bootstrap_mode = True

                        frames.append(df_snapshot)
                        df_market_history = pd.concat(frames, ignore_index=True) if frames else df_snapshot.copy()
                        if "market" not in df_market_history.columns:
                            df_market_history.insert(0, "market", market)
                        df_market_history["market"] = df_market_history["market"].fillna(market).astype(str).str.strip().str.upper()
                        df_market_history = df_market_history[df_market_history["market"] == market].copy()
                        keep_columns = [col for col in ("market", "unified_id", "year", "revenue") if col in df_market_history.columns]
                        df_market_history = df_market_history.loc[:, keep_columns].copy()
                        df_market_history = df_market_history.drop_duplicates(
                            subset=["market", "unified_id", "year"],
                            keep="last",
                        )
                        df_market_history = df_market_history.sort_values(["market", "year", "unified_id"]).reset_index(drop=True)

                        _write_csv_dataframe_atomic(df_market_history, market_paths["history"])
                        _copy_file_to_current(market_paths["history_current"], market_paths["history"])
                        new_game_ids = set(df_snapshot["unified_id"].dropna().tolist()) - current_game_ids
                        new_game_ids_all.update(new_game_ids)
                        history_sources.add(history_source)
                        market_histories.append(df_market_history)
                        market_metrics[market] = {
                            "row_count": int(len(df_market_history)),
                            "unique_game_count": int(df_market_history["unified_id"].nunique()),
                            "new_game_count": int(len(new_game_ids)),
                            "history_source": history_source,
                            "history_path": str(market_paths["history"]),
                            "history_current_path": str(market_paths["history_current"]),
                            "file_size_bytes": _file_size(market_paths["history"]),
                        }

                    df_history = (
                        pd.concat(market_histories, ignore_index=True)
                        if market_histories
                        else pd.DataFrame(columns=["market", "unified_id", "year", "revenue"])
                    )
                    df_history = df_history.drop_duplicates(subset=["market", "unified_id", "year"], keep="last")
                    df_history = df_history.sort_values(["market", "year", "unified_id"]).reset_index(drop=True)
                    _write_csv_dataframe_atomic(df_history, outputs["top_game_history_all_markets"])
                    _copy_file_to_current(outputs["top_game_history_all_markets_current"], outputs["top_game_history_all_markets"])
                    _write_csv_dataframe_atomic(df_history, outputs["top_game_history"])
                    _copy_file_to_current(outputs["top_game_history_current"], outputs["top_game_history"])
                    return {
                        "row_count": int(len(df_history)),
                        "unique_game_count": int(df_history["unified_id"].nunique()),
                        "new_game_count": int(len(new_game_ids_all)),
                        "markets": _snapshot_markets(config, ctx),
                        "market_metrics": market_metrics,
                        "bootstrap_mode": bootstrap_mode,
                        "force_history_rebuild_from_seed": should_rebuild_from_seed,
                        "history_source": ",".join(sorted(history_sources)),
                        "file_size_bytes": _file_size(outputs["top_game_history"]),
                    }

                seed_history_path = _resolve_top_game_history_seed_path(config, ctx, resolved_inputs)
                current_input_files = {
                    "top_game_snapshot": str(outputs["top_game_snapshot"]),
                }
                if outputs["top_game_history_current"].exists():
                    current_input_files["top_game_history_current"] = str(outputs["top_game_history_current"])
                if seed_history_path is not None:
                    current_input_files["top_game_all_history_initial"] = str(seed_history_path)
                current_output_files = {
                    "top_game_history": str(outputs["top_game_history"]),
                    "top_game_history_current": str(outputs["top_game_history_current"]),
                    "top_game_history_all_markets": str(outputs["top_game_history_all_markets"]),
                    "top_game_history_all_markets_current": str(outputs["top_game_history_all_markets_current"]),
                    **{
                        f"top_game_history_{market.lower()}": str(
                            _top_game_market_outputs(ctx.output_root, ctx.run_timestamp, market)["history"]
                        )
                        for market in _snapshot_markets(config, ctx)
                    },
                    **{
                        f"top_game_history_{market.lower()}_current": str(
                            _top_game_market_outputs(ctx.output_root, ctx.run_timestamp, market)["history_current"]
                        )
                        for market in _snapshot_markets(config, ctx)
                    },
                }
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            elif stage_name == "materialize_game_list":
                def _run() -> dict[str, Any]:
                    game_list_source = (ctx.game_list_source or "history").strip().lower()
                    if game_list_source == "snapshot":
                        df_source = _read_top_game_annual_performance(outputs["top_game_snapshot"])
                        unified_app_ids = _extract_game_ids_from_history(df_source)
                    else:
                        game_list_source = "history"
                        source_path = outputs["top_game_history_all_markets"]
                        if not source_path.exists():
                            source_path = outputs["top_game_history"]
                        df_source = _read_top_game_annual_performance(source_path, default_market="VN")
                        unified_app_ids = _extract_game_ids_from_history(df_source)
                    original_list_count = len(unified_app_ids)
                    if ctx.game_list_limit is not None:
                        unified_app_ids = unified_app_ids[: ctx.game_list_limit]
                    by_market: dict[str, list[str]] = {}
                    if "market" in df_source.columns:
                        for market in sorted(df_source["market"].dropna().astype(str).str.upper().unique()):
                            market_ids = _extract_game_ids_from_history(df_source[df_source["market"] == market])
                            by_market[market] = market_ids[: ctx.game_list_limit] if ctx.game_list_limit is not None else market_ids
                    payload = {"unified_app_ids": unified_app_ids}
                    _write_json_payload(outputs["list_of_all_games"], payload)
                    _write_json_payload(
                        outputs["list_of_all_games_by_market"],
                        {
                            "markets": by_market,
                            "market_counts": {market: len(ids) for market, ids in by_market.items()},
                            "union_count": len(unified_app_ids),
                        },
                    )
                    return {
                        "list_count": len(unified_app_ids),
                        "original_list_count": original_list_count,
                        "game_list_limit": ctx.game_list_limit,
                        "game_list_source": game_list_source,
                        "market_counts": {market: len(ids) for market, ids in by_market.items()},
                        "parent_unique_count": int(df_source["unified_id"].nunique()),
                        "is_consistent_with_parent": (
                            len(unified_app_ids) == int(df_source["unified_id"].nunique())
                            if ctx.game_list_limit is None
                            else len(unified_app_ids) <= int(df_source["unified_id"].nunique())
                        ),
                        "file_size_bytes": _file_size(outputs["list_of_all_games"]),
                    }

                current_input_files = {
                    "top_game_history": str(outputs["top_game_history"]),
                    "top_game_history_all_markets": str(outputs["top_game_history_all_markets"]),
                    "top_game_snapshot": str(outputs["top_game_snapshot"]),
                }
                current_output_files = {
                    "list_of_all_games": str(outputs["list_of_all_games"]),
                    "list_of_all_games_by_market": str(outputs["list_of_all_games_by_market"]),
                }
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            elif stage_name == "build_game_full_info":
                def _run() -> dict[str, Any]:
                    nonlocal api_key
                    if api_key is None:
                        api_key = _read_api_key(ctx.repo_root)
                    unified_app_ids = _load_game_ids_from_json(outputs["list_of_all_games"])
                    run_unified_lookup = _load_ndjson_lookup(outputs["snapshot_unified_raw"], _unified_cache_key)
                    current_unified_lookup = _load_ndjson_lookup(
                        state_outputs["snapshot_unified_raw_current"],
                        _unified_cache_key,
                    )
                    current_columns, current_row_order, current_game_lookup = _load_game_full_info_current_lookup(
                        state_outputs["game_full_info_current"]
                    )

                    missing_ids: list[str] = []
                    unified_cache_hit_count = 0
                    current_output_hit_count = 0
                    for unified_app_id in unified_app_ids:
                        if (
                            _normalize_load_mode(ctx.facts_load_mode) == DAILY_LOAD_MODE
                            and state_outputs["game_full_info_current"].exists()
                            and unified_app_id in current_game_lookup
                        ):
                            current_output_hit_count += 1
                            continue

                        cached = run_unified_lookup.get(unified_app_id) or current_unified_lookup.get(unified_app_id)
                        if cached is None:
                            missing_ids.append(unified_app_id)
                            continue
                        run_unified_lookup[unified_app_id] = cached
                        unified_cache_hit_count += 1

                    fetched_records, invalid_ids = _fetch_unified_apps_resilient(api_key, ctx.base_url, missing_ids)
                    _upsert_lookup(run_unified_lookup, fetched_records, _unified_cache_key)
                    _upsert_lookup(current_unified_lookup, fetched_records, _unified_cache_key)
                    _write_lookup_ndjson(outputs["snapshot_unified_raw"], run_unified_lookup)
                    _write_lookup_ndjson(state_outputs["snapshot_unified_raw_current"], current_unified_lookup)

                    combined_lookup = dict(current_unified_lookup)
                    combined_lookup.update(run_unified_lookup)

                    requested_unique_ids = list(dict.fromkeys(unified_app_ids))
                    refresh_all_rows = (
                        _normalize_load_mode(ctx.facts_load_mode) != DAILY_LOAD_MODE
                        or not state_outputs["game_full_info_current"].exists()
                    )
                    if refresh_all_rows:
                        ordered_records: list[dict[str, Any]] = []
                        for unified_app_id in requested_unique_ids:
                            cached = combined_lookup.get(unified_app_id)
                            if cached is None:
                                continue
                            ordered_records.append(cached)

                        df_game_full_info = _build_game_full_info_from_unified_records(
                            api_key, ctx.base_url, ordered_records
                        )
                        _write_csv_dataframe_atomic(df_game_full_info, outputs["game_full_info"])
                        _write_csv_dataframe_atomic(df_game_full_info, state_outputs["game_full_info_current"])
                        tag_fetch_game_count = int(len(df_game_full_info))
                    else:
                        new_unified_ids = [uid for uid in requested_unique_ids if uid not in current_game_lookup]
                        new_unified_records: list[dict[str, Any]] = []
                        for unified_app_id in new_unified_ids:
                            cached = combined_lookup.get(unified_app_id)
                            if cached is None:
                                continue
                            new_unified_records.append(cached)

                        df_new_game_rows = _build_game_full_info_from_unified_records(
                            api_key, ctx.base_url, new_unified_records
                        )
                        new_game_lookup: dict[str, dict[str, Any]] = {}
                        for record in df_new_game_rows.to_dict(orient="records"):
                            unified_app_id = _normalize_id(record.get("unified_app_id"))
                            if not unified_app_id:
                                continue
                            record["unified_app_id"] = unified_app_id
                            new_game_lookup[unified_app_id] = record

                        merged_row_order = list(current_row_order)
                        for unified_app_id, record in new_game_lookup.items():
                            if unified_app_id not in current_game_lookup:
                                merged_row_order.append(unified_app_id)
                            current_game_lookup[unified_app_id] = record

                        merged_current_records = [
                            current_game_lookup[unified_app_id]
                            for unified_app_id in merged_row_order
                            if unified_app_id in current_game_lookup
                        ]
                        merged_columns = list(dict.fromkeys(current_columns + GAME_FULL_INFO_COLUMNS))
                        df_game_full_info_current = _records_to_dataframe(merged_current_records, merged_columns)
                        _write_csv_dataframe_atomic(df_game_full_info_current, state_outputs["game_full_info_current"])

                        requested_records = [
                            current_game_lookup[unified_app_id]
                            for unified_app_id in requested_unique_ids
                            if unified_app_id in current_game_lookup
                        ]
                        df_game_full_info = _records_to_dataframe(requested_records, merged_columns)
                        _write_csv_dataframe_atomic(df_game_full_info, outputs["game_full_info"])
                        tag_fetch_game_count = int(len(df_new_game_rows))

                    return {
                        "requested_game_count": len(requested_unique_ids),
                        "row_count": int(len(df_game_full_info)),
                        "current_output_hit_count": current_output_hit_count,
                        "current_output_miss_count": len(requested_unique_ids) - current_output_hit_count,
                        "unified_cache_hit_count": unified_cache_hit_count,
                        "unified_cache_miss_count": len(missing_ids),
                        "tag_fetch_game_count": tag_fetch_game_count,
                        "dropped_unresolved_unified_id_count": len(invalid_ids),
                        "file_size_bytes": _file_size(outputs["game_full_info"]),
                        "current_file_size_bytes": _file_size(state_outputs["game_full_info_current"]),
                    }

                current_input_files = {"list_of_all_games": str(outputs["list_of_all_games"])}
                current_output_files = {
                    "game_full_info": str(outputs["game_full_info"]),
                    "game_full_info_current": str(state_outputs["game_full_info_current"]),
                }
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            elif stage_name == "materialize_app_list":
                def _run() -> dict[str, Any]:
                    df_game_full_info = pd.read_csv(outputs["game_full_info"], low_memory=False)
                    apps = _extract_app_records_from_game_full_info(df_game_full_info)
                    payload = {"apps": apps}
                    _write_json_payload(outputs["list_of_all_apps"], payload)
                    parent_count = len(apps)
                    return {
                        "list_count": parent_count,
                        "parent_app_reference_count": parent_count,
                        "is_consistent_with_parent": True,
                        "file_size_bytes": _file_size(outputs["list_of_all_apps"]),
                    }

                current_input_files = {"game_full_info": str(outputs["game_full_info"])}
                current_output_files = {"list_of_all_apps": str(outputs["list_of_all_apps"])}
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            elif stage_name == "build_app_full_info":
                def _run() -> dict[str, Any]:
                    nonlocal api_key
                    if api_key is None:
                        api_key = _read_api_key(ctx.repo_root)
                    df_game_full_info = pd.read_csv(outputs["game_full_info"], low_memory=False)
                    app_records = _extract_app_records_from_game_full_info(df_game_full_info)
                    expected_parent_by_key, expected_parent_conflict_count = _expected_app_parent_lookup(app_records)
                    if expected_parent_conflict_count:
                        raise RuntimeError(
                            "Conflicting parent games found for the same app in game metadata: "
                            f"{expected_parent_conflict_count} app key(s)"
                        )
                    run_ios_lookup = _load_ndjson_lookup(outputs["snapshot_ios_local_raw"], _local_cache_key)
                    run_android_lookup = _load_ndjson_lookup(outputs["snapshot_android_local_raw"], _local_cache_key)
                    current_local_lookup = _load_ndjson_lookup(
                        state_outputs["snapshot_local_raw_current"],
                        _local_cache_key,
                    )
                    current_app_order, current_app_lookup = _load_ordered_ndjson_lookup(
                        state_outputs["app_full_info_current"],
                        _local_cache_key,
                    )

                    ios_ids = [record["app_id"] for record in app_records if record["os"] == "ios"]
                    android_ids = [record["app_id"] for record in app_records if record["os"] == "android"]
                    ios_missing: list[str] = []
                    android_missing: list[str] = []
                    local_cache_hit_count = 0
                    current_output_hit_count = 0
                    requested_keys = list(dict.fromkeys((record["os"], record["app_id"]) for record in app_records))

                    for app_id in list(dict.fromkeys(ios_ids)):
                        if (
                            _normalize_load_mode(ctx.facts_load_mode) == DAILY_LOAD_MODE
                            and state_outputs["app_full_info_current"].exists()
                            and ("ios", app_id) in current_app_lookup
                        ):
                            current_output_hit_count += 1
                            continue
                        cached = run_ios_lookup.get(("ios", app_id)) or current_local_lookup.get(("ios", app_id))
                        if cached is None:
                            ios_missing.append(app_id)
                            continue
                        run_ios_lookup[("ios", app_id)] = cached
                        local_cache_hit_count += 1

                    for app_id in list(dict.fromkeys(android_ids)):
                        if (
                            _normalize_load_mode(ctx.facts_load_mode) == DAILY_LOAD_MODE
                            and state_outputs["app_full_info_current"].exists()
                            and ("android", app_id) in current_app_lookup
                        ):
                            current_output_hit_count += 1
                            continue
                        cached = run_android_lookup.get(("android", app_id)) or current_local_lookup.get(("android", app_id))
                        if cached is None:
                            android_missing.append(app_id)
                            continue
                        run_android_lookup[("android", app_id)] = cached
                        local_cache_hit_count += 1

                    fetched_ios = _fetch_local_app_records(api_key, ctx.base_url, "ios", ios_missing)
                    fetched_android = _fetch_local_app_records(api_key, ctx.base_url, "android", android_missing)

                    _upsert_lookup(run_ios_lookup, fetched_ios, _local_cache_key)
                    _upsert_lookup(run_android_lookup, fetched_android, _local_cache_key)
                    _upsert_lookup(current_local_lookup, fetched_ios, _local_cache_key)
                    _upsert_lookup(current_local_lookup, fetched_android, _local_cache_key)

                    _write_lookup_ndjson(outputs["snapshot_ios_local_raw"], run_ios_lookup)
                    _write_lookup_ndjson(outputs["snapshot_android_local_raw"], run_android_lookup)
                    _write_lookup_ndjson(state_outputs["snapshot_local_raw_current"], current_local_lookup)

                    combined_local_lookup = dict(current_local_lookup)
                    combined_local_lookup.update(run_ios_lookup)
                    combined_local_lookup.update(run_android_lookup)

                    refresh_all_rows = (
                        _normalize_load_mode(ctx.facts_load_mode) != DAILY_LOAD_MODE
                        or not state_outputs["app_full_info_current"].exists()
                    )
                    missing_row_count = 0
                    unresolved_requested_keys: set[tuple[str, str]] = set()
                    parent_reconcile_metrics = {
                        "parent_mismatch_count": 0,
                        "parent_filled_from_game_count": 0,
                    }
                    if refresh_all_rows:
                        ordered_rows: list[dict[str, Any]] = []
                        seen_keys: set[tuple[str, str]] = set()
                        for app_record in app_records:
                            lookup_key = (app_record["os"], app_record["app_id"])
                            if lookup_key in seen_keys:
                                continue
                            seen_keys.add(lookup_key)
                            local_record = combined_local_lookup.get(lookup_key)
                            if local_record is None:
                                missing_row_count += 1
                                unresolved_requested_keys.add(lookup_key)
                                continue
                            ordered_rows.append(local_record)

                        ordered_rows, parent_reconcile_metrics = _reconcile_app_metadata_rows(
                            ordered_rows,
                            expected_parent_by_key,
                        )
                        parent_closure_metrics = _extend_game_metadata_for_app_parents(
                            api_key=api_key,
                            base_url=ctx.base_url,
                            app_rows=ordered_rows,
                            run_game_path=outputs["game_full_info"],
                            current_game_path=state_outputs["game_full_info_current"],
                            run_unified_cache_path=outputs["snapshot_unified_raw"],
                            current_unified_cache_path=state_outputs["snapshot_unified_raw_current"],
                        )

                        _write_ndjson_file(outputs["app_full_info"], ordered_rows, append=False)
                        _write_ndjson_file(state_outputs["app_full_info_current"], ordered_rows, append=False)
                    else:
                        new_rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
                        for lookup_key in requested_keys:
                            if lookup_key in current_app_lookup:
                                continue
                            local_record = combined_local_lookup.get(lookup_key)
                            if local_record is None:
                                missing_row_count += 1
                                unresolved_requested_keys.add(lookup_key)
                                continue
                            new_rows_by_key[lookup_key] = local_record

                        merged_app_order = list(current_app_order)
                        for lookup_key, row in new_rows_by_key.items():
                            if lookup_key not in current_app_lookup:
                                merged_app_order.append(lookup_key)
                            current_app_lookup[lookup_key] = row

                        merged_rows = [
                            current_app_lookup[lookup_key]
                            for lookup_key in merged_app_order
                            if lookup_key in current_app_lookup
                        ]
                        merged_rows, parent_reconcile_metrics = _reconcile_app_metadata_rows(
                            merged_rows,
                            expected_parent_by_key,
                        )
                        current_app_lookup = {
                            key: row
                            for row in merged_rows
                            if (key := _local_cache_key(row)) is not None
                        }
                        parent_closure_metrics = _extend_game_metadata_for_app_parents(
                            api_key=api_key,
                            base_url=ctx.base_url,
                            app_rows=merged_rows,
                            run_game_path=outputs["game_full_info"],
                            current_game_path=state_outputs["game_full_info_current"],
                            run_unified_cache_path=outputs["snapshot_unified_raw"],
                            current_unified_cache_path=state_outputs["snapshot_unified_raw_current"],
                        )
                        _write_ndjson_file(state_outputs["app_full_info_current"], merged_rows, append=False)

                        ordered_rows = []
                        for lookup_key in requested_keys:
                            local_record = current_app_lookup.get(lookup_key)
                            if local_record is None:
                                if lookup_key not in unresolved_requested_keys:
                                    missing_row_count += 1
                                continue
                            ordered_rows.append(local_record)
                        _write_ndjson_file(outputs["app_full_info"], ordered_rows, append=False)

                    return {
                        "row_count": _count_ndjson_lines(outputs["app_full_info"]),
                        "expected_app_count": len(requested_keys),
                        "current_output_hit_count": current_output_hit_count,
                        "current_output_miss_count": len(requested_keys) - current_output_hit_count,
                        "local_cache_hit_count": local_cache_hit_count,
                        "local_cache_miss_count": len(ios_missing) + len(android_missing),
                        "missing_local_app_record_count": missing_row_count,
                        "expected_parent_conflict_count": expected_parent_conflict_count,
                        **parent_reconcile_metrics,
                        **parent_closure_metrics,
                        "file_size_bytes": _file_size(outputs["app_full_info"]),
                        "current_file_size_bytes": _file_size(state_outputs["app_full_info_current"]),
                    }

                current_input_files = {
                    "game_full_info": str(outputs["game_full_info"]),
                    "list_of_all_apps": str(outputs["list_of_all_apps"]),
                }
                current_output_files = {"app_full_info": str(outputs["app_full_info"])}
                current_output_files["app_full_info_current"] = str(state_outputs["app_full_info_current"])
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            elif stage_name == "build_app_performance":
                def _run() -> dict[str, Any]:
                    nonlocal api_key
                    if api_key is None:
                        api_key = _read_api_key(ctx.repo_root)
                    window = _facts_window_for_mode(
                        ctx.performance_end_date,
                        ctx.facts_load_mode,
                        _app_performance_current_available(state_outputs["app_performance_current"]),
                        ctx.facts_start_date,
                    )
                    fact_country_codes = _fact_country_codes(config, ctx)
                    df_app_full_info = pd.read_json(outputs["app_full_info"], lines=True)
                    app_performance = lsdp.create_table_app_performance_checkpointed(
                        api_key,
                        ctx.base_url,
                        str_start_date=window["start_date"],
                        str_end_date=window["end_date"],
                        df_app_full_info=df_app_full_info,
                        json_export_path=str(ctx.output_root / "base"),
                        export_path=str(outputs["app_performance"]),
                        country_codes=fact_country_codes,
                    )
                    row_count = int(app_performance["row_count"])
                    del app_performance
                    gc.collect()
                    merge_metrics = _merge_app_performance_into_current_streaming(
                        current_path=state_outputs["app_performance_current"],
                        run_output_path=outputs["app_performance"],
                        history_archive_path=state_outputs["app_performance_history_archive"],
                        key_builder=lambda row: (
                            str(row.get("aid")),
                            str(row.get("c") or row.get("cc")),
                            str(row.get("d")),
                        ),
                        record_date_field="d",
                        window=window,
                        dataset_name="app_performance_daily",
                        expired_at=_utc_now_iso(),
                        drop_missing_in_window=_normalize_load_mode(ctx.facts_load_mode)
                        not in {BACKFILL_LOAD_MODE, QUARTERLY_LOAD_MODE},
                    )
                    return {
                        "row_count": row_count,
                        "fact_country_codes": fact_country_codes,
                        "file_size_bytes": _file_size(outputs["app_performance"]),
                        **merge_metrics,
                    }

                current_input_files = {
                    "app_full_info": str(outputs["app_full_info"]),
                    "app_performance_current": str(state_outputs["app_performance_current"]),
                }
                current_output_files = {
                    "app_performance": str(outputs["app_performance"]),
                    "app_performance_current": str(state_outputs["app_performance_current"]),
                    "app_performance_history_archive": str(state_outputs["app_performance_history_archive"]),
                }
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            elif stage_name == "build_app_active_users":
                def _run() -> dict[str, Any]:
                    nonlocal api_key
                    if api_key is None:
                        api_key = _read_api_key(ctx.repo_root)
                    window = _facts_window_for_mode(
                        ctx.performance_end_date,
                        ctx.facts_load_mode,
                        _app_active_users_current_available(state_outputs["app_active_users_current"]),
                        ctx.facts_start_date,
                    )
                    fact_country_codes = _fact_country_codes(config, ctx)
                    df_app_full_info = pd.read_json(outputs["app_full_info"], lines=True)
                    active_users_result = lsdp.create_table_app_active_users_checkpointed(
                        api_key=api_key,
                        base_url=ctx.base_url,
                        str_start_date=window["start_date"],
                        str_end_date=window["end_date"],
                        df_app_full_info=df_app_full_info,
                        json_export_path=str(ctx.output_root / "base"),
                        export_path=str(outputs["app_active_users"]),
                        country_codes=fact_country_codes,
                    )
                    row_count = int(active_users_result["row_count"])
                    del active_users_result
                    gc.collect()
                    merge_metrics = _merge_app_active_users_into_current_streaming(
                        current_path=state_outputs["app_active_users_current"],
                        run_output_path=outputs["app_active_users"],
                        history_archive_path=state_outputs["app_active_users_history_archive"],
                        key_builder=lambda row: (
                            str(row.get("app_id")),
                            str(row.get("country")),
                            str(row.get("date")),
                        ),
                        record_date_field="date",
                        window=window,
                        dataset_name="app_active_users",
                        expired_at=_utc_now_iso(),
                        drop_missing_in_window=_normalize_load_mode(ctx.facts_load_mode)
                        not in {BACKFILL_LOAD_MODE, QUARTERLY_LOAD_MODE},
                    )
                    return {
                        "row_count": row_count,
                        "fact_country_codes": fact_country_codes,
                        "file_size_bytes": _file_size(outputs["app_active_users"]),
                        **merge_metrics,
                    }

                current_input_files = {
                    "app_full_info": str(outputs["app_full_info"]),
                    "app_active_users_current": str(state_outputs["app_active_users_current"]),
                }
                current_output_files = {
                    "app_active_users": str(outputs["app_active_users"]),
                    "app_active_users_current": str(state_outputs["app_active_users_current"]),
                    "app_active_users_history_archive": str(state_outputs["app_active_users_history_archive"]),
                }
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            elif stage_name == "build_app_retention":
                def _run() -> dict[str, Any]:
                    nonlocal api_key
                    if api_key is None:
                        api_key = _read_api_key(ctx.repo_root)
                    window = _facts_window_for_mode(
                        ctx.performance_end_date,
                        ctx.facts_load_mode,
                        state_outputs["app_retention_current"].exists(),
                        ctx.facts_start_date,
                    )
                    df_app_full_info = pd.read_json(outputs["app_full_info"], lines=True)
                    out_path, record_count = lsdp.create_table_app_retention_daily_ndjson(
                        api_key=api_key,
                        base_url=ctx.base_url,
                        str_start_date=window["start_date"],
                        str_end_date=window["end_date"],
                        df_app_full_info=df_app_full_info,
                        ndjson_export_path=str(ctx.output_root / "base"),
                        export_path=str(outputs["app_retention"]),
                    )
                    merge_metrics = _merge_ndjson_dataset_into_current(
                        current_path=state_outputs["app_retention_current"],
                        run_output_path=outputs["app_retention"],
                        history_archive_path=state_outputs["app_retention_history_archive"],
                        key_builder=lambda row: (
                            str(row.get("app_id")),
                            str(row.get("country")),
                            str(row.get("date")),
                        ),
                        record_date_field="date",
                        window=window,
                        dataset_name="app_retention_daily",
                        expired_at=_utc_now_iso(),
                    )
                    return {
                        "output_path": out_path,
                        "record_count": int(record_count),
                        "file_size_bytes": _file_size(outputs["app_retention"]),
                        **merge_metrics,
                    }

                current_input_files = {
                    "app_full_info": str(outputs["app_full_info"]),
                    "app_retention_current": str(state_outputs["app_retention_current"]),
                }
                current_output_files = {
                    "app_retention": str(outputs["app_retention"]),
                    "app_retention_current": str(state_outputs["app_retention_current"]),
                    "app_retention_history_archive": str(state_outputs["app_retention_history_archive"]),
                }
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            elif stage_name == "build_user_demographics":
                def _run() -> dict[str, Any]:
                    nonlocal api_key
                    if api_key is None:
                        api_key = _read_api_key(ctx.repo_root)
                    df_app_full_info = pd.read_json(outputs["app_full_info"], lines=True)
                    out_path, record_count = lsdp.create_table_app_demographics_all_time_ndjson_streaming(
                        api_key=api_key,
                        base_url=ctx.base_url,
                        df_app_full_info=df_app_full_info,
                        ndjson_export_path=str(ctx.output_root / "base"),
                        start_date=DEFAULT_ANALYTICS_START_DATE,
                        export_path=str(outputs["user_demographics"]),
                    )
                    merge_metrics = _merge_snapshot_ndjson_into_current(
                        current_path=state_outputs["user_demographics_current"],
                        run_output_path=outputs["user_demographics"],
                        history_archive_path=state_outputs["user_demographics_history_archive"],
                        key_builder=lambda row: (
                            str(row.get("app_id")),
                            str(row.get("country")),
                            str(row.get("date_granularity")),
                        ),
                        load_mode=ctx.facts_load_mode,
                        dataset_name="app_demographics",
                        expired_at=_utc_now_iso(),
                    )
                    return {
                        "output_path": out_path,
                        "record_count": int(record_count),
                        "file_size_bytes": _file_size(outputs["user_demographics"]),
                        **merge_metrics,
                    }

                current_input_files = {
                    "app_full_info": str(outputs["app_full_info"]),
                    "user_demographics_current": str(state_outputs["user_demographics_current"]),
                }
                current_output_files = {
                    "user_demographics": str(outputs["user_demographics"]),
                    "user_demographics_current": str(state_outputs["user_demographics_current"]),
                    "user_demographics_history_archive": str(state_outputs["user_demographics_history_archive"]),
                }
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            elif stage_name == "build_user_reviews":
                def _run() -> dict[str, Any]:
                    nonlocal api_key
                    if api_key is None:
                        api_key = _read_api_key(ctx.repo_root)
                    df_app_full_info = pd.read_json(outputs["app_full_info"], lines=True)
                    out_path, record_count = lsdp.create_table_app_user_reviews_ndjson_streaming(
                        api_key=api_key,
                        base_url=ctx.base_url,
                        str_start_date=ctx.performance_start_date,
                        str_end_date=ctx.performance_end_date,
                        df_app_full_info=df_app_full_info,
                        ndjson_export_path=str(ctx.output_root / "base"),
                        limit=DEFAULT_REVIEWS_LIMIT,
                        export_path=str(outputs["user_reviews"]),
                    )
                    return {
                        "output_path": out_path,
                        "record_count": int(record_count),
                        "file_size_bytes": _file_size(outputs["user_reviews"]),
                    }

                current_input_files = {"app_full_info": str(outputs["app_full_info"])}
                current_output_files = {"user_reviews": str(outputs["user_reviews"])}
                stage_result = _execute_stage(ctx, stage_name, current_input_files, current_output_files, _run)

            else:
                raise ValueError(f"Unsupported source-base stage '{stage_name}'.")

            summary["stage_results"].append(stage_result)

        summary["result_file"] = _write_named_result(ctx, "run_source_base_pipeline", summary)
        return summary

    except Exception as exc:
        summary["status"] = "failed"
        summary["failed_stage"] = stage_name
        summary["error_summary"] = str(exc)
        summary["traceback"] = traceback.format_exc()
        failure_result = _build_stage_result(
            stage_name=stage_name,
            started_at=_utc_now_iso(),
            finished_at=_utc_now_iso(),
            input_files=current_input_files,
            output_files=current_output_files,
            status="failed",
            error_summary=str(exc),
        )
        summary["stage_results"].append(failure_result)
        summary["result_file"] = _write_named_result(ctx, "run_source_base_pipeline", summary)
        return summary


def validate_base_outputs(ctx: PipelineContext) -> dict[str, Any]:
    _ensure_output_dirs(ctx)
    started_at = _utc_now_iso()
    config = _load_config(ctx.config_path)
    outputs = expected_source_base_output_paths(ctx.output_root, ctx.run_timestamp)
    state_outputs = _source_base_state_outputs(ctx.output_root)
    include_user_reviews = bool(config.get("INCLUDE_USER_REVIEWS_IN_BASE_FLOW", True))
    lightweight_json_datasets = {"app_performance", "app_active_users"}
    checks: dict[str, Any] = {}
    missing_files: list[dict[str, str]] = []

    try:
        for name, file_path in outputs.items():
            if name == "top_game_history_current":
                continue
            if name == "user_reviews" and not include_user_reviews:
                continue
            path_obj = Path(file_path)
            if not path_obj.exists():
                missing_files.append({"name": name, "path": file_path})
                continue
            if name == "app_retention":
                checks[name] = _validate_retention_run_output(
                    path_obj,
                    current_path=state_outputs["app_retention_current"],
                    facts_load_mode=ctx.facts_load_mode,
                )
                continue
            if path_obj.suffix == ".csv":
                checks[name] = _validate_csv(path_obj)
            elif path_obj.suffix == ".ndjson":
                checks[name] = _validate_ndjson(path_obj)
            elif path_obj.suffix == ".json":
                if name in lightweight_json_datasets:
                    checks[name] = _validate_json_array_lightweight(path_obj)
                    continue
                payload = _read_json_payload(path_obj)
                if isinstance(payload, dict) and "unified_app_ids" in payload:
                    checks[name] = {
                        "file_size_bytes": _file_size(path_obj),
                        "row_count": len(payload["unified_app_ids"]),
                        "sample_keys": ["unified_app_ids"],
                    }
                elif isinstance(payload, dict) and "markets" in payload:
                    checks[name] = {
                        "file_size_bytes": _file_size(path_obj),
                        "row_count": int(payload.get("union_count") or 0),
                        "sample_keys": ["markets", "market_counts", "union_count"],
                    }
                elif isinstance(payload, dict) and "apps" in payload:
                    checks[name] = {
                        "file_size_bytes": _file_size(path_obj),
                        "row_count": len(payload["apps"]),
                        "sample_keys": ["apps"],
                    }
                else:
                    checks[name] = _validate_json_array(path_obj)
            else:
                checks[name] = {"file_size_bytes": _file_size(path_obj)}

        status = "success" if not missing_files else "failed"
        error_summary = None if not missing_files else "Missing expected base outputs."
    except Exception as exc:
        status = "failed"
        error_summary = str(exc)

    finished_at = _utc_now_iso()
    stage_result = _build_stage_result(
        stage_name="validate_base_outputs",
        started_at=started_at,
        finished_at=finished_at,
        input_files={},
        output_files={
            name: path
            for name, path in outputs.items()
            if include_user_reviews or name != "user_reviews"
        },
        metrics={
            "checked_output_count": len(checks),
            "missing_output_count": len(missing_files),
            "include_user_reviews": include_user_reviews,
        },
        status=status,
        error_summary=error_summary,
    )
    summary = {
        "run_id": ctx.run_id,
        "run_timestamp": ctx.run_timestamp,
        "status": status,
        "stage_name": "validate_base_outputs",
        "output_root": str(ctx.output_root),
        "include_user_reviews": include_user_reviews,
        "checked_outputs": checks,
        "missing_files": missing_files,
        "stage_result": stage_result,
    }
    if status == "failed" and error_summary:
        summary["error_summary"] = error_summary
    summary["result_file"] = _write_named_result(ctx, "validate_base_outputs", summary)
    return summary


def _validate_csv(path: Path) -> dict[str, Any]:
    df = pd.read_csv(path, nrows=1)
    return {
        "file_size_bytes": _file_size(path),
        "is_non_empty": _file_size(path) > 0,
        "sample_columns": list(df.columns),
    }


def _validate_ndjson(path: Path) -> dict[str, Any]:
    first_record = None
    row_count = 0
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            obj = json.loads(line)
            row_count += 1
            if first_record is None:
                first_record = list(obj.keys())[:10]
    if row_count == 0:
        raise ValueError(f"NDJSON file is empty: {path}")
    return {
        "file_size_bytes": _file_size(path),
        "row_count": row_count,
        "sample_keys": first_record or [],
    }


def _ndjson_has_record(path: Path) -> bool:
    if not path.exists():
        return False
    with open(path, "r", encoding="utf-8") as handle:
        return any(bool(line.strip()) for line in handle)


def _validate_retention_run_output(
    path: Path,
    *,
    current_path: Path,
    facts_load_mode: Optional[str],
) -> dict[str, Any]:
    if _ndjson_has_record(path):
        return _validate_ndjson(path)

    normalized_mode = _normalize_load_mode(facts_load_mode)
    if normalized_mode not in {DAILY_LOAD_MODE, WEEKLY_LOAD_MODE}:
        raise ValueError(
            f"Retention NDJSON file is empty for {normalized_mode} load mode: {path}"
        )
    if not _ndjson_has_record(current_path):
        raise ValueError(
            "Retention NDJSON file is empty and no non-empty current baseline exists: "
            f"run_output={path}; current_baseline={current_path}"
        )

    return {
        "file_size_bytes": _file_size(path),
        "row_count": 0,
        "sample_keys": [],
        "validation_mode": "empty_incremental_retention_allowed",
        "reason": (
            "No retention rows were returned for this incremental window; "
            "existing current retention baseline remains the source of truth."
        ),
        "current_baseline_path": str(current_path),
        "current_baseline_file_size_bytes": _file_size(current_path),
    }


def _validate_json_array(path: Path) -> dict[str, Any]:
    row_count = 0
    first_record = None
    with open(path, "rb") as handle:
        for item in ijson.items(handle, "item"):
            row_count += 1
            if first_record is None and isinstance(item, dict):
                first_record = list(item.keys())[:10]
    if row_count == 0:
        raise ValueError(f"JSON array file is empty: {path}")
    return {
        "file_size_bytes": _file_size(path),
        "row_count": row_count,
        "sample_keys": first_record or [],
    }


def _last_non_whitespace_byte(path: Path) -> Optional[bytes]:
    with open(path, "rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        while position > 0:
            position -= 1
            handle.seek(position)
            value = handle.read(1)
            if value and not value.isspace():
                return value
    return None


def _validate_json_array_lightweight(path: Path) -> dict[str, Any]:
    first_record = None
    with open(path, "rb") as handle:
        for item in ijson.items(handle, "item"):
            if isinstance(item, dict):
                first_record = list(item.keys())[:10]
            else:
                first_record = []
            break

    if first_record is None:
        raise ValueError(f"JSON array file is empty: {path}")

    trailing_byte = _last_non_whitespace_byte(path)
    return {
        "file_size_bytes": _file_size(path),
        "validation_mode": "lightweight",
        "closed_json_array": trailing_byte == b"]",
        "sample_keys": first_record,
    }


def validate_outputs(ctx: PipelineContext) -> dict[str, Any]:
    _ensure_output_dirs(ctx)
    started_at = _utc_now_iso()
    outputs = expected_output_paths(ctx.output_root, ctx.run_timestamp)
    checks: dict[str, Any] = {}
    missing_files = []

    try:
        for name, file_path in outputs.items():
            path_obj = Path(file_path)
            if not path_obj.exists():
                missing_files.append({"name": name, "path": file_path})
                continue
            if path_obj.suffix == ".csv":
                checks[name] = _validate_csv(path_obj)
            elif path_obj.suffix == ".ndjson":
                checks[name] = _validate_ndjson(path_obj)
            elif path_obj.suffix == ".json":
                checks[name] = _validate_json_array(path_obj)
            else:
                checks[name] = {"file_size_bytes": _file_size(path_obj)}

        status = "success" if not missing_files else "failed"
        error_summary = None if not missing_files else "Missing expected outputs."
    except Exception as exc:
        status = "failed"
        error_summary = str(exc)

    finished_at = _utc_now_iso()
    stage_result = _build_stage_result(
        stage_name="validate_outputs",
        started_at=started_at,
        finished_at=finished_at,
        input_files={},
        output_files=outputs,
        metrics={
            "checked_output_count": len(checks),
            "missing_output_count": len(missing_files),
        },
        status=status,
        error_summary=error_summary,
    )

    summary = {
        "run_id": ctx.run_id,
        "run_timestamp": ctx.run_timestamp,
        "status": status,
        "stage_name": "validate_outputs",
        "output_root": str(ctx.output_root),
        "checked_outputs": checks,
        "missing_files": missing_files,
        "stage_result": stage_result,
    }
    if status == "failed" and error_summary:
        summary["error_summary"] = error_summary
    summary["result_file"] = _write_named_result(ctx, "validate_outputs", summary)
    return summary


def emit_and_exit(summary: dict[str, Any]) -> int:
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("status") == "success" else 1

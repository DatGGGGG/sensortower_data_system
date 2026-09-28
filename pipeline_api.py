from __future__ import annotations

import argparse
import hmac
import json
import os
import subprocess
import threading
import time
import traceback
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from mapping_artifacts import MappingArtifactError, MappingArtifactRegistry

from pipeline_runtime import (
    SOURCE_BASE_STAGE_ORDER,
    STAGING_STAGE_ORDER,
    _source_base_outputs,
    _staging_current_outputs,
    _read_api_key,
    BASE_URL,
    create_context,
    lsdp,
    run_staging_pipeline,
    run_source_base_pipeline,
    validate_base_outputs,
    validate_inputs,
)


SOURCE_BASE_STAGE_ROUTE_MAP = {
    "fetch-top-game-snapshot": "fetch_top_game_snapshot",
    "update-top-game-history": "update_top_game_history",
    "materialize-game-list": "materialize_game_list",
    "build-game-full-info": "build_game_full_info",
    "materialize-app-list": "materialize_app_list",
    "build-app-full-info": "build_app_full_info",
    "build-app-performance": "build_app_performance",
    "build-app-active-users": "build_app_active_users",
    "build-app-retention": "build_app_retention",
    "build-user-demographics": "build_user_demographics",
    "build-user-reviews": "build_user_reviews",
}

STAGING_STAGE_ROUTE_MAP = {
    "publisher-mapping-top-down": "publisher_mapping_top_down",
    "publisher-revenue-mapping-full": "publisher_revenue_mapping_full",
    "app-full-info-adjusted": "app_full_info_adjusted_ndjson",
    "app-performance-daily-revenue-adjusted": "app_performance_daily_revenue_adjusted",
    "app-active-users-current-ndjson": "app_active_users_current_ndjson",
    "game-full-info-adjusted": "game_full_info_adjusted",
}

ASYNC_SOURCE_BASE_JOB_SPECS = {
    "build-app-performance": {
        "stage_name": "build_app_performance",
        "job_prefix": "app_performance_job_",
        "output_key": "app_performance",
    },
    "build-app-active-users": {
        "stage_name": "build_app_active_users",
        "job_prefix": "app_active_users_job_",
        "output_key": "app_active_users",
    },
    "build-app-retention": {
        "stage_name": "build_app_retention",
        "job_prefix": "app_retention_job_",
        "output_key": "app_retention",
    },
    "build-user-demographics": {
        "stage_name": "build_user_demographics",
        "job_prefix": "user_demographics_job_",
        "output_key": "user_demographics",
    },
}
ASYNC_STAGING_JOB_SPECS = {
    "app-performance-daily-revenue-adjusted": {
        "stage_name": "app_performance_daily_revenue_adjusted",
        "job_prefix": "staging_app_performance_revenue_adjusted_job_",
        "output_key": "app_performance_daily_revenue_adjusted",
    },
    "app-active-users-current-ndjson": {
        "stage_name": "app_active_users_current_ndjson",
        "job_prefix": "staging_app_active_users_current_ndjson_job_",
        "output_key": "app_active_users_current_ndjson",
    },
}
_JOB_LOCK = threading.Lock()
_JOB_THREADS: dict[str, threading.Thread] = {}
_ARTIFACT_JOB_THREADS: dict[str, threading.Thread] = {}


def _json_response(handler: BaseHTTPRequestHandler, status_code: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _csv_response(handler: BaseHTTPRequestHandler, artifact: dict[str, Any], content: bytes) -> None:
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", "text/csv; charset=utf-8")
    handler.send_header("Content-Length", str(len(content)))
    handler.send_header("Content-Disposition", f'attachment; filename="{artifact["canonical_filename"]}"')
    handler.send_header("X-Artifact-Id", artifact["id"])
    handler.send_header("X-Checksum-SHA256", artifact["checksum_sha256"])
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(content)


def _error_response(handler: BaseHTTPRequestHandler, error: MappingArtifactError) -> None:
    _json_response(
        handler,
        error.http_status,
        {"status": "failed", "error_code": error.code, "error_summary": str(error)},
    )


def _pipeline_data_root() -> Path:
    configured = os.environ.get("PIPELINE_DATA_ROOT", "").strip()
    return Path(configured).expanduser().resolve() if configured else (Path(__file__).resolve().parent / "data")


def _artifact_registry() -> MappingArtifactRegistry:
    registry = MappingArtifactRegistry(_pipeline_data_root())
    registry.initialize()
    return registry


def _repository_git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parent,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def _configured_service_token() -> str:
    return os.environ.get("PIPELINE_API_SERVICE_TOKEN", "").strip()


def _authorize(handler: BaseHTTPRequestHandler) -> MappingArtifactError | None:
    configured_token = _configured_service_token()
    if not configured_token:
        return MappingArtifactError(
            "service_not_configured",
            "PIPELINE_API_SERVICE_TOKEN is not configured.",
            HTTPStatus.SERVICE_UNAVAILABLE,
        )
    authorization = handler.headers.get("Authorization", "")
    if not authorization.lower().startswith("bearer "):
        return MappingArtifactError("unauthorized", "Missing bearer authentication.", HTTPStatus.UNAUTHORIZED)
    supplied_token = authorization[7:].strip()
    if not supplied_token or not hmac.compare_digest(supplied_token, configured_token):
        return MappingArtifactError("unauthorized", "Invalid bearer authentication.", HTTPStatus.UNAUTHORIZED)
    return None


def _read_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    content_length = int(handler.headers.get("Content-Length", "0") or "0")
    max_body_bytes = int(os.environ.get("PIPELINE_API_MAX_BODY_BYTES", str(10 * 1024 * 1024)))
    if content_length < 0 or content_length > max_body_bytes:
        raise MappingArtifactError(
            "request_too_large",
            f"Request body exceeds the {max_body_bytes}-byte limit.",
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
        )
    if content_length == 0:
        return {}
    raw_body = handler.rfile.read(content_length)
    if not raw_body:
        return {}
    return json.loads(raw_body.decode("utf-8"))


def _build_context(payload: dict[str, Any]) -> Any:
    args = SimpleNamespace(
        config_path=payload.get("config_path", "config.json"),
        input_root=payload.get("input_root", "data/base"),
        output_root=payload.get("output_root", "data"),
        run_id=payload.get("run_id"),
        run_timestamp=payload.get("run_timestamp"),
        trigger_mode=payload.get("trigger_mode", "manual"),
        performance_end_date=payload.get("performance_end_date"),
        facts_load_mode=payload.get("facts_load_mode"),
        facts_start_date=payload.get("facts_start_date") or payload.get("factsStartDate"),
        snapshot_start_year=payload.get("snapshot_start_year"),
        snapshot_end_year=payload.get("snapshot_end_year"),
        snapshot_regions=payload.get("snapshot_regions"),
        fact_country_codes=payload.get("fact_country_codes") or payload.get("factCountryCodes"),
        snapshot_category=payload.get("snapshot_category"),
        snapshot_custom_fields_filter_id=payload.get("snapshot_custom_fields_filter_id"),
        snapshot_data_model=payload.get("snapshot_data_model"),
        snapshot_target_count=payload.get("snapshot_target_count"),
        game_list_limit=payload.get("game_list_limit"),
        game_list_source=payload.get("game_list_source"),
        force_history_rebuild_from_seed=payload.get("force_history_rebuild_from_seed"),
        force_fail_stage=payload.get("force_fail_stage"),
        mapping_artifact_manifest=payload.get("mapping_artifact_manifest"),
        mapping_publisher_to_apps_top_down=payload.get("mapping_publisher_to_apps_top_down"),
        mapping_publisher_to_publisherids_revenue_multiplier_bottom_up=payload.get(
            "mapping_publisher_to_publisherids_revenue_multiplier_bottom_up"
        ),
        mapping_app_to_revenue_multiplier_special_cases=payload.get(
            "mapping_app_to_revenue_multiplier_special_cases"
        ),
        game_info_adjustment=payload.get("game_info_adjustment"),
    )
    return create_context(args)


def _job_log_root_for_context(ctx: Any) -> Path:
    job_dir = ctx.logs_root / ctx.run_id / "jobs"
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


def _job_status_path(job_dir: Path, job_prefix: str, job_id: str) -> Path:
    return job_dir / f"{job_prefix}{job_id}.json"


def _write_job_payload(path: Path, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _find_job_status_path(job_prefix: str, job_id: str) -> Path | None:
    logs_root = Path("data") / "logs"
    if not logs_root.exists():
        return None
    matches = list(logs_root.glob(f"*/jobs/{job_prefix}{job_id}.json"))
    if not matches:
        return None
    matches.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return matches[0]


def _build_job_state(
    *,
    job_id: str,
    stage_name: str,
    ctx: Any,
    status: str,
    created_at: float,
    started_at: float | None = None,
    finished_at: float | None = None,
    error_summary: str | None = None,
    result_file: str | None = None,
    output_file: str | None = None,
    output_file_size_bytes: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "job_id": job_id,
        "stage_name": stage_name,
        "run_id": ctx.run_id,
        "run_timestamp": ctx.run_timestamp,
        "trigger_mode": ctx.trigger_mode,
        "status": status,
        "created_at_epoch": created_at,
        "output_root": str(ctx.output_root),
    }
    if started_at is not None:
        payload["started_at_epoch"] = started_at
    if finished_at is not None:
        payload["finished_at_epoch"] = finished_at
    if error_summary:
        payload["error_summary"] = error_summary
    if result_file:
        payload["result_file"] = result_file
    if output_file:
        payload["output_file"] = output_file
    if output_file_size_bytes is not None:
        payload["output_file_size_bytes"] = output_file_size_bytes
    return payload


def _source_base_stage_output_path(ctx: Any, output_key: str) -> Path:
    return _source_base_outputs(ctx.output_root, ctx.run_timestamp)[output_key]


def _staging_stage_output_path(ctx: Any, output_key: str) -> Path:
    return _staging_current_outputs(ctx.output_root)[output_key]


def _run_source_base_job(job_spec: dict[str, str], job_id: str, payload: dict[str, Any]) -> None:
    ctx = _build_context(payload)
    created_at = time.time()
    job_dir = _job_log_root_for_context(ctx)
    status_path = _job_status_path(job_dir, job_spec["job_prefix"], job_id)
    output_path = _source_base_stage_output_path(ctx, job_spec["output_key"])
    started_at = time.time()
    _write_job_payload(
        status_path,
        _build_job_state(
            job_id=job_id,
            stage_name=job_spec["stage_name"],
            ctx=ctx,
            status="running",
            created_at=created_at,
            started_at=started_at,
            output_file=str(output_path),
            output_file_size_bytes=output_path.stat().st_size if output_path.exists() else 0,
        ),
    )
    try:
        summary = run_source_base_pipeline(ctx, stage=job_spec["stage_name"])
        finished_at = time.time()
        if summary.get("status") == "success":
            _write_job_payload(
                status_path,
                _build_job_state(
                    job_id=job_id,
                    stage_name=job_spec["stage_name"],
                    ctx=ctx,
                    status="success",
                    created_at=created_at,
                    started_at=started_at,
                    finished_at=finished_at,
                    result_file=summary.get("result_file"),
                    output_file=str(output_path),
                    output_file_size_bytes=output_path.stat().st_size if output_path.exists() else 0,
                ),
            )
        else:
            _write_job_payload(
                status_path,
                _build_job_state(
                    job_id=job_id,
                    stage_name=job_spec["stage_name"],
                    ctx=ctx,
                    status="failed",
                    created_at=created_at,
                    started_at=started_at,
                    finished_at=finished_at,
                    error_summary=summary.get("error_summary", "App performance job failed."),
                    result_file=summary.get("result_file"),
                    output_file=str(output_path),
                    output_file_size_bytes=output_path.stat().st_size if output_path.exists() else 0,
                ),
            )
    except Exception as exc:  # pragma: no cover - defensive async path
        finished_at = time.time()
        _write_job_payload(
            status_path,
            _build_job_state(
                job_id=job_id,
                stage_name=job_spec["stage_name"],
                ctx=ctx,
                status="failed",
                created_at=created_at,
                started_at=started_at,
                finished_at=finished_at,
                error_summary=str(exc),
                output_file=str(output_path),
                output_file_size_bytes=output_path.stat().st_size if output_path.exists() else 0,
            ),
        )
    finally:
        with _JOB_LOCK:
            _JOB_THREADS.pop(job_id, None)


def _route_start_source_base_job(route_stage_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    job_spec = ASYNC_SOURCE_BASE_JOB_SPECS[route_stage_name]
    ctx = _build_context(payload)
    created_at = time.time()
    job_dir = _job_log_root_for_context(ctx)
    job_id = uuid.uuid4().hex
    status_path = _job_status_path(job_dir, job_spec["job_prefix"], job_id)
    output_path = _source_base_stage_output_path(ctx, job_spec["output_key"])
    _write_job_payload(
        status_path,
        _build_job_state(
            job_id=job_id,
            stage_name=job_spec["stage_name"],
            ctx=ctx,
            status="queued",
            created_at=created_at,
            output_file=str(output_path),
            output_file_size_bytes=output_path.stat().st_size if output_path.exists() else 0,
        ),
    )
    thread = threading.Thread(
        target=_run_source_base_job,
        args=(job_spec, job_id, dict(payload)),
        name=f"{job_spec['stage_name']}-job-{job_id}",
        daemon=True,
    )
    with _JOB_LOCK:
        _JOB_THREADS[job_id] = thread
    thread.start()
    return {
        "job_id": job_id,
        "stage_name": job_spec["stage_name"],
        "run_id": ctx.run_id,
        "run_timestamp": ctx.run_timestamp,
        "status": "started",
        "status_file": str(status_path),
        "output_file": str(output_path),
    }


def _route_get_source_base_job_status(route_stage_name: str, job_id: str) -> dict[str, Any]:
    job_spec = ASYNC_SOURCE_BASE_JOB_SPECS[route_stage_name]
    status_path = _find_job_status_path(job_spec["job_prefix"], job_id)
    if status_path is None or not status_path.exists():
        raise ValueError(f"Unknown {job_spec['stage_name']} job_id: {job_id}")
    with open(status_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    output_file = payload.get("output_file")
    if output_file:
        output_path = Path(output_file)
        payload["output_file_exists"] = output_path.exists()
        payload["output_file_size_bytes"] = output_path.stat().st_size if output_path.exists() else 0
    payload["status_file"] = str(status_path)
    return payload


def _run_staging_job(job_spec: dict[str, str], job_id: str, payload: dict[str, Any]) -> None:
    ctx = _build_context(payload)
    created_at = time.time()
    job_dir = _job_log_root_for_context(ctx)
    status_path = _job_status_path(job_dir, job_spec["job_prefix"], job_id)
    output_path = _staging_stage_output_path(ctx, job_spec["output_key"])
    started_at = time.time()
    _write_job_payload(
        status_path,
        _build_job_state(
            job_id=job_id,
            stage_name=job_spec["stage_name"],
            ctx=ctx,
            status="running",
            created_at=created_at,
            started_at=started_at,
            output_file=str(output_path),
            output_file_size_bytes=output_path.stat().st_size if output_path.exists() else 0,
        ),
    )
    try:
        summary = run_staging_pipeline(ctx, stage=job_spec["stage_name"])
        finished_at = time.time()
        if summary.get("status") == "success":
            _write_job_payload(
                status_path,
                _build_job_state(
                    job_id=job_id,
                    stage_name=job_spec["stage_name"],
                    ctx=ctx,
                    status="success",
                    created_at=created_at,
                    started_at=started_at,
                    finished_at=finished_at,
                    result_file=summary.get("result_file"),
                    output_file=str(output_path),
                    output_file_size_bytes=output_path.stat().st_size if output_path.exists() else 0,
                ),
            )
        else:
            _write_job_payload(
                status_path,
                _build_job_state(
                    job_id=job_id,
                    stage_name=job_spec["stage_name"],
                    ctx=ctx,
                    status="failed",
                    created_at=created_at,
                    started_at=started_at,
                    finished_at=finished_at,
                    error_summary=summary.get("error_summary", "Staging job failed."),
                    result_file=summary.get("result_file"),
                    output_file=str(output_path),
                    output_file_size_bytes=output_path.stat().st_size if output_path.exists() else 0,
                ),
            )
    except Exception as exc:  # pragma: no cover - defensive async path
        finished_at = time.time()
        _write_job_payload(
            status_path,
            _build_job_state(
                job_id=job_id,
                stage_name=job_spec["stage_name"],
                ctx=ctx,
                status="failed",
                created_at=created_at,
                started_at=started_at,
                finished_at=finished_at,
                error_summary=str(exc),
                output_file=str(output_path),
                output_file_size_bytes=output_path.stat().st_size if output_path.exists() else 0,
            ),
        )
    finally:
        with _JOB_LOCK:
            _JOB_THREADS.pop(job_id, None)


def _route_start_staging_job(route_stage_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    job_spec = ASYNC_STAGING_JOB_SPECS[route_stage_name]
    ctx = _build_context(payload)
    created_at = time.time()
    job_dir = _job_log_root_for_context(ctx)
    job_id = uuid.uuid4().hex
    status_path = _job_status_path(job_dir, job_spec["job_prefix"], job_id)
    output_path = _staging_stage_output_path(ctx, job_spec["output_key"])
    _write_job_payload(
        status_path,
        _build_job_state(
            job_id=job_id,
            stage_name=job_spec["stage_name"],
            ctx=ctx,
            status="queued",
            created_at=created_at,
            output_file=str(output_path),
            output_file_size_bytes=output_path.stat().st_size if output_path.exists() else 0,
        ),
    )
    thread = threading.Thread(
        target=_run_staging_job,
        args=(job_spec, job_id, dict(payload)),
        name=f"{job_spec['stage_name']}-job-{job_id}",
        daemon=True,
    )
    with _JOB_LOCK:
        _JOB_THREADS[job_id] = thread
    thread.start()
    return {
        "job_id": job_id,
        "stage_name": job_spec["stage_name"],
        "run_id": ctx.run_id,
        "run_timestamp": ctx.run_timestamp,
        "status": "started",
        "status_file": str(status_path),
        "output_file": str(output_path),
    }


def _route_get_staging_job_status(route_stage_name: str, job_id: str) -> dict[str, Any]:
    job_spec = ASYNC_STAGING_JOB_SPECS[route_stage_name]
    status_path = _find_job_status_path(job_spec["job_prefix"], job_id)
    if status_path is None or not status_path.exists():
        raise ValueError(f"Unknown {job_spec['stage_name']} job_id: {job_id}")
    with open(status_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    output_file = payload.get("output_file")
    if output_file:
        output_path = Path(output_file)
        payload["output_file_exists"] = output_path.exists()
        payload["output_file_size_bytes"] = output_path.stat().st_size if output_path.exists() else 0
    payload["status_file"] = str(status_path)
    return payload


def _route_validate_inputs(payload: dict[str, Any]) -> dict[str, Any]:
    ctx = _build_context(payload)
    scope = payload.get("scope", "source_base")
    return validate_inputs(ctx, scope=scope)


def _route_source_base_stage(payload: dict[str, Any], route_stage_name: str) -> dict[str, Any]:
    ctx = _build_context(payload)
    internal_stage_name = SOURCE_BASE_STAGE_ROUTE_MAP[route_stage_name]
    return run_source_base_pipeline(ctx, stage=internal_stage_name)


def _route_validate_base_outputs(payload: dict[str, Any]) -> dict[str, Any]:
    ctx = _build_context(payload)
    return validate_base_outputs(ctx)


def _route_staging_stage(payload: dict[str, Any], route_stage_name: str) -> dict[str, Any]:
    ctx = _build_context(payload)
    internal_stage_name = STAGING_STAGE_ROUTE_MAP[route_stage_name]
    return run_staging_pipeline(ctx, stage=internal_stage_name)


def _required_text(payload: dict[str, Any], field: str) -> str:
    value = str(payload.get(field, "")).strip()
    if not value:
        raise MappingArtifactError("invalid_request", f"Missing required field: {field}", HTTPStatus.UNPROCESSABLE_ENTITY)
    return value


def _required_text_alias(payload: dict[str, Any], canonical_field: str, alias_field: str) -> str:
    value = str(payload.get(canonical_field) or payload.get(alias_field) or "").strip()
    if not value:
        raise MappingArtifactError("invalid_request", f"Missing required field: {canonical_field}", HTTPStatus.UNPROCESSABLE_ENTITY)
    return value


def _route_publish_mapping_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    csv_content = payload.get("csv_content")
    if not isinstance(csv_content, str):
        raise MappingArtifactError("invalid_request", "csv_content must be a UTF-8 string.", HTTPStatus.UNPROCESSABLE_ENTITY)
    artifact = _artifact_registry().publish(
        artifact_type=_required_text(payload, "artifact_type"),
        content=csv_content.encode("utf-8"),
        supplied_checksum=_required_text(payload, "checksum_sha256"),
        workflow=_required_text_alias(payload, "source_workbench_workflow", "workflow"),
        version_id=_required_text_alias(payload, "source_workbench_version_id", "version_id"),
        version_label=str(
            payload.get("source_workbench_version_label") or payload.get("version_label") or ""
        ).strip() or None,
        created_by=_required_text(payload, "created_by"),
        repository_git_sha=_repository_git_sha(),
        source_artifact_ids=payload.get("source_artifact_ids", []),
        source_artifact_checksums=payload.get("source_artifact_checksums") or {},
        provenance=payload.get("provenance") or {},
    )
    return {"status": "success", "artifact": artifact}


def _run_top_down_artifact_job(job_id: str, request: dict[str, Any]) -> None:
    registry = _artifact_registry()
    registry.start_job(job_id)
    try:
        artifact = registry.generate_top_down_with_publisher_ids(
            source_artifact_id=request["source_artifact_id"],
            created_by=request["created_by"],
            api_key=_read_api_key(Path(__file__).resolve().parent),
            base_url=BASE_URL,
            generator_version="top_down_publisher_ids_v1",
            repository_git_sha=_repository_git_sha(),
            enrich=lsdp.create_mapping_publisher_to_apps_publisherids,
        )
    except MappingArtifactError as exc:
        registry.fail_job(job_id, exc.code, str(exc))
    except Exception as exc:  # pragma: no cover - defensive worker boundary
        registry.fail_job(job_id, "generation_failed", str(exc))
    else:
        registry.finish_job(job_id, artifact["id"])
    finally:
        with _JOB_LOCK:
            _ARTIFACT_JOB_THREADS.pop(job_id, None)


def _route_generate_top_down_publisher_ids(payload: dict[str, Any]) -> dict[str, Any]:
    request = {
        "source_artifact_id": _required_text(payload, "source_artifact_id"),
        "created_by": _required_text(payload, "created_by"),
    }
    registry = _artifact_registry()
    registry._require_committed(request["source_artifact_id"], "top_down_raw")
    existing = registry.get_running_job("top_down_publisher_ids", request)
    if existing:
        return {"status": "running", "job": existing, "idempotent": True}
    job = registry.create_job("top_down_publisher_ids", request)
    thread = threading.Thread(
        target=_run_top_down_artifact_job,
        args=(job["id"], request),
        name=f"mapping-artifact-{job['id']}",
        daemon=True,
    )
    with _JOB_LOCK:
        _ARTIFACT_JOB_THREADS[job["id"]] = thread
    thread.start()
    return {"status": "queued", "job": job, "idempotent": False}


def _route_generate_full_mapping(payload: dict[str, Any]) -> dict[str, Any]:
    artifact = _artifact_registry().generate_full(
        top_down_artifact_id=_required_text(payload, "top_down_artifact_id"),
        bottom_up_artifact_id=_required_text(payload, "bottom_up_artifact_id"),
        created_by=_required_text(payload, "created_by"),
        generator_version="full_mapping_v1",
        repository_git_sha=_repository_git_sha(),
        merge=lsdp.create_mapping_publisher_to_publisherids_revenue_multiplier,
    )
    return {"status": "success", "artifact": artifact}


def _route_commit_mapping_artifact(artifact_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    artifact = _artifact_registry().commit(artifact_id, _required_text(payload, "committed_by"))
    return {"status": "success", "artifact": artifact}


def _route_activate_mapping_artifact(artifact_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    active = _artifact_registry().activate(artifact_id, _required_text(payload, "activated_by"))
    return {"status": "success", "active": active}


class PipelineApiHandler(BaseHTTPRequestHandler):
    server_version = "SensorTowerPipelineAPI/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            query = parse_qs(parsed.query)
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "status": "success",
                    "service": "sensor-tower-pipeline-api",
                    "available_source_base_stages": list(SOURCE_BASE_STAGE_ORDER),
                    "available_staging_stages": list(STAGING_STAGE_ORDER),
                    "available_async_source_base_jobs": list(ASYNC_SOURCE_BASE_JOB_SPECS.keys()),
                    "available_async_staging_jobs": list(ASYNC_STAGING_JOB_SPECS.keys()),
                    "requested_query": query,
                },
            )
            return

        authorization_error = _authorize(self)
        if authorization_error:
            _error_response(self, authorization_error)
            return

        try:
            if parsed.path == "/mapping-artifacts":
                query = parse_qs(parsed.query)
                artifacts = _artifact_registry().list_artifacts(
                    artifact_type=(query.get("type") or query.get("artifact_type") or [None])[0],
                    status=(query.get("status") or [None])[0],
                )
                _json_response(self, HTTPStatus.OK, {"status": "success", "artifacts": artifacts})
                return
            if parsed.path == "/mapping-artifacts/active":
                _json_response(self, HTTPStatus.OK, {"status": "success", "active": _artifact_registry().get_active()})
                return
            if parsed.path == "/mapping-artifacts/activation-history":
                query = parse_qs(parsed.query)
                events = _artifact_registry().list_activation_events(
                    artifact_type=(query.get("type") or query.get("artifact_type") or [None])[0]
                )
                _json_response(self, HTTPStatus.OK, {"status": "success", "events": events})
                return
            if parsed.path.startswith("/mapping-artifacts/jobs/"):
                job_id = parsed.path.removeprefix("/mapping-artifacts/jobs/")
                _json_response(self, HTTPStatus.OK, {"status": "success", "job": _artifact_registry().get_job(job_id)})
                return
            if parsed.path.startswith("/mapping-artifacts/"):
                suffix = parsed.path.removeprefix("/mapping-artifacts/")
                if suffix.endswith("/download"):
                    artifact_id = suffix.removesuffix("/download")
                    artifact_file = _artifact_registry().read_artifact(artifact_id)
                    _csv_response(self, artifact_file.artifact, artifact_file.content)
                    return
                if "/" not in suffix:
                    _json_response(
                        self,
                        HTTPStatus.OK,
                        {"status": "success", "artifact": _artifact_registry().get_artifact(suffix)},
                    )
                    return
        except MappingArtifactError as exc:
            _error_response(self, exc)
            return

        for route_stage_name in ASYNC_SOURCE_BASE_JOB_SPECS:
            if parsed.path == f"/source-base/{route_stage_name}/status":
                query = parse_qs(parsed.query)
                job_id = (query.get("job_id") or [None])[0]
                if not job_id:
                    _json_response(
                        self,
                        HTTPStatus.BAD_REQUEST,
                        {"status": "failed", "error_summary": "Missing required query parameter: job_id"},
                    )
                    return
                try:
                    summary = _route_get_source_base_job_status(route_stage_name, job_id)
                except ValueError as exc:
                    _json_response(
                        self,
                        HTTPStatus.NOT_FOUND,
                        {"status": "failed", "error_summary": str(exc)},
                    )
                    return
                _json_response(self, HTTPStatus.OK, summary)
                return

        for route_stage_name in ASYNC_STAGING_JOB_SPECS:
            if parsed.path == f"/base-staging/{route_stage_name}/status":
                query = parse_qs(parsed.query)
                job_id = (query.get("job_id") or [None])[0]
                if not job_id:
                    _json_response(
                        self,
                        HTTPStatus.BAD_REQUEST,
                        {"status": "failed", "error_summary": "Missing required query parameter: job_id"},
                    )
                    return
                try:
                    summary = _route_get_staging_job_status(route_stage_name, job_id)
                except ValueError as exc:
                    _json_response(
                        self,
                        HTTPStatus.NOT_FOUND,
                        {"status": "failed", "error_summary": str(exc)},
                    )
                    return
                _json_response(self, HTTPStatus.OK, summary)
                return

        _json_response(
            self,
            HTTPStatus.NOT_FOUND,
            {"status": "failed", "error_summary": f"Unknown route: {parsed.path}"},
        )

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        authorization_error = _authorize(self)
        if authorization_error:
            _error_response(self, authorization_error)
            return
        try:
            payload = _read_json_body(self)
        except MappingArtifactError as exc:
            _error_response(self, exc)
            return
        except (json.JSONDecodeError, ValueError) as exc:
            _json_response(
                self,
                HTTPStatus.BAD_REQUEST,
                {"status": "failed", "error_code": "invalid_request", "error_summary": f"Invalid JSON body: {exc}"},
            )
            return

        try:
            response_status = HTTPStatus.OK
            if parsed.path == "/mapping-artifacts/publish":
                summary = _route_publish_mapping_artifact(payload)
                response_status = HTTPStatus.CREATED
            elif parsed.path == "/mapping-artifacts/generate/top-down-publisher-ids":
                summary = _route_generate_top_down_publisher_ids(payload)
                response_status = HTTPStatus.ACCEPTED
            elif parsed.path == "/mapping-artifacts/generate/full":
                summary = _route_generate_full_mapping(payload)
                response_status = HTTPStatus.CREATED
            elif parsed.path.startswith("/mapping-artifacts/") and parsed.path.endswith("/commit"):
                artifact_id = parsed.path.removeprefix("/mapping-artifacts/").removesuffix("/commit")
                summary = _route_commit_mapping_artifact(artifact_id, payload)
            elif parsed.path.startswith("/mapping-artifacts/") and parsed.path.endswith("/activate"):
                artifact_id = parsed.path.removeprefix("/mapping-artifacts/").removesuffix("/activate")
                summary = _route_activate_mapping_artifact(artifact_id, payload)
            else:
                summary = self._dispatch(parsed.path, payload)
        except MappingArtifactError as exc:
            _error_response(self, exc)
            return
        except ValueError as exc:
            _json_response(
                self,
                HTTPStatus.BAD_REQUEST,
                {"status": "failed", "error_code": "invalid_request", "error_summary": str(exc)},
            )
            return
        except Exception as exc:  # pragma: no cover - defensive API surface
            request_id = uuid.uuid4().hex
            _json_response(
                self,
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "status": "failed",
                    "error_code": "internal_error",
                    "error_summary": "Unexpected server error. Consult service logs with the request ID.",
                    "request_id": request_id,
                },
            )
            return

        _json_response(self, response_status, summary)

    def _dispatch(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        route_handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
            "/validate-inputs": _route_validate_inputs,
            "/validate-base-outputs": _route_validate_base_outputs,
        }
        for route_stage_name in ASYNC_SOURCE_BASE_JOB_SPECS:
            route_handlers[f"/source-base/{route_stage_name}/start"] = (
                lambda payload, route_stage_name=route_stage_name: _route_start_source_base_job(route_stage_name, payload)
            )
        for route_stage_name in ASYNC_STAGING_JOB_SPECS:
            route_handlers[f"/base-staging/{route_stage_name}/start"] = (
                lambda payload, route_stage_name=route_stage_name: _route_start_staging_job(route_stage_name, payload)
            )
        if path in route_handlers:
            return route_handlers[path](payload)

        stage_prefix = "/source-base/"
        if path.startswith(stage_prefix):
            route_stage_name = path[len(stage_prefix):]
            if route_stage_name not in SOURCE_BASE_STAGE_ROUTE_MAP:
                raise ValueError(f"Unknown source-base stage route: {route_stage_name}")
            return _route_source_base_stage(payload, route_stage_name)

        staging_prefix = "/base-staging/"
        if path.startswith(staging_prefix):
            route_stage_name = path[len(staging_prefix):]
            if route_stage_name not in STAGING_STAGE_ROUTE_MAP:
                raise ValueError(f"Unknown base-staging stage route: {route_stage_name}")
            return _route_staging_stage(payload, route_stage_name)

        raise ValueError(f"Unknown route: {path}")

    def log_message(self, format: str, *args: Any) -> None:
        return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local HTTP API for Sensor Tower pipeline stages.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    interrupted_jobs = _artifact_registry().recover_interrupted_jobs()
    if interrupted_jobs:
        print(f"Marked {interrupted_jobs} interrupted mapping-artifact jobs as failed.", flush=True)
    server = ThreadingHTTPServer((args.host, args.port), PipelineApiHandler)
    print(f"Sensor Tower pipeline API listening on http://{args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

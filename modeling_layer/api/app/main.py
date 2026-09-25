from __future__ import annotations

from datetime import date
import time
from typing import Any

import psycopg
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .charts import (
    ChartArtifactAccessError,
    ChartValidationError,
    RenderChartRequest,
    load_chart_artifact,
    render_chart_artifact,
)
from .catalog import CATALOG, allowed_catalog_objects, enrich_catalog_columns
from .config import get_settings
from .db import (
    execute_statement,
    fetch_all,
    fetch_catalog_columns,
    fetch_one,
    ping_database,
    run_read_only_query,
    validate_read_only_query,
)
from .query_guard import QueryValidationError, validate_read_only_sql
from .openapi_models import (
    AppDetailResponse,
    AppListResponse,
    AppPerformanceResponse,
    CatalogResponse,
    ErrorResponse,
    GameDetailResponse,
    GameListResponse,
    GameSearchResponse,
)
from .security import require_api_key

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "SensorTower mobile market-data warehouse API. Use the documented game, app, "
        "performance, and catalog reads for Portal-facing data access."
    ),
)
protected = APIRouter(dependencies=[Depends(require_api_key)])

PORTAL_PUBLIC_OPENAPI = {"x-portal-public": True}
PUBLIC_READ_ERRORS = {
    401: {"model": ErrorResponse, "description": "Missing or invalid X-API-Key."},
    403: {"model": ErrorResponse, "description": "The authenticated caller is not authorized."},
    500: {"model": ErrorResponse, "description": "Unexpected warehouse service failure."},
}
PUBLIC_DETAIL_ERRORS = {
    **PUBLIC_READ_ERRORS,
    404: {"model": ErrorResponse, "description": "The requested game or app was not found."},
    408: {"model": ErrorResponse, "description": "The warehouse query timed out."},
}


def _public_responses(success_description: str, example: dict[str, Any], *, detail: bool = False) -> dict[int, Any]:
    return {
        200: {
            "description": success_description,
            "content": {"application/json": {"example": example}},
        },
        **(PUBLIC_DETAIL_ERRORS if detail else PUBLIC_READ_ERRORS),
    }


class QueryRequest(BaseModel):
    sql: str = Field(description="Read-only SQL against approved analytics objects.")
    max_rows: int | None = Field(default=None, ge=1, le=500)


def _safe_log_api_request(
    *,
    request: Request,
    status_code: int,
    duration_ms: float,
    error_code: str | None = None,
) -> None:
    key_record = getattr(request.state, "api_key_record", None)
    query_result = getattr(request.state, "query_result_metadata", {}) or {}
    try:
        execute_statement(
            """
            INSERT INTO api_access.api_request_logs (
                api_key_id,
                api_key_name,
                key_prefix,
                method,
                path,
                status_code,
                duration_ms,
                row_count,
                truncated,
                error_code
            )
            VALUES (
                %(api_key_id)s,
                %(api_key_name)s,
                %(key_prefix)s,
                %(method)s,
                %(path)s,
                %(status_code)s,
                %(duration_ms)s,
                %(row_count)s,
                %(truncated)s,
                %(error_code)s
            )
            """,
            {
                "api_key_id": key_record.get("id") if key_record else None,
                "api_key_name": key_record.get("name") if key_record else None,
                "key_prefix": key_record.get("key_prefix") if key_record else None,
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "duration_ms": round(duration_ms, 2),
                "row_count": query_result.get("row_count"),
                "truncated": query_result.get("truncated"),
                "error_code": error_code,
            },
        )
    except Exception:
        # Usage logging must never break the read-only data API.
        return


@app.middleware("http")
async def log_api_usage(request: Request, call_next):  # type: ignore[no-untyped-def]
    started_at = time.perf_counter()
    status_code = 500
    error_code = None
    try:
        response = await call_next(request)
        status_code = response.status_code
        if status_code >= 400:
            error_code = f"http_{status_code}"
        return response
    except Exception:
        error_code = "unhandled_exception"
        raise
    finally:
        if request.url.path != "/health":
            duration_ms = (time.perf_counter() - started_at) * 1000
            _safe_log_api_request(
                request=request,
                status_code=status_code,
                duration_ms=duration_ms,
                error_code=error_code,
            )


def explain_database_error(exc: psycopg.Error) -> tuple[int, str]:
    if isinstance(exc, (psycopg.errors.InvalidSchemaName, psycopg.errors.UndefinedTable)):
        return (
            503,
            "Database schema is not ready. Run the SQL schema and load scripts before using data endpoints.",
        )
    if isinstance(exc, psycopg.errors.QueryCanceled):
        return 408, "Database query timed out. Narrow the filters and try again."
    return 500, f"Database query failed: {exc}"


@app.exception_handler(psycopg.Error)
def handle_database_error(_: Request, exc: psycopg.Error) -> JSONResponse:
    status_code, detail = explain_database_error(exc)
    return JSONResponse(status_code=status_code, content={"detail": detail})


def normalize_limit(limit: int | None) -> int:
    value = limit or settings.default_limit
    return max(1, min(value, settings.max_limit))


def normalize_query_row_limit(limit: int | None) -> int:
    value = limit or settings.query_default_rows
    return max(1, min(value, settings.query_max_rows))


def build_search_pattern(q: str | None) -> str | None:
    if q is None:
        return None
    cleaned = q.strip()
    return f"%{cleaned}%" if cleaned else None


def custom_openapi() -> dict[str, Any]:
    """Expose a stable, deployment-derived Portal contract without runtime settings."""
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=settings.app_name,
        version=settings.app_version,
        description=app.description,
        routes=app.routes,
    )
    schema["info"].update(
        {
            "x-portal-source": "sensortower",
            "x-portal-contract-version": 1,
        }
    )
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "auth": {"header": "X-API-Key", "protected_endpoints": "all data endpoints plus /charts/render"},
        "endpoints": [
            "/health",
            "/games/search",
            "/games",
            "/games/{unified_app_id}",
            "/meta/catalog",
            "/query",
            "/query/validate",
            "/charts/render",
            "/charts/artifacts/{artifact_id}",
            "/apps",
            "/apps/{app_id}",
            "/apps/{app_id}/performance",
            "/steam/games",
            "/steam/games/{app_id}",
            "/steam/games/{app_id}/performance",
        ],
    }


@app.get("/health")
def health() -> dict[str, str]:
    try:
        ping_database()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}") from exc

    return {"status": "ok", "database": "ok"}


@protected.get(
    "/games/search",
    response_model=GameSearchResponse,
    tags=["SensorTower games"],
    summary="Search SensorTower games",
    description="Find game clusters by display name or canonical app ID. Results are paginated with limit and offset.",
    operation_id="searchSensorTowerGames",
    openapi_extra=PORTAL_PUBLIC_OPENAPI,
    responses=_public_responses(
        "Matching SensorTower game clusters.",
        {"items": [{"unified_app_id": "game-123", "game_name": "Example Game", "canonical_app_id": "com.example.game"}], "limit": 20, "offset": 0},
    ),
)
def search_games(
    q: str = Query(description="Required case-insensitive text to match against game name or canonical app ID."),
    limit: int | None = Query(default=20, ge=1, le=200, description="Maximum matches to return; defaults to 20."),
    offset: int = Query(default=0, ge=0, description="Number of matching rows to skip for pagination."),
) -> dict[str, Any]:
    query = """
        SELECT
            g.unified_app_id,
            g.name AS game_name,
            g.canonical_app_id,
            g.game_class,
            g.game_genre,
            g.game_subgenre
        FROM core.dim_game_info AS g
        WHERE g.name ILIKE CAST(%(pattern)s AS TEXT)
           OR g.canonical_app_id ILIKE CAST(%(pattern)s AS TEXT)
        ORDER BY g.name NULLS LAST, g.unified_app_id
        LIMIT %(limit)s
        OFFSET %(offset)s
    """
    items = fetch_all(
        query,
        {
            "pattern": build_search_pattern(q),
            "limit": normalize_limit(limit),
            "offset": offset,
        },
    )
    return {"items": items, "limit": normalize_limit(limit), "offset": offset}


@protected.get(
    "/games",
    response_model=GameListResponse,
    tags=["SensorTower games"],
    summary="List SensorTower game clusters",
    description="List game clusters with optional text search. Use limit and offset for deterministic pagination.",
    operation_id="listSensorTowerGames",
    openapi_extra=PORTAL_PUBLIC_OPENAPI,
    responses=_public_responses(
        "A page of SensorTower game clusters.",
        {"items": [{"unified_app_id": "game-123", "name": "Example Game", "linked_app_count": 2}], "limit": 50, "offset": 0},
    ),
)
def list_games(
    q: str | None = Query(default=None, description="Search by game name or canonical app id."),
    limit: int | None = Query(default=None, ge=1, le=500, description="Maximum rows to return; defaults to the service limit."),
    offset: int = Query(default=0, ge=0, description="Number of matching rows to skip for pagination."),
) -> dict[str, Any]:
    query = """
        SELECT
            g.unified_app_id,
            g.canonical_app_id,
            g.name,
            g.game_class,
            g.game_genre,
            g.game_subgenre,
            g.game_release_date_ww,
            COUNT(a.app_id) AS linked_app_count
        FROM core.dim_game_info AS g
        LEFT JOIN core.dim_app_info AS a
          ON a.unified_app_id = g.unified_app_id
        WHERE (
            CAST(%(pattern)s AS TEXT) IS NULL
            OR g.name ILIKE CAST(%(pattern)s AS TEXT)
            OR g.canonical_app_id ILIKE CAST(%(pattern)s AS TEXT)
        )
        GROUP BY
            g.unified_app_id,
            g.canonical_app_id,
            g.name,
            g.game_class,
            g.game_genre,
            g.game_subgenre,
            g.game_release_date_ww
        ORDER BY g.name NULLS LAST, g.unified_app_id
        LIMIT %(limit)s
        OFFSET %(offset)s
    """
    items = fetch_all(
        query,
        {
            "pattern": build_search_pattern(q),
            "limit": normalize_limit(limit),
            "offset": offset,
        },
    )
    return {"items": items, "limit": normalize_limit(limit), "offset": offset}


@protected.get(
    "/meta/catalog",
    response_model=CatalogResponse,
    tags=["SensorTower metadata"],
    summary="Get the SensorTower analytics catalog",
    description="Returns approved analytics objects, columns, grain, semantics, and safe SQL examples for query construction.",
    operation_id="getSensorTowerCatalog",
    openapi_extra=PORTAL_PUBLIC_OPENAPI,
    responses=_public_responses(
        "The approved SensorTower analytics catalog.",
        {"objects": [{"name": "analytics.agg_game_performance_monthly", "object_type": "table", "grain": "game + country + month", "description": "Monthly game performance.", "columns": [], "examples": []}]},
    ),
)
def get_catalog() -> dict[str, Any]:
    metadata_rows = fetch_catalog_columns(list(allowed_catalog_objects()))
    grouped_columns: dict[str, list[dict[str, Any]]] = {}
    object_types: dict[str, str] = {}

    for row in metadata_rows:
        object_name = row["object_name"]
        object_types[object_name] = row["object_type"]
        grouped_columns.setdefault(object_name, []).append(
            {
                "name": row["column_name"],
                "data_type": row["data_type"],
                "nullable": row["is_nullable"] is True or row["is_nullable"] == "YES",
            }
        )

    objects: list[dict[str, Any]] = []
    for object_name, entry in CATALOG.items():
        objects.append(
            {
                "name": entry.name,
                "object_type": object_types.get(object_name, "unknown"),
                "grain": entry.grain,
                "description": entry.description,
                "columns": enrich_catalog_columns(object_name, grouped_columns.get(object_name, [])),
                "examples": list(entry.examples),
            }
        )

    return {"objects": objects}


@protected.post("/query")
def query_analytics(http_request: Request, request: QueryRequest) -> dict[str, Any]:
    try:
        validated = validate_read_only_sql(request.sql, set(allowed_catalog_objects()))
    except QueryValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result = run_read_only_query(
        validated.sql,
        normalize_query_row_limit(request.max_rows),
        settings.query_timeout_ms,
    )
    result["referenced_objects"] = list(validated.referenced_objects)
    result["max_rows"] = normalize_query_row_limit(request.max_rows)
    http_request.state.query_result_metadata = {
        "row_count": result.get("row_count"),
        "truncated": result.get("truncated"),
    }
    return result


def _validation_database_error(exc: psycopg.Error) -> tuple[int, dict[str, Any]]:
    diagnostic = getattr(exc, "diag", None)
    if isinstance(exc, psycopg.errors.QueryCanceled):
        status_code, code, message, retryable = (
            408,
            "QUERY_TIMEOUT",
            "Validation timed out. Narrow the query.",
            True,
        )
    elif isinstance(exc, psycopg.errors.SyntaxError):
        status_code, code, message, retryable = 400, "SQL_SYNTAX_ERROR", str(exc).split("\n", 1)[0], False
    elif isinstance(exc, psycopg.errors.UndefinedColumn):
        status_code, code, message, retryable = 400, "UNKNOWN_COLUMN", str(exc).split("\n", 1)[0], False
    elif isinstance(exc, psycopg.errors.GroupingError):
        status_code, code, message, retryable = 400, "INVALID_GROUPING", str(exc).split("\n", 1)[0], False
    elif isinstance(exc, (psycopg.errors.InvalidSchemaName, psycopg.errors.UndefinedTable)):
        status_code, code, message, retryable = (
            503,
            "WAREHOUSE_OBJECT_UNAVAILABLE",
            str(exc).split("\n", 1)[0],
            True,
        )
    else:
        status_code, code, message, retryable = 400, "WAREHOUSE_QUERY_REJECTED", str(exc).split("\n", 1)[0], False
    return status_code, {
        "error": {
            "code": code,
            "message": message,
            "hint": getattr(diagnostic, "message_hint", None),
            "position": getattr(diagnostic, "statement_position", None),
            "retryable": retryable,
        }
    }


@protected.post("/query/validate")
def validate_query_analytics(http_request: Request, request: QueryRequest) -> dict[str, Any]:
    try:
        validated = validate_read_only_sql(request.sql, set(allowed_catalog_objects()))
        plan = validate_read_only_query(validated.sql, 2_000)
    except QueryValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error": {
                    "code": "PORTAL_QUERY_REJECTED",
                    "message": str(exc),
                    "retryable": False,
                }
            },
        ) from exc
    except psycopg.Error as exc:
        status_code, detail = _validation_database_error(exc)
        raise HTTPException(status_code=status_code, detail=detail) from exc

    return {
        "valid": True,
        "normalized_sql": validated.sql,
        "referenced_objects": list(validated.referenced_objects),
        "warnings": [],
        **plan,
        "request_id": getattr(http_request.state, "request_id", None),
    }


@protected.post("/charts/render")
def render_chart(http_request: Request, request: RenderChartRequest) -> dict[str, Any]:
    try:
        return render_chart_artifact(
            request,
            settings,
            str(http_request.base_url).rstrip("/"),
        )
    except ChartValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/charts/artifacts/{artifact_id}")
def get_chart_artifact(artifact_id: str, token: str = Query(..., min_length=8)) -> FileResponse:
    try:
        artifact_path = load_chart_artifact(artifact_id, token, settings)
    except ChartArtifactAccessError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    return FileResponse(
        artifact_path,
        media_type="text/html",
        headers={"Cache-Control": "private, no-store"},
    )


@protected.get(
    "/games/{unified_app_id}",
    response_model=GameDetailResponse,
    tags=["SensorTower games"],
    summary="Get one SensorTower game cluster",
    description="Returns game taxonomy, release metadata, and the number of linked mobile apps for one unified game ID.",
    operation_id="getSensorTowerGame",
    openapi_extra=PORTAL_PUBLIC_OPENAPI,
    responses=_public_responses(
        "The requested SensorTower game cluster.",
        {"unified_app_id": "game-123", "name": "Example Game", "linked_app_count": 2},
        detail=True,
    ),
)
def get_game(unified_app_id: str = Path(description="Unified SensorTower game-cluster identifier.")) -> dict[str, Any]:
    query = """
        SELECT
            g.*,
            COUNT(a.app_id) AS linked_app_count
        FROM core.dim_game_info AS g
        LEFT JOIN core.dim_app_info AS a
          ON a.unified_app_id = g.unified_app_id
        WHERE g.unified_app_id = %(unified_app_id)s
        GROUP BY
            g.unified_app_id,
            g.canonical_app_id,
            g.name,
            g.cohort_id,
            g.itunes_apps,
            g.android_apps,
            g.unified_publisher_ids,
            g.itunes_publisher_ids,
            g.android_publisher_ids,
            g.game_class,
            g.game_genre,
            g.game_subgenre,
            g.game_art_style,
            g.game_camera_pov,
            g.game_setting,
            g.game_theme,
            g.game_product_model,
            g.game_ip_corporate_parent,
            g.game_ip_operator,
            g.game_ip_media_type,
            g.game_licensed_ip,
            g.game_earliest_release_date,
            g.game_release_date_ww,
            g.game_release_date_us,
            g.game_release_date_jp,
            g.game_release_date_cn
    """
    game = fetch_one(query, {"unified_app_id": unified_app_id})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    return game


@protected.get(
    "/apps",
    response_model=AppListResponse,
    tags=["SensorTower apps"],
    summary="List SensorTower mobile apps",
    description="List mobile apps with optional name, publisher, platform, and unified-game filters.",
    operation_id="listSensorTowerApps",
    openapi_extra=PORTAL_PUBLIC_OPENAPI,
    responses=_public_responses(
        "A page of SensorTower mobile apps.",
        {"items": [{"app_id": "com.example.game", "name": "Example Game", "os": "android", "unified_app_id": "game-123"}], "limit": 50, "offset": 0},
    ),
)
def list_apps(
    q: str | None = Query(default=None, description="Search by app name, publisher, or app id."),
    os: str | None = Query(default=None, description="Filter by app operating system, for example ios or android."),
    unified_app_id: str | None = Query(default=None, description="Filter to one game cluster."),
    limit: int | None = Query(default=None, ge=1, le=500, description="Maximum rows to return; defaults to the service limit."),
    offset: int = Query(default=0, ge=0, description="Number of matching rows to skip for pagination."),
) -> dict[str, Any]:
    query = """
        SELECT
            a.app_id,
            a.name,
            a.publisher_name,
            a.os,
            a.active,
            a.price,
            a.rating,
            a.global_rating_count,
            a.unified_app_id,
            g.name AS game_name,
            g.game_genre,
            g.game_class
        FROM core.dim_app_info AS a
        LEFT JOIN core.dim_game_info AS g
          ON g.unified_app_id = a.unified_app_id
        WHERE (
            CAST(%(pattern)s AS TEXT) IS NULL
            OR a.name ILIKE CAST(%(pattern)s AS TEXT)
            OR a.publisher_name ILIKE CAST(%(pattern)s AS TEXT)
            OR a.app_id ILIKE CAST(%(pattern)s AS TEXT)
        )
          AND (CAST(%(os)s AS TEXT) IS NULL OR a.os = CAST(%(os)s AS TEXT))
          AND (
              CAST(%(unified_app_id)s AS TEXT) IS NULL
              OR a.unified_app_id = CAST(%(unified_app_id)s AS TEXT)
          )
        ORDER BY a.name NULLS LAST, a.app_id
        LIMIT %(limit)s
        OFFSET %(offset)s
    """
    items = fetch_all(
        query,
        {
            "pattern": build_search_pattern(q),
            "os": os,
            "unified_app_id": unified_app_id,
            "limit": normalize_limit(limit),
            "offset": offset,
        },
    )
    return {"items": items, "limit": normalize_limit(limit), "offset": offset}


@protected.get(
    "/apps/{app_id}",
    response_model=AppDetailResponse,
    tags=["SensorTower apps"],
    summary="Get one SensorTower mobile app",
    description="Returns app metadata and linked game taxonomy for one SensorTower app ID.",
    operation_id="getSensorTowerApp",
    openapi_extra=PORTAL_PUBLIC_OPENAPI,
    responses=_public_responses(
        "The requested SensorTower mobile app.",
        {"app_id": "com.example.game", "name": "Example Game", "os": "android", "unified_app_id": "game-123"},
        detail=True,
    ),
)
def get_app(app_id: str = Path(description="Android package name or iOS App Store ID.")) -> dict[str, Any]:
    query = """
        SELECT
            a.*,
            g.name AS game_name,
            g.game_class,
            g.game_genre,
            g.game_subgenre
        FROM core.dim_app_info AS a
        LEFT JOIN core.dim_game_info AS g
          ON g.unified_app_id = a.unified_app_id
        WHERE a.app_id = %(app_id)s
    """
    app_row = fetch_one(query, {"app_id": app_id})
    if not app_row:
        raise HTTPException(status_code=404, detail="App not found")
    return app_row


@protected.get(
    "/apps/{app_id}/performance",
    response_model=AppPerformanceResponse,
    tags=["SensorTower performance"],
    summary="Get daily app downloads and revenue",
    description="Returns daily platform-specific download and revenue values for one mobile app, optionally constrained by date and country.",
    operation_id="getSensorTowerAppPerformance",
    openapi_extra=PORTAL_PUBLIC_OPENAPI,
    responses=_public_responses(
        "Daily performance rows for the requested mobile app.",
        {"app": {"app_id": "com.example.game", "name": "Example Game"}, "items": [{"app_id": "com.example.game", "date": "2026-09-01", "country_android": "VN", "downloads_android": 120, "revenue_android": 5000}], "count": 1, "limit": 100},
        detail=True,
    ),
)
def get_app_performance(
    app_id: str = Path(description="Android package name or iOS App Store ID."),
    start_date: date | None = Query(default=None, description="Inclusive UTC calendar date; omit for the earliest available date."),
    end_date: date | None = Query(default=None, description="Inclusive UTC calendar date; omit for the latest available date."),
    country: str | None = Query(default=None, description="Matches either country_android or country_ios."),
    limit: int | None = Query(default=100, ge=1, le=1000, description="Maximum daily rows to return."),
) -> dict[str, Any]:
    app_exists = fetch_one(
        "SELECT app_id, name FROM core.dim_app_info WHERE app_id = %(app_id)s",
        {"app_id": app_id},
    )
    if not app_exists:
        raise HTTPException(status_code=404, detail="App not found")

    query = """
        SELECT
            app_id,
            country_android,
            country_ios,
            date,
            downloads_android,
            downloads_iphone,
            downloads_ipad,
            revenue_android,
            revenue_iphone,
            revenue_ipad
        FROM core.fact_app_performance_daily
        WHERE app_id = %(app_id)s
          AND (CAST(%(start_date)s AS DATE) IS NULL OR date::date >= CAST(%(start_date)s AS DATE))
          AND (CAST(%(end_date)s AS DATE) IS NULL OR date::date <= CAST(%(end_date)s AS DATE))
          AND (
              CAST(%(country)s AS TEXT) IS NULL
              OR country_android = CAST(%(country)s AS TEXT)
              OR country_ios = CAST(%(country)s AS TEXT)
          )
        ORDER BY date DESC, country_android, country_ios
        LIMIT %(limit)s
    """
    items = fetch_all(
        query,
        {
            "app_id": app_id,
            "start_date": start_date,
            "end_date": end_date,
            "country": country,
            "limit": normalize_limit(limit),
        },
    )
    return {
        "app": app_exists,
        "items": items,
        "count": len(items),
        "limit": normalize_limit(limit),
    }


@protected.get("/steam/games")
def list_steam_games(
    q: str | None = Query(default=None, description="Search by Steam game name or app id."),
    limit: int | None = Query(default=None, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    query = """
        SELECT
            app_id,
            name,
            game_class,
            game_genre,
            game_subgenre,
            developer,
            publisher,
            initial_price,
            release_date
        FROM steam.dim_steam_game_info
        WHERE (
            CAST(%(pattern)s AS TEXT) IS NULL
            OR name ILIKE CAST(%(pattern)s AS TEXT)
            OR CAST(app_id AS TEXT) ILIKE CAST(%(pattern)s AS TEXT)
        )
        ORDER BY name NULLS LAST, app_id
        LIMIT %(limit)s
        OFFSET %(offset)s
    """
    items = fetch_all(
        query,
        {
            "pattern": build_search_pattern(q),
            "limit": normalize_limit(limit),
            "offset": offset,
        },
    )
    return {"items": items, "limit": normalize_limit(limit), "offset": offset}


@protected.get("/steam/games/{app_id}")
def get_steam_game(app_id: int) -> dict[str, Any]:
    query = """
        SELECT
            g.*,
            COALESCE(p.month_count, 0) AS performance_month_count
        FROM steam.dim_steam_game_info AS g
        LEFT JOIN (
            SELECT app_id, COUNT(*) AS month_count
            FROM steam.fact_steam_game_performance_monthly
            GROUP BY app_id
        ) AS p
          ON p.app_id = g.app_id
        WHERE g.app_id = %(app_id)s
    """
    game = fetch_one(query, {"app_id": app_id})
    if not game:
        raise HTTPException(status_code=404, detail="Steam game not found")
    return game


@protected.get("/steam/games/{app_id}/performance")
def get_steam_game_performance(
    app_id: int,
    start_month: date | None = Query(default=None),
    end_month: date | None = Query(default=None),
    limit: int | None = Query(default=120, ge=1, le=1000),
) -> dict[str, Any]:
    game = fetch_one(
        "SELECT app_id, name FROM steam.dim_steam_game_info WHERE app_id = %(app_id)s",
        {"app_id": app_id},
    )
    if not game:
        raise HTTPException(status_code=404, detail="Steam game not found")

    query = """
        SELECT
            app_id,
            month,
            peak_ccu
        FROM steam.fact_steam_game_performance_monthly
        WHERE app_id = %(app_id)s
          AND (CAST(%(start_month)s AS DATE) IS NULL OR month >= CAST(%(start_month)s AS DATE))
          AND (CAST(%(end_month)s AS DATE) IS NULL OR month <= CAST(%(end_month)s AS DATE))
        ORDER BY month DESC
        LIMIT %(limit)s
    """
    items = fetch_all(
        query,
        {
            "app_id": app_id,
            "start_month": start_month,
            "end_month": end_month,
            "limit": normalize_limit(limit),
        },
    )
    return {
        "game": game,
        "items": items,
        "count": len(items),
        "limit": normalize_limit(limit),
    }


app.include_router(protected)

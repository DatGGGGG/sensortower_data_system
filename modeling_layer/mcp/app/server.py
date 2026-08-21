from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
import hashlib
import hmac
import os
import secrets
import time
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP
import psycopg
from psycopg.rows import dict_row
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value else default


MCP_SHARED_TOKEN = _env("MCP_SHARED_TOKEN")
WAREHOUSE_API_BASE_URL = (_env("WAREHOUSE_API_BASE_URL", "http://api:8000") or "").rstrip("/")
WAREHOUSE_API_KEY = _env("WAREHOUSE_API_KEY") or _env("CATALOG_API_KEY")
DATABASE_URL = _env("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/mydb") or ""
REQUEST_TIMEOUT_SECONDS = float(_env("MCP_REQUEST_TIMEOUT_SECONDS", "30") or "30")
CURRENT_MCP_TOKEN_RECORD: ContextVar[dict[str, Any] | None] = ContextVar(
    "CURRENT_MCP_TOKEN_RECORD",
    default=None,
)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def token_prefix(token: str) -> str:
    return token[:16]


def generate_mcp_token() -> str:
    return f"mcp_live_{secrets.token_urlsafe(32)}"


def authenticate_mcp_token(token: str) -> dict[str, Any] | None:
    query = """
        UPDATE api_access.mcp_tokens
        SET last_used_at = now()
        WHERE token_hash = %(token_hash)s
          AND is_active = TRUE
        RETURNING id, name, token_prefix, created_at, last_used_at
    """
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, {"token_hash": hash_token(token)})
            return cur.fetchone()


class BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        if request.url.path == "/health":
            return await call_next(request)

        if not DATABASE_URL and not MCP_SHARED_TOKEN:
            return JSONResponse(
                {"error": "mcp_not_configured", "detail": "DATABASE_URL or MCP_SHARED_TOKEN is required."},
                status_code=503,
            )

        auth_scheme, _, auth_token = request.headers.get("authorization", "").partition(" ")
        token = auth_token.strip()
        if auth_scheme.lower() != "bearer" or not token:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        if MCP_SHARED_TOKEN and hmac.compare_digest(token, MCP_SHARED_TOKEN):
            context_token = CURRENT_MCP_TOKEN_RECORD.set(
                {
                    "id": None,
                    "name": "shared-token",
                    "token_prefix": token_prefix(token),
                }
            )
            try:
                return await call_next(request)
            finally:
                CURRENT_MCP_TOKEN_RECORD.reset(context_token)

        try:
            token_record = authenticate_mcp_token(token)
        except (psycopg.errors.InvalidSchemaName, psycopg.errors.UndefinedTable):
            return JSONResponse(
                {
                    "error": "mcp_token_auth_not_ready",
                    "detail": "Run /sql/api_access_schema.sql before using MCP token auth.",
                },
                status_code=503,
            )

        if not token_record:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        context_token = CURRENT_MCP_TOKEN_RECORD.set(dict(token_record))
        try:
            return await call_next(request)
        finally:
            CURRENT_MCP_TOKEN_RECORD.reset(context_token)


def _safe_log_mcp_tool_usage(
    *,
    tool_name: str,
    status: str,
    duration_ms: float,
    row_count: int | None = None,
    truncated: bool | None = None,
    error_code: str | None = None,
) -> None:
    token_record = CURRENT_MCP_TOKEN_RECORD.get()
    try:
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_access.mcp_tool_logs (
                        mcp_token_id,
                        token_name,
                        token_prefix,
                        tool_name,
                        status,
                        duration_ms,
                        row_count,
                        truncated,
                        error_code
                    )
                    VALUES (
                        %(mcp_token_id)s,
                        %(token_name)s,
                        %(token_prefix)s,
                        %(tool_name)s,
                        %(status)s,
                        %(duration_ms)s,
                        %(row_count)s,
                        %(truncated)s,
                        %(error_code)s
                    )
                    """,
                    {
                        "mcp_token_id": token_record.get("id") if token_record else None,
                        "token_name": token_record.get("name") if token_record else None,
                        "token_prefix": token_record.get("token_prefix") if token_record else None,
                        "tool_name": tool_name,
                        "status": status,
                        "duration_ms": round(duration_ms, 2),
                        "row_count": row_count,
                        "truncated": truncated,
                        "error_code": error_code,
                    },
                )
    except Exception:
        return


async def _logged_tool_call(tool_name: str, operation):  # type: ignore[no-untyped-def]
    started_at = time.perf_counter()
    status = "success"
    error_code = None
    row_count = None
    truncated = None
    try:
        result = await operation()
        if isinstance(result, dict):
            if result.get("status") == "error":
                status = "error"
                error_code = str(result.get("error") or "tool_error")
            if isinstance(result.get("row_count"), int):
                row_count = result.get("row_count")
            elif isinstance(result.get("items"), list):
                row_count = len(result["items"])
            truncated_value = result.get("truncated")
            if isinstance(truncated_value, bool):
                truncated = truncated_value
        return result
    except Exception:
        status = "error"
        error_code = "unhandled_exception"
        raise
    finally:
        _safe_log_mcp_tool_usage(
            tool_name=tool_name,
            status=status,
            duration_ms=(time.perf_counter() - started_at) * 1000,
            row_count=row_count,
            truncated=truncated,
            error_code=error_code,
        )


async def health(_: Request) -> JSONResponse:
    configured = bool((DATABASE_URL or MCP_SHARED_TOKEN) and WAREHOUSE_API_BASE_URL and WAREHOUSE_API_KEY)
    return JSONResponse(
        {
            "status": "ok" if configured else "not_configured",
            "mcp": "ok",
            "warehouse_api_base_url": WAREHOUSE_API_BASE_URL,
            "warehouse_api_key_configured": bool(WAREHOUSE_API_KEY),
            "mcp_token_auth_configured": bool(DATABASE_URL),
            "mcp_shared_token_fallback_configured": bool(MCP_SHARED_TOKEN),
        },
        status_code=200 if configured else 503,
    )


async def ready(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "mcp": "ready"})


def _require_config() -> None:
    missing = [
        name
        for name, value in (
            ("WAREHOUSE_API_BASE_URL", WAREHOUSE_API_BASE_URL),
            ("WAREHOUSE_API_KEY", WAREHOUSE_API_KEY),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"missing_configuration: {', '.join(missing)}")


def _headers() -> dict[str, str]:
    _require_config()
    return {"X-API-Key": WAREHOUSE_API_KEY or ""}


def _structured_error(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, httpx.TimeoutException):
        return {"status": "error", "error": "query_timeout", "detail": "Warehouse API request timed out."}
    if isinstance(exc, httpx.ConnectError):
        return {"status": "error", "error": "warehouse_unavailable", "detail": "Cannot connect to warehouse API."}
    if isinstance(exc, httpx.HTTPStatusError):
        response = exc.response
        try:
            detail: Any = response.json()
        except Exception:
            detail = response.text
        if response.status_code in {400, 422}:
            code = "query_rejected"
        elif response.status_code in {401, 403}:
            code = "warehouse_auth_failed"
        elif response.status_code == 408:
            code = "query_timeout"
        elif response.status_code >= 500:
            code = "warehouse_unavailable"
        else:
            code = "unexpected_error"
        return {"status": "error", "error": code, "http_status": response.status_code, "detail": detail}
    return {"status": "error", "error": "unexpected_error", "detail": str(exc)}


async def _get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.get(
                f"{WAREHOUSE_API_BASE_URL}{path}",
                headers=_headers(),
                params={k: v for k, v in (params or {}).items() if v is not None},
            )
            response.raise_for_status()
            return response.json()
    except Exception as exc:
        return _structured_error(exc)


async def _post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.post(f"{WAREHOUSE_API_BASE_URL}{path}", headers=_headers(), json=payload)
            response.raise_for_status()
            return response.json()
    except Exception as exc:
        return _structured_error(exc)


mcp = FastMCP(
    "VN Strategy Data Warehouse",
    instructions=(
        "Read-only MCP adapter for VN mobile game market data. Prefer analytics.* catalog objects. "
        "Use core.* objects only as fallback when analytics objects are insufficient. Revenue is stored "
        "in cents in warehouse facts; divide by 100.0 for user-facing money unless an analytics field "
        "is explicitly already USD."
    ),
    stateless_http=True,
    json_response=True,
)


@mcp.tool()
async def health_check() -> dict[str, Any]:
    """Check whether the warehouse API and database are healthy."""
    return await _logged_tool_call("health_check", lambda: _get("/health"))


@mcp.tool()
async def search_games(q: str, limit: int = 20, offset: int = 0) -> dict[str, Any]:
    """Search games by name or canonical app id before running game-specific queries."""
    return await _logged_tool_call(
        "search_games",
        lambda: _get("/games/search", {"q": q, "limit": limit, "offset": offset}),
    )


@mcp.tool()
async def get_catalog() -> dict[str, Any]:
    """Return approved warehouse objects, columns, descriptions, and query examples."""
    return await _logged_tool_call("get_catalog", lambda: _get("/meta/catalog"))


@mcp.tool()
async def query_warehouse(sql: str, max_rows: int | None = None) -> dict[str, Any]:
    """Run read-only SQL against approved warehouse catalog objects."""
    payload: dict[str, Any] = {"sql": sql}
    if max_rows is not None:
        payload["max_rows"] = max_rows
    return await _logged_tool_call("query_warehouse", lambda: _post("/query", payload))


@mcp.tool()
async def render_chart(data: list[dict[str, Any]], spec: dict[str, Any], source: dict[str, Any] | None = None) -> dict[str, Any]:
    """Render a small interactive chart from query rows using the warehouse chart API."""
    payload: dict[str, Any] = {"data": data, "spec": spec}
    if source is not None:
        payload["source"] = source
    return await _logged_tool_call("render_chart", lambda: _post("/charts/render", payload))


@asynccontextmanager
async def lifespan(_: Starlette):
    async with mcp.session_manager.run():
        yield


app = Starlette(
    routes=[
        Route("/health", health, methods=["GET"]),
        Route("/ready", ready, methods=["GET"]),
        Mount("/", app=mcp.streamable_http_app()),
    ],
    middleware=[Middleware(BearerAuthMiddleware)],
    lifespan=lifespan,
)

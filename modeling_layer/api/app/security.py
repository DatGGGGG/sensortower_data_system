from __future__ import annotations

import hashlib
import secrets
from typing import Any

import psycopg
from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from .db import fetch_one

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def generate_api_key() -> str:
    return f"dsql_live_{secrets.token_urlsafe(24)}"


def api_key_prefix(api_key: str) -> str:
    return api_key[:16]


def authenticate_api_key(api_key: str) -> dict[str, Any] | None:
    query = """
        UPDATE api_access.api_keys
        SET last_used_at = now()
        WHERE key_hash = %(key_hash)s
          AND is_active = TRUE
        RETURNING id, name, key_prefix, created_at, last_used_at
    """
    return fetch_one(query, {"key_hash": hash_api_key(api_key)})


def require_api_key(
    request: Request,
    api_key: str | None = Security(api_key_header),
) -> dict[str, Any]:
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing API key. Provide X-API-Key.")

    try:
        key_record = authenticate_api_key(api_key)
    except (psycopg.errors.InvalidSchemaName, psycopg.errors.UndefinedTable) as exc:
        raise HTTPException(
            status_code=503,
            detail="API key auth is not ready. Run /sql/api_access_schema.sql before using protected endpoints.",
        ) from exc

    if not key_record:
        raise HTTPException(status_code=401, detail="Invalid API key.")

    request.state.api_key_record = key_record
    return key_record

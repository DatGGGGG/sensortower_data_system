from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
import time
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .config import get_settings


@contextmanager
def get_connection() -> Iterator[psycopg.Connection]:
    settings = get_settings()
    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        yield conn


def fetch_all(query: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, params or {})
        return list(cur.fetchall())


def fetch_one(query: str, params: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, params or {})
        row = cur.fetchone()
        return dict(row) if row else None


def execute_statement(query: str, params: Mapping[str, Any] | None = None) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, params or {})


def ping_database() -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()


def fetch_catalog_columns(object_names: list[str]) -> list[dict[str, Any]]:
    query = """
        SELECT
            concat(n.nspname, '.', c.relname) AS object_name,
            CASE c.relkind
                WHEN 'v' THEN 'view'
                WHEN 'm' THEN 'materialized view'
                WHEN 'r' THEN 'table'
                ELSE c.relkind::text
            END AS object_type,
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            NOT a.attnotnull AS is_nullable,
            a.attnum AS ordinal_position
        FROM pg_class AS c
        JOIN pg_namespace AS n
          ON n.oid = c.relnamespace
        JOIN pg_attribute AS a
          ON a.attrelid = c.oid
        WHERE concat(n.nspname, '.', c.relname) = ANY(%(object_names)s)
          AND c.relkind IN ('r', 'v', 'm')
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY object_name, a.attnum
    """
    return fetch_all(query, {"object_names": object_names})


def run_read_only_query(sql: str, row_cap: int, timeout_ms: int) -> dict[str, Any]:
    wrapped_sql = f"SELECT * FROM ({sql}) AS query_result LIMIT {row_cap + 1}"
    started_at = time.perf_counter()

    with get_connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION READ ONLY")
                cur.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
                cur.execute(wrapped_sql)
                columns = [desc.name for desc in cur.description or []]
                rows = list(cur.fetchall())

    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    truncated = len(rows) > row_cap
    if truncated:
        rows = rows[:row_cap]

    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "truncated": truncated,
        "duration_ms": duration_ms,
    }

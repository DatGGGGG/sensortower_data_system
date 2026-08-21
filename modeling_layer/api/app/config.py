from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_version: str
    database_url: str
    default_limit: int
    max_limit: int
    query_timeout_ms: int
    query_default_rows: int
    query_max_rows: int
    chart_artifact_dir: str
    chart_ttl_hours: int
    chart_max_rows: int
    chart_signing_secret: str
    public_base_url: str | None


def get_str_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def get_optional_str_env(name: str) -> str | None:
    value = os.getenv(name)
    return value or None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    default_chart_dir = str(Path(__file__).resolve().parents[1] / "artifacts" / "charts")
    return Settings(
        app_name=get_str_env("API_NAME", "Strategy Data API"),
        app_version=get_str_env("API_VERSION", "0.1.0"),
        database_url=get_str_env(
            "DATABASE_URL",
            "postgresql://postgres:postgres@localhost:5433/mydb",
        ),
        default_limit=int(get_str_env("API_DEFAULT_LIMIT", "50")),
        max_limit=int(get_str_env("API_MAX_LIMIT", "500")),
        query_timeout_ms=int(get_str_env("API_QUERY_TIMEOUT_MS", "10000")),
        query_default_rows=int(get_str_env("API_QUERY_DEFAULT_ROWS", "100")),
        query_max_rows=int(get_str_env("API_QUERY_MAX_ROWS", "500")),
        chart_artifact_dir=get_str_env("API_CHART_ARTIFACT_DIR", default_chart_dir),
        chart_ttl_hours=int(get_str_env("API_CHART_TTL_HOURS", "168")),
        chart_max_rows=int(get_str_env("API_CHART_MAX_ROWS", "500")),
        chart_signing_secret=get_str_env(
            "API_CHART_SIGNING_SECRET",
            "dev-insecure-chart-secret-change-me",
        ),
        public_base_url=get_optional_str_env("API_PUBLIC_BASE_URL"),
    )

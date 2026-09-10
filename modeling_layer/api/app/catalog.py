from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CatalogEntry:
    name: str
    grain: str
    description: str
    examples: tuple[dict[str, str], ...]


ANALYTICS_GUIDANCE = (
    "Preferred curated analytics object for normal user-facing market questions. "
    "Supported performance markets are VN, CN, and WW; country='WW' means SensorTower worldwide scope, not a literal country. "
    "Revenue is stored in cents; divide by 100.0 for user-facing money. "
    "DAU, WAU, and MAU are user counts and should not be divided."
)

CORE_FALLBACK_GUIDANCE = (
    "Foundational core table exposed only as a fallback when analytics.* cannot answer the question. "
    "Avoid broad scans; always filter tightly by date/month/year, country, app/game id, or metric scope."
)


CATALOG: dict[str, CatalogEntry] = {
    "analytics.agg_game_performance_daily": CatalogEntry(
        name="analytics.agg_game_performance_daily",
        grain="date + country + unified_app_id",
        description=(
            "Daily game downloads, revenue, DAU, WAU, and MAU aggregated across linked app ids "
            f"and mobile platforms. {ANALYTICS_GUIDANCE}"
        ),
        examples=(
            {
                "question": "Show daily downloads, revenue, and active users for one game in VN.",
                "sql": (
                    "SELECT date, country, game_name, downloads, revenue / 100.0 AS revenue, dau, wau, mau "
                    "FROM analytics.agg_game_performance_daily "
                    "WHERE unified_app_id = 'YOUR_UNIFIED_APP_ID' AND country = 'VN' "
                    "AND date >= DATE '2026-06-01' AND date <= DATE '2026-06-30' "
                    "ORDER BY date"
                ),
            },
        ),
    ),
    "analytics.agg_game_performance_monthly": CatalogEntry(
        name="analytics.agg_game_performance_monthly",
        grain="month + country + unified_app_id",
        description=(
            "Monthly game downloads and revenue plus avg_dau, avg_wau, and latest monthly MAU, "
            f"aggregated across linked apps. {ANALYTICS_GUIDANCE}"
        ),
        examples=(
            {
                "question": "Show top downloaded games in VN in August 2026.",
                "sql": (
                    "SELECT game_name, unified_app_id, SUM(downloads) AS total_downloads "
                    "FROM analytics.agg_game_performance_monthly "
                    "WHERE country = 'VN' AND month = DATE '2026-08-01' "
                    "AND downloads IS NOT NULL "
                    "GROUP BY game_name, unified_app_id "
                    "ORDER BY total_downloads DESC LIMIT 10"
                ),
            },
            {
                "question": "Show top games by MAU in VN for June 2026.",
                "sql": (
                    "SELECT month, country, game_name, mau, avg_dau, avg_wau "
                    "FROM analytics.agg_game_performance_monthly "
                    "WHERE country = 'VN' AND month = DATE '2026-06-01' "
                    "ORDER BY mau DESC NULLS LAST LIMIT 20"
                ),
            },
            {
                "question": "Show top games by monthly revenue in VN.",
                "sql": (
                    "SELECT month, game_name, cleaned_publisher_name, downloads, revenue / 100.0 AS revenue "
                    "FROM analytics.agg_game_performance_monthly "
                    "WHERE country = 'VN' AND month = DATE '2026-05-01' "
                    "ORDER BY revenue DESC LIMIT 20"
                ),
            },
            {
                "question": "Show top games by monthly revenue in CN.",
                "sql": (
                    "SELECT month, country, game_name, cleaned_publisher_name, downloads, revenue / 100.0 AS revenue "
                    "FROM analytics.agg_game_performance_monthly "
                    "WHERE country = 'CN' AND month = DATE '2026-06-01' "
                    "ORDER BY revenue DESC LIMIT 20"
                ),
            },
            {
                "question": "Show top games by worldwide MAU.",
                "sql": (
                    "SELECT month, country, game_name, mau, avg_dau, avg_wau "
                    "FROM analytics.agg_game_performance_monthly "
                    "WHERE country = 'WW' AND month = DATE '2026-06-01' "
                    "ORDER BY mau DESC NULLS LAST LIMIT 20"
                ),
            },
        ),
    ),
    "analytics.agg_game_performance_yearly": CatalogEntry(
        name="analytics.agg_game_performance_yearly",
        grain="year + country + unified_app_id",
        description=(
            "Yearly game downloads and revenue plus average DAU, WAU, and MAU by country. "
            f"{ANALYTICS_GUIDANCE}"
        ),
        examples=(
            {
                "question": "Find the highest revenue games in VN for a given year.",
                "sql": (
                    "SELECT year, country, game_name, downloads, revenue / 100.0 AS revenue, avg_mau "
                    "FROM analytics.agg_game_performance_yearly "
                    "WHERE country = 'VN' AND year = 2025 "
                    "ORDER BY revenue DESC LIMIT 20"
                ),
            },
        ),
    ),
    "analytics.agg_subgenre_performance_monthly": CatalogEntry(
        name="analytics.agg_subgenre_performance_monthly",
        grain="month + country + game_class + game_genre + subgenre",
        description=(
            "Monthly subgenre downloads, revenue, avg_dau, avg_wau, and MAU by market. "
            "Active-user metrics are summed from game-level aggregates across all games in the country/subgenre. "
            f"{ANALYTICS_GUIDANCE}"
        ),
        examples=(
            {
                "question": "Track monthly Puzzle subgenre downloads and MAU in VN.",
                "sql": (
                    "SELECT month, country, game_class, game_genre, subgenre, downloads, revenue / 100.0 AS revenue, mau "
                    "FROM analytics.agg_subgenre_performance_monthly "
                    "WHERE country = 'VN' AND game_genre = 'Puzzle' "
                    "ORDER BY month DESC LIMIT 24"
                ),
            },
        ),
    ),
    "analytics.agg_subgenre_performance_yearly": CatalogEntry(
        name="analytics.agg_subgenre_performance_yearly",
        grain="year + country + game_class + game_genre + subgenre",
        description=(
            "Yearly subgenre downloads, revenue, avg_dau, avg_wau, and avg_mau by market. "
            "Active-user metrics are summed from game-level aggregates across all games in the country/subgenre. "
            f"{ANALYTICS_GUIDANCE}"
        ),
        examples=(
            {
                "question": "Compare yearly subgenre revenue and MAU in WW.",
                "sql": (
                    "SELECT year, country, game_class, game_genre, subgenre, downloads, revenue / 100.0 AS revenue, avg_mau "
                    "FROM analytics.agg_subgenre_performance_yearly "
                    "WHERE country = 'WW' AND year BETWEEN 2021 AND 2025 "
                    "ORDER BY year, revenue DESC LIMIT 100"
                ),
            },
        ),
    ),
    "analytics.agg_new_game_new_performance": CatalogEntry(
        name="analytics.agg_new_game_new_performance",
        grain="launch year + unified_app_id",
        description=(
            "New game launch performance windows with estimated release date, first 7/30/90/365 day revenue, "
            "downloads, and average DAU benchmarks. Revenue columns ending in _usd are already divided by 100."
        ),
        examples=(
            {
                "question": "Find strongest new launches in 2025 by first 30 day revenue.",
                "sql": (
                    "SELECT year, game_name, estimated_release_date, first_30d_downloads, "
                    "first_30d_revenue_usd, first_30d_avg_dau "
                    "FROM analytics.agg_new_game_new_performance "
                    "WHERE year = 2025 "
                    "ORDER BY first_30d_revenue_usd DESC NULLS LAST LIMIT 20"
                ),
            },
        ),
    ),
    "analytics.agg_game_retention": CatalogEntry(
        name="analytics.agg_game_retention",
        grain="month + country + unified_app_id",
        description=(
            "Monthly game-level retention metrics D1 through D365. Component app retention is weighted by "
            "same-month app MAU when available, otherwise falls back to simple averaging."
        ),
        examples=(
            {
                "question": "Show retention for one game in VN by month.",
                "sql": (
                    "SELECT month, country, game_name, est_retention_d1, est_retention_d7, est_retention_d30 "
                    "FROM analytics.agg_game_retention "
                    "WHERE unified_app_id = 'YOUR_UNIFIED_APP_ID' AND country = 'VN' "
                    "AND month >= DATE '2026-01-01' "
                    "ORDER BY month"
                ),
            },
        ),
    ),
    "analytics.enriched_app_user_demographics": CatalogEntry(
        name="analytics.enriched_app_user_demographics",
        grain="app_id + country + date + date_granularity",
        description=(
            "App user demographic facts enriched with app_name, unified_app_id, and game_name. "
            "Use for gender/age distribution questions when aggregate analytics views are not enough."
        ),
        examples=(
            {
                "question": "Show age and gender demographics for one game.",
                "sql": (
                    "SELECT game_name, app_name, country, date, female, male, average_age_total "
                    "FROM analytics.enriched_app_user_demographics "
                    "WHERE unified_app_id = 'YOUR_UNIFIED_APP_ID' AND country = 'SE_ASIA' "
                    "ORDER BY date DESC LIMIT 20"
                ),
            },
        ),
    ),
    "analytics.mv_mobile_download_total_yearly": CatalogEntry(
        name="analytics.mv_mobile_download_total_yearly",
        grain="year",
        description="Yearly total downloads with current-year annualization metadata.",
        examples=(
            {
                "question": "Check annualized total downloads by year.",
                "sql": (
                    "SELECT year, total_downloads, data_through_current_year, days_elapsed_current_year "
                    "FROM analytics.mv_mobile_download_total_yearly "
                    "ORDER BY year"
                ),
            },
        ),
    ),
    "core.dim_game_info": CatalogEntry(
        name="core.dim_game_info",
        grain="unified_app_id",
        description=f"Game dimension with taxonomy and release metadata. {CORE_FALLBACK_GUIDANCE}",
        examples=(
            {
                "question": "Find taxonomy fields for a known game.",
                "sql": (
                    "SELECT unified_app_id, name, game_class, game_genre, game_subgenre "
                    "FROM core.dim_game_info "
                    "WHERE unified_app_id = 'YOUR_UNIFIED_APP_ID' "
                    "LIMIT 1"
                ),
            },
        ),
    ),
    "core.dim_app_info": CatalogEntry(
        name="core.dim_app_info",
        grain="app_id",
        description=f"App dimension linked to unified games, publishers, platform, and revenue multiplier. {CORE_FALLBACK_GUIDANCE}",
        examples=(
            {
                "question": "List component apps for a known game.",
                "sql": (
                    "SELECT app_id, name, os, publisher_name, cleaned_publisher_name "
                    "FROM core.dim_app_info "
                    "WHERE unified_app_id = 'YOUR_UNIFIED_APP_ID' "
                    "ORDER BY os, app_id LIMIT 50"
                ),
            },
        ),
    ),
    "core.fact_app_performance_daily": CatalogEntry(
        name="core.fact_app_performance_daily",
        grain="app_id + date + android country + ios country",
        description=f"Foundational app-level daily downloads and revenue fact table. {CORE_FALLBACK_GUIDANCE}",
        examples=(
            {
                "question": "Inspect app-level daily downloads/revenue for one app.",
                "sql": (
                    "SELECT app_id, date, downloads_android, downloads_iphone, downloads_ipad, "
                    "revenue_android / 100.0 AS revenue_android, revenue_iphone / 100.0 AS revenue_iphone, "
                    "revenue_ipad / 100.0 AS revenue_ipad "
                    "FROM core.fact_app_performance_daily "
                    "WHERE app_id = 'YOUR_APP_ID' "
                    "AND date >= DATE '2026-06-01' AND date <= DATE '2026-06-30' "
                    "ORDER BY date LIMIT 100"
                ),
            },
        ),
    ),
    "core.fact_app_performance_active_users": CatalogEntry(
        name="core.fact_app_performance_active_users",
        grain="app_id + country + date",
        description=f"Foundational app-level DAU/WAU/MAU fact table. {CORE_FALLBACK_GUIDANCE}",
        examples=(
            {
                "question": "Inspect app-level active users for one app.",
                "sql": (
                    "SELECT app_id, country, date, "
                    "COALESCE(dau_android, 0) + COALESCE(dau_iphone, 0) + COALESCE(dau_ipad, 0) AS dau, "
                    "COALESCE(wau_android, 0) + COALESCE(wau_iphone, 0) + COALESCE(wau_ipad, 0) AS wau, "
                    "COALESCE(mau_android, 0) + COALESCE(mau_iphone, 0) + COALESCE(mau_ipad, 0) AS mau "
                    "FROM core.fact_app_performance_active_users "
                    "WHERE app_id = 'YOUR_APP_ID' AND country = 'VN' "
                    "AND date >= DATE '2026-06-01' AND date <= DATE '2026-06-30' "
                    "ORDER BY date LIMIT 100"
                ),
            },
        ),
    ),
    "core.fact_app_performance_retention": CatalogEntry(
        name="core.fact_app_performance_retention",
        grain="app_id + country + month",
        description=f"Foundational app-level monthly retention D1 through D365. {CORE_FALLBACK_GUIDANCE}",
        examples=(
            {
                "question": "Inspect app-level retention for one app.",
                "sql": (
                    "SELECT app_id, country, date, est_retention_d1, est_retention_d7, est_retention_d30 "
                    "FROM core.fact_app_performance_retention "
                    "WHERE app_id = 'YOUR_APP_ID' AND country = 'VN' "
                    "AND date >= DATE '2026-01-01' "
                    "ORDER BY date LIMIT 100"
                ),
            },
        ),
    ),
    "core.fact_user_demographics": CatalogEntry(
        name="core.fact_user_demographics",
        grain="app_id + country + date + date_granularity",
        description=f"Foundational app-level user demographics. Prefer analytics.enriched_app_user_demographics first. {CORE_FALLBACK_GUIDANCE}",
        examples=(
            {
                "question": "Inspect app-level demographics for one app.",
                "sql": (
                    "SELECT app_id, country, date, date_granularity, female, male, average_age_total "
                    "FROM core.fact_user_demographics "
                    "WHERE app_id = 'YOUR_APP_ID' AND country = 'SE_ASIA' "
                    "ORDER BY date DESC LIMIT 20"
                ),
            },
        ),
    ),
}


def allowed_catalog_objects() -> tuple[str, ...]:
    return tuple(CATALOG.keys())


SEMANTIC_TEMPLATES: dict[str, dict[str, Any]] = {
    "identifier": {
        "semantic_type": "identifier",
        "unit": "text",
        "additivity": "non_additive",
        "default_aggregation": "none",
        "allowed_aggregations": [],
        "null_policy": "preserve",
    },
    "dimension": {
        "semantic_type": "dimension",
        "unit": "text",
        "additivity": "non_additive",
        "default_aggregation": "none",
        "allowed_aggregations": [],
        "null_policy": "preserve",
    },
    "date": {
        "semantic_type": "date",
        "unit": "date",
        "additivity": "non_additive",
        "default_aggregation": "none",
        "allowed_aggregations": [],
        "null_policy": "preserve",
    },
    "count": {
        "semantic_type": "measure",
        "unit": "count",
        "additivity": "additive",
        "default_aggregation": "sum",
        "allowed_aggregations": ["sum"],
        "null_policy": "exclude",
    },
    "active_users": {
        "semantic_type": "measure",
        "unit": "count",
        "additivity": "non_additive",
        "default_aggregation": "latest",
        "allowed_aggregations": ["latest", "average", "maximum"],
        "null_policy": "exclude",
    },
    "retention": {
        "semantic_type": "ratio",
        "unit": "percent",
        "additivity": "non_additive",
        "default_aggregation": "average",
        "allowed_aggregations": ["average", "latest"],
        "null_policy": "exclude",
    },
    "revenue_cents": {
        "semantic_type": "currency",
        "unit": "usd_cents",
        "additivity": "additive",
        "default_aggregation": "sum",
        "allowed_aggregations": ["sum"],
        "null_policy": "exclude",
    },
    "revenue_usd": {
        "semantic_type": "currency",
        "unit": "usd",
        "additivity": "additive",
        "default_aggregation": "sum",
        "allowed_aggregations": ["sum"],
        "null_policy": "exclude",
    },
}


# Explicit overrides are authoritative for business-sensitive units and grains.
# Name-based inference below is only a fallback for newly added catalog columns.
COLUMN_SEMANTICS_BY_OBJECT: dict[str, dict[str, str]] = {
    "analytics.agg_game_performance_daily": {
        "downloads": "count",
        "revenue": "revenue_cents",
        "dau": "active_users",
        "wau": "active_users",
        "mau": "active_users",
    },
    "analytics.agg_game_performance_monthly": {
        "downloads": "count",
        "revenue": "revenue_cents",
        "avg_dau": "active_users",
        "avg_wau": "active_users",
        "mau": "active_users",
    },
    "analytics.agg_game_performance_yearly": {
        "downloads": "count",
        "revenue": "revenue_cents",
        "avg_dau": "active_users",
        "avg_wau": "active_users",
        "avg_mau": "active_users",
    },
    "analytics.agg_new_game_new_performance": {
        "first_7d_revenue_usd": "revenue_usd",
        "first_30d_revenue_usd": "revenue_usd",
        "first_90d_revenue_usd": "revenue_usd",
        "first_365d_revenue_usd": "revenue_usd",
    },
}


def enrich_catalog_columns(object_name: str, columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add stable analytical semantics while preserving existing column fields."""
    object_overrides = COLUMN_SEMANTICS_BY_OBJECT.get(object_name, {})
    enriched: list[dict[str, Any]] = []
    for column in columns:
        name = str(column["name"])
        semantic_name = object_overrides.get(name) or _infer_semantic_template(name)
        item = dict(column)
        item.update(SEMANTIC_TEMPLATES[semantic_name])
        enriched.append(item)
    return enriched


def _infer_semantic_template(name: str) -> str:
    if name in {"app_id", "unified_app_id"}:
        return "identifier"
    if name in {"date", "month", "year", "estimated_release_date", "data_through_current_year"}:
        return "date"
    if name in {"dau", "wau", "mau", "avg_dau", "avg_wau", "avg_mau"} or name.endswith("_avg_dau"):
        return "active_users"
    if name.startswith("est_retention_"):
        return "retention"
    if name.endswith("_revenue_usd"):
        return "revenue_usd"
    if name == "revenue" or name.startswith("revenue_"):
        return "revenue_cents"
    if name == "downloads" or name.startswith("downloads_") or name.endswith("_downloads"):
        return "count"
    return "dimension"

from __future__ import annotations

from dataclasses import dataclass
import re


class QueryValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ValidatedQuery:
    sql: str
    referenced_objects: tuple[str, ...]


SQL_START_RE = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
FORBIDDEN_KEYWORD_RE = re.compile(
    r"\b("
    r"insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|call|do|"
    r"merge|vacuum|analyze|refresh|comment|listen|unlisten|notify|execute|prepare|"
    r"set\s+role|reset\s+role"
    r")\b",
    re.IGNORECASE,
)
SCHEMA_OBJECT_RE = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w$]*\.[a-zA-Z_][\w$]*)", re.IGNORECASE)
WHERE_CLAUSE_RE = re.compile(
    r"\bwhere\b\s+(.*?)"
    r"(?=\b(?:group\s+by|order\s+by|having|limit|offset|fetch|union|intersect|except)\b|[;)]|$)",
    re.IGNORECASE,
)
DAILY_FILTER_COLUMN_RE = re.compile(
    r"(?:\b[a-zA-Z_][\w$]*\s*\.\s*)?\b(?:date|country|unified_app_id)\b",
    re.IGNORECASE,
)


def _inspection_sql(sql: str) -> str:
    """Return a whitespace-insensitive copy used only for guard inspection."""
    return " ".join(sql.split())


def validate_read_only_sql(sql: str, allowed_objects: set[str]) -> ValidatedQuery:
    normalized = sql.strip()
    if not normalized:
        raise QueryValidationError("SQL is required.")

    if normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()

    if ";" in normalized:
        raise QueryValidationError("Only a single SQL statement is allowed.")

    if "--" in normalized or "/*" in normalized or "*/" in normalized:
        raise QueryValidationError("SQL comments are not allowed in the query endpoint.")

    if not SQL_START_RE.match(normalized):
        raise QueryValidationError("Only SELECT-style read queries are allowed.")

    if FORBIDDEN_KEYWORD_RE.search(normalized):
        raise QueryValidationError("Only read-only analytics queries are allowed.")

    referenced_objects = tuple(
        sorted(
            {
                match.group(1).lower()
                for match in SCHEMA_OBJECT_RE.finditer(normalized)
            }
        )
    )
    if not referenced_objects:
        raise QueryValidationError(
            "Queries must reference at least one approved object using schema-qualified names."
        )

    disallowed = [name for name in referenced_objects if name not in allowed_objects]
    if disallowed:
        joined = ", ".join(disallowed)
        raise QueryValidationError(f"Queries may only read approved catalog objects. Disallowed: {joined}")

    if "analytics.agg_game_performance_daily" in referenced_objects:
        inspection_sql = _inspection_sql(normalized)
        where_predicates = WHERE_CLAUSE_RE.findall(inspection_sql)
        if not where_predicates:
            raise QueryValidationError(
                "Queries against analytics.agg_game_performance_daily must include a WHERE filter."
            )
        if not any(DAILY_FILTER_COLUMN_RE.search(predicate) for predicate in where_predicates):
            raise QueryValidationError(
                "Queries against analytics.agg_game_performance_daily must filter by date, country, or unified_app_id."
            )

    return ValidatedQuery(sql=normalized, referenced_objects=referenced_objects)

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
import unittest
from unittest.mock import patch

import psycopg
from fastapi import HTTPException

from modeling_layer.api.app import db, main
from modeling_layer.api.app.catalog import enrich_catalog_columns
from modeling_layer.api.app.query_guard import QueryValidationError, validate_read_only_sql


ALLOWED = {"analytics.agg_game_performance_monthly"}
VALID_AGGREGATE = (
    "SELECT country, SUM(downloads) AS downloads "
    "FROM analytics.agg_game_performance_monthly GROUP BY country"
)


class RequestStub:
    class State:
        request_id = "request-123"

    state = State()


@dataclass
class ColumnStub:
    name: str
    type_code: int


class CursorStub:
    def __init__(self) -> None:
        self.executions: list[str] = []
        self.description: list[ColumnStub] = []
        self.user_results_fetched = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, query, _params=None) -> None:
        normalized = " ".join(str(query).split())
        self.executions.append(normalized)
        if "AS query_validation LIMIT 0" in normalized:
            self.description = [ColumnStub("downloads", 20)]

    def fetchall(self):
        if "FROM pg_catalog.pg_type" not in self.executions[-1]:
            self.user_results_fetched = True
        return [{"oid": 20, "data_type": "bigint"}]


class ConnectionStub:
    def __init__(self, cursor: CursorStub) -> None:
        self._cursor = cursor

    def transaction(self):
        return nullcontext()

    def cursor(self):
        return self._cursor


class QueryGuardTests(unittest.TestCase):
    def test_accepts_cte(self) -> None:
        result = validate_read_only_sql(
            "WITH totals AS (SELECT downloads FROM analytics.agg_game_performance_monthly) "
            "SELECT SUM(downloads) FROM totals",
            ALLOWED,
        )
        self.assertEqual(result.referenced_objects, ("analytics.agg_game_performance_monthly",))

    def test_rejects_forbidden_object(self) -> None:
        with self.assertRaises(QueryValidationError):
            validate_read_only_sql("SELECT * FROM private.secret_table", ALLOWED)


class DatabaseValidationTests(unittest.TestCase):
    def test_describes_query_without_fetching_its_rows(self) -> None:
        cursor = CursorStub()
        connection = ConnectionStub(cursor)
        with patch.object(db, "get_connection", return_value=nullcontext(connection)):
            result = db.validate_read_only_query(VALID_AGGREGATE, 2_000)

        self.assertFalse(cursor.user_results_fetched)
        self.assertIn("LIMIT 0", cursor.executions[2])
        self.assertEqual(
            result["output_columns"],
            [{"name": "downloads", "data_type": "bigint"}],
        )


class ValidationEndpointTests(unittest.TestCase):
    def call_endpoint(self, database_side_effect=None):
        with patch.object(main, "validate_read_only_query") as validator:
            if database_side_effect is None:
                validator.return_value = {
                    "output_columns": [{"name": "downloads", "data_type": "bigint"}],
                    "duration_ms": 1.0,
                }
            else:
                validator.side_effect = database_side_effect
            return main.validate_query_analytics(RequestStub(), main.QueryRequest(sql=VALID_AGGREGATE))

    def assert_error(self, exception: psycopg.Error, status: int, code: str) -> None:
        with self.assertRaises(HTTPException) as caught:
            self.call_endpoint(exception)
        self.assertEqual(caught.exception.status_code, status)
        self.assertEqual(caught.exception.detail["error"]["code"], code)

    def test_valid_aggregate_query(self) -> None:
        result = self.call_endpoint()
        self.assertTrue(result["valid"])
        self.assertEqual(result["output_columns"][0]["data_type"], "bigint")

    def test_syntax_error(self) -> None:
        self.assert_error(psycopg.errors.SyntaxError("syntax error"), 400, "SQL_SYNTAX_ERROR")

    def test_unknown_column(self) -> None:
        self.assert_error(psycopg.errors.UndefinedColumn("column does not exist"), 400, "UNKNOWN_COLUMN")

    def test_grouping_error(self) -> None:
        self.assert_error(psycopg.errors.GroupingError("must appear in GROUP BY"), 400, "INVALID_GROUPING")

    def test_timeout(self) -> None:
        self.assert_error(psycopg.errors.QueryCanceled("statement timeout"), 408, "QUERY_TIMEOUT")


class CatalogSemanticsTests(unittest.TestCase):
    def test_explicit_usd_semantics_override_name_fallback(self) -> None:
        columns = enrich_catalog_columns(
            "analytics.agg_new_game_new_performance",
            [{"name": "first_30d_revenue_usd", "data_type": "numeric", "nullable": True}],
        )
        self.assertEqual(columns[0]["unit"], "usd")

    def test_existing_column_fields_are_preserved(self) -> None:
        original = {"name": "revenue", "data_type": "numeric", "nullable": False}
        enriched = enrich_catalog_columns("analytics.agg_game_performance_monthly", [original])[0]
        self.assertEqual(enriched["data_type"], "numeric")
        self.assertFalse(enriched["nullable"])
        self.assertEqual(enriched["unit"], "usd_cents")


if __name__ == "__main__":
    unittest.main()

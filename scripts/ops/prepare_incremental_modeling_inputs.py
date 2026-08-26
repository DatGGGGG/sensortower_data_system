from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_runtime import (  # noqa: E402
    APP_PERFORMANCE_REVENUE_ADJUSTMENT_COUNTRIES,
    _adjust_app_performance_revenue_record,
    _file_size,
    _iter_json_array_items,
    _load_revenue_factor_by_app,
    _write_ndjson_items_atomic,
)


def _date_text(row: dict[str, Any], field: str) -> str | None:
    value = row.get(field)
    if value is None:
        return None
    return str(value)


def _summarize_dates(rows: Iterable[dict[str, Any]], field: str) -> tuple[Iterable[dict[str, Any]], dict[str, Any]]:
    stats: dict[str, Any] = {"records_written": 0, "min_date": None, "max_date": None}

    def _wrapped() -> Iterable[dict[str, Any]]:
        for row in rows:
            date_text = _date_text(row, field)
            if date_text:
                if stats["min_date"] is None or date_text < stats["min_date"]:
                    stats["min_date"] = date_text
                if stats["max_date"] is None or date_text > stats["max_date"]:
                    stats["max_date"] = date_text
            stats["records_written"] += 1
            yield row

    return _wrapped(), stats


def prepare_app_performance(
    *,
    output_root: Path,
    run_timestamp: int,
    adjusted_app_full_info_path: Path,
    incremental_dir: Path,
) -> dict[str, Any]:
    source_path = output_root / "base" / f"st_app_performance_daily_{run_timestamp}.json"
    if not source_path.exists():
        raise FileNotFoundError(source_path)

    output_path = incremental_dir / f"st_app_performance_daily_revenue_adjusted_{run_timestamp}.ndjson"
    revenue_factor_by_app = _load_revenue_factor_by_app(adjusted_app_full_info_path)

    def _rows() -> Iterable[dict[str, Any]]:
        for row in _iter_json_array_items(source_path):
            yield _adjust_app_performance_revenue_record(row, revenue_factor_by_app)

    wrapped_rows, stats = _summarize_dates(_rows(), "d")
    records_written = _write_ndjson_items_atomic(output_path, wrapped_rows)
    stats["records_written"] = records_written
    return {
        "dataset": "app_performance",
        "source_path": str(source_path),
        "output_path": str(output_path),
        "records_written": records_written,
        "min_date": stats["min_date"],
        "max_date": stats["max_date"],
        "file_size_bytes": _file_size(output_path),
        "revenue_multiplier_app_count": len(revenue_factor_by_app),
        "revenue_adjustment_country_codes": sorted(APP_PERFORMANCE_REVENUE_ADJUSTMENT_COUNTRIES),
    }


def prepare_app_active_users(
    *,
    output_root: Path,
    run_timestamp: int,
    incremental_dir: Path,
) -> dict[str, Any]:
    source_path = output_root / "base" / f"st_app_active_users_{run_timestamp}.json"
    if not source_path.exists():
        raise FileNotFoundError(source_path)

    output_path = incremental_dir / f"st_app_active_users_{run_timestamp}.ndjson"
    wrapped_rows, stats = _summarize_dates(_iter_json_array_items(source_path), "date")
    records_written = _write_ndjson_items_atomic(output_path, wrapped_rows)
    stats["records_written"] = records_written
    return {
        "dataset": "app_active_users",
        "source_path": str(source_path),
        "output_path": str(output_path),
        "records_written": records_written,
        "min_date": stats["min_date"],
        "max_date": stats["max_date"],
        "file_size_bytes": _file_size(output_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare run-scoped incremental modeling NDJSON inputs.")
    parser.add_argument("--output-root", default="data")
    parser.add_argument("--run-timestamp", type=int, required=True)
    parser.add_argument("--adjusted-app-full-info", default=None)
    parser.add_argument(
        "--dataset",
        choices=["all", "app_performance", "app_active_users"],
        default="all",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root).resolve()
    incremental_dir = output_root / "staging" / "incremental"
    incremental_dir.mkdir(parents=True, exist_ok=True)
    adjusted_app_full_info_path = Path(
        args.adjusted_app_full_info
        or output_root / "staging" / "st_app_full_info_adjusted_current.ndjson"
    ).resolve()

    results: list[dict[str, Any]] = []
    if args.dataset in {"all", "app_performance"}:
        results.append(
            prepare_app_performance(
                output_root=output_root,
                run_timestamp=args.run_timestamp,
                adjusted_app_full_info_path=adjusted_app_full_info_path,
                incremental_dir=incremental_dir,
            )
        )
    if args.dataset in {"all", "app_active_users"}:
        results.append(
            prepare_app_active_users(
                output_root=output_root,
                run_timestamp=args.run_timestamp,
                incremental_dir=incremental_dir,
            )
        )

    print(json.dumps({"status": "success", "results": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

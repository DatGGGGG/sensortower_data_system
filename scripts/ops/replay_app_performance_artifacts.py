from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_runtime import (  # noqa: E402
    BACKFILL_LOAD_MODE,
    _iter_json_array_items,
    _merge_app_performance_into_current_streaming,
    _parse_record_date,
    _source_base_outputs,
    _source_base_state_outputs,
    _utc_now_iso,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay existing App Performance run artifacts into the partitioned current store without refetching SensorTower."
    )
    parser.add_argument("--output-root", default="data")
    parser.add_argument("--run-timestamp", type=int, action="append", required=True)
    parser.add_argument(
        "--drop-missing-in-window",
        action="store_true",
        help="Drop current rows in the replayed date window when absent from the run artifact. Off by default for safe partial-country repair.",
    )
    return parser.parse_args()


def artifact_window(path: Path) -> dict[str, Any]:
    row_count = 0
    min_date = None
    max_date = None
    countries: set[str] = set()
    for row in _iter_json_array_items(path):
        row_count += 1
        date_value = _parse_record_date(row.get("d"))
        min_date = date_value if min_date is None or date_value < min_date else min_date
        max_date = date_value if max_date is None or date_value > max_date else max_date
        country = row.get("c") or row.get("cc")
        if country:
            countries.add(str(country))
    if row_count == 0 or min_date is None or max_date is None:
        raise ValueError(f"App Performance artifact has no dated records: {path}")
    return {
        "effective_mode": BACKFILL_LOAD_MODE,
        "start_date": min_date.isoformat(),
        "end_date": max_date.isoformat(),
        "archive_enabled": False,
        "overlap_days": None,
        "row_count": row_count,
        "countries": sorted(countries),
    }


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root).resolve()
    state_outputs = _source_base_state_outputs(output_root)
    results = []

    for run_timestamp in args.run_timestamp:
        outputs = _source_base_outputs(output_root, run_timestamp)
        artifact_path = outputs["app_performance"]
        if not artifact_path.exists():
            raise FileNotFoundError(f"Missing App Performance artifact: {artifact_path}")
        window = artifact_window(artifact_path)
        merge_metrics = _merge_app_performance_into_current_streaming(
            current_path=state_outputs["app_performance_current"],
            run_output_path=artifact_path,
            history_archive_path=state_outputs["app_performance_history_archive"],
            key_builder=lambda row: (
                str(row.get("aid")),
                str(row.get("c") or row.get("cc")),
                str(row.get("d")),
            ),
            record_date_field="d",
            window=window,
            dataset_name="app_performance_daily",
            expired_at=_utc_now_iso(),
            drop_missing_in_window=args.drop_missing_in_window,
        )
        results.append(
            {
                "run_timestamp": run_timestamp,
                "artifact_path": str(artifact_path),
                "artifact_window": window,
                "merge_metrics": merge_metrics,
            }
        )

    print(
        json.dumps(
            {
                "status": "success",
                "output_root": str(output_root),
                "drop_missing_in_window": args.drop_missing_in_window,
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

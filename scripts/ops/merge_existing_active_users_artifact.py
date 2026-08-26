from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_runtime import (  # noqa: E402
    _app_active_users_current_available,
    _count_json_array_items,
    _facts_window_for_mode,
    _file_size,
    _merge_app_active_users_into_current_streaming,
    _source_base_outputs,
    _source_base_state_outputs,
    _utc_now_iso,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Merge an existing app-active-users run artifact into the canonical "
            "partitioned current store without refetching SensorTower."
        )
    )
    parser.add_argument("--run-timestamp", type=int, required=True)
    parser.add_argument("--output-root", default="data")
    parser.add_argument("--facts-load-mode", choices=["daily", "weekly", "quarterly"], default="weekly")
    parser.add_argument("--performance-end-date", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root).resolve()
    outputs = _source_base_outputs(output_root, args.run_timestamp)
    state_outputs = _source_base_state_outputs(output_root)
    artifact_path = outputs["app_active_users"]
    current_store = state_outputs["app_active_users_current"]

    if not artifact_path.exists():
        raise SystemExit(f"Missing active-users run artifact: {artifact_path}")

    window = _facts_window_for_mode(
        args.performance_end_date,
        args.facts_load_mode,
        _app_active_users_current_available(current_store),
    )
    merge_metrics = _merge_app_active_users_into_current_streaming(
        current_path=current_store,
        run_output_path=artifact_path,
        history_archive_path=state_outputs["app_active_users_history_archive"],
        key_builder=lambda row: (
            str(row.get("app_id")),
            str(row.get("country")),
            str(row.get("date")),
        ),
        record_date_field="date",
        window=window,
        dataset_name="app_active_users",
        expired_at=_utc_now_iso(),
    )

    payload = {
        "status": "success",
        "run_timestamp": args.run_timestamp,
        "facts_load_mode": args.facts_load_mode,
        "performance_end_date": args.performance_end_date,
        "artifact_path": str(artifact_path),
        "artifact_row_count": _count_json_array_items(artifact_path),
        "artifact_file_size_bytes": _file_size(artifact_path),
        "merge_metrics": merge_metrics,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

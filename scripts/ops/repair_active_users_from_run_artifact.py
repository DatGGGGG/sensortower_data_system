from __future__ import annotations

import argparse
import json
import sys
import tempfile
from collections import Counter
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_runtime import (  # noqa: E402
    APP_ACTIVE_USERS_CURRENT_STORE_FORMAT,
    _file_size,
    _iter_json_array_items,
    _iter_ndjson_items,
    _merge_app_active_users_metric_row,
    _replace_file_atomically,
    _temp_path_for_atomic_write,
    _time_series_current_manifest_path,
    _time_series_manifest_from_partition_stats,
    _time_series_partition_key_from_date_text,
    _time_series_partition_path,
    _utc_now_iso,
    _write_json_payload_atomic,
    _write_ndjson_items_atomic,
    _write_ndjson_items_to_path,
    iter_time_series_current_records,
)


def _active_users_key(row: dict) -> tuple[str, str, str]:
    return (
        str(row.get("app_id")),
        str(row.get("country")),
        str(row.get("date")),
    )


def _load_manifest_stats(current_store: Path) -> dict[tuple[int, int], dict]:
    manifest_path = _time_series_current_manifest_path(current_store)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing active-users current manifest: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") != APP_ACTIVE_USERS_CURRENT_STORE_FORMAT:
        raise ValueError(
            "Unsupported active-users current manifest format: "
            f"{manifest.get('format')}"
        )

    stats: dict[tuple[int, int], dict] = {}
    for partition in manifest.get("partitions", []):
        key = (int(partition["year"]), int(partition["month"]))
        stats[key] = {
            "row_count": int(partition.get("row_count") or 0),
            "min_date": partition.get("min_date"),
            "max_date": partition.get("max_date"),
        }
    return stats


def _write_partition_rows_atomic(partition_path: Path, rows: list[dict]) -> dict:
    partition_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _temp_path_for_atomic_write(partition_path)
    row_count = _write_ndjson_items_to_path(tmp_path, rows)
    _replace_file_atomically(tmp_path, partition_path)

    min_date = None
    max_date = None
    for row in rows:
        date_text = str(row.get("date") or "")
        if not date_text:
            continue
        if min_date is None or date_text < min_date:
            min_date = date_text
        if max_date is None or date_text > max_date:
            max_date = date_text

    return {"row_count": row_count, "min_date": min_date, "max_date": max_date}


def _repair_current_store_from_rows(
    *,
    current_store: Path,
    repair_rows_by_key: dict[tuple[str, str, str], dict],
) -> dict:
    partition_stats = _load_manifest_stats(current_store)
    repair_rows_by_partition: dict[tuple[int, int], dict[tuple[str, str, str], dict]] = {}

    for key, row in repair_rows_by_key.items():
        partition_key = _time_series_partition_key_from_date_text(key[2])
        repair_rows_by_partition.setdefault(partition_key, {})[key] = row

    touched_partition_count = 0
    repaired_row_count = 0
    inserted_row_count = 0
    unchanged_row_count = 0

    for partition_key, partition_repair_rows in sorted(repair_rows_by_partition.items()):
        partition_path = _time_series_partition_path(current_store, partition_key)
        current_rows: list[dict] = []
        remaining = dict(partition_repair_rows)

        if partition_path.exists():
            for current_row in _iter_ndjson_items(partition_path):
                key = _active_users_key(current_row)
                repair_row = remaining.pop(key, None)
                if repair_row is None:
                    current_rows.append(current_row)
                    continue

                merged_row = _merge_app_active_users_metric_row(current_row, repair_row)
                current_rows.append(merged_row)
                if merged_row == current_row:
                    unchanged_row_count += 1
                else:
                    repaired_row_count += 1

        for key, repair_row in sorted(remaining.items(), key=lambda item: (item[0][2], item[0])):
            current_rows.append(repair_row)
            inserted_row_count += 1

        partition_stats[partition_key] = _write_partition_rows_atomic(partition_path, current_rows)
        touched_partition_count += 1

    manifest = _time_series_manifest_from_partition_stats(
        partition_stats,
        manifest_format=APP_ACTIVE_USERS_CURRENT_STORE_FORMAT,
    )
    _write_json_payload_atomic(_time_series_current_manifest_path(current_store), manifest)

    return {
        "current_output_path": str(current_store),
        "current_manifest_path": str(_time_series_current_manifest_path(current_store)),
        "current_row_count": manifest["row_count"],
        "load_mode": "repair",
        "archived_row_count": 0,
        "repaired_overlap_record_count": repaired_row_count,
        "unchanged_overlap_record_count": unchanged_row_count,
        "inserted_overlap_record_count": inserted_row_count,
        "dropped_overlap_record_count": 0,
        "touched_partition_count": touched_partition_count,
        "updated_at": _utc_now_iso(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Repair canonical app-active-users current rows from a trusted run artifact "
            "without refetching SensorTower."
        )
    )
    parser.add_argument("--source-artifact", default="data/base/st_app_active_users_1783509210.json")
    parser.add_argument("--current-store", default="data/base/st_app_active_users_current")
    parser.add_argument("--history-archive", default="data/base/st_app_active_users_history.ndjson")
    parser.add_argument("--output-root", default="data")
    parser.add_argument("--repair-timestamp", type=int, required=True)
    parser.add_argument(
        "--date",
        action="append",
        required=True,
        help="Date to repair in YYYY-MM-DD. Pass multiple --date values for multiple dates.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_artifact = Path(args.source_artifact).resolve()
    current_store = Path(args.current_store).resolve()
    history_archive = Path(args.history_archive).resolve()
    output_root = Path(args.output_root).resolve()
    repair_dates = set(args.date)

    if not source_artifact.exists():
        raise SystemExit(f"Missing source artifact: {source_artifact}")
    if not repair_dates:
        raise SystemExit("At least one --date is required.")

    incremental_path = (
        output_root
        / "staging"
        / "incremental"
        / f"st_app_active_users_{args.repair_timestamp}.ndjson"
    )
    counts_by_date: Counter[str] = Counter()

    with tempfile.TemporaryDirectory(prefix="repair_active_users_") as tmp:
        repair_json_path = Path(tmp) / f"st_app_active_users_repair_{args.repair_timestamp}.json"
        repair_row_count = 0
        repair_rows_by_key: dict[tuple[str, str, str], dict] = {}

        with repair_json_path.open("w", encoding="utf-8") as handle:
            handle.write("[")
            first = True
            for row in _iter_json_array_items(source_artifact):
                date_text = str(row.get("date") or "")[:10]
                if date_text not in repair_dates:
                    continue
                if not first:
                    handle.write(",")
                first = False
                json.dump(row, handle, ensure_ascii=False)
                repair_rows_by_key[_active_users_key(row)] = row
                counts_by_date[date_text] += 1
                repair_row_count += 1
            handle.write("]")

        if repair_row_count == 0:
            raise SystemExit(
                f"No repair rows found in {source_artifact} for dates: {sorted(repair_dates)}"
            )

        merge_metrics = _repair_current_store_from_rows(
            current_store=current_store,
            repair_rows_by_key=repair_rows_by_key,
        )

    def _current_repaired_rows():
        partition_dates = set(repair_dates)

        for row in iter_time_series_current_records(
            current_store,
            manifest_format=APP_ACTIVE_USERS_CURRENT_STORE_FORMAT,
            dataset_label="App active users",
        ):
            if str(row.get("date") or "")[:10] in partition_dates:
                yield row

    incremental_row_count = _write_ndjson_items_atomic(incremental_path, _current_repaired_rows())

    payload = {
        "status": "success",
        "source_artifact": str(source_artifact),
        "current_store": str(current_store),
        "incremental_ndjson": str(incremental_path),
        "repair_timestamp": args.repair_timestamp,
        "repair_dates": sorted(repair_dates),
        "source_rows_used": repair_row_count,
        "incremental_rows_written": incremental_row_count,
        "incremental_file_size_bytes": _file_size(incremental_path),
        "counts_by_date": dict(sorted(counts_by_date.items())),
        "merge_metrics": merge_metrics,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

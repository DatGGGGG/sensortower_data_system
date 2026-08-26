from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_runtime import (  # noqa: E402
    APP_ACTIVE_USERS_CURRENT_STORE_FORMAT,
    _file_size,
    _write_ndjson_items_atomic,
    iter_time_series_current_records,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a date slice from the canonical app-active-users current store to NDJSON."
    )
    parser.add_argument(
        "--current-store",
        default="data/base/st_app_active_users_current",
        help="Path to the partitioned app active users current store.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="NDJSON file to write.",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Inclusive start date in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="Inclusive end date in YYYY-MM-DD.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    current_store = Path(args.current_store).resolve()
    output_path = Path(args.output_path).resolve()
    start_date = args.start_date
    end_date = args.end_date

    if end_date < start_date:
        raise SystemExit("--end-date must be >= --start-date")

    counts_by_date: Counter[str] = Counter()

    def _rows():
        for row in iter_time_series_current_records(
            current_store,
            manifest_format=APP_ACTIVE_USERS_CURRENT_STORE_FORMAT,
            dataset_label="App active users",
        ):
            date_text = str(row.get("date") or "")[:10]
            if start_date <= date_text <= end_date:
                counts_by_date[date_text] += 1
                yield row

    records_written = _write_ndjson_items_atomic(output_path, _rows())

    payload = {
        "status": "success",
        "current_store": str(current_store),
        "output_path": str(output_path),
        "start_date": start_date,
        "end_date": end_date,
        "records_written": records_written,
        "file_size_bytes": _file_size(output_path),
        "counts_by_date": dict(sorted(counts_by_date.items())),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

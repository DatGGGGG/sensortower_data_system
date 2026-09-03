from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_runtime import SOURCE_BASE_STAGE_ORDER, create_context, emit_and_exit, run_source_base_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run source-to-base Sensor Tower pipeline stages.")
    parser.add_argument("--config-path", default="config.json")
    parser.add_argument("--input-root", default=None)
    parser.add_argument("--output-root", default="data")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--run-timestamp", type=int, default=None)
    parser.add_argument("--trigger-mode", default="manual")
    parser.add_argument("--performance-end-date", default=None)
    parser.add_argument("--facts-load-mode", choices=["full", "daily", "weekly", "quarterly", "backfill"], default=None)
    parser.add_argument("--facts-start-date", default=None)
    parser.add_argument("--snapshot-start-year", type=int, default=None)
    parser.add_argument("--snapshot-end-year", type=int, default=None)
    parser.add_argument("--snapshot-regions", default=None)
    parser.add_argument("--fact-country-codes", default=None)
    parser.add_argument("--snapshot-category", default=None)
    parser.add_argument("--snapshot-custom-fields-filter-id", default=None)
    parser.add_argument("--snapshot-data-model", default=None)
    parser.add_argument("--snapshot-target-count", type=int, default=None)
    parser.add_argument("--force-fail-stage", default=None)
    parser.add_argument("--stage", choices=["all", *SOURCE_BASE_STAGE_ORDER], default="all")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    context = create_context(args)
    raise SystemExit(emit_and_exit(run_source_base_pipeline(context, stage=args.stage)))

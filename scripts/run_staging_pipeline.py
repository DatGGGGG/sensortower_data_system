from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_runtime import STAGING_STAGE_ORDER, create_context, emit_and_exit, run_staging_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run staging-layer Sensor Tower pipeline stages.")
    parser.add_argument("--config-path", default="config.json")
    parser.add_argument("--input-root", default=None)
    parser.add_argument("--output-root", default="data")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--run-timestamp", type=int, default=None)
    parser.add_argument("--trigger-mode", default="manual")
    parser.add_argument("--mapping-publisher-to-apps-top-down", default=None)
    parser.add_argument("--mapping-publisher-to-publisherids-revenue-multiplier-bottom-up", default=None)
    parser.add_argument("--mapping-app-to-revenue-multiplier-special-cases", default=None)
    parser.add_argument("--game-info-adjustment", default=None)
    parser.add_argument("--force-fail-stage", default=None)
    parser.add_argument("--mapping-artifact-manifest", default=None)
    parser.add_argument("--stage", choices=["all", *STAGING_STAGE_ORDER], default="all")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    context = create_context(args)
    raise SystemExit(emit_and_exit(run_staging_pipeline(context, stage=args.stage)))

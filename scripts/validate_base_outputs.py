from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_runtime import create_context, emit_and_exit, validate_base_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate generated source-to-base outputs.")
    parser.add_argument("--config-path", default="config.json")
    parser.add_argument("--output-root", default="data")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--run-timestamp", type=int, default=None)
    parser.add_argument("--trigger-mode", default="manual")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    context = create_context(args)
    raise SystemExit(emit_and_exit(validate_base_outputs(context)))

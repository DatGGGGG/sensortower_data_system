#!/usr/bin/env python3
"""Convert a CSV file to NDJSON without loading the whole file into memory."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert CSV rows to NDJSON.")
    parser.add_argument("--input", required=True, help="Input CSV path.")
    parser.add_argument("--output", required=True, help="Output NDJSON path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with input_path.open("r", encoding="utf-8-sig", newline="") as src:
        reader = csv.DictReader(src)
        with output_path.open("w", encoding="utf-8", newline="\n") as dst:
            for row in reader:
                dst.write(json.dumps(row, ensure_ascii=False))
                dst.write("\n")
                count += 1

    print(json.dumps({"input": str(input_path), "output": str(output_path), "rows": count}, indent=2))


if __name__ == "__main__":
    main()

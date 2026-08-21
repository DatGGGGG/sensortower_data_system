import argparse
import json
import os
import sys
import time
from typing import Any, Iterable, List


def render_bar(done: int, total: int, width: int = 30) -> str:
    if total <= 0:
        return "[" + "?" * width + "]"
    ratio = max(0.0, min(1.0, done / total))
    filled = int(ratio * width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def fmt_bytes(n: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024.0:
            return f"{f:.1f}{u}"
        f /= 1024.0
    return f"{f:.1f}PB"


def default_out_path(inp_path: str) -> str:
    base, ext = os.path.splitext(inp_path)
    # input could be .json, output becomes .ndjson
    return base + ".ndjson"


def main(inp: str, outp: str):
    total_size = os.path.getsize(inp)
    start = time.time()
    last_print = 0.0

    # Load the list (you confirmed this file is a list)
    with open(inp, "r", encoding="utf-8", errors="replace") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise SystemExit(f"Expected a JSON list at top-level, got: {type(data)}")

    total = len(data)
    print(f"Loaded {total:,} records from {inp}")

    with open(outp, "w", encoding="utf-8", newline="") as f_out:
        for i, obj in enumerate(data, start=1):
            f_out.write(json.dumps(obj, ensure_ascii=False) + "\n")

            now = time.time()
            if now - last_print >= 0.2:
                # progress based on record count; show file size too for context
                bar = render_bar(i, total)
                elapsed = now - start
                speed = i / elapsed if elapsed > 0 else 0.0
                pct = (i / total * 100.0) if total > 0 else 100.0
                sys.stdout.write(
                    f"\r{bar} {pct:6.2f}% records:{i:,}/{total:,}  {speed:,.1f}/s  input:{fmt_bytes(total_size)}"
                )
                sys.stdout.flush()
                last_print = now

    sys.stdout.write("\n")
    print(f"Done. Wrote NDJSON: {outp}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Convert top-level JSON list -> NDJSON (1 object per line).")
    ap.add_argument("input_json", help="Input JSON path (top-level must be a list)")
    ap.add_argument("-o", "--output", default=None, help="Output NDJSON path (default: same name with .ndjson)")
    args = ap.parse_args()

    inp = args.input_json
    outp = args.output or default_out_path(inp)

    main(inp, outp)

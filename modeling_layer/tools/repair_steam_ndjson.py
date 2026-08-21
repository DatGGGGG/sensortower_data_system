import argparse
import json
import re
from typing import Optional, Tuple

CTRL_RE = re.compile(r"[\x00-\x1f\x7f]")  # control chars
BAD_X_RE = re.compile(r"\\x([0-9A-Fa-f]{2})")
STRAY_BS_RE = re.compile(r"\\([^\"\\/bfnrtu])")  # backslash not starting a valid escape

def sanitize_basic(s: str) -> str:
    s = CTRL_RE.sub("", s)
    s = BAD_X_RE.sub(r"x\1", s)          # neutralize \xNN
    s = STRAY_BS_RE.sub(r"\\\\\1", s)    # fix stray backslashes
    return s

def repair_unescaped_quotes_in_description(line: str) -> str:
    """
    Heuristic repair:
    - find the JSON field "description":"...."
    - escape interior " characters that are not ending the description value.
    This fixes the common case where a description contains raw ".
    """
    key = '"description"'
    i = line.find(key)
    if i == -1:
        return line

    # find the first quote that begins the description string
    j = line.find(":", i)
    if j == -1:
        return line
    # skip spaces
    k = j + 1
    while k < len(line) and line[k].isspace():
        k += 1
    if k >= len(line) or line[k] != '"':
        return line

    # scan string value, escaping " that are not the closing quote
    out = []
    out.append(line[:k+1])  # include opening quote
    p = k + 1
    escaped = False
    while p < len(line):
        ch = line[p]
        if escaped:
            out.append(ch)
            escaped = False
            p += 1
            continue

        if ch == "\\":
            out.append(ch)
            escaped = True
            p += 1
            continue

        if ch == '"':
            # decide if this " is the closing quote of description value:
            # look ahead to next non-space char; if it's ',' or '}', treat as closing
            q = p + 1
            while q < len(line) and line[q].isspace():
                q += 1
            if q < len(line) and line[q] in [",", "}"]:
                out.append('"')
                out.append(line[p+1:])  # rest of line unchanged
                return "".join(out)
            else:
                # interior quote -> escape it
                out.append('\\"')
                p += 1
                continue

        out.append(ch)
        p += 1

    return line  # fallback

def try_load(line: str) -> Tuple[bool, Optional[dict], str]:
    s = sanitize_basic(line)
    s = repair_unescaped_quotes_in_description(s)
    try:
        obj = json.loads(s)
        return True, obj, s
    except Exception:
        return False, None, s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="input NDJSON")
    ap.add_argument("-o", "--output", default=None, help="output fixed NDJSON")
    ap.add_argument("-r", "--rejects", default=None, help="rejects file")
    args = ap.parse_args()

    out_path = args.output or (args.input + ".FIXED.ndjson")
    rej_path = args.rejects or (args.input + ".REJECTS.ndjson")

    ok_cnt = 0
    bad_cnt = 0

    with open(args.input, "r", encoding="utf-8", errors="replace") as fin, \
         open(out_path, "w", encoding="utf-8") as fout, \
         open(rej_path, "w", encoding="utf-8") as frej:

        for idx, raw in enumerate(fin, start=1):
            raw = raw.rstrip("\n")
            if not raw.strip():
                continue
            ok, obj, repaired = try_load(raw)
            if ok:
                fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                ok_cnt += 1
            else:
                frej.write(json.dumps({"line_no": idx, "raw": raw}, ensure_ascii=False) + "\n")
                bad_cnt += 1

    print(f"Done. OK={ok_cnt}, BAD={bad_cnt}")
    print(f"Wrote: {out_path}")
    print(f"Rejects: {rej_path}")

if __name__ == "__main__":
    main()

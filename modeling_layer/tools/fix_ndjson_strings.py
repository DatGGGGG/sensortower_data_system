import os
import sys
import time

def render_bar(done, total, width=30):
    if total <= 0:
        return "[" + "?" * width + "]"
    ratio = max(0.0, min(1.0, done / total))
    filled = int(ratio * width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"

def fmt_bytes(n):
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024.0:
            return f"{f:.1f}{u}"
        f /= 1024.0
    return f"{f:.1f}PB"

def default_out_path(inp: str) -> str:
    base, ext = os.path.splitext(inp)
    if ext.lower() != ".ndjson":
        return inp + ".FIXED.ndjson"
    return base + ".FIXED.ndjson"

def main(inp_path: str, out_path: str):
    total = os.path.getsize(inp_path)
    start = time.time()
    last = 0.0

    in_str = False
    esc = False
    depth = 0  # counts { } and [ ] at top level (when not in string)
    buf = []

    bytes_read = 0
    lines_out = 0
    repaired_newlines = 0
    repaired_cr = 0

    with open(inp_path, "rb") as f_in, open(out_path, "wb") as f_out:
        while True:
            chunk = f_in.read(1024 * 1024)
            if not chunk:
                break
            bytes_read += len(chunk)

            for b in chunk:
                ch = chr(b)

                # Handle LF / CR
                if ch == "\n":
                    if in_str:
                        # Escape newline inside a JSON string
                        buf.append("\\n")
                        repaired_newlines += 1
                    else:
                        # Record boundary only if we're not inside JSON structures
                        if depth == 0:
                            rec = "".join(buf).strip()
                            if rec:
                                f_out.write(rec.encode("utf-8", errors="replace") + b"\n")
                                lines_out += 1
                            buf = []
                        else:
                            # newline as whitespace inside object
                            buf.append(" ")
                    continue

                if ch == "\r":
                    if in_str:
                        buf.append("\\r")
                        repaired_cr += 1
                    else:
                        # ignore or convert to whitespace
                        buf.append(" ")
                    continue

                # Track escapes inside strings
                if esc:
                    buf.append(ch)
                    esc = False
                    continue

                if ch == "\\":
                    buf.append(ch)
                    if in_str:
                        esc = True
                    continue

                if ch == '"':
                    in_str = not in_str
                    buf.append(ch)
                    continue

                # Track nesting only when not inside string
                if not in_str:
                    if ch in "{[":
                        depth += 1
                    elif ch in "}]":
                        depth = max(0, depth - 1)

                buf.append(ch)

            now = time.time()
            if now - last >= 0.2:
                bar = render_bar(bytes_read, total)
                elapsed = now - start
                speed = bytes_read / elapsed if elapsed > 0 else 0.0
                pct = (bytes_read / total * 100.0) if total > 0 else 0.0
                sys.stdout.write(
                    f"\r{bar} {pct:6.2f}% {fmt_bytes(bytes_read)}/{fmt_bytes(total)} "
                    f"{fmt_bytes(speed)}/s out:{lines_out:,} repaired_nl:{repaired_newlines:,}"
                )
                sys.stdout.flush()
                last = now

        # flush any remaining buffered record
        rec = "".join(buf).strip()
        if rec:
            f_out.write(rec.encode("utf-8", errors="replace") + b"\n")
            lines_out += 1

    sys.stdout.write("\n")
    print(f"Done. Wrote: {out_path}")
    print(f"Records: {lines_out:,}")
    print(f"Repaired newlines in strings: {repaired_newlines:,} | Repaired CR: {repaired_cr:,}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fix_ndjson_strings.py <input.ndjson> [output.ndjson]")
        raise SystemExit(2)

    inp = sys.argv[1]
    outp = sys.argv[2] if len(sys.argv) >= 3 else default_out_path(inp)
    main(inp, outp)

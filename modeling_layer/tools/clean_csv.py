import csv
import os
import sys
import time

def smart_split(line: str):
    fields = []
    buf = []
    in_double = False
    esc = False
    depth_sq = 0   # []
    depth_curly = 0  # {}

    for ch in line:
        if esc:
            buf.append(ch)
            esc = False
            continue

        if ch == '\\':
            buf.append(ch)
            esc = True
            continue

        if ch == '"':
            in_double = not in_double
            buf.append(ch)
            continue

        if not in_double:
            if ch == '[':
                depth_sq += 1
            elif ch == ']':
                depth_sq = max(0, depth_sq - 1)
            elif ch == '{':
                depth_curly += 1
            elif ch == '}':
                depth_curly = max(0, depth_curly - 1)

        # split only when top-level
        if ch == ',' and (not in_double) and depth_sq == 0 and depth_curly == 0:
            fields.append(''.join(buf))
            buf = []
        else:
            buf.append(ch)

    fields.append(''.join(buf))
    return fields

def render_bar(done, total, width=32):
    if total <= 0:
        return "[????????????????????????????????]"
    ratio = min(max(done / total, 0.0), 1.0)
    filled = int(ratio * width)
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"

def fmt_bytes(n):
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024.0:
            return f"{f:.1f}{u}"
        f /= 1024.0
    return f"{f:.1f}PB"

def main(inp, outp, sink_col_name="description"):
    total_size = os.path.getsize(inp)
    start = time.time()
    last_print = 0.0

    with open(inp, 'r', encoding='utf-8', errors='replace', newline='') as f_in:
        header_line = f_in.readline()
        if not header_line:
            raise SystemExit("Empty input file")

        header = header_line.rstrip('\n').rstrip('\r').split(',')
        expected = len(header)

        try:
            sink_idx = header.index(sink_col_name)
        except ValueError:
            raise SystemExit(f"Sink column '{sink_col_name}' not found in header")

        with open(outp, 'w', encoding='utf-8', newline='') as f_out:
            w = csv.writer(f_out, quoting=csv.QUOTE_MINIMAL)
            w.writerow(header)

            # Progress: bytes consumed so far (tell() is reliable in text mode for our purpose here)
            line_no = 1
            bad_rows = 0

            while True:
                line = f_in.readline()
                if not line:
                    break
                line_no += 1

                line = line.rstrip('\n').rstrip('\r')
                if not line:
                    continue

                parts = smart_split(line)

                # Too many columns: merge overflow into sink column (description)
                if len(parts) > expected:
                    extra = parts[expected:]
                    parts = parts[:expected]
                    parts[sink_idx] = (parts[sink_idx] or "") + "," + ",".join(extra)
                    bad_rows += 1
                elif len(parts) < expected:
                    parts += [''] * (expected - len(parts))
                    bad_rows += 1

                w.writerow(parts)

                # update progress about 5x per second
                now = time.time()
                if now - last_print >= 0.2:
                    pos = f_in.tell()
                    bar = render_bar(pos, total_size)
                    elapsed = now - start
                    speed = pos / elapsed if elapsed > 0 else 0.0
                    pct = (pos / total_size * 100.0) if total_size > 0 else 0.0
                    msg = (
                        f"\r{bar} {pct:6.2f}%  "
                        f"{fmt_bytes(pos)}/{fmt_bytes(total_size)}  "
                        f"{fmt_bytes(speed)}/s  "
                        f"lines:{line_no:,}  bad:{bad_rows:,}"
                    )
                    sys.stdout.write(msg)
                    sys.stdout.flush()
                    last_print = now

            # final print
            pos = total_size
            elapsed = time.time() - start
            bar = render_bar(pos, total_size)
            msg = (
                f"\r{bar} 100.00%  "
                f"{fmt_bytes(total_size)}/{fmt_bytes(total_size)}  "
                f"elapsed:{elapsed:.1f}s  "
                f"lines:{line_no:,}  bad:{bad_rows:,}\n"
            )
            sys.stdout.write(msg)

    print(f"Done. Wrote cleaned CSV: {outp}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python clean_csv.py <input.csv> <output.csv>")
        raise SystemExit(2)

    main(sys.argv[1], sys.argv[2], sink_col_name="description")

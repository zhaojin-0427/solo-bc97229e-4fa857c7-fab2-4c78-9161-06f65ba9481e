#!/usr/bin/env python3
"""生成演示用 CSV：python3 gen_sample.py <输出目录> <文件数> <每文件行数>"""

import csv
import random
import sys
from pathlib import Path


def main():
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "sample")
    n_files = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    n_rows = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(42)
    base_ts = 1_760_000_000
    for i in range(n_files):
        path = out / f"site-batch-{i + 1}.csv"
        with path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["site", "ts", "reading"])
            for j in range(n_rows):
                site = f"site-{rng.randint(1, 8)}"
                ts = str(base_ts + i * n_rows + j)
                reading = f"{rng.uniform(-20, 45):.3f}"
                w.writerow([site, ts, reading])
        print(f"已生成 {path}（{n_rows} 行）")


if __name__ == "__main__":
    main()

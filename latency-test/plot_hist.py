#!/usr/bin/env python3
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def main(csv_path: str):
    if not os.path.isfile(csv_path):
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path)
    required_cols = {"size_bytes", "pattern", "round", "latency_usec"}
    if not required_cols.issubset(df.columns):
        print(f"CSV missing required columns: {required_cols}")
        sys.exit(1)

    sizes = [1024, 1024 * 1024 * 1024]
    titles = {1024: "1KB", 1024 * 1024 * 1024: "1GB"}

    # Use the latest entry per (size, pattern, round) in case CSV has multiple runs appended
    df = df.sort_index()  # original order; tail selection uses last occurrence

    for size in sizes:
        sub = df[df["size_bytes"] == size]
        if sub.empty:
            print(f"No rows for size {size}")
            continue

        labels = []
        values = []
        colors = []
        color_map = {"ZMQ": "tab:blue", "NCCL": "tab:orange"}

        for pattern in ["ZMQ", "NCCL"]:
            for r in [1, 2, 3]:
                sel = sub[(sub["pattern"] == pattern) & (sub["round"] == r)]
                if sel.empty:
                    continue
                val = float(sel.iloc[-1]["latency_usec"])  # latest
                labels.append(f"{pattern}-{r}")
                values.append(val)
                colors.append(color_map.get(pattern, "gray"))

        x = np.arange(len(values))
        plt.figure(figsize=(8, 4))
        plt.bar(x, values, color=colors)
        plt.xticks(x, labels, rotation=0)
        plt.ylabel("Latency (usec)")
        plt.title(f"Latency by round {titles[size]}")
        plt.tight_layout()
        out_name = f"hist_{titles[size].lower()}.png"
        plt.savefig(out_name, dpi=150)
        print(f"Saved {out_name}")


if __name__ == "__main__":
    csv = sys.argv[1] if len(sys.argv) > 1 else "results.csv"
    main(csv)



#!/usr/bin/env python3
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt


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

    for size in sizes:
        sub = df[df["size_bytes"] == size]
        if sub.empty:
            print(f"No rows for size {size}")
            continue

        plt.figure(figsize=(7, 4))
        for pattern, color in [("ZMQ", "tab:blue"), ("NCCL", "tab:orange")]:
            data = sub[sub["pattern"] == pattern]["latency_usec"]
            if data.empty:
                continue
            plt.hist(data, bins=5, alpha=0.6, label=pattern, color=color)

        plt.xlabel("Latency (usec)")
        plt.ylabel("Count")
        plt.title(f"Latency histogram {titles[size]}")
        plt.legend()
        out_name = f"hist_{titles[size].lower()}.png"
        plt.tight_layout()
        plt.savefig(out_name, dpi=150)
        print(f"Saved {out_name}")


if __name__ == "__main__":
    csv = sys.argv[1] if len(sys.argv) > 1 else "results.csv"
    main(csv)



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

    sizes = [512, 1024, 2048, 4096, 8192, 16384, 1024 * 1024 * 1024]
    titles = {512: "0.5KB", 1024: "1KB", 2048: "2KB", 4096: "4KB", 8192: "8KB", 16384: "16KB", 1024 * 1024 * 1024: "1GB"}

    # Use the latest entry per (size, pattern, round) in case CSV has multiple runs appended
    df = df.sort_index()  # original order; tail selection uses last occurrence

    # Per-size bar charts
    for size in sizes:
        sub = df[df["size_bytes"] == size]
        if sub.empty:
            print(f"No rows for size {size}")
            continue

        labels = []
        values = []
        colors = []
        color_map = {"ZMQ": "tab:blue", "NCCL": "tab:orange", "UCX": "tab:green"}

        for pattern in ["ZMQ", "NCCL", "UCX"]:
            for r in range(1, 21):
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
        # Means
        for pattern, color in [("ZMQ", "tab:blue"), ("NCCL", "tab:orange"), ("UCX", "tab:green")]:
            pdata = sub[sub["pattern"] == pattern]["latency_usec"].astype(float)
            if not pdata.empty:
                mean_val = pdata.mean()
                plt.axhline(mean_val, color=color, linestyle="--", linewidth=1.5, alpha=0.8, label=f"{pattern} mean")
        plt.legend()
        plt.tight_layout()
        out_name = f"hist_{titles[size].lower()}.png"
        plt.savefig(out_name, dpi=150)
        print(f"Saved {out_name}")

    # Combined figure: subplots for all sizes in one PNG
    cols = 3
    rows = int(np.ceil(len(sizes) / cols))
    fig, axes = plt.subplots(rows, cols, figsize=(cols * 4.5, rows * 3.2))
    axes = np.array(axes).reshape(-1)
    for idx, size in enumerate(sizes):
        ax = axes[idx]
        sub = df[df["size_bytes"] == size]
        labels = []
        values = []
        colors = []
        color_map = {"ZMQ": "tab:blue", "NCCL": "tab:orange", "UCX": "tab:green"}
        for pattern in ["ZMQ", "NCCL", "UCX"]:
            for r in range(1, 21):
                sel = sub[(sub["pattern"] == pattern) & (sub["round"] == r)]
                if sel.empty:
                    continue
                val = float(sel.iloc[-1]["latency_usec"])
                labels.append(f"{pattern}-{r}")
                values.append(val)
                colors.append(color_map.get(pattern, "gray"))
        x = np.arange(len(values))
        ax.bar(x, values, color=colors)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=0, fontsize=8)
        ax.set_ylabel("usec")
        ax.set_title(titles[size])
        # Means
        for pattern, color in [("ZMQ", "tab:blue"), ("NCCL", "tab:orange"), ("UCX", "tab:green")]:
            pdata = sub[sub["pattern"] == pattern]["latency_usec"].astype(float)
            if not pdata.empty:
                mean_val = pdata.mean()
                ax.axhline(mean_val, color=color, linestyle="--", linewidth=1.0, alpha=0.8)
    # Hide any unused axes
    for j in range(len(sizes), len(axes)):
        fig.delaxes(axes[j])
    fig.tight_layout()
    fig.savefig("hist_all_sizes.png", dpi=150)
    print("Saved hist_all_sizes.png")


if __name__ == "__main__":
    csv = sys.argv[1] if len(sys.argv) > 1 else "results.csv"
    main(csv)



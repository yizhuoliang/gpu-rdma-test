#!/usr/bin/env python3
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch
from matplotlib.lines import Line2D


def main(csv_path: str):
    if not os.path.isfile(csv_path):
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path)
    required_cols = {"size_bytes", "pattern", "round", "latency_usec"}
    if not required_cols.issubset(df.columns):
        print(f"CSV missing required columns: {required_cols}")
        sys.exit(1)

    # Ensure consistent ordering
    df = df.sort_values(["size_bytes", "pattern", "round"]).reset_index(drop=True)

    sizes = sorted(df["size_bytes"].unique())
    titles = {512: "0.5KB", 1024: "1KB", 2048: "2KB", 4096: "4KB", 8192: "8KB", 16384: "16KB", 65536: "64KB", 131072: "128KB", 262144: "256KB", 1048576: "1MB", 10485760: "10MB"}

    # Split patterns into transport and scope (e.g., UCX_local, ZMQ_remote)
    def parse_scope(p: str):
        if "_" in p:
            t, s = p.split("_", 1)
            return t, s
        return p, "remote"

    df["transport"], df["scope"] = zip(*df["pattern"].map(parse_scope))

    for scope in sorted(df["scope"].unique()):
        sdf = df[df["scope"] == scope]
        means = (
            sdf.groupby(["size_bytes", "transport"], as_index=False)["latency_usec"].mean()
        )
        pivot = means.pivot(index="size_bytes", columns="transport", values="latency_usec").fillna(np.nan)
        patterns = [p for p in ["UCX", "ZMQ"] if p in pivot.columns]
        if patterns:
            x = np.arange(len(pivot.index))
            width = 0.35
            plt.figure(figsize=(max(6, len(x) * 0.8), 4))
            for i, p in enumerate(patterns):
                vals = pivot[p].values
                plt.bar(x + (i - (len(patterns) - 1) / 2) * width, vals, width=width, label=p)
            plt.xticks(x, [titles.get(int(s), str(int(s))) for s in pivot.index], rotation=0)
            plt.ylabel("Mean latency (usec)")
            plt.title(f"UCX vs ZMQ fan-in ({scope}) mean over repeats")
            plt.legend()
            plt.tight_layout()
            out = f"compare_mean_ucx_vs_zmq_{scope}.png"
            plt.savefig(out, dpi=150, bbox_inches="tight")
            print(f"Saved {out}")

    # For each size, plot per-round bars side-by-side per transport (like plot_hist.py)
    for size in sizes:
        sub = df[df["size_bytes"] == size]
        if sub.empty:
            continue
        for scope in sorted(sub["scope"].unique()):
            ssub = sub[sub["scope"] == scope]
            labels = []
            values = []
            colors = []
            color_map = {"ZMQ": "tab:blue", "UCX": "tab:green"}
            for pattern in ["UCX", "ZMQ"]:
                sel = ssub[ssub["transport"] == pattern].sort_values("round")
                for _, row in sel.iterrows():
                    labels.append(f"{pattern}-{int(row['round'])}")
                    values.append(float(row["latency_usec"]))
                    colors.append(color_map.get(pattern, "gray"))
            if not values:
                continue
            x = np.arange(len(values))
            plt.figure(figsize=(8, 4))
            plt.bar(x, values, color=colors)
            plt.xticks([])
            plt.ylabel("Latency (usec)")
            plt.title(f"Latency by round {titles.get(int(size), str(int(size)))} ({scope})")
            # Means
            mean_handles = []
            for pattern, color in [("UCX", "tab:green"), ("ZMQ", "tab:blue")]:
                pdata = ssub[ssub["transport"] == pattern]["latency_usec"].astype(float)
                if not pdata.empty:
                    mean_val = pdata.mean()
                    plt.axhline(mean_val, color=color, linestyle="--", linewidth=1.5, alpha=0.8)
                    mean_handles.append(Line2D([0], [0], color=color, linestyle="--", linewidth=1.5, label=f"{pattern} mean"))
                    plt.annotate(f"{mean_val:.1f} usec", xy=(1.0, mean_val), xycoords=("axes fraction", "data"), xytext=(-4, 6), textcoords="offset points", ha="right", va="bottom", color=color, fontsize=8)
            bar_handles = [Patch(color="tab:green", label="UCX"), Patch(color="tab:blue", label="ZMQ")]
            plt.legend(handles=bar_handles + mean_handles, loc="upper center", ncol=3, bbox_to_anchor=(0.5, 1.15))
            plt.tight_layout(rect=[0, 0, 1, 0.92])
            out_name = f"compare_rounds_{titles.get(int(size), str(int(size))).lower()}_{scope}.png"
            plt.savefig(out_name, dpi=150, bbox_inches="tight")
            print(f"Saved {out_name}")

    # Combined figure: subplots for all sizes in one PNG (like plot_hist.py)
    if len(sizes) > 0:
        cols = 3
        rows = int(np.ceil(len(sizes) / cols))
        for scope in sorted(df["scope"].unique()):
            sdf = df[df["scope"] == scope]
            fig, axes = plt.subplots(rows, cols, figsize=(cols * 4.5, rows * 3.2))
            axes = np.array(axes).reshape(-1)
            for idx, size in enumerate(sizes):
                ax = axes[idx]
                sub = sdf[sdf["size_bytes"] == size]
                labels = []
                values = []
                colors = []
                color_map = {"UCX": "tab:green", "ZMQ": "tab:blue"}
                for pattern in ["UCX", "ZMQ"]:
                    sel = sub[sub["transport"] == pattern].sort_values("round")
                    for _, row in sel.iterrows():
                        labels.append(f"{pattern}-{int(row['round'])}")
                        values.append(float(row["latency_usec"]))
                        colors.append(color_map.get(pattern, "gray"))
                x = np.arange(len(values))
                ax.bar(x, values, color=colors)
                ax.set_xticks([])
                ax.set_ylabel("usec")
                ax.set_title(titles.get(int(size), str(int(size))) + f" ({scope})")
                # Means per transport
                for pattern, color in [("UCX", "tab:green"), ("ZMQ", "tab:blue")]:
                    pdata = sub[sub["transport"] == pattern]["latency_usec"].astype(float)
                    if not pdata.empty:
                        mean_val = pdata.mean()
                        ax.axhline(mean_val, color=color, linestyle="--", linewidth=1.0, alpha=0.8)
                        ax.annotate(
                            f"{mean_val:.1f}",
                            xy=(1.0, mean_val),
                            xycoords=("axes fraction", "data"),
                            xytext=(-4, 5),
                            textcoords="offset points",
                            ha="right",
                            va="bottom",
                            color=color,
                            fontsize=7,
                        )
            # Figure-level legend
            fig.legend(
                handles=[
                    Patch(color="tab:green", label="UCX"),
                    Patch(color="tab:blue", label="ZMQ"),
                    Line2D([0], [0], color="black", linestyle="--", label="Mean (per transport)")
                ],
                loc="upper center",
                ncol=3,
                bbox_to_anchor=(0.5, 1.02)
            )
            # Hide any unused axes
            for j in range(len(sizes), len(axes)):
                fig.delaxes(axes[j])
            fig.tight_layout(rect=[0, 0, 1, 0.95])
            out = f"compare_all_sizes_{scope}.png"
            fig.savefig(out, dpi=150, bbox_inches="tight")
            print(f"Saved {out}")


if __name__ == "__main__":
    csv = sys.argv[1] if len(sys.argv) > 1 else "results_ucx_zmq.csv"
    main(csv)



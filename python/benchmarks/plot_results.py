"""Plots run_benchmark.py's CSV output. See
docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import sys

import matplotlib

matplotlib.use("Agg")  # headless: this script only writes PNGs
import matplotlib.pyplot as plt
import pandas as pd


def plot_total_time_vs_agents(df: pd.DataFrame, out_path: str) -> None:
    totals = df[df["action"] == "__total__"]
    fig, ax = plt.subplots()
    for (backend, block_height), group in totals.groupby(["backend", "block_height"]):
        group = group.sort_values("num_agents")
        ax.plot(group["num_agents"], group["elapsed_ms"], marker="o", label=f"{backend} @ {block_height}")
    ax.set_xlabel("number of concurrent agents")
    ax.set_ylabel("total simulation time (ms)")
    ax.set_title("Total simulation time vs. agent count")
    ax.legend()
    fig.savefig(out_path)
    plt.close(fig)


def plot_action_latency(df: pd.DataFrame, out_path: str) -> None:
    per_action = df[df["action"] != "__total__"]
    medians = per_action.groupby(["action", "backend"])["elapsed_ms"].median().unstack("backend")
    fig, ax = plt.subplots()
    medians.plot(kind="bar", ax=ax)
    ax.set_xlabel("action")
    ax.set_ylabel("median latency (ms)")
    ax.set_title("Per-action median latency by backend")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def main() -> None:
    csv_path = sys.argv[1]
    df = pd.read_csv(csv_path)
    base = csv_path.rsplit(".", 1)[0]
    plot_total_time_vs_agents(df, f"{base}_total_time.png")
    plot_action_latency(df, f"{base}_action_latency.png")


if __name__ == "__main__":
    main()

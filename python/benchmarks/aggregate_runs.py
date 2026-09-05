"""Median and spread across repeated runs of the same sweep.

A single run of these benchmarks is not quotable — repeats on the same machine
have differed severalfold. This reads `<name>_rep*.csv` and reports, per
(backend, agent count), the median and the min-max range so a reader can see
how stable each number is.
"""

from __future__ import annotations

import argparse
import statistics
from collections import defaultdict
from pathlib import Path

import pandas as pd


def load_reps(directory: Path, name: str) -> list[pd.DataFrame]:
    """`_warmup.csv` is excluded on purpose: it is the run that populates both
    persistent caches, so including it would report a cold number."""
    return [pd.read_csv(p) for p in sorted(directory.glob(f"{name}_rep*.csv"))]


def totals_by_group(frames: list[pd.DataFrame], group: list[str], value: str) -> dict[tuple, list[float]]:
    series: dict[tuple, list[float]] = defaultdict(list)
    for frame in frames:
        rows = frame[frame["action"] == "__total__"] if "action" in frame.columns else frame
        for key, chunk in rows.groupby(group):
            series[key if isinstance(key, tuple) else (key,)].append(float(chunk[value].iloc[0]))
    return series


def summarize(series: dict[tuple, list[float]], scale: float = 1.0) -> pd.DataFrame:
    rows = []
    for key, values in sorted(series.items()):
        scaled = [v / scale for v in values]
        rows.append({
            **{f"key{i}": k for i, k in enumerate(key)},
            "n": len(scaled),
            "median": round(statistics.median(scaled), 2),
            "min": round(min(scaled), 2),
            "max": round(max(scaled), 2),
            "spread_x": round(max(scaled) / min(scaled), 2) if min(scaled) else float("inf"),
        })
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--name", required=True, help="sweep name, e.g. core")
    parser.add_argument("--group", default="backend,num_agents")
    parser.add_argument("--value", default="elapsed_ms")
    parser.add_argument("--scale", type=float, default=1000.0, help="divide by this (ms -> s)")
    args = parser.parse_args()

    frames = load_reps(args.directory, args.name)
    if not frames:
        raise SystemExit(f"no {args.name}_rep*.csv under {args.directory}")
    group = args.group.split(",")
    table = summarize(totals_by_group(frames, group, args.value), args.scale)
    table.columns = group + list(table.columns[len(group):])
    print(f"{args.name}: {len(frames)} repetitions")
    print(table.to_string(index=False))


if __name__ == "__main__":
    main()

"""Analyze standalone riblt round duration as a function of (a) the set
difference it reconciles and (b) the set size, reusing the metrics produced by
the existing experiment harness (run_experiment.sh / run_similarity_sweep.sh).

The similarity knob already controls the difference: with each node holding
`size` keys and Jaccard similarity J, the data generator (generate_data.py)
shares intersection = floor(2*size*J/(1+J)) keys, so the symmetric difference is

    difference = 2 * (size - intersection)        # J=0 -> 2*size, J=1 -> 0

Size is recoverable from the run_id (".._n<SIZE>_.."); similarity is a column.

Usage:
  # difference plot from the standard fixed-size sweep:
  python3 scripts/analyze_riblt_scaling.py metrics_output

  # size plot overlaying labelled size-sweep dirs (fixed-diff vs fixed-sim):
  python3 scripts/analyze_riblt_scaling.py \
      metrics_output fixed-difference=sweep_fixdiff fixed-similarity=sweep_fixsim
"""

import argparse
import math
import os
import re
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

SIZE_RE = re.compile(r"n(\d+)")


def intersection(size, sim):
    return math.floor(2 * size * sim / (1 + sim)) if (1 + sim) else 0


def difference(size, sim):
    return 2 * (size - intersection(size, sim))


def parse_size(run_id, run_dir_name):
    for source in (run_id, run_dir_name):
        if isinstance(source, str):
            m = SIZE_RE.search(source)
            if m:
                return int(m.group(1))
    return None


def load_rows(metrics_root, label, protocol_filter):
    rows = []
    for run_dir in Path(metrics_root).iterdir():
        if not run_dir.is_dir():
            continue
        metric_file = run_dir / "reconciliation_round_duration_seconds.csv"
        if not metric_file.exists():
            continue
        df = pd.read_csv(metric_file)
        for _, row in df.iterrows():
            protocol = row.get("protocol")
            if protocol_filter and protocol != protocol_filter:
                continue
            value = pd.to_numeric(row.get("value"), errors="coerce")
            sim = pd.to_numeric(row.get("similarity"), errors="coerce")
            size = parse_size(row.get("run_id"), run_dir.name)
            if pd.isna(value) or value < 0 or pd.isna(sim) or size is None:
                continue
            rows.append(
                {
                    "source": label,
                    "protocol": protocol,
                    "trial": str(row.get("trial")),
                    "similarity": float(sim),
                    "size": size,
                    "difference": difference(size, float(sim)),
                    "round_duration_seconds": float(value),
                }
            )
    return pd.DataFrame(rows)


def aggregate(df, key):
    g = df.groupby(key, as_index=False).agg(
        mean_seconds=("round_duration_seconds", "mean"),
        std_seconds=("round_duration_seconds", "std"),
        min_seconds=("round_duration_seconds", "min"),
        max_seconds=("round_duration_seconds", "max"),
        samples=("round_duration_seconds", "count"),
    )
    g["std_seconds"] = g["std_seconds"].fillna(0)
    return g


def plot_vs_difference(df, output_dir):
    # Use only fixed-size sources: a difference sweep lives at a single size but
    # spans many differences. Size-sweep sources span multiple sizes and would
    # inject stray points / inflate sample counts here, so exclude them.
    if df.empty:
        return
    fixed_size_sources = [s for s, g in df.groupby("source") if g["size"].nunique() == 1]
    sub = df[df["source"].isin(fixed_size_sources)]
    if sub.empty:
        return
    target_size = sub["size"].mode().iloc[0]
    sub = sub[sub["size"] == target_size]
    summary = aggregate(sub, "difference").sort_values("difference")
    summary.insert(0, "size", target_size)
    summary.to_csv(
        os.path.join(output_dir, "riblt_duration_vs_difference.csv"), index=False
    )

    plt.figure(figsize=(10, 6))
    mean = summary["mean_seconds"]
    yerr = [mean - summary["min_seconds"], summary["max_seconds"] - mean]
    plt.errorbar(
        summary["difference"], mean, yerr=yerr, marker="o", capsize=3,
        label=f"riblt (n={target_size:,})",
    )
    plt.xlabel("Reconciled set difference (keys)")
    plt.ylabel("Mean reconciliation round duration (s)")
    plt.title("RIBLT round duration vs set difference (fixed set size)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "riblt_duration_vs_difference.pdf"))
    plt.close()


def plot_vs_size(df, output_dir):
    # One line per source label (e.g. fixed-difference vs fixed-similarity).
    sources = [s for s in df["source"].unique() if s]
    if not sources:
        return
    plt.figure(figsize=(10, 6))
    all_summaries = []
    for source in sorted(sources):
        sub = df[df["source"] == source]
        if sub["size"].nunique() < 2:
            continue  # not a size sweep
        summary = aggregate(sub, "size").sort_values("size")
        summary.insert(0, "source", source)
        all_summaries.append(summary)
        mean = summary["mean_seconds"]
        yerr = [mean - summary["min_seconds"], summary["max_seconds"] - mean]
        plt.errorbar(
            summary["size"], mean, yerr=yerr, marker="o", capsize=3, label=source
        )
    if not all_summaries:
        plt.close()
        return
    pd.concat(all_summaries).to_csv(
        os.path.join(output_dir, "riblt_duration_vs_size.csv"), index=False
    )
    plt.xlabel("Set size (keys per node)")
    plt.ylabel("Mean reconciliation round duration (s)")
    plt.title("RIBLT round duration vs set size")
    plt.xscale("log")
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _p: f"{int(v):,}"))
    plt.grid(True, which="both")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "riblt_duration_vs_size.pdf"))
    plt.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "sources",
        nargs="+",
        help="metrics dirs; either DIR or LABEL=DIR (label used in the size plot legend)",
    )
    parser.add_argument("--protocol", default="riblt")
    parser.add_argument("--output-dir", default="metrics_output/analysis")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    frames = []
    for spec in args.sources:
        if "=" in spec:
            label, path = spec.split("=", 1)
        else:
            label, path = Path(spec).name, spec
        frames.append(load_rows(path, label, args.protocol))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if df.empty:
        print("no matching rows found")
        return

    df.to_csv(os.path.join(args.output_dir, "riblt_scaling_rows.csv"), index=False)
    plot_vs_difference(df, args.output_dir)
    plot_vs_size(df, args.output_dir)
    print(f"Wrote riblt scaling analysis to {args.output_dir}")


if __name__ == "__main__":
    main()

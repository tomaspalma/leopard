import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

SUPPORTED_PROTOCOLS = ["riblt", "merkle", "rbf_riblt", "rf_riblt"]


def parse_labels(label_str):
    parsed = {}
    if not isinstance(label_str, str):
        return parsed
    for pair in label_str.split(";"):
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def load_cpu_rows(metrics_root):
    rows = []
    for run_dir in Path(metrics_root).iterdir():
        if not run_dir.is_dir():
            continue

        metric_file = run_dir / "process_cpu_delta_seconds.csv"
        if not metric_file.exists():
            continue

        df = pd.read_csv(metric_file)
        if df.empty:
            continue

        for _, row in df.iterrows():
            labels = parse_labels(row.get("labels", ""))
            protocol = row.get("protocol")
            trial = row.get("trial")
            similarity = row.get("similarity")
            run_id = row.get("run_id")

            rows.append(
                {
                    "run_dir": run_dir.name,
                    "iteration": pd.to_numeric(row.get("iteration"), errors="coerce"),
                    "timestamp": pd.to_numeric(row.get("timestamp"), errors="coerce"),
                    "cpu_seconds": pd.to_numeric(row.get("value"), errors="coerce"),
                    "node": row.get("node", "unknown"),
                    "protocol": protocol
                    if isinstance(protocol, str) and protocol
                    else labels.get("protocol", "unknown"),
                    "trial": str(trial)
                    if pd.notna(trial) and str(trial)
                    else labels.get("trial", "unknown"),
                    "similarity": str(similarity)
                    if pd.notna(similarity) and str(similarity)
                    else labels.get("similarity", "unknown"),
                    "run_id": run_id
                    if isinstance(run_id, str) and run_id
                    else labels.get("run_id", run_dir.name),
                }
            )
    return pd.DataFrame(rows)


def aggregate_cpu(df):
    if df.empty:
        return pd.DataFrame()

    df = df[df["protocol"].isin(SUPPORTED_PROTOCOLS)].copy()
    df = df[df["iteration"].notna() & df["cpu_seconds"].notna()]
    df["cpu_seconds"] = df["cpu_seconds"].clip(lower=0)
    df["cpu_ms"] = df["cpu_seconds"] * 1000.0
    df["similarity_numeric"] = pd.to_numeric(df["similarity"], errors="coerce")

    # Sum CPU across all nodes per (run, protocol, trial, similarity, iteration),
    # then take the median across trials/runs to get the typical cost per round.
    per_round = (
        df.groupby(["run_id", "protocol", "trial", "similarity_numeric", "iteration"], as_index=False)["cpu_ms"]
        .sum()
        .rename(columns={"cpu_ms": "total_cpu_ms"})
    )

    summary = per_round.groupby(["protocol", "similarity_numeric", "iteration"], as_index=False).agg(
        mean_cpu_ms=("total_cpu_ms", "mean"),
        median_cpu_ms=("total_cpu_ms", "median"),
        std_cpu_ms=("total_cpu_ms", "std"),
        min_cpu_ms=("total_cpu_ms", "min"),
        max_cpu_ms=("total_cpu_ms", "max"),
        samples=("total_cpu_ms", "count"),
    )
    summary["std_cpu_ms"] = summary["std_cpu_ms"].fillna(0)
    return summary.sort_values(["protocol", "similarity_numeric", "iteration"])


def apply_log_plain_ticks():
    ax = plt.gca()
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))


def plot_cpu_per_round(summary, similarity_filter, output_path):
    if summary.empty:
        return

    if similarity_filter is not None:
        subset = summary[summary["similarity_numeric"] == similarity_filter]
    else:
        # Aggregate across all similarity levels
        subset = (
            summary.groupby(["protocol", "iteration"], as_index=False)
            .agg(
                median_cpu_ms=("median_cpu_ms", "median"),
                min_cpu_ms=("min_cpu_ms", "min"),
                max_cpu_ms=("max_cpu_ms", "max"),
            )
        )

    if subset.empty:
        return

    plt.figure(figsize=(10, 6))
    for protocol, group in subset.groupby("protocol"):
        group = group.sort_values("iteration")
        median = group["median_cpu_ms"]
        yerr = [median - group["min_cpu_ms"], group["max_cpu_ms"] - median]
        plt.errorbar(
            group["iteration"],
            median,
            yerr=yerr,
            marker="o",
            capsize=3,
            label=protocol,
        )

    sim_label = f" (similarity={similarity_filter})" if similarity_filter is not None else " (all similarities)"
    plt.xlabel("Round (iteration)")
    plt.ylabel("Total CPU Time (ms)")
    plt.title(f"CPU Usage Per Round{sim_label}")
    apply_log_plain_ticks()
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def plot_cpu_by_similarity(summary, output_path):
    if summary.empty:
        return

    # Total CPU across all rounds, aggregated per protocol+similarity
    totals = (
        summary.groupby(["protocol", "similarity_numeric"], as_index=False)
        .agg(
            total_median_cpu_ms=("median_cpu_ms", "sum"),
            min_cpu_ms=("min_cpu_ms", "min"),
            max_cpu_ms=("max_cpu_ms", "max"),
        )
        .sort_values(["protocol", "similarity_numeric"])
    )

    plt.figure(figsize=(10, 6))
    for protocol, group in totals.groupby("protocol"):
        group = group.sort_values("similarity_numeric")
        median = group["total_median_cpu_ms"]
        yerr = [median - group["min_cpu_ms"], group["max_cpu_ms"] - median]
        plt.errorbar(
            group["similarity_numeric"],
            median,
            yerr=yerr,
            marker="o",
            capsize=3,
            label=protocol,
        )

    plt.xlabel("Similarity (Jaccard)")
    plt.xlim(-0.03, 1.03)
    plt.ylabel("Total CPU Time (ms)")
    plt.title("Total CPU Usage vs Similarity")
    apply_log_plain_ticks()
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Plot CPU usage per round across protocols")
    parser.add_argument(
        "metrics_root",
        nargs="?",
        default="metrics_output",
        help="Directory containing per-run metrics subdirectories",
    )
    parser.add_argument(
        "--output-dir",
        default="metrics_output/analysis",
        help="Output directory for plots and summary files",
    )
    parser.add_argument(
        "--similarity",
        type=float,
        default=None,
        help="Filter plots to a specific similarity value (e.g. 0.9)",
    )
    args = parser.parse_args()

    df = load_cpu_rows(args.metrics_root)
    if df.empty:
        print("No CPU data found.")
        return

    summary = aggregate_cpu(df)
    os.makedirs(args.output_dir, exist_ok=True)

    summary.to_csv(os.path.join(args.output_dir, "cpu_usage_per_round.csv"), index=False)

    plot_cpu_per_round(
        summary,
        args.similarity,
        os.path.join(args.output_dir, "cpu_usage_per_round.pdf"),
    )
    plot_cpu_by_similarity(
        summary,
        os.path.join(args.output_dir, "cpu_total_vs_similarity.pdf"),
    )

    print(f"Wrote CPU usage analysis to {args.output_dir}")
    if not summary.empty:
        print("\nCPU usage summary (first 20 rows):")
        print(summary.head(20).to_string(index=False))


if __name__ == "__main__":
    main()

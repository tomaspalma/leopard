import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

SUPPORTED_PROTOCOLS = ["riblt", "merkle", "rbf_riblt"]


def load_round_duration_rows(metrics_root):
    rows = []
    for run_dir in Path(metrics_root).iterdir():
        if not run_dir.is_dir():
            continue

        metric_file = run_dir / "reconciliation_round_duration_seconds.csv"
        if not metric_file.exists():
            continue

        df = pd.read_csv(metric_file)
        if df.empty:
            continue

        for _, row in df.iterrows():
            protocol = row.get("protocol")
            trial = row.get("trial")
            similarity = row.get("similarity")
            run_id = row.get("run_id")
            value = pd.to_numeric(row.get("value"), errors="coerce")

            if pd.isna(value) or value < 0:
                continue

            rows.append(
                {
                    "run_dir": run_dir.name,
                    "iteration": pd.to_numeric(row.get("iteration"), errors="coerce"),
                    "round_duration_seconds": value,
                    "protocol": protocol if isinstance(protocol, str) and protocol else "unknown",
                    "trial": str(trial) if pd.notna(trial) else "unknown",
                    "similarity": str(similarity) if pd.notna(similarity) else "unknown",
                    "run_id": run_id if isinstance(run_id, str) and run_id else run_dir.name,
                }
            )

    return pd.DataFrame(rows)


def make_summary(df):
    if df.empty:
        return pd.DataFrame(
            columns=[
                "protocol",
                "similarity",
                "mean_round_duration_seconds",
                "std_round_duration_seconds",
                "median_round_duration_seconds",
                "rounds",
                "max_round_duration_seconds",
                "min_round_duration_seconds",
            ]
        )

    df = df[df["protocol"].isin(SUPPORTED_PROTOCOLS)].copy()
    df["similarity_numeric"] = pd.to_numeric(df["similarity"], errors="coerce")

    summary = df.groupby(["protocol", "similarity_numeric"], as_index=False).agg(
        mean_round_duration_seconds=("round_duration_seconds", "mean"),
        std_round_duration_seconds=("round_duration_seconds", "std"),
        median_round_duration_seconds=("round_duration_seconds", "median"),
        rounds=("round_duration_seconds", "count"),
        max_round_duration_seconds=("round_duration_seconds", "max"),
        min_round_duration_seconds=("round_duration_seconds", "min"),
    )
    summary["std_round_duration_seconds"] = summary["std_round_duration_seconds"].fillna(0)
    summary = summary.rename(columns={"similarity_numeric": "similarity"})
    return summary.sort_values(["protocol", "similarity"])


def make_protocol_comparison(summary):
    if summary.empty:
        return pd.DataFrame(
            columns=[
                "similarity",
                "riblt_mean_round_duration_seconds",
                "merkle_mean_round_duration_seconds",
                "riblt_std_round_duration_seconds",
                "merkle_std_round_duration_seconds",
                "riblt_rounds",
                "merkle_rounds",
                "riblt_minus_merkle_seconds",
                "riblt_to_merkle_ratio",
            ]
        )

    pivot = summary.pivot_table(
        index="similarity",
        columns="protocol",
        values=["mean_round_duration_seconds", "std_round_duration_seconds", "rounds"],
        aggfunc="first",
    )

    def get_metric(metric_name, protocol_name):
        key = (metric_name, protocol_name)
        if key in pivot.columns:
            return pivot[key]
        return pd.Series(index=pivot.index, dtype=float)

    comparison = pd.DataFrame(
        {
            "similarity": pivot.index,
            "riblt_mean_round_duration_seconds": get_metric("mean_round_duration_seconds", "riblt"),
            "merkle_mean_round_duration_seconds": get_metric("mean_round_duration_seconds", "merkle"),
            "riblt_std_round_duration_seconds": get_metric("std_round_duration_seconds", "riblt"),
            "merkle_std_round_duration_seconds": get_metric("std_round_duration_seconds", "merkle"),
            "riblt_rounds": get_metric("rounds", "riblt"),
            "merkle_rounds": get_metric("rounds", "merkle"),
        }
    ).reset_index(drop=True)

    comparison["riblt_minus_merkle_seconds"] = (
        comparison["riblt_mean_round_duration_seconds"]
        - comparison["merkle_mean_round_duration_seconds"]
    )
    comparison["riblt_to_merkle_ratio"] = comparison[
        "riblt_mean_round_duration_seconds"
    ] / comparison["merkle_mean_round_duration_seconds"].replace({0: pd.NA})
    return comparison.sort_values("similarity")


def plot_summary(summary, output_dir):
    if summary.empty:
        return

    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 6))
    for protocol, group in summary.groupby("protocol"):
        group = group.sort_values("similarity")
        mean = group["mean_round_duration_seconds"]
        yerr = [
            mean - group["min_round_duration_seconds"],
            group["max_round_duration_seconds"] - mean,
        ]
        plt.errorbar(
            group["similarity"],
            mean,
            yerr=yerr,
            marker="o",
            capsize=3,
            label=protocol,
        )

    plt.xlabel("Similarity (Jaccard)")
    plt.ylabel("Mean Reconciliation Round Duration (seconds)")
    plt.title("Reconciliation Round Duration vs Similarity")
    plt.yscale("log")
    ax = plt.gca()
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda value, _pos: f"{value:g}")
    )
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "reconciliation_duration_vs_similarity.pdf"))
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze reconciliation round duration across similarity levels"
    )
    parser.add_argument(
        "metrics_root",
        nargs="?",
        default="metrics_output",
        help="Directory containing per-run metrics subdirectories",
    )
    parser.add_argument(
        "--output-dir",
        default="metrics_output/analysis",
        help="Output directory for summary files",
    )
    args = parser.parse_args()

    df = load_round_duration_rows(args.metrics_root)
    summary = make_summary(df)

    os.makedirs(args.output_dir, exist_ok=True)
    df.to_csv(os.path.join(args.output_dir, "round_duration_by_trial.csv"), index=False)
    summary.to_csv(
        os.path.join(args.output_dir, "duration_summary_by_similarity.csv"), index=False
    )

    comparison = make_protocol_comparison(summary)
    comparison.to_csv(
        os.path.join(args.output_dir, "protocol_duration_comparison_by_similarity.csv"),
        index=False,
    )

    plot_summary(summary, args.output_dir)
    print(f"Wrote duration analysis to {args.output_dir}")
    if not summary.empty:
        print(summary.to_string(index=False))


if __name__ == "__main__":
    main()

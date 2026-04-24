import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

SUPPORTED_PROTOCOLS = ["riblt", "merkle", "rbf_riblt"]


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


def load_reconciliation_rows(metrics_root):
    rows = []
    for run_dir in Path(metrics_root).iterdir():
        if not run_dir.is_dir():
            continue

        metric_file = run_dir / "reconciliation_completed.csv"
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
                    "timestamp_ms": pd.to_numeric(
                        row.get("timestamp"), errors="coerce"
                    ),
                    "value": pd.to_numeric(row.get("value"), errors="coerce"),
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


def build_round_durations(df):
    if df.empty:
        return pd.DataFrame()

    filtered = df[
        (df["value"] > 0) & df["iteration"].notna() & df["timestamp_ms"].notna()
    ].copy()

    if filtered.empty:
        return pd.DataFrame()

    rounds = (
        filtered.groupby(
            ["run_id", "protocol", "trial", "similarity", "iteration"], as_index=False
        )["timestamp_ms"]
        .max()
        .rename(columns={"timestamp_ms": "round_completed_timestamp_ms"})
    )

    rounds = rounds[rounds["protocol"].isin(SUPPORTED_PROTOCOLS)].copy()
    rounds["similarity_numeric"] = pd.to_numeric(rounds["similarity"], errors="coerce")

    # Iteration counters come from per-target export cycles and may not be globally
    # monotonic for a run. Sort by observed completion timestamp to build durations.
    rounds = rounds.sort_values(["run_id", "protocol", "round_completed_timestamp_ms"])
    rounds["round_duration_ms"] = rounds.groupby(["run_id", "protocol"])[
        "round_completed_timestamp_ms"
    ].diff()
    rounds["round_duration_ms"] = rounds["round_duration_ms"].fillna(
        rounds["round_completed_timestamp_ms"]
    )
    rounds["round_duration_ms"] = rounds["round_duration_ms"].clip(lower=0)
    rounds["round_duration_seconds"] = rounds["round_duration_ms"] / 1000.0
    return rounds


def make_summary(rounds):
    if rounds.empty:
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

    summary = rounds.groupby(["protocol", "similarity_numeric"], as_index=False).agg(
        mean_round_duration_seconds=("round_duration_seconds", "mean"),
        std_round_duration_seconds=("round_duration_seconds", "std"),
        median_round_duration_seconds=("round_duration_seconds", "median"),
        rounds=("round_duration_seconds", "count"),
        max_round_duration_seconds=("round_duration_seconds", "max"),
        min_round_duration_seconds=("round_duration_seconds", "min"),
    )
    summary["std_round_duration_seconds"] = summary[
        "std_round_duration_seconds"
    ].fillna(0)
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
            "riblt_mean_round_duration_seconds": get_metric(
                "mean_round_duration_seconds", "riblt"
            ),
            "merkle_mean_round_duration_seconds": get_metric(
                "mean_round_duration_seconds", "merkle"
            ),
            "riblt_std_round_duration_seconds": get_metric(
                "std_round_duration_seconds", "riblt"
            ),
            "merkle_std_round_duration_seconds": get_metric(
                "std_round_duration_seconds", "merkle"
            ),
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
    plt.savefig(os.path.join(output_dir, "reconciliation_duration_vs_similarity.png"))
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

    completed_df = load_reconciliation_rows(args.metrics_root)
    rounds = build_round_durations(completed_df)

    os.makedirs(args.output_dir, exist_ok=True)
    rounds.to_csv(
        os.path.join(args.output_dir, "round_duration_by_trial.csv"), index=False
    )

    summary = make_summary(rounds)
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

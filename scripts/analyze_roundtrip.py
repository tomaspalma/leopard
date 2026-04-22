import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
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


def load_metric_rows(metrics_root, metric_name):
    rows = []
    for run_dir in Path(metrics_root).iterdir():
        if not run_dir.is_dir():
            continue

        metric_file = run_dir / f"{metric_name}.csv"
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


def build_round_trip_counts(df):
    if df.empty:
        return pd.DataFrame()

    message_rows = df[df["iteration"].notna() & df["value"].notna()].copy()
    if message_rows.empty:
        return pd.DataFrame()

    message_rows = message_rows[
        message_rows["protocol"].isin(SUPPORTED_PROTOCOLS)
    ].copy()
    message_rows["similarity_numeric"] = pd.to_numeric(
        message_rows["similarity"], errors="coerce"
    )

    # Each row corresponds to one completed reconciliation round for one neighbor.
    # Value already represents the per-round counter exported at reconciliation finish.
    rounds = message_rows.rename(columns={"value": "round_trips"}).copy()
    return rounds


def make_round_trip_summary(round_trips):
    if round_trips.empty:
        return pd.DataFrame(
            columns=[
                "protocol",
                "similarity",
                "mean_round_trips",
                "std_round_trips",
                "median_round_trips",
                "trials",
                "max_round_trips",
                "min_round_trips",
            ]
        )

    summary = round_trips.groupby(
        ["protocol", "similarity_numeric"], as_index=False
    ).agg(
        mean_round_trips=("round_trips", "mean"),
        std_round_trips=("round_trips", "std"),
        median_round_trips=("round_trips", "median"),
        trials=("round_trips", "count"),
        max_round_trips=("round_trips", "max"),
        min_round_trips=("round_trips", "min"),
    )
    summary["std_round_trips"] = summary["std_round_trips"].fillna(0)
    summary = summary.rename(columns={"similarity_numeric": "similarity"})
    return summary.sort_values(["protocol", "similarity"])


def make_round_trip_protocol_comparison(summary):
    if summary.empty:
        return pd.DataFrame(
            columns=[
                "similarity",
                "riblt_mean_round_trips",
                "merkle_mean_round_trips",
                "riblt_std_round_trips",
                "merkle_std_round_trips",
                "riblt_trials",
                "merkle_trials",
                "riblt_minus_merkle_round_trips",
                "riblt_to_merkle_ratio",
            ]
        )

    pivot = summary.pivot_table(
        index="similarity",
        columns="protocol",
        values=["mean_round_trips", "std_round_trips", "trials"],
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
            "riblt_mean_round_trips": get_metric("mean_round_trips", "riblt"),
            "merkle_mean_round_trips": get_metric("mean_round_trips", "merkle"),
            "riblt_std_round_trips": get_metric("std_round_trips", "riblt"),
            "merkle_std_round_trips": get_metric("std_round_trips", "merkle"),
            "riblt_trials": get_metric("trials", "riblt"),
            "merkle_trials": get_metric("trials", "merkle"),
        }
    ).reset_index(drop=True)

    comparison["riblt_minus_merkle_round_trips"] = (
        comparison["riblt_mean_round_trips"] - comparison["merkle_mean_round_trips"]
    )
    comparison["riblt_to_merkle_ratio"] = comparison[
        "riblt_mean_round_trips"
    ] / comparison["merkle_mean_round_trips"].replace({0: pd.NA})
    return comparison.sort_values("similarity")


def plot_round_trip_summary(summary, output_dir):
    if summary.empty:
        return

    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 6))
    for protocol, group in summary.groupby("protocol"):
        group = group.sort_values("similarity")
        plt.errorbar(
            group["similarity"],
            group["mean_round_trips"],
            yerr=group["std_round_trips"],
            marker="o",
            capsize=3,
            label=protocol,
        )

    plt.xlabel("Similarity (Jaccard)")
    plt.ylabel("Mean Reconciliation Round Trips")
    plt.title("Reconciliation Round Trips vs Similarity")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "reconciliation_round_trips_vs_similarity.png")
    )
    plt.close()


def print_missing_metric_hint(metrics_root):
    print("No protocol round-trip metrics found.")
    print(
        "Run experiments after this change so each run contains protocol_round_trip_count.csv under metrics_output/<run_id>/"
    )
    print(f"Searched in: {metrics_root}")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze protocol message round trips across similarity levels"
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

    messages_df = load_metric_rows(args.metrics_root, "protocol_round_trip_count")
    round_trips = build_round_trip_counts(messages_df)

    if messages_df.empty:
        print_missing_metric_hint(args.metrics_root)
        return

    os.makedirs(args.output_dir, exist_ok=True)
    round_trips.to_csv(
        os.path.join(args.output_dir, "round_trip_samples_by_iteration.csv"),
        index=False,
    )

    round_trip_summary = make_round_trip_summary(round_trips)
    round_trip_summary.to_csv(
        os.path.join(args.output_dir, "round_trip_summary_by_similarity.csv"),
        index=False,
    )

    round_trip_comparison = make_round_trip_protocol_comparison(round_trip_summary)
    round_trip_comparison.to_csv(
        os.path.join(
            args.output_dir, "protocol_round_trip_comparison_by_similarity.csv"
        ),
        index=False,
    )

    plot_round_trip_summary(round_trip_summary, args.output_dir)
    print(f"Wrote round-trip analysis to {args.output_dir}")
    if not round_trip_summary.empty:
        print(round_trip_summary.to_string(index=False))


if __name__ == "__main__":
    main()

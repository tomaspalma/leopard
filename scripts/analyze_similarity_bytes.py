import argparse
import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def parse_labels(label_str):
    result = {}
    if not isinstance(label_str, str):
        return result
    for pair in label_str.split(";"):
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        result[key.strip()] = value.strip()
    return result


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
                    "value": float(row.get("value", 0)),
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


def aggregate_total_bytes(df_sent, df_received):
    if df_sent.empty and df_received.empty:
        return pd.DataFrame()

    sent = (
        df_sent.groupby(["run_id", "protocol", "trial", "similarity"], as_index=False)[
            "value"
        ]
        .sum()
        .rename(columns={"value": "bytes_sent"})
    )
    recv = (
        df_received.groupby(
            ["run_id", "protocol", "trial", "similarity"], as_index=False
        )["value"]
        .sum()
        .rename(columns={"value": "bytes_received"})
    )

    merged = sent.merge(
        recv,
        on=["run_id", "protocol", "trial", "similarity"],
        how="outer",
    ).fillna(0)
    merged["total_bytes"] = merged["bytes_sent"] + merged["bytes_received"]

    merged = merged[merged["protocol"].isin(["riblt", "merkle"])].copy()
    merged["similarity_numeric"] = pd.to_numeric(merged["similarity"], errors="coerce")
    return merged


def make_summary(merged):
    if merged.empty:
        return pd.DataFrame(
            columns=[
                "protocol",
                "similarity",
                "mean_total_bytes",
                "median_total_bytes",
                "trials",
                "max_total_bytes",
                "min_total_bytes",
            ]
        )

    summary = merged.groupby(["protocol", "similarity_numeric"], as_index=False).agg(
        mean_total_bytes=("total_bytes", "mean"),
        median_total_bytes=("total_bytes", "median"),
        trials=("total_bytes", "count"),
        max_total_bytes=("total_bytes", "max"),
        min_total_bytes=("total_bytes", "min"),
    )
    summary = summary.rename(columns={"similarity_numeric": "similarity"})
    return summary.sort_values(["protocol", "similarity"])


def plot_summary(summary, output_dir):
    if summary.empty:
        return
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 6))
    for protocol, group in summary.groupby("protocol"):
        group = group.sort_values("similarity")
        plt.plot(
            group["similarity"],
            group["mean_total_bytes"],
            marker="o",
            label=protocol,
        )

    plt.xlabel("Similarity (Jaccard)")
    plt.ylabel("Mean Total Bytes (Sent + Received)")
    plt.title("Reconciliation Bytes vs Similarity")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "bytes_vs_similarity.png"))
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze bytes across similarity levels"
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

    sent_df = load_metric_rows(args.metrics_root, "protocol_bytes_sent")
    recv_df = load_metric_rows(args.metrics_root, "protocol_bytes_received")
    merged = aggregate_total_bytes(sent_df, recv_df)

    os.makedirs(args.output_dir, exist_ok=True)
    merged.to_csv(os.path.join(args.output_dir, "trial_totals.csv"), index=False)

    summary = make_summary(merged)
    summary.to_csv(
        os.path.join(args.output_dir, "summary_by_similarity.csv"), index=False
    )

    plot_summary(summary, args.output_dir)
    print(f"Wrote analysis to {args.output_dir}")
    if not summary.empty:
        print(summary.to_string(index=False))


if __name__ == "__main__":
    main()

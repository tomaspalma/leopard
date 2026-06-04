import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

SUPPORTED_PROTOCOLS = ["riblt", "merkle", "rbf_riblt", "rf_riblt"]


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


def aggregate_transmitted_bytes(df_sent):
    if df_sent.empty:
        return pd.DataFrame()

    sent = (
        df_sent.groupby(["run_id", "protocol", "trial", "similarity"], as_index=False)[
            "value"
        ]
        .sum()
        .rename(columns={"value": "bytes_sent"})
    )
    merged = sent.copy()
    merged["transmitted_bytes"] = merged["bytes_sent"]

    merged = merged[merged["protocol"].isin(SUPPORTED_PROTOCOLS)].copy()
    merged["similarity_numeric"] = pd.to_numeric(merged["similarity"], errors="coerce")
    return merged


def make_summary(merged):
    if merged.empty:
        return pd.DataFrame(
            columns=[
                "protocol",
                "similarity",
                "mean_transmitted_bytes",
                "std_transmitted_bytes",
                "median_transmitted_bytes",
                "trials",
                "max_transmitted_bytes",
                "min_transmitted_bytes",
            ]
        )

    summary = merged.groupby(["protocol", "similarity_numeric"], as_index=False).agg(
        mean_transmitted_bytes=("transmitted_bytes", "mean"),
        std_transmitted_bytes=("transmitted_bytes", "std"),
        median_transmitted_bytes=("transmitted_bytes", "median"),
        trials=("transmitted_bytes", "count"),
        max_transmitted_bytes=("transmitted_bytes", "max"),
        min_transmitted_bytes=("transmitted_bytes", "min"),
    )
    summary["std_transmitted_bytes"] = summary["std_transmitted_bytes"].fillna(0)
    summary = summary.rename(columns={"similarity_numeric": "similarity"})
    return summary.sort_values(["protocol", "similarity"])


def make_protocol_comparison(summary):
    if summary.empty:
        return pd.DataFrame(
            columns=[
                "similarity",
                "riblt_mean_transmitted_bytes",
                "merkle_mean_transmitted_bytes",
                "riblt_std_transmitted_bytes",
                "merkle_std_transmitted_bytes",
                "riblt_trials",
                "merkle_trials",
                "riblt_minus_merkle",
                "riblt_to_merkle_ratio",
            ]
        )

    pivot = summary.pivot_table(
        index="similarity",
        columns="protocol",
        values=["mean_transmitted_bytes", "std_transmitted_bytes", "trials"],
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
            "riblt_mean_transmitted_bytes": get_metric(
                "mean_transmitted_bytes", "riblt"
            ),
            "merkle_mean_transmitted_bytes": get_metric(
                "mean_transmitted_bytes", "merkle"
            ),
            "riblt_std_transmitted_bytes": get_metric("std_transmitted_bytes", "riblt"),
            "merkle_std_transmitted_bytes": get_metric(
                "std_transmitted_bytes", "merkle"
            ),
            "riblt_trials": get_metric("trials", "riblt"),
            "merkle_trials": get_metric("trials", "merkle"),
        }
    ).reset_index(drop=True)

    comparison["riblt_minus_merkle"] = (
        comparison["riblt_mean_transmitted_bytes"]
        - comparison["merkle_mean_transmitted_bytes"]
    )
    comparison["riblt_to_merkle_ratio"] = comparison[
        "riblt_mean_transmitted_bytes"
    ] / comparison["merkle_mean_transmitted_bytes"].replace({0: pd.NA})
    return comparison.sort_values("similarity")


def plot_summary(summary, output_dir):
    if summary.empty:
        return
    os.makedirs(output_dir, exist_ok=True)
    bytes_per_megabyte = 1024 * 1024
    plt.figure(figsize=(10, 6))
    for protocol, group in summary.groupby("protocol"):
        group = group.sort_values("similarity")
        mean = group["mean_transmitted_bytes"] / bytes_per_megabyte
        yerr = [
            mean - group["min_transmitted_bytes"] / bytes_per_megabyte,
            group["max_transmitted_bytes"] / bytes_per_megabyte - mean,
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
    plt.xlim(-0.03, 1.03)
    plt.ylabel("Mean Data Transmitted (MB)")
    plt.title("Reconciliation Transmitted Data vs Similarity")
    plt.yscale("log")
    ax = plt.gca()
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda value, _pos: f"{value:g}")
    )
    ax.xaxis.set_major_locator(mticker.MultipleLocator(0.05))
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "bytes_vs_similarity.pdf"))
    plt.close()

    _plot_ratio(summary, output_dir)
    _plot_overhead(summary, output_dir)


def _plot_ratio(summary, output_dir):
    """Ratio of each protocol's bytes to riblt baseline, per similarity."""
    pivot = summary.pivot_table(
        index="similarity", columns="protocol", values="mean_transmitted_bytes"
    )
    if "riblt" not in pivot.columns:
        return

    plt.figure(figsize=(10, 6))
    for proto in pivot.columns:
        if proto == "riblt":
            continue
        ratio = pivot[proto] / pivot["riblt"]
        plt.plot(pivot.index, ratio, marker="o", label=proto)

    plt.axhline(1.0, color="black", linewidth=0.8, linestyle="--", label="riblt (baseline)")
    plt.xlabel("Similarity (Jaccard)")
    plt.xlim(-0.03, 1.03)
    plt.ylabel("Bytes / RIBLT bytes  (lower is better)")
    plt.title("Relative Transmitted Bytes vs RIBLT Baseline")
    plt.gca().xaxis.set_major_locator(mticker.MultipleLocator(0.05))
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "bytes_ratio_vs_similarity.pdf"))
    plt.close()


def _plot_overhead(summary, output_dir):
    """Absolute bytes saved vs riblt, showing where filter overhead lies."""
    pivot = summary.pivot_table(
        index="similarity", columns="protocol", values="mean_transmitted_bytes"
    )
    if "riblt" not in pivot.columns:
        return

    target_protos = [p for p in ["rbf_riblt", "rf_riblt"] if p in pivot.columns]
    if not target_protos:
        return

    plt.figure(figsize=(10, 6))
    for proto in target_protos:
        overhead = pivot[proto] - pivot["riblt"]
        plt.plot(pivot.index, overhead, marker="o", label=f"{proto} overhead vs riblt")

    plt.axhline(0, color="black", linewidth=0.8, linestyle="--")
    plt.xlabel("Similarity (Jaccard)")
    plt.xlim(-0.03, 1.03)
    plt.ylabel("Extra bytes vs plain RIBLT")
    plt.title("Protocol Filter Overhead (bytes above RIBLT baseline)")
    plt.gca().xaxis.set_major_locator(mticker.MultipleLocator(0.05))
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "bytes_overhead_vs_similarity.pdf"))
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

    frames = []
    for proto in SUPPORTED_PROTOCOLS:
        df = load_metric_rows(args.metrics_root, f"{proto}_bytes_sent")
        if not df.empty:
            df["protocol"] = proto
            frames.append(df)
    sent_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    merged = aggregate_transmitted_bytes(sent_df)

    os.makedirs(args.output_dir, exist_ok=True)
    merged.to_csv(
        os.path.join(args.output_dir, "trial_transmitted_totals.csv"), index=False
    )

    summary = make_summary(merged)
    summary.to_csv(
        os.path.join(args.output_dir, "summary_by_similarity.csv"), index=False
    )

    comparison = make_protocol_comparison(summary)
    comparison.to_csv(
        os.path.join(args.output_dir, "protocol_comparison_by_similarity.csv"),
        index=False,
    )

    plot_summary(summary, args.output_dir)
    print(f"Wrote analysis to {args.output_dir}")
    if not summary.empty:
        print(summary.to_string(index=False))


if __name__ == "__main__":
    main()

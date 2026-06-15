import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

# Only rbf_riblt has a bloom/s_com phase, so it is the only protocol that emits
# these gauges. Kept as a list so the loader stays uniform with the other
# similarity analyzers.
SUPPORTED_PROTOCOLS = ["rbf_riblt"]

# metric file name -> (per-row value column, plot y-label, plot title, output pdf)
METRICS = {
    "scom_size": (
        "scom_size",
        "Median |s_com| (candidate-common keys)",
        "Post-bloom Candidate-Set Size vs Similarity",
        "scom_size_vs_similarity.pdf",
    ),
    "bloom_slices": (
        "bloom_slices",
        "Median bloom slices applied (S)",
        "Bloom Slice Count vs Similarity",
        "bloom_slices_vs_similarity.pdf",
    ),
}


def load_metric_rows(metrics_root, metric_name, value_col):
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
                    value_col: value,
                    "protocol": protocol if isinstance(protocol, str) and protocol else "unknown",
                    "trial": str(trial) if pd.notna(trial) else "unknown",
                    "similarity": str(similarity) if pd.notna(similarity) else "unknown",
                    "run_id": run_id if isinstance(run_id, str) and run_id else run_dir.name,
                }
            )

    return pd.DataFrame(rows)


def make_summary(df, value_col):
    summary_cols = [
        "protocol",
        "similarity",
        f"mean_{value_col}",
        f"std_{value_col}",
        f"median_{value_col}",
        "samples",
        f"q75_{value_col}",
        f"q25_{value_col}",
    ]
    if df.empty:
        return pd.DataFrame(columns=summary_cols)

    df = df[df["protocol"].isin(SUPPORTED_PROTOCOLS)].copy()
    df["similarity_numeric"] = pd.to_numeric(df["similarity"], errors="coerce")

    summary = df.groupby(["protocol", "similarity_numeric"], as_index=False).agg(
        **{
            f"mean_{value_col}": (value_col, "mean"),
            f"std_{value_col}": (value_col, "std"),
            f"median_{value_col}": (value_col, "median"),
            "samples": (value_col, "count"),
            f"q75_{value_col}": (value_col, lambda x: x.quantile(0.75)),
            f"q25_{value_col}": (value_col, lambda x: x.quantile(0.25)),
        }
    )
    summary[f"std_{value_col}"] = summary[f"std_{value_col}"].fillna(0)
    summary = summary.rename(columns={"similarity_numeric": "similarity"})
    return summary.sort_values(["protocol", "similarity"])


def plot_summary(summary, value_col, ylabel, title, output_dir, filename):
    if summary.empty:
        return

    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 6))
    for protocol, group in summary.groupby("protocol"):
        group = group.sort_values("similarity")
        median = group[f"median_{value_col}"]
        yerr = [
            median - group[f"q25_{value_col}"],
            group[f"q75_{value_col}"] - median,
        ]
        plt.errorbar(
            group["similarity"],
            median,
            yerr=yerr,
            marker="o",
            capsize=3,
            label=protocol,
        )

    plt.xlabel("Similarity (Jaccard)")
    plt.xlim(-0.03, 1.03)
    plt.ylabel(ylabel)
    plt.title(title)
    ax = plt.gca()
    ax.xaxis.set_major_locator(mticker.MultipleLocator(0.05))
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, filename))
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze post-bloom s_com size and bloom slice count across similarity levels"
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

    os.makedirs(args.output_dir, exist_ok=True)

    for metric_name, (value_col, ylabel, title, filename) in METRICS.items():
        df = load_metric_rows(args.metrics_root, metric_name, value_col)
        summary = make_summary(df, value_col)

        df.to_csv(
            os.path.join(args.output_dir, f"{metric_name}_by_trial.csv"), index=False
        )
        summary.to_csv(
            os.path.join(args.output_dir, f"{metric_name}_summary_by_similarity.csv"),
            index=False,
        )

        plot_summary(summary, value_col, ylabel, title, args.output_dir, filename)
        print(f"Wrote {metric_name} analysis to {args.output_dir}")
        if not summary.empty:
            print(summary.to_string(index=False))


if __name__ == "__main__":
    main()

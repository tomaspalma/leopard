import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd


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


def round_values(df, value_column):
    if df.empty:
        return pd.DataFrame()

    if value_column not in df.columns:
        return pd.DataFrame()

    out = (
        df[df["iteration"].notna() & df[value_column].notna()]
        .groupby(
            ["run_id", "protocol", "trial", "similarity", "iteration"], as_index=False
        )[value_column]
        .max()
    )
    out = out[out["protocol"].isin(["riblt", "merkle"])].copy()
    out["similarity_numeric"] = pd.to_numeric(out["similarity"], errors="coerce")
    return out


def build_cpu_round_deltas(cpu_df):
    if cpu_df.empty:
        return pd.DataFrame()

    cpu_df = cpu_df.copy().rename(columns={"value": "cpu_time_seconds_total"})
    rounds = round_values(cpu_df, "cpu_time_seconds_total")
    if rounds.empty:
        return rounds

    rounds = rounds.sort_values(["run_id", "protocol", "cpu_time_seconds_total"])
    rounds["cpu_round_seconds"] = rounds.groupby(["run_id", "protocol"])[
        "cpu_time_seconds_total"
    ].diff()
    rounds["cpu_round_seconds"] = rounds["cpu_round_seconds"].fillna(
        rounds["cpu_time_seconds_total"]
    )
    rounds["cpu_round_seconds"] = rounds["cpu_round_seconds"].clip(lower=0)
    rounds["cpu_round_ms"] = rounds["cpu_round_seconds"] * 1000.0
    return rounds


def build_memory_round_stats(mem_df):
    if mem_df.empty:
        return pd.DataFrame()

    mem_df = mem_df.copy().rename(columns={"value": "rss_memory_bytes"})
    rounds = round_values(mem_df, "rss_memory_bytes")
    if rounds.empty:
        return rounds

    rounds["rss_memory_mb"] = rounds["rss_memory_bytes"] / (1024 * 1024)
    return rounds


def summarize_round_metric(rounds, metric_column, prefix):
    if rounds.empty:
        return pd.DataFrame(
            columns=[
                "protocol",
                "similarity",
                f"mean_{prefix}",
                f"std_{prefix}",
                f"median_{prefix}",
                "samples",
                f"max_{prefix}",
                f"min_{prefix}",
            ]
        )

    summary = rounds.groupby(["protocol", "similarity_numeric"], as_index=False).agg(
        **{
            f"mean_{prefix}": (metric_column, "mean"),
            f"std_{prefix}": (metric_column, "std"),
            f"median_{prefix}": (metric_column, "median"),
            "samples": (metric_column, "count"),
            f"max_{prefix}": (metric_column, "max"),
            f"min_{prefix}": (metric_column, "min"),
        }
    )
    summary[f"std_{prefix}"] = summary[f"std_{prefix}"].fillna(0)
    summary = summary.rename(columns={"similarity_numeric": "similarity"})
    return summary.sort_values(["protocol", "similarity"])


def make_protocol_comparison(summary, prefix, unit_suffix):
    if summary.empty:
        return pd.DataFrame(
            columns=[
                "similarity",
                f"riblt_mean_{prefix}",
                f"merkle_mean_{prefix}",
                f"riblt_std_{prefix}",
                f"merkle_std_{prefix}",
                "riblt_samples",
                "merkle_samples",
                f"riblt_minus_merkle_{unit_suffix}",
                "riblt_to_merkle_ratio",
            ]
        )

    pivot = summary.pivot_table(
        index="similarity",
        columns="protocol",
        values=[f"mean_{prefix}", f"std_{prefix}", "samples"],
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
            f"riblt_mean_{prefix}": get_metric(f"mean_{prefix}", "riblt"),
            f"merkle_mean_{prefix}": get_metric(f"mean_{prefix}", "merkle"),
            f"riblt_std_{prefix}": get_metric(f"std_{prefix}", "riblt"),
            f"merkle_std_{prefix}": get_metric(f"std_{prefix}", "merkle"),
            "riblt_samples": get_metric("samples", "riblt"),
            "merkle_samples": get_metric("samples", "merkle"),
        }
    ).reset_index(drop=True)

    comparison[f"riblt_minus_merkle_{unit_suffix}"] = (
        comparison[f"riblt_mean_{prefix}"] - comparison[f"merkle_mean_{prefix}"]
    )
    comparison["riblt_to_merkle_ratio"] = comparison[
        f"riblt_mean_{prefix}"
    ] / comparison[f"merkle_mean_{prefix}"].replace({0: pd.NA})
    return comparison.sort_values("similarity")


def apply_log_plain_ticks():
    ax = plt.gca()
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda value, _pos: f"{value:g}")
    )


def plot_metric(summary, value_col, std_col, ylabel, title, output_path):
    if summary.empty:
        return

    plt.figure(figsize=(10, 6))
    for protocol, group in summary.groupby("protocol"):
        group = group.sort_values("similarity")
        plt.errorbar(
            group["similarity"],
            group[value_col],
            yerr=group[std_col],
            marker="o",
            capsize=3,
            label=protocol,
        )

    plt.xlabel("Similarity (Jaccard)")
    plt.ylabel(ylabel)
    plt.title(title)
    apply_log_plain_ticks()
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze CPU and memory usage across similarity levels"
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

    cpu_df = load_metric_rows(args.metrics_root, "process_cpu_time_seconds_total")
    mem_df = load_metric_rows(args.metrics_root, "process_rss_memory_bytes")

    cpu_rounds = build_cpu_round_deltas(cpu_df)
    mem_rounds = build_memory_round_stats(mem_df)

    os.makedirs(args.output_dir, exist_ok=True)
    cpu_rounds.to_csv(
        os.path.join(args.output_dir, "cpu_round_usage_by_trial.csv"), index=False
    )
    mem_rounds.to_csv(
        os.path.join(args.output_dir, "memory_round_usage_by_trial.csv"), index=False
    )

    cpu_summary = summarize_round_metric(
        cpu_rounds, "cpu_round_seconds", "cpu_round_seconds"
    )
    mem_summary = summarize_round_metric(mem_rounds, "rss_memory_mb", "rss_memory_mb")

    cpu_summary.to_csv(
        os.path.join(args.output_dir, "cpu_summary_by_similarity.csv"), index=False
    )
    mem_summary.to_csv(
        os.path.join(args.output_dir, "memory_summary_by_similarity.csv"), index=False
    )

    cpu_comparison = make_protocol_comparison(
        cpu_summary, "cpu_round_seconds", "seconds"
    )
    mem_comparison = make_protocol_comparison(mem_summary, "rss_memory_mb", "mb")

    cpu_comparison.to_csv(
        os.path.join(args.output_dir, "protocol_cpu_comparison_by_similarity.csv"),
        index=False,
    )
    mem_comparison.to_csv(
        os.path.join(args.output_dir, "protocol_memory_comparison_by_similarity.csv"),
        index=False,
    )

    plot_metric(
        cpu_summary,
        "mean_cpu_round_seconds",
        "std_cpu_round_seconds",
        "Mean CPU Time Per Round (seconds)",
        "CPU Time Per Round vs Similarity",
        os.path.join(args.output_dir, "cpu_vs_similarity.png"),
    )

    plot_metric(
        mem_summary,
        "mean_rss_memory_mb",
        "std_rss_memory_mb",
        "Mean RSS Memory (MB)",
        "RSS Memory vs Similarity",
        os.path.join(args.output_dir, "memory_vs_similarity.png"),
    )

    print(f"Wrote resource analysis to {args.output_dir}")
    if not cpu_summary.empty:
        print("\nCPU summary:")
        print(cpu_summary.to_string(index=False))
    if not mem_summary.empty:
        print("\nMemory summary:")
        print(mem_summary.to_string(index=False))


if __name__ == "__main__":
    main()

"""Attribute reconciliation cost between the IBLT *seed* phase (O(local set
size)) and the *decode* phase (O(difference)), across similarity levels.

Reads the per-trial gauges emitted at IBLT completion:
  reconciliation_seed_seconds, reconciliation_decode_seconds,
  reconciliation_decoded_difference
(emitted by both riblt and rbf_riblt's scom phase via the shared engine).

For rbf_riblt this shows the seeding term (which scales with |s_com|, and so
grows with similarity) against the decode term (the false-positive residual),
closing the loop on why rbf_riblt's round duration rises with similarity.

Usage: python3 scripts/analyze_phase_split.py metrics_output [--protocol rbf_riblt]
"""

import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

METRICS = {
    "reconciliation_seed_seconds": "seed_seconds",
    "reconciliation_decode_seconds": "decode_seconds",
    "reconciliation_decoded_difference": "decoded_difference",
}


def load_metric(metrics_root, metric_name, value_col, protocol):
    rows = []
    for run_dir in Path(metrics_root).iterdir():
        if not run_dir.is_dir():
            continue
        f = run_dir / f"{metric_name}.csv"
        if not f.exists():
            continue
        df = pd.read_csv(f)
        for _, row in df.iterrows():
            if protocol and row.get("protocol") != protocol:
                continue
            value = pd.to_numeric(row.get("value"), errors="coerce")
            sim = pd.to_numeric(row.get("similarity"), errors="coerce")
            if pd.isna(value) or value < 0 or pd.isna(sim):
                continue
            rows.append({"similarity": float(sim), value_col: float(value)})
    if not rows:
        return pd.DataFrame(columns=["similarity", value_col])
    return (
        pd.DataFrame(rows)
        .groupby("similarity", as_index=False)
        .agg(**{value_col: (value_col, "mean")})
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("metrics_root", nargs="?", default="metrics_output")
    parser.add_argument("--protocol", default="rbf_riblt")
    parser.add_argument("--output-dir", default="metrics_output/analysis")
    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    summary = None
    for metric_name, value_col in METRICS.items():
        m = load_metric(args.metrics_root, metric_name, value_col, args.protocol)
        summary = m if summary is None else summary.merge(m, on="similarity", how="outer")

    if summary is None or summary.empty:
        print(f"no phase-split rows for protocol={args.protocol}")
        return

    summary = summary.sort_values("similarity").fillna(0)
    summary.insert(0, "protocol", args.protocol)
    summary.to_csv(
        os.path.join(args.output_dir, "phase_split_summary_by_similarity.csv"),
        index=False,
    )

    # Figure: stacked seed vs decode time across similarity.
    plt.figure(figsize=(10, 6))
    x = summary["similarity"]
    seed = summary.get("seed_seconds", pd.Series(0, index=summary.index))
    decode = summary.get("decode_seconds", pd.Series(0, index=summary.index))
    plt.stackplot(
        x, seed, decode,
        labels=["seed phase (O(|s_com|))", "decode phase (O(difference))"],
        colors=["#4C72B0", "#DD8452"], alpha=0.85,
    )
    plt.plot(x, seed + decode, color="black", linewidth=1, label="seed + decode")
    plt.xlabel("Similarity (Jaccard)")
    plt.xlim(-0.03, 1.03)
    plt.ylabel("Mean IBLT phase time (s)")
    plt.title(f"{args.protocol}: IBLT seed vs decode time across similarity")
    plt.gca().xaxis.set_major_locator(mticker.MultipleLocator(0.05))
    plt.grid(True)
    plt.legend(loc="upper left")
    plt.tight_layout()
    plt.savefig(
        os.path.join(args.output_dir, f"{args.protocol}_phase_split_vs_similarity.pdf")
    )
    plt.close()

    print(f"Wrote phase-split analysis to {args.output_dir}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()

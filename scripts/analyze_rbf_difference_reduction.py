"""Quantify how much the RBF (bloom) phase shrinks the difference that reaches
RIBLT, by comparing the symmetric difference RIBLT actually peels in plain
`riblt` (the full d) against the residual it peels in `rbf_riblt`'s scom phase.

Both protocols emit `reconciliation_decoded_difference` from the shared stream
engine (riblt/stream.rs): for `riblt` it is the full symmetric difference d; for
`rbf_riblt` it is the difference *left inside s_com* after the bloom filter has
moved the provably-absent keys (s_tn) out of the IBLT path. The ratio of the two
is the direct measure of "how much the RBF phase removes before RIBLT runs".

Because RIBLT's coded-symbol cost is linear in the difference (~OVERHEAD * d, the
constant measured by riblt_decode_scaling), the same ratio is the factor by which
the coded-symbol stream -- and thus the coded-symbol bytes -- shrinks. Pass
--overhead to also print the implied coded-symbol counts.

Usage:
  python3 scripts/analyze_rbf_difference_reduction.py [metrics_root]
      [--baseline riblt] [--filtered rbf_riblt]
      [--overhead 1.35]
      [--output-dir metrics_output/analysis]
"""

import argparse
import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

METRIC = "reconciliation_decoded_difference"


def load(metrics_root, protocol):
    """Mean/min/max decoded difference per similarity for one protocol."""
    rows = []
    for run_dir in sorted(Path(metrics_root).iterdir()):
        f = run_dir / f"{METRIC}.csv"
        if not run_dir.is_dir() or not f.exists():
            continue
        df = pd.read_csv(f)
        for _, row in df.iterrows():
            if row.get("protocol") != protocol:
                continue
            value = pd.to_numeric(row.get("value"), errors="coerce")
            sim = pd.to_numeric(row.get("similarity"), errors="coerce")
            if pd.isna(value) or value < 0 or pd.isna(sim):
                continue
            rows.append({"similarity": float(sim), "diff": float(value)})
    if not rows:
        return pd.DataFrame(columns=["similarity", "diff_mean", "diff_min", "diff_max"])
    return (
        pd.DataFrame(rows)
        .groupby("similarity", as_index=False)
        .agg(
            diff_mean=("diff", "mean"),
            diff_min=("diff", "min"),
            diff_max=("diff", "max"),
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("metrics_root", nargs="?", default="metrics_output")
    parser.add_argument("--baseline", default="riblt",
                        help="protocol whose RIBLT sees the full difference")
    parser.add_argument("--filtered", default="rbf_riblt",
                        help="protocol whose RIBLT runs only on s_com")
    parser.add_argument("--overhead", type=float, default=None,
                        help="coded-symbols-per-difference constant (e.g. 1.35); "
                             "if set, also report implied coded-symbol counts")
    parser.add_argument("--output-dir", default="metrics_output/analysis")
    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    base = load(args.metrics_root, args.baseline).rename(
        columns={"diff_mean": "base_mean", "diff_min": "base_min", "diff_max": "base_max"})
    filt = load(args.metrics_root, args.filtered).rename(
        columns={"diff_mean": "filt_mean", "diff_min": "filt_min", "diff_max": "filt_max"})

    if base.empty or filt.empty:
        print(f"missing data: baseline rows={len(base)}, filtered rows={len(filt)}")
        return

    m = base.merge(filt, on="similarity", how="inner").sort_values("similarity")
    m["reduction"] = m["base_mean"] / m["filt_mean"].where(m["filt_mean"] > 0)

    cols = ["similarity", "base_mean", "filt_mean", "reduction"]
    if args.overhead:
        m["base_symbols"] = m["base_mean"] * args.overhead
        m["filt_symbols"] = m["filt_mean"] * args.overhead
        cols += ["base_symbols", "filt_symbols"]

    out_csv = os.path.join(args.output_dir, "rbf_difference_reduction.csv")
    m.to_csv(out_csv, index=False)

    # Figure: the difference RIBLT peels, baseline vs filtered, with the
    # reduction factor on a secondary axis.
    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = m["similarity"]
    ax.plot(x, m["base_mean"], "o-", color="#2ca02c",
            label=f"{args.baseline}: full difference reaching RIBLT")
    ax.plot(x, m["filt_mean"], "s-", color="#ff7f0e",
            label=f"{args.filtered}: residual after RBF (s_com only)")
    ax.set_yscale("log")
    ax.set_xlabel("Similarity (Jaccard)")
    ax.set_ylabel("Symmetric difference peeled by RIBLT")
    ax.grid(True, which="both", alpha=0.3)
    ax.xaxis.set_major_locator(mticker.MultipleLocator(0.1))
    ax.legend(loc="lower left", fontsize=8)
    fig.tight_layout()
    out_pdf = os.path.join(args.output_dir, "rbf_difference_reduction.pdf")
    fig.savefig(out_pdf)
    plt.close(fig)

    print(f"wrote {out_csv}")
    print(f"wrote {out_pdf}")
    with pd.option_context("display.float_format", lambda v: f"{v:.1f}"):
        print(m[cols].to_string(index=False))


if __name__ == "__main__":
    main()

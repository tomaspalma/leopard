"""Generate the rbf_riblt phase-split LaTeX table directly from sweep data.

One row per similarity level, trial- and peer-averaged:

  J            Jaccard similarity (the configured sweep level)
  S            mean number of bloom slices applied (bloom_slices)
  FP           per-key false-positive rate after S slices, (1/2)^S
  |s_com|      mean post-bloom candidate-set size (scom_size)
  decoded diff mean symmetric difference recovered by the IBLT
                 (reconciliation_decoded_difference)
  seed (s)     mean IBLT seeding time      (reconciliation_seed_seconds)
  decode (s)   mean IBLT peeling time      (reconciliation_decode_seconds)

Reads the same per-trial CSVs every other analysis script consumes, so it
always reflects the current sweep output -- no manual assembly.

Usage:
  python3 scripts/make_phase_split_table.py [metrics_root]
      [--protocol rbf_riblt]
      [--similarities 0,0.10,0.30,0.50,0.70,0.85,0.95,0.99,1]
      [--output tab_rbf_phase_split.tex]

Writes the LaTeX table to the --output path (default tab_rbf_phase_split.tex).
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# metric name (CSV stem) -> output column key
METRICS = {
    "bloom_slices": "slices",
    "scom_size": "scom",
    "reconciliation_decoded_difference": "decoded_diff",
    "reconciliation_seed_seconds": "seed",
    "reconciliation_decode_seconds": "decode",
}


def load_metric(metrics_root, metric_name, col, protocol):
    rows = []
    for run_dir in sorted(Path(metrics_root).iterdir()):
        f = run_dir / f"{metric_name}.csv"
        if not run_dir.is_dir() or not f.exists():
            continue
        df = pd.read_csv(f)
        for _, row in df.iterrows():
            if protocol and row.get("protocol") != protocol:
                continue
            value = pd.to_numeric(row.get("value"), errors="coerce")
            sim = pd.to_numeric(row.get("similarity"), errors="coerce")
            if pd.isna(value) or value < 0 or pd.isna(sim):
                continue
            rows.append({"similarity": float(sim), col: float(value)})
    if not rows:
        return pd.DataFrame(columns=["similarity", col])
    return (
        pd.DataFrame(rows)
        .groupby("similarity", as_index=False)
        .agg(**{col: (col, "mean")})
    )


def fmt_fp(fp_fraction):
    pct = fp_fraction * 100.0
    # Drop a trailing ".0" so 50% renders as "50" like the original table.
    s = f"{pct:.1f}"
    if s.endswith(".0"):
        s = s[:-2]
    return rf"{s}\,\%"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("metrics_root", nargs="?", default="metrics_output/sweep_phase")
    parser.add_argument("--protocol", default="rbf_riblt")
    parser.add_argument(
        "--similarities",
        default=None,
        help="comma-separated subset of J values to keep (default: all found)",
    )
    parser.add_argument(
        "--output",
        default="tab_rbf_phase_split.tex",
        help="path of the .tex file to write (default: tab_rbf_phase_split.tex)",
    )
    args = parser.parse_args()

    summary = None
    for metric_name, col in METRICS.items():
        m = load_metric(args.metrics_root, metric_name, col, args.protocol)
        summary = m if summary is None else summary.merge(m, on="similarity", how="outer")

    if summary is None or summary.empty:
        print(f"no rows for protocol={args.protocol} under {args.metrics_root}", file=sys.stderr)
        return 1

    summary = summary.sort_values("similarity").reset_index(drop=True)

    if args.similarities:
        wanted = [float(x) for x in args.similarities.split(",")]
        summary = summary[summary["similarity"].round(4).isin([round(w, 4) for w in wanted])]

    lines = [
        r"\begin{table}[t]",
        r"    \centering",
        r"    \caption{}",
        r"    \label{tab:rbf-phase-split}",
        r"    \begin{tabular}{rrrrrrr}",
        r"      \toprule",
        r"      $J$ & $S$ & $FP$ & $|s_{\mathrm{com}}|$ &",
        r"        \makecell{decoded\\diff.} & \makecell{seed\\(s)} & \makecell{decode\\(s)} \\",
        r"      \midrule",
    ]

    for _, r in summary.iterrows():
        slices = r.get("slices", float("nan"))
        fp = 0.5 ** slices if pd.notna(slices) else float("nan")
        lines.append(
            f"      {r['similarity']:.2f} & {slices:.1f} & {fmt_fp(fp)} & "
            f"{r.get('scom', 0):.0f} & {r.get('decoded_diff', 0):.0f} & "
            f"{r.get('seed', 0):.3f} & {r.get('decode', 0):.3f} \\\\"
        )

    lines += [r"      \bottomrule", r"    \end{tabular}", r"  \end{table}"]
    out = "\n".join(lines) + "\n"

    Path(args.output).write_text(out)
    print(f"Wrote {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

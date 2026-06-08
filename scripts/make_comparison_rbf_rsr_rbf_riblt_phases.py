"""Generate the rbf_riblt transmitted-bytes phase split from sweep data.

Splits rbf_riblt's transmitted bytes into its two phases:

  RBF    bytes of the ribbon-filter slices, the deterministic
         2 * S * ceil(m_bits/8) (both directions, m_bits = ceil(n / ln2))
  riblt  the coded-symbol stream sent once no slices remain, taken as
         total_transmitted - RBF

All inputs are measured per-trial CSVs (the same ones the other analysis scripts
read), so the table always reflects the current sweep:
  rbf_riblt_bytes_sent.csv  (counter, cumulative total per node)
  bloom_slices.csv          (slices applied per node)

Usage:
  python3 scripts/make_comparison_rbf_rsr_rbf_riblt_phases.py [metrics_root]
      [--protocol rbf_riblt]
      [--similarities 0,0.1,0.3,0.5,0.7,0.9]
      [--output tab_bytes_saving.tex]
"""

import argparse
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

RUN_RE = re.compile(r"sweep_(?P<proto>[a-z_]+?)_n(?P<n>\d+)_sim\d+_t\d+$")


def total_bytes_by_similarity(metrics_root, proto):
    """Mean total transmitted bytes per similarity for `proto`.

    `<proto>_bytes_sent` is a cumulative counter logged per node; the per-trial
    total is the sum over nodes of each node's final (max) value. Averaged over
    trials per similarity level.
    """
    per_trial = defaultdict(list)
    for run_dir in sorted(Path(metrics_root).iterdir()):
        m = RUN_RE.match(run_dir.name)
        if not m or m.group("proto") != proto:
            continue
        f = run_dir / f"{proto}_bytes_sent.csv"
        if not f.exists():
            continue
        df = pd.read_csv(f)
        node_max = defaultdict(float)
        sim = None
        for _, row in df.iterrows():
            v = pd.to_numeric(row.get("value"), errors="coerce")
            s = pd.to_numeric(row.get("similarity"), errors="coerce")
            if pd.isna(v):
                continue
            if not pd.isna(s):
                sim = float(s)
            node_max[row.get("node")] = max(node_max[row.get("node")], float(v))
        if sim is not None and node_max:
            per_trial[sim].append(sum(node_max.values()))
    return {s: sum(v) / len(v) for s, v in per_trial.items() if v}


def slices_and_n_by_similarity(metrics_root, proto):
    """Mean bloom_slices per similarity, and the n seen for that protocol."""
    per_trial = defaultdict(list)
    n_seen = None
    for run_dir in sorted(Path(metrics_root).iterdir()):
        m = RUN_RE.match(run_dir.name)
        if not m or m.group("proto") != proto:
            continue
        n_seen = int(m.group("n"))
        f = run_dir / "bloom_slices.csv"
        if not f.exists():
            continue
        df = pd.read_csv(f)
        for _, row in df.iterrows():
            v = pd.to_numeric(row.get("value"), errors="coerce")
            s = pd.to_numeric(row.get("similarity"), errors="coerce")
            if pd.isna(v) or pd.isna(s) or v < 0:
                continue
            per_trial[float(s)].append(float(v))
    slices = {s: sum(v) / len(v) for s, v in per_trial.items() if v}
    return slices, n_seen


def slice_bytes(n):
    """Wire size of one bloom slice: ceil(ceil(n / ln2) / 8) bytes."""
    m_bits = math.ceil(n / math.log(2))
    return math.ceil(m_bits / 8)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("metrics_root", nargs="?", default="metrics_output")
    parser.add_argument("--protocol", default="rbf_riblt")
    parser.add_argument("--similarities", default=None,
                        help="comma-separated subset of J values to keep")
    parser.add_argument("--output", default="tab_bytes_saving.tex")
    args = parser.parse_args()

    total = total_bytes_by_similarity(args.metrics_root, args.protocol)
    slices, n = slices_and_n_by_similarity(args.metrics_root, args.protocol)

    if not total:
        print(f"missing data: {args.protocol} rows={len(total)}", file=sys.stderr)
        return 1

    sb = slice_bytes(n) if n else 0
    sims = sorted(total)
    if args.similarities:
        wanted = {round(float(x), 4) for x in args.similarities.split(",")}
        sims = [s for s in sims if round(s, 4) in wanted]

    MB = 1024 * 1024
    rows = []
    for s in sims:
        tot = total[s]
        # Slices are sent in both directions: 2 * slices * slice_bytes.
        rbf_bytes = 2 * slices.get(s, 0.0) * sb
        # Everything sent once no slices remain is the coded-symbol stream.
        riblt_bytes = tot - rbf_bytes
        ratio = riblt_bytes / rbf_bytes if rbf_bytes > 0 else float("nan")
        rows.append((s, rbf_bytes / MB, riblt_bytes / MB, tot / MB, ratio))

    # Plain-text echo.
    print(f"n={n}, slice={sb} bytes")
    print(f"{'J':>5} {'RBF MB':>10} {'riblt MB':>10} {'total MB':>10} "
          f"{'riblt/RBF':>10}")
    for s, rbf, riblt, tot, r in rows:
        rr = "n/a" if math.isnan(r) else f"{r:.0f}x"
        print(f"{s:>5.2f} {rbf:>10.3f} {riblt:>10.2f} {tot:>10.2f} {rr:>10}")

    # LaTeX table. Protocol name carries underscores -> escape for LaTeX text.
    proto_tex = args.protocol.replace("_", r"\_")
    lines = [
        r"\begin{table}[t]",
        r"    \centering",
        rf"    \caption{{Transmitted-bytes split inside {proto_tex} ($n={n}$): "
        r"the ribbon-filter slices (RBF) against the coded-symbol stream sent "
        r"once no slices remain (riblt). The last column is the coded-symbol "
        r"stream as a multiple of the filter cost.}",
        r"    \label{tab:bytes-saving}",
        r"    \begin{tabular}{rrrrr}",
        r"      \toprule",
        r"      $J$ & \makecell{RBF\\(MB)} & \makecell{riblt\\(MB)} & "
        r"\makecell{total\\(MB)} & \makecell{riblt\\$\div$ RBF} \\",
        r"      \midrule",
    ]
    for s, rbf, riblt, tot, r in rows:
        rr = "--" if math.isnan(r) else rf"{r:.0f}$\times$"
        lines.append(
            f"      {s:.2f} & {rbf:.3f} & {riblt:.1f} & {tot:.1f} & {rr} \\\\"
        )
    lines += [r"      \bottomrule", r"    \end{tabular}", r"\end{table}"]
    Path(args.output).write_text("\n".join(lines) + "\n")
    print(f"\nWrote {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

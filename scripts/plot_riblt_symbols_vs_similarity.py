#!/usr/bin/env python3
"""Plot RIBLT coded-symbols-needed against replica similarity.

Reads the decode-scaling CSV produced by scripts/riblt_decode_scaling.sh
(columns: d, cells_needed, overhead, ...), maps each symmetric difference d
back to a similarity level via d = 2*(1-s)*n  ->  s = 1 - d / (2n), and plots
cells_needed (the coded symbols the peeler consumes before it decodes) versus
similarity. This is the controlled-microbenchmark evidence that the peeling
process needs fewer symbols as similarity rises.

Usage:
  scripts/plot_riblt_symbols_vs_similarity.py            # n=100000 default
  N=100000 scripts/plot_riblt_symbols_vs_similarity.py
"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

N = int(os.environ.get("N", "100000"))
DMAX = N  # similarity convention: d = (1 - s) * n  ->  s = 1 - d / n
CSV = os.environ.get("CSV", "metrics_output/riblt_decode_scaling.csv")
OUT = os.environ.get("OUT", "metrics_output/analysis/riblt_symbols_vs_similarity.pdf")

d, cells, overhead = [], [], []
with open(CSV) as f:
    for row in csv.DictReader(f):
        d.append(int(row["d"]))
        cells.append(int(row["cells_needed"]))
        overhead.append(float(row["overhead"]))

# Map difference -> similarity and sort ascending by similarity.
sim = [1 - di / DMAX for di in d]
pts = sorted(zip(sim, cells, overhead))
sim, cells, overhead = zip(*pts)

fig, ax = plt.subplots(figsize=(6, 4))
ax.plot([s * 100 for s in sim], cells, "o-", color="C0", label="coded symbols needed")
ax.set_xlabel("Similarity (%)")
ax.set_ylabel("Coded symbols consumed before decode")
ax.grid(True, alpha=0.3)
ax.set_title(rf"RIBLT peeling cost vs. similarity ($n={N}$)")

mean_oh = sum(overhead) / len(overhead)
fig.tight_layout()
os.makedirs(os.path.dirname(OUT), exist_ok=True)
fig.savefig(OUT)
print(f"wrote {OUT}")
print(f"mean overhead (cells/d) = {mean_oh:.3f}")

#!/usr/bin/env bash
#
# Measure how RIBLT decoding scales with the symmetric difference d.
# Drives the real Encoder/Decoder (riblt crate) and writes a CSV showing
# cells_needed (~1.35*d), peel XOR-ops (~d log d), and wall times.
#
# Usage:
#   scripts/riblt_decode_scaling.sh                 # default sweep (max d = 100000)
#   scripts/riblt_decode_scaling.sh 10 100 1000     # custom d values
#   OUT=metrics_output/riblt_decode_scaling.csv scripts/riblt_decode_scaling.sh
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

OUT=${OUT:-"metrics_output/riblt_decode_scaling.csv"}
mkdir -p "$(dirname "$OUT")"

# Default difference sweep, capped at d = 100000 (= n). With d = (1-s)*n these
# map to similarity 0%..99%: 100000->0%, 50000->50%, 10000->90%, 1000->99%.
DEFAULT_DS="100000 95000 90000 80000 70000 60000 50000 40000 30000 25000 20000 15000 10000 5000 3000 1000"

# Build once (release: the O(d log d) work is real at d=100000), then run.
cargo run --release --quiet --manifest-path riblt/Cargo.toml \
    --example decode_scaling -- ${@:-$DEFAULT_DS} >"$OUT"

echo "wrote $OUT"
column -t -s, "$OUT"

# Plot coded-symbols-needed vs similarity from the CSV just written.
PLOT_OUT=${PLOT_OUT:-"metrics_output/analysis/riblt_symbols_vs_similarity.pdf"}
CSV="$OUT" OUT="$PLOT_OUT" python3 "$ROOT_DIR/scripts/plot_riblt_symbols_vs_similarity.py"

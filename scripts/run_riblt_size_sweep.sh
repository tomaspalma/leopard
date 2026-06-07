#!/usr/bin/env bash
#
# Drive the standalone riblt protocol across a range of set sizes, holding
# EITHER the absolute set difference constant (MODE=fixdiff) OR the similarity
# constant (MODE=fixsim). Reuses generate_data.py and run_experiment.sh; only
# the per-size similarity for the fixed-difference curve is computed here.
#
#   fixdiff: difference d held constant -> isolates the pure size effect
#            (similarity per size: J = (size - d/2)/(size + d/2))
#   fixsim:  similarity held constant   -> difference scales with size
#
# Each riblt run lands in metrics_output/<run_id> (run_id carries n<size> so the
# analyzer can recover the size); we move it into $OUTDIR. Pair with:
#   python3 scripts/analyze_riblt_scaling.py metrics_output \
#       fixed-difference=<fixdiff OUTDIR> fixed-similarity=<fixsim OUTDIR>
#
# Usage:  MODE=fixdiff DIFF=2000 SIZES="1000,5000,10000,50000,100000" ./scripts/run_riblt_size_sweep.sh
#         MODE=fixsim  SIM=0.5   SIZES="1000,5000,10000,50000,100000" ./scripts/run_riblt_size_sweep.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

MODE=${MODE:-fixdiff}     # fixdiff | fixsim
SIZES=${SIZES:-"1000,5000,10000,50000,100000"}
DIFF=${DIFF:-2000}        # target symmetric difference (fixdiff mode); sizes must be >= DIFF/2
SIM=${SIM:-0.5}           # fixed similarity (fixsim mode)
TRIALS=${TRIALS:-5}
OUTDIR=${OUTDIR:-"metrics_output/sweep_size_${MODE}"}

export DISABLE_STORAGE_FLUSH=${DISABLE_STORAGE_FLUSH:-1}
export RUST_LOG=${RUST_LOG:-warn}

rm -rf "$OUTDIR"
mkdir -p "$OUTDIR"

IFS=',' read -r -a size_values <<<"$SIZES"

for size in "${size_values[@]}"; do
  if [ "$MODE" = "fixdiff" ]; then
    sim="$(python3 -c "s=$size; d=$DIFF; print(max(0.0, (s - d/2)/(s + d/2)))")"
  else
    sim="$SIM"
  fi
  echo "size=$size mode=$MODE similarity=$sim"

  for trial in $(seq 1 "$TRIALS"); do
    prefix="size_${MODE}_n${size}_t${trial}"
    seed=$((trial * 999983 + size * 97))
    python3 scripts/generate_data.py \
      --size "$size" --similarity "$sim" \
      --seed "$seed" --prefix "$prefix" --output-dir data/

    run_id="sizesweep_${MODE}_riblt_n${size}_t${trial}"
    ./scripts/run_experiment.sh riblt "$prefix" "$run_id" "$trial" "$sim"
    mv "metrics_output/${run_id}" "$OUTDIR/"
    rm -f "data/${prefix}_node1.json" "data/${prefix}_node2.json" "data/${prefix}_node3.json"
  done
done

echo "Done. riblt size sweep ($MODE) written to $OUTDIR"

#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

SIZES=${SIZES:-"1000"}
SIMILARITIES=${SIMILARITIES:-"0,0.05,0.10,0.20,0.30,0.40,0.50,0.60,0.70,0.75,0.80,0.85,0.90,0.95,0.97,0.99"}
TRIALS=${TRIALS:-"10"}
PROTOCOLS=${PROTOCOLS:-"riblt,merkle,rbf_riblt"}
SEED=${SEED:-"12345"}
OUTPUT_ROOT=${OUTPUT_ROOT:-"sweep"}

echo "Generating datasets for sweep..."
python3 scripts/generate_data.py --default-matrix --sizes "$SIZES" --similarities "$SIMILARITIES" --seed "$SEED"

echo "Removing metrics_output folder"
rm -rf metrics_output
mkdir -p metrics_output

IFS=',' read -r -a similarity_values <<<"$SIMILARITIES"
IFS=',' read -r -a protocol_values <<<"$PROTOCOLS"
IFS=',' read -r -a size_values <<<"$SIZES"

for size in "${size_values[@]}"; do
  for sim in "${similarity_values[@]}"; do
    sim_clean="$(python3 -c 'import sys; print(f"{int(round(float(sys.argv[1]) * 100)):02d}")' "$sim")"
    dataset_prefix="n${size}_sim${sim_clean}"

    for protocol in "${protocol_values[@]}"; do
      for trial in $(seq 1 "$TRIALS"); do
        run_id="${OUTPUT_ROOT}_${protocol}_n${size}_sim${sim_clean}_t${trial}"
        echo "Running $run_id"
        ./scripts/run_experiment.sh "$protocol" "$dataset_prefix" "$run_id" "$trial" "$sim"
      done
    done
  done
done

echo "Sweep finished. Analyzing with: python3 scripts/analyze_similarity_bytes.py metrics_output"

python3 scripts/analyze_similarity_bytes.py metrics_output
python3 scripts/analyze_similarity_resources.py metrics_output
python3 scripts/analyze_similarity_duration.py metrics_output
python3 scripts/analyze_roundtrip.py metrics_output

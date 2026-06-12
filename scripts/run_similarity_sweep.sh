#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

SIZES=${SIZES:-"100000"}
SIMILARITIES=${SIMILARITIES:-"0,0.05,0.10,0.20,0.30,0.40,0.50,0.60,0.70,0.75,0.80,0.85,0.90,0.95,0.97,0.99,1"}
TRIALS=${TRIALS:-"5"}
PROTOCOLS=${PROTOCOLS:-"riblt,merkle,rbf_riblt"}
OUTPUT_ROOT=${OUTPUT_ROOT:-"sweep"}
PER_TRIAL_DATASETS=${PER_TRIAL_DATASETS:-true}

# Keep disk persistence off during benchmark runs so disk I/O does not
# contaminate the measured metrics (see runtime::storage_flush_enabled).
export DISABLE_STORAGE_FLUSH=${DISABLE_STORAGE_FLUSH:-1}

# Keep logging off the measured hot path. Per-save info! logging would
# otherwise inflate the round-duration metric and produce huge logs at scale.
export RUST_LOG=${RUST_LOG:-warn}

echo "Generating datasets for sweep..."
python3 scripts/generate_data.py --default-matrix --sizes "$SIZES" --similarities "$SIMILARITIES"

# Keep every sweep's output under its own date-stamped folder so re-running the
# sweep no longer overwrites previous results.
RUN_TIMESTAMP="$(date +%Y-%m-%d_%H-%M-%S)"
METRICS_OUTPUT_DIR="${METRICS_OUTPUT_DIR:-metrics_output/${RUN_TIMESTAMP}}"
ANALYSIS_DIR="${METRICS_OUTPUT_DIR}/analysis"
export METRICS_OUTPUT_DIR
echo "Writing metrics for this sweep to ${METRICS_OUTPUT_DIR}"
mkdir -p "$METRICS_OUTPUT_DIR"

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

        if [ "$PER_TRIAL_DATASETS" = "true" ]; then
          trial_prefix="${dataset_prefix}_t${trial}"
          seed=$((trial * 999983 + size * 97 + 10#$sim_clean))
          python3 scripts/generate_data.py \
            --size "$size" --similarity "$sim" \
            --seed "$seed" \
            --prefix "$trial_prefix" \
            --output-dir data/
          ./scripts/run_experiment.sh "$protocol" "$trial_prefix" "$run_id" "$trial" "$sim"
          rm -f "data/${trial_prefix}_node1.json" "data/${trial_prefix}_node2.json" "data/${trial_prefix}_node3.json"
        else
          ./scripts/run_experiment.sh "$protocol" "$dataset_prefix" "$run_id" "$trial" "$sim"
        fi
      done
    done
  done
done

echo "Sweep finished. Analyzing ${METRICS_OUTPUT_DIR} into ${ANALYSIS_DIR}"

python3 scripts/analyze_similarity_bytes.py "$METRICS_OUTPUT_DIR" --output-dir "$ANALYSIS_DIR"
python3 scripts/analyze_similarity_resources.py "$METRICS_OUTPUT_DIR" --output-dir "$ANALYSIS_DIR"
python3 scripts/analyze_similarity_duration.py "$METRICS_OUTPUT_DIR" --output-dir "$ANALYSIS_DIR"
python3 scripts/analyze_similarity_scom.py "$METRICS_OUTPUT_DIR" --output-dir "$ANALYSIS_DIR"
python3 scripts/analyze_phase_split.py "$METRICS_OUTPUT_DIR" --output-dir "$ANALYSIS_DIR"
python3 scripts/analyze_roundtrip.py "$METRICS_OUTPUT_DIR" --output-dir "$ANALYSIS_DIR"
python3 scripts/analyze_cpu_usage.py "$METRICS_OUTPUT_DIR" --output-dir "$ANALYSIS_DIR"
python3 scripts/analyze_riblt_scaling.py "$METRICS_OUTPUT_DIR" --output-dir "$ANALYSIS_DIR"
python3 scripts/analyze_rbf_difference_reduction.py "$METRICS_OUTPUT_DIR" --output-dir "$ANALYSIS_DIR"
python3 scripts/make_phase_split_table.py "$METRICS_OUTPUT_DIR" --output "$ANALYSIS_DIR/tab_rbf_phase_split.tex"
python3 scripts/make_comparison_rbf_rsr_rbf_riblt_phases.py "$METRICS_OUTPUT_DIR" --output "$ANALYSIS_DIR/rbf_rsr_rbf_riblt_bytes.tex"

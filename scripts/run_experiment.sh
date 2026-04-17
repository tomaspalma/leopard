#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ "$#" -lt 2 ] || [ "$#" -gt 5 ]; then
    echo "Usage: $0 <protocol> <dataset_prefix> [run_id] [trial] [similarity]"
    echo "  protocol:       merkle | riblt"
    echo "  dataset_prefix: prefix used by data/<prefix>_nodeX.json"
    echo "  run_id:         optional metrics run id (default: <protocol>_<dataset_prefix>)"
    echo "  trial:          optional trial label (default: 1)"
    echo "  similarity:     optional similarity label (default: unknown)"
    exit 1
fi

PROTOCOL=$1
DATASET_PREFIX=$2
RUN_ID=${3:-"${PROTOCOL}_${DATASET_PREFIX}"}
TRIAL=${4:-"1"}
SIMILARITY=${5:-"unknown"}

echo "Running experiment with protocol=$PROTOCOL dataset=$DATASET_PREFIX run_id=$RUN_ID trial=$TRIAL similarity=$SIMILARITY"

cargo run -- --run-id "$RUN_ID" --trial "$TRIAL" --similarity "$SIMILARITY" custom-nodes --node-type "default" --protocol "$PROTOCOL" \
  --nodes "127.0.0.1,9000,3000,data/${DATASET_PREFIX}_node1.json" \
  --nodes "127.0.0.1,9001,3001,data/${DATASET_PREFIX}_node2.json" \
  --nodes "127.0.0.1,9002,3002,data/${DATASET_PREFIX}_node3.json"

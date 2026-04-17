#!/usr/bin/env bash

# Go to project root
cd "$(dirname "$0")/.."

if [ ! -d "data" ] || [ ! -f "data/small_node1.json" ]; then
    echo "Generating test data..."
    python3 generate_data.py --default-matrix
fi

echo "Running Merkle Tree tests..."
echo "-----------------------------------"
echo "Testing SMALL dataset (10 items)"
./scripts/run_experiment.sh merkle small

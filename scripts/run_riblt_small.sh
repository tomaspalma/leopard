#!/usr/bin/env bash

# Go to project root
cd "$(dirname "$0")/.."

if [ ! -d "data" ] || [ ! -f "data/small_node1.json" ]; then
    echo "Generating test data..."
    python generate_data.py
fi

echo "Running RIBLT tests..."
echo "-----------------------------------"
echo "Testing SMALL dataset (10 items)"
./scripts/run_experiment.sh riblt small

#!/usr/bin/env bash

if [ ! -d "data" ]; then
    echo "Generating test data..."
    python generate_data.py
fi

echo "Running Merkle Tree tests..."
echo "-----------------------------------"
echo "Testing SMALL dataset (10 items)"
./run_experiment.sh merkle small


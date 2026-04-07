#!/usr/bin/env bash

# This script runs the replication engine with custom nodes.
# You can add or remove '--nodes' flags as needed.
# Format: --nodes "[TYPE,]IP,INTERNAL_PORT,HTTP_PORT,DATASET_FILENAME"
# Note: TYPE is optional and defaults to "default".

echo "Starting custom nodes experiment..."

cargo run -- custom-nodes \
  --nodes "default,127.0.0.1,9000,3000,node1_data.json" \
  --nodes "default,127.0.0.1,9001,3001,node2_data.json" \
  --nodes "default,127.0.0.1,9002,3002,node3_data.json"

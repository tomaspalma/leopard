#!/usr/bin/env bash

echo "Starting custom nodes experiment..."

cargo run -- custom-nodes \
  --nodes "127.0.0.1,9000,3000,node1_data.json" \
  --nodes "127.0.0.1,9001,3001,node2_data.json" \
  --nodes "127.0.0.1,9002,3002,node3_data.json"

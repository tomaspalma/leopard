#!/usr/bin/env bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <node_type> <dataset_size>"
    echo "  node_type:    default | merkle | riblt"
    echo "  dataset_size: small | medium | large"
    exit 1
fi

NODE_TYPE=$1
SIZE=$2

echo "Running experiment with default nodes and $SIZE dataset..."

cargo run -- custom-nodes \
  --nodes "default,127.0.0.1,9000,3000,data/${SIZE}_node1.json" \
  --nodes "default,127.0.0.1,9001,3001,data/${SIZE}_node2.json" \
  --nodes "default,127.0.0.1,9002,3002,data/${SIZE}_node3.json"
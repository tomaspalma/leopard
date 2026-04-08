#!/usr/bin/env bash

# Go to project root
cd "$(dirname "$0")/.."

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <protocol> <dataset_size>"
    echo "  protocol:     merkle | riblt"
    echo "  dataset_size: small | medium | large"
    exit 1
fi

PROTOCOL=$1
SIZE=$2

echo "Running experiment with $PROTOCOL protocol and $SIZE dataset..."

cargo run -- custom-nodes --node-type "default" --protocol "$PROTOCOL" \
  --nodes "127.0.0.1,9000,3000,data/${SIZE}_node1.json" \
  --nodes "127.0.0.1,9001,3001,data/${SIZE}_node2.json" \
  --nodes "127.0.0.1,9002,3002,data/${SIZE}_node3.json"
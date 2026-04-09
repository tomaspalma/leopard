#!/usr/bin/env bash

# Go to project root so we can find generate_requests.py
cd "$(dirname "$0")/.."

# Check if count argument is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_requests>"
  exit 1
fi

COUNT=$1

# Validate count is a number
if ! [[ "$COUNT" =~ ^[0-9]+$ ]]; then
  echo "Error: The argument must be a positive integer."
  echo "Usage: $0 <number_of_requests>"
  exit 1
fi

PORTS=(3000 3001 3002)

for PORT in "${PORTS[@]}"; do
  echo "=================================================="
  echo "Sending $COUNT requests to 127.0.0.1:$PORT..."
  python3 generate_requests.py --count "$COUNT" --port "$PORT"
done

echo "=================================================="
echo "Finished sending requests to all nodes."

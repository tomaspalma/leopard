#!/usr/bin/env bash
#
# Attribute the Merkle protocol's peak RSS to its individual structures.
#
# Peak RSS (VmHWM) is a process high-water mark, so each structure is built in a
# fresh process and we diff the peaks:
#   map      - baseline = the BTreeMap<String,String> dataset
#   tree     - map      = the boxed-node hash tree (tree mode also holds an
#                         internal copy of the map, so this isolates the nodes)
#   snapshot - tree     = the per-session deep clone of the tree
#
# Usage: scripts/merkle_mem_profile.sh [data_file]

set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${1:-data/memprof_node1.json}"

if [ ! -f "$DATA" ]; then
  echo "Data file $DATA not found; generating n=100000 sim0 dataset..."
  python3 scripts/generate_data.py --size 100000 --similarity 0 --seed 42 \
    --prefix memprof --output-dir data/
fi

cargo build --release --bin merkle_mem_profile >/dev/null 2>&1

run() { cargo run --release --quiet --bin merkle_mem_profile -- "$1" "$DATA" | awk '{print $3}'; }

baseline=$(run baseline)
map=$(run map)
tree=$(run tree)
snapshot=$(run snapshot)

awk -v b="$baseline" -v m="$map" -v t="$tree" -v s="$snapshot" 'BEGIN {
  printf "\n=== Merkle peak-RSS attribution (data: %s) ===\n", "'"$DATA"'";
  printf "%-34s %8.1f MB\n", "baseline (entries Vec only)", b/1048576;
  printf "%-34s %8.1f MB\n", "+ dataset map (BTreeMap)",    (m-b)/1048576;
  printf "%-34s %8.1f MB\n", "+ hash tree (boxed nodes)",   (t-m)/1048576;
  printf "%-34s %8.1f MB\n", "+ session snapshot (clone)",  (s-t)/1048576;
  printf "%s\n", "----------------------------------------------";
  tree_side = (t-m) + (s-t);
  printf "%-34s %8.1f MB\n", "map total",         (m-b)/1048576;
  printf "%-34s %8.1f MB\n", "tree + snapshot",   tree_side/1048576;
  printf "%-34s %7.0f %%\n", "tree+snapshot share of the two", 100*tree_side/((m-b)+tree_side);
  printf "%-34s %8.1f MB\n", "full snapshot-mode peak RSS", s/1048576;
}'

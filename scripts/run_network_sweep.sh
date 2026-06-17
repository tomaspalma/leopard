#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

NET_DEV="${NET_DEV:-lo}"

# tc needs privilege; use sudo transparently when we are not already root.
if [ "$(id -u)" -eq 0 ]; then
  TC=(tc)
else
  TC=(sudo tc)
fi

# ---------------------------------------------------------------------------
# Network profiles: each row stands for communication between two locations, so
# the sweep shows how the protocols behave as peers get geographically further
# apart. The dominant axis is RTT (speed-of-light + routing), which is exactly
# the term a round-based reconciliation protocol is most sensitive to.
#
#   name | target_rtt_ms | jitter_ms | rate | loss_percent
#
# RTT/bandwidth values are representative of public inter-region latency
# matrices (e.g. AWS cross-region / cloudping); adjust to the exact city pairs
# you want to cite. "baseline" runs unshaped to anchor the intrinsic numbers.
# Empty fields are skipped. Override the active set per-run with the PROFILES env
# var (space-separated profile NAMES selecting a subset of the rows below).
# ---------------------------------------------------------------------------
PROFILE_ROWS=(
  "baseline|35|3|1gbit|0.01"      # e.g. Lisbon <-> Frankfurt
)

# Optional subset selection: PROFILES="baseline wan50" picks just those rows.
SELECTED="${PROFILES:-}"

NETSWEEP_ROOT="${NETSWEEP_ROOT:-metrics_output/network_speed}"
mkdir -p "$NETSWEEP_ROOT"

clear_netem() {
  # `|| true` so teardown never aborts the script (e.g. nothing was applied).
  "${TC[@]}" qdisc del dev "$NET_DEV" root 2>/dev/null || true
}

# Always strip shaping on the way out, including Ctrl-C / kill / errors.
trap clear_netem EXIT INT TERM

apply_netem() {
  local rtt_ms="$1" jitter_ms="$2" rate="$3" loss="$4"

  # Start from a clean slate for this profile.
  clear_netem

  # rtt 0 (and nothing else) => leave loopback untouched (true baseline).
  if [ "$rtt_ms" = "0" ] && [ -z "$rate" ] && { [ -z "$loss" ] || [ "$loss" = "0" ]; }; then
    echo "  (no shaping applied — baseline)"
    return 0
  fi

  # One-way delay is half the target RTT (see calibration note at top).
  local oneway jitter_oneway
  oneway="$(python3 -c 'import sys; print(f"{float(sys.argv[1])/2:g}")' "$rtt_ms")"
  jitter_oneway="$(python3 -c 'import sys; print(f"{float(sys.argv[1])/2:g}")' "$jitter_ms")"

  local params=(delay "${oneway}ms")
  if [ "$jitter_ms" != "0" ] && [ -n "$jitter_ms" ]; then
    params+=("${jitter_oneway}ms" distribution normal)
  fi
  if [ -n "$rate" ]; then
    params+=(rate "$rate")
  fi
  if [ -n "$loss" ] && [ "$loss" != "0" ]; then
    params+=(loss "${loss}%")
  fi

  echo "  Applying: tc qdisc add dev $NET_DEV root netem ${params[*]}"
  "${TC[@]}" qdisc add dev "$NET_DEV" root netem "${params[@]}"

  # Confirm realized RTT so calibration is visible in the log.
  echo "  Verifying realized RTT (target ${rtt_ms} ms):"
  ping -c 3 -q 127.0.0.1 | sed 's/^/    /' || true
}

# Cache sudo credentials up front so the prompt doesn't land mid-run.
if [ "${TC[0]}" = "sudo" ]; then
  sudo -v
fi

echo "Network sweep root: $NETSWEEP_ROOT"
echo

for row in "${PROFILE_ROWS[@]}"; do
  IFS='|' read -r name rtt jitter rate loss <<<"$row"

  # Honor PROFILES subset selection if provided.
  if [ -n "$SELECTED" ] && ! grep -qw -- "$name" <<<"$SELECTED"; then
    continue
  fi

  echo "==================================================================="
  echo "Profile: $name  (rtt=${rtt}ms jitter=${jitter}ms rate=${rate:-none} loss=${loss:-0}%)"
  echo "==================================================================="

  apply_netem "$rtt" "$jitter" "$rate" "$loss"

  # Send this profile's metrics + analysis to its own folder.
  profile_dir="${NETSWEEP_ROOT}/${name}"
  echo "  Metrics -> ${profile_dir}"
  echo

  METRICS_OUTPUT_DIR="$profile_dir" \
    ./scripts/run_similarity_sweep.sh

  clear_netem
  echo "  Profile '$name' done."
  echo
done

echo "All profiles finished. Results under: $NETSWEEP_ROOT"
echo "Per-profile analysis is in <profile>/analysis/ alongside the raw metrics."

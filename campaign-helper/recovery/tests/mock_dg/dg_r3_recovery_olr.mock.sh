#!/usr/bin/env bash
# Deprecated: use tests/harness/* against bin/dg_r3_recovery_olr.sh (recovery_phase mock path).
# Kept for manual fixture inspection only.
set -euo pipefail
port=${OLR_PORT:?}
RECOVERY_OLR_PORT=${RECOVERY_OLR_PORT:-39233}
FX="$(cd "$(dirname "$0")/.." && pwd)/fixtures"
MOCK_STATE="${MOCK_DG_STATE:?}"

assert_recovery_port() {
  [ "$port" = "$RECOVERY_OLR_PORT" ] || { echo "FATAL wrong port"; exit 2; }
}

mkdir -p "$MOCK_STATE"
case "${1:?}" in
snapshot)
  assert_recovery_port
  cp "$FX/olr-snapshot-pre.txt" "$MOCK_STATE/snapshot.txt"
  cat "$MOCK_STATE/snapshot.txt"
  ;;
stop)
  assert_recovery_port
  if [ -f "$MOCK_STATE/olr_dead" ]; then echo "already stopped"; exit 0; fi
  if [ -f "$MOCK_STATE/force_stop_fail" ]; then echo "FATAL still alive"; exit 1; fi
  cp "$FX/baseline-follower-pre.txt" "$MOCK_STATE/baseline_follower.txt"
  touch "$MOCK_STATE/olr_dead"
  echo "stop ok"
  cat "$MOCK_STATE/baseline_follower.txt"
  echo "baseline_follower_unchanged=1"
  ;;
continue)
  assert_recovery_port
  [ -f "$MOCK_STATE/olr_dead" ] || { echo "FATAL old pid alive"; exit 1; }
  [ -f "$MOCK_STATE/force_continue_fail" ] && { echo "FATAL continue died"; exit 1; }
  rm -f "$MOCK_STATE/olr_dead"
  cp "$FX/olr-snapshot-post.txt" "$MOCK_STATE/continue.txt"
  echo "config_unchanged=1"
  cat "$MOCK_STATE/continue.txt"
  ;;
verify)
  assert_recovery_port
  [ ! -f "$MOCK_STATE/olr_dead" ] || { echo "verify: not listening"; exit 1; }
  echo "verify ok"
  ;;
*)
  exit 2
  ;;
esac

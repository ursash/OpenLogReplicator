#!/usr/bin/env bash
set -euo pipefail
state="/home/shihao/.paseo/worktrees/3u7e3tdp/live-srl-flink-20261012/campaign-helper/recovery/evidence/local-r3-081025-3640347/phases/R3/mock-dg-state"
run="/home/shihao/.paseo/worktrees/3u7e3tdp/live-srl-flink-20261012/campaign-helper/recovery/evidence/local-r3-081025-3640347/phases/R3/mock-dg-state/run"
bin="/opt/olr-rac/src/rac9-wsguard-r1/cmake-build-release/OpenLogReplicator"
pin="4d93d70a98cd915c6a665929498e867bd2bec1b01be185620d4d15237cfbb9af"
touch "$state/proc/9001/gone"
rm -f "$state/olr_dead" "$state/listening"
echo 9002 > "$state/olr.pid"
write_olr_proc() {
  local pid=$1 starttime=$2 cmdline=$3
  local d="$state/proc/$pid"
  mkdir -p "$d"
  printf '%s' "$bin" > "$d/exe_path"
  printf '%s' "$pin" > "$d/exe_sha256"
  echo 54321 > "$d/uid"
  echo "$starttime" > "$d/starttime"
  printf '%s' "$cmdline" > "$d/cmdline"
  touch "$d/stat"
}
write_olr_proc 9002 200002 "$bin -f $run/config.json"
touch "$state/listening"

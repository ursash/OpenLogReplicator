#!/usr/bin/env bash
set -euo pipefail
state=$(dirname "$(dirname "$0")")
rm -f "$state/olr_dead"
echo 9002 > "$state/olr.pid"
touch "$state/listening"
printf '%s' "/opt/olr-rac/src/rac9-wsguard-r1/cmake-build-release/OpenLogReplicator" > "$state/proc/9002/exe"
run_dir="$state/run"
printf '%s' "/opt/olr-rac/src/rac9-wsguard-r1/cmake-build-release/OpenLogReplicator -f $run_dir/config.json" > "$state/proc/9002/cmdline"
echo 1000 > "$state/proc/9002/uid"
echo "Mon Oct 12 00:02:00 2026" > "$state/proc/9002/lstart"

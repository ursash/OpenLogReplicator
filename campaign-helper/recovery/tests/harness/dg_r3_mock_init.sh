#!/usr/bin/env bash
# Seed DG_R3_TEST_STATE for mock R3 (proc-shaped fixtures + boundary command mocks).
set -euo pipefail
state=${1:?}
run_id=${2:?}
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
HARNESS="$ROOT/tests/harness"
run="$state/run"
baseline="$state/baseline_run"
lab="$state/lab"
bin="/opt/olr-rac/src/rac9-wsguard-r1/cmake-build-release/OpenLogReplicator"
oracle_uid=54321

mkdir -p "$run/checkpoint" "$baseline" "$lab"
printf '%s\n' "{\"writer\":{\"uri\":\"127.0.0.1:39233\"}}" > "$run/config.json"
printf '%s\n' "{\"writer\":{\"uri\":\"127.0.0.1:39232\"}}" > "$baseline/config.json"

BLOB="$state/pinned_exe.blob"
export DG_R3_PINNED_EXE_BLOB="${DG_R3_PINNED_EXE_BLOB:-$ROOT/tests/fixtures/olr-pinned-exe.bin}"
python3 "$HARNESS/ensure_pinned_exe.py" "$BLOB"

write_olr() {
  local pid=$1 starttime=$2 cmdline=$3
  python3 "$HARNESS/write_proc_fixture.py" "$state/proc/$pid" "$pid" "$starttime" "$cmdline" "$BLOB"
}

write_follower() {
  local pid=$1 run_dir=$2
  local d="$state/proc/$pid"
  mkdir -p "$d"
  python3 "$HARNESS/write_proc_fixture.py" "$d" "$pid" "100001" "/bin/bash $lab/inventory_follow.sh" "$BLOB" "$run_dir"
  # Follower uses same blob for /proc/PID/exe shape; identity pin check applies to OLR pid only.
}

write_olr 9001 200001 "$bin -f $run/config.json"
write_follower 8101 "$baseline"
write_follower 8201 "$run"

echo 9001 > "$state/olr.pid"
echo "8101 8201" > "$state/follower_pids"
touch "$state/listening"

cat > "$state/on_continue.sh" <<SH
#!/usr/bin/env bash
set -euo pipefail
state="$state"
run="$run"
bin="$bin"
touch "\$state/proc/9001/gone"
rm -f "\$state/olr_dead" "\$state/listening"
echo 9002 > "\$state/olr.pid"
python3 "$HARNESS/write_proc_fixture.py" "\$state/proc/9002" 9002 200002 "\$bin -f \$run/config.json" "$BLOB"
touch "\$state/listening"
SH
chmod +x "$state/on_continue.sh"

[ -f "$state/continue_fail" ] && echo 'exit 1' >> "$state/on_continue.sh"

touch "$state/.init"
echo "init ok state=$state run_id=$run_id"

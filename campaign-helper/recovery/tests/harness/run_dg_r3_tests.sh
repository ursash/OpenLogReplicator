#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DG="$ROOT/bin/dg_r3_recovery_olr.sh"
PROCIO="$ROOT/bin/dg_r3_procio.py"
INIT="$ROOT/tests/harness/dg_r3_mock_init.sh"
MOCK_BIN="$ROOT/tests/harness/bin"
RUN_ID=MOCKTEST
RUN_NAME="ocr370-rac9-live-srl-e2e-${RUN_ID}-recovery-olr"
FAIL=0
SKIP_HAPPY=0
export DG_R3_PINNED_EXE_BLOB="${DG_R3_PINNED_EXE_BLOB:-$ROOT/tests/fixtures/olr-pinned-exe.bin}"
if ! python3 "$ROOT/tests/harness/ensure_pinned_exe.py" "$ROOT/tests/fixtures/.pinned_exe.cache" 2>/dev/null; then
  SKIP_HAPPY=1
  echo "dg3: skip happy-path (no olr-pinned-exe.bin matching pin; set OLR_PINNED_EXE_SOURCE)"
fi

run_case() {
  local name=$1 exp=$2
  shift 2
  set +e
  "$@"
  local got=$?
  set -e
  if [ "$got" = "$exp" ]; then echo "ok dg3 $name rc=$got"; else echo "FAIL dg3 $name want=$exp got=$got"; FAIL=1; fi
}

invoke() {
  local state=$1 sub=$2
  shift 2
  env PATH="$MOCK_BIN:$PATH" LAB="$state/lab" RUN_ID="$RUN_ID" RUN_NAME="$RUN_NAME" OLR_PORT=39233 \
    DG_R3_ORACLE_UID=54321 DG_R3_STOP_MAX_S="${DG_R3_STOP_MAX_S:-90}" \
    DG_R3_TEST_STATE="$state" DG_R3_RUN_DIR="$state/run" \
    DG_R3_BASELINE_RUN_DIR="$state/baseline_run" \
    DG_R3_PROCIO_FIXTURE_ROOT="$state/proc" DG_R3_PROCIO="$PROCIO" \
    DG_R3_PINNED_EXE_BLOB="$DG_R3_PINNED_EXE_BLOB" \
    bash "$DG" "$sub" "$@"
}

if [ "$SKIP_HAPPY" = 0 ]; then
  state_ok=$(mktemp -d)
  bash "$INIT" "$state_ok" "$RUN_ID"
  run_case preflight_pass 0 invoke "$state_ok" preflight
  run_case snapshot_pass 0 invoke "$state_ok" snapshot
  run_case stop_pass 0 invoke "$state_ok" stop
  run_case continue_pass 0 invoke "$state_ok" continue
  run_case verify_pass 0 invoke "$state_ok" verify
else
  echo "ok dg3 happy_path_skipped (no pinned exe blob)"
fi

pe_state=$(mktemp -d)
mkdir -p "$pe_state/proc/9001"
touch "$pe_state/proc/9001/stat"
run_case procio_exists_rc0 0 env DG_R3_PROCIO_FIXTURE_ROOT="$pe_state/proc" python3 "$PROCIO" proc_exists 9001
touch "$pe_state/proc/9001/gone"
run_case procio_gone_rc1 1 env DG_R3_PROCIO_FIXTURE_ROOT="$pe_state/proc" python3 "$PROCIO" proc_exists 9001
rm -f "$pe_state/proc/9001/gone"
touch "$pe_state/proc/9001/proc_exists_eperm"
run_case procio_eperm_rc4 4 env DG_R3_PROCIO_FIXTURE_ROOT="$pe_state/proc" python3 "$PROCIO" proc_exists 9001

run_case wrong_run_name 2 env PATH="$MOCK_BIN:$PATH" LAB=/tmp/lab RUN_ID="$RUN_ID" \
  RUN_NAME="ocr370-rac9-live-srl-e2e-${RUN_ID}-olr" OLR_PORT=39233 DG_R3_PROCIO="$PROCIO" \
  bash "$DG" snapshot

state_bad_pin=$(mktemp -d)
echo "0000000000000000000000000000000000000000000000000000000000000000" > "$state_bad_pin/bin_sha"
run_case wrong_pin 1 env PATH="$MOCK_BIN:$PATH" LAB="$state_bad_pin/lab" RUN_ID="$RUN_ID" RUN_NAME="$RUN_NAME" \
  OLR_PORT=39233 DG_R3_PROCIO="$PROCIO" DG_R3_TEST_STATE="$state_bad_pin" bash "$DG" preflight

if [ "$SKIP_HAPPY" = 0 ]; then
  state_no_olr=$(mktemp -d)
  bash "$INIT" "$state_no_olr" "$RUN_ID"
  rm -f "$state_no_olr/olr.pid" "$state_no_olr/listening"
  run_case missing_olr_at_start 1 invoke "$state_no_olr" preflight

  state_pgrep_fail=$(mktemp -d)
  bash "$INIT" "$state_pgrep_fail" "$RUN_ID"
  touch "$state_pgrep_fail/pgrep_fail"
  run_case pgrep_query_error 1 invoke "$state_pgrep_fail" preflight

  state_stop_fail=$(mktemp -d)
  bash "$INIT" "$state_stop_fail" "$RUN_ID"
  touch "$state_stop_fail/force_stop_fail"
  export DG_R3_STOP_MAX_S=2
  run_case stop_old_pid_alive 1 invoke "$state_stop_fail" stop
  unset DG_R3_STOP_MAX_S

  state_proc_eperm=$(mktemp -d)
  bash "$INIT" "$state_proc_eperm" "$RUN_ID"
  touch "$state_proc_eperm/force_stop_fail" "$state_proc_eperm/proc/9001/proc_exists_eperm"
  export DG_R3_STOP_MAX_S=2
  run_case proc_exists_query_error_on_stop 1 invoke "$state_proc_eperm" stop
  unset DG_R3_STOP_MAX_S

  state_cont=$(mktemp -d)
  bash "$INIT" "$state_cont" "$RUN_ID"
  run_case continue_while_alive 1 invoke "$state_cont" continue

  state_follower_absent=$(mktemp -d)
  bash "$INIT" "$state_follower_absent" "$RUN_ID"
  rm -f "$state_follower_absent/proc/8101/environ"
  run_case follower_run_dir_unreadable 1 invoke "$state_follower_absent" preflight

  state_wrong_loaded_pin=$(mktemp -d)
  bash "$INIT" "$state_wrong_loaded_pin" "$RUN_ID"
  printf 'CORRUPT-EXE-PAYLOAD' > "$state_wrong_loaded_pin/proc/9001/exe"
  run_case loaded_exe_pin_wrong 1 invoke "$state_wrong_loaded_pin" preflight

  state_listener_wrong=$(mktemp -d)
  bash "$INIT" "$state_listener_wrong" "$RUN_ID"
  touch "$state_listener_wrong/listener_wrong_pid"
  run_case listener_owner_mismatch 1 invoke "$state_listener_wrong" preflight

  state_continue_fail=$(mktemp -d)
  touch "$state_continue_fail/continue_fail"
  bash "$INIT" "$state_continue_fail" "$RUN_ID"
  invoke "$state_continue_fail" stop >/dev/null
  run_case continue_launch_error 1 invoke "$state_continue_fail" continue
fi

run_case no_pkill_in_script 0 bash -c '! rg -q pkill "'"$DG"'"'
run_case no_follower_launch 0 bash -c '! rg -q "nohup.*inventory_follow|pkill.*inventory_follow|olr_offline_continue" "'"$DG"'"'
run_case no_function_hook 0 bash -c '! rg -q DG_R3_TEST_HOOK "'"$DG"'"'
run_case requires_dg_r3_procio 0 bash -c '! rg -q BASH_SOURCE "'"$DG"'"'
run_case requires_procio_env 0 bash -c 'rg -q "DG_R3_PROCIO" "'"$DG"'"'
run_case listener_uses_sudo_ss 0 bash -c 'rg -q "sudo ss -Hltnp" "'"$DG"'"'

[ "$FAIL" = 0 ] && echo "DG_R3_TESTS=PASS" || { echo "DG_R3_TESTS=FAIL"; exit 1; }

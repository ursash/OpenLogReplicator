#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MOCK_PREP="$ROOT/tests/mock_prep"
FX="$ROOT/tests/fixtures"
DG="$ROOT/bin/dg_r3_recovery_olr.sh"
FAIL=0

run_case() {
  local name=$1 exp_rc=$2
  shift 2
  set +e
  "$@"
  local got=$?
  set -e
  if [ "$got" = "$exp_rc" ]; then echo "ok $name rc=$got"; else echo "FAIL $name expected=$exp_rc got=$got"; FAIL=1; fi
}

run_phase_mock() {
  local phase=$1 receipt=$2
  local att="$ROOT/evidence/local-$(echo "$phase" | tr '[:upper:]' '[:lower:]')-$(date -u +%H%M%S)-$$"
  mkdir -p "$att/ev/state/recovery"
  env RUN_ID=MOCK EV="$att/ev" PREP="$MOCK_PREP" ATTEMPT="$att" RECOVERY_EXEC=mock \
    CONNECTOR_NAME=ocr370-rac9-live-srl-MOCK-recovery \
    RECOVERY_CATCHUP_RECEIPT="$receipt" \
    TAP_PATH="$FX/tap-synthetic.jsonl" \
    "$ROOT/bin/recovery_phase.sh" "$phase"
}

run_case r1_mock_pass 0 run_phase_mock R1 "$FX/catchup-receipt-r1.json"
run_case r2_mock_pass 0 run_phase_mock R2 "$FX/catchup-receipt-r2.json"
bash "$ROOT/tests/harness/test_r2_launcher_cp.sh" || FAIL=1
if python3 "$ROOT/tests/harness/ensure_pinned_exe.py" "$ROOT/tests/fixtures/.pinned_exe.cache" 2>/dev/null; then
  run_case r3_mock_pass 0 run_phase_mock R3 "$FX/catchup-receipt-r3.json"
else
  echo "skip r3_mock_pass (no olr-pinned-exe.bin; set OLR_PINNED_EXE_SOURCE for full R3 mock)"
fi

run_case reject_baseline_rest 2 env RUN_ID=MOCK EV=/tmp/ev PREP="$MOCK_PREP" ATTEMPT=/tmp/att REST=http://127.0.0.1:28132 RECOVERY_EXEC=live \
  "$ROOT/bin/recovery_phase.sh" R1

att="$ROOT/evidence/local-no-receipt-$$"
mkdir -p "$att/ev/state/recovery"
run_case r1_progress_only_no_receipt 2 env RUN_ID=MOCK EV="$att/ev" PREP="$MOCK_PREP" ATTEMPT="$att" RECOVERY_EXEC=mock \
  CONNECTOR_NAME=ocr370-rac9-live-srl-MOCK-recovery TAP_PATH="$FX/tap-synthetic.jsonl" \
  "$ROOT/bin/recovery_phase.sh" R1

run_case dg_r3_no_pkill 0 bash -c '! rg -q pkill "'"$DG"'"'
run_case dg_r3_no_follower_launch 0 bash -c '! rg -q "nohup.*inventory_follow|pkill.*inventory_follow" "'"$DG"'"'

bash "$ROOT/tests/harness/run_dg_r3_tests.sh" || FAIL=1

run_case evidence_selftest 0 python3 "$ROOT/bin/recovery_evidence.py" selftest

[ "$FAIL" = 0 ] && echo "LOCAL_VERIFY=PASS" || { echo "LOCAL_VERIFY=FAIL"; exit 1; }

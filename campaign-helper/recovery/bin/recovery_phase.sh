#!/usr/bin/env bash
# Recovery lane fault phase: R1 (stop/resume), R2 (worker SIGKILL+relaunch), R3 (OLR stop/start).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=recovery_env.sh
source "$SCRIPT_DIR/recovery_env.sh"

PHASE=${1:?R1|R2|R3}
PHASE_DIR="$PHASE_OUT/$PHASE"
mkdir -p "$PHASE_DIR"

WORKER_PROPS=${WORKER_PROPS:-$PREP/config/$WORKER_PROPS_BASENAME}
REGISTER_JSON=${REGISTER_JSON:-$PREP/config/$REGISTER_JSON_BASENAME}
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
DG1=${DG1:-10.129.81.234}
P9_KEY=${P9_KEY:-$PREP/private/p9_ssh_key}
OLR_PORT=${OLR_PORT:-$RECOVERY_OLR_PORT}
RUN_NAME=${RUN_NAME:-ocr370-rac9-live-srl-e2e-${RUN_ID}-recovery-olr}
DG_LAB=${DG_LAB:-/opt/olr-rac/live-srl-e2e-${RUN_ID}-recovery-lab}
TAP_PATH=${TAP_PATH:-}
BASELINE_READONLY_SNAPSHOT=${BASELINE_READONLY_SNAPSHOT:-}
RECOVERY_CATCHUP_RECEIPT=${RECOVERY_CATCHUP_RECEIPT:-}
DG_R3_SCRIPT=${DG_R3_SCRIPT:-$SCRIPT_DIR/dg_r3_recovery_olr.sh}
DG_R3_PROCIO=${DG_R3_PROCIO:-$DG_LAB/dg_r3_procio.py}
DG_R3_REMOTE_SH=${DG_R3_REMOTE_SH:-$DG_LAB/dg_r3_recovery_olr.sh}
MOCK_DG_STATE=${MOCK_DG_STATE:-$PHASE_DIR/mock-dg-state}

PY="$SCRIPT_DIR/recovery_evidence.py"
FX="$SCRIPT_DIR/../tests/fixtures"
CMD_LOG="$PHASE_DIR/commands.jsonl"
verdict=INCONCLUSIVE
rc=3

log_cmd() {
  local cmd=$1 c=${2:-0}
  printf '{"utc":"%s","cmd":%s,"rc":%s}\n' "$(_ts)" "$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$cmd")" "$c" >> "$CMD_LOG"
}

write_verdict() {
  verdict=$1
  rc=$2
  python3 - "$PHASE_DIR" "$PHASE" "$verdict" "$rc" "$CMD_LOG" <<'PY'
import json, sys
from pathlib import Path
from datetime import datetime, timezone
phase_dir, phase, verdict, rc, cmd_log = Path(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4]), Path(sys.argv[5])
cmds = []
if cmd_log.is_file():
    cmds = [json.loads(l) for l in cmd_log.read_text().splitlines() if l.strip()]
doc = {
    "phase": phase,
    "verdict": verdict,
    "rc": rc,
    "utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "attempt": __import__("os").environ.get("ATTEMPT", ""),
    "recovery_exec": __import__("os").environ.get("RECOVERY_EXEC", ""),
    "conn": __import__("os").environ.get("CONN", ""),
    "offset_topic": __import__("os").environ.get("OFFSET_TOPIC", ""),
    "commands": cmds,
}
(phase_dir / f"{phase.lower()}-phase-record.json").write_text(json.dumps(doc, indent=2) + "\n")
PY
}

snap_rest() {
  local tag=$1
  local d="$PHASE_DIR/$1"
  mkdir -p "$d"
  if [ "$RECOVERY_EXEC" = "mock" ]; then
    cp "$FX/connector-config.json" "$d/config.json"
    cp "$FX/connector-running.json" "$d/status.json"
  else
    curl -sS -m 15 "$REST/connectors/$CONN/config" > "$d/config.json" 2> "$d/config.err" || true
    curl -sS -m 15 "$REST/connectors/$CONN/status" > "$d/status.json" 2> "$d/status.err" || true
  fi
}

snap_offsets() {
  local tag=$1
  local out="$PHASE_DIR/$1/connect-offsets.jsonl"
  mkdir -p "$(dirname "$out")"
  local mock=""
  [ "$RECOVERY_EXEC" = "mock" ] && mock="$FX/offsets-a.jsonl"
  python3 - <<PY
import sys
from pathlib import Path
sys.path.insert(0, "$SCRIPT_DIR")
import recovery_evidence as re
ok = re.dump_connect_offsets(
    Path("$PREP"), "$BOOTSTRAP", "$OFFSET_TOPIC", Path("$out"),
    Path("$ST/classes"), mock_fixture=Path("$mock") if "$mock" else None,
)
sys.exit(0 if ok else 1)
PY
}

wait_stopped() {
  local i max=${WAIT_STOPPED_S:-120}
  for i in $(seq 1 "$max"); do
    [ "$RECOVERY_EXEC" = "mock" ] && [ "$i" -ge 1 ] && return 0
    python3 - <<PY && return 0
import json, urllib.request, sys
sys.path.insert(0, "$SCRIPT_DIR")
import recovery_evidence as re
j=json.load(urllib.request.urlopen("$REST/connectors/$CONN/status", timeout=10))
sys.exit(0 if re.is_stopped(j) else 1)
PY
    sleep 1
  done
  return 1
}

wait_running() {
  local i max=${WAIT_RUNNING_S:-180}
  for i in $(seq 1 "$max"); do
    [ "$RECOVERY_EXEC" = "mock" ] && return 0
    python3 - <<PY && return 0
import json, urllib.request, sys
sys.path.insert(0, "$SCRIPT_DIR")
import recovery_evidence as re
j=json.load(urllib.request.urlopen("$REST/connectors/$CONN/status", timeout=10))
sys.exit(0 if re.is_running(j) else 1)
PY
    sleep 1
  done
  return 1
}

record_offset_progress() {
  local tag=$1 max=${WAIT_CATCHUP_S:-240}
  snap_offsets "${tag}-pre" || return 1
  local a="$PHASE_DIR/${tag}-pre/connect-offsets.jsonl"
  local pre_fp post_fp changed=0
  pre_fp=$(python3 "$PY" fingerprint "$a")
  local i b
  for i in $(seq 1 "$max"); do
    if [ "$RECOVERY_EXEC" = "mock" ]; then
      mkdir -p "$PHASE_DIR/${tag}-post"
      cp "$FX/offsets-b.jsonl" "$PHASE_DIR/${tag}-post/connect-offsets.jsonl"
      post_fp=$(python3 "$PY" fingerprint "$PHASE_DIR/${tag}-post/connect-offsets.jsonl")
      changed=1
      break
    fi
    snap_offsets "${tag}-poll" || continue
    b="$PHASE_DIR/${tag}-poll/connect-offsets.jsonl"
    post_fp=$(python3 "$PY" fingerprint "$b")
    if [ "$pre_fp" != "$post_fp" ]; then
      mkdir -p "$PHASE_DIR/${tag}-post"
      cp "$b" "$PHASE_DIR/${tag}-post/connect-offsets.jsonl"
      changed=1
      break
    fi
    sleep 1
  done
  post_fp=${post_fp:-$pre_fp}
  python3 - <<PY
import sys
from pathlib import Path
sys.path.insert(0, "$SCRIPT_DIR")
import recovery_evidence as re
re.write_progress_record(
    Path("$PHASE_DIR/offset-progress.json"),
    phase="$PHASE",
    changed=bool($changed),
    pre_fp="$pre_fp",
    post_fp="$post_fp",
)
PY
  [ "$changed" = 1 ]
}

require_catchup_receipt() {
  if [ -z "$RECOVERY_CATCHUP_RECEIPT" ] || [ ! -f "$RECOVERY_CATCHUP_RECEIPT" ]; then
    return 1
  fi
  python3 "$PY" check-receipt "$RECOVERY_CATCHUP_RECEIPT" "$PHASE" >/dev/null
}

run_dg_r3() {
  local sub=$1
  local out="$PHASE_DIR/dg-r3-${1}.txt"
  local erc=0
  local harness="$SCRIPT_DIR/../tests/harness"
  mkdir -p "$PHASE_DIR" "$MOCK_DG_STATE"
  if [ "$RECOVERY_EXEC" = "mock" ]; then
    if [ ! -f "$MOCK_DG_STATE/.init" ]; then
      bash "$harness/dg_r3_mock_init.sh" "$MOCK_DG_STATE" "$RUN_ID"
    fi
    env PATH="$harness/bin:$PATH" LAB="$MOCK_DG_STATE/lab" RUN_ID="$RUN_ID" RUN_NAME="$RUN_NAME" \
      OLR_PORT="$OLR_PORT" BASELINE_RUN_NAME="ocr370-rac9-live-srl-e2e-${RUN_ID}-olr" \
      DG_R3_ORACLE_UID=54321 DG_R3_TEST_STATE="$MOCK_DG_STATE" DG_R3_RUN_DIR="$MOCK_DG_STATE/run" \
      DG_R3_BASELINE_RUN_DIR="$MOCK_DG_STATE/baseline_run" \
      DG_R3_PROCIO_FIXTURE_ROOT="$MOCK_DG_STATE/proc" \
      DG_R3_PROCIO="$SCRIPT_DIR/dg_r3_procio.py" \
      DG_R3_PINNED_EXE_BLOB="${DG_R3_PINNED_EXE_BLOB:-$SCRIPT_DIR/../tests/fixtures/olr-pinned-exe.bin}" \
      bash "$DG_R3_SCRIPT" "$sub" > "$out" 2>&1 || erc=$?
  else
    dg_ssh "env LAB=$DG_LAB RUN_NAME=$RUN_NAME RUN_ID=$RUN_ID OLR_PORT=$OLR_PORT BASELINE_RUN_NAME=ocr370-rac9-live-srl-e2e-${RUN_ID}-olr DG_R3_PROCIO=$DG_R3_PROCIO bash $DG_R3_REMOTE_SH $sub" \
      > "$out" 2>&1 || erc=$?
  fi
  log_cmd "dg_r3_recovery_olr.sh $sub" "$erc"
  return "$erc"
}

verify_inflight() {
  snap_offsets inflight-a || return 1
  if [ -n "$TAP_PATH" ] && [ -f "$TAP_PATH" ]; then
    python3 - <<PY > "$PHASE_DIR/tap-pre.json"
import json, sys
sys.path.insert(0, "$SCRIPT_DIR")
import recovery_evidence as re
print(json.dumps(re.tap_progress_evidence(__import__("pathlib").Path("$TAP_PATH"))))
PY
  elif [ "$RECOVERY_EXEC" = "live" ]; then
    return 1
  fi
  sleep "${WAIT_INFLIGHT_POLL_S:-8}"
  if [ "$RECOVERY_EXEC" = "mock" ]; then
    mkdir -p "$PHASE_DIR/inflight-b"
    cp "$FX/offsets-b.jsonl" "$PHASE_DIR/inflight-b/connect-offsets.jsonl"
  else
    snap_offsets inflight-b || return 1
  fi
  if [ -f "$PHASE_DIR/tap-pre.json" ]; then
    TAP_POST_PATH="$TAP_PATH"
    if [ "$RECOVERY_EXEC" = "mock" ]; then
      TAP_POST_PATH="$PHASE_DIR/tap-post-synthetic.jsonl"
      cp "$TAP_PATH" "$TAP_POST_PATH"
      echo '{"conn":5}	{"payload":[{"op":"u","after":{"ID":1,"VAL":99}}]}' >> "$TAP_POST_PATH"
    fi
    python3 - <<PY > "$PHASE_DIR/tap-post.json"
import json, sys
sys.path.insert(0, "$SCRIPT_DIR")
import recovery_evidence as re
print(json.dumps(re.tap_progress_evidence(__import__("pathlib").Path("$TAP_POST_PATH"))))
PY
    python3 - <<PY || return 1
import json, sys
pre=json.load(open("$PHASE_DIR/tap-pre.json"))
post=json.load(open("$PHASE_DIR/tap-post.json"))
sys.exit(0 if pre.get("tail_sha256")!=post.get("tail_sha256") or pre.get("size_bytes")!=post.get("size_bytes") else 1)
PY
  fi
  python3 - <<PY
import sys
from pathlib import Path
sys.path.insert(0, "$SCRIPT_DIR")
import recovery_evidence as re
sys.exit(0 if re.inflight_progress(Path("$PHASE_DIR/inflight-a/connect-offsets.jsonl"), Path("$PHASE_DIR/inflight-b/connect-offsets.jsonl")) else 1)
PY
}

config_hash() {
  python3 "$PY" config-hash "$PREP" "$WORKER_PROPS_BASENAME" "$REGISTER_JSON_BASENAME"
}

worker_pid() {
  [ "$RECOVERY_EXEC" = "mock" ] && { echo 424242; return; }
  pgrep -f "ConnectDistributed.*worker-recovery.properties" | head -1 || true
}

dg_ssh() {
  ssh -o BatchMode=yes -i "$P9_KEY" -o UserKnownHostsFile="$PREP/private/known_hosts" "rocky@${DG1}" "$@"
}

assert_recovery_targets
log "phase=$PHASE exec=$RECOVERY_EXEC conn=$CONN"

CFG_HASH=$(config_hash 2>/dev/null || echo missing)
echo "$CFG_HASH" > "$PHASE_OUT/config-hash.txt"
python3 - <<PY > "$PHASE_OUT/register-identity.json"
import json, sys
sys.path.insert(0, "$SCRIPT_DIR")
import recovery_evidence as re
from pathlib import Path
print(json.dumps(re.parse_register_identity(Path("$REGISTER_JSON")), indent=2))
PY
[ -n "$BASELINE_READONLY_SNAPSHOT" ] && [ -f "$BASELINE_READONLY_SNAPSHOT" ] && \
  cp "$BASELINE_READONLY_SNAPSHOT" "$PHASE_OUT/baseline-continuity-snapshot.json"

attempt_pass() {
  record_offset_progress catchup || true
  if ! require_catchup_receipt; then
    verdict=INCONCLUSIVE
    rc=2
    log "missing or invalid RECOVERY_CATCHUP_RECEIPT for $PHASE"
    finish
  fi
  verdict=PASS
  rc=0
  finish
}

finish() {
  write_verdict "$verdict" "$rc"
  log "phase=$PHASE verdict=$verdict rc=$rc"
  exit "$rc"
}

case "$PHASE" in
R1)
  snap_rest pre
  snap_offsets pre-offsets || true
  if [ "$RECOVERY_EXEC" = "mock" ]; then
    log_cmd "PUT $REST/connectors/$CONN/stop" 0
    mkdir -p "$PHASE_DIR/stopped"
    cp "$FX/connector-stopped.json" "$PHASE_DIR/stopped/status.json"
    log_cmd "PUT $REST/connectors/$CONN/resume" 0
  else
    c=$(curl -sS -m 30 -o "$PHASE_DIR/stop.body" -w '%{http_code}' -X PUT "$REST/connectors/$CONN/stop" || echo 000)
    log_cmd "PUT stop" "$c"
  fi
  if ! wait_stopped; then verdict=FAIL; rc=1; finish; fi
  snap_offsets stopped-a || true
  sleep 15
  snap_offsets stopped-b || true
  if [ "$RECOVERY_EXEC" = "live" ] && [ -f "$PHASE_DIR/stopped-a/connect-offsets.jsonl" ] && [ -f "$PHASE_DIR/stopped-b/connect-offsets.jsonl" ]; then
    fa=$(python3 "$PY" fingerprint "$PHASE_DIR/stopped-a/connect-offsets.jsonl")
    fb=$(python3 "$PY" fingerprint "$PHASE_DIR/stopped-b/connect-offsets.jsonl")
    if [ "$fa" != "$fb" ]; then verdict=FAIL; rc=1; finish; fi
  fi
  if [ "$RECOVERY_EXEC" = "live" ]; then
    c=$(curl -sS -m 30 -o "$PHASE_DIR/resume.body" -w '%{http_code}' -X PUT "$REST/connectors/$CONN/resume" || echo 000)
    log_cmd "PUT resume" "$c"
  fi
  if ! wait_running; then verdict=FAIL; rc=1; finish; fi
  snap_rest post
  attempt_pass
  ;;
R2)
  if ! verify_inflight; then verdict=INCONCLUSIVE; rc=2; finish; fi
  WPID=$(worker_pid)
  [ -z "$WPID" ] && { verdict=INCONCLUSIVE; rc=2; finish; }
  snap_rest pre-kill
  if [ "$RECOVERY_EXEC" = "live" ]; then
    ps -o pid=,lstart=,args= -p "$WPID" > "$PHASE_DIR/worker-pre.ps" || true
    grep -q "worker-recovery.properties" "$PHASE_DIR/worker-pre.ps" || { verdict=INCONCLUSIVE; rc=2; finish; }
    [ -n "${RECOVERY_ALLOW_KILL:-}" ] || { verdict=INCONCLUSIVE; rc=2; finish; }
    kill -KILL "$WPID" && log_cmd "kill -KILL $WPID" 0 || { verdict=FAIL; rc=1; finish; }
    sleep 5
    "$PREP/scripts/p9_env.py" exec -- java -cp "$PREP/worker-lib/*" org.apache.kafka.connect.cli.ConnectDistributed \
      "$WORKER_PROPS" >> "$ST/logs/connect-r2-relaunch.log" 2>&1 &
    log_cmd "ConnectDistributed relaunch" 0
  else
    log_cmd "mock kill/relaunch" 0
  fi
  sleep 8
  NPID=$(worker_pid)
  [ "$RECOVERY_EXEC" = "mock" ] && NPID=999001
  [ -z "$NPID" ] && { verdict=FAIL; rc=1; finish; }
  [ "$(config_hash 2>/dev/null || echo x)" = "$CFG_HASH" ] || { verdict=FAIL; rc=1; finish; }
  if ! wait_running; then verdict=FAIL; rc=1; finish; fi
  snap_offsets post-relaunch || true
  echo "worker_before=$WPID worker_after=$NPID" > "$PHASE_DIR/worker-pids.txt"
  attempt_pass
  ;;
R3)
  snap_rest pre-olr
  snap_offsets pre-olr || true
  run_dg_r3 preflight || { verdict=FAIL; rc=1; finish; }
  run_dg_r3 snapshot || { verdict=FAIL; rc=1; finish; }
  cp "$PHASE_DIR/dg-r3-snapshot.txt" "$ST/dg/r3-snapshot.txt"
  run_dg_r3 stop || { verdict=FAIL; rc=1; finish; }
  if [ "$RECOVERY_EXEC" = "live" ] && grep -q "FATAL: recovery OLR still alive" "$PHASE_DIR/dg-r3-stop.txt" 2>/dev/null; then
    verdict=FAIL; rc=1; finish
  fi
  run_dg_r3 continue || { verdict=FAIL; rc=1; finish; }
  run_dg_r3 verify || { verdict=FAIL; rc=1; finish; }
  if ! wait_running; then verdict=FAIL; rc=1; finish; fi
  attempt_pass
  ;;
*)
  echo "usage: $0 R1|R2|R3" >&2
  exit 2
  ;;
esac

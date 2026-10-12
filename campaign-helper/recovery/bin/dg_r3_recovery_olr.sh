#!/usr/bin/env bash
# Recovery-lane OLR cold-continue on DG1. No inventory_follow stop/start. No dg/dg_live_srl.sh.
# env: LAB RUN_ID RUN_NAME OLR_PORT=39233 BASELINE_RUN_NAME (optional, for read-only follower evidence)
set -euo pipefail

PROCIO=${DG_R3_PROCIO:?DG_R3_PROCIO absolute path required (deploy dg_r3_procio.py to DG_LAB first)}

lab=${LAB:?}
name=${RUN_NAME:?}
port=${OLR_PORT:?}
: "${RUN_ID:?RUN_ID required for recovery namespace binding}"

BASELINE_RUN_NAME=${BASELINE_RUN_NAME:-ocr370-rac9-live-srl-e2e-${RUN_ID}-olr}
run=${DG_R3_RUN_DIR:-/opt/olr-rac/runs/$name}
baseline_run_dir=${DG_R3_BASELINE_RUN_DIR:-/opt/olr-rac/runs/$BASELINE_RUN_NAME}
bin=/opt/olr-rac/src/rac9-wsguard-r1/cmake-build-release/OpenLogReplicator
pin_bin=4d93d70a98cd915c6a665929498e867bd2bec1b01be185620d4d15237cfbb9af
RECOVERY_OLR_PORT=${RECOVERY_OLR_PORT:-39233}
STOP_IDENTITY_FILE="$run/.r3_stop_identity"

expected_recovery_run_name="ocr370-rac9-live-srl-e2e-${RUN_ID}-recovery-olr"
expected_olr_cmdline="$bin -f $run/config.json"

ts() { date -u +%FT%T.%3NZ; }

procio() {
  # Opens /proc inside privileged process (sudo python), not via shell redirection as rocky.
  sudo env DG_R3_PROCIO_FIXTURE_ROOT="${DG_R3_PROCIO_FIXTURE_ROOT:-}" python3 "$PROCIO" "$@"
}

oracle_uid() {
  sudo id -u oracle 2>/dev/null
}

assert_recovery_port() {
  if [ "$port" != "$RECOVERY_OLR_PORT" ]; then
    echo "FATAL: OLR_PORT=$port != recovery $RECOVERY_OLR_PORT" >&2
    exit 2
  fi
}

assert_recovery_namespace() {
  assert_recovery_port
  if [ "$name" != "$expected_recovery_run_name" ]; then
    echo "FATAL: RUN_NAME=$name != expected $expected_recovery_run_name" >&2
    exit 2
  fi
  if [ "$name" = "$BASELINE_RUN_NAME" ]; then
    echo "FATAL: RUN_NAME equals baseline run name" >&2
    exit 2
  fi
  case "$name" in
    *-baseline-*) echo "FATAL: baseline RUN_NAME rejected" >&2; exit 2 ;;
  esac
}

assert_bin_pinned() {
  local sha
  sha=$(sha256sum "$bin" | awk '{print $1}')
  if [ "$sha" != "$pin_bin" ]; then
    echo "FATAL: binary pin mismatch got=$sha want=$pin_bin" >&2
    exit 1
  fi
}

assert_config_writer_uri() {
  local cfg="$run/config.json"
  if [ -r "$cfg" ]; then
    [ -f "$cfg" ] || { echo "FATAL: missing $cfg" >&2; exit 1; }
    grep -Eq "127\\.0\\.0\\.1:${port}|:${port}\"" "$cfg" || {
      echo "FATAL: config.json missing writer/listener for recovery port $port" >&2
      exit 1
    }
    if grep -q "$baseline_run_dir/config.json" "$cfg" 2>/dev/null; then
      echo "FATAL: config references baseline run path" >&2
      exit 1
    fi
    return 0
  fi
  sudo test -f "$cfg" || { echo "FATAL: missing $cfg" >&2; exit 1; }
  if ! sudo grep -Eq "127\\.0\\.0\\.1:${port}|:${port}\"" "$cfg"; then
    echo "FATAL: config.json missing writer/listener for recovery port $port" >&2
    exit 1
  fi
  if sudo grep -q "$baseline_run_dir/config.json" "$cfg" 2>/dev/null; then
    echo "FATAL: config references baseline run path" >&2
    exit 1
  fi
}

pgrep_olr() {
  local rc=0 out
  set +e
  out=$(sudo -u oracle pgrep -f "^${bin} -f ${run}/config.json\$" 2>&1)
  rc=$?
  set -e
  case "$rc" in
    0)
      if [ "$(printf '%s\n' "$out" | sed '/^$/d' | wc -l)" -gt 1 ]; then
        echo "FATAL: multiple recovery OLR pids: $out" >&2
        exit 1
      fi
      printf '%s' "$out" | head -1
      ;;
    1) return 1 ;;
    *)
      echo "FATAL: pgrep olr failed rc=$rc: $out" >&2
      exit 1
      ;;
  esac
}

pgrep_followers() {
  local rc=0 out
  set +e
  out=$(pgrep -f "inventory_follow.sh" 2>&1)
  rc=$?
  set -e
  case "$rc" in
    0) printf '%s' "$out" ;;
    1) return 1 ;;
    *)
      echo "FATAL: pgrep inventory_follow failed rc=$rc: $out" >&2
      exit 1
      ;;
  esac
}

olr_pid() {
  pgrep_olr || return 1
}

listener_pid() {
  sudo ss -Hltnp "sport = :$port" 2>/dev/null | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -1
}

listening() {
  local lp
  lp=$(listener_pid)
  [ -n "$lp" ]
}

config_sha() {
  local cfg="$run/config.json"
  if [ -r "$cfg" ]; then
    sha256sum "$cfg" | awk '{print $1}'
  else
    sudo sha256sum "$cfg" 2>/dev/null | awk '{print $1}'
  fi
}

state_fingerprint() {
  if [ -n "${DG_R3_TEST_STATE:-}" ]; then
    (cd "$run" 2>/dev/null && find . -maxdepth 2 -type f \( -name 'config.json' -o -path './checkpoint/*' -o -name 'inventory_observations.jsonl' \) -print0 2>/dev/null | sort -z | xargs -0 sha256sum 2>/dev/null | sha256sum | awk '{print $1}')
    return
  fi
  sudo bash -c "cd '$run' 2>/dev/null && find . -maxdepth 2 -type f \\( -name 'config.json' -o -path './checkpoint/*' -o -name 'inventory_observations.jsonl' \\) -print0 2>/dev/null | sort -z | xargs -0 sha256sum 2>/dev/null | sha256sum | awk '{print \$1}'"
}

proc_identity_line() {
  local pid=$1 id rc=0
  set +e
  id=$(procio identity "$pid" 2>/dev/null)
  rc=$?
  set -e
  if [ "$rc" -ne 0 ] || [ -z "$id" ]; then
    echo "FATAL: unreadable proc identity pid=$pid rc=$rc" >&2
    exit 1
  fi
  python3 -c 'import json,sys; j=json.loads(sys.argv[1]); print("pid=%s uid=%s starttime=%s exe=%s exe_sha256=%s cmdline=%s" % (j["pid"], j["uid"], j["starttime"], j["exe"], j["exe_sha256"], j["cmdline"]))' "$id"
}

read_follower_run_dir() {
  local pid=$1 rc=0 rd
  set +e
  rd=$(procio environ_var "$pid" RUN_DIR 2>/dev/null)
  rc=$?
  set -e
  case "$rc" in
    0) printf '%s' "$rd" ;;
    3) return 3 ;;
    *)
      echo "FATAL: RUN_DIR read failed pid=$pid rc=$rc" >&2
      exit 1
      ;;
  esac
}

assert_recovery_olr_identity() {
  local p=$1 ouid lp sha
  ouid=$(oracle_uid)
  [ -n "$ouid" ] || { echo "FATAL: cannot resolve oracle uid" >&2; exit 1; }
  local line id
  set +e
  id=$(procio identity "$p" 2>/dev/null)
  local rc=$?
  set -e
  [ "$rc" -eq 0 ] && [ -n "$id" ] || { echo "FATAL: proc identity unreadable pid=$p" >&2; exit 1; }
  echo "olr_identity: $(proc_identity_line "$p")"
  sha=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["exe_sha256"])' "$id")
  if [ "$sha" != "$pin_bin" ]; then
    echo "FATAL: loaded exe sha256 mismatch pid=$p got=$sha want=$pin_bin" >&2
    exit 1
  fi
  python3 -c 'import json,sys; j=json.loads(sys.argv[1]); sys.exit(0 if str(j["uid"])==sys.argv[2] else 1)' "$id" "$ouid" \
    || { echo "FATAL: oracle uid mismatch pid=$p" >&2; exit 1; }
  python3 -c 'import json,sys; j=json.loads(sys.argv[1]); sys.exit(0 if j["cmdline"]==sys.argv[2] else 1)' "$id" "$expected_olr_cmdline" \
    || { echo "FATAL: cmdline mismatch pid=$p" >&2; exit 1; }
  lp=$(listener_pid)
  [ -n "$lp" ] || { echo "FATAL: no listener on :$port" >&2; exit 1; }
  [ "$lp" = "$p" ] || { echo "FATAL: listener pid=$lp != olr pid=$p" >&2; exit 1; }
}

follower_evidence_for_run_dir() {
  local label=$1 expect_dir=$2
  local found=0 pid rd rc pids
  if ! pids=$(pgrep_followers); then
    echo "FATAL: ${label}_follower missing (no inventory_follow processes)" >&2
    exit 1
  fi
  while read -r pid; do
    [ -n "$pid" ] || continue
    rd=""
    rc=0
    set +e
    rd=$(read_follower_run_dir "$pid")
    rc=$?
    set -e
    if [ "$rc" = 3 ] || [ -z "$rd" ]; then
      echo "FATAL: ${label}_follower pid=$pid RUN_DIR unreadable" >&2
      exit 1
    fi
    if [ "$rd" = "$expect_dir" ]; then
      found=1
      local fst
      fst=$(procio starttime "$pid") || { echo "FATAL: ${label}_follower pid=$pid starttime unreadable" >&2; exit 1; }
      echo "${label}_follower MATCH pid=$pid starttime=$fst RUN_DIR=$rd"
    fi
  done <<< "$pids"
  if [ "$found" = 0 ]; then
    echo "FATAL: ${label}_follower no MATCH for RUN_DIR=$expect_dir" >&2
    exit 1
  fi
}

snapshot_followers_readonly() {
  follower_evidence_for_run_dir baseline "$baseline_run_dir"
  follower_evidence_for_run_dir recovery "$run"
}

save_stop_identity() {
  local p=$1 id
  id=$(procio identity "$p")
  if [ -w "$run" ] 2>/dev/null; then
    printf '%s\n' "$id" > "$STOP_IDENTITY_FILE"
  else
    printf '%s\n' "$id" | sudo tee "$STOP_IDENTITY_FILE" >/dev/null
  fi
}

proc_exists_rc() {
  local pid=$1 rc=0
  set +e
  procio proc_exists "$pid" >/dev/null 2>&1
  rc=$?
  set -e
  printf '%s' "$rc"
}

original_process_gone() {
  local pid starttime now_st rc id_file=$STOP_IDENTITY_FILE
  if [ ! -r "$id_file" ]; then
    id_file=$(mktemp)
    sudo cat "$STOP_IDENTITY_FILE" > "$id_file"
  fi
  pid=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["pid"])' "$id_file")
  starttime=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["starttime"])' "$id_file")
  rc=$(proc_exists_rc "$pid")
  case "$rc" in
    1) return 0 ;;
    0)
      now_st=$(procio starttime "$pid") || {
        echo "FATAL: starttime unreadable for original pid=$pid during gone check" >&2
        exit 1
      }
      [ "$now_st" != "$starttime" ] && return 0
      return 1
      ;;
    *)
      echo "FATAL: proc_exists original pid=$pid rc=$rc (only rc=1 means gone)" >&2
      exit 1
      ;;
  esac
}

assert_original_gone_before_continue() {
  if [ ! -f "$STOP_IDENTITY_FILE" ]; then
    sudo test -f "$STOP_IDENTITY_FILE" || { echo "FATAL: missing $STOP_IDENTITY_FILE (stop first)" >&2; exit 1; }
  fi
  if original_process_gone; then
    return 0
  fi
  echo "FATAL: original OLR pid/starttime still present in /proc" >&2
  exit 1
}

assert_new_identity_after_continue() {
  local p=$1 old_id new_id
  if [ -r "$STOP_IDENTITY_FILE" ]; then
    old_id=$(cat "$STOP_IDENTITY_FILE")
  else
    old_id=$(sudo cat "$STOP_IDENTITY_FILE")
  fi
  new_id=$(procio identity "$p")
  python3 -c 'import json,sys; old=json.loads(sys.argv[1]); new=json.loads(sys.argv[2]); sys.exit(1 if str(old["pid"])==str(new["pid"]) and old["starttime"]==new["starttime"] else 0)' \
    "$old_id" "$new_id" || { echo "FATAL: new OLR identity equals pre-stop original" >&2; exit 1; }
}

r3_launch_pinned_olr() {
  local log="$run/olr.log"
  sudo -u oracle bash -c "cd $(printf '%q' "$run") && nohup $(printf '%q' "$bin") -f $(printf '%q' "$run/config.json") >> $(printf '%q' "$log") 2>&1 &"
}

cmd_preflight() {
  assert_recovery_namespace
  assert_bin_pinned
  assert_config_writer_uri
  local p
  p=$(olr_pid) || { echo "FATAL: recovery OLR not running at R3 preflight (no cold-restart PASS)" >&2; exit 1; }
  assert_recovery_olr_identity "$p"
  listening || { echo "FATAL: not listening on :$port" >&2; exit 1; }
  echo "PREFLIGHT=pass run=$run port=$port config_sha=$(config_sha)"
  snapshot_followers_readonly
}

cmd_snapshot() {
  assert_recovery_namespace
  assert_bin_pinned
  assert_config_writer_uri
  echo "SNAPSHOT t=$(ts) run=$run port=$port"
  echo "config_sha=$(config_sha)"
  echo "state_fp=$(state_fingerprint)"
  local p
  if p=$(olr_pid); then
    echo "olr_pid=$p"
    assert_recovery_olr_identity "$p"
  else
    echo "olr_pid=none"
  fi
  listening && echo "listening=1" || echo "listening=0"
  snapshot_followers_readonly
}

cmd_stop() {
  assert_recovery_namespace
  assert_bin_pinned
  assert_config_writer_uri
  local p
  p=$(olr_pid) || { echo "FATAL: recovery OLR not running; refuse stop/continue cold path" >&2; exit 1; }
  assert_recovery_olr_identity "$p"
  save_stop_identity "$p"
  local pre_f post_f
  pre_f=$(mktemp)
  post_f=$(mktemp)
  snapshot_followers_readonly > "$pre_f"
  [ -s "$pre_f" ] || { echo "FATAL: empty follower evidence before stop" >&2; exit 1; }
  echo "follower_evidence_pre_stop:" >&2
  cat "$pre_f" >&2
  echo "stop: INT to recovery pid=$p only (no inventory_follow changes)"
  proc_identity_line "$p"
  sudo -u oracle kill -INT "$p"
  local i gone=0
  for i in $(seq 1 "${DG_R3_STOP_MAX_S:-90}"); do
    if original_process_gone && ! pgrep_olr >/dev/null 2>&1; then
      gone=1
      break
    fi
    sleep 1
  done
  if [ "$gone" != 1 ]; then
    rm -f "$pre_f" "$post_f"
    echo "FATAL: recovery OLR still alive after INT (pid/starttime or pgrep)" >&2
    exit 1
  fi
  snapshot_followers_readonly > "$post_f"
  [ -s "$post_f" ] || { echo "FATAL: empty follower evidence after stop" >&2; exit 1; }
  echo "follower_evidence_post_stop:" >&2
  cat "$post_f" >&2
  if ! cmp -s "$pre_f" "$post_f"; then
    rm -f "$pre_f" "$post_f"
    echo "FATAL: follower read-only evidence changed across recovery OLR stop" >&2
    exit 1
  fi
  rm -f "$pre_f" "$post_f"
  echo "$(ts) stop ok original_gone=1"
}

cmd_continue() {
  assert_recovery_namespace
  assert_bin_pinned
  assert_config_writer_uri
  assert_original_gone_before_continue
  if pgrep_olr >/dev/null 2>&1; then
    echo "FATAL: recovery OLR pgrep match before continue launch" >&2
    exit 1
  fi
  local cfg_before
  cfg_before=$(config_sha)
  [ -n "$cfg_before" ] || { echo "FATAL: missing config.json before continue" >&2; exit 1; }
  echo "$(ts) continue direct pinned binary (no config rewrite; persisted checkpoint)"
  r3_launch_pinned_olr
  local i
  for i in $(seq 1 120); do listening && break; sleep 0.5; done
  listening || { echo "FATAL: not listening on :$port after continue" >&2; exit 1; }
  local p
  p=$(olr_pid) || { echo "FATAL: no pid after continue" >&2; exit 1; }
  assert_recovery_olr_identity "$p"
  assert_new_identity_after_continue "$p"
  local cfg_after
  cfg_after=$(config_sha)
  if [ "$cfg_before" != "$cfg_after" ]; then
    echo "FATAL: config.json changed across continue" >&2
    exit 1
  fi
  echo "$(ts) continue pid=$p config_unchanged=1 state_fp=$(state_fingerprint)"
  proc_identity_line "$p"
  sudo tail -8 "$run/olr.log" 2>/dev/null | sed 's/^/olr: /' || true
}

cmd_verify() {
  assert_recovery_namespace
  listening || { echo "verify: not listening"; exit 1; }
  local p
  p=$(olr_pid) || { echo "verify: no pid"; exit 1; }
  assert_recovery_olr_identity "$p"
  assert_config_writer_uri
  echo "verify ok pid=$p state_fp=$(state_fingerprint)"
  snapshot_followers_readonly
}

case "${1:?}" in
preflight) cmd_preflight ;;
snapshot) cmd_snapshot ;;
stop) cmd_stop ;;
continue) cmd_continue ;;
verify) cmd_verify ;;
*)
  echo "usage: env LAB RUN_ID RUN_NAME OLR_PORT=39233 bash $0 preflight|snapshot|stop|continue|verify" >&2
  exit 2
  ;;
esac

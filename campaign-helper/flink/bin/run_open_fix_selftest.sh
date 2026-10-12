#!/usr/bin/env bash
# OPEN-fix local verification: real scripts/modules + mocks (no P9 / no Flink cluster).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
ATTEMPT_ID="$(date -u +%Y%m%dT%H%M%SZ)"
SUMMARY="$ROOT/evidence/fix-hold-selftest/CURRENT-ATTEMPT-${ATTEMPT_ID}.json"
mkdir -p "$(dirname "$SUMMARY")"
FAIL=0

run_case() {
  local name=$1 expected=$2
  shift 2
  local got="pass" ec=0
  set +e
  "$@" >/tmp/open-fix-case.out 2>&1
  ec=$?
  set -e
  if [ "$ec" -ne 0 ]; then
    got="fail"
  fi
  python3 - "$SUMMARY" "$ATTEMPT_ID" "$name" "$expected" "$got" "$ec" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
attempt, name, expected, got, ec = sys.argv[2:7]
doc = json.loads(p.read_text()) if p.is_file() else {"attempt_id": attempt, "cases": []}
doc["cases"].append({"case": name, "expected": expected, "got": got, "exit_code": int(ec)})
p.write_text(json.dumps(doc, indent=2))
PY
  if [ "$expected" != "$got" ]; then
    echo "FAIL case=$name expected=$expected got=$got" >&2
    cat /tmp/open-fix-case.out >&2 || true
    FAIL=1
  else
    echo "ok $name"
  fi
}

# Astra finite recipe on worktree code paths
HARNESS_OUT="/tmp/astra-harness-${ATTEMPT_ID}.json"
set +e
python3 "$ROOT/bin/astra_regress_harness.py" >"$HARNESS_OUT"
HARNESS_EC=$?
set -e
run_case astra_harness pass bash -c "exit $HARNESS_EC"

run_case submit_bind_selftest pass python3 "$ROOT/bin/submit_bind.py" --selftest
run_case wait_ready_selftest pass python3 "$ROOT/bin/wait_ready_probe.py" --selftest

# Identity: empty pid dir + REQUIRE_PID_FILES
FAKE_PARCEL="$(mktemp -d)"
mkdir -p "$FAKE_PARCEL/lib/flink"
run_case identity_empty_pid_dir_fail fail bash -c "
  export FLINK_PARCEL='$FAKE_PARCEL'
  td=\$(mktemp -d)
  export FLINK_RUNTIME_MODE=aux RUNTIME_ROOT=\"\$td\" REST_PORT=28199
  export FLINK_CONF_DIR=\"\$td/conf\" FLINK_PID_DIR=\"\$td/pid\" FLINK_LOG_DIR=\"\$td/log\"
  mkdir -p \"\$FLINK_CONF_DIR\" \"\$FLINK_PID_DIR\"
  printf 'rest.port: 28199\njobmanager.rpc.port: 28200\nstate.checkpoints.dir: file://%s/cp\n' \"\$td\" >\"\$FLINK_CONF_DIR/flink-conf.yaml\"
  printf '{\"mode\":\"aux\",\"runtime_root\":\"%s\",\"rest_port\":28199,\"jm_rpc\":28200,\"tm_rpc\":28201}\n' \"\$td\" >\"\$FLINK_CONF_DIR/runtime-identity.json\"
  export JM_RPC_PORT=28200 TM_RPC_PORT=28201 TM_DATA_PORT=28202 BLOB_PORT=28203
  export RUNTIME_IDENTITY=\"aux:\$td:rest=28199:jm=28200\"
  cd '$ROOT' && REQUIRE_PID_FILES=1 ./bin/verify_runtime_identity.sh
" fail

# Identity: standalonesession + taskexecutor cmdline
run_case identity_roles_ok pass bash -c "
  export FLINK_PARCEL='$FAKE_PARCEL'
  td=\$(mktemp -d)
  export FLINK_RUNTIME_MODE=aux RUNTIME_ROOT=\"\$td\" REST_PORT=28210
  export FLINK_CONF_DIR=\"\$td/conf\" FLINK_PID_DIR=\"\$td/pid\" FLINK_LOG_DIR=\"\$td/log\"
  mkdir -p \"\$FLINK_CONF_DIR\" \"\$FLINK_PID_DIR\"
  printf 'rest.port: 28210\njobmanager.rpc.port: 28211\nstate.checkpoints.dir: file://%s/cp\n' \"\$td\" >\"\$FLINK_CONF_DIR/flink-conf.yaml\"
  printf '{\"mode\":\"aux\",\"runtime_root\":\"%s\",\"rest_port\":28210,\"jm_rpc\":28211,\"tm_rpc\":28212}\n' \"\$td\" >\"\$FLINK_CONF_DIR/runtime-identity.json\"
  export JM_RPC_PORT=28211 TM_RPC_PORT=28212 TM_DATA_PORT=28213 BLOB_PORT=28214
  export RUNTIME_IDENTITY=\"aux:\$td:rest=28210:jm=28211\"
  bash -c 'sleep 600' -- --configDir \"\$FLINK_CONF_DIR\" standalonesession &
  jm=\$!
  bash -c 'sleep 600' -- --configDir \"\$FLINK_CONF_DIR\" taskexecutor &
  tm=\$!
  echo \$jm >\"\$FLINK_PID_DIR/flink-aux-standalonesession.pid\"
  echo \$tm >\"\$FLINK_PID_DIR/flink-aux-taskexecutor.pid\"
  cd '$ROOT' && REQUIRE_PID_FILES=1 ./bin/verify_runtime_identity.sh
  kill \$jm \$tm 2>/dev/null || true
" pass

# Stop fail-closed: fake FLINK_HOME stop exits 1
run_case runtime_stop_fail_nonzero fail bash -c "
  export FLINK_PARCEL='$FAKE_PARCEL'
  td=\$(mktemp -d)
  fh=\"\$td/fake-flink\"
  mkdir -p \"\$fh/bin\" \"\$fh/lib\" \"\$td/conf\" \"\$td/pid\" \"\$td/log\" \"\$td/lib\"
  touch \"\$td/lib/flink-sql-connector-kafka-3.4.0-1.20.jar\"
  printf '#!/bin/bash\nexit 1\n' >\"\$fh/bin/taskmanager.sh\"
  printf '#!/bin/bash\nexit 1\n' >\"\$fh/bin/jobmanager.sh\"
  chmod +x \"\$fh/bin/\"*.sh
  export FLINK_RUNTIME_MODE=aux RUNTIME_ROOT=\"\$td\" REST_PORT=28220
  export FLINK_CONF_DIR=\"\$td/conf\" FLINK_PID_DIR=\"\$td/pid\" FLINK_LOG_DIR=\"\$td/log\"
  export FLINK_HOME=\"\$fh\"
  export FLINK_HELPER_QUIET=1
  export FLINK_HELPER_SKIP_ENSURE=1
  export FLINK_HOME_APACHE=\"\$fh\"
  printf 'rest.port: 28220\njobmanager.rpc.port: 28221\nstate.checkpoints.dir: file://%s/cp\n' \"\$td\" >\"\$FLINK_CONF_DIR/flink-conf.yaml\"
  printf '{\"mode\":\"aux\",\"runtime_root\":\"%s\",\"rest_port\":28220,\"jm_rpc\":28221,\"tm_rpc\":28222}\n' \"\$td\" >\"\$FLINK_CONF_DIR/runtime-identity.json\"
  export JM_RPC_PORT=28221 TM_RPC_PORT=28222 TM_DATA_PORT=28223 BLOB_PORT=28224
  export RUNTIME_IDENTITY=\"aux:\$td:rest=28220:jm=28221\"
  bash -c 'sleep 600' -- --configDir \"\$FLINK_CONF_DIR\" standalonesession &
  jm=\$!
  bash -c 'sleep 600' -- --configDir \"\$FLINK_CONF_DIR\" taskexecutor &
  tm=\$!
  echo \$jm >\"\$FLINK_PID_DIR/flink-aux-standalonesession.pid\"
  echo \$tm >\"\$FLINK_PID_DIR/flink-aux-taskexecutor.pid\"
  cd '$ROOT'
  if ./bin/runtime_ctl.sh stop; then exit 0; else exit 1; fi
" fail

python3 - "$SUMMARY" "$HARNESS_OUT" "$FAIL" <<'PY'
import json, pathlib, sys
summary = pathlib.Path(sys.argv[1])
harness = pathlib.Path(sys.argv[2])
fail = int(sys.argv[3])
doc = json.loads(summary.read_text())
doc["harness_output"] = json.loads(harness.read_text())
doc["overall"] = "PASS" if fail == 0 else "FAIL"
doc["note"] = "NEGATIVE-RESULTS.json history preserved; this file is current attempt only."
summary.write_text(json.dumps(doc, indent=2))
print(summary)
PY

"$ROOT/bin/freeze_manifest.sh"
(cd "$ROOT" && sha256sum -c FROZEN-SHA256.txt >/dev/null)

echo "open_fix_selftest attempt=$ATTEMPT_ID fail=$FAIL summary=$SUMMARY"
exit "$FAIL"

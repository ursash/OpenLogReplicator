#!/usr/bin/env bash
# OPEN37 remaining 3: identity + READY + seal (Submit/MD closed — not run here).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
ATTEMPT_ID="$(date -u +%Y%m%dT%H%M%SZ)"
SUMMARY="$ROOT/evidence/fix-hold-selftest/CURRENT-ATTEMPT-${ATTEMPT_ID}.json"
mkdir -p "$(dirname "$SUMMARY")"
FAIL=0

record() {
  python3 - "$SUMMARY" "$ATTEMPT_ID" "$1" "$2" "$3" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
doc = json.loads(p.read_text()) if p.is_file() else {"attempt_id": sys.argv[2], "scope": "open37_remaining_3", "cases": []}
doc["cases"].append({"case": sys.argv[3], "expected": sys.argv[4], "got": sys.argv[5]})
p.write_text(json.dumps(doc, indent=2))
PY
}

run() {
  local name=$1 exp=$2
  shift 2
  local got=pass
  if ! "$@"; then
    got=fail
  fi
  record "$name" "$exp" "$got"
  if [ "$got" != "$exp" ]; then
    echo "FAIL $name expected=$exp got=$got" >&2
    FAIL=1
  else
    echo "ok $name"
  fi
}

run identity_parser pass python3 "$ROOT/bin/flink_identity_parser.py" --selftest
run ready_probe pass python3 "$ROOT/bin/wait_ready_probe.py" --selftest
run seal_core pass python3 "$ROOT/bin/seal_evidence_core.py" --selftest

FAKE_PARCEL="$(mktemp -d)"
mkdir -p "$FAKE_PARCEL/lib/flink"
export FLINK_PARCEL="$FAKE_PARCEL"

run identity_empty_pid fail bash -c "
  td=\$(mktemp -d)
  export FLINK_PARCEL='$FAKE_PARCEL' FLINK_RUNTIME_MODE=aux RUNTIME_ROOT=\"\$td\" REST_PORT=28199
  export FLINK_CONF_DIR=\"\$td/conf\" FLINK_PID_DIR=\"\$td/pid\" FLINK_LOG_DIR=\"\$td/log\"
  mkdir -p \"\$FLINK_CONF_DIR\" \"\$FLINK_PID_DIR\"
  printf 'rest.port: 28199\njobmanager.rpc.port: 28200\n' >\"\$FLINK_CONF_DIR/flink-conf.yaml\"
  printf '{\"mode\":\"aux\",\"runtime_root\":\"%s\",\"rest_port\":28199,\"jm_rpc\":28200,\"tm_rpc\":28201}\n' \"\$td\" >\"\$FLINK_CONF_DIR/runtime-identity.json\"
  export JM_RPC_PORT=28200 TM_RPC_PORT=28201 TM_DATA_PORT=28202 BLOB_PORT=28203
  export RUNTIME_IDENTITY=\"aux:\$td:rest=28199:jm=28200\"
  cd '$ROOT' && REQUIRE_PID_FILES=1 ./bin/verify_runtime_identity.sh
"

python3 - "$SUMMARY" "$FAIL" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
doc = json.loads(p.read_text())
doc["overall"] = "PASS" if int(sys.argv[2]) == 0 else "FAIL"
doc["not_live"] = [
    "Java Flink process identity on P9 (parser validated against P9-shaped argv fixture only)",
    "REST listener owner vs JM pid on production host",
    "seal_evidence.sh end-to-end with live REST checkpoint fetch",
]
doc["closed_not_retested"] = ["submit_bind", "MAIN_INTEGRATION_md_cd"]
p.write_text(json.dumps(doc, indent=2))
print(p)
PY

"$ROOT/bin/freeze_manifest.sh"
(cd "$ROOT" && sha256sum -c FROZEN-SHA256.txt >/dev/null)
echo "open37_local_verify attempt=$ATTEMPT_ID fail=$FAIL"
exit "$FAIL"

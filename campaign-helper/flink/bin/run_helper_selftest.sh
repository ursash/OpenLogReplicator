#!/usr/bin/env bash
# Deterministic counter-example checks for HOLD fixes (no Flink cluster required).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
FAIL=0
RESULTS="$ROOT/evidence/fix-hold-selftest/NEGATIVE-RESULTS.json"
mkdir -p "$(dirname "$RESULTS")"

record() {
  python3 - "$RESULTS" "$@" <<'PY'
import json, pathlib, sys
out = pathlib.Path(sys.argv[1])
rows = json.loads(out.read_text()) if out.is_file() else []
rows.append(json.loads(sys.argv[2]))
out.write_text(json.dumps(rows, indent=2))
PY
}

assert_fail() {
  local name=$1
  shift
  if "$@" >/dev/null 2>&1; then
    record "{\"case\":\"$name\",\"expected\":\"fail\",\"got\":\"pass\"}"
    echo "FAIL: $name should have failed" >&2
    FAIL=1
  else
    record "{\"case\":\"$name\",\"expected\":\"fail\",\"got\":\"fail\"}"
    echo "ok $name"
  fi
}

assert_ok() {
  local name=$1
  shift
  if "$@" >/dev/null 2>&1; then
    record "{\"case\":\"$name\",\"expected\":\"pass\",\"got\":\"pass\"}"
    echo "ok $name"
  else
    record "{\"case\":\"$name\",\"expected\":\"pass\",\"got\":\"fail\"}"
    echo "FAIL: $name should pass" >&2
    FAIL=1
  fi
}

APPLY_PORTS="source '$ROOT/bin/runtime_ports.sh' && apply_runtime_ports"

# --- runtime_ports: env must not override campaign JM RPC to 6123 ---
assert_fail campaign_jm_rpc_override bash -c "
  export FLINK_RUNTIME_MODE=campaign
  export RUNTIME_ROOT=/opt/oracle-cdc-benchmark/flink-live-srl-20261012T031500Z
  export REST_PORT=28120 JM_RPC_PORT=6123
  $APPLY_PORTS
"

assert_ok campaign_ports_locked bash -c "
  export FLINK_RUNTIME_MODE=campaign
  export RUNTIME_ROOT=/opt/oracle-cdc-benchmark/flink-live-srl-20261012T031500Z
  export REST_PORT=28120
  $APPLY_PORTS
  test \"\$JM_RPC_PORT\" = \"28121\" && test \"\$TM_RPC_PORT\" = \"28122\"
"

assert_fail aux_rest_main_reserved bash -c "
  export FLINK_RUNTIME_MODE=aux
  export RUNTIME_ROOT=/tmp/flink-aux-smoke-aux20261012T099999Z
  export REST_PORT=28120
  $APPLY_PORTS
"

assert_ok aux_portmap_offset bash -c "
  export FLINK_RUNTIME_MODE=aux
  export RUNTIME_ROOT=/tmp/flink-aux-smoke-aux20261012T099999Z
  export REST_PORT=28140
  $APPLY_PORTS
  test \"\$JM_RPC_PORT\" = \"28141\" && test \"\$BLOB_PORT\" = \"28144\"
"

echo "delegating to bin/open39_local_verify.sh (reviewer39 HOLD; NEGATIVE-RESULTS history preserved)" >&2
exec "$ROOT/bin/open39_local_verify.sh"

#!/usr/bin/env bash
# Verify PID files / listeners belong to this RUNTIME_ROOT before stop or start reuse.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=flink_env.sh
source "$SCRIPT_DIR/flink_env.sh"

REQUIRE_PID=${REQUIRE_PID_FILES:-0}
REQUIRE_REUSE=${REQUIRE_START_REUSE:-0}

expected_conf_dir="$(readlink -f "$FLINK_CONF_DIR" 2>/dev/null || echo "$FLINK_CONF_DIR")"
expected_root="$(readlink -f "$RUNTIME_ROOT" 2>/dev/null || echo "$RUNTIME_ROOT")"

verify_pid_file() {
  local pf=$1
  local role=${2:?standalonesession|taskexecutor}
  [ -f "$pf" ] || {
    echo "FATAL: missing pid file $pf (cannot prove ownership)" >&2
    return 1
  }
  local pid
  pid="$(cat "$pf" 2>/dev/null)" || {
    echo "FATAL: unreadable pid file $pf" >&2
    return 1
  }
  [[ "$pid" =~ ^[0-9]+$ ]] || {
    echo "FATAL: invalid pid in $pf" >&2
    return 1
  }
  if [[ "$pf" != *"$role"* ]]; then
    echo "FATAL: pid filename $pf does not match role $role" >&2
    return 1
  fi
  parsed_pid="$(python3 "$SCRIPT_DIR/flink_identity_parser.py" verify-live "$pid" "$role" "$expected_conf_dir")"
  [[ "$parsed_pid" =~ ^[0-9]+$ ]] || {
    echo "FATAL: identity parser stdout must be numeric pid (got: $parsed_pid)" >&2
    return 1
  }
  [ "$parsed_pid" = "$pid" ] || {
    echo "FATAL: identity parser pid $parsed_pid != pid file $pid" >&2
    return 1
  }
  printf '%s' "$pid"
}

verify_conf_ownership() {
  local conf="$FLINK_CONF_DIR/flink-conf.yaml"
  [ -f "$conf" ] || {
    echo "FATAL: missing $conf" >&2
    return 1
  }
  local rp jm
  rp="$(grep -E '^rest\.port:' "$conf" | awk '{print $2}')"
  jm="$(grep -E '^jobmanager\.rpc\.port:' "$conf" | awk '{print $2}')"
  [ "$rp" = "$REST_PORT" ] || {
    echo "FATAL: conf rest.port=$rp != REST_PORT=$REST_PORT" >&2
    return 1
  }
  [ "$jm" = "$JM_RPC_PORT" ] || {
    echo "FATAL: conf jobmanager.rpc.port=$jm != JM_RPC_PORT=$JM_RPC_PORT" >&2
    return 1
  }
  local ident="$FLINK_CONF_DIR/runtime-identity.json"
  [ -f "$ident" ] || {
    echo "FATAL: missing $ident" >&2
    return 1
  }
  python3 - "$ident" "$FLINK_RUNTIME_MODE" "$expected_root" "$REST_PORT" "$JM_RPC_PORT" <<'PY'
import json, sys
p, mode, root, rest, jm = sys.argv[1:6]
d = json.loads(open(p).read())
checks = [
    ("mode", d.get("mode"), mode),
    ("runtime_root", d.get("runtime_root"), root),
    ("rest_port", str(d.get("rest_port")), rest),
    ("jm_rpc", str(d.get("jm_rpc")), jm),
]
for k, got, exp in checks:
    if str(got) != str(exp):
        raise SystemExit(f"FATAL: runtime-identity.json {k}={got} != expected {exp}")
PY
}

verify_rest_listener_owner() {
  local jm_pid=$1
  if ! ss -Hltn "sport = :${REST_PORT}" | grep -q .; then
    return 0
  fi
  local owner=""
  owner="$(ss -Hltnp "sport = :${REST_PORT}" 2>/dev/null | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2 || true)"
  if [ -z "$owner" ]; then
    echo "FATAL: REST ${REST_PORT} listening but cannot resolve owner pid (ss -p required)" >&2
    return 1
  fi
  if [ "$owner" != "$jm_pid" ]; then
    echo "FATAL: REST listener pid=$owner != verified standalonesession pid=$jm_pid" >&2
    return 1
  fi
}

verify_all() {
  verify_conf_ownership
  shopt -s nullglob
  local all_pids=("$FLINK_PID_DIR"/*.pid)
  if [ "${#all_pids[@]}" -eq 0 ]; then
    if [ "$REQUIRE_PID" = 1 ] || [ "$REQUIRE_REUSE" = 1 ]; then
      echo "FATAL: no pid files under $FLINK_PID_DIR (REQUIRE_PID_FILES)" >&2
      return 1
    fi
    echo "IDENTITY_OK $RUNTIME_IDENTITY (no pid files)"
    return 0
  fi

  local jm_pf="" tm_pf=""
  for pf in "${all_pids[@]}"; do
    if [[ "$pf" == *standalonesession* ]]; then
      jm_pf=$pf
    elif [[ "$pf" == *taskexecutor* ]]; then
      tm_pf=$pf
    else
      echo "FATAL: unexpected pid filename (want standalonesession/taskexecutor): $pf" >&2
      return 1
    fi
  done

  if [ "$REQUIRE_PID" = 1 ] || [ "$REQUIRE_REUSE" = 1 ]; then
    [ -n "$jm_pf" ] && [ -n "$tm_pf" ] || {
      echo "FATAL: require standalonesession + taskexecutor pid files (jm=$jm_pf tm=$tm_pf)" >&2
      return 1
    }
  fi

  local jm_pid="" tm_pid=""
  if [ -n "$jm_pf" ]; then
    jm_pid="$(verify_pid_file "$jm_pf" standalonesession)"
  fi
  if [ -n "$tm_pf" ]; then
    tm_pid="$(verify_pid_file "$tm_pf" taskexecutor)"
  fi

  if [ -n "$jm_pid" ] && ss -Hltn "sport = :${REST_PORT}" | grep -q .; then
    verify_rest_listener_owner "$jm_pid"
  fi

  echo "IDENTITY_OK $RUNTIME_IDENTITY jm_pid=${jm_pid:-none} tm_pid=${tm_pid:-none}"
}

verify_all

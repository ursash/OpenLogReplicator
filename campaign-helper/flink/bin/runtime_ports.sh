#!/usr/bin/env bash
# Port layout per FLINK_RUNTIME_MODE (campaign vs aux); avoids default JM RPC 6123 collisions.
set -euo pipefail

_lock_port() {
  local name=$1 expected=$2 actual=$3
  if [ "$actual" != "$expected" ]; then
    echo "FATAL: ${name} locked to ${expected} in ${FLINK_RUNTIME_MODE} mode (env override=${actual})" >&2
    return 1
  fi
}

apply_runtime_ports() {
  local mode=${FLINK_RUNTIME_MODE:?}
  local helper_root
  helper_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  local map="$helper_root/config/portmap.json"

  case "$mode" in
    campaign)
      CAMPAIGN_ROOT="${SHARED_RUNTIME_ROOT:-/opt/oracle-cdc-benchmark/flink-live-srl-20261012T031500Z}"
      if [ "${RUNTIME_ROOT}" != "${CAMPAIGN_ROOT}" ]; then
        echo "FATAL: campaign mode requires RUNTIME_ROOT=${CAMPAIGN_ROOT}" >&2
        return 1
      fi
      REST_PORT="${REST_PORT:-28120}"
      _lock_port REST_PORT 28120 "$REST_PORT"
      JM_RPC_PORT="${JM_RPC_PORT:-28121}"
      TM_RPC_PORT="${TM_RPC_PORT:-28122}"
      TM_DATA_PORT="${TM_DATA_PORT:-28123}"
      BLOB_PORT="${BLOB_PORT:-28124}"
      _lock_port JM_RPC_PORT 28121 "$JM_RPC_PORT"
      _lock_port TM_RPC_PORT 28122 "$TM_RPC_PORT"
      _lock_port TM_DATA_PORT 28123 "$TM_DATA_PORT"
      _lock_port BLOB_PORT 28124 "$BLOB_PORT"
      ;;
    aux)
      REST_PORT="${REST_PORT:-28140}"
      for p in 28120 28132 28133; do
        [ "$REST_PORT" = "$p" ] && {
          echo "FATAL: aux REST_PORT $p is MAIN/campaign reserved" >&2
          return 1
        }
      done
      if [ "$REST_PORT" -lt 28140 ] 2>/dev/null; then
        echo "FATAL: aux REST_PORT must be >= 28140 (got $REST_PORT)" >&2
        return 1
      fi
      local base=$REST_PORT
      JM_RPC_PORT="${JM_RPC_PORT:-$((base + 1))}"
      TM_RPC_PORT="${TM_RPC_PORT:-$((base + 2))}"
      TM_DATA_PORT="${TM_DATA_PORT:-$((base + 3))}"
      BLOB_PORT="${BLOB_PORT:-$((base + 4))}"
      _lock_port JM_RPC_PORT "$((base + 1))" "$JM_RPC_PORT"
      _lock_port TM_RPC_PORT "$((base + 2))" "$TM_RPC_PORT"
      _lock_port TM_DATA_PORT "$((base + 3))" "$TM_DATA_PORT"
      _lock_port BLOB_PORT "$((base + 4))" "$BLOB_PORT"
      ;;
    *)
      echo "FATAL: FLINK_RUNTIME_MODE must be campaign or aux (got $mode)" >&2
      return 1
      ;;
  esac

  export REST_PORT JM_RPC_PORT TM_RPC_PORT TM_DATA_PORT BLOB_PORT
  export RUNTIME_IDENTITY="${mode}:${RUNTIME_ROOT}:rest=${REST_PORT}:jm=${JM_RPC_PORT}"
}

#!/usr/bin/env bash
# Recovery lane guards — source from recovery_phase.sh only.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${RECOVERY_DEFAULTS:-$ROOT/config/recovery_lane.defaults.env}"

: "${RUN_ID:?RUN_ID}"
: "${EV:?EV evidence root}"
: "${PREP:?PREP connect prep root}"
: "${ATTEMPT:?ATTEMPT output dir}"

RECOVERY_EXEC=${RECOVERY_EXEC:-live}
RECOVERY_REST_PORT=${RECOVERY_REST_PORT:-28133}
RECOVERY_OLR_PORT=${RECOVERY_OLR_PORT:-39233}
CONN="${CONNECTOR_NAME:-${CONNECTOR_NAME_PREFIX}-${RUN_ID}-recovery}"
TOPIC_PREFIX="${TOPIC_PREFIX:-${TOPIC_PREFIX_TEMPLATE}.${RUN_ID}.recovery}"
OFFSET_TOPIC="${OFFSET_TOPIC:-${TOPIC_PREFIX}.connect-offsets}"
REST="${REST:-http://127.0.0.1:${RECOVERY_REST_PORT}}"
ST="${ST:-$EV/state/recovery}"
PHASE_OUT="${PHASE_OUT:-$ATTEMPT/phases}"

export RUN_ID EV PREP ATTEMPT CONN TOPIC_PREFIX OFFSET_TOPIC REST ST PHASE_OUT RECOVERY_EXEC

_ts() { date -u +%FT%T.%3NZ; }
log() { echo "$(_ts) $*" | tee -a "${ATTEMPT}/recovery-phase.log" >&2; }

_recovery_rest_port() {
  local u=$1
  if [[ "$u" =~ :([0-9]+)(/|$) ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "0"
  fi
}

assert_recovery_targets() {
  local rp
  rp=$(_recovery_rest_port "$REST")
  if [ "$rp" != "$RECOVERY_REST_PORT" ]; then
    echo "FATAL: REST port $rp != recovery-only $RECOVERY_REST_PORT" >&2
    exit 2
  fi
  case "$REST" in
    *":${BASELINE_REST_PORT}"*) echo "FATAL: baseline REST forbidden" >&2; exit 2 ;;
  esac
  if [ -n "${OLR_PORT:-}" ] && [ "$OLR_PORT" != "$RECOVERY_OLR_PORT" ]; then
    echo "FATAL: OLR_PORT must be recovery $RECOVERY_OLR_PORT" >&2
    exit 2
  fi
}

forbidden_global_kill_check() {
  if [ -n "${RECOVERY_ALLOW_KILL:-}" ] && [ "$RECOVERY_EXEC" = "live" ]; then
    return 0
  fi
  if [ "$RECOVERY_EXEC" = "mock" ]; then
    return 0
  fi
  :
}

mkdir -p "$PHASE_OUT" "$ST/rest" "$ST/logs" "$ST/dg" "$ST/classes"

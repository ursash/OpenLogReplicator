#!/usr/bin/env bash
set -euo pipefail
AUX_RUN_ID=${1:?}
BOOTSTRAP=${BOOTSTRAP:-10.129.80.147:9092}
REST_PORT=${REST_PORT:-28140}
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../bin/aux_run_id.sh
source "$ROOT/bin/aux_run_id.sh"

validate_aux_run_id "$AUX_RUN_ID"
RUNTIME_ROOT="$(resolve_aux_runtime_root "$AUX_RUN_ID")"
LANE_PREFIX="ocr370.live.srl.aux.${AUX_RUN_ID}"
EVID="$ROOT/evidence/aux-${AUX_RUN_ID}"
ARCHIVE_DIR="$ROOT/evidence/archives"

if [ -d "$EVID" ]; then
  mkdir -p "$ARCHIVE_DIR"
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  tar -czf "$ARCHIVE_DIR/${AUX_RUN_ID}-raw-${ts}.tar.gz" -C "$ROOT/evidence" "aux-${AUX_RUN_ID}"
  echo "archived raw evidence -> $ARCHIVE_DIR/${AUX_RUN_ID}-raw-${ts}.tar.gz"
fi

export FLINK_RUNTIME_MODE=aux
export RUNTIME_ROOT REST_PORT
# shellcheck source=../bin/flink_env.sh
source "$ROOT/bin/flink_env.sh"

if [ -d "$RUNTIME_ROOT/conf" ]; then
  REQUIRE_PID_FILES=1 "$ROOT/bin/verify_runtime_identity.sh"
  "$ROOT/bin/runtime_ctl.sh" stop
fi

for t in \
  "${LANE_PREFIX}.OLR_RAC_TEST.EV" \
  "${LANE_PREFIX}.OLR_RAC_TEST.PP" \
  "${LANE_PREFIX}.flink.sink.EV" \
  "${LANE_PREFIX}.flink.sink.PP"; do
  kafka-topics --bootstrap-server "$BOOTSTRAP" --delete --topic "$t" 2>/dev/null || {
    echo "FATAL: kafka topic delete failed for $t" >&2
    exit 1
  }
done

case "$RUNTIME_ROOT" in
  /tmp/flink-aux-smoke-*) rm -rf "$RUNTIME_ROOT" ;;
  *) echo "FATAL: refuse rm on $RUNTIME_ROOT" >&2; exit 1 ;;
esac
echo "cleaned aux ${AUX_RUN_ID}"

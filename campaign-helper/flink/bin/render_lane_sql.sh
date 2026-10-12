#!/usr/bin/env bash
# Render EV/PP SQL for one campaign lane (baseline|recovery) into OUT_DIR.
set -euo pipefail
LANE=${1:?lane baseline|recovery}
OUT_DIR=${2:?output dir}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=flink_env.sh
source "$SCRIPT_DIR/flink_env.sh"

RUN_ID="${RUN_ID:?}"
TOPIC_PREFIX="${TOPIC_PREFIX:?}"
TABLE_QUALIFIER="${TABLE_QUALIFIER:-OLR_RAC_TEST}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:?}"

case "$LANE" in
  baseline)
    CONSUMER_GROUP="live-srl-${RUN_ID}-baseline-flink"
    CHECKPOINT_DIR="file://${SHARED_RUNTIME_ROOT}/checkpoints/baseline"
    LANE_PREFIX="${TOPIC_PREFIX}.baseline"
    ;;
  recovery)
    CONSUMER_GROUP="live-srl-${RUN_ID}-recovery-flink"
    CHECKPOINT_DIR="file://${SHARED_RUNTIME_ROOT}/checkpoints/recovery"
    LANE_PREFIX="${TOPIC_PREFIX}.recovery"
    ;;
  *)
    echo "unknown lane: $LANE" >&2
    exit 1
    ;;
esac

SOURCE_TOPIC_EV="${LANE_PREFIX}.${TABLE_QUALIFIER}.EV"
SOURCE_TOPIC_PP="${LANE_PREFIX}.${TABLE_QUALIFIER}.PP"
SINK_TOPIC_EV="${LANE_PREFIX}.flink.sink.EV"
SINK_TOPIC_PP="${LANE_PREFIX}.flink.sink.PP"

mkdir -p "$OUT_DIR"
CONNECTOR_JAR_URI="file://${CONNECTOR_JAR:-$PRIVATE_LIB/$KAFKA_CONNECTOR_ARTIFACT}"
[ -f "${CONNECTOR_JAR:-}" ] || CONNECTOR_JAR_URI="file://${SHARED_RUNTIME_ROOT}/lib/${KAFKA_CONNECTOR_ARTIFACT}"

_render() {
  local tpl=$1 out=$2 job_name=$3 consumer_group=$4
  sed \
    -e "s|@CONNECTOR_JAR_URI@|${CONNECTOR_JAR_URI}|g" \
    -e "s|@JOB_NAME@|${job_name}|g" \
    -e "s|@CHECKPOINT_DIR@|${CHECKPOINT_DIR}|g" \
    -e "s|@CHECKPOINT_INTERVAL@|${CHECKPOINT_INTERVAL}|g" \
    -e "s|@BOOTSTRAP_SERVERS@|${BOOTSTRAP_SERVERS}|g" \
    -e "s|@SOURCE_TOPIC_EV@|${SOURCE_TOPIC_EV}|g" \
    -e "s|@SOURCE_TOPIC_PP@|${SOURCE_TOPIC_PP}|g" \
    -e "s|@SINK_TOPIC_EV@|${SINK_TOPIC_EV}|g" \
    -e "s|@SINK_TOPIC_PP@|${SINK_TOPIC_PP}|g" \
    -e "s|@CONSUMER_GROUP@|${consumer_group}|g" \
    -e "s|@SOURCE_SCAN_MODE@|${SOURCE_SCAN_MODE}|g" \
    -e "s|@SOURCE_ISOLATION_LEVEL@|${SOURCE_ISOLATION_LEVEL}|g" \
    < "$tpl" > "$out"
}

_render "$ROOT/sql/templates/ev_pipeline.sql.tpl" "$OUT_DIR/ev_job.sql" \
  "live-srl-${RUN_ID}-${LANE}-ev" "${CONSUMER_GROUP}-ev"
_render "$ROOT/sql/templates/pp_pipeline.sql.tpl" "$OUT_DIR/pp_job.sql" \
  "live-srl-${RUN_ID}-${LANE}-pp" "${CONSUMER_GROUP}-pp"

cat > "$OUT_DIR/lane.params.json" <<EOF
{
  "contract_version": "${CONTRACT_VERSION}",
  "lane": "${LANE}",
  "bootstrap_servers": "${BOOTSTRAP_SERVERS}",
  "consumer_group_ev": "${CONSUMER_GROUP}-ev",
  "consumer_group_pp": "${CONSUMER_GROUP}-pp",
  "checkpoint_dir": "${CHECKPOINT_DIR}",
  "source_topic_ev": "${SOURCE_TOPIC_EV}",
  "source_topic_pp": "${SOURCE_TOPIC_PP}",
  "sink_topic_ev": "${SINK_TOPIC_EV}",
  "sink_topic_pp": "${SINK_TOPIC_PP}",
  "source_isolation_level": "${SOURCE_ISOLATION_LEVEL}",
  "source_scan_mode": "${SOURCE_SCAN_MODE}"
}
EOF
echo "rendered lane ${LANE} -> ${OUT_DIR}"

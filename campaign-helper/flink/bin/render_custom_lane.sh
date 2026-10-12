#!/usr/bin/env bash
# Render EV/PP SQL for auxiliary or custom topic prefix (not campaign baseline/recovery names).
set -euo pipefail
OUT_DIR=${1:?}
LANE_PREFIX=${2:?topic prefix e.g. ocr370.live.srl.aux.foo}
CONSUMER_GROUP=${3:?}
CHECKPOINT_DIR=${4:?file:///tmp/...}
JOB_PREFIX=${5:-aux-flink}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=flink_env.sh
source "$SCRIPT_DIR/flink_env.sh"

TABLE_QUALIFIER="${TABLE_QUALIFIER:-OLR_RAC_TEST}"
SOURCE_TOPIC_EV="${LANE_PREFIX}.${TABLE_QUALIFIER}.EV"
SOURCE_TOPIC_PP="${LANE_PREFIX}.${TABLE_QUALIFIER}.PP"
SINK_TOPIC_EV="${LANE_PREFIX}.flink.sink.EV"
SINK_TOPIC_PP="${LANE_PREFIX}.flink.sink.PP"

mkdir -p "$OUT_DIR"
"$SCRIPT_DIR/ensure_connectors.sh" >/dev/null
CONNECTOR_JAR_URI="file://${CONNECTOR_JAR}"

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
  "${JOB_PREFIX}-ev" "${CONSUMER_GROUP}-ev"
_render "$ROOT/sql/templates/pp_pipeline.sql.tpl" "$OUT_DIR/pp_job.sql" \
  "${JOB_PREFIX}-pp" "${CONSUMER_GROUP}-pp"

cat > "$OUT_DIR/lane.params.json" <<EOF
{
  "lane_prefix": "${LANE_PREFIX}",
  "bootstrap_servers": "${BOOTSTRAP_SERVERS}",
  "consumer_group_ev": "${CONSUMER_GROUP}-ev",
  "consumer_group_pp": "${CONSUMER_GROUP}-pp",
  "checkpoint_dir": "${CHECKPOINT_DIR}",
  "source_topic_ev": "${SOURCE_TOPIC_EV}",
  "source_topic_pp": "${SOURCE_TOPIC_PP}",
  "sink_topic_ev": "${SINK_TOPIC_EV}",
  "sink_topic_pp": "${SINK_TOPIC_PP}",
  "source_isolation_level": "${SOURCE_ISOLATION_LEVEL}"
}
EOF

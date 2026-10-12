#!/usr/bin/env bash
set -euo pipefail
_campaign_helper_flink_dir() {
  local d
  d="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  printf '%s' "$d"
}
FLINK_HELPER_ROOT="$(_campaign_helper_flink_dir)"
# shellcheck source=/dev/null
[ -f "$FLINK_HELPER_ROOT/config/campaign.defaults.env" ] && source "$FLINK_HELPER_ROOT/config/campaign.defaults.env"

PARCEL="${FLINK_PARCEL:-$(ls -d /opt/cloudera/parcels/FLINK-* 2>/dev/null | sort -V | tail -1)}"
[ -n "$PARCEL" ] && [ -d "$PARCEL/lib/flink" ] || { echo "FATAL: CSA FLINK parcel not found under /opt/cloudera/parcels" >&2; exit 1; }
export FLINK_PARCEL_REF="$PARCEL"
export FLINK_PARCEL_BIN="$PARCEL/bin"
export HADOOP_HOME="${HADOOP_HOME:-/usr/lib/hadoop}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
export HBASE_CONF_DIR="${HBASE_CONF_DIR:-/etc/hbase/conf}"
if command -v hadoop >/dev/null 2>&1; then
  export HADOOP_CLASSPATH="${HADOOP_CLASSPATH:-$(hadoop classpath)}"
fi

RUNTIME_ROOT="${RUNTIME_ROOT:?set RUNTIME_ROOT (private Flink instance root)}"
# Never inherit foreign Flink dirs from shell/CSA — always bind to this RUNTIME_ROOT.
export FLINK_CONF_DIR="$RUNTIME_ROOT/conf"
export FLINK_LOG_DIR="$RUNTIME_ROOT/log"
export FLINK_PID_DIR="$RUNTIME_ROOT/pid"
PRIVATE_LIB="${PRIVATE_LIB:-$RUNTIME_ROOT/lib}"
CONNECTOR_JAR="${CONNECTOR_JAR:-$PRIVATE_LIB/$KAFKA_CONNECTOR_ARTIFACT}"

FLINK_RUNTIME_MODE="${FLINK_RUNTIME_MODE:-aux}"
# shellcheck source=runtime_ports.sh
source "$(dirname "${BASH_SOURCE[0]}")/runtime_ports.sh"
apply_runtime_ports

mkdir -p "$PRIVATE_LIB" "$FLINK_CONF_DIR" "$FLINK_LOG_DIR" "$FLINK_PID_DIR" "$RUNTIME_ROOT/checkpoints" "$RUNTIME_ROOT/savepoints"

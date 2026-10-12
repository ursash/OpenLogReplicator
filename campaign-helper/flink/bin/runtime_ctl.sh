#!/usr/bin/env bash
set -euo pipefail
ACTION=${1:?start|stop|status}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=flink_env.sh
source "$SCRIPT_DIR/flink_env.sh"
if [ "${FLINK_HELPER_SKIP_ENSURE:-}" != 1 ]; then
  # shellcheck source=ensure_sql_client.sh
  source "$SCRIPT_DIR/ensure_sql_client.sh"
  "$SCRIPT_DIR/ensure_connectors.sh" >/dev/null
fi

export FLINK_HOME="${FLINK_HOME_APACHE:-$PRIVATE_LIB/flink-1.20.5}"
[ -x "$FLINK_HOME/bin/jobmanager.sh" ] || { echo "FATAL: missing Apache FLINK_HOME at $FLINK_HOME" >&2; exit 1; }
if [ ! -f "$FLINK_HOME/lib/$(basename "$CONNECTOR_JAR")" ]; then
  cp -f "$CONNECTOR_JAR" "$FLINK_HOME/lib/"
fi
export PATH="$FLINK_HOME/bin:$PATH"

CLUSTER_CP="${RUNTIME_ROOT}/checkpoints/_cluster"
CLUSTER_SP="${RUNTIME_ROOT}/savepoints/_cluster"
mkdir -p "$CLUSTER_CP" "$CLUSTER_SP"

write_conf() {
  cat > "$FLINK_CONF_DIR/flink-conf.yaml" <<EOF
rest.port: ${REST_PORT}
rest.address: 127.0.0.1
jobmanager.rpc.port: ${JM_RPC_PORT}
jobmanager.rpc.address: 127.0.0.1
jobmanager.bind-host: 127.0.0.1
taskmanager.rpc.port: ${TM_RPC_PORT}
taskmanager.data.port: ${TM_DATA_PORT}
blob.server.port: ${BLOB_PORT}
taskmanager.bind-host: 127.0.0.1
taskmanager.host: 127.0.0.1
parallelism.default: 1
taskmanager.numberOfTaskSlots: 4
jobmanager.memory.process.size: 1600m
taskmanager.memory.process.size: 2048m
state.checkpoints.dir: file://${CLUSTER_CP}
state.savepoints.dir: file://${CLUSTER_SP}
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
pipeline.classpaths: file://${CONNECTOR_JAR}
classloader.parent-first-patterns.additional: org.apache.kafka.;org.apache.flink.connector.kafka.;org.apache.flink.streaming.connectors.kafka.
EOF
  ln -sf flink-conf.yaml "$FLINK_CONF_DIR/config.yaml"
  cat > "$FLINK_CONF_DIR/runtime-identity.json" <<EOF
{"mode":"${FLINK_RUNTIME_MODE}","runtime_root":"${RUNTIME_ROOT}","rest_port":${REST_PORT},"jm_rpc":${JM_RPC_PORT},"tm_rpc":${TM_RPC_PORT},"flink_home":"${FLINK_HOME}","identity":"${RUNTIME_IDENTITY}"}
EOF
}

start_runtime() {
  if ss -Hltn "sport = :${REST_PORT}" | grep -q .; then
    if REQUIRE_START_REUSE=1 REQUIRE_PID_FILES=1 "$SCRIPT_DIR/verify_runtime_identity.sh"; then
      echo "RUNTIME_REUSE mode=${FLINK_RUNTIME_MODE} rest=${REST_PORT} jm_rpc=${JM_RPC_PORT} root=${RUNTIME_ROOT}"
      return 0
    fi
    echo "FATAL: REST ${REST_PORT} in use but identity/config/PID do not match this RUNTIME_ROOT=${RUNTIME_ROOT}" >&2
    echo "  Refusing success-on-listener; will not stop foreign instance." >&2
    exit 1
  fi
  write_conf
  export FLINK_HOME FLINK_CONF_DIR FLINK_LOG_DIR FLINK_PID_DIR HADOOP_CLASSPATH HADOOP_CONF_DIR
  "$FLINK_HOME/bin/jobmanager.sh" start >>"$RUNTIME_ROOT/jobmanager.log" 2>&1
  "$FLINK_HOME/bin/taskmanager.sh" start >>"$RUNTIME_ROOT/taskmanager.log" 2>&1
  for _ in $(seq 1 45); do
    if curl -fsS "http://127.0.0.1:${REST_PORT}/overview" >/dev/null 2>&1; then
      echo "RUNTIME_LISTENING mode=${FLINK_RUNTIME_MODE} rest=${REST_PORT} jm_rpc=${JM_RPC_PORT} root=${RUNTIME_ROOT}"
      return 0
    fi
    sleep 2
  done
  echo "FATAL: REST not up on ${REST_PORT}" >&2
  tail -30 "$RUNTIME_ROOT/jobmanager.log" >&2 || true
  exit 1
}

stop_runtime() {
  REQUIRE_PID_FILES=1 REST_PORT="${REST_PORT:?}" "$SCRIPT_DIR/verify_runtime_identity.sh"
  export FLINK_HOME FLINK_CONF_DIR FLINK_LOG_DIR FLINK_PID_DIR
  if ! "$FLINK_HOME/bin/taskmanager.sh" stop >>"$RUNTIME_ROOT/taskmanager.log" 2>&1; then
    echo "FATAL: taskmanager stop failed for RUNTIME_ROOT=$RUNTIME_ROOT" >&2
    exit 1
  fi
  if ! "$FLINK_HOME/bin/jobmanager.sh" stop >>"$RUNTIME_ROOT/jobmanager.log" 2>&1; then
    echo "FATAL: jobmanager stop failed for RUNTIME_ROOT=$RUNTIME_ROOT" >&2
    exit 1
  fi
  echo "RUNTIME_STOPPED mode=${FLINK_RUNTIME_MODE} root=${RUNTIME_ROOT}"
}

status_runtime() {
  if ss -Hltn "sport = :${REST_PORT}" | grep -q .; then
    curl -fsS "http://127.0.0.1:${REST_PORT}/jobs" || true
  else
    echo '{"listening":false}'
  fi
}

case "$ACTION" in
  start) start_runtime ;;
  stop) stop_runtime ;;
  status) status_runtime ;;
  *) echo "usage: $0 start|stop|status" >&2; exit 1 ;;
esac

#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=flink_env.sh
source "$SCRIPT_DIR/flink_env.sh"

MAVEN_BASE="${MAVEN_BASE:-https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_COORD}}"
JAR_URL="${MAVEN_BASE}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_COORD}.jar"

if [ ! -f "$CONNECTOR_JAR" ]; then
  echo "fetch $JAR_URL -> $CONNECTOR_JAR"
  curl -fsSL "$JAR_URL" -o "$CONNECTOR_JAR.part"
  mv "$CONNECTOR_JAR.part" "$CONNECTOR_JAR"
fi
sha256sum "$CONNECTOR_JAR" | tee "$CONNECTOR_JAR.sha256"
ls -la "$CONNECTOR_JAR"

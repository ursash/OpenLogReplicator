#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP=${1:?}
TOPIC=${2:?}
OUT=${3:?}
TIMEOUT_MS=${4:-8000}
# Collect compacted upsert topic from beginning; includes tombstones (null value).
kafka-console-consumer \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --from-beginning \
  --timeout-ms "$TIMEOUT_MS" \
  --property print.key=true \
  --property print.value=true \
  --property key.separator=$'\t' \
  >"$OUT" 2>/dev/null || true
echo "wrote $(wc -l <"$OUT") lines -> $OUT"

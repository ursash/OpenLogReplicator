#!/usr/bin/env bash
set -euo pipefail
SQL_DIR=${1:?directory with ev_job.sql and pp_job.sql}
OUT_IDS=${2:-}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=flink_env.sh
source "$SCRIPT_DIR/flink_env.sh"
# shellcheck source=ensure_sql_client.sh
source "$SCRIPT_DIR/ensure_sql_client.sh"
REST_PORT="${REST_PORT:?}"

[ -f "$SQL_DIR/ev_job.sql" ] && [ -f "$SQL_DIR/pp_job.sql" ] || { echo "missing SQL in $SQL_DIR" >&2; exit 1; }

pipeline_name() {
  grep -E "^SET 'pipeline.name'" "$1" | head -1 | sed "s/.*= '\([^']*\)'.*/\1/"
}
checkpoint_dir_from_sql() {
  grep -E "^SET 'state.checkpoints.dir'" "$1" | head -1 | sed "s/.*= '\([^']*\)'.*/\1/"
}

EV_NAME="$(pipeline_name "$SQL_DIR/ev_job.sql")"
PP_NAME="$(pipeline_name "$SQL_DIR/pp_job.sql")"
EV_CP="$(checkpoint_dir_from_sql "$SQL_DIR/ev_job.sql")"
PP_CP="$(checkpoint_dir_from_sql "$SQL_DIR/pp_job.sql")"

ATTEMPT_ID="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUNTIME_ROOT/submit-logs"
JOBS_BEFORE="$RUNTIME_ROOT/submit-logs/jobs_before_${ATTEMPT_ID}.json"
EV_LOG="$RUNTIME_ROOT/submit-logs/ev_submit_${ATTEMPT_ID}.log"
PP_LOG="$RUNTIME_ROOT/submit-logs/pp_submit_${ATTEMPT_ID}.log"
curl -fsS "http://127.0.0.1:${REST_PORT}/jobs" >"$JOBS_BEFORE"

submit_one() {
  local sql_file=$1
  local log_file=$2
  echo "submit $(basename "$sql_file") attempt=${ATTEMPT_ID}"
  {
    echo "=== SUBMIT_ATTEMPT_BEGIN ==="
    echo "attempt_id=${ATTEMPT_ID}"
  } >"$log_file"
  "$SQL_CLIENT" -j "$CONNECTOR_JAR" -f "$sql_file" >>"$log_file" 2>&1
}

submit_one "$SQL_DIR/ev_job.sql" "$EV_LOG"
submit_one "$SQL_DIR/pp_job.sql" "$PP_LOG"

IDS_FILE="${OUT_IDS:-$RUNTIME_ROOT/submit-logs/job_ids_${ATTEMPT_ID}.json}"
python3 "$SCRIPT_DIR/submit_bind.py" \
  "$REST_PORT" "$EV_NAME" "$PP_NAME" "$EV_CP" "$PP_CP" "$IDS_FILE" \
  "$EV_LOG" "$PP_LOG" "$JOBS_BEFORE"

curl -fsS "http://127.0.0.1:${REST_PORT}/jobs" | tee "$RUNTIME_ROOT/submit-logs/jobs_after_${ATTEMPT_ID}.json"

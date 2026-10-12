#!/usr/bin/env bash
# Targeted re-verify on EXISTING aux runtime: requires submit-bound job_ids.json (no name rescan).
set -euo pipefail
EVID=${1:?evidence dir}
REST_PORT=${2:?}
RUNTIME_ROOT=${3:?}
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

[ -f "$EVID/sql/ev_job.sql" ] || { echo "missing SQL" >&2; exit 1; }
[ -f "$EVID/job_ids.json" ] || {
  echo "FATAL: missing job_ids.json; run submit_lane_jobs.sh (receipt binding) first" >&2
  exit 1
}
python3 - "$EVID/job_ids.json" <<'PY'
import json, sys
p = json.loads(open(sys.argv[1]).read())
if p.get("binding") not in ("submit_receipt_and_diff", "submit_receipt_sql_client_job_id_line"):
    raise SystemExit("FATAL: job_ids.json not submit-receipt bound; refuse name-scan verify")
jobs = p.get("jobs") or []
if len(jobs) != 2:
    raise SystemExit(f"FATAL: expected 2 lane jobs, got {len(jobs)}")
PY

export FLINK_RUNTIME_MODE=aux RUNTIME_ROOT REST_PORT
"$ROOT/bin/wait_job_ready.sh" "$REST_PORT" "$EVID/job_ids.json" 120 | tee "$EVID/wait_ready_fix.log"
python3 "$ROOT/aux/validate_sink_state.py" "$EVID" | tee "$EVID/validate_fix.out"
"$ROOT/bin/seal_evidence.sh" "$EVID" "$RUNTIME_ROOT" "$REST_PORT" "$EVID/job_ids.json"

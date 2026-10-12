#!/usr/bin/env bash
# Wait for exact job IDs from submit job_ids.json; verify REST completed checkpoints per job.
set -euo pipefail
REST_PORT=${1:?}
JOB_IDS_JSON=${2:?path to job_ids.json}
MAX_WAIT=${3:-240}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/wait_ready_probe.py" "$REST_PORT" "$JOB_IDS_JSON" "$MAX_WAIT"

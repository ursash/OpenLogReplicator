#!/usr/bin/env bash
set -euo pipefail
AUX_RUN_ID=${AUX_RUN_ID:-aux20261012T042200Z}
REST_PORT=${REST_PORT:-28140}
BOOTSTRAP=${BOOTSTRAP:-10.129.80.147:9092}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=../bin/aux_run_id.sh
source "$ROOT/bin/aux_run_id.sh"
validate_aux_run_id "$AUX_RUN_ID"

export FLINK_RUNTIME_MODE=aux
export RUNTIME_ROOT="$(resolve_aux_runtime_root "$AUX_RUN_ID")"
export REST_PORT
export BOOTSTRAP_SERVERS="$BOOTSTRAP"

LANE_PREFIX="ocr370.live.srl.aux.${AUX_RUN_ID}"
CONSUMER_GROUP="live-srl-aux-${AUX_RUN_ID}-flink"
CHECKPOINT_DIR="file://${RUNTIME_ROOT}/checkpoints/lane"
EVID="$ROOT/evidence/aux-${AUX_RUN_ID}"
mkdir -p "$EVID"

ss -Hltn "sport = :${REST_PORT}" | grep -q . && { echo "port ${REST_PORT} busy" >&2; exit 1; }

chmod +x "$ROOT"/bin/*.sh "$SCRIPT_DIR"/*.py 2>/dev/null || true

TOPICS=(
  "${LANE_PREFIX}.OLR_RAC_TEST.EV"
  "${LANE_PREFIX}.OLR_RAC_TEST.PP"
  "${LANE_PREFIX}.flink.sink.EV"
  "${LANE_PREFIX}.flink.sink.PP"
)
for t in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$t" \
    --partitions 1 --replication-factor 1 --config cleanup.policy=compact
done

"$ROOT/bin/render_custom_lane.sh" "$EVID/sql" "$LANE_PREFIX" "$CONSUMER_GROUP" "$CHECKPOINT_DIR" "aux-${AUX_RUN_ID}"

python3 "$SCRIPT_DIR/synthetic_debezium_producer.py" \
  --bootstrap "$BOOTSTRAP" \
  --topic-ev "${LANE_PREFIX}.OLR_RAC_TEST.EV" \
  --topic-pp "${LANE_PREFIX}.OLR_RAC_TEST.PP" | tee "$EVID/input_expectations.json"

export FLINK_RUNTIME_MODE=aux RUNTIME_ROOT REST_PORT
"$ROOT/bin/runtime_ctl.sh" start | tee "$EVID/runtime_start.log"

PARCEL="${FLINK_PARCEL_REF:-$(ls -d /opt/cloudera/parcels/FLINK-* 2>/dev/null | tail -1)}"
APACHE_HOME="$RUNTIME_ROOT/lib/flink-1.20.5"
python3 - "$EVID/RUNTIME-PIN.json" "$RUNTIME_ROOT/conf/runtime-identity.json" "$PARCEL" "$APACHE_HOME" "$BOOTSTRAP" <<'PY'
import json, pathlib, sys
out, ident_path, parcel, apache, boot = sys.argv[1:6]
ident = json.loads(pathlib.Path(ident_path).read_text()) if pathlib.Path(ident_path).is_file() else {}
pin = {
  "aux_run_id": ident.get("runtime_root", "").split("flink-aux-smoke-")[-1] or None,
  "csa_flink_parcel_ref": parcel,
  "private_runtime_flink_home": apache,
  "flink_runtime_mode": ident.get("mode", "aux"),
  "rest_port": ident.get("rest_port"),
  "jm_rpc_port": ident.get("jm_rpc"),
  "tm_rpc_port": ident.get("tm_rpc"),
  "runtime_root": ident.get("runtime_root"),
  "bootstrap_servers": boot,
  "source_isolation_level": "read_committed",
}
pathlib.Path(out).write_text(json.dumps(pin, indent=2))
print(json.dumps(pin, indent=2))
PY

"$ROOT/bin/submit_lane_jobs.sh" "$EVID/sql" "$EVID/job_ids.json" | tee "$EVID/submit.log"
"$ROOT/bin/wait_job_ready.sh" "$REST_PORT" "$EVID/job_ids.json" 240 | tee "$EVID/wait_ready.log"

sleep 45
"$ROOT/bin/dump_upsert_sink.sh" "$BOOTSTRAP" "${LANE_PREFIX}.flink.sink.EV" "$EVID/sink_ev.raw.tsv" 12000
"$ROOT/bin/dump_upsert_sink.sh" "$BOOTSTRAP" "${LANE_PREFIX}.flink.sink.PP" "$EVID/sink_pp.raw.tsv" 12000

python3 "$SCRIPT_DIR/validate_sink_state.py" "$EVID" | tee "$EVID/validate.out"
"$ROOT/bin/seal_evidence.sh" "$EVID" "$RUNTIME_ROOT" "$REST_PORT" "$EVID/job_ids.json"
echo "AUX_SMOKE_DONE evid=$EVID"

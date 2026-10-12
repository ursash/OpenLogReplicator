#!/usr/bin/env bash
# Seal config + checkpoint readback + raw sinks into EVIDENCE-SEAL.sha256 (per aux run).
set -euo pipefail
EVID=${1:?evidence dir}
RUNTIME_ROOT=${2:?}
REST_PORT=${3:?}
JOB_IDS=${4:-$RUNTIME_ROOT/submit-logs/job_ids.json}

HELPER_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVID="$(readlink -f "$EVID")"
RUNTIME_ROOT="$(readlink -f "$RUNTIME_ROOT")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python3 - "$HELPER_ROOT" "$EVID" "$RUNTIME_ROOT" "$REST_PORT" "$JOB_IDS" "$SCRIPT_DIR" <<'PY'
import hashlib, json, pathlib, shutil, sys, urllib.request

helper_root = pathlib.Path(sys.argv[1])
evid = pathlib.Path(sys.argv[2])
runtime_root = pathlib.Path(sys.argv[3])
port = sys.argv[4]
job_ids_path = pathlib.Path(sys.argv[5])
script_dir = pathlib.Path(sys.argv[6])

sys.path.insert(0, str(script_dir))
import seal_evidence_core as sec  # noqa: E402

def incomplete(reasons: list[str]) -> None:
    status = {"status": "INCOMPLETE", "missing_or_invalid": reasons}
    (evid / "SEAL-STATUS.json").write_text(json.dumps(status, indent=2))
    raise SystemExit(f"FATAL: seal incomplete: {reasons}")

def sha(p: pathlib.Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def rel_hash(p: pathlib.Path) -> str:
    rel = p.relative_to(helper_root).as_posix()
    return f"{sha(p)}  {rel}"

required = [
    "input_expectations.json",
    "sink_ev.raw.tsv",
    "sink_pp.raw.tsv",
    "SMOKE-OUTCOME.json",
    "RUNTIME-PIN.json",
    "job_ids.json",
    "wait_ready_report.json",
]
for rel in required:
    if not (evid / rel).is_file():
        incomplete([rel])

cap = evid / "runtime_capture"
cap.mkdir(parents=True, exist_ok=True)
conf_src = runtime_root / "conf/flink-conf.yaml"
ident_src = runtime_root / "conf/runtime-identity.json"
if not conf_src.is_file() or not ident_src.is_file():
    incomplete(["runtime conf missing"])
shutil.copy2(conf_src, cap / "flink-conf.yaml")
shutil.copy2(ident_src, cap / "runtime-identity.json")

flink_conf = conf_src.read_text()
identity = json.loads(ident_src.read_text())
pin = json.loads((evid / "RUNTIME-PIN.json").read_text())
job_spec = json.loads((evid / "job_ids.json").read_text())

if not job_ids_path.is_file():
    incomplete(["job_ids path missing"])

cp_by_job = {}
for j in job_spec.get("jobs", []):
    jid = j["job_id"]
    cps = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/jobs/{jid}/checkpoints", timeout=15).read())
    cp_by_job[jid] = cps

try:
    sec.assess_seal_inputs(
        flink_conf=flink_conf,
        identity=identity,
        pin=pin,
        job_spec=job_spec,
        checkpoint_by_job=cp_by_job,
        runtime_root=str(runtime_root),
    )
except ValueError as e:
    payload = json.loads(str(e))
    incomplete(payload.get("missing_or_invalid", [str(e)]))

runtime_doc = {
    "actual_apache_flink_home": identity.get("flink_home"),
    "planned_private_apache_home": str(runtime_root / "lib/flink-1.20.5"),
    "csa_flink_parcel_ref": pin.get("csa_flink_parcel_ref"),
    "runtime_root": str(runtime_root),
    "rest_port": identity.get("rest_port"),
    "jm_rpc_port": identity.get("jm_rpc"),
    "tm_rpc_port": identity.get("tm_rpc"),
    "mode": identity.get("mode"),
}
(evid / "runtime.json").write_text(json.dumps(runtime_doc, indent=2))

cp_readback = {"jobs": [{"job_id": jid, "checkpoints": cp_by_job[jid]} for jid in cp_by_job]}
rb = evid / "checkpoint_readback.json"
rb.write_text(json.dumps(cp_readback, indent=2))

rows = []
for rel in required + ["runtime.json", "runtime_capture/flink-conf.yaml", "runtime_capture/runtime-identity.json", "checkpoint_readback.json"]:
    rows.append(rel_hash(evid / rel))

out = evid / "EVIDENCE-SEAL.sha256"
out.write_text("\n".join(rows) + "\n")
(evid / "SEAL-STATUS.json").write_text(json.dumps({"status": "COMPLETE"}, indent=2))
print(out.read_text())
PY

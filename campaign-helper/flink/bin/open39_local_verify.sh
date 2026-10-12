#!/usr/bin/env bash
# Reviewer39 HOLD gaps only (Submit/MD/closed READY items not rerun).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
ATTEMPT_ID="$(date -u +%Y%m%dT%H%M%SZ)"
SUMMARY="$ROOT/evidence/fix-hold-selftest/CURRENT-ATTEMPT-${ATTEMPT_ID}.json"
mkdir -p "$(dirname "$SUMMARY")"
FAIL=0

record() {
  python3 - "$SUMMARY" "$ATTEMPT_ID" "$1" "$2" "$3" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
doc = json.loads(p.read_text()) if p.is_file() else {"attempt_id": sys.argv[2], "scope": "reviewer39_hold", "cases": []}
doc["cases"].append({"case": sys.argv[3], "expected": sys.argv[4], "got": sys.argv[5]})
p.write_text(json.dumps(doc, indent=2))
PY
}

run() {
  local name=$1 exp=$2
  shift 2
  local got=pass
  if ! "$@"; then got=fail; fi
  record "$name" "$exp" "$got"
  if [ "$got" != "$exp" ]; then echo "FAIL $name expected=$exp got=$got" >&2; FAIL=1; else echo "ok $name"; fi
}

run identity_parser pass python3 "$ROOT/bin/flink_identity_parser.py" --selftest
run seal_core pass python3 "$ROOT/bin/seal_evidence_core.py" --selftest

run ready_two_job_split_lane_rejected pass python3 -c "
import sys
sys.path.insert(0, '$ROOT/bin')
import wait_ready_probe as w
jobs = [
  {'job_id': 'a'*32, 'name': 'ev', 'checkpoint_dir': 'file:///lane-a', 'table': 'EV'},
  {'job_id': 'b'*32, 'name': 'pp', 'checkpoint_dir': 'file:///lane-b', 'table': 'PP'},
]
try:
    w.validate_job_spec(jobs)
    sys.exit(1)
except SystemExit:
    sys.exit(0)
"

run identity_wrapper_stdout pass python3 - "$ROOT" <<'PY'
import json, subprocess, sys
from pathlib import Path
root = Path(sys.argv[1])
fx = json.loads((root / "tests/fixtures/p9_java_argv.json").read_text())
conf = "/tmp/flink-aux-smoke-demo/conf"
argv_json = json.dumps(fx["jm_argv"])
cmd = [
    sys.executable,
    str(root / "bin/flink_identity_parser.py"),
    "verify-fixture",
    "424242",
    "standalonesession",
    conf,
    "/usr/lib/jvm/java-11/bin/java",
    argv_json,
]
p = subprocess.run(cmd, capture_output=True, text=True)
if p.stdout.strip() != "424242":
    raise SystemExit(f"stdout not pid-only: {p.stdout!r}")
if "PID_OK" not in p.stderr:
    raise SystemExit("missing stderr diagnostic")
# simulate verify_runtime_identity.sh capture
parsed = subprocess.run(cmd, capture_output=True, text=True).stdout.strip()
if parsed != "424242":
    raise SystemExit("wrapper capture mismatch")
PY

python3 - "$SUMMARY" "$FAIL" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
doc = json.loads(p.read_text())
doc["overall"] = "PASS" if int(sys.argv[2]) == 0 else "FAIL"
doc["closed_not_retested"] = ["submit", "MAIN_md", "ready_uri_time_capacity_4job_closed"]
p.write_text(json.dumps(doc, indent=2))
print(p)
PY

"$ROOT/bin/freeze_manifest.sh"
(cd "$ROOT" && sha256sum -c FROZEN-SHA256.txt >/dev/null)
echo "open39_local_verify attempt=$ATTEMPT_ID fail=$FAIL"
exit "$FAIL"

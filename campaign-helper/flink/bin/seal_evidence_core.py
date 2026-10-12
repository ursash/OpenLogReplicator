#!/usr/bin/env python3
"""Seal preflight: cross-field conf/identity/pin + strict checkpoint path semantics."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

_BIN = Path(__file__).resolve().parent
if str(_BIN) not in sys.path:
    sys.path.insert(0, str(_BIN))
import wait_ready_probe as wr  # noqa: E402


def _fail(reasons: list[str]) -> None:
    raise ValueError(json.dumps({"status": "INCOMPLETE", "missing_or_invalid": reasons}))


def _require_int(name: str, v: Any) -> int:
    if v is None:
        _fail([f"{name} missing"])
    if isinstance(v, bool):
        _fail([f"{name} bool not allowed"])
    if not isinstance(v, int):
        _fail([f"{name} must be int"])
    return v


def _parse_conf(text: str) -> dict[str, str]:
    lines: dict[str, str] = {}
    for ln in text.splitlines():
        if ":" not in ln or ln.strip().startswith("#"):
            continue
        k, v = ln.split(":", 1)
        lines[k.strip()] = v.strip()
    return lines


def validate_pin(pin: dict) -> tuple[int, int, int, int]:
    for key in (
        "private_runtime_flink_home",
        "csa_flink_parcel_ref",
        "rest_port",
        "jm_rpc_port",
        "tm_rpc_port",
        "runtime_root",
        "taskmanager_slots",
    ):
        if pin.get(key) in (None, ""):
            _fail([f"RUNTIME-PIN.json missing {key}"])
    rest = _require_int("pin.rest_port", pin.get("rest_port"))
    jm = _require_int("pin.jm_rpc_port", pin.get("jm_rpc_port"))
    tm = _require_int("pin.tm_rpc_port", pin.get("tm_rpc_port"))
    slots = _require_int("pin.taskmanager_slots", pin.get("taskmanager_slots"))
    return rest, jm, tm, slots


def validate_identity_json(ident: dict, *, runtime_root: str, pin: dict, rest: int, jm: int, tm: int) -> None:
    for key in ("mode", "runtime_root", "rest_port", "jm_rpc", "tm_rpc", "flink_home"):
        if ident.get(key) in (None, ""):
            _fail([f"runtime-identity.json missing {key}"])
    _require_int("identity.rest_port", ident.get("rest_port"))
    _require_int("identity.jm_rpc", ident.get("jm_rpc"))
    _require_int("identity.tm_rpc", ident.get("tm_rpc"))
    if str(ident.get("runtime_root")) != str(runtime_root):
        _fail(["runtime-identity.json runtime_root mismatch"])
    if str(ident.get("runtime_root")) != str(pin.get("runtime_root")):
        _fail(["identity.runtime_root != pin.runtime_root"])
    if str(ident.get("rest_port")) != str(rest):
        _fail(["identity rest_port != pin"])
    if str(ident.get("jm_rpc")) != str(jm):
        _fail(["identity jm_rpc != pin"])
    if str(ident.get("tm_rpc")) != str(tm):
        _fail(["identity tm_rpc != pin"])
    if str(ident.get("flink_home")) != str(pin.get("private_runtime_flink_home")):
        _fail(["identity.flink_home != pin.private_runtime_flink_home"])


def validate_flink_conf_text(
    text: str,
    *,
    rest: int,
    jm: int,
    tm: int,
    slots: int,
    runtime_root: str,
) -> None:
    if not text.strip():
        _fail(["flink-conf.yaml empty"])
    lines = _parse_conf(text)
    for req in ("rest.port", "jobmanager.rpc.port", "taskmanager.rpc.port", "taskmanager.numberOfTaskSlots"):
        if req not in lines or lines[req] == "":
            _fail([f"flink-conf.yaml missing {req}"])
    if lines.get("rest.port") != str(rest):
        _fail(["flink-conf.yaml rest.port mismatch"])
    if lines.get("jobmanager.rpc.port") != str(jm):
        _fail(["flink-conf.yaml jobmanager.rpc.port mismatch"])
    if lines.get("taskmanager.rpc.port") != str(tm):
        _fail(["flink-conf.yaml taskmanager.rpc.port mismatch"])
    try:
        conf_slots = int(lines["taskmanager.numberOfTaskSlots"])
    except ValueError:
        _fail(["flink-conf.yaml taskmanager.numberOfTaskSlots not int"])
    if isinstance(conf_slots, bool):
        _fail(["flink-conf.yaml taskmanager.numberOfTaskSlots bool"])
    if conf_slots != slots:
        _fail(["flink-conf.yaml slots != pin.taskmanager_slots"])
    cp = lines.get("state.checkpoints.dir", "")
    if runtime_root not in text and wr.normalize_checkpoint_uri(cp).find(runtime_root.lstrip("/")) < 0:
        _fail(["flink-conf.yaml checkpoint dir not under runtime_root"])


def validate_checkpoint_payload(cps: dict, job: dict) -> None:
    jid = job["job_id"]
    counts = cps.get("counts") or {}
    completed = counts.get("completed", 0)
    if not isinstance(completed, int) or isinstance(completed, bool) or completed < 1:
        _fail([f"job {jid} checkpoints.counts.completed invalid"])
    latest = cps.get("latest") or {}
    comp = latest.get("completed")
    if comp is None:
        _fail([f"job {jid} latest.completed null"])
    if comp.get("status") != "COMPLETED":
        _fail([f"job {jid} latest.completed.status not COMPLETED"])
    ext = comp.get("external_path") or comp.get("storage_path")
    if not ext or not str(ext).strip():
        _fail([f"job {jid} missing external_path"])
    ok, reason = wr.checkpoint_under_lane(jid, job["checkpoint_dir"], str(ext))
    if not ok:
        _fail([f"job {jid} checkpoint path: {reason}"])


def validate_job_spec_for_seal(spec: dict) -> list[dict]:
    jobs = spec.get("jobs")
    if not jobs:
        _fail(["job_ids.json jobs empty"])
    if not spec.get("binding"):
        _fail(["job_ids.json missing binding"])
    out = []
    for j in jobs:
        if not j.get("job_id") or not j.get("name") or not j.get("checkpoint_dir"):
            _fail(["job_ids job missing job_id/name/checkpoint_dir"])
        out.append(j)
    return out


def assess_seal_inputs(
    *,
    flink_conf: str,
    identity: dict,
    pin: dict,
    job_spec: dict,
    checkpoint_by_job: dict[str, dict],
    runtime_root: str,
) -> None:
    if str(pin.get("runtime_root")) != str(runtime_root):
        _fail(["pin.runtime_root != runtime_root"])
    rest, jm, tm, slots = validate_pin(pin)
    validate_identity_json(identity, runtime_root=runtime_root, pin=pin, rest=rest, jm=jm, tm=tm)
    validate_flink_conf_text(flink_conf, rest=rest, jm=jm, tm=tm, slots=slots, runtime_root=runtime_root)
    jobs = validate_job_spec_for_seal(job_spec)
    expected_ids = {j["job_id"] for j in jobs}
    if set(checkpoint_by_job) != expected_ids:
        _fail(["checkpoint readback job_id set mismatch"])
    for j in jobs:
        validate_checkpoint_payload(checkpoint_by_job[j["job_id"]], j)


def _valid_fixture() -> dict:
    lane = "/lane"
    a, b = "a" * 32, "b" * 32
    return {
        "flink_conf": (
            "rest.port: 28140\n"
            "jobmanager.rpc.port: 28141\n"
            "taskmanager.rpc.port: 28142\n"
            "taskmanager.numberOfTaskSlots: 4\n"
            "state.checkpoints.dir: file:///tmp/r/cp\n"
        ),
        "identity": {
            "mode": "aux",
            "runtime_root": "/tmp/r",
            "rest_port": 28140,
            "jm_rpc": 28141,
            "tm_rpc": 28142,
            "flink_home": "/tmp/r/lib/flink",
        },
        "pin": {
            "private_runtime_flink_home": "/tmp/r/lib/flink",
            "csa_flink_parcel_ref": "/opt/flink",
            "rest_port": 28140,
            "jm_rpc_port": 28141,
            "tm_rpc_port": 28142,
            "taskmanager_slots": 4,
            "runtime_root": "/tmp/r",
        },
        "job_spec": {
            "binding": "x",
            "jobs": [
                {"job_id": a, "name": "ev", "checkpoint_dir": f"file://{lane}", "table": "EV"},
                {"job_id": b, "name": "pp", "checkpoint_dir": f"file://{lane}", "table": "PP"},
            ],
        },
        "checkpoint_by_job": {
            a: {
                "counts": {"completed": 1},
                "latest": {"completed": {"status": "COMPLETED", "external_path": f"file://{lane}/{a}/chk"}},
            },
            b: {
                "counts": {"completed": 1},
                "latest": {"completed": {"status": "COMPLETED", "external_path": f"file://{lane}/{b}/chk"}},
            },
        },
        "runtime_root": "/tmp/r",
    }


def run_mutation_selftest() -> int:
    valid = _valid_fixture()

    def run(**kw):
        assess_seal_inputs(**kw)

    results = []
    try:
        run(**valid)
        results.append({"case": "valid_fixture", "expected": "pass", "got": "pass"})
    except ValueError:
        results.append({"case": "valid_fixture", "expected": "pass", "got": "fail"})

    a = "a" * 32
    mutations = [
        ("pin_runtime_root_other", lambda v: {**v, "pin": {**v["pin"], "runtime_root": "/other"}}),
        ("pin_tm_rpc_wrong", lambda v: {**v, "pin": {**v["pin"], "tm_rpc_port": 29999}}),
        ("identity_flink_home_other", lambda v: {**v, "identity": {**v["identity"], "flink_home": "/other"}}),
        ("conf_missing_taskmanager_slots", lambda v: {**v, "flink_conf": "rest.port: 28140\njobmanager.rpc.port: 28141\n"}),
        (
            "conf_bool_taskmanager_slots",
            lambda v: {
                **v,
                "flink_conf": v["flink_conf"].replace(
                    "taskmanager.numberOfTaskSlots: 4", "taskmanager.numberOfTaskSlots: true"
                ),
            },
        ),
        ("pin_missing_taskmanager_slots", lambda v: {**v, "pin": {k: val for k, val in v["pin"].items() if k != "taskmanager_slots"}}),
        (
            "checkpoint_external_wrong_lane",
            lambda v: {
                **v,
                "checkpoint_by_job": {
                    **v["checkpoint_by_job"],
                    a: {
                        "counts": {"completed": 1},
                        "latest": {
                            "completed": {
                                "status": "COMPLETED",
                                "external_path": f"file:///other/cp/{a}/chk",
                            }
                        },
                    },
                },
            },
        ),
        (
            "checkpoint_substring_only",
            lambda v: {
                **v,
                "checkpoint_by_job": {
                    **v["checkpoint_by_job"],
                    a: {
                        "counts": {"completed": 1},
                        "latest": {
                            "completed": {"status": "COMPLETED", "external_path": f"/x{a}/chk"},
                        },
                    },
                },
            },
        ),
    ]
    for name, mut in mutations:
        try:
            run(**mut(valid))
            got = "pass"
        except ValueError:
            got = "fail"
        results.append({"case": name, "expected": "fail", "got": got})

    ok = results[0]["got"] == "pass" and all(r["got"] == "fail" for r in results[1:])
    print(json.dumps({"seal_evidence_core_selftest": ok, "results": results}, indent=2))
    return 0 if ok else 1


def main() -> None:
    if len(sys.argv) >= 2 and sys.argv[1] == "--selftest":
        raise SystemExit(run_mutation_selftest())
    raise SystemExit("usage: seal_evidence_core.py --selftest")


if __name__ == "__main__":
    main()

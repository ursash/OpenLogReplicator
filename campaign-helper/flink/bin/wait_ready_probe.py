#!/usr/bin/env python3
"""Shared wait_job_ready validation (used by wait_job_ready.sh and selftests)."""
from __future__ import annotations

import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path, PurePosixPath


def normalize_checkpoint_uri(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("file:"):
        tail = u[5:]
        if tail.startswith("//"):
            # file:///abs/path and file://host/path (host-less //tmp/... → /tmp/...)
            path = tail[2:] if not tail.startswith("///") else tail[1:]
            path = "/" + path.lstrip("/") if not path.startswith("/") else path
            if tail.startswith("///"):
                path = urllib.parse.unquote(urllib.parse.urlparse(u).path or "")
            else:
                path = urllib.parse.unquote("/" + tail.lstrip("/"))
        else:
            path = tail if tail.startswith("/") else "/" + tail
            path = urllib.parse.unquote(path)
        return str(PurePosixPath(path))
    return str(PurePosixPath(urllib.parse.unquote(u)))


def checkpoint_under_lane(jid: str, cp_dir: str, ext: str) -> tuple[bool, str]:
    if not ext or not ext.strip():
        return False, "empty checkpoint external_path"
    ne = normalize_checkpoint_uri(ext)
    nd = normalize_checkpoint_uri(cp_dir)
    if not nd:
        return False, "empty expected checkpoint_dir"
    if jid not in ne:
        return False, "job_id not in checkpoint path"
    if not (ne == nd or ne.startswith(nd + "/")):
        return False, "checkpoint path outside lane root"
    parts = ne.split("/")
    if jid not in parts:
        return False, "job_id not a path segment under lane root"
    return True, "ok"


def require_submit_ms(spec: dict) -> int:
    v = spec.get("submitted_at_epoch_ms")
    if v is None:
        raise SystemExit("FATAL: submitted_at_epoch_ms mandatory")
    if isinstance(v, bool) or not isinstance(v, int):
        raise SystemExit("FATAL: submitted_at_epoch_ms must be non-bool integer")
    return v


def validate_job_spec(jobs: list[dict] | None) -> None:
    if not jobs:
        raise SystemExit("FATAL: job_ids.json jobs=[] empty")
    tables = [j.get("table") for j in jobs]
    if not all(t in ("EV", "PP") for t in tables):
        raise SystemExit(f"FATAL: unknown table labels in {tables}")
    ev = [j for j in jobs if j.get("table") == "EV"]
    pp = [j for j in jobs if j.get("table") == "PP"]
    if len(jobs) not in (2, 4):
        raise SystemExit(f"FATAL: expected 2 (one lane) or 4 (two lane) jobs, got {len(jobs)}")
    if len(ev) not in (1, 2) or len(pp) not in (1, 2) or len(ev) + len(pp) != len(jobs):
        raise SystemExit(f"FATAL: lane table counts ev={len(ev)} pp={len(pp)} total={len(jobs)}")
    names = [j.get("name") for j in jobs]
    if len(set(names)) != len(names):
        raise SystemExit("FATAL: duplicate pipeline.name across jobs")
    lane_roots = [normalize_checkpoint_uri(j.get("checkpoint_dir", "")) for j in jobs]
    if any(not r for r in lane_roots):
        raise SystemExit("FATAL: empty checkpoint_dir in lane spec")
    if len(jobs) == 2:
        if len(set(lane_roots)) != 1:
            raise SystemExit("FATAL: two jobs require the same lane checkpoint_dir root")
    if len(jobs) == 4:
        uniq_lanes = set(lane_roots)
        if len(uniq_lanes) != 2:
            raise SystemExit("FATAL: four jobs require exactly two distinct lane checkpoint roots")
        for lane in uniq_lanes:
            ts = {j.get("table") for j in jobs if normalize_checkpoint_uri(j.get("checkpoint_dir", "")) == lane}
            if ts != {"EV", "PP"}:
                raise SystemExit(f"FATAL: lane {lane} must have one EV and one PP")
    combos = {(normalize_checkpoint_uri(j.get("checkpoint_dir", "")), j.get("name"), j.get("job_id")) for j in jobs}
    if len(combos) != len(jobs):
        raise SystemExit("FATAL: duplicate lane/name/job_id combination")
    for j in jobs:
        if not j.get("job_id"):
            raise SystemExit("FATAL: empty job_id in lane spec")
        if not j.get("name"):
            raise SystemExit(f"FATAL: empty job name for job {j.get('job_id')}")
    ids = [j["job_id"] for j in jobs]
    if len(set(ids)) != len(ids):
        raise SystemExit("FATAL: duplicate job_id entries in job_ids.json")


def capacity_from_overview(overview: dict, jobs_expected: int) -> dict:
    total = overview.get("slots-total")
    avail = overview.get("slots-available")
    if total is None or avail is None:
        raise SystemExit("FATAL: REST /overview missing typed slots-total or slots-available")
    if isinstance(total, bool) or isinstance(avail, bool):
        raise SystemExit("FATAL: slots-total/slots-available must not be bool")
    if not isinstance(total, int) or not isinstance(avail, int):
        raise SystemExit("FATAL: slots-total/slots-available must be integers")
    return {
        "slots_total": total,
        "slots_available": avail,
        "jobs_expected": jobs_expected,
    }


def checkpoint_fresh(latest: dict, submitted_at_ms: int) -> tuple[bool, str]:
    trig = latest.get("trigger_timestamp")
    if trig is None:
        return False, "missing trigger_timestamp on completed checkpoint"
    if isinstance(trig, bool) or not isinstance(trig, int):
        return False, "trigger_timestamp must be int"
    if int(trig) < int(submitted_at_ms):
        return False, "checkpoint older than submit receipt (stale baseline)"
    return True, "ok"


def probe_job(
    port: str,
    j: dict,
    submitted_at_ms: int,
    get_json,
) -> tuple[bool, dict]:
    jid = j["job_id"]
    detail = get_json(f"http://127.0.0.1:{port}/jobs/{jid}")
    state = detail.get("state")
    if (detail.get("name") or "") != j.get("name"):
        return False, {
            "job_id": jid,
            "ready": False,
            "reason": "job name mismatch vs submit receipt",
            "rest_name": detail.get("name"),
            "expected_name": j.get("name"),
        }
    if state != "RUNNING":
        return False, {"job_id": jid, "table": j.get("table"), "state": state, "ready": False}
    cps = get_json(f"http://127.0.0.1:{port}/jobs/{jid}/checkpoints")
    completed = cps.get("counts", {}).get("completed", 0)
    latest = cps.get("latest", {}).get("completed")
    if not latest or completed < 1:
        return False, {
            "job_id": jid,
            "table": j.get("table"),
            "state": state,
            "checkpoints_completed": completed,
            "ready": False,
        }
    ext = latest.get("external_path") or latest.get("storage_path") or ""
    matched, reason = checkpoint_under_lane(jid, j.get("checkpoint_dir", ""), ext)
    if not matched:
        return False, {
            "job_id": jid,
            "table": j.get("table"),
            "path": ext,
            "expected_checkpoint_dir": j.get("checkpoint_dir"),
            "ready": False,
            "reason": reason,
        }
    fresh, freason = checkpoint_fresh(latest, submitted_at_ms)
    if not fresh:
        return False, {
            "job_id": jid,
            "ready": False,
            "reason": freason,
            "latest_checkpoint": latest,
        }
    return True, {
        "job_id": jid,
        "table": j.get("table"),
        "state": state,
        "checkpoints_completed": completed,
        "latest_checkpoint": latest,
        "checkpoint_match": reason,
        "ready": True,
    }


def run_wait(port: str, ids_path: Path, max_wait: int) -> None:
    spec = json.loads(ids_path.read_text())
    jobs = spec.get("jobs")
    validate_job_spec(jobs)
    submitted_at_ms = require_submit_ms(spec)

    def get_json(url):
        return json.loads(urllib.request.urlopen(url, timeout=15).read())

    overview = get_json(f"http://127.0.0.1:{port}/overview")
    capacity = capacity_from_overview(overview, len(jobs))

    deadline = time.time() + max_wait
    report: list[dict] = []
    while time.time() < deadline:
        ok = True
        report = []
        for j in jobs:
            job_ok, row = probe_job(port, j, submitted_at_ms, get_json)
            report.append(row)
            if not job_ok:
                ok = False
        if ok:
            out = {"result": "READY", "jobs": report, "cluster_capacity": capacity}
            print(json.dumps(out, indent=2))
            ids_path.parent.joinpath("wait_ready_report.json").write_text(json.dumps(out, indent=2))
            return
        time.sleep(0.2)

    ids_path.parent.joinpath("wait_ready_report.json").write_text(
        json.dumps({"result": "TIMEOUT", "jobs": report, "cluster_capacity": capacity}, indent=2)
    )
    raise SystemExit(f"FATAL: jobs not ready with completed checkpoints: {report}")


def run_selftest_with_mock() -> int:
    import contextlib
    import io
    import subprocess
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from threading import Thread

    ev_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    pp_id = "ffffffffffffffffffffffffffffffff"
    lane = "/tmp/lane-a/checkpoints"
    submit_ms = 1_700_000_000_000
    mode = {"v": "two_ok", "ext_fmt": "plain"}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def do_GET(self):
            m = mode["v"]
            if self.path == "/overview":
                body = {"slots-total": 4, "slots-available": 0}
                if m == "slot_bool":
                    body = {"slots-total": True, "slots-available": 0}
            elif self.path == "/jobs":
                body = {"jobs": [{"id": ev_id}, {"id": pp_id}]}
            elif self.path.endswith("/checkpoints"):
                jid = self.path.split("/")[2]
                ts = submit_ms + 5000
                if m == "stale_cp" and jid == ev_id:
                    ts = submit_ms - 5000
                fmt = mode.get("ext_fmt", "plain")
                if m == "wrong_lane" and jid == ev_id:
                    base = "/tmp/lane-WRONG"
                else:
                    base = lane
                if fmt == "file3":
                    ext = f"file:///{base.lstrip('/')}/{jid}/chk"
                elif fmt == "file2":
                    ext = f"file:{base}/{jid}/chk"
                else:
                    ext = f"{base}/{jid}/chk"
                body = {
                    "counts": {"completed": 1},
                    "latest": {"completed": {"external_path": ext, "trigger_timestamp": ts}},
                }
            elif self.path.startswith("/jobs/"):
                jid = self.path.split("/")[2]
                names = {ev_id: "lane-a-ev", pp_id: "lane-a-pp"}
                if m == "wrong_name" and jid == ev_id:
                    names[ev_id] = "wrong-ev-name"
                body = {"name": names.get(jid, "x"), "state": "RUNNING"}
            else:
                self.send_response(404)
                self.end_headers()
                return
            raw = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(raw)

    srv = HTTPServer(("127.0.0.1", 0), Handler)
    port = srv.server_address[1]
    Thread(target=srv.serve_forever, daemon=True).start()

    base_spec = {
        "submitted_at_epoch_ms": submit_ms,
        "jobs": [
            {"job_id": ev_id, "name": "lane-a-ev", "checkpoint_dir": f"file://{lane}", "table": "EV"},
            {"job_id": pp_id, "name": "lane-a-pp", "checkpoint_dir": f"file://{lane}", "table": "PP"},
        ],
    }
    td = Path(f"/tmp/wait-selftest-{port}")
    td.mkdir(parents=True, exist_ok=True)
    results = []
    ok_all = True

    def run_case(name: str, spec: dict, mv: str, want: bool, ext_fmt: str = "plain"):
        nonlocal ok_all
        mode["v"] = mv
        mode["ext_fmt"] = ext_fmt
        p = td / f"{name}.json"
        p.write_text(json.dumps(spec))
        try:
            run_wait(str(port), p, 2)
            got = True
        except SystemExit:
            got = False
        if got != want:
            ok_all = False
        results.append({"case": name, "expected_pass": want, "got_pass": got})

    run_case("positive_2jobs", base_spec, "two_ok", True)
    run_case("wrong_lane_prefix", base_spec, "wrong_lane", False)
    run_case("stale_checkpoint", base_spec, "stale_cp", False)
    run_case("wrong_job_name", base_spec, "wrong_name", False)

    for fmt in ("plain", "file2", "file3"):
        ok, _ = checkpoint_under_lane(ev_id, f"file://{lane}", f"{lane}/{ev_id}/chk" if fmt == "plain" else (
            f"file:/{lane}/{ev_id}/chk" if fmt == "file2" else f"file:///{lane.lstrip('/')}/{ev_id}/chk"
        ))
        if not ok:
            ok_all = False
        results.append({"case": f"uri_format_{fmt}", "expected_pass": True, "got_pass": ok})

    no_submit = dict(base_spec)
    del no_submit["submitted_at_epoch_ms"]
    p = td / "no_submit_ms.json"
    p.write_text(json.dumps(no_submit))
    try:
        run_wait(str(port), p, 1)
        results.append({"case": "missing_submit_ms", "expected_pass": False, "got_pass": True})
        ok_all = False
    except SystemExit:
        results.append({"case": "missing_submit_ms", "expected_pass": False, "got_pass": False})

    four_one_lane = {
        "submitted_at_epoch_ms": submit_ms,
        "jobs": [
            {"job_id": ev_id, "name": "n1", "checkpoint_dir": f"file://{lane}", "table": "EV"},
            {"job_id": pp_id, "name": "n2", "checkpoint_dir": f"file://{lane}", "table": "PP"},
            {"job_id": "c" * 32, "name": "n3", "checkpoint_dir": f"file://{lane}", "table": "EV"},
            {"job_id": "d" * 32, "name": "n4", "checkpoint_dir": f"file://{lane}", "table": "PP"},
        ],
    }
    try:
        validate_job_spec(four_one_lane["jobs"])
        results.append({"case": "four_jobs_one_lane", "expected": "fail", "got": "pass"})
        ok_all = False
    except SystemExit:
        results.append({"case": "four_jobs_one_lane", "expected": "fail", "got": "fail"})

    dup_name_spec = dict(base_spec)
    dup_name_spec["jobs"] = [
        base_spec["jobs"][0],
        {**base_spec["jobs"][1], "name": "lane-a-ev"},
    ]
    try:
        validate_job_spec(dup_name_spec["jobs"])
        results.append({"case": "duplicate_name", "expected": "fail", "got": "pass"})
        ok_all = False
    except SystemExit:
        results.append({"case": "duplicate_name", "expected": "fail", "got": "fail"})

    run_case("slot_bool", base_spec, "slot_bool", False)

    sh = Path(__file__).resolve().parent / "wait_job_ready.sh"
    mode["v"] = "two_ok"
    p = td / "via_shell.json"
    p.write_text(json.dumps(base_spec))
    r = subprocess.run([str(sh), str(port), str(p), "3"], capture_output=True, text=True)
    results.append({"case": "wait_job_ready_sh_positive", "expected_pass": True, "got_pass": r.returncode == 0})
    if r.returncode != 0:
        ok_all = False

    srv.shutdown()
    print(json.dumps({"wait_ready_selftest": ok_all, "results": results}, indent=2))
    return 0 if ok_all else 1


def main() -> None:
    if len(sys.argv) >= 2 and sys.argv[1] == "--selftest":
        raise SystemExit(run_selftest_with_mock())
    port, ids_path, max_wait = sys.argv[1], Path(sys.argv[2]), int(sys.argv[3])
    run_wait(port, ids_path, max_wait)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Finite regress harness (Astra recipe) against worktree code paths — no network/disk writes."""
from __future__ import annotations

import io
import json
import sys
import time
import urllib.request
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bin"))

import submit_bind  # noqa: E402
import wait_ready_probe as wr  # noqa: E402


def submit_cases() -> list[dict]:
    out = []
    stock = "Job ID: 03df92956e4f90acb852c6ad46270936\n"
    got = submit_bind.parse_sql_client_job_id(stock)
    out.append(
        {
            "case": "actual_stock_sql_client",
            "expected_id": "03df92956e4f90acb852c6ad46270936",
            "got": got,
            "pass": got == "03df92956e4f90acb852c6ad46270936",
        }
    )
    old_new = (
        "=== SUBMIT_ATTEMPT_BEGIN ===\n"
        "Job ID: " + "a" * 32 + "\n"
        "Job ID: " + "b" * 32 + "\n"
    )
    got2 = submit_bind.parse_sql_client_job_id(old_new)
    out.append(
        {
            "case": "old_then_new_receipt_same_attempt",
            "expected_id": "b" * 32,
            "got": got2,
            "pass": got2 == "b" * 32,
        }
    )
    legacy_jobid_only = "JobID: " + "a" * 32 + "\nJobID: " + "b" * 32 + "\n"
    got3 = submit_bind.parse_sql_client_job_id(legacy_jobid_only)
    out.append(
        {
            "case": "legacy_jobid_line_fail_closed",
            "expected_id": None,
            "got": got3,
            "pass": got3 is None,
        }
    )
    return out


def ready_probe(name: str, spec: dict, external_fn, names: dict | None = None) -> dict:
    writes: dict[str, str] = {}
    ids = [j["job_id"] for j in spec.get("jobs", [])]

    def read_text(_self, *a, **k):
        return json.dumps(spec)

    def write_text(_self, data, *a, **k):
        writes[str(_self)] = data
        return len(data)

    def urlopen(url, **kw):
        url_s = url.full_url if hasattr(url, "full_url") else str(url)
        if url_s.endswith("/overview"):
            data = {"slots-total": 4, "slots-available": 2}
        elif url_s.endswith("/checkpoints"):
            jid = url_s.rstrip("/").split("/")[-2]
            data = {
                "counts": {"completed": 1},
                "latest": {
                    "completed": {
                        "external_path": external_fn(jid),
                        "status": "COMPLETED",
                        "trigger_timestamp": spec.get("submitted_at_epoch_ms", 1_700_000_000_000) + 5000,
                        "latest_ack_timestamp": 2,
                    }
                },
            }
        else:
            jid = url_s.rstrip("/").split("/")[-1]
            nm = (names or {}).get(jid, "unrelated")
            data = {"state": "RUNNING", "name": nm, "start-time": 1000}
        return io.BytesIO(json.dumps(data).encode())

    def no_sleep(*a):
        raise RuntimeError("would wait")

    rc: object
    with (
        patch.object(Path, "read_text", read_text),
        patch.object(Path, "write_text", write_text),
        patch.object(urllib.request, "urlopen", urlopen),
        patch.object(time, "sleep", no_sleep),
    ):
        try:
            wr.run_wait("28120", Path("memory-job-ids.json"), 1)
            rc = 0
        except SystemExit as e:
            rc = e.code if e.code is not None else 1
        except RuntimeError as e:
            if "would wait" in str(e):
                rc = "would_wait"
            else:
                rc = str(e)
        except Exception as e:
            rc = type(e).__name__ + ":" + str(e)

    report = json.loads(next(iter(writes.values()))) if writes else {}
    ready = report.get("result") == "READY"
    bad_ready = ready or rc == 0
    return {
        "case": name,
        "exit": rc,
        "result": report.get("result"),
        "capacity": report.get("cluster_capacity"),
        "must_not_ready": True,
        "pass": not bad_ready,
    }


def ready_cases() -> list[dict]:
    ids = ["a" * 32, "b" * 32]
    base = {
        "binding": "submit_receipt_sql_client_job_id_line",
        "submitted_at_epoch_ms": 1_700_000_000_000,
        "rest_port": 28120,
        "jobs": [
            {"job_id": ids[0], "table": "EV", "checkpoint_dir": "file:///lane", "name": "lane-ev"},
            {"job_id": ids[1], "table": "PP", "checkpoint_dir": "file:///lane", "name": "lane-pp"},
        ],
    }
    names_ok = {ids[0]: "lane-ev", ids[1]: "lane-pp"}
    rows = [
        ready_probe(
            "old_checkpoint_wrong_name",
            base,
            lambda j: "/lane/" + j + "/chk-1",
            {ids[0]: "wrong-name", ids[1]: "lane-pp"},
        ),
        ready_probe("lane_prefix_collision", base, lambda j: "/lane-WRONG/" + j + "/chk-1", names_ok),
        ready_probe(
            "single_other_table",
            {"jobs": [{"job_id": ids[0], "table": "OTHER", "checkpoint_dir": "file:///lane", "name": "x"}]},
            lambda j: "/lane/" + j + "/chk-1",
        ),
    ]
    # empty jobs — validate only
    try:
        wr.validate_job_spec([])
        empty_rc = "pass"
    except SystemExit:
        empty_rc = "fail"
    rows.append(
        {
            "case": "empty_jobs_control",
            "exit": empty_rc,
            "must_not_ready": False,
            "pass": empty_rc == "fail",
        }
    )
    # positive control (valid names/paths/times)
    pos_writes: dict[str, str] = {}

    def pos_read(_self, *a, **k):
        return json.dumps(base)

    def pos_write(_self, data, *a, **k):
        pos_writes[str(_self)] = data
        return len(data)

    def pos_urlopen(url, **kw):
        url_s = url.full_url if hasattr(url, "full_url") else str(url)
        if url_s.endswith("/overview"):
            data = {"slots-total": 4, "slots-available": 2}
        elif url_s.endswith("/checkpoints"):
            jid = url_s.rstrip("/").split("/")[-2]
            data = {
                "counts": {"completed": 1},
                "latest": {
                    "completed": {
                        "external_path": f"/lane/{jid}/chk-1",
                        "trigger_timestamp": base["submitted_at_epoch_ms"] + 1000,
                    }
                },
            }
        else:
            jid = url_s.rstrip("/").split("/")[-1]
            data = {"state": "RUNNING", "name": names_ok[jid]}
        return io.BytesIO(json.dumps(data).encode())

    import contextlib

    with (
        patch.object(Path, "read_text", pos_read),
        patch.object(Path, "write_text", pos_write),
        patch.object(urllib.request, "urlopen", pos_urlopen),
        patch.object(time, "sleep", lambda *a: None),
        contextlib.redirect_stdout(io.StringIO()),
    ):
        try:
            wr.run_wait("28120", Path("memory-job-ids.json"), 1)
            pos_rc = 0
        except SystemExit as e:
            pos_rc = e.code

    pos_report = json.loads(next(iter(pos_writes.values()))) if pos_writes else {}
    rows.append(
        {
            "case": "positive_2jobs_valid_fixture",
            "exit": pos_rc,
            "result": pos_report.get("result"),
            "capacity": pos_report.get("cluster_capacity"),
            "pass": pos_report.get("result") == "READY"
            and pos_report.get("cluster_capacity", {}).get("slots_total") == 4,
        }
    )
    return rows


def main() -> int:
    submit = submit_cases()
    ready = ready_cases()
    ok = all(r.get("pass") for r in submit + ready)
    doc = {
        "harness": "astra_finite_recipe",
        "module_root": str(ROOT),
        "code_paths": ["bin/submit_bind.py", "bin/wait_ready_probe.py"],
        "submit": submit,
        "ready": ready,
        "all_pass": ok,
    }
    print(json.dumps(doc, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

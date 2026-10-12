#!/usr/bin/env python3
"""Bind EV/PP Job IDs from SQL Client stdout (Flink 1.20: 'Job ID: <32hex>')."""
from __future__ import annotations

import json
import re
import sys
import time
import urllib.request
from pathlib import Path

ATTEMPT_MARK = "=== SUBMIT_ATTEMPT_BEGIN ==="
JOB_ID_LINE = re.compile(r"^Job ID:\s*([0-9a-f]{32})\s*$", re.MULTILINE | re.IGNORECASE)


def parse_sql_client_job_id(log_text: str) -> str | None:
    """Last Job ID line in the log (attempt-scoped file or segment after last marker)."""
    segment = log_text
    if ATTEMPT_MARK in log_text:
        segment = log_text.split(ATTEMPT_MARK)[-1]
    matches = JOB_ID_LINE.findall(segment)
    if not matches:
        return None
    return matches[-1]


def parse_log_file(path: Path) -> str | None:
    return parse_sql_client_job_id(path.read_text(errors="replace"))


def job_set(blob: dict) -> set[str]:
    return {e.get("id") for e in blob.get("jobs", []) if e.get("id")}


def bind_jobs(
    port: str,
    ev_name: str,
    pp_name: str,
    ev_cp: str,
    pp_cp: str,
    ev_log: Path,
    pp_log: Path,
    before_path: Path,
) -> dict:
    before = json.loads(before_path.read_text())
    before_ids = job_set(before)

    ev_id = parse_log_file(ev_log)
    pp_id = parse_log_file(pp_log)
    if not ev_id or not pp_id:
        raise SystemExit(f"FATAL: missing Job ID line in submit logs ev={ev_id} pp={pp_id}")
    if ev_id == pp_id:
        raise SystemExit("FATAL: duplicate JobID from EV/PP submit receipts")

    deadline = time.time() + 60
    after_ids: set[str] = set()
    while time.time() < deadline:
        jobs = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/jobs", timeout=15).read())
        after_ids = job_set(jobs)
        new_ids = after_ids - before_ids
        if {ev_id, pp_id}.issubset(new_ids):
            break
        time.sleep(0.2)
    else:
        raise SystemExit(
            f"FATAL: submit receipts {ev_id}/{pp_id} not in new job set {sorted(after_ids - before_ids)}"
        )

    def detail(jid: str) -> dict:
        return json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/jobs/{jid}", timeout=15).read())

    ev_detail = detail(ev_id)
    pp_detail = detail(pp_id)
    if (ev_detail.get("name") or "") != ev_name:
        raise SystemExit(f"FATAL: EV receipt id {ev_id} name {ev_detail.get('name')!r} != {ev_name!r}")
    if (pp_detail.get("name") or "") != pp_name:
        raise SystemExit(f"FATAL: PP receipt id {pp_id} name {pp_detail.get('name')!r} != {pp_name!r}")

    names_seen: dict[str, list[str]] = {}
    for jid in after_ids:
        n = detail(jid).get("name") or ""
        names_seen.setdefault(n, []).append(jid)
    for expected in (ev_name, pp_name):
        ids = names_seen.get(expected, [])
        if len(ids) > 1:
            raise SystemExit(
                f"FATAL: duplicate pipeline.name {expected!r} on job ids {ids}; refuse name-scan binding"
            )

    return {
        "rest_port": int(port),
        "submitted_at_epoch_ms": int(time.time() * 1000),
        "binding": "submit_receipt_sql_client_job_id_line",
        "jobs_before_count": len(before_ids),
        "jobs": [
            {
                "job_id": ev_id,
                "name": ev_name,
                "checkpoint_dir": ev_cp,
                "table": "EV",
                "submit_log": str(ev_log),
            },
            {
                "job_id": pp_id,
                "name": pp_name,
                "checkpoint_dir": pp_cp,
                "table": "PP",
                "submit_log": str(pp_log),
            },
        ],
    }


def main() -> None:
    if len(sys.argv) >= 2 and sys.argv[1] == "--parse-log":
        print(parse_log_file(Path(sys.argv[2])) or "")
        raise SystemExit(0 if parse_log_file(Path(sys.argv[2])) else 1)

    if len(sys.argv) >= 2 and sys.argv[1] == "--selftest":
        raise SystemExit(run_selftest())

    (
        port,
        ev_name,
        pp_name,
        ev_cp,
        pp_cp,
        out,
        ev_log,
        pp_log,
        before_path,
    ) = sys.argv[1:10]
    payload = bind_jobs(
        port, ev_name, pp_name, ev_cp, pp_cp, Path(ev_log), Path(pp_log), Path(before_path)
    )
    Path(out).write_text(json.dumps(payload, indent=2))
    print(json.dumps(payload, indent=2))


def run_selftest() -> int:
    fixtures = Path(__file__).resolve().parent.parent / "tests/fixtures/submit_logs"
    ok = True
    results: list[dict] = []

    old = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    new = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    sample = fixtures / "ev_sql_client_ok.log"
    if parse_log_file(sample) != old:
        ok = False
        results.append({"case": "parse_ok_last_id", "expected": old, "got": parse_log_file(sample)})

    append = fixtures / "ev_append_old_new.log"
    if parse_log_file(append) != new:
        ok = False
        results.append({"case": "parse_append_takes_last", "expected": new, "got": parse_log_file(append)})

    if parse_log_file(fixtures / "ev_missing_id.log") is not None:
        ok = False
        results.append({"case": "parse_missing_id", "expected": None, "got": "something"})

    print(json.dumps({"submit_bind_selftest": ok, "results": results}, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    main()

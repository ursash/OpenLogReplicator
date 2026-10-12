#!/usr/bin/env python3
"""Recovery phase evidence: REST snapshots, offset-topic raw, verdict records (no data PASS)."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_lines(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for line in f:
            h.update(line)
    return h.hexdigest()


def config_bundle_hash(prep: Path, worker_base: str, register_base: str) -> str:
    wp = prep / "config" / worker_base
    rp = prep / "config" / register_base
    if not wp.is_file() or not rp.is_file():
        raise FileNotFoundError(f"missing config bundle: {wp} or {rp}")
    h = hashlib.sha256()
    for p in (wp, rp):
        h.update(p.read_bytes())
    return h.hexdigest()


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n")


def curl_request(
    method: str,
    url: str,
    out_body: Path,
    out_code: Path,
    *,
    data_path: Path | None = None,
    timeout: int = 30,
) -> int:
    out_body.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, method=method)
    if data_path and data_path.is_file():
        req.data = data_path.read_bytes()
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            code = resp.getcode()
    except urllib.error.HTTPError as e:
        body = e.read()
        code = e.code
    except OSError as e:
        out_code.write_text("000")
        out_body.write_text(str(e))
        return 0
    out_body.write_bytes(body)
    out_code.write_text(str(code))
    return int(code)


def connector_states(status: dict) -> tuple[str, list[str]]:
    c = status.get("connector", {}).get("state", "")
    tasks = [t.get("state", "") for t in status.get("tasks") or []]
    return c, tasks


def is_stopped(status: dict) -> bool:
    c, tasks = connector_states(status)
    if c != "STOPPED":
        return False
    if not tasks:
        return True
    return all(t == "STOPPED" for t in tasks)


def is_running(status: dict) -> bool:
    c, tasks = connector_states(status)
    return c == "RUNNING" and tasks == ["RUNNING"]


def dump_connect_offsets(
    prep: Path,
    bootstrap: str,
    topic: str,
    out_jsonl: Path,
    classes_dir: Path,
    *,
    mock_fixture: Path | None = None,
) -> bool:
    if mock_fixture and mock_fixture.is_file():
        out_jsonl.write_text(mock_fixture.read_text())
        return True
    ro_java = prep / "scripts" / "ReadOffsetsTopic.java"
    if not ro_java.is_file():
        out_jsonl.write_text("")
        return False
    classes_dir.mkdir(parents=True, exist_ok=True)
    ro_class = classes_dir / "ReadOffsetsTopic.class"
    jars = list((prep / "worker-lib").glob("*.jar"))
    if not jars:
        return False
    cp = ":".join(str(j) for j in jars)
    if not ro_class.is_file():
        subprocess.run(
            ["javac", "-cp", cp, "-d", str(classes_dir), str(ro_java)],
            check=False,
            capture_output=True,
        )
    if not ro_class.is_file():
        return False
    r = subprocess.run(
        ["java", "-cp", f"{cp}:{classes_dir}", "ReadOffsetsTopic", bootstrap, topic],
        capture_output=True,
        text=True,
    )
    out_jsonl.write_text(r.stdout)
    return r.returncode == 0


def inflight_progress(a: Path, b: Path) -> bool:
    if not a.is_file() or not b.is_file():
        return False
    return sha256_lines(a) != sha256_lines(b)


def tap_progress_evidence(tap_path: Path, min_bytes_delta: int = 1) -> dict:
    """Read-only tap tail stats; caller supplies path to actual capture file."""
    if not tap_path.is_file():
        return {"ok": False, "reason": "tap_missing", "path": str(tap_path)}
    st = tap_path.stat()
    tail = tap_path.read_bytes()[-4096:]
    return {
        "ok": True,
        "path": str(tap_path),
        "size_bytes": st.st_size,
        "mtime_epoch": int(st.st_mtime),
        "tail_sha256": hashlib.sha256(tail).hexdigest(),
        "min_bytes_delta_hint": min_bytes_delta,
    }


def phase_record(
    out_dir: Path,
    phase: str,
    verdict: str,
    *,
    rc: int,
    commands: list[dict],
    snapshots: dict,
    notes: list[str] | None = None,
) -> None:
    doc = {
        "phase": phase,
        "verdict": verdict,
        "rc": rc,
        "utc": utc_now(),
        "commands": commands,
        "snapshots": snapshots,
        "notes": notes or [],
    }
    write_json(out_dir / f"{phase.lower()}-phase-record.json", doc)


def load_catchup_receipt(path: Path, phase: str) -> tuple[bool, str]:
    """MAIN-supplied business catch-up binding; progress-only is never sufficient."""
    if not path.is_file():
        return False, "missing RECOVERY_CATCHUP_RECEIPT"
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        return False, f"invalid receipt json: {e}"
    if doc.get("phase") != phase:
        return False, f"receipt phase {doc.get('phase')!r} != {phase!r}"
    if doc.get("qualified") is not True:
        return False, "receipt qualified != true"
    if not doc.get("offset_watermark") and not doc.get("business_binding"):
        return False, "receipt missing offset_watermark or business_binding"
    return True, "ok"


def write_progress_record(out: Path, *, phase: str, changed: bool, pre_fp: str, post_fp: str) -> None:
    write_json(
        out,
        {
            "kind": "offset_progress_only",
            "phase": phase,
            "utc": utc_now(),
            "fingerprint_changed": changed,
            "pre_fingerprint": pre_fp,
            "post_fingerprint": post_fp,
            "note": "Does not imply phase PASS or permission to inject next fault",
        },
    )


def parse_register_identity(register_json: Path) -> dict:
    reg = json.loads(register_json.read_text())
    cfg = reg.get("config") or {}
    keys = (
        "topic.prefix",
        "database.connection.adapter",
        "openlogreplicator.source",
        "openlogreplicator.host",
        "openlogreplicator.port",
        "provide.transaction.metadata",
        "exactly.once.support",
    )
    return {k: cfg.get(k) for k in keys}


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: recovery_evidence.py fingerprint|config-hash|selftest", file=sys.stderr)
        return 2
    cmd = sys.argv[1]
    if cmd == "fingerprint":
        print(sha256_lines(Path(sys.argv[2])))
        return 0
    if cmd == "config-hash":
        print(config_bundle_hash(Path(sys.argv[2]), sys.argv[3], sys.argv[4]))
        return 0
    if cmd == "check-receipt":
        ok, msg = load_catchup_receipt(Path(sys.argv[2]), sys.argv[3])
        print(json.dumps({"ok": ok, "msg": msg}))
        return 0 if ok else 1
    if cmd == "selftest":
        td = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
        a, b = td / "offsets-a.jsonl", td / "offsets-b.jsonl"
        assert inflight_progress(a, b)
        assert not inflight_progress(a, a)
        st = json.loads((td / "connector-running.json").read_text())
        assert is_running(st)
        st2 = json.loads((td / "connector-stopped.json").read_text())
        assert is_stopped(st2)
        ok, _ = load_catchup_receipt(td / "catchup-receipt-r1.json", "R1")
        assert ok
        bad, _ = load_catchup_receipt(td / "catchup-receipt-r1.json", "R2")
        assert not bad
        print(json.dumps({"recovery_evidence_selftest": True}))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

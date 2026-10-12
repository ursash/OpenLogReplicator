#!/usr/bin/env python3
"""Write /proc-shaped fixture nodes (status/stat/environ/exe) for dg_r3_procio.py."""
from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

PIN = "4d93d70a98cd915c6a665929498e867bd2bec1b01be185620d4d15237cfbb9af"


def write_status(d: Path, uid: int, ppid: int = 1) -> None:
    (d / "status").write_text(
        f"Name:\tOpenLogReplicator\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\nPPid:\t{ppid}\n",
        encoding="utf-8",
    )


def write_stat(d: Path, pid: int, ppid: int, starttime: str) -> None:
    # Field index 3 (1-based fourth token after comm) is ppid; field 20 is starttime.
    (d / "stat").write_text(
        f"{pid} (OpenLogReplicator) S 0 {ppid} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 {starttime} 0\n",
        encoding="utf-8",
    )


def materialize_exe(d: Path, blob: Path) -> None:
    data = blob.read_bytes()
    if hashlib.sha256(data).hexdigest() != PIN:
        raise SystemExit(f"pinned exe blob sha256 mismatch: {blob}")
    (d / "exe").write_bytes(data)


def main() -> None:
    if len(sys.argv) < 6:
        print(
            "usage: write_proc_fixture.py PROC_DIR PID STARTTIME CMDLINE BLOB [RUN_DIR]",
            file=sys.stderr,
        )
        sys.exit(2)
    proc_dir, pid_s, starttime, cmdline, blob_s = sys.argv[1:6]
    run_dir = sys.argv[6] if len(sys.argv) > 6 else None
    d = Path(proc_dir)
    d.mkdir(parents=True, exist_ok=True)
    uid = int(os.environ.get("DG_R3_ORACLE_UID", "54321"))
    pid = int(pid_s)
    write_status(d, uid)
    write_stat(d, pid, 1, starttime)
    (d / "cmdline").write_bytes(cmdline.encode("utf-8") + b"\0")
    if run_dir:
        (d / "environ").write_bytes(f"RUN_DIR={run_dir}\0".encode("utf-8"))
    materialize_exe(d, Path(blob_s))


if __name__ == "__main__":
    main()

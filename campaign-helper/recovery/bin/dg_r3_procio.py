#!/usr/bin/env python3
"""Privileged /proc reads for dg_r3_recovery_olr.sh (run via sudo).

Exit codes (proc_exists):
  0  process present
  1  not found (gone)
  3  missing expected /proc node (ENOTFOUND)
  4  permission denied on query
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

PROC_EXISTS_PRESENT = 0
PROC_EXISTS_GONE = 1
PROC_EXISTS_ENOENT = 3
PROC_EXISTS_EPERM = 4


def _root(pid: str) -> Path:
    fix = os.environ.get("DG_R3_PROCIO_FIXTURE_ROOT")
    if fix:
        return Path(fix) / pid
    return Path(f"/proc/{pid}")


def _read_bytes(path: Path) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _parse_stat_fields(pid: str) -> list[str]:
    root = _root(pid)
    stat = (root / "stat").read_text(encoding="utf-8")
    return stat.rsplit(")", 1)[-1].split()


def _cmdline_str(pid: str) -> str:
    return _read_bytes(_root(pid) / "cmdline").replace(b"\0", b" ").decode("utf-8", errors="replace").strip()


def _starttime(pid: str) -> str:
    root = _root(pid)
    st = root / "starttime"
    if st.is_file():
        return st.read_text(encoding="utf-8").strip()
    return _parse_stat_fields(pid)[19]


def _uid(pid: str) -> int:
    root = _root(pid)
    status = root / "status"
    if status.is_file():
        for line in status.read_text(encoding="utf-8").splitlines():
            if line.startswith("Uid:"):
                return int(line.split()[1])
    try:
        return os.stat(root, follow_symlinks=False).st_uid
    except PermissionError as e:
        raise e
    except FileNotFoundError as e:
        raise e


def _exe_display_path(pid: str) -> str:
    root = _root(pid)
    if (root / "exe").is_symlink():
        return os.readlink(root / "exe")
    if (root / "exe").is_file():
        return str(root / "exe")
    raise FileNotFoundError(str(root / "exe"))


def _exe_sha256(pid: str) -> str:
    # Hash loaded mapping via /proc/PID/exe (do not follow to disk path).
    exe = _root(pid) / "exe"
    return hashlib.file_digest(open(exe, "rb"), "sha256").hexdigest()


def _proc_exists(pid: str) -> None:
    root = _root(pid)
    if (root / "gone").is_file():
        sys.exit(PROC_EXISTS_GONE)
    if (root / "proc_exists_eperm").is_file():
        sys.exit(PROC_EXISTS_EPERM)
    try:
        if (root / "stat").is_file() or (root / "cmdline").is_file():
            sys.exit(PROC_EXISTS_PRESENT)
        sys.exit(PROC_EXISTS_GONE)
    except PermissionError:
        sys.exit(PROC_EXISTS_EPERM)


def cmdline(pid: str) -> None:
    sys.stdout.buffer.write(_read_bytes(_root(pid) / "cmdline"))


def environ_var(pid: str, key: str) -> None:
    root = _root(pid)
    data = _read_bytes(root / "environ")
    prefix = (key + "=").encode()
    for part in data.split(b"\0"):
        if part.startswith(prefix):
            sys.stdout.write(part[len(prefix) :].decode("utf-8", errors="replace"))
            return
    sys.exit(PROC_EXISTS_ENOENT)


def main() -> None:
    if len(sys.argv) < 3:
        print(
            "usage: dg_r3_procio.py cmdline|environ_var|starttime|exe_path|exe_sha256|uid|proc_exists|identity PID [KEY]",
            file=sys.stderr,
        )
        sys.exit(2)
    op, pid = sys.argv[1], sys.argv[2]
    try:
        if op == "cmdline":
            cmdline(pid)
        elif op == "environ_var":
            environ_var(pid, sys.argv[3])
        elif op == "starttime":
            sys.stdout.write(_starttime(pid))
        elif op == "exe_path":
            sys.stdout.write(_exe_display_path(pid))
        elif op == "exe_sha256":
            sys.stdout.write(_exe_sha256(pid))
        elif op == "uid":
            sys.stdout.write(str(_uid(pid)))
        elif op == "proc_exists":
            _proc_exists(pid)
        elif op == "identity":
            doc = {
                "pid": int(pid),
                "uid": _uid(pid),
                "starttime": _starttime(pid),
                "exe": _exe_display_path(pid),
                "exe_sha256": _exe_sha256(pid),
                "cmdline": _cmdline_str(pid),
            }
            sys.stdout.write(json.dumps(doc, separators=(",", ":")))
        else:
            sys.exit(2)
    except FileNotFoundError:
        sys.exit(PROC_EXISTS_ENOENT)
    except PermissionError:
        sys.exit(PROC_EXISTS_EPERM)


if __name__ == "__main__":
    main()

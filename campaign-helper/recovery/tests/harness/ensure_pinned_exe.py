#!/usr/bin/env python3
"""Materialize pinned OpenLogReplicator bytes for proc fixture exe (sha256 must match pin)."""
from __future__ import annotations

import hashlib
import os
import shutil
import sys
from pathlib import Path

PIN = "4d93d70a98cd915c6a665929498e867bd2bec1b01be185620d4d15237cfbb9af"


def ok(path: Path) -> bool:
    if not path.is_file():
        return False
    return hashlib.file_digest(path.open("rb"), "sha256").hexdigest() == PIN


def main() -> None:
    dest = Path(sys.argv[1])
    candidates = [
        Path(os.environ.get("OLR_PINNED_EXE_SOURCE", "")),
        Path(os.environ.get("DG_R3_PINNED_EXE_BLOB", "")),
        Path(__file__).resolve().parent.parent / "fixtures" / "olr-pinned-exe.bin",
    ]
    for src in candidates:
        if ok(src):
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, dest)
            sys.exit(0)
    print("no pinned exe blob (sha256 pin match); set OLR_PINNED_EXE_SOURCE or add tests/fixtures/olr-pinned-exe.bin", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()

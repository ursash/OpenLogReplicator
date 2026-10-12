"""Minimal JSONL artifact line fetch (locator line:N only)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from srl_hashes import sha256_file

_LINE_LOC = re.compile(r"^line:(\d+)$")


def read_artifact_line(path: str, locator: str) -> tuple[dict[str, Any] | None, str | None]:
    m = _LINE_LOC.match(str(locator or "").strip())
    if not m:
        return None, f"unsupported locator {locator!r} (use line:N)"
    line_no = int(m.group(1))
    if line_no < 1:
        return None, "line number must be >= 1"
    p = Path(path)
    if not p.is_file():
        return None, f"artifact not found: {path}"
    with p.open(encoding="utf-8") as fh:
        for i, line in enumerate(fh, 1):
            if i == line_no:
                line = line.strip()
                if not line:
                    return None, f"empty line {line_no} in {path}"
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    return None, f"JSON line {line_no}: {e}"
                if not isinstance(obj, dict):
                    return None, f"line {line_no} not object"
                return obj, None
    return None, f"line {line_no} past EOF in {path}"


def resolve_artifact_record(
    ref: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    """Return (record, sha256, error)."""
    if not isinstance(ref, dict):
        return None, None, "artifact_ref not object"
    path = ref.get("path")
    locator = ref.get("locator")
    expect_sha = ref.get("sha256")
    if not path or not locator or not expect_sha:
        return None, None, "artifact_ref requires path, locator, sha256"
    es = str(expect_sha).lower()
    if es in ("0" * 64, "placeholder-partition-bound-sha256", "placeholder-mw-sha256"):
        return None, None, "placeholder sha256 rejected"
    got_sha = sha256_file(str(path))
    if got_sha != es:
        return None, got_sha, f"sha256 mismatch for {path}"
    rec, err = read_artifact_line(str(path), str(locator))
    if err:
        return None, got_sha, err
    return rec, got_sha, None

"""Strict timezone-aware UTC parsing."""

from __future__ import annotations

from datetime import datetime, timezone


def parse_utc_strict(value: str, *, field: str) -> datetime:
    s = str(value).strip()
    if not s:
        raise ValueError(f"{field}: empty datetime")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        raise ValueError(f"{field}: naive datetime forbidden")
    return dt.astimezone(timezone.utc)

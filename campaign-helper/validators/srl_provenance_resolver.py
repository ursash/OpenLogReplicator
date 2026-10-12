"""Read-only artifact access for provenance (SHA + JSONL line parse)."""

from __future__ import annotations

from typing import Any

from srl_provenance_artifacts import resolve_artifact_record

ResolveRecordFn = type(None)  # noqa: F821 — use resolve_artifact_record directly


def verify_artifact_ref_fields(ref: dict) -> list[str]:
    errs: list[str] = []
    rec, _sha, err = resolve_artifact_record(ref)
    if err:
        errs.append(err)
    elif rec is None:
        errs.append("artifact record missing")
    return errs


def load_artifact_record(ref: dict) -> tuple[dict[str, Any] | None, list[str]]:
    rec, _sha, err = resolve_artifact_record(ref)
    if err:
        return None, [err]
    return rec, []

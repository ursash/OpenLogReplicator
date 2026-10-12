"""Typed field comparison: NULL (_null_fields), absent (__kind__), exact VARCHAR."""

from __future__ import annotations

import json
from typing import Any

from srl_contracts import INT_COLUMNS, STRING_COLUMNS, TABLE_COLUMNS

KIND_ABSENT = "absent"
KIND_UNAVAILABLE = "unavailable"


def field_spec_kind(value: Any) -> str | None:
    if isinstance(value, dict) and len(value) == 1:
        k = value.get("__kind__")
        if k in (KIND_ABSENT, KIND_UNAVAILABLE):
            return k
    return None


def normalize_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("bool not int")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            raise ValueError("empty int")
        if not s.lstrip("-").isdigit():
            raise ValueError(f"not int literal: {s!r}")
        return int(s)
    raise ValueError(f"cannot int: {value!r}")


def normalize_string_exact(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def normalize_column(table: str, col: str, value: Any) -> Any:
    if value is None:
        return None
    if col in INT_COLUMNS.get(table, ()):
        return normalize_int(value)
    if col in STRING_COLUMNS.get(table, ()):
        return normalize_string_exact(value)
    return value


def materialize_oracle_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    r = {k: v for k, v in row.items() if k != "_null_fields"}
    for f in row.get("_null_fields") or []:
        r[str(f)] = None
    out: dict[str, Any] = {}
    table_guess = "EV" if "TAG" in r or "PAD" in r else "PP" if "CTR" in r else "EV"
    for k, v in r.items():
        if k in TABLE_COLUMNS.get(table_guess, ()):
            try:
                out[k] = normalize_column(table_guess, k, v)
            except ValueError:
                out[k] = v
        else:
            out[k] = v
    return out


def materialize_oracle_row_for_table(table: str, row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    base = {k: v for k, v in row.items() if k != "_null_fields"}
    for f in row.get("_null_fields") or []:
        base[str(f)] = None
    out: dict[str, Any] = {}
    for k, v in base.items():
        if k not in TABLE_COLUMNS.get(table, ()):
            continue
        if v is None:
            out[k] = None
        else:
            try:
                out[k] = normalize_column(table, k, v)
            except ValueError:
                out[k] = v
    return out


def oracle_row_complete(row: dict[str, Any] | None, table: str, columns: tuple[str, ...]) -> bool:
    if row is None:
        return True
    null_fields = set(row.get("_null_fields") or [])
    for c in columns:
        if c in row or c in null_fields:
            continue
        return False
    return True


def normalize_row_for_table(table: str, row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    clean = {k: v for k, v in row.items() if k != "_null_fields"}
    out: dict[str, Any] = {}
    for k, v in clean.items():
        if k not in TABLE_COLUMNS.get(table, ()):
            continue
        if v is None:
            out[k] = None
        else:
            try:
                out[k] = normalize_column(table, k, v)
            except ValueError:
                out[k] = v
    return out


def actual_field_null(actual: dict | None, field: str) -> bool:
    if not actual:
        return True
    if field not in actual:
        return True
    return actual.get(field) is None


def compare_field(table: str, field: str, expected: Any, actual: Any) -> str | None:
    kind = field_spec_kind(expected)
    if kind == KIND_ABSENT:
        if isinstance(actual, dict) and field in actual:
            return f"{field}: expected absent column, present in actual"
        return None
    if kind == KIND_UNAVAILABLE:
        return f"{field}: unavailable spec not verifiable (reject in oracle)"
    if expected is None:
        if actual is None:
            return None
        if actual_field_null({"x": actual}, "x"):
            return None
        return f"{field}: expected NULL, got {actual!r}"
    try:
        exp_n = normalize_column(table, field, expected)
        act_n = normalize_column(table, field, actual) if actual is not None else None
    except ValueError as e:
        return f"{field}: {e}"
    if exp_n != act_n:
        return f"{field}: expected {exp_n!r}, got {act_n!r}"
    return None


def compare_rows(
    expected: dict[str, Any] | None,
    actual: dict[str, Any] | None,
    table: str,
    fields: tuple[str, ...] | None = None,
) -> list[str]:
    errors: list[str] = []
    if expected is None and actual is None:
        return errors
    if expected is None:
        return ["expected null row, got data"]
    if actual is None:
        return ["expected row data, got null"]
    fields = fields or TABLE_COLUMNS[table]
    exp_mat = materialize_oracle_row_for_table(table, expected)
    act_n = normalize_row_for_table(table, actual)

    null_fields = set(expected.get("_null_fields") or [])
    for f in fields:
        if f in null_fields:
            if not act_n or f not in act_n:
                errors.append(
                    f"{f}: expected explicit JSON null (_null_fields), column key absent in actual"
                )
            elif act_n.get(f) is not None:
                errors.append(f"{f}: expected NULL (_null_fields), got {act_n.get(f)!r}")
            continue
        kind = field_spec_kind(expected.get(f)) if f in expected else None
        if kind == KIND_ABSENT:
            if act_n and f in act_n:
                errors.append(f"{f}: expected absent, present in actual")
            continue
        if f not in (exp_mat or {}):
            continue
        err = compare_field(table, f, exp_mat.get(f) if exp_mat else None, act_n.get(f) if act_n else None)
        if err:
            errors.append(err)
    return errors


def row_signature(table: str, op: str, key: dict, before, after) -> str:
    payload = {
        "table": table,
        "op": op,
        "key": materialize_oracle_row_for_table(table, key),
        "before": materialize_oracle_row_for_table(table, before) if before is not None else None,
        "after": materialize_oracle_row_for_table(table, after) if after is not None else None,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))

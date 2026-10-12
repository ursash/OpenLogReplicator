"""Flink sink row loader and final-state comparison."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from srl_contracts import BUSINESS_TABLES, TABLE_COLUMNS
from srl_fields import compare_rows, materialize_oracle_row_for_table, normalize_row_for_table


@dataclass
class FlinkSinkState:
    tables: dict[str, dict[Any, dict[str, Any] | None]] = field(
        default_factory=lambda: {t: {} for t in BUSINESS_TABLES}
    )
    errors: list[str] = field(default_factory=list)


def _apply_sink_line(state: FlinkSinkState, obj: dict, line_no: int) -> None:
    table = str(obj.get("table", "")).upper()
    if table not in BUSINESS_TABLES:
        state.errors.append(f"line {line_no}: unknown table {table!r}")
        return
    if obj.get("deleted") is True or obj.get("tombstone") is True:
        key = normalize_row_for_table(table, obj.get("key") or {})
        kid = key.get("ID") if key else None
        if kid is None:
            state.errors.append(f"line {line_no}: delete without key.ID")
            return
        state.tables[table][kid] = None
        return
    if "row" in obj:
        row = materialize_oracle_row_for_table(table, obj["row"])
    elif "values" in obj:
        row = materialize_oracle_row_for_table(
            table, {**(obj.get("key") or {}), **obj["values"]}
        )
    else:
        row = materialize_oracle_row_for_table(table, obj)
    if not row or row.get("ID") is None:
        state.errors.append(f"line {line_no}: missing ID")
        return
    state.tables[table][row["ID"]] = row


def load_flink_sink(path: str) -> FlinkSinkState:
    state = FlinkSinkState()
    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                state.errors.append(f"line {line_no}: {e}")
                continue
            if not isinstance(obj, dict):
                state.errors.append(f"line {line_no}: not object")
                continue
            _apply_sink_line(state, obj, line_no)
    return state


def flink_rows(state: FlinkSinkState) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for table in BUSINESS_TABLES:
        live = [r for r in state.tables[table].values() if r is not None]
        out[table] = sorted(live, key=lambda r: r.get("ID"))
    return out


def compare_final_states(
    expected: dict[str, list[dict]],
    actual: dict[str, list[dict]],
    absent_ids: dict[str, list[Any]],
) -> list[str]:
    errors: list[str] = []
    for table in BUSINESS_TABLES:
        cols = TABLE_COLUMNS[table]
        exp_map = {r["ID"]: r for r in expected.get(table, [])}
        act_map = {r["ID"]: r for r in actual.get(table, [])}
        for kid in absent_ids.get(table) or []:
            if kid in act_map:
                errors.append(f"{table} ID {kid}: should be absent/deleted in sink")
        for kid, erow in exp_map.items():
            if kid not in act_map:
                errors.append(f"{table} ID {kid}: missing in sink")
                continue
            arow = act_map[kid]
            errors.extend(compare_rows(erow, arow, table, cols))
        extra = set(act_map) - set(exp_map) - set(absent_ids.get(table) or [])
        for kid in sorted(extra):
            errors.append(f"{table} ID {kid}: unexpected row in sink")
    return errors

"""Strict event match including tombstone-as-delete when tombstones.on.delete=false."""

from __future__ import annotations

from srl_contracts import TABLE_COLUMNS
from srl_fields import compare_rows, materialize_oracle_row_for_table, normalize_row_for_table
from srl_kafka import ParsedBusinessOp
from srl_oracle import ExpectedOp


def _delete_match(exp: ExpectedOp, act: ParsedBusinessOp) -> bool:
    if exp.op != "d" or act.op != "d":
        return False
    if materialize_oracle_row_for_table(exp.table, exp.key) != normalize_row_for_table(
        act.table, act.key
    ):
        return False
    cols = TABLE_COLUMNS[exp.table]
    if exp.before is not None:
        if compare_rows(exp.before, act.before, exp.table, cols):
            return False
    if exp.after is not None and not act.tombstone:
        if compare_rows(exp.after, act.after, exp.table, cols):
            return False
    elif exp.after is None and not act.tombstone and act.after is not None:
        return False
    return True


def ops_match(exp: ExpectedOp, act: ParsedBusinessOp) -> bool:
    if exp.table != act.table or exp.op != act.op:
        return False
    if materialize_oracle_row_for_table(exp.table, exp.key) != normalize_row_for_table(
        act.table, act.key
    ):
        return False
    if exp.op == "d" and (act.tombstone or act.after is None):
        return _delete_match(exp, act)
    cols = TABLE_COLUMNS[exp.table]
    if exp.before is not None:
        if compare_rows(exp.before, act.before, exp.table, cols):
            return False
    elif act.before is not None and exp.op in ("u", "d"):
        return False
    if exp.after is not None:
        if compare_rows(exp.after, act.after, exp.table, cols):
            return False
    elif act.after is not None and exp.op in ("c", "u") and not act.tombstone:
        return False
    return True


def forbidden_leaked(exp: ExpectedOp, act: ParsedBusinessOp) -> bool:
    if exp.committed:
        return False
    if exp.table != act.table or exp.op != act.op:
        return False
    if materialize_oracle_row_for_table(exp.table, exp.key) != normalize_row_for_table(
        act.table, act.key
    ):
        return False
    cols = TABLE_COLUMNS[exp.table]
    if exp.after is not None and not compare_rows(exp.after, act.after, exp.table, cols):
        return False
    if exp.before is not None and not compare_rows(exp.before, act.before, exp.table, cols):
        return False
    return True

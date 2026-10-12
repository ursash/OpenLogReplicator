"""Oracle + final-state qualification (v2)."""

from __future__ import annotations

from typing import Any

from srl_contracts import EXPECTED_OPS_FINAL_STATE_VERSIONS, TABLE_COLUMNS
from srl_fields import oracle_row_complete
from srl_oracle import ExpectedOracle, crosscheck_final_state_vs_dml, oracle_row_valid


def _has_placeholder(obj: Any) -> bool:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "_null_fields":
                continue
            if isinstance(v, str) and "$" in v:
                return True
            if _has_placeholder(v):
                return True
    elif isinstance(obj, list):
        for v in obj:
            if _has_placeholder(v):
                return True
    return False


def _expected_short_committed(n: int) -> int:
    return n - (n // 10)


def qualify_oracle(oracle: ExpectedOracle) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []

    if not oracle.final_state_doc:
        issues.append(
            {
                "code": "ORACLE_MISSING_FINAL_STATE_ARTIFACT",
                "detail": "workload-expected-final-state.json not loaded",
            }
        )
    elif oracle.final_state_doc.get("contract_version") not in EXPECTED_OPS_FINAL_STATE_VERSIONS:
        issues.append(
            {"code": "ORACLE_FINAL_STATE_VERSION", "detail": "final state contract_version mismatch"}
        )

    if not oracle.committed_kafka:
        issues.append({"code": "ORACLE_NO_COMMITTED_DML", "detail": "zero committed DML ops"})

    pin = oracle.pin
    if pin and pin.short_tx_n:
        p103 = [o for o in oracle.committed_kafka if o.phase == "P103"]
        need = _expected_short_committed(pin.short_tx_n)
        if len(p103) < need:
            issues.append(
                {
                    "code": "ORACLE_TRUNCATED",
                    "detail": f"P103 committed {len(p103)} < expected {need} for short_tx_n={pin.short_tx_n}",
                }
            )
        forb_roll = [o for o in oracle.forbidden if o.rollback]
        if len(forb_roll) < 2:
            issues.append(
                {
                    "code": "ORACLE_FORBIDDEN_SET_INCOMPLETE",
                    "detail": "expected rollback forbidden ops (partial + full) missing",
                }
            )

    p101 = [o for o in oracle.committed_kafka if o.phase == "P101"]
    if len(p101) < 6:
        issues.append(
            {"code": "ORACLE_TRUNCATED", "detail": "P101 burst incomplete (<6 committed ops)"}
        )

    for raw in oracle.raw_lines:
        op_id = str(raw.get("op_id", ""))
        note = str(raw.get("note", "")).lower()
        if "no-op" in note or "may be no-op" in note:
            issues.append(
                {
                    "code": "ORACLE_AMBIGUOUS_COMMITTED_DELETE",
                    "detail": f"{op_id}: optional/no-op committed delete",
                }
            )
        if bool(raw.get("committed", True)) and _has_placeholder(raw):
            issues.append(
                {
                    "code": "ORACLE_UNRESOLVED_PLACEHOLDER",
                    "detail": f"{op_id}: $ placeholder in committed row",
                }
            )

    for op in oracle.committed_kafka:
        for err in oracle_row_valid(op):
            issues.append({"code": "ORACLE_INCOMPLETE_ROW", "detail": f"{op.op_id}: {err}"})
        cols = TABLE_COLUMNS[op.table]
        if op.op in ("u", "d") and op.before is not None:
            if not oracle_row_complete(op.before, op.table, cols):
                issues.append(
                    {
                        "code": "ORACLE_INCOMPLETE_BEFORE",
                        "detail": f"{op.op_id}: before missing columns",
                    }
                )
        if op.op in ("c", "u") and op.after is not None:
            if not oracle_row_complete(op.after, op.table, cols):
                issues.append(
                    {
                        "code": "ORACLE_INCOMPLETE_AFTER",
                        "detail": f"{op.op_id}: after missing columns",
                    }
                )

    for op in oracle.forbidden:
        if not op.key.get("ID"):
            issues.append(
                {
                    "code": "ORACLE_FORBIDDEN_SPEC_INCOMPLETE",
                    "detail": f"{op.op_id}: forbidden op missing concrete key.ID",
                }
            )

    for err in crosscheck_final_state_vs_dml(oracle):
        issues.append({"code": "ORACLE_FINAL_STATE_CROSSCHECK", "detail": err})

    notes = (oracle.final_state_doc or {}).get("notes") or {}
    if pin and pin.short_tx_n and notes.get("short_committed_count") is not None:
        if int(notes["short_committed_count"]) != _expected_short_committed(pin.short_tx_n):
            issues.append(
                {
                    "code": "ORACLE_FINAL_STATE_NOTES",
                    "detail": "short_committed_count inconsistent with pin short_tx_n",
                }
            )

    return issues

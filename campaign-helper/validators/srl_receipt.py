"""Workload execution receipts (receipt-v1)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from srl_contracts import RECEIPT_CONTRACT_VERSION, RECEIPT_REQUIRED_FIELDS, RECEIPT_TYPES, RUN_ID

PHASE_ALIASES = {
    "W101": "P101",
    "W102": "P102",
    "W103": "P103",
    "W104": "P104",
    "W105": "P105",
    "W106": "P106",
    "W107": "P107",
    "W099a": "P099",
    "W099b": "P099",
}


def normalize_phase(phase: str) -> str:
    p = str(phase or "").strip()
    return PHASE_ALIASES.get(p, p)


@dataclass
class ExecutionReceipt:
    line_no: int
    run_id: str
    phase: str
    phase_norm: str
    receipt_type: str
    utc: str
    host: str | None
    raw: dict[str, Any]


@dataclass
class ReceiptLoadResult:
    receipts: list[ExecutionReceipt] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def load_execution_receipts(path: str) -> ReceiptLoadResult:
    out = ReceiptLoadResult()
    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                out.errors.append(f"receipt line {line_no}: JSON {e}")
                continue
            if not isinstance(obj, dict):
                out.errors.append(f"receipt line {line_no}: not object")
                continue
            missing = [f for f in RECEIPT_REQUIRED_FIELDS if f not in obj]
            if missing:
                out.errors.append(f"receipt line {line_no}: missing {missing}")
                continue
            rtype = str(obj.get("receipt_type", ""))
            if rtype not in RECEIPT_TYPES:
                out.errors.append(f"receipt line {line_no}: unknown receipt_type {rtype!r}")
                continue
            rid = str(obj["run_id"])
            phase = str(obj["phase"])
            out.receipts.append(
                ExecutionReceipt(
                    line_no=line_no,
                    run_id=rid,
                    phase=phase,
                    phase_norm=normalize_phase(phase),
                    receipt_type=rtype,
                    utc=str(obj["utc"]),
                    host=str(obj["host"]) if obj.get("host") is not None else None,
                    raw=obj,
                )
            )
    if not out.receipts and not out.errors:
        out.errors.append("execution receipts empty")
    return out


def receipt_index(receipts: list[ExecutionReceipt]) -> dict[tuple[str, str], list[ExecutionReceipt]]:
    idx: dict[tuple[str, str], list[ExecutionReceipt]] = {}
    for r in receipts:
        idx.setdefault((r.phase_norm, r.receipt_type), []).append(r)
    return idx

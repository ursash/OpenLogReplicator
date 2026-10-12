"""Workload journal adapter (v1 intent lines — not an acceptance oracle)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from srl_contracts import CONTRACT_VERSION, JOURNAL_REQUIRED_FIELDS, RUN_ID


@dataclass
class JournalSummary:
    contract_version: str
    line_count: int
    run_ids: set[str] = field(default_factory=set)
    ops: dict[str, int] = field(default_factory=dict)
    event_ids: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    has_committed_dml_expectations: bool = False


def load_journal(path: str, contract_version: str) -> JournalSummary:
    summary = JournalSummary(contract_version=contract_version, line_count=0)
    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            summary.line_count += 1
            try:
                ev = json.loads(line)
            except json.JSONDecodeError as e:
                summary.errors.append(f"line {line_no}: {e}")
                continue
            if not isinstance(ev, dict):
                summary.errors.append(f"line {line_no}: not object")
                continue
            for f in JOURNAL_REQUIRED_FIELDS:
                if f not in ev:
                    summary.errors.append(f"line {line_no}: missing {f}")
            rid = ev.get("run_id")
            if rid:
                summary.run_ids.add(str(rid))
            op = ev.get("op")
            if op:
                summary.ops[str(op)] = summary.ops.get(str(op), 0) + 1
            eid = ev.get("event_id")
            if eid:
                summary.event_ids.append(str(eid))
            if ev.get("committed_operations") or ev.get("expected_dml"):
                summary.has_committed_dml_expectations = True

    if summary.run_ids and RUN_ID not in summary.run_ids and len(summary.run_ids) == 1:
        pass
    elif summary.run_ids and RUN_ID not in summary.run_ids:
        summary.errors.append(f"run_id mismatch: expected {RUN_ID}, saw {sorted(summary.run_ids)}")

    return summary

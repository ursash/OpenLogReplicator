"""Expected-operations v2: JSONL DML + separate final_key_state JSON artifact."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from srl_contracts import (
    BUSINESS_TABLES,
    EXCLUDED_DML_PHASES,
    EXPECTED_OPS_CONTRACT_VERSION,
    EXPECTED_OPS_FINAL_STATE_VERSIONS,
    FINAL_STATE_RECORD_TYPE,
    RECORD_FORBIDDEN,
    RECORD_KAFKA,
    RUN_ID,
    TABLE_COLUMNS,
)
from srl_fields import (
    materialize_oracle_row_for_table,
    oracle_row_complete,
)


def table_short(name: str) -> str:
    n = name.upper()
    if n.endswith(".EV") or n == "EV":
        return "EV"
    if n.endswith(".PP") or n == "PP":
        return "PP"
    if "OLR_RAC_TEST.EV" in n:
        return "EV"
    if "OLR_RAC_TEST.PP" in n:
        return "PP"
    return n.split(".")[-1] if "." in n else n


@dataclass
class ExpectedOp:
    op_id: str
    run_id: str
    phase: str
    table: str
    op: str
    committed: bool
    key: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    record_type: str
    seq_on_key: int | None = None
    step: int | None = None
    rollback: str | None = None


@dataclass
class FinalStateEntry:
    table: str
    key: dict[str, Any]
    present: bool
    after: dict[str, Any] | None = None


@dataclass
class ExpectedOracle:
    expected_ops_contract_version: str = EXPECTED_OPS_CONTRACT_VERSION
    run_id: str = ""
    raw_lines: list[dict] = field(default_factory=list)
    lines: list[ExpectedOp] = field(default_factory=list)
    committed_kafka: list[ExpectedOp] = field(default_factory=list)
    forbidden: list[ExpectedOp] = field(default_factory=list)
    final_state_entries: list[FinalStateEntry] = field(default_factory=list)
    final_state_doc: dict[str, Any] | None = None
    absent_ids: dict[str, list[Any]] = field(default_factory=lambda: {"EV": [], "PP": []})
    pin: "OraclePin | None" = None
    errors: list[str] = field(default_factory=list)


@dataclass
class OraclePin:
    run_id: str
    rac1_instance: int
    rac2_instance: int
    mode: str = "smoke"
    base_key: int | None = None
    pp_ctr_before_pingpong: int | None = None
    short_tx_n: int | None = None


def load_oracle_pin(path: str | None) -> OraclePin | None:
    if not path:
        return None
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    return OraclePin(
        run_id=str(doc.get("run_id", RUN_ID)),
        rac1_instance=int(doc["rac1_instance"]),
        rac2_instance=int(doc["rac2_instance"]),
        mode=str(doc.get("mode", "smoke")),
        base_key=doc.get("base_key"),
        pp_ctr_before_pingpong=doc.get("pp_ctr_before_pingpong"),
        short_tx_n=doc.get("short_tx_n"),
    )


def _parse_dml_line(obj: dict) -> ExpectedOp | None:
    if "op_id" not in obj:
        return None
    committed = bool(obj.get("committed", True))
    table = table_short(str(obj.get("table", "")))
    op = str(obj.get("op", "")).lower() if obj.get("op") else ""
    phase = str(obj.get("phase", ""))

    if phase in EXCLUDED_DML_PHASES:
        return None

    rtype = str(obj.get("record_type") or (RECORD_KAFKA if committed else RECORD_FORBIDDEN))

    if committed:
        if table not in BUSINESS_TABLES or op not in ("c", "u", "d"):
            return None
    elif table not in BUSINESS_TABLES and "OLR_RAC_TEST" not in str(obj.get("table", "")):
        return None

    key_raw = obj.get("key") or {}
    key = (
        materialize_oracle_row_for_table(table, key_raw)
        if isinstance(key_raw, dict) and table in BUSINESS_TABLES
        else {}
    )
    before_raw = obj.get("before")
    after_raw = obj.get("after")
    before = before_raw if before_raw is None or isinstance(before_raw, dict) else None
    after = after_raw if after_raw is None or isinstance(after_raw, dict) else None

    seq = obj.get("seq_on_key")
    step = obj.get("step")

    return ExpectedOp(
        op_id=str(obj["op_id"]),
        run_id=str(obj.get("run_id", RUN_ID)),
        phase=phase,
        table=table if table in BUSINESS_TABLES else table_short(str(obj.get("table", ""))),
        op=op,
        committed=committed,
        key=key or {},
        before=before,
        after=after,
        record_type=rtype,
        seq_on_key=int(seq) if seq is not None else None,
        step=int(step) if step is not None else None,
        rollback=str(obj["rollback"]) if obj.get("rollback") else None,
    )


def load_expected_oracle(path: str, pin_path: str | None = None) -> ExpectedOracle:
    oracle = ExpectedOracle()
    oracle.pin = load_oracle_pin(pin_path)

    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                oracle.errors.append(f"line {line_no}: {e}")
                continue
            if not isinstance(obj, dict):
                oracle.errors.append(f"line {line_no}: not object")
                continue
            oracle.raw_lines.append(obj)
            op = _parse_dml_line(obj)
            if op is None:
                if str(obj.get("phase", "")) not in EXCLUDED_DML_PHASES:
                    oracle.errors.append(f"line {line_no}: invalid {obj.get('op_id')!r}")
                continue
            oracle.lines.append(op)
            if op.run_id:
                oracle.run_id = op.run_id
            if not op.committed or op.record_type == RECORD_FORBIDDEN:
                oracle.forbidden.append(op)
            else:
                oracle.committed_kafka.append(op)

    if not oracle.raw_lines:
        oracle.errors.append("expected-events empty")

    if oracle.run_id and oracle.run_id != RUN_ID:
        oracle.errors.append(f"run_id {oracle.run_id!r} != pinned {RUN_ID}")

    return oracle


def load_expected_final_state(path: str | None, oracle: ExpectedOracle) -> ExpectedOracle:
    if not path:
        oracle.errors.append("missing --expected-final-state (required for expected-ops-v2)")
        return oracle
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except json.JSONDecodeError as e:
        oracle.errors.append(f"final_state JSON parse: {e}")
        return oracle
    oracle.final_state_doc = doc
    cv = str(doc.get("contract_version", ""))
    if cv not in EXPECTED_OPS_FINAL_STATE_VERSIONS:
        oracle.errors.append(
            f"final_state contract_version {cv!r} not in {EXPECTED_OPS_FINAL_STATE_VERSIONS}"
        )
    if doc.get("record_type") != FINAL_STATE_RECORD_TYPE:
        oracle.errors.append(
            f"final_state record_type must be {FINAL_STATE_RECORD_TYPE!r}"
        )
    for i, entry in enumerate(doc.get("entries") or []):
        if not isinstance(entry, dict):
            oracle.errors.append(f"final_state entries[{i}] not object")
            continue
        table = table_short(str(entry.get("table", "")))
        key = materialize_oracle_row_for_table(table, entry.get("key") or {}) or {}
        present = bool(entry.get("present", True))
        after_raw = entry.get("after")
        after = after_raw if isinstance(after_raw, dict) else None
        if present and after is None:
            oracle.errors.append(f"final_state entries[{i}]: present=true requires after")
        rec = FinalStateEntry(table=table, key=key, present=present, after=after)
        oracle.final_state_entries.append(rec)
        kid = key.get("ID")
        if kid is not None and not present:
            oracle.absent_ids.setdefault(table, []).append(kid)
    if not oracle.final_state_entries:
        oracle.errors.append("final_state entries empty")
    return oracle


def build_expected_final_from_records(oracle: ExpectedOracle) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {t: [] for t in BUSINESS_TABLES}
    for entry in oracle.final_state_entries:
        if not entry.present or entry.table not in BUSINESS_TABLES:
            continue
        if entry.after:
            row = materialize_oracle_row_for_table(entry.table, entry.after)
            if row:
                out[entry.table].append(row)
    for table in BUSINESS_TABLES:
        out[table] = sorted(out[table], key=lambda r: r.get("ID"))
    return out


def reconstruct_final_from_dml(ops: list[ExpectedOp]) -> dict[str, dict[Any, dict[str, Any] | None]]:
    state: dict[str, dict[Any, dict[str, Any] | None]] = {t: {} for t in BUSINESS_TABLES}
    for op in sorted(ops, key=lambda o: o.op_id):
        if op.table not in BUSINESS_TABLES:
            continue
        kid = op.key.get("ID")
        if kid is None:
            continue
        if op.op == "d":
            state[op.table][kid] = None
        elif op.op in ("c", "u") and op.after is not None:
            row = materialize_oracle_row_for_table(op.table, op.after)
            state[op.table][kid] = row
    return state


def crosscheck_final_state_vs_dml(oracle: ExpectedOracle) -> list[str]:
    errors: list[str] = []
    seen: set[tuple[str, Any]] = set()
    for entry in oracle.final_state_entries:
        kid = entry.key.get("ID")
        sig = (entry.table, kid)
        if sig in seen:
            errors.append(f"duplicate final_state key {sig}")
        seen.add(sig)
        if entry.table not in BUSINESS_TABLES:
            errors.append(f"unknown final_state table {entry.table}")
            continue
        if entry.present and not entry.after:
            errors.append(f"final_state present=true missing after for {sig}")
    rebuilt = reconstruct_final_from_dml(oracle.committed_kafka)
    for entry in oracle.final_state_entries:
        if entry.table not in BUSINESS_TABLES:
            continue
        kid = entry.key.get("ID")
        got = rebuilt[entry.table].get(kid)
        if not entry.present:
            if got is not None:
                errors.append(f"final_state absent but DML rebuild has {entry.table} ID {kid}")
            continue
        exp_row = materialize_oracle_row_for_table(entry.table, entry.after or {})
        if exp_row != got:
            errors.append(
                f"final_state/DML crosscheck {entry.table} ID {kid}: artifact != rebuild from committed ops"
            )

    for table in BUSINESS_TABLES:
        live_rebuilt = {k for k, v in rebuilt[table].items() if v is not None}
        live_final = {
            e.key.get("ID")
            for e in oracle.final_state_entries
            if e.table == table and e.present and e.key.get("ID") is not None
        }
        absent_final = {
            e.key.get("ID")
            for e in oracle.final_state_entries
            if e.table == table and not e.present and e.key.get("ID") is not None
        }
        if live_rebuilt != live_final:
            errors.append(
                f"final_state coverage {table}: present keys {sorted(live_final)} != DML rebuild {sorted(live_rebuilt)}"
            )
        if live_rebuilt & absent_final:
            errors.append(f"final_state {table}: key both absent and present in rebuild")
    return errors


def oracle_row_valid(op: ExpectedOp) -> list[str]:
    errs: list[str] = []
    cols = TABLE_COLUMNS.get(op.table, ())
    if not cols:
        return errs
    if op.op in ("u", "d") and op.before is not None:
        if not oracle_row_complete(op.before, op.table, cols):
            errs.append("before incomplete")
    if op.op in ("c", "u") and op.after is not None:
        if not oracle_row_complete(op.after, op.table, cols):
            errs.append("after incomplete")
    return errs

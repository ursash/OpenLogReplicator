"""Stock37 Debezium source metadata (db/schema/table/txId/scn); no required source.thread."""

from __future__ import annotations

import json
from typing import Any

from mandatory_schema import SOURCE_STOCK37_REQUIRED, issue, missing_keys
from srl_contracts import REQUIRED_SOURCE_SCHEMA
from srl_kafka import ParsedBusinessOp


def _envelope(value_raw: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    try:
        val = json.loads(value_raw)
    except json.JSONDecodeError:
        return None, None
    if isinstance(val, dict) and "payload" in val and isinstance(val["payload"], dict):
        val = val["payload"]
    if not isinstance(val, dict):
        return None, None
    return val, val.get("source") if isinstance(val.get("source"), dict) else None


def _table_matches(exp_table: str, source_table: Any) -> bool:
    if source_table is None:
        return False
    t = str(source_table).strip()
    if not t:
        return False
    if t.upper() == exp_table.upper():
        return True
    if "." in t:
        return False
    return False


def verify_envelope_source_stock37(
    capture_rows: list[dict[str, Any]],
    business_ops: list[ParsedBusinessOp],
) -> list[dict[str, str]]:
    failures: list[dict[str, str]] = []
    row_by_offset: dict[tuple[str, int, int], dict] = {}
    for r in capture_rows:
        row_by_offset[(r["topic"], int(r["partition"]), int(r["kafka_offset"]))] = r

    for op in business_ops:
        if op.snapshot or op.tombstone:
            continue
        row = row_by_offset.get((op.topic, op.partition, op.kafka_offset))
        if not row:
            continue
        val, src = _envelope(row.get("value_raw") or "")
        if val is None:
            failures.append(
                issue("SOURCE_ENVELOPE_MISSING", f"offset {op.kafka_offset}: invalid value_raw JSON")
            )
            continue
        if src is None:
            failures.append(
                issue("SOURCE_ENVELOPE_MISSING", f"offset {op.kafka_offset}: missing source object")
            )
            continue
        miss = missing_keys(src, SOURCE_STOCK37_REQUIRED)
        if miss:
            code = "SOURCE_TABLE_MISSING" if "table" in miss else "SOURCE_METADATA_MISSING"
            failures.append(
                issue(code, f"offset {op.kafka_offset}: source missing {miss}")
            )
            continue
        schema = src.get("schema")
        if not schema or str(schema).strip() != REQUIRED_SOURCE_SCHEMA:
            failures.append(
                issue(
                    "SOURCE_SCHEMA_MISMATCH",
                    f"offset {op.kafka_offset}: schema {schema!r} != {REQUIRED_SOURCE_SCHEMA}",
                )
            )
        table = src.get("table")
        if not _table_matches(op.table, table):
            failures.append(
                issue(
                    "SOURCE_TABLE_MISMATCH",
                    f"offset {op.kafka_offset}: source.table {table!r} != {op.table} (suffix/OTHER not allowed)",
                )
            )
        for fld in ("txId", "scn"):
            if src.get(fld) in (None, ""):
                failures.append(
                    issue("SOURCE_METADATA_MISSING", f"offset {op.kafka_offset}: source.{fld} empty")
                )
    return failures

"""Parse Kafka raw capture JSONL per KAFKA-RAW-CAPTURE-CONTRACT v1."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterator

from srl_contracts import (
    BUSINESS_TABLES,
    CAPTURE_ROW_REQUIRED,
    LANE_REGEX,
    TOMBSTONES_ON_DELETE_DEFAULT,
)
from srl_fields import normalize_row_for_table


class ParseError(Exception):
    def __init__(self, message: str, line_no: int | None = None):
        self.line_no = line_no
        super().__init__(message)


@dataclass
class ParsedBusinessOp:
    line_no: int
    topic: str
    partition: int
    kafka_offset: int
    table: str
    op: str
    key: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    tombstone: bool = False
    snapshot: bool = False


@dataclass
class CaptureParseResult:
    business_ops: list[ParsedBusinessOp] = field(default_factory=list)
    tombstones: int = 0
    snapshots: int = 0
    transaction_metadata: int = 0
    schema_history: int = 0
    connect_internal: int = 0
    other_control: int = 0
    lane_filtered: int = 0
    transport_tombstones: int = 0
    malformed_lines: list[str] = field(default_factory=list)
    rows_by_topic_partition: dict[tuple[str, int], list[int]] = field(default_factory=dict)


def _lane_pattern(lane: str) -> re.Pattern[str]:
    if lane not in LANE_REGEX:
        raise ValueError(f"unknown lane {lane!r}")
    return re.compile(LANE_REGEX[lane])


def _table_from_topic(topic: str) -> str | None:
    if topic.endswith(".OLR_RAC_TEST.EV"):
        return "EV"
    if topic.endswith(".OLR_RAC_TEST.PP"):
        return "PP"
    return None


def _classify_topic(topic: str, prefix_lane: str) -> str:
    if topic.endswith(".transaction"):
        return "transaction"
    if topic.endswith(".schema-history"):
        return "schema_history"
    for suf in (".connect-offsets", ".connect-configs", ".connect-status"):
        if topic.endswith(suf):
            return "connect_internal"
    if ".OLR_RAC_TEST." in topic:
        return "business"
    return "other"


def _unwrap_debezium_json(raw: str | None) -> dict[str, Any] | None:
    if raw is None or raw == "":
        return None
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ParseError(f"invalid JSON in key/value: {e}") from e
    if isinstance(obj, dict) and "payload" in obj and isinstance(obj["payload"], dict):
        return obj["payload"]
    return obj if isinstance(obj, dict) else None


def _parse_key(key_raw: str | None, table: str) -> dict[str, Any]:
    payload = _unwrap_debezium_json(key_raw)
    if not payload:
        raise ParseError("missing or empty key_raw for business row")
    row = normalize_row_for_table(table, payload) or payload
    if not row:
        raise ParseError("empty key after parse")
    return row


def parse_capture_line(
    line: str,
    line_no: int,
    lane: str,
    lane_re: re.Pattern[str],
) -> tuple[str, ParsedBusinessOp | None]:
    """Returns (classification, business_op or None)."""
    try:
        row = json.loads(line)
    except json.JSONDecodeError as e:
        raise ParseError(f"line not JSON: {e}", line_no) from e
    if not isinstance(row, dict):
        raise ParseError("capture row must be object", line_no)

    missing = [f for f in CAPTURE_ROW_REQUIRED if f not in row]
    if missing:
        if missing == ["headers_count"] and "headers" in row:
            row = dict(row)
            row["headers_count"] = row["headers"]
            missing = [f for f in CAPTURE_ROW_REQUIRED if f not in row]
    if missing:
        raise ParseError(f"missing fields: {', '.join(missing)}", line_no)

    topic = row["topic"]
    if not lane_re.match(topic):
        return ("lane_filtered", None)

    kind = _classify_topic(topic, lane)
    if kind == "transaction":
        return ("transaction", None)
    if kind == "schema_history":
        return ("schema_history", None)
    if kind == "connect_internal":
        return ("connect_internal", None)
    if kind != "business":
        return ("other", None)

    table = _table_from_topic(topic)
    if table not in BUSINESS_TABLES:
        return ("other", None)

    partition = int(row["partition"])
    offset = int(row["kafka_offset"])
    value_raw = row.get("value_raw")

    if value_raw is None or value_raw == "":
        return (
            "tombstone",
            ParsedBusinessOp(
                line_no=line_no,
                topic=topic,
                partition=partition,
                kafka_offset=offset,
                table=table,
                op="d",
                key=_parse_key(row.get("key_raw"), table),
                before=None,
                after=None,
                tombstone=True,
            ),
        )

    try:
        val = _unwrap_debezium_json(value_raw)
    except ParseError:
        raise
    if not val:
        raise ParseError("empty value envelope", line_no)

    op = val.get("op")
    if op not in ("c", "u", "d"):
        raise ParseError(f"business row missing op c/u/d, got {op!r}", line_no)

    source = val.get("source") or {}
    snapshot = str(source.get("snapshot", "false")).lower() in ("true", "last", "initial")
    before = normalize_row_for_table(table, val.get("before"))
    after = normalize_row_for_table(table, val.get("after"))
    key = _parse_key(row.get("key_raw"), table)

    return (
        "business",
        ParsedBusinessOp(
            line_no=line_no,
            topic=topic,
            partition=partition,
            kafka_offset=offset,
            table=table,
            op=op,
            key=key,
            before=before,
            after=after,
            tombstone=False,
            snapshot=snapshot,
        ),
    )


def load_lane_capture_rows(path: str, lane: str) -> list[dict[str, Any]]:
    lane_re = _lane_pattern(lane)
    rows: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            topic = row.get("topic")
            if topic and lane_re.match(topic):
                rows.append(row)
    return rows


def load_kafka_capture(path: str, lane: str) -> CaptureParseResult:
    lane_re = _lane_pattern(lane)
    result = CaptureParseResult()
    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                kind, op = parse_capture_line(line, line_no, lane, lane_re)
            except ParseError as e:
                loc = f"line {e.line_no or line_no}"
                result.malformed_lines.append(f"{loc}: {e}")
                continue

            if kind == "lane_filtered":
                result.lane_filtered += 1
                continue
            if kind == "transaction":
                result.transaction_metadata += 1
                continue
            if kind == "schema_history":
                result.schema_history += 1
                continue
            if kind == "connect_internal":
                result.connect_internal += 1
                continue
            if kind == "other":
                result.other_control += 1
                continue
            if kind == "tombstone":
                result.tombstones += 1
                if not TOMBSTONES_ON_DELETE_DEFAULT:
                    result.transport_tombstones += 1
                elif op:
                    result.business_ops.append(op)
                continue
            if kind == "business" and op:
                if op.snapshot:
                    result.snapshots += 1
                result.business_ops.append(op)
                tp = (op.topic, op.partition)
                result.rows_by_topic_partition.setdefault(tp, []).append(op.kafka_offset)

    if result.malformed_lines:
        raise ParseError(
            "malformed capture rows: " + "; ".join(result.malformed_lines[:5])
            + (f" (+{len(result.malformed_lines)-5} more)" if len(result.malformed_lines) > 5 else "")
        )
    return result


def iter_raw_lines(path: str) -> Iterator[tuple[int, str]]:
    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            s = line.strip()
            if s:
                yield line_no, s

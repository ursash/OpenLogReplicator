"""KafkaRawCapture.java manifest validation (partitions[], gaps allowed)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from srl_contracts import LANE_REGEX, REQUIRED_CAPTURE_ISOLATION


@dataclass
class PartitionManifest:
    topic: str
    partition: int
    beginning: int
    end: int
    final_position: int
    captured: int
    first_offset: int
    last_offset: int
    exclusive_end_reached: bool = False


@dataclass
class CaptureManifest:
    complete: bool
    records: int
    isolation_level: str | None
    partitions: list[PartitionManifest] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class JsonlCaptureIndex:
    row_count: int
    by_tp: dict[tuple[str, int], list[int]] = field(default_factory=dict)
    duplicates: list[tuple[str, int, int]] = field(default_factory=list)


def load_capture_manifest(path: str) -> CaptureManifest:
    out = CaptureManifest(complete=False, records=0, isolation_level=None)
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    out.complete = bool(doc.get("complete"))
    out.records = int(doc.get("records") or 0)
    out.isolation_level = doc.get("isolation_level") or doc.get("isolation_level_configured")
    parts = doc.get("partitions")
    if not isinstance(parts, list):
        out.errors.append("manifest missing partitions[] array")
        return out
    for i, p in enumerate(parts):
        if not isinstance(p, dict):
            out.errors.append(f"partitions[{i}] not object")
            continue
        try:
            end_val = p.get("end", p.get("end_offset_exclusive"))
            captured_val = p.get("captured", p.get("captured_count"))
            beginning_val = p.get("beginning", p.get("beginning_offset", 0))
            if end_val is None or captured_val is None:
                raise KeyError("end/end_offset_exclusive or captured/captured_count")
            out.partitions.append(
                PartitionManifest(
                    topic=str(p["topic"]),
                    partition=int(p["partition"]),
                    beginning=int(beginning_val),
                    end=int(end_val),
                    final_position=int(p["final_position"]),
                    captured=int(captured_val),
                    first_offset=int(p["first_offset"]),
                    last_offset=int(p["last_offset"]),
                    exclusive_end_reached=bool(p.get("exclusive_end_reached", p.get("exclusive_end", False))),
                )
            )
        except (KeyError, TypeError, ValueError) as e:
            out.errors.append(f"partitions[{i}] invalid: {e}")
    return out


def index_jsonl_capture(path: str, lane: str) -> JsonlCaptureIndex:
    import re

    lane_re = re.compile(LANE_REGEX[lane])
    idx = JsonlCaptureIndex(row_count=0)
    seen: set[tuple[str, int, int]] = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            topic = row.get("topic")
            if not topic or not lane_re.match(topic):
                continue
            idx.row_count += 1
            tp = (topic, int(row["partition"]))
            off = int(row["kafka_offset"])
            coord = (topic, int(row["partition"]), off)
            if coord in seen:
                idx.duplicates.append(coord)
            seen.add(coord)
            idx.by_tp.setdefault(tp, []).append(off)
    return idx


def verify_capture_manifest_against_jsonl(
    manifest: CaptureManifest,
    jsonl_index: JsonlCaptureIndex,
    *,
    lane: str,
    require_isolation: bool = True,
) -> list[dict[str, str]]:
    failures: list[dict[str, str]] = []
    if manifest.errors:
        for e in manifest.errors:
            failures.append({"code": "CAPTURE_MANIFEST_PARSE", "detail": e})
        return failures

    if not manifest.partitions:
        failures.append({"code": "CAPTURE_MANIFEST_PARSE", "detail": "partitions[] empty"})
        return failures

    if not manifest.complete:
        failures.append(
            {"code": "CAPTURE_INCOMPLETE", "detail": "manifest complete=false"}
        )
    if manifest.records <= 0:
        failures.append(
            {"code": "CAPTURE_ZERO_RECORDS", "detail": "manifest records=0"}
        )
    if require_isolation:
        if manifest.isolation_level is None:
            failures.append(
                {
                    "code": "CAPTURE_ISOLATION_UNKNOWN",
                    "detail": f"manifest missing isolation_level (required {REQUIRED_CAPTURE_ISOLATION})",
                }
            )
        elif manifest.isolation_level != REQUIRED_CAPTURE_ISOLATION:
            failures.append(
                {
                    "code": "CAPTURE_ISOLATION",
                    "detail": f"isolation {manifest.isolation_level!r} != {REQUIRED_CAPTURE_ISOLATION}",
                }
            )

    if jsonl_index.duplicates:
        failures.append(
            {
                "code": "CAPTURE_DUPLICATE_COORD",
                "detail": f"duplicate topic/partition/offset: {jsonl_index.duplicates[:3]}",
            }
        )

    manifest_rows = sum(p.captured for p in manifest.partitions)
    if manifest.records != jsonl_index.row_count:
        failures.append(
            {
                "code": "CAPTURE_COUNT_MISMATCH",
                "detail": f"manifest records={manifest.records} jsonl lane rows={jsonl_index.row_count}",
            }
        )

    for p in manifest.partitions:
        tp = (p.topic, p.partition)
        offs = sorted(jsonl_index.by_tp.get(tp, []))
        if p.captured != len(offs):
            failures.append(
                {
                    "code": "CAPTURE_PARTITION_COUNT",
                    "detail": f"{p.topic} p{p.partition}: manifest captured={p.captured} jsonl={len(offs)}",
                }
            )
        if p.final_position != p.end:
            failures.append(
                {
                    "code": "CAPTURE_END_NOT_REACHED",
                    "detail": f"{p.topic} p{p.partition}: final_position {p.final_position} != end {p.end}",
                }
            )
        if p.captured > 0:
            if p.first_offset < 0 or p.last_offset < p.first_offset:
                failures.append(
                    {
                        "code": "CAPTURE_OFFSET_RANGE",
                        "detail": f"{p.topic} p{p.partition}: invalid first/last offset",
                    }
                )
            if offs and (offs[0] != p.first_offset or offs[-1] != p.last_offset):
                failures.append(
                    {
                        "code": "CAPTURE_OFFSET_BOUNDS",
                        "detail": f"{p.topic} p{p.partition}: jsonl bounds mismatch manifest first/last",
                    }
                )
            for o in offs:
                if o < p.first_offset or o > p.last_offset:
                    failures.append(
                        {
                            "code": "CAPTURE_OFFSET_OUT_OF_RANGE",
                            "detail": f"{p.topic} p{p.partition}: offset {o} outside [{p.first_offset},{p.last_offset}]",
                        }
                    )
        if (
            p.end > p.beginning
            and p.captured == 0
            and p.final_position == p.end
            and not p.exclusive_end_reached
        ):
            failures.append(
                {
                    "code": "CAPTURE_EMPTY_PARTITION",
                    "detail": f"{p.topic} p{p.partition}: at end but captured=0",
                }
            )

    return failures

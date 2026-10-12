"""Capture manifest + finite offset range completeness."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class CoverageSpec:
    topic: str
    partition: int
    start_offset: int
    end_offset_inclusive: int


@dataclass
class ManifestCheck:
    errors: list[str] = field(default_factory=list)
    specs: list[CoverageSpec] = field(default_factory=list)


def _partition_range(obj: dict) -> tuple[int, int] | None:
    start = obj.get("start_offset", obj.get("low", obj.get("begin")))
    end = obj.get("end_offset_inclusive", obj.get("high", obj.get("end")))
    if start is None or end is None:
        return None
    return int(start), int(end)


def load_coverage_from_manifest(path: str) -> ManifestCheck:
    check = ManifestCheck()
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    topics = doc.get("topics") or doc.get("partitions") or doc
    if not isinstance(topics, dict):
        check.errors.append("manifest missing topics object")
        return check
    for topic, part_map in topics.items():
        if not isinstance(part_map, dict):
            continue
        for pstr, rng in part_map.items():
            if not isinstance(rng, dict):
                continue
            parsed = _partition_range(rng)
            if not parsed:
                check.errors.append(f"manifest {topic} p{pstr}: missing offset range")
                continue
            start, end = parsed
            check.specs.append(
                CoverageSpec(
                    topic=str(topic),
                    partition=int(pstr),
                    start_offset=start,
                    end_offset_inclusive=end,
                )
            )
    if not check.specs:
        check.errors.append("manifest produced zero partition ranges")
    return check


def load_coverage_from_oracle(capture_coverage: dict | None) -> ManifestCheck:
    check = ManifestCheck()
    if not capture_coverage:
        check.errors.append("oracle capture_coverage missing")
        return check
    topics = capture_coverage.get("topics") or {}
    for topic, part_map in topics.items():
        partitions = part_map.get("partitions") if isinstance(part_map, dict) else part_map
        if not isinstance(partitions, dict):
            check.errors.append(f"oracle coverage bad shape for {topic}")
            continue
        for pstr, rng in partitions.items():
            if not isinstance(rng, dict):
                continue
            parsed = _partition_range(rng)
            if not parsed:
                check.errors.append(f"oracle coverage {topic} p{pstr}: missing range")
                continue
            start, end = parsed
            check.specs.append(
                CoverageSpec(
                    topic=str(topic),
                    partition=int(pstr),
                    start_offset=start,
                    end_offset_inclusive=end,
                )
            )
    return check


def verify_capture_completeness(
    specs: list[CoverageSpec],
    offsets_seen: dict[tuple[str, int], set[int]],
) -> list[str]:
    errors: list[str] = []
    for spec in specs:
        tp = (spec.topic, spec.partition)
        seen = offsets_seen.get(tp, set())
        expected = set(range(spec.start_offset, spec.end_offset_inclusive + 1))
        missing = sorted(expected - seen)
        extra = sorted(seen - expected)
        if missing:
            errors.append(
                f"{spec.topic} p{spec.partition}: missing offsets "
                f"{missing[:5]}{'...' if len(missing)>5 else ''} "
                f"(range {spec.start_offset}-{spec.end_offset_inclusive})"
            )
        if extra:
            errors.append(
                f"{spec.topic} p{spec.partition}: offsets outside range: {extra[:5]}"
            )
    return errors


def collect_all_lane_offsets(path: str, lane_re) -> dict[tuple[str, int], set[int]]:
    import re

    offsets: dict[tuple[str, int], set[int]] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            topic = row.get("topic")
            if not topic or not lane_re.match(topic):
                continue
            tp = (topic, int(row["partition"]))
            offsets.setdefault(tp, set()).add(int(row["kafka_offset"]))
    return offsets

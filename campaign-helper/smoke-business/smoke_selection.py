"""Offset-bound smoke selection (never filter by expected-op match)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from smoke_slice_guards import SMOKE_SLICE_VERSION, SMOKE_SLICE_VERSION_LEGACY, strict_int


@dataclass(frozen=True)
class OffsetBound:
    topic: str
    partition: int
    begin_offset_inclusive: int
    end_offset_exclusive: int

    def contains(self, topic: str, partition: int, offset: int) -> bool:
        return (
            self.topic == topic
            and self.partition == partition
            and self.begin_offset_inclusive <= offset < self.end_offset_exclusive
        )


@dataclass
class SmokeSlice:
    doc: dict[str, Any]
    lane: str
    measurement_bounds: list[OffsetBound]
    p099_claimed_coords: set[tuple[str, int, int]] = field(default_factory=set)
    errors: list[str] = field(default_factory=list)


@dataclass
class SelectionAccounting:
    selected_coords: list[dict[str, Any]] = field(default_factory=list)
    excluded_coords: list[dict[str, Any]] = field(default_factory=list)
    p099_identified_coords: list[dict[str, Any]] = field(default_factory=list)
    full_lane_business_count: int = 0


def load_smoke_slice(path: str) -> SmokeSlice:
    errors: list[str] = []
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    cv = str(doc.get("contract_version", ""))
    if cv not in (SMOKE_SLICE_VERSION, SMOKE_SLICE_VERSION_LEGACY):
        errors.append(f"smoke_slice contract_version must be {SMOKE_SLICE_VERSION}")
    if cv == SMOKE_SLICE_VERSION_LEGACY:
        errors.append("smoke_slice v1 rejected: use v2 with cut/manifest bindings")
    lane = str(doc.get("lane", ""))
    if lane not in ("baseline", "recovery"):
        errors.append("smoke_slice lane must be baseline|recovery")
    bounds_raw = doc.get("smoke_measurement_bounds")
    bounds: list[OffsetBound] = []
    if not isinstance(bounds_raw, list) or not bounds_raw:
        errors.append("smoke_measurement_bounds[] required")
    else:
        for i, b in enumerate(bounds_raw):
            if not isinstance(b, dict):
                errors.append(f"smoke_measurement_bounds[{i}] not object")
                continue
            try:
                begin, e1 = strict_int(b.get("begin_offset_inclusive"), field="begin_offset_inclusive")
                end_ex, e2 = strict_int(b.get("end_offset_exclusive"), field="end_offset_exclusive")
                if e1 or e2:
                    errors.append(f"smoke_measurement_bounds[{i}]: {e1 or e2}")
                    continue
                assert begin is not None and end_ex is not None
                bounds.append(
                    OffsetBound(
                        topic=str(b["topic"]),
                        partition=int(b["partition"]),
                        begin_offset_inclusive=begin,
                        end_offset_exclusive=end_ex,
                    )
                )
            except (KeyError, TypeError, ValueError) as e:
                errors.append(f"smoke_measurement_bounds[{i}] invalid: {e}")
            if bounds and bounds[-1].begin_offset_inclusive >= bounds[-1].end_offset_exclusive:
                errors.append(f"smoke_measurement_bounds[{i}] empty or inverted range")
    p099: set[tuple[str, int, int]] = set()
    prep = doc.get("p099_prep")
    if prep is not None:
        if not isinstance(prep, dict):
            errors.append("p099_prep must be object")
        else:
            coords = prep.get("identified_coords")
            if isinstance(coords, list):
                for j, c in enumerate(coords):
                    try:
                        off, err = strict_int(c.get("offset"), field="offset")
                        if err:
                            errors.append(f"p099_prep.identified_coords[{j}]: {err}")
                            continue
                        assert off is not None
                        p099.add((str(c["topic"]), int(c["partition"]), off))
                    except (KeyError, TypeError, ValueError):
                        errors.append(f"p099_prep.identified_coords[{j}] invalid")
    return SmokeSlice(doc=doc, lane=lane, measurement_bounds=bounds, p099_claimed_coords=p099, errors=errors)


def coord_in_measurement_bounds(
    topic: str, partition: int, offset: int, bounds: list[OffsetBound]
) -> bool:
    return any(b.contains(topic, partition, offset) for b in bounds)


def partition_manifest_ranges(manifest) -> dict[tuple[str, int], tuple[int, int]]:
    out: dict[tuple[str, int], tuple[int, int]] = {}
    for p in manifest.partitions:
        lo = min(p.first_offset, p.beginning)
        hi = max(p.last_offset, p.end - 1)
        out[(p.topic, p.partition)] = (lo, hi)
    return out


def classify_business_coord(
    topic: str,
    partition: int,
    offset: int,
    *,
    slice_doc: SmokeSlice,
    manifest_ranges: dict[tuple[str, int], tuple[int, int]],
    verified_p099: set[tuple[str, int, int]],
) -> tuple[str, str]:
    coord = (topic, partition, offset)
    if coord in verified_p099:
        return ("p099_identified", "p099_prep_verified")
    if coord in slice_doc.p099_claimed_coords and coord not in verified_p099:
        return ("excluded", "p099_claim_unverified_not_prep")
    if coord_in_measurement_bounds(topic, partition, offset, slice_doc.measurement_bounds):
        return ("selected", "inside_smoke_measurement_bounds")
    tp = (topic, partition)
    if tp in manifest_ranges:
        lo, hi = manifest_ranges[tp]
        if lo <= offset <= hi:
            return ("excluded", "inside_full_capture_outside_smoke_slice")
    return ("excluded", "outside_manifest_partition_range")


def apply_selection_to_business_ops(
    business_ops,
    *,
    slice_doc: SmokeSlice,
    manifest,
    verified_p099: set[tuple[str, int, int]],
) -> tuple[list, SelectionAccounting]:
    acc = SelectionAccounting()
    acc.full_lane_business_count = len(business_ops)
    ranges = partition_manifest_ranges(manifest)
    selected = []
    for op in business_ops:
        kind, reason = classify_business_coord(
            op.topic,
            op.partition,
            op.kafka_offset,
            slice_doc=slice_doc,
            manifest_ranges=ranges,
            verified_p099=verified_p099,
        )
        entry = {
            "topic": op.topic,
            "partition": op.partition,
            "offset": op.kafka_offset,
            "table": op.table,
            "op": op.op,
            "reason": reason,
        }
        if kind == "selected":
            selected.append(op)
            acc.selected_coords.append(entry)
        elif kind == "p099_identified":
            acc.p099_identified_coords.append(entry)
        else:
            acc.excluded_coords.append(entry)
    return selected, acc

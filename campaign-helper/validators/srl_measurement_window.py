"""MEASUREMENT-WINDOW.json (measure-v1) strict validation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from mandatory_schema import LANE_SLOT_REQUIRED_KEYS, MW_REQUIRED_TOP_KEYS, issue, missing_keys
from srl_contracts import MEASUREMENT_WINDOW_VERSION, RUN_ID
from srl_dates import parse_utc_strict


@dataclass
class MeasurementWindow:
    doc: dict[str, Any]
    run_id: str
    contract_version: str
    errors: list[str] = field(default_factory=list)


def load_measurement_window(path: str) -> MeasurementWindow:
    errors: list[str] = []
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    miss = missing_keys(doc, MW_REQUIRED_TOP_KEYS)
    if miss:
        errors.extend(f"missing top-level {miss}")
    cv = str(doc.get("contract_version", ""))
    if cv != MEASUREMENT_WINDOW_VERSION:
        errors.append(f"measurement_window contract_version {cv!r} != {MEASUREMENT_WINDOW_VERSION}")
    if str(doc.get("run_id", "")) != RUN_ID:
        errors.append(f"measurement_window run_id mismatch")
    excluded = doc.get("source_phases_excluded") or []
    if "P099" not in excluded:
        errors.append("source_phases_excluded must include P099")
    measured = doc.get("source_phases_in_oracle") or []
    if "P099" in measured:
        errors.append("P099 must not appear in source_phases_in_oracle")
    scope = doc.get("consumer_scope")
    if not isinstance(scope, dict):
        errors.append("consumer_scope object required")
    elif scope.get("isolation") != "read_committed":
        errors.append("consumer_scope.isolation must be read_committed")
    try:
        parse_utc_strict(str(doc.get("source_measured_dml_from_utc", "")), field="source_measured_dml_from_utc")
    except ValueError as e:
        errors.append(str(e))
    lanes = doc.get("lanes")
    if not isinstance(lanes, dict):
        errors.append("lanes object required")
    else:
        for lane in ("baseline", "recovery"):
            slot = lanes.get(lane)
            if not isinstance(slot, dict):
                errors.append(f"lanes.{lane} required")
                continue
            miss_lane = missing_keys(slot, LANE_SLOT_REQUIRED_KEYS)
            if miss_lane:
                errors.append(f"lanes.{lane} missing {miss_lane}")
                continue
            try:
                start = parse_utc_strict(str(slot["consumer_start_utc"]), field=f"lanes.{lane}.consumer_start_utc")
                catch = parse_utc_strict(
                    str(slot["catch_up_after_p099_utc"]), field=f"lanes.{lane}.catch_up_after_p099_utc"
                )
            except ValueError as e:
                errors.append(str(e))
                continue
            if catch > start:
                errors.append(f"lanes.{lane}: catch_up_after_p099_utc after consumer_start_utc")
            pos = slot.get("p099_catch_up_position")
            if not isinstance(pos, dict):
                errors.append(f"lanes.{lane}.p099_catch_up_position object required")
    parts = doc.get("partition_capture_bounds")
    if not isinstance(parts, list) or not parts:
        errors.append("partition_capture_bounds[] required")
    else:
        for i, p in enumerate(parts):
            if not isinstance(p, dict):
                errors.append(f"partition_capture_bounds[{i}] not object")
                continue
            for req in ("topic", "partition", "capture_start_utc", "capture_end_utc", "manifest_ref_sha256"):
                if req not in p:
                    errors.append(f"partition_capture_bounds[{i}] missing {req}")
    hashes = doc.get("artifact_hashes")
    if not isinstance(hashes, dict) or not hashes.get("measurement_window_sha256"):
        errors.append("artifact_hashes.measurement_window_sha256 required")
    return MeasurementWindow(doc=doc, run_id=str(doc.get("run_id", "")), contract_version=cv, errors=errors)


def verify_measurement_window_issues(mw: MeasurementWindow) -> list[dict[str, str]]:
    return [issue("MEASUREMENT_WINDOW_INVALID", e) for e in mw.errors]

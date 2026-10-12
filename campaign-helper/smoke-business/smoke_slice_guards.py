"""Smoke-slice binding + P099 prep verification (verify only, MAIN supplies coords)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

SMOKE_SLICE_VERSION = "20261012-smoke-slice-v2"
SMOKE_SLICE_VERSION_LEGACY = "20261012-smoke-slice-v1"


@dataclass(frozen=True)
class P099CoordClaim:
    topic: str
    partition: int
    offset: int
    prep_instance: str  # rac1 | rac2


def strict_int(value: Any, *, field: str) -> tuple[int | None, str | None]:
    if isinstance(value, bool):
        return None, f"{field} bool forbidden"
    if isinstance(value, int):
        return value, None
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip()), None
    return None, f"{field} not a strict integer"


def _sha256_file(path: str) -> str:
    import sys
    from pathlib import Path

    root = Path(path).resolve().parent
    # validator srl_hashes when validator-root already on path
    try:
        from srl_hashes import sha256_file  # type: ignore

        return sha256_file(path)
    except ImportError:
        import hashlib

        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()


def assigned_lower_bounds_from_pre_workload_manifest(manifest_doc: dict[str, Any]) -> dict[tuple[str, int], int]:
    """Inclusive smoke begin = exclusive end cut recorded on original pre-workload capture manifest."""
    out: dict[tuple[str, int], int] = {}
    for p in manifest_doc.get("partitions") or []:
        if not isinstance(p, dict):
            continue
        end_val = p.get("end", p.get("end_offset_exclusive"))
        if end_val is None:
            continue
        tp = (str(p["topic"]), int(p["partition"]))
        out[tp] = int(end_val)
    return out


def verify_pre_workload_and_manifest_bindings(
    slice_doc: dict[str, Any],
    *,
    measurement_window_path: str,
    capture_manifest_path: str,
    pre_workload_capture_manifest_path: str | None,
    manifest,
    lane: str,
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    cut = slice_doc.get("pre_workload_cut_ref")
    rec = slice_doc.get("recovered_capture_binding")
    bounds = slice_doc.get("smoke_measurement_bounds")
    if not isinstance(cut, dict):
        issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": "pre_workload_cut_ref required"})
        return issues
    if not isinstance(rec, dict):
        issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": "recovered_capture_binding required"})
        return issues
    if not isinstance(bounds, list) or not bounds:
        issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": "smoke_measurement_bounds[] required"})
        return issues

    want_mw = str(cut.get("measurement_window_sha256", ""))
    got_mw = _sha256_file(measurement_window_path)
    if not want_mw or want_mw != got_mw:
        issues.append(
            {
                "code": "SMOKE_SLICE_CUT_REF_MISMATCH",
                "detail": "pre_workload_cut_ref.measurement_window_sha256 != file",
            }
        )

    want_man = str(rec.get("capture_manifest_sha256", ""))
    got_man = _sha256_file(capture_manifest_path)
    if not want_man or want_man != got_man:
        issues.append(
            {
                "code": "SMOKE_SLICE_MANIFEST_REF_MISMATCH",
                "detail": "recovered_capture_binding.capture_manifest_sha256 != file",
            }
        )

    lower_list = cut.get("partition_lower_bounds")
    if not isinstance(lower_list, list):
        issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": "partition_lower_bounds[] required"})
        lower_list = []

    rec_parts = rec.get("partitions")
    if not isinstance(rec_parts, list) or not rec_parts:
        issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": "recovered_capture_binding.partitions[] required"})
        rec_parts = []

    lower_by_tp: dict[tuple[str, int], int] = {}
    for i, item in enumerate(lower_list):
        if not isinstance(item, dict):
            issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": f"partition_lower_bounds[{i}]"})
            continue
        try:
            tp = (str(item["topic"]), int(item["partition"]))
            lo, err = strict_int(item.get("begin_offset_inclusive"), field="begin_offset_inclusive")
            if err:
                issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": f"{tp}: {err}"})
                continue
            assert lo is not None
            lower_by_tp[tp] = lo
        except (KeyError, TypeError, ValueError):
            issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": f"partition_lower_bounds[{i}] invalid"})

    want_pre_man = str(cut.get("pre_workload_capture_manifest_sha256", ""))
    if not want_pre_man:
        issues.append(
            {
                "code": "SMOKE_SLICE_BOUND_INVALID",
                "detail": "pre_workload_cut_ref.pre_workload_capture_manifest_sha256 required",
            }
        )
    elif not pre_workload_capture_manifest_path:
        issues.append(
            {
                "code": "INPUT_INCOMPLETE",
                "detail": "missing --pre-workload-capture-manifest for pre_workload_cut_ref",
            }
        )
    else:
        got_pre = _sha256_file(pre_workload_capture_manifest_path)
        if got_pre != want_pre_man:
            issues.append(
                {
                    "code": "SMOKE_SLICE_PRE_CUT_MANIFEST_MISMATCH",
                    "detail": "pre_workload_capture_manifest_sha256 != file",
                }
            )
        else:
            try:
                with open(pre_workload_capture_manifest_path, encoding="utf-8") as fh:
                    pre_doc = json.load(fh)
            except (OSError, json.JSONDecodeError) as e:
                issues.append(
                    {"code": "SMOKE_SLICE_BOUND_INVALID", "detail": f"pre-workload manifest unreadable: {e}"}
                )
                pre_doc = {}
            assigned = assigned_lower_bounds_from_pre_workload_manifest(pre_doc)
            if not assigned:
                issues.append(
                    {
                        "code": "SMOKE_SLICE_BOUND_INVALID",
                        "detail": "pre-workload manifest partitions[] empty",
                    }
                )
            for tp, lo in lower_by_tp.items():
                if tp not in assigned:
                    issues.append(
                        {
                            "code": "SMOKE_SLICE_LOWER_BOUND_MISMATCH",
                            "detail": f"{tp}: no partition in pre-workload cut manifest",
                        }
                    )
                elif lo != assigned[tp]:
                    issues.append(
                        {
                            "code": "SMOKE_SLICE_LOWER_BOUND_MISMATCH",
                            "detail": f"{tp}: slice begin {lo} != pre-workload manifest end cut {assigned[tp]}",
                        }
                    )

    manifest_end: dict[tuple[str, int], int] = {}
    for p in manifest.partitions:
        manifest_end[(p.topic, p.partition)] = int(p.end)

    rec_end: dict[tuple[str, int], int] = {}
    for i, item in enumerate(rec_parts):
        if not isinstance(item, dict):
            continue
        try:
            tp = (str(item["topic"]), int(item["partition"]))
            end_ex, err = strict_int(item.get("end_offset_exclusive"), field="end_offset_exclusive")
            if err:
                issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": f"recovered {tp}: {err}"})
                continue
            assert end_ex is not None
            rec_end[tp] = end_ex
            if tp in manifest_end and manifest_end[tp] != end_ex:
                issues.append(
                    {
                        "code": "SMOKE_SLICE_MANIFEST_END_MISMATCH",
                        "detail": f"{tp}: binding end {end_ex} != manifest.end {manifest_end[tp]}",
                    }
                )
        except (KeyError, TypeError, ValueError):
            issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": f"recovered partitions[{i}] invalid"})

    bound_by_tp: dict[tuple[str, int], tuple[int, int]] = {}
    for i, b in enumerate(bounds):
        if not isinstance(b, dict):
            issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": f"smoke_measurement_bounds[{i}]"})
            continue
        try:
            tp = (str(b["topic"]), int(b["partition"]))
            begin, e1 = strict_int(b.get("begin_offset_inclusive"), field="begin_offset_inclusive")
            end_ex, e2 = strict_int(b.get("end_offset_exclusive"), field="end_offset_exclusive")
            if e1 or e2 or begin is None or end_ex is None:
                issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": f"{tp}: offset fields"})
                continue
            if begin >= end_ex:
                issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": f"{tp}: inverted range"})
            bound_by_tp[tp] = (begin, end_ex)
            if tp in lower_by_tp and begin != lower_by_tp[tp]:
                issues.append(
                    {
                        "code": "SMOKE_SLICE_LOWER_BOUND_MISMATCH",
                        "detail": f"{tp}: smoke begin {begin} != pre_workload cut {lower_by_tp[tp]}",
                    }
                )
            if tp in rec_end and end_ex != rec_end[tp]:
                issues.append(
                    {
                        "code": "SMOKE_SLICE_UPPER_BOUND_MISMATCH",
                        "detail": f"{tp}: smoke end {end_ex} != recovered exclusive end {rec_end[tp]}",
                    }
                )
            if tp in rec_end and end_ex < rec_end[tp]:
                issues.append(
                    {
                        "code": "SMOKE_SLICE_NARROWED_END",
                        "detail": f"{tp}: smoke end narrowed below recovered manifest end",
                    }
                )
        except (KeyError, TypeError, ValueError):
            issues.append({"code": "SMOKE_SLICE_BOUND_INVALID", "detail": f"smoke_measurement_bounds[{i}] invalid"})

    if set(bound_by_tp) != set(manifest_end):
        issues.append(
            {
                "code": "SMOKE_SLICE_PARTITION_SET_MISMATCH",
                "detail": f"bounds topics {sorted(bound_by_tp)} != manifest {sorted(manifest_end)}",
            }
        )

    return issues


def _prep_rows_from_pin(pin_doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for inst in ("rac1", "rac2"):
        key = f"tombstone_{inst}_before"
        row = pin_doc.get(key)
        if isinstance(row, dict):
            out[inst] = row
    return out


PREP_INSTANCE_SOURCE_PHASE = {"rac1": "W099a", "rac2": "W099b"}


def _successful_p099_sql_by_instance(receipts: list[Any]) -> dict[str, Any]:
    """At most one proven successful source op per prep instance (W099a/W099b sql_commit)."""
    proven: dict[str, Any] = {}
    for r in receipts:
        if r.receipt_type != "sql_commit":
            continue
        raw_phase = str(r.phase)
        if raw_phase not in PREP_INSTANCE_SOURCE_PHASE.values():
            continue
        if r.raw.get("sql_exit") is not None and int(r.raw["sql_exit"]) != 0:
            continue
        inst = next(k for k, ph in PREP_INSTANCE_SOURCE_PHASE.items() if ph == raw_phase)
        if inst not in proven:
            proven[inst] = r
    return proven


def _row_matches_prep(after: dict[str, Any] | None, prep: dict[str, Any], table: str) -> bool:
    if after is None or not isinstance(after, dict):
        return False
    if table != "EV":
        return False
    for k, v in prep.items():
        if k == "INST" and v is None:
            if after.get("INST") is not None:
                return False
        elif after.get(k) != v:
            return False
    return True


def verify_p099_prep_claims(
    slice_doc: dict[str, Any],
    *,
    pin_path: str,
    receipts_path: str,
    kafka_rows: list[dict[str, Any]],
    business_by_coord: dict[tuple[str, int, int], Any],
) -> tuple[set[tuple[str, int, int]], list[dict[str, str]]]:
    """Returns verified p099 coords; rejects bad claims with INPUT issues."""
    issues: list[dict[str, str]] = []
    verified: set[tuple[str, int, int]] = set()
    prep_block = slice_doc.get("p099_prep")
    if prep_block is None:
        return verified, issues
    if not isinstance(prep_block, dict):
        return verified, [{"code": "P099_PREP_INVALID", "detail": "p099_prep must be object"}]

    with open(pin_path, encoding="utf-8") as fh:
        pin_doc = json.load(fh)
    prep_rows = _prep_rows_from_pin(pin_doc)
    if len(prep_rows) < 2:
        issues.append({"code": "P099_PREP_INVALID", "detail": "pin missing tombstone_rac1/rac2_before"})

    import sys
    from pathlib import Path

    loaded_receipts: list[Any] = []
    try:
        from srl_receipt import load_execution_receipts  # type: ignore

        loaded = load_execution_receipts(receipts_path)
        loaded_receipts = loaded.receipts
        for e in loaded.errors:
            issues.append({"code": "P099_PREP_RECEIPT", "detail": e})
    except ImportError:
        issues.append({"code": "P099_PREP_RECEIPT", "detail": "receipt loader unavailable"})

    proven_source_by_inst = _successful_p099_sql_by_instance(loaded_receipts)

    coords_raw = prep_block.get("identified_coords")
    if not isinstance(coords_raw, list) or not coords_raw:
        issues.append({"code": "P099_PREP_INVALID", "detail": "identified_coords[] required"})
        return verified, issues

    row_index = {
        (str(r["topic"]), int(r["partition"]), int(r["kafka_offset"])): r for r in kafka_rows
    }
    seen: set[tuple[str, int, int]] = set()
    verified_inst: set[str] = set()
    for j, c in enumerate(coords_raw):
        if not isinstance(c, dict):
            issues.append({"code": "P099_PREP_INVALID", "detail": f"identified_coords[{j}]"})
            continue
        try:
            tp = (str(c["topic"]), int(c["partition"]))
            off, err = strict_int(c.get("offset"), field="offset")
            if err or off is None:
                issues.append({"code": "P099_PREP_INVALID", "detail": f"coord[{j}]: {err}"})
                continue
            coord = (tp[0], tp[1], off)
        except (KeyError, TypeError, ValueError):
            issues.append({"code": "P099_PREP_INVALID", "detail": f"identified_coords[{j}] invalid"})
            continue
        if coord in seen:
            issues.append({"code": "P099_PREP_DUPLICATE_CLAIM", "detail": f"duplicate {coord}"})
            continue
        seen.add(coord)

        inst = str(c.get("prep_instance", "")).lower()
        if inst not in prep_rows:
            issues.append({"code": "P099_PREP_IDENTITY_MISMATCH", "detail": f"{coord}: bad prep_instance"})
            continue
        if inst not in proven_source_by_inst:
            issues.append(
                {
                    "code": "P099_PREP_SOURCE_OP_UNPROVEN",
                    "detail": f"{coord}: no successful {PREP_INSTANCE_SOURCE_PHASE[inst]} sql_commit",
                }
            )
            continue
        if inst in verified_inst:
            issues.append(
                {
                    "code": "P099_PREP_DUPLICATE_CLAIM",
                    "detail": f"second p099 claim for prep_instance {inst}",
                }
            )
            continue

        if coord not in row_index:
            issues.append({"code": "P099_PREP_COORD_MISSING", "detail": f"{coord} not in capture"})
            continue

        op = business_by_coord.get(coord)
        if op is None:
            issues.append({"code": "P099_PREP_IDENTITY_MISMATCH", "detail": f"{coord}: not a business row"})
            continue
        if op.op != "c":
            issues.append(
                {"code": "P099_PREP_IDENTITY_MISMATCH", "detail": f"{coord}: prep must be INSERT op=c"}
            )
            continue
        if not _row_matches_prep(op.after, prep_rows[inst], op.table):
            issues.append(
                {
                    "code": "P099_PREP_IDENTITY_MISMATCH",
                    "detail": f"{coord}: row does not match pin {inst} prep identity",
                }
            )
            continue
        verified.add(coord)
        verified_inst.add(inst)

    return verified, issues


def build_pre_workload_cut_manifest_doc(smoke_measurement_bounds: list[dict[str, Any]]) -> dict[str, Any]:
    """Minimal pre-workload cut manifest: partition.end == assigned inclusive smoke begin."""
    partitions: list[dict[str, Any]] = []
    for b in smoke_measurement_bounds:
        lo = int(b["begin_offset_inclusive"])
        last = lo - 1 if lo > 0 else 0
        partitions.append(
            {
                "topic": str(b["topic"]),
                "partition": int(b["partition"]),
                "beginning": 0,
                "end": lo,
                "final_position": lo,
                "first_offset": 0 if lo == 0 else lo - 1,
                "last_offset": last,
                "captured": 0,
            }
        )
    return {
        "complete": True,
        "records": 0,
        "isolation_level": "read_committed",
        "partitions": partitions,
    }


def build_slice_v2_from_manifest(
    manifest_doc: dict[str, Any],
    *,
    lane: str,
    run_id: str,
    measurement_window_sha256: str,
    capture_manifest_sha256: str,
    pre_workload_capture_manifest_sha256: str,
    partition_lower_bounds: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Harness helper: bind smoke bounds to manifest ends (no narrowing)."""
    lower_by: dict[tuple[str, int], int] = {}
    if partition_lower_bounds:
        for item in partition_lower_bounds:
            lower_by[(str(item["topic"]), int(item["partition"]))] = int(item["begin_offset_inclusive"])

    smoke_bounds: list[dict[str, Any]] = []
    recovered: list[dict[str, Any]] = []
    lower_list: list[dict[str, Any]] = []
    for p in manifest_doc.get("partitions") or []:
        topic = str(p["topic"])
        part = int(p["partition"])
        end_ex = int(p.get("end", p.get("end_offset_exclusive")))
        begin = lower_by.get((topic, part), int(p.get("beginning", p.get("beginning_offset", 0))))
        smoke_bounds.append(
            {
                "topic": topic,
                "partition": part,
                "begin_offset_inclusive": begin,
                "end_offset_exclusive": end_ex,
            }
        )
        recovered.append(
            {
                "topic": topic,
                "partition": part,
                "beginning": int(p.get("beginning", 0)),
                "end_offset_exclusive": end_ex,
            }
        )
        lower_list.append(
            {"topic": topic, "partition": part, "begin_offset_inclusive": begin}
        )
    return {
        "contract_version": SMOKE_SLICE_VERSION,
        "run_id": run_id,
        "lane": lane,
        "pre_workload_cut_ref": {
            "measurement_window_sha256": measurement_window_sha256,
            "pre_workload_capture_manifest_sha256": pre_workload_capture_manifest_sha256,
            "partition_lower_bounds": lower_list,
        },
        "recovered_capture_binding": {
            "capture_manifest_sha256": capture_manifest_sha256,
            "partitions": recovered,
        },
        "smoke_measurement_bounds": smoke_bounds,
    }

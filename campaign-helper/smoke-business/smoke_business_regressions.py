#!/usr/bin/env python3
"""In-memory regressions for smoke_business.py (same entry, no validate_lane)."""

from __future__ import annotations

import copy
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent
VALIDATORS = ROOT.parent / "validators"
EVIDENCE = ROOT / "evidence" / "smoke-business-regressions-last.json"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(VALIDATORS) not in sys.path:
    sys.path.insert(0, str(VALIDATORS))

from smoke_business import EXIT_FAIL, EXIT_INPUT, EXIT_SMOKE_PASS, validate_smoke_business  # noqa: E402
from smoke_slice_guards import (  # noqa: E402
    build_pre_workload_cut_manifest_doc,
    build_slice_v2_from_manifest,
)


def _archive(path: Path) -> None:
    if not path.is_file():
        return
    hist = path.parent / "history"
    hist.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    import shutil

    shutil.copy(path, hist / f"{path.stem}-{stamp}{path.suffix}")


def _sha(path: str) -> str:
    from srl_hashes import sha256_file  # noqa: WPS433

    return sha256_file(path)


def _build_bundle() -> dict[str, Any]:
    import mandatory_field_regressions as mfr  # noqa: WPS433

    base = mfr.build_base()
    return {"base": base, "slice": None}


def _sync_pre_workload_manifest(bundle: dict[str, Any], paths: dict[str, str]) -> str:
    bounds = bundle["slice"]["smoke_measurement_bounds"]
    pre_doc = build_pre_workload_cut_manifest_doc(bounds)
    paths["pre_manifest"] = paths.get("pre_manifest") or str(Path(paths["manifest"]).with_name("pre-workload-manifest.json"))
    Path(paths["pre_manifest"]).write_text(json.dumps(pre_doc), encoding="utf-8")
    return _sha(paths["pre_manifest"])


def _rebuild_slice(bundle: dict[str, Any], paths: dict[str, str]) -> None:
    draft = build_slice_v2_from_manifest(
        bundle["base"]["m"],
        lane="baseline",
        run_id="20261012T031500Z",
        measurement_window_sha256=_sha(paths["mw"]),
        capture_manifest_sha256=_sha(paths["manifest"]),
        pre_workload_capture_manifest_sha256="0" * 64,
    )
    bundle["slice"] = draft
    pre_sha = _sync_pre_workload_manifest(bundle, paths)
    bundle["slice"] = build_slice_v2_from_manifest(
        bundle["base"]["m"],
        lane="baseline",
        run_id="20261012T031500Z",
        measurement_window_sha256=_sha(paths["mw"]),
        capture_manifest_sha256=_sha(paths["manifest"]),
        pre_workload_capture_manifest_sha256=pre_sha,
    )


def _write_bundle(tmp: Path, bundle: dict[str, Any]) -> dict[str, str]:
    b = bundle["base"]
    paths: dict[str, str] = {}
    paths["kafka"] = str(tmp / "kafka.jsonl")
    with open(paths["kafka"], "w", encoding="utf-8") as fh:
        for row in b["k"]:
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
    paths["flink"] = str(tmp / "flink.jsonl")
    with open(paths["flink"], "w", encoding="utf-8") as fh:
        for row in b["f"]:
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
    paths["receipts"] = str(tmp / "receipts.jsonl")
    with open(paths["receipts"], "w", encoding="utf-8") as fh:
        for row in b["r"]:
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
    paths["mw"] = str(tmp / "mw.json")
    Path(paths["mw"]).write_text(json.dumps(b["w"]), encoding="utf-8")
    paths["qev"] = str(tmp / "qev.json")
    Path(paths["qev"]).write_text(json.dumps(b["q"]), encoding="utf-8")
    paths["manifest"] = str(tmp / "manifest.json")
    Path(paths["manifest"]).write_text(json.dumps(b["m"]), encoding="utf-8")
    _rebuild_slice(bundle, paths)
    paths["smoke_slice"] = str(tmp / "smoke-slice.json")
    Path(paths["smoke_slice"]).write_text(json.dumps(bundle["slice"]), encoding="utf-8")
    fix = VALIDATORS / "fixtures"
    paths["ops"] = str(fix / "workload-expected-operations.smoke.jsonl")
    paths["final"] = str(fix / "workload-expected-final-state.smoke.json")
    paths["pin"] = str(fix / "workload-oracle-pin.json")
    return paths


def _persist(bundle: dict[str, Any], paths: dict[str, str]) -> None:
    b = bundle["base"]
    if paths.get("_rewrite_kafka"):
        with open(paths["kafka"], "w", encoding="utf-8") as fh:
            for row in b["k"]:
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
    if paths.get("_rewrite_flink"):
        with open(paths["flink"], "w", encoding="utf-8") as fh:
            for row in b["f"]:
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
    if paths.get("_rewrite_manifest"):
        Path(paths["manifest"]).write_text(json.dumps(b["m"]), encoding="utf-8")
    if paths.get("_rewrite_receipts"):
        with open(paths["receipts"], "w", encoding="utf-8") as fh:
            for row in b["r"]:
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
    if paths.get("_rewrite_manifest") or paths.get("_rewrite_mw"):
        _rebuild_slice(bundle, paths)
        Path(paths["smoke_slice"]).write_text(json.dumps(bundle["slice"]), encoding="utf-8")
    elif paths.get("_rewrite_slice_only"):
        Path(paths["smoke_slice"]).write_text(json.dumps(bundle["slice"]), encoding="utf-8")


def _run_case(
    name: str,
    mutate: Callable[[dict[str, Any], dict[str, str]], None] | None,
    *,
    expect_exit: int,
    expect_outcome: str,
    codes: list[str],
    forbid_codes: list[str] | None = None,
) -> dict[str, Any]:
    bundle = _build_bundle()
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        paths = _write_bundle(tmp, bundle)
        if mutate:
            mutate(bundle, paths)
            _persist(bundle, paths)
        code, report = validate_smoke_business(
            validator_root=str(VALIDATORS),
            lane="baseline",
            kafka_path=paths["kafka"],
            expected_path=paths["ops"],
            expected_final_state_path=paths["final"],
            oracle_pin_path=paths["pin"],
            flink_sink_path=paths["flink"],
            capture_manifest_path=paths["manifest"],
            execution_receipts_path=paths["receipts"],
            measurement_window_path=paths["mw"],
            qualification_evidence_path=paths["qev"],
            smoke_slice_path=paths["smoke_slice"],
            pre_workload_capture_manifest_path=paths["pre_manifest"],
        )
    got_codes: list[str] = []
    for f in report.get("failures", []):
        got_codes.append(str(f.get("code")))
        if f.get("code") == "RUNTIME_GATE" and ": " in str(f.get("detail", "")):
            got_codes.append(str(f["detail"]).split(":", 1)[0])
    errs: list[str] = []
    if code != expect_exit:
        errs.append(f"exit {code}!={expect_exit}")
    if report.get("outcome") != expect_outcome:
        errs.append(f"outcome {report.get('outcome')!r}!={expect_outcome!r}")
    for c in codes:
        if c not in got_codes:
            errs.append(f"missing code {c!r} in {got_codes}")
    for c in forbid_codes or []:
        if c in got_codes:
            errs.append(f"forbidden code {c!r} in {got_codes}")
    dims = report.get("acceptance_dimensions") or {}
    if dims.get("independent_source_qualification") != "UNVERIFIED":
        errs.append("independent_source must stay UNVERIFIED")
    return {
        "name": name,
        "exit_code": code,
        "outcome": report.get("outcome"),
        "assert_ok": not errs,
        "assert_errors": errs,
        "failure_codes": got_codes,
    }


def run_all() -> int:
    cases: list[dict] = []

    cases.append(
        _run_case(
            "positive_component_smoke_pass",
            None,
            expect_exit=EXIT_SMOKE_PASS,
            expect_outcome="SMOKE_BUSINESS_PASS",
            codes=[],
        )
    )

    def drop_one_op(bundle, paths):
        bundle["base"]["k"].pop(0)
        paths["_rewrite_kafka"] = True
        bundle["base"]["m"]["records"] -= 1
        bundle["base"]["m"]["partitions"][0]["captured"] -= 1
        paths["_rewrite_manifest"] = True

    cases.append(
        _run_case(
            "negative_missing_op_in_smoke_slice",
            drop_one_op,
            expect_exit=EXIT_FAIL,
            expect_outcome="SMOKE_BUSINESS_FAIL",
            codes=["MISSING_OPS"],
        )
    )

    def dup_op_distinct_coord(bundle, paths):
        import self_test as st_mod  # noqa: WPS433

        last = bundle["base"]["k"][-1]
        new_off = max(int(r["kafka_offset"]) for r in bundle["base"]["k"]) + 1
        extra = copy.deepcopy(last)
        extra["kafka_offset"] = new_off
        extra["timestamp"] = int(last.get("timestamp", 0)) + 1
        bundle["base"]["k"].append(extra)
        bundle["base"]["m"] = st_mod._manifest_for(bundle["base"]["k"])
        paths["_rewrite_kafka"] = True
        paths["_rewrite_manifest"] = True

    cases.append(
        _run_case(
            "negative_duplicate_distinct_coord_valid_manifest",
            dup_op_distinct_coord,
            expect_exit=EXIT_FAIL,
            expect_outcome="SMOKE_BUSINESS_FAIL",
            codes=["EXTRA_OPS"],
            forbid_codes=["MISSING_OPS"],
        )
    )

    def wrong_val(bundle, paths):
        row = bundle["base"]["k"][0]
        val = json.loads(row["value_raw"])
        val["after"]["VAL"] = 999999
        row["value_raw"] = json.dumps(val, separators=(",", ":"))
        paths["_rewrite_kafka"] = True

    cases.append(
        _run_case(
            "negative_wrong_kafka_value",
            wrong_val,
            expect_exit=EXIT_FAIL,
            expect_outcome="SMOKE_BUSINESS_FAIL",
            codes=["CAUSAL_TRANSITION"],
        )
    )

    def uu_swap(bundle, paths):
        r1, r2 = bundle["base"]["k"][1], bundle["base"]["k"][2]
        r1["value_raw"], r2["value_raw"] = r2["value_raw"], r1["value_raw"]
        paths["_rewrite_kafka"] = True

    cases.append(
        _run_case(
            "negative_uu_reorder_same_key",
            uu_swap,
            expect_exit=EXIT_FAIL,
            expect_outcome="SMOKE_BUSINESS_FAIL",
            codes=["CAUSAL_TRANSITION"],
        )
    )

    def manifest_incomplete(bundle, paths):
        bundle["base"]["m"]["complete"] = False
        paths["_rewrite_manifest"] = True

    cases.append(
        _run_case(
            "negative_capture_manifest_incomplete",
            manifest_incomplete,
            expect_exit=EXIT_FAIL,
            expect_outcome="SMOKE_BUSINESS_FAIL",
            codes=["CAPTURE_INCOMPLETE"],
        )
    )

    def bad_flink(bundle, paths):
        for row in bundle["base"]["f"]:
            if row.get("table") == "EV" and isinstance(row.get("row"), dict):
                row["row"] = dict(row["row"])
                row["row"]["VAL"] = 999999
                break
        paths["_rewrite_flink"] = True

    cases.append(
        _run_case(
            "negative_flink_final_wrong",
            bad_flink,
            expect_exit=EXIT_FAIL,
            expect_outcome="SMOKE_BUSINESS_FAIL",
            codes=["FLINK_FINAL_STATE"],
        )
    )

    def narrowed_end_only(bundle, paths):
        b = bundle["slice"]["smoke_measurement_bounds"][0]
        b["end_offset_exclusive"] = int(b["begin_offset_inclusive"]) + 1
        paths["_rewrite_slice_only"] = True

    cases.append(
        _run_case(
            "negative_narrowed_end_vs_recovered_binding",
            narrowed_end_only,
            expect_exit=EXIT_INPUT,
            expect_outcome="SMOKE_BUSINESS_INPUT_INCOMPLETE",
            codes=["SMOKE_SLICE_UPPER_BOUND_MISMATCH"],
        )
    )

    def missing_cut_ref(bundle, paths):
        bundle["slice"].pop("pre_workload_cut_ref", None)
        paths["_rewrite_slice_only"] = True

    cases.append(
        _run_case(
            "negative_missing_pre_workload_cut_ref",
            missing_cut_ref,
            expect_exit=EXIT_INPUT,
            expect_outcome="SMOKE_BUSINESS_INPUT_INCOMPLETE",
            codes=["SMOKE_SLICE_BOUND_INVALID"],
        )
    )

    def false_p099_on_smoke_row(bundle, paths):
        row = bundle["base"]["k"][0]
        bundle["slice"]["p099_prep"] = {
            "receipt_phase": "P099",
            "identified_coords": [
                {
                    "topic": row["topic"],
                    "partition": int(row["partition"]),
                    "offset": int(row["kafka_offset"]),
                    "prep_instance": "rac1",
                }
            ],
        }
        paths["_rewrite_slice_only"] = True

    cases.append(
        _run_case(
            "negative_false_p099_claim_on_committed_smoke_row",
            false_p099_on_smoke_row,
            expect_exit=EXIT_INPUT,
            expect_outcome="SMOKE_BUSINESS_INPUT_INCOMPLETE",
            codes=["P099_PREP_SOURCE_OP_UNPROVEN"],
        )
    )

    def wrong_lower_vs_pre_cut_manifest(bundle, paths):
        b0 = bundle["slice"]["smoke_measurement_bounds"][0]
        b0["begin_offset_inclusive"] = 1
        bundle["slice"]["pre_workload_cut_ref"]["partition_lower_bounds"][0]["begin_offset_inclusive"] = 1
        paths["_rewrite_slice_only"] = True

    cases.append(
        _run_case(
            "negative_lower_bound_vs_pre_workload_manifest_end_cut",
            wrong_lower_vs_pre_cut_manifest,
            expect_exit=EXIT_INPUT,
            expect_outcome="SMOKE_BUSINESS_INPUT_INCOMPLETE",
            codes=["SMOKE_SLICE_LOWER_BOUND_MISMATCH"],
        )
    )

    def duplicate_prep_insert_beyond_proven_source(bundle, paths):
        import self_test as st_mod  # noqa: WPS433

        pin = json.loads(Path(paths["pin"]).read_text(encoding="utf-8"))
        prep_after = dict(pin["tombstone_rac1_before"])
        template = copy.deepcopy(bundle["base"]["k"][0])
        base_off = max(int(r["kafka_offset"]) for r in bundle["base"]["k"])
        rows = []
        for off in (base_off + 1, base_off + 2):
            row = copy.deepcopy(template)
            row["kafka_offset"] = off
            row["timestamp"] = int(template.get("timestamp", 0)) + off
            val = json.loads(row["value_raw"])
            val["before"] = None
            val["after"] = prep_after
            val["op"] = "c"
            row["value_raw"] = json.dumps(val, separators=(",", ":"))
            rows.append(row)
        bundle["base"]["k"].extend(rows)
        bundle["base"]["m"] = st_mod._manifest_for(bundle["base"]["k"])
        bundle["base"]["r"].append(
            {
                "run_id": "20261012T031500Z",
                "phase": "W099a",
                "receipt_type": "sql_commit",
                "utc": "2026-10-12T03:19:29.000Z",
                "host": "RAC1",
                "file": "prep_rac1.sql",
                "sql_exit": 0,
            }
        )
        claimed = rows[0]
        bundle["slice"]["p099_prep"] = {
            "receipt_phase": "P099",
            "identified_coords": [
                {
                    "topic": claimed["topic"],
                    "partition": int(claimed["partition"]),
                    "offset": int(claimed["kafka_offset"]),
                    "prep_instance": "rac1",
                }
            ],
        }
        paths["_rewrite_kafka"] = True
        paths["_rewrite_manifest"] = True
        paths["_rewrite_receipts"] = True

    cases.append(
        _run_case(
            "negative_duplicate_identical_prep_extra_not_excluded",
            duplicate_prep_insert_beyond_proven_source,
            expect_exit=EXIT_FAIL,
            expect_outcome="SMOKE_BUSINESS_FAIL",
            codes=["EXTRA_OPS"],
            forbid_codes=["MISSING_OPS"],
        )
    )

    def rollback_leak(bundle, paths):
        row = bundle["base"]["k"][0]
        val = json.loads(row["value_raw"])
        if isinstance(val.get("after"), dict):
            val["after"] = dict(val["after"])
            val["after"]["TAG"] = "ROLLED_BACK"
        row["value_raw"] = json.dumps(val, separators=(",", ":"))
        paths["_rewrite_kafka"] = True

    cases.append(
        _run_case(
            "negative_rollback_tag_leak",
            rollback_leak,
            expect_exit=EXIT_FAIL,
            expect_outcome="SMOKE_BUSINESS_FAIL",
            codes=["ROLLBACK_LEAK"],
        )
    )

    def missing_slice(bundle, paths):
        paths["smoke_slice"] = str(Path(paths["smoke_slice"]).with_name("missing-slice.json"))

    cases.append(
        _run_case(
            "negative_missing_smoke_slice_file",
            missing_slice,
            expect_exit=EXIT_INPUT,
            expect_outcome="SMOKE_BUSINESS_INPUT_INCOMPLETE",
            codes=["INPUT_INCOMPLETE"],
        )
    )

    summary = {
        "suite": "smoke-business-regressions",
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "validator_root": str(VALIDATORS),
        "smoke_slice_contract": "20261012-smoke-slice-v2",
        "overall_pass": all(c.get("assert_ok") for c in cases),
        "case_count": len(cases),
        "failed_cases": [c["name"] for c in cases if not c.get("assert_ok")],
        "cases": cases,
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    _archive(EVIDENCE)
    EVIDENCE.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0 if summary["overall_pass"] else 1


if __name__ == "__main__":
    sys.exit(run_all())

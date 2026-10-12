#!/usr/bin/env python3
"""
Bounded SMOKE BUSINESS validator entry (no per-op provenance / validate_lane).

Uses accepted v73 modules from --validator-root only.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from smoke_selection import apply_selection_to_business_ops, load_smoke_slice
from smoke_slice_guards import (
    SMOKE_SLICE_VERSION,
    verify_p099_prep_claims,
    verify_pre_workload_and_manifest_bindings,
)

EXIT_SMOKE_PASS = 0
EXIT_FAIL = 1
EXIT_INPUT = 2
EXIT_USAGE = 3

_COMPUTED = {
    "independent_source_qualification": "UNVERIFIED",
    "prearchive_status": "UNKNOWN",
    "campaign_status": "NOT_ACCEPTED",
}


def _configure_validator_root(validator_root: str) -> Path:
    root = Path(validator_root).resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"validator-root not found: {root}")
    p = str(root)
    if p not in sys.path:
        sys.path.insert(0, p)
    return root


def _missing_file(path: str | None, label: str, failures: list[dict]) -> bool:
    if not path:
        failures.append({"code": "INPUT_INCOMPLETE", "detail": f"missing {label}"})
        return True
    if not Path(path).is_file():
        failures.append({"code": "INPUT_INCOMPLETE", "detail": f"{label} not found: {path}"})
        return True
    return False


def validate_smoke_business(
    *,
    validator_root: str,
    lane: str,
    kafka_path: str,
    expected_path: str,
    expected_final_state_path: str,
    oracle_pin_path: str,
    flink_sink_path: str,
    capture_manifest_path: str,
    execution_receipts_path: str,
    measurement_window_path: str,
    qualification_evidence_path: str,
    smoke_slice_path: str,
    pre_workload_capture_manifest_path: str,
    contract_version: str | None = None,
    workload_mode: str | None = None,
    invoked_argv: list[str] | None = None,
) -> tuple[int, dict[str, Any]]:
    report: dict[str, Any] = {
        "status": "FAIL",
        "outcome": "SMOKE_BUSINESS_FAIL",
        "lane": lane,
        "smoke_slice_contract": SMOKE_SLICE_VERSION,
        "acceptance_dimensions": dict(_COMPUTED),
        "checks": [],
        "failures": [],
        "counts": {},
        "capture_accounting": {},
        "invoked_argv": invoked_argv or [],
    }
    failures: list[dict[str, str]] = []

    try:
        vroot = _configure_validator_root(validator_root)
    except OSError as e:
        failures.append({"code": "INPUT_INCOMPLETE", "detail": str(e)})
        report["failures"] = failures
        report["status"] = "INPUT_INCOMPLETE"
        report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
        return EXIT_INPUT, report

    from srl_contracts import CONTRACT_VERSION, FORBIDDEN_TAGS  # noqa: WPS433
    from srl_capture_manifest import (  # noqa: WPS433
        index_jsonl_capture,
        load_capture_manifest,
        verify_capture_manifest_against_jsonl,
    )
    from srl_flink import compare_final_states, flink_rows, load_flink_sink  # noqa: WPS433
    from srl_hashes import sha256_file  # noqa: WPS433
    from srl_kafka import load_kafka_capture, load_lane_capture_rows  # noqa: WPS433
    from srl_match import forbidden_leaked, ops_match  # noqa: WPS433
    from srl_oracle import build_expected_final_from_records, load_expected_final_state, load_expected_oracle  # noqa: WPS433
    from srl_oracle_qualify import qualify_oracle  # noqa: WPS433
    from srl_order import verify_per_key_ordered_transitions  # noqa: WPS433
    from srl_runtime_gates import run_runtime_gates  # noqa: WPS433
    from srl_source_envelope import verify_envelope_source_stock37  # noqa: WPS433

    cv = contract_version or CONTRACT_VERSION
    report["contract_version"] = cv

    for path, label in (
        (kafka_path, "--kafka"),
        (expected_path, "--expected-events"),
        (expected_final_state_path, "--expected-final-state"),
        (oracle_pin_path, "--oracle-pin"),
        (flink_sink_path, "--flink-sink"),
        (capture_manifest_path, "--capture-manifest"),
        (execution_receipts_path, "--execution-receipts"),
        (measurement_window_path, "--measurement-window"),
        (qualification_evidence_path, "--qualification-evidence"),
        (smoke_slice_path, "--smoke-slice"),
        (pre_workload_capture_manifest_path, "--pre-workload-capture-manifest"),
    ):
        if _missing_file(path, label, failures):
            report["failures"] = failures
            report["status"] = "INPUT_INCOMPLETE"
            report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
            return EXIT_INPUT, report

    if lane not in ("baseline", "recovery"):
        failures.append({"code": "UNKNOWN_LANE", "detail": lane})
        report["failures"] = failures
        report["status"] = "INPUT_INCOMPLETE"
        report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
        return EXIT_INPUT, report

    slice_doc = load_smoke_slice(smoke_slice_path)
    if slice_doc.errors:
        for e in slice_doc.errors:
            failures.append({"code": "SMOKE_SLICE_INVALID", "detail": e})
        report["failures"] = failures
        report["status"] = "INPUT_INCOMPLETE"
        report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
        return EXIT_INPUT, report
    if slice_doc.lane != lane:
        failures.append(
            {"code": "SMOKE_SLICE_LANE_MISMATCH", "detail": f"slice lane {slice_doc.lane} != {lane}"}
        )
        report["failures"] = failures
        report["status"] = "INPUT_INCOMPLETE"
        report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
        return EXIT_INPUT, report

    oracle = load_expected_oracle(expected_path, oracle_pin_path)
    oracle = load_expected_final_state(expected_final_state_path, oracle)
    if oracle.errors:
        for e in oracle.errors:
            failures.append({"code": "ORACLE_INVALID", "detail": e})
        report["failures"] = failures
        report["status"] = "INPUT_INCOMPLETE"
        report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
        return EXIT_INPUT, report

    q_issues = qualify_oracle(oracle)
    if q_issues:
        for q in q_issues:
            failures.append({"code": "ORACLE_QUALIFICATION", "detail": f"{q['code']}: {q['detail']}"})
        report["qualification"] = {"status": "INCOMPLETE", "issues": q_issues}
        report["failures"] = failures
        report["status"] = "INPUT_INCOMPLETE"
        report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
        return EXIT_INPUT, report
    report["qualification"] = {"status": "PASS", "issues": []}

    mode = workload_mode or (oracle.pin.mode if oracle.pin else "smoke")
    pin_sha = sha256_file(oracle_pin_path)
    ops_sha = sha256_file(expected_path)
    final_sha = sha256_file(expected_final_state_path)
    rt_summary, rt_issues = run_runtime_gates(
        lane=lane,
        mode=mode,
        oracle=oracle,
        receipts_path=execution_receipts_path,
        measurement_window_path=measurement_window_path,
        qualification_evidence_path=qualification_evidence_path,
        pin_path=oracle_pin_path,
        ops_path=expected_path,
        final_path=expected_final_state_path,
        pin_sha256=pin_sha,
        ops_sha256=ops_sha,
        final_sha256=final_sha,
        require_all=True,
    )
    report["runtime_gates"] = {"status": rt_summary.get("status"), "issues": rt_issues}
    if rt_issues:
        for q in rt_issues:
            failures.append({"code": "RUNTIME_GATE", "detail": f"{q['code']}: {q['detail']}"})
        report["failures"] = failures
        report["status"] = "INPUT_INCOMPLETE"
        report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
        return EXIT_INPUT, report

    try:
        capture = load_kafka_capture(kafka_path, lane)
    except Exception as e:
        failures.append({"code": "KAFKA_PARSE", "detail": str(e)})
        report["failures"] = failures
        report["status"] = "INPUT_INCOMPLETE"
        report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
        return EXIT_INPUT, report

    raw_rows = load_lane_capture_rows(kafka_path, lane)
    report["capture_accounting"]["full_lane_rows"] = len(raw_rows)

    manifest = load_capture_manifest(capture_manifest_path)
    if manifest.errors:
        for e in manifest.errors:
            failures.append({"code": "CAPTURE_MANIFEST_PARSE", "detail": e})

    for q in verify_pre_workload_and_manifest_bindings(
        slice_doc.doc,
        measurement_window_path=measurement_window_path,
        capture_manifest_path=capture_manifest_path,
        pre_workload_capture_manifest_path=pre_workload_capture_manifest_path,
        manifest=manifest,
        lane=lane,
    ):
        failures.append(q)

    business_all = [o for o in capture.business_ops if not o.snapshot]
    business_by_coord = {
        (op.topic, op.partition, op.kafka_offset): op for op in business_all
    }
    verified_p099, p099_issues = verify_p099_prep_claims(
        slice_doc.doc,
        pin_path=oracle_pin_path,
        receipts_path=execution_receipts_path,
        kafka_rows=raw_rows,
        business_by_coord=business_by_coord,
    )
    for q in p099_issues:
        failures.append(q)

    slice_and_prep_input_codes = {
        "SMOKE_SLICE_BOUND_INVALID",
        "SMOKE_SLICE_CUT_REF_MISMATCH",
        "SMOKE_SLICE_MANIFEST_REF_MISMATCH",
        "SMOKE_SLICE_MANIFEST_END_MISMATCH",
        "SMOKE_SLICE_LOWER_BOUND_MISMATCH",
        "SMOKE_SLICE_UPPER_BOUND_MISMATCH",
        "SMOKE_SLICE_NARROWED_END",
        "SMOKE_SLICE_PARTITION_SET_MISMATCH",
        "SMOKE_SLICE_PRE_CUT_MANIFEST_MISMATCH",
        "P099_PREP_INVALID",
        "P099_PREP_RECEIPT",
        "P099_PREP_DUPLICATE_CLAIM",
        "P099_PREP_IDENTITY_MISMATCH",
        "P099_PREP_COORD_MISSING",
        "P099_PREP_SOURCE_OP_UNPROVEN",
    }
    if any(f.get("code") in slice_and_prep_input_codes for f in failures):
        report["failures"] = failures
        report["status"] = "INPUT_INCOMPLETE"
        report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
        return EXIT_INPUT, report

    selected_ops, selection_acc = apply_selection_to_business_ops(
        business_all,
        slice_doc=slice_doc,
        manifest=manifest,
        verified_p099=verified_p099,
    )
    report["capture_accounting"] = {
        "full_lane_rows": len(raw_rows),
        "full_lane_business_ops": len(business_all),
        "selected_business_ops": len(selected_ops),
        "selected_coords": selection_acc.selected_coords,
        "excluded_coords": selection_acc.excluded_coords,
        "p099_identified_coords": selection_acc.p099_identified_coords,
        "smoke_measurement_bounds": [
            {
                "topic": b.topic,
                "partition": b.partition,
                "begin_offset_inclusive": b.begin_offset_inclusive,
                "end_offset_exclusive": b.end_offset_exclusive,
            }
            for b in slice_doc.measurement_bounds
        ],
    }

    if slice_doc.p099_claimed_coords and not verified_p099:
        failures.append(
            {
                "code": "P099_PREP_COORD_MISSING",
                "detail": "p099_prep claims none verified in capture",
            }
        )

    business_only = selected_ops
    for fail in verify_envelope_source_stock37(raw_rows, business_only):
        failures.append(fail)

    jsonl_idx = index_jsonl_capture(kafka_path, lane)
    for fail in verify_capture_manifest_against_jsonl(manifest, jsonl_idx, lane=lane):
        failures.append({"code": fail["code"], "detail": fail["detail"]})

    required = list(oracle.committed_kafka)
    pool = list(business_only)
    unmatched_req: list[str] = []
    for exp in required:
        idx = next((i for i, a in enumerate(pool) if ops_match(exp, a)), None)
        if idx is None:
            unmatched_req.append(exp.op_id)
        else:
            pool.pop(idx)

    if unmatched_req:
        failures.append({"code": "MISSING_OPS", "detail": f"unmatched op_ids: {unmatched_req[:8]}"})
    if pool:
        failures.append({"code": "EXTRA_OPS", "detail": f"unexpected kafka ops in smoke slice: {len(pool)}"})
    if not unmatched_req and not pool:
        report["checks"].append("committed_multiset")

    for fail in verify_per_key_ordered_transitions(oracle.committed_kafka, business_only):
        failures.append(fail)

    for op in business_only:
        for side in (op.after, op.before):
            if not side:
                continue
            tag = side.get("TAG")
            if tag and str(tag) in FORBIDDEN_TAGS:
                failures.append(
                    {"code": "ROLLBACK_LEAK", "detail": f"forbidden TAG {tag} in kafka capture"}
                )
    for forb in oracle.forbidden:
        for act in business_only:
            if forbidden_leaked(forb, act):
                failures.append(
                    {
                        "code": "ROLLBACK_LEAK",
                        "detail": f"committed=false op {forb.op_id} leaked to kafka",
                    }
                )

    expected_final = build_expected_final_from_records(oracle)
    sink = load_flink_sink(flink_sink_path)
    for e in sink.errors:
        failures.append({"code": "FLINK_SINK_PARSE", "detail": e})
    actual_final = flink_rows(sink)
    fs_errs = compare_final_states(expected_final, actual_final, oracle.absent_ids)
    for err in fs_errs:
        failures.append({"code": "FLINK_FINAL_STATE", "detail": err})
    if not fs_errs and oracle.final_state_entries:
        report["checks"].append("flink_final_state")

    report["counts"] = {
        "oracle_kafka_committed_events": len(oracle.committed_kafka),
        "oracle_forbidden_records": len(oracle.forbidden),
        "kafka_smoke_slice_business_ops": len(business_only),
        "kafka_full_capture_business_ops": len(business_all),
    }

    input_codes = {
        "KAFKA_PARSE",
        "ORACLE_INVALID",
        "ORACLE_QUALIFICATION",
        "RUNTIME_GATE",
        "INPUT_INCOMPLETE",
        "SMOKE_SLICE_INVALID",
        "SMOKE_SLICE_LANE_MISMATCH",
        "P099_PREP_COORD_MISSING",
        "SMOKE_SLICE_BOUND_INVALID",
        "SMOKE_SLICE_CUT_REF_MISMATCH",
        "SMOKE_SLICE_MANIFEST_REF_MISMATCH",
        "SMOKE_SLICE_MANIFEST_END_MISMATCH",
        "SMOKE_SLICE_LOWER_BOUND_MISMATCH",
        "SMOKE_SLICE_UPPER_BOUND_MISMATCH",
        "SMOKE_SLICE_NARROWED_END",
        "SMOKE_SLICE_PARTITION_SET_MISMATCH",
        "SMOKE_SLICE_PRE_CUT_MANIFEST_MISMATCH",
        "P099_PREP_INVALID",
        "P099_PREP_RECEIPT",
        "P099_PREP_DUPLICATE_CLAIM",
        "P099_PREP_IDENTITY_MISMATCH",
        "P099_PREP_SOURCE_OP_UNPROVEN",
    }
    source_codes = {
        "SOURCE_ENVELOPE_MISSING",
        "SOURCE_SCHEMA_MISMATCH",
        "SOURCE_TABLE_MISMATCH",
        "SOURCE_TABLE_MISSING",
        "SOURCE_METADATA_MISSING",
    }
    if any(f.get("code") in input_codes or f.get("code") in source_codes for f in failures):
        report["failures"] = failures
        report["status"] = "INPUT_INCOMPLETE"
        report["outcome"] = "SMOKE_BUSINESS_INPUT_INCOMPLETE"
        return EXIT_INPUT, report

    if failures:
        report["failures"] = failures
        report["status"] = "FAIL"
        report["outcome"] = "SMOKE_BUSINESS_FAIL"
        return EXIT_FAIL, report

    report["failures"] = []
    report["status"] = "PASS"
    report["outcome"] = "SMOKE_BUSINESS_PASS"
    report["acceptance"] = "SMOKE_BUSINESS_COMPONENT"
    return EXIT_SMOKE_PASS, report


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    ap = argparse.ArgumentParser(description="Bounded smoke business validator (no provenance lane gate)")
    ap.add_argument("--validator-root", required=False, help="path to frozen campaign-helper/validators")
    ap.add_argument("--lane", choices=("baseline", "recovery"))
    ap.add_argument("--kafka")
    ap.add_argument("--expected-events")
    ap.add_argument("--expected-final-state")
    ap.add_argument("--oracle-pin")
    ap.add_argument("--flink-sink")
    ap.add_argument("--capture-manifest")
    ap.add_argument("--execution-receipts")
    ap.add_argument("--measurement-window")
    ap.add_argument("--qualification-evidence")
    ap.add_argument("--smoke-slice", help="MAIN-supplied smoke-slice-v2 (cut + manifest bindings)")
    ap.add_argument(
        "--pre-workload-capture-manifest",
        help="original full capture manifest at pre-workload assigned/end cut (lower bound source)",
    )
    ap.add_argument("--contract-version")
    ap.add_argument("--workload-mode", choices=("smoke", "campaign"))
    ap.add_argument("--json-out")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args(argv)

    if args.self_test:
        from smoke_business_regressions import run_all

        return run_all()

    if not args.validator_root or not args.lane:
        print(json.dumps({"status": "USAGE", "failures": [{"code": "USAGE", "detail": "flags required"}]}))
        return EXIT_USAGE

    code, report = validate_smoke_business(
        validator_root=args.validator_root,
        lane=args.lane,
        kafka_path=args.kafka or "",
        expected_path=args.expected_events or "",
        expected_final_state_path=args.expected_final_state or "",
        oracle_pin_path=args.oracle_pin or "",
        flink_sink_path=args.flink_sink or "",
        capture_manifest_path=args.capture_manifest or "",
        execution_receipts_path=args.execution_receipts or "",
        measurement_window_path=args.measurement_window or "",
        qualification_evidence_path=args.qualification_evidence or "",
        smoke_slice_path=args.smoke_slice or "",
        pre_workload_capture_manifest_path=args.pre_workload_capture_manifest or "",
        contract_version=args.contract_version,
        workload_mode=args.workload_mode,
        invoked_argv=argv,
    )
    text = json.dumps(report, indent=2, ensure_ascii=False)
    print(text)
    if args.json_out:
        Path(args.json_out).write_text(text + "\n", encoding="utf-8")
    return code


if __name__ == "__main__":
    sys.exit(main())

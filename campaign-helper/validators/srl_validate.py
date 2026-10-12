"""Core validation orchestration."""

from __future__ import annotations

from typing import Any

from srl_contracts import CONTRACT_VERSION, EXPECTED_OPS_CONTRACT_VERSION, FORBIDDEN_TAGS, TABLE_COLUMNS
from srl_capture_manifest import (
    index_jsonl_capture,
    load_capture_manifest,
    verify_capture_manifest_against_jsonl,
)
from srl_flink import compare_final_states, flink_rows, load_flink_sink
from srl_journal import load_journal
from srl_kafka import load_kafka_capture
from srl_match import forbidden_leaked, ops_match
from srl_order import verify_per_key_ordered_transitions
from srl_oracle import build_expected_final_from_records, load_expected_final_state, load_expected_oracle
from srl_hashes import sha256_file
from srl_oracle_qualify import qualify_oracle
from srl_runtime_gates import run_runtime_gates
from srl_provenance import (
    COMPUTED_CAMPAIGN_STATUS,
    COMPUTED_INDEPENDENT_SOURCE_QUALIFICATION,
    COMPUTED_PREARCHIVE_STATUS,
    PROVENANCE_INPUT_INCOMPLETE_CODES,
    extract_untrusted_declared_qualifiers,
    load_provenance_evidence,
    verify_provenance_for_acceptance,
)
from srl_receipt import load_execution_receipts
from srl_source_envelope import verify_envelope_source_stock37
from srl_kafka import load_lane_capture_rows


EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_INPUT = 2
EXIT_USAGE = 3

_BUSINESS_FAIL_CODES = frozenset(
    {
        "MISSING_OPS",
        "EXTRA_OPS",
        "CAUSAL_TRANSITION",
        "ROLLBACK_LEAK",
        "FINAL_STATE_EMPTY",
        "FLINK_SINK_PARSE",
        "FLINK_FINAL_STATE",
        "MISSING_FLINK_SINK",
        "CAPTURE_COVERAGE",
        "CAPTURE_INCOMPLETE",
        "CAPTURE_ZERO_RECORDS",
        "CAPTURE_PARTITION_COUNT",
        "CAPTURE_OFFSET_RANGE",
        "CAPTURE_MANIFEST_PARSE",
        "CAPTURE_EMPTY_PARTITION",
        "CAPTURE_ISOLATION",
    }
)

_SOURCE_RUNTIME_CODES = frozenset(
    {
        "SOURCE_ENVELOPE_MISSING",
        "SOURCE_SCHEMA_MISMATCH",
        "SOURCE_TABLE_MISMATCH",
        "SOURCE_TABLE_MISSING",
        "SOURCE_METADATA_MISSING",
    }
)

_PROVENANCE_CONTENT_CODES = frozenset(
    c
    for c in PROVENANCE_INPUT_INCOMPLETE_CODES
    if c
    not in (
        "PROVENANCE_NOT_PRIMARY",
        "PROVENANCE_EVIDENCE_MISSING",
        "PROVENANCE_EVIDENCE_INVALID",
    )
)


def _attach_acceptance_dimensions(
    report: dict[str, Any],
    *,
    acceptance_mode: str,
    prov_doc: dict[str, Any] | None,
    prov_issues: list[dict[str, str]],
) -> None:
    failures = report.get("failures") or []
    business_hits = [f for f in failures if f.get("code") in _BUSINESS_FAIL_CODES]
    source_hits = [f for f in failures if f.get("code") in _SOURCE_RUNTIME_CODES]
    prov_content_hits = [f for f in failures if f.get("code") in _PROVENANCE_CONTENT_CODES]
    if prov_issues and not prov_content_hits:
        prov_content_hits = list(prov_issues)

    declared = extract_untrusted_declared_qualifiers(prov_doc or {})
    report["acceptance_dimensions"] = {
        "business_correctness": "PASS" if not business_hits else "FAIL",
        "provenance_content_consistency": (
            "PASS"
            if not prov_content_hits and not source_hits and prov_doc is not None
            else "INCOMPLETE"
            if prov_doc is not None
            else "UNKNOWN"
        ),
        "independent_source_qualification": COMPUTED_INDEPENDENT_SOURCE_QUALIFICATION,
        "prearchive_status": COMPUTED_PREARCHIVE_STATUS,
        "campaign_status": COMPUTED_CAMPAIGN_STATUS,
    }
    if declared:
        report["untrusted_declared_qualifiers"] = declared
    if acceptance_mode == "component":
        report["acceptance"] = "TEST_ONLY_COMPONENT"
    elif acceptance_mode == "live":
        report["acceptance"] = "not_campaign_accepted"


def _detect_forbidden_tags(business_only) -> list[str]:
    hits: list[str] = []
    for op in business_only:
        for side in (op.after, op.before):
            if not side:
                continue
            tag = side.get("TAG")
            if tag and str(tag) in FORBIDDEN_TAGS:
                hits.append(str(tag))
    return hits


def validate_lane(
    *,
    lane: str,
    journal_path: str | None,
    kafka_path: str | None,
    expected_path: str | None,
    expected_final_state_path: str | None,
    oracle_pin_path: str | None,
    flink_sink_path: str | None,
    capture_manifest_path: str | None,
    contract_version: str,
    require_capture_manifest: bool = True,
    skip_oracle_qualification: bool = False,
    manifest_require_isolation: bool = True,
    execution_receipts_path: str | None = None,
    measurement_window_path: str | None = None,
    qualification_evidence_path: str | None = None,
    require_runtime_gates: bool = True,
    workload_mode: str | None = None,
    provenance_evidence_path: str | None = None,
    acceptance_mode: str = "live",
    invoked_argv: list[str] | None = None,
) -> tuple[int, dict[str, Any]]:
    report: dict[str, Any] = {
        "status": "FAIL",
        "contract_version": contract_version,
        "expected_ops_contract": EXPECTED_OPS_CONTRACT_VERSION,
        "lane": lane,
        "checks": [],
        "counts": {},
        "qualification": {"status": "unknown", "issues": []},
        "runtime_gates": {"status": "unknown", "issues": []},
        "provenance": {"status": "unknown", "issues": []},
        "acceptance": "not_campaign_accepted",
        "acceptance_dimensions": {
            "business_correctness": "UNKNOWN",
            "provenance_content_consistency": "UNKNOWN",
            "independent_source_qualification": "UNVERIFIED",
            "prearchive_status": "UNKNOWN",
            "campaign_status": "NOT_ACCEPTED",
        },
        "invoked_argv": invoked_argv or [],
        "failures": [],
    }

    if contract_version != CONTRACT_VERSION:
        report["status"] = "INPUT_INCOMPLETE"
        report["failures"].append(
            {"code": "CONTRACT_VERSION_MISMATCH", "detail": f"need {CONTRACT_VERSION}"}
        )
        return EXIT_INPUT, report

    if lane not in ("baseline", "recovery"):
        report["failures"].append({"code": "UNKNOWN_LANE", "detail": lane})
        return EXIT_USAGE, report

    if not kafka_path:
        report["status"] = "INPUT_INCOMPLETE"
        report["failures"].append({"code": "MISSING_KAFKA", "detail": "--kafka required"})
        return EXIT_INPUT, report

    if not expected_path:
        report["status"] = "INPUT_INCOMPLETE"
        report["failures"].append({"code": "INPUT_INCOMPLETE", "detail": "missing --expected-events"})
        return EXIT_INPUT, report

    oracle = load_expected_oracle(expected_path, oracle_pin_path)
    oracle = load_expected_final_state(expected_final_state_path, oracle)
    if oracle.errors:
        report["status"] = "INPUT_INCOMPLETE"
        for e in oracle.errors:
            report["failures"].append({"code": "ORACLE_INVALID", "detail": e})
        return EXIT_INPUT, report

    if not skip_oracle_qualification:
        q_issues = qualify_oracle(oracle)
        report["qualification"] = {
            "status": "PASS" if not q_issues else "INCOMPLETE",
            "issues": q_issues,
        }
        if q_issues:
            report["status"] = "INPUT_INCOMPLETE"
            for q in q_issues:
                report["failures"].append(
                    {"code": "ORACLE_QUALIFICATION", "detail": f"{q['code']}: {q['detail']}"}
                )
            report["counts"] = {
                "oracle_kafka_committed_events": len(oracle.committed_kafka),
                "oracle_final_state_entries": len(oracle.final_state_entries),
                "oracle_forbidden_records": len(oracle.forbidden),
            }
            return EXIT_INPUT, report

    report["qualification"] = {"status": "PASS", "issues": []}

    mode = workload_mode or (oracle.pin.mode if oracle.pin else "smoke")
    pin_sha = sha256_file(oracle_pin_path) if oracle_pin_path else None
    ops_sha = sha256_file(expected_path) if expected_path else None
    final_sha = sha256_file(expected_final_state_path) if expected_final_state_path else None
    report["runtime_bindings"] = {
        "mode": mode,
        "pin_sha256": pin_sha,
        "dml_sha256": ops_sha,
        "final_state_sha256": final_sha,
    }
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
        require_all=require_runtime_gates,
    )
    report["runtime_gates"] = {"status": rt_summary.get("status", "INCOMPLETE"), "issues": rt_issues}
    if rt_issues:
        report["status"] = "INPUT_INCOMPLETE"
        for q in rt_issues:
            report["failures"].append({"code": "RUNTIME_GATE", "detail": f"{q['code']}: {q['detail']}"})
        report["counts"] = {
            "oracle_kafka_committed_events": len(oracle.committed_kafka),
            "oracle_final_state_entries": len(oracle.final_state_entries),
            "oracle_forbidden_records": len(oracle.forbidden),
        }
        return EXIT_INPUT, report

    try:
        capture = load_kafka_capture(kafka_path, lane)
    except Exception as e:
        report["status"] = "INPUT_INCOMPLETE"
        report["failures"].append({"code": "KAFKA_PARSE", "detail": str(e)})
        return EXIT_INPUT, report

    business_only = [o for o in capture.business_ops if not o.snapshot]
    raw_rows = load_lane_capture_rows(kafka_path, lane)
    for fail in verify_envelope_source_stock37(raw_rows, business_only):
        report["failures"].append(fail)

    prov_doc: dict | None = None
    prov_issues: list[dict[str, str]] = []
    if not provenance_evidence_path:
        report["provenance"] = {
            "status": "INCOMPLETE",
            "issues": [{"code": "PROVENANCE_EVIDENCE_MISSING", "detail": "--provenance-evidence required"}],
        }
        report["failures"].append(
            {"code": "PROVENANCE_EVIDENCE_MISSING", "detail": "external provenance evidence required"}
        )
    else:
        consumer_start_utc: str | None = None
        if measurement_window_path:
            try:
                import json as _json

                with open(measurement_window_path, encoding="utf-8") as _mw:
                    _doc = _json.load(_mw)
                consumer_start_utc = (_doc.get("lanes") or {}).get(lane, {}).get("consumer_start_utc")
            except (OSError, TypeError, ValueError):
                consumer_start_utc = None
        exec_receipts = None
        if execution_receipts_path:
            loaded = load_execution_receipts(execution_receipts_path)
            exec_receipts = loaded.receipts
        prov = load_provenance_evidence(provenance_evidence_path)
        prov_doc = prov.doc
        p_issues = verify_provenance_for_acceptance(
            prov,
            acceptance_mode=acceptance_mode,
            oracle=oracle,
            business_ops=business_only,
            kafka_rows=raw_rows,
            lane=lane,
            measurement_consumer_start_utc=consumer_start_utc,
            execution_receipts=exec_receipts,
        )
        prov_issues = p_issues
        report["provenance"] = {
            "status": "PASS" if not p_issues else "INCOMPLETE",
            "issues": p_issues,
            "evidence_class": prov.doc.get("evidence_class"),
            "independent_source_qualification": COMPUTED_INDEPENDENT_SOURCE_QUALIFICATION,
            "prearchive_status": COMPUTED_PREARCHIVE_STATUS,
            "campaign_status": COMPUTED_CAMPAIGN_STATUS,
            "untrusted_declared_qualifiers": extract_untrusted_declared_qualifiers(prov.doc),
        }
        for q in p_issues:
            report["failures"].append({"code": q["code"], "detail": q["detail"]})
    report["counts"] = {
        "kafka_capture_business_ops": len(business_only),
        "kafka_transport_tombstones": capture.transport_tombstones,
        "tombstones": capture.tombstones,
        "transaction_metadata": capture.transaction_metadata,
        "oracle_kafka_committed_events": len(oracle.committed_kafka),
        "oracle_final_state_entries": len(oracle.final_state_entries),
        "oracle_forbidden_records": len(oracle.forbidden),
    }

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
        report["failures"].append(
            {"code": "MISSING_OPS", "detail": f"unmatched op_ids: {unmatched_req[:8]}"}
        )
    if pool:
        report["failures"].append(
            {"code": "EXTRA_OPS", "detail": f"unexpected kafka ops: {len(pool)} rows"}
        )
    if not unmatched_req and not pool:
        report["checks"].append("committed_multiset")

    for fail in verify_per_key_ordered_transitions(oracle.committed_kafka, business_only):
        report["failures"].append(fail)

    for tag in _detect_forbidden_tags(business_only):
        report["failures"].append(
            {"code": "ROLLBACK_LEAK", "detail": f"forbidden TAG {tag} in kafka capture"}
        )
    for forb in oracle.forbidden:
        for act in business_only:
            if forbidden_leaked(forb, act):
                report["failures"].append(
                    {
                        "code": "ROLLBACK_LEAK",
                        "detail": f"committed=false op {forb.op_id} leaked to kafka",
                    }
                )

    expected_final = build_expected_final_from_records(oracle)
    if not any(expected_final[t] for t in expected_final) and oracle.final_state_entries:
        report["failures"].append(
            {"code": "FINAL_STATE_EMPTY", "detail": "no present rows in final_state artifact"}
        )

    if flink_sink_path:
        sink = load_flink_sink(flink_sink_path)
        for e in sink.errors:
            report["failures"].append({"code": "FLINK_SINK_PARSE", "detail": e})
        actual_final = flink_rows(sink)
        if not any(actual_final[t] for t in actual_final) and any(
            expected_final[t] for t in expected_final
        ):
            report["failures"].append(
                {"code": "FLINK_FINAL_STATE", "detail": "sink empty but oracle expects rows"}
            )
        fs_errs = compare_final_states(expected_final, actual_final, oracle.absent_ids)
        for err in fs_errs:
            report["failures"].append({"code": "FLINK_FINAL_STATE", "detail": err})
        if not fs_errs and oracle.final_state_entries and any(actual_final[t] for t in actual_final):
            report["checks"].append("flink_final_state")
    else:
        report["failures"].append({"code": "MISSING_FLINK_SINK", "detail": "--flink-sink required"})

    if capture_manifest_path:
        manifest = load_capture_manifest(capture_manifest_path)
        jsonl_idx = index_jsonl_capture(kafka_path, lane)
        for fail in verify_capture_manifest_against_jsonl(
            manifest,
            jsonl_idx,
            lane=lane,
            require_isolation=manifest_require_isolation,
        ):
            report["failures"].append(
                {"code": fail["code"], "detail": fail["detail"]}
            )
        if not report["failures"] or all(
            f.get("code") not in ("CAPTURE_INCOMPLETE", "CAPTURE_ZERO_RECORDS") for f in report["failures"]
        ):
            if manifest.complete and manifest.records > 0:
                report["checks"].append("capture_manifest")
    elif require_capture_manifest:
        report["failures"].append({"code": "CAPTURE_COVERAGE", "detail": "--capture-manifest required"})

    if journal_path:
        js = load_journal(journal_path, contract_version)
        report["counts"]["journal_lines"] = js.line_count

    hard_input = any(
        f.get("code")
        in (
            "KAFKA_PARSE",
            "ORACLE_INVALID",
            "INPUT_INCOMPLETE",
            "MISSING_KAFKA",
            "ORACLE_QUALIFICATION",
            "RUNTIME_GATE",
        )
        for f in report["failures"]
    )
    runtime_input = any(
        f.get("code")
        in (
            "SOURCE_ENVELOPE_MISSING",
            "SOURCE_SCHEMA_MISMATCH",
            "SOURCE_TABLE_MISMATCH",
            "SOURCE_TABLE_MISSING",
            "SOURCE_METADATA_MISSING",
            "PROVENANCE_EVIDENCE_MISSING",
            "PROVENANCE_NOT_PRIMARY",
        )
        for f in report["failures"]
    ) or any(f.get("code") in PROVENANCE_INPUT_INCOMPLETE_CODES for f in report["failures"])
    if runtime_input:
        report["status"] = "INPUT_INCOMPLETE"
        _attach_acceptance_dimensions(
            report, acceptance_mode=acceptance_mode, prov_doc=prov_doc, prov_issues=prov_issues
        )
        return EXIT_INPUT, report
    if hard_input:
        report["status"] = "INPUT_INCOMPLETE"
        _attach_acceptance_dimensions(
            report, acceptance_mode=acceptance_mode, prov_doc=prov_doc, prov_issues=prov_issues
        )
        return EXIT_INPUT, report

    if report["failures"]:
        report["status"] = "FAIL"
        _attach_acceptance_dimensions(
            report, acceptance_mode=acceptance_mode, prov_doc=prov_doc, prov_issues=prov_issues
        )
        return EXIT_FAIL, report

    report["status"] = "PASS"
    _attach_acceptance_dimensions(
        report, acceptance_mode=acceptance_mode, prov_doc=prov_doc, prov_issues=prov_issues
    )
    return EXIT_PASS, report

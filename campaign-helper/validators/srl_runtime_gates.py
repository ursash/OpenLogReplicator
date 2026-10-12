"""Receipt + measurement window + qualification evidence runtime gates."""

from __future__ import annotations

from typing import Any

from srl_measurement_window import MeasurementWindow, load_measurement_window, verify_measurement_window_issues
from srl_oracle import ExpectedOracle
from srl_qualification_evidence import (
    QualificationEvidence,
    forbidden_rollback_key_ids,
    load_qualification_evidence,
    verify_evidence_bindings,
    verify_evidence_counts,
)
from srl_receipt import ExecutionReceipt, load_execution_receipts, receipt_index
from srl_contracts import RUN_ID


SMOKE_REQUIRED: list[tuple[str, str]] = [
    ("P099", "phase_boundary"),
    ("P100", "phase_boundary"),
    ("P103", "key_anchor"),
    ("P104", "sql_commit"),
    ("P101", "sql_commit"),
    ("P102", "sql_commit"),
    ("P105", "phase_boundary"),
]

CAMPAIGN_EXTRA: list[tuple[str, str]] = [
    ("P107", "barrier"),
    ("P107", "key_anchor"),
    ("P106", "phase_boundary"),
]


def verify_receipts_for_mode(
    receipts: list[ExecutionReceipt],
    *,
    mode: str,
    run_id: str = RUN_ID,
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    for r in receipts:
        if r.run_id != run_id:
            issues.append(
                {
                    "code": "RECEIPT_RUN_ID_MISMATCH",
                    "detail": f"line {r.line_no}: run_id {r.run_id!r}",
                }
            )
        if r.raw.get("sql_exit") is not None and int(r.raw["sql_exit"]) != 0:
            issues.append(
                {
                    "code": "RECEIPT_SQL_FAILED",
                    "detail": f"line {r.line_no}: sql_exit={r.raw.get('sql_exit')}",
                }
            )
    idx = receipt_index(receipts)
    need = list(SMOKE_REQUIRED)
    if mode == "campaign":
        need.extend(CAMPAIGN_EXTRA)
    for phase, rtype in need:
        if not idx.get((phase, rtype)):
            issues.append(
                {
                    "code": "RECEIPT_MISSING_PHASE",
                    "detail": f"missing {rtype} for {phase}",
                }
            )
    hosts = {str(r.host) for r in receipts if r.host}
    if "RAC1" not in hosts or "RAC2" not in hosts:
        issues.append(
            {
                "code": "DUAL_INSTANCE_COVERAGE_MISSING",
                "detail": f"receipt hosts need RAC1 and RAC2, got {sorted(hosts)}",
            }
        )
    for r in receipts:
        if r.receipt_type == "sql_commit":
            if r.raw.get("sql_exit") is None:
                issues.append(
                    {
                        "code": "RECEIPT_SQL_EXIT_MISSING",
                        "detail": f"line {r.line_no}: sql_commit requires sql_exit=0",
                    }
                )
            elif int(r.raw["sql_exit"]) != 0:
                issues.append(
                    {
                        "code": "RECEIPT_SQL_FAILED",
                        "detail": f"line {r.line_no}: sql_exit={r.raw.get('sql_exit')}",
                    }
                )
        try:
            from srl_dates import parse_utc_strict

            parse_utc_strict(r.utc, field=f"receipt line {r.line_no} utc")
        except ValueError as e:
            issues.append({"code": "RECEIPT_DATE_INVALID", "detail": str(e)})
    if mode == "campaign":
        barriers = [r for r in receipts if r.phase_norm == "P107" and r.receipt_type == "barrier"]
        notes = " ".join(str(r.raw.get("note", "")) for r in barriers).lower()
        for token in ("long_tx", "log_switch"):
            if token.replace("_", " ") not in notes and token not in notes:
                issues.append(
                    {
                        "code": "BARRIER_MISSING_CAMPAIGN",
                        "detail": f"P107 barrier missing {token} proof",
                    }
                )
    return issues


def verify_rollback_receipts(
    receipts: list[ExecutionReceipt], oracle: ExpectedOracle
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    keys = forbidden_rollback_key_ids(oracle)
    anchors = [r for r in receipts if r.phase_norm == "P103" and r.receipt_type == "key_anchor"]
    if not anchors:
        return issues
    payload = anchors[-1].raw.get("payload")
    if not isinstance(payload, dict):
        issues.append(
            {"code": "ROLLBACK_ID_NOT_RECEIPTED", "detail": "P103 key_anchor payload missing object"}
        )
        return issues
    partial = payload.get("partial_rollback_key")
    full_keys = payload.get("full_rollback_keys") or []
    if keys["rollback_partial"]:
        exp_p = keys["rollback_partial"][0]
        if partial != exp_p:
            issues.append(
                {
                    "code": "ROLLBACK_ID_NOT_RECEIPTED",
                    "detail": f"partial rollback key {exp_p} not in key_anchor payload",
                }
            )
    if keys["rollback_full"]:
        full_set = set(int(x) for x in full_keys if x is not None)
        for kid in keys["rollback_full"]:
            if int(kid) not in full_set:
                issues.append(
                    {
                        "code": "ROLLBACK_ID_NOT_RECEIPTED",
                        "detail": f"full rollback key {kid} not receipted",
                    }
                )
    return issues


def verify_measurement_window_lane(mw: MeasurementWindow, lane: str) -> list[dict[str, str]]:
    issues = verify_measurement_window_issues(mw)
    if issues:
        return issues
    return []


def run_runtime_gates(
    *,
    lane: str,
    mode: str,
    oracle: ExpectedOracle,
    receipts_path: str | None,
    measurement_window_path: str | None,
    qualification_evidence_path: str | None,
    pin_path: str | None,
    ops_path: str | None,
    final_path: str | None,
    pin_sha256: str | None,
    ops_sha256: str | None,
    final_sha256: str | None,
    require_all: bool = True,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """Returns (runtime_summary, issues)."""
    summary: dict[str, Any] = {"status": "PASS", "lane": lane, "mode": mode}
    issues: list[dict[str, str]] = []

    missing = []
    if not receipts_path:
        missing.append("--execution-receipts")
    if not measurement_window_path:
        missing.append("--measurement-window")
    if not qualification_evidence_path:
        missing.append("--qualification-evidence")
    if missing and require_all:
        for m in missing:
            issues.append({"code": "RUNTIME_EVIDENCE_MISSING", "detail": m})
        summary["status"] = "INCOMPLETE"
        return summary, issues

    evidence: QualificationEvidence | None = None
    if qualification_evidence_path:
        evidence = load_qualification_evidence(qualification_evidence_path)
        issues.extend(
            verify_evidence_bindings(
                evidence,
                pin_path=pin_path,
                ops_path=ops_path,
                final_path=final_path,
                pin_sha256=pin_sha256,
                ops_sha256=ops_sha256,
                final_sha256=final_sha256,
            )
        )
        if evidence.doc.get("mode") != mode:
            issues.append(
                {
                    "code": "QUALIFICATION_MODE_MISMATCH",
                    "detail": f"evidence mode {evidence.doc.get('mode')!r} != run mode {mode!r}",
                }
            )
        issues.extend(verify_evidence_counts(evidence, oracle))

    recs: list[ExecutionReceipt] = []
    if receipts_path:
        loaded = load_execution_receipts(receipts_path)
        issues.extend({"code": "RECEIPT_PARSE", "detail": e} for e in loaded.errors)
        recs = loaded.receipts
        issues.extend(verify_receipts_for_mode(recs, mode=mode))
        issues.extend(verify_rollback_receipts(recs, oracle))

    mw: MeasurementWindow | None = None
    if measurement_window_path:
        mw = load_measurement_window(measurement_window_path)
        issues.extend(verify_measurement_window_lane(mw, lane))

    if issues:
        summary["status"] = "INCOMPLETE"
    summary["receipt_count"] = len(recs)
    if evidence:
        summary["qualification_evidence"] = evidence.doc.get("contract_version")
    if mw:
        summary["measurement_window"] = mw.contract_version
    return summary, issues

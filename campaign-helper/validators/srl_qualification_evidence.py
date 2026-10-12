"""ORACLE-QUALIFICATION-EVIDENCE.json runtime binding."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from mandatory_schema import QEV_REQUIRED_KEYS, issue, missing_keys
from srl_contracts import EXPECTED_OPS_FINAL_STATE_VERSIONS, RUN_ID
from srl_hashes import sha256_file
from srl_oracle import ExpectedOracle, table_short


@dataclass
class QualificationEvidence:
    doc: dict[str, Any]
    errors: list[str] = field(default_factory=list)


def load_qualification_evidence(path: str) -> QualificationEvidence:
    errors: list[str] = []
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    miss = missing_keys(doc, QEV_REQUIRED_KEYS)
    if miss:
        errors.append(f"qualification evidence missing required keys {miss}")
    extra = [k for k in doc if k not in QEV_REQUIRED_KEYS]
    if extra:
        errors.append(f"qualification evidence unexpected keys {sorted(extra)}")
    cv = doc.get("contract_version")
    if cv not in EXPECTED_OPS_FINAL_STATE_VERSIONS:
        errors.append(f"qualification evidence contract_version {cv!r} unsupported")
    if str(doc.get("run_id", "")) != RUN_ID:
        errors.append("qualification evidence run_id mismatch")
    if doc.get("mode") not in ("smoke", "campaign"):
        errors.append("qualification evidence mode must be smoke|campaign")
    cap = doc.get("capture_manifest_requirements") or {}
    if cap.get("isolation_level") != "read_committed" or cap.get("complete") is not True:
        errors.append("capture_manifest_requirements incomplete")
    for path_key in ("pin_ref", "dml_artifact", "final_state_artifact"):
        p = doc.get(path_key)
        if not p:
            continue
        from pathlib import Path

        if path_key != "pin_ref" and not Path(str(p)).is_file() and not str(p).startswith("mem:"):
            errors.append(f"{path_key} not a readable file: {p}")
    return QualificationEvidence(doc=doc, errors=errors)


def oracle_committed_counts(oracle: ExpectedOracle) -> Counter[str]:
    c: Counter[str] = Counter()
    for op in oracle.committed_kafka:
        c[op.phase] += 1
    return c


def oracle_table_counts(oracle: ExpectedOracle) -> Counter[tuple[str, str]]:
    c: Counter[tuple[str, str]] = Counter()
    for op in oracle.committed_kafka:
        c[(op.phase, op.table)] += 1
    return c


def oracle_forbidden_counts(oracle: ExpectedOracle) -> Counter[str]:
    c: Counter[str] = Counter()
    for op in oracle.forbidden:
        rb = op.rollback or ""
        if rb == "full_transaction":
            c["rollback_full"] += 1
        elif rb == "savepoint_partial":
            c["rollback_partial"] += 1
    return c


def forbidden_rollback_key_ids(oracle: ExpectedOracle) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {"rollback_full": [], "rollback_partial": []}
    for op in oracle.forbidden:
        kid = op.key.get("ID")
        if kid is None:
            continue
        rb = op.rollback or ""
        if rb == "full_transaction":
            out["rollback_full"].append(int(kid))
        elif rb == "savepoint_partial":
            out["rollback_partial"].append(int(kid))
    return out


def verify_evidence_bindings(
    evidence: QualificationEvidence,
    *,
    pin_path: str | None,
    ops_path: str | None,
    final_path: str | None,
    pin_sha256: str | None = None,
    ops_sha256: str | None = None,
    final_sha256: str | None = None,
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    doc = evidence.doc
    if evidence.errors:
        for e in evidence.errors:
            issues.append({"code": "QUALIFICATION_EVIDENCE_INVALID", "detail": e})
        return issues

    if pin_sha256 and doc.get("pin_sha256") != pin_sha256:
        issues.append({"code": "RUNTIME_PIN_HASH_MISMATCH", "detail": "pin_sha256 mismatch"})
    if ops_sha256 and doc.get("dml_sha256") != ops_sha256:
        issues.append({"code": "RUNTIME_OPS_HASH_MISMATCH", "detail": "dml_sha256 mismatch"})
    if final_sha256 and doc.get("final_state_sha256") != final_sha256:
        issues.append({"code": "RUNTIME_FINAL_HASH_MISMATCH", "detail": "final_state_sha256 mismatch"})

    if pin_path and sha256_file(pin_path) != sha256_file(str(doc.get("pin_ref", ""))):
        if doc.get("pin_ref") and pin_path != doc.get("pin_ref"):
            issues.append(
                {
                    "code": "QUALIFICATION_PIN_PATH_MISMATCH",
                    "detail": f"pin path {pin_path!r} != evidence pin_ref",
                }
            )
    return issues


def verify_evidence_counts(
    evidence: QualificationEvidence, oracle: ExpectedOracle
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    doc = evidence.doc
    exp_comm = doc.get("expected_committed_counts") or {}
    got_comm = oracle_committed_counts(oracle)
    for ph, need in exp_comm.items():
        if ph.startswith("rollback"):
            continue
        if int(need) != got_comm.get(ph, 0):
            issues.append(
                {
                    "code": "QUALIFICATION_COUNT_MISMATCH",
                    "detail": f"phase {ph}: evidence {need} != oracle {got_comm.get(ph, 0)}",
                }
            )
    exp_forb = doc.get("expected_forbidden_counts") or {}
    got_forb = oracle_forbidden_counts(oracle)
    for k in ("rollback_full", "rollback_partial"):
        if k in exp_forb and int(exp_forb[k]) != got_forb.get(k, 0):
            issues.append(
                {
                    "code": "QUALIFICATION_FORBIDDEN_MISMATCH",
                    "detail": f"{k}: evidence {exp_forb[k]} != oracle {got_forb.get(k, 0)}",
                }
            )
    exp_tables = doc.get("expected_committed_table_counts") or {}
    if exp_tables:
        got_t = oracle_table_counts(oracle)
        for key, need in exp_tables.items():
            phase, table = key.split("/", 1) if "/" in key else (key, "EV")
            table = table_short(table)
            if int(need) != got_t.get((phase, table), 0):
                issues.append(
                    {
                        "code": "QUALIFICATION_TABLE_COUNT_MISMATCH",
                        "detail": f"{phase}/{table}: evidence {need} != oracle {got_t.get((phase, table), 0)}",
                    }
                )
    mode = doc.get("mode")
    if mode == "smoke" and got_comm.get("P107"):
        issues.append({"code": "QUALIFICATION_MODE_MISMATCH", "detail": "smoke evidence but P107 ops present"})
    if mode == "campaign" and not got_comm.get("P107"):
        issues.append(
            {"code": "QUALIFICATION_MODE_MISMATCH", "detail": "campaign evidence missing P107 ops"}
        )
    return issues

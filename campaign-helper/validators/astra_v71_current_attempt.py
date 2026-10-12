#!/usr/bin/env python3
"""Astra v71 gap regressions (commit artifact + XID); CURRENT ATTEMPT evidence only."""

from __future__ import annotations

import copy
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mandatory_field_regressions import case, run  # noqa: E402
from srl_hashes import sha256_file  # noqa: E402
from srl_validate import EXIT_FAIL, EXIT_INPUT, EXIT_PASS  # noqa: E402

ORACLE_JL = ROOT / "fixtures" / "oracle-commit.smoke.jsonl"
EVIDENCE = ROOT / "evidence" / "astra-v71-current-attempt-last.json"


def _archive(path: Path) -> None:
    if not path.is_file():
        return
    hist = path.parent / "history"
    hist.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    import shutil

    shutil.copy(path, hist / f"{path.stem}-{stamp}{path.suffix}")


def _patch_commit_line1(d: dict[str, Any], mutate: Callable[[dict], None]) -> None:
    lines = ORACLE_JL.read_text(encoding="utf-8").splitlines()
    rec = json.loads(lines[0])
    mutate(rec)
    lines[0] = json.dumps(rec, separators=(",", ":"))
    ORACLE_JL.write_text("\n".join(lines) + "\n", encoding="utf-8")
    sha = sha256_file(str(ORACLE_JL))
    for b in d["p"]["event_bindings"]:
        ref = b["oracle"]["commit_evidence_ref"]
        if ref.get("locator") == "line:1":
            ref["sha256"] = sha


def _oracle_binding(d: dict, change: Callable[[dict], None]) -> None:
    change(d["p"]["event_bindings"][0]["oracle"])


def run_all() -> int:
    cases: list[dict] = []

    cases.append(
        case(
            "v71_commit_missing_schema",
            lambda d: _patch_commit_line1(d, lambda r: r.pop("schema", None)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_COMMIT_RECORD_INCOMPLETE"],
        )
    )
    cases.append(
        case(
            "v71_commit_schema_other",
            lambda d: _patch_commit_line1(d, lambda r: r.update(schema="OTHER")),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_COMMIT_RECORD_MISMATCH"],
        )
    )
    cases.append(
        case(
            "v71_commit_missing_lane",
            lambda d: _patch_commit_line1(d, lambda r: r.pop("lane", None)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_COMMIT_RECORD_INCOMPLETE"],
        )
    )
    cases.append(
        case(
            "v71_commit_missing_table",
            lambda d: _patch_commit_line1(d, lambda r: r.pop("table", None)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_COMMIT_RECORD_INCOMPLETE"],
        )
    )
    cases.append(
        case(
            "v71_commit_missing_key",
            lambda d: _patch_commit_line1(d, lambda r: r.pop("key", None)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_COMMIT_RECORD_INCOMPLETE"],
        )
    )
    cases.append(
        case(
            "v71_commit_missing_op",
            lambda d: _patch_commit_line1(d, lambda r: r.pop("op", None)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_COMMIT_RECORD_INCOMPLETE"],
        )
    )
    cases.append(
        case(
            "v71_commit_wrong_after_val",
            lambda d: _patch_commit_line1(
                d,
                lambda r: r.update(
                    after=copy.deepcopy(r.get("after") or {}) | {"VAL": 999999}
                ),
            ),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_COMMIT_RECORD_MISMATCH"],
        )
    )

    def kafka_bad_val(d: dict) -> None:
        import mandatory_field_regressions as mfr

        row = d["k"][0]
        val = json.loads(row["value_raw"])
        if isinstance(val.get("after"), dict):
            val["after"] = dict(val["after"])
            val["after"]["VAL"] = 999999
        row["value_raw"] = json.dumps(val, separators=(",", ":"))

    cases.append(
        case(
            "v71_business_kafka_wrong_val_exit_fail",
            kafka_bad_val,
            expect_exit=EXIT_FAIL,
            expect_status="FAIL",
            codes=["CAUSAL_TRANSITION"],
        )
    )

    cases.append(
        case(
            "v71_xid_dual_raw_alias_contradiction",
            lambda d: _oracle_binding(
                d,
                lambda o: (
                    o.__setitem__("raw_xid_hex", "0000000000000000")
                    if o.get("raw_xid_bytes_hex")
                    else None
                ),
            ),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_XID_RAW_INCONSISTENT"],
        )
    )
    cases.append(
        case(
            "v71_xid_fractional_xidusn",
            lambda d: _oracle_binding(d, lambda o: o.__setitem__("xidusn", 36865.9)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_XID_MISMATCH"],
        )
    )
    cases.append(
        case(
            "v71_xid_bool_xidusn",
            lambda d: _oracle_binding(d, lambda o: o.__setitem__("xidusn", True)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_XID_MISMATCH"],
        )
    )
    cases.append(
        case(
            "v71_xid_usn_overflow",
            lambda d: _oracle_binding(d, lambda o: o.__setitem__("xidusn", 1 << 16)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_XID_MISMATCH"],
        )
    )
    cases.append(
        case(
            "v71_xid_positive_single_raw_bytes_only_on_binding",
            lambda d: _oracle_binding(
                d,
                lambda o: (
                    o.pop("xidusn", None),
                    o.pop("xidslot", None),
                    o.pop("xidsqn", None),
                ),
            ),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_ORACLE_FIELD_NULL"],
        )
    )

    summary = {
        "suite": "astra-v71-current-attempt",
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scope": ["commit_original_artifact_identity", "oracle_xid_encoding"],
        "overall_pass": all(c.get("assert_ok", False) for c in cases),
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

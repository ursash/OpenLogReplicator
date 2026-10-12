#!/usr/bin/env python3
"""Astra pure-memory mutation harness + mandatory field-deletion regressions."""

from __future__ import annotations

import builtins
import copy
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import self_test as st
from srl_xid import OracleXid
from srl_capture_manifest import (
    CaptureManifest,
    JsonlCaptureIndex,
    PartitionManifest,
    verify_capture_manifest_against_jsonl,
)
from srl_validate import EXIT_FAIL, EXIT_INPUT, EXIT_PASS, EXIT_USAGE, validate_lane

PROV = ROOT / "fixtures" / "PROVENANCE-EVIDENCE.synthetic.json"
EVIDENCE = ROOT / "evidence" / "mandatory-regressions-last.json"
CAPTURE_BASE_MS = 1791775210000  # 2026-10-12T03:20:10Z aligned with build_runtime_smoke_fixtures


def _archive(path: Path) -> None:
    if not path.is_file():
        return
    hist = path.parent / "history"
    hist.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    import shutil

    shutil.copy(path, hist / f"{path.stem}-{stamp}{path.suffix}")


def _envelope_side(row_dict: dict) -> dict:
    after = row_dict.get("after")
    before = row_dict.get("before")
    key = row_dict.get("key") or {}
    return after or before or key or {}


def _row_value_with_stock37(
    table: str,
    offset: int,
    op: str,
    before,
    after,
    key,
) -> str:
    side = _envelope_side({"after": after, "before": before, "key": key})
    null_fields = set()
    for part in (after, before):
        if isinstance(part, dict):
            null_fields.update(part.get("_null_fields") or [])
    def norm(part):
        if not isinstance(part, dict):
            return part
        out = {k: v for k, v in part.items() if k != "_null_fields"}
        for f in part.get("_null_fields") or []:
            out[str(f)] = None
        return out

    payload = {
        "before": norm(before) if before else before,
        "after": norm(after) if after else after,
        "op": op,
        "source": {
            "db": "ORCL",
            "schema": "OLR_RAC_TEST",
            "table": table,
            "txId": OracleXid(usn=0x9001 + offset, slot=0x0001, sqn=0x1000 + offset).stock37_tx_id(),
            "scn": 2701000 + offset,
            "redo_thread": None,
        },
        "ts_ms": CAPTURE_BASE_MS + offset,
    }
    return json.dumps(payload, separators=(",", ":"))


def build_base() -> dict[str, Any]:
    st.FIX.mkdir(parents=True, exist_ok=True)
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "build_runtime_smoke_fixtures", st.FIX / "build_runtime_smoke_fixtures.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    mod.main()

    k: list[dict] = []
    oracle = st.load_expected_oracle(str(st.OPS), str(st.PIN))
    for off, op in enumerate(oracle.committed_kafka):
        row = st._capture_row(op.table, off, op.op, op.key, op.before, op.after)
        row["value_raw"] = _row_value_with_stock37(
            op.table, off, op.op, op.before, op.after, op.key
        )
        row["timestamp"] = CAPTURE_BASE_MS + off
        k.append(row)
    base = {
        "k": k,
        "f": st.build_flink_sink(st.OPS, st.PIN, st.FINAL_PRIMARY),
        "r": [json.loads(x) for x in st.REC.read_text().splitlines() if x.strip()],
        "w": json.loads(st.MW.read_text()),
        "q": json.loads(st.QEV.read_text()),
        "p": json.loads(PROV.read_text()),
    }
    for rec in base["r"]:
        if rec.get("receipt_type") == "sql_commit" and "sql_exit" not in rec:
            rec["sql_exit"] = 0
    base["m"] = st._manifest_for(base["k"])
    return base


real_open = builtins.open


def run(mutate: Callable[[dict], None] | None = None, *, acceptance_mode: str = "component"):
    base = build_base()
    d = copy.deepcopy(base)
    skip_prov = False
    if mutate:
        mutate(d)
        skip_prov = bool(d.pop("_skip_prov", False))
    texts = {
        "mem:k": "\n".join(json.dumps(x, separators=(",", ":")) for x in d["k"]),
        "mem:f": "\n".join(json.dumps(x, separators=(",", ":")) for x in d["f"]),
        "mem:r": "\n".join(json.dumps(x, separators=(",", ":")) for x in d["r"]),
        "mem:w": json.dumps(d["w"]),
        "mem:q": json.dumps(d["q"]),
        "mem:p": json.dumps(d["p"]),
        "mem:m": json.dumps(d["m"]),
    }

    def memory_open(path, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode", "r")
        if mode not in ("r", "rb"):
            raise OSError(f"write forbidden in harness: {path}")
        ps = str(path)
        if ps in texts:
            data = texts[ps]
            return io.BytesIO(data.encode()) if mode == "rb" else io.StringIO(data)
        return real_open(path, *args, **kwargs)

    with patch("builtins.open", memory_open):
        return validate_lane(
            lane="baseline",
            journal_path=None,
            kafka_path="mem:k",
            expected_path=str(st.OPS),
            expected_final_state_path=str(st.FINAL_PRIMARY),
            oracle_pin_path=str(st.PIN),
            flink_sink_path="mem:f",
            capture_manifest_path="mem:m",
            contract_version="20261012T031500Z-v1",
            execution_receipts_path="mem:r",
            measurement_window_path="mem:w",
            qualification_evidence_path="mem:q",
            provenance_evidence_path=None if skip_prov else "mem:p",
            require_runtime_gates=True,
            require_capture_manifest=True,
            acceptance_mode=acceptance_mode,
            invoked_argv=["mandatory_field_regressions"],
        )


def source_mutation(d: dict, change: Callable[[dict], None]) -> None:
    for row in d["k"]:
        value = json.loads(row["value_raw"])
        src = value.setdefault("source", {})
        change(src)
        row["value_raw"] = json.dumps(value, separators=(",", ":"))


def case(
    name: str,
    mutate,
    *,
    expect_exit: int,
    expect_status: str,
    codes: list[str],
    acceptance_mode: str = "component",
) -> dict:
    code, report = run(mutate, acceptance_mode=acceptance_mode)
    got_codes: list[str] = []
    for f in report.get("failures", []):
        got_codes.append(str(f.get("code")))
        if f.get("code") == "RUNTIME_GATE" and ": " in str(f.get("detail", "")):
            got_codes.append(str(f["detail"]).split(":", 1)[0])
        if f.get("code") == "PROVENANCE_GATE" and ": " in str(f.get("detail", "")):
            got_codes.append(str(f["detail"]).split(":", 1)[0])
    qual = (report.get("qualification") or {}).get("status")
    errs: list[str] = []
    if code != expect_exit:
        errs.append(f"exit {code}!={expect_exit}")
    if report.get("status") != expect_status:
        errs.append(f"status {report.get('status')!r}!={expect_status!r}")
    for c in codes:
        if c not in got_codes:
            errs.append(f"missing code {c!r} in {got_codes}")
    return {
        "name": name,
        "exit_code": code,
        "status": report.get("status"),
        "qualification_status": qual,
        "failure_codes": got_codes,
        "expected": {"exit_code": expect_exit, "status": expect_status, "failure_codes": codes},
        "assert_ok": not errs,
        "assert_errors": errs,
    }


def _spoof_primary_and_claimed_qualifiers(d: dict) -> None:
    p = d["p"]
    p["evidence_class"] = "primary_stock37"
    p["independent_source_qualification"] = {
        "status": "VERIFIED",
        "verified_by": "untrusted_doc_label_only",
    }
    p["prearchive_status"] = {"status": "PREARCHIVE_CLAIMED"}
    p["campaign_status"] = {"status": "ACCEPTED"}


def _assert_untrusted_declared_label_case(
    name: str,
    mutate,
    *,
    acceptance_mode: str,
    expect_exit: int,
    expect_status: str,
) -> dict:
    code, report = run(mutate, acceptance_mode=acceptance_mode)
    dims = report.get("acceptance_dimensions") or {}
    declared = report.get("untrusted_declared_qualifiers") or {}
    errs: list[str] = []
    if code != expect_exit:
        errs.append(f"exit {code}!={expect_exit}")
    if report.get("status") != expect_status:
        errs.append(f"status {report.get('status')!r}!={expect_status!r}")
    for key, want in (
        ("independent_source_qualification", "UNVERIFIED"),
        ("prearchive_status", "UNKNOWN"),
        ("campaign_status", "NOT_ACCEPTED"),
    ):
        if dims.get(key) != want:
            errs.append(f"acceptance_dimensions.{key}={dims.get(key)!r}, want {want!r}")
    indep = declared.get("independent_source_qualification")
    if not (isinstance(indep, dict) and str(indep.get("status", "")).upper() == "VERIFIED"):
        errs.append(f"untrusted_declared_qualifiers missing spoof VERIFIED: {declared!r}")
    if str((declared.get("campaign_status") or {}).get("status", "")).upper() != "ACCEPTED":
        errs.append("untrusted_declared_qualifiers must echo spoof campaign ACCEPTED")
    if report.get("acceptance") == "live":
        errs.append("acceptance must not be live")
    if acceptance_mode == "component" and report.get("acceptance") != "TEST_ONLY_COMPONENT":
        errs.append(f"acceptance {report.get('acceptance')!r} != TEST_ONLY_COMPONENT")
    if acceptance_mode == "live" and report.get("acceptance") != "not_campaign_accepted":
        errs.append(f"acceptance {report.get('acceptance')!r} != not_campaign_accepted")
    return {
        "name": name,
        "suite": "untrusted-declared-label",
        "exit_code": code,
        "status": report.get("status"),
        "acceptance": report.get("acceptance"),
        "acceptance_dimensions": dims,
        "untrusted_declared_qualifiers": declared,
        "assert_ok": not errs,
        "assert_errors": errs,
    }


def run_untrusted_declared_label_cases() -> list[dict]:
    return [
        _assert_untrusted_declared_label_case(
            "component_synthetic_primary_plus_spoof_verified_labels",
            _spoof_primary_and_claimed_qualifiers,
            acceptance_mode="component",
            expect_exit=EXIT_PASS,
            expect_status="PASS",
        ),
        _assert_untrusted_declared_label_case(
            "live_validate_lane_primary_plus_spoof_verified_labels",
            _spoof_primary_and_claimed_qualifiers,
            acceptance_mode="live",
            expect_exit=EXIT_PASS,
            expect_status="PASS",
        ),
    ]


def run_all() -> int:
    cases: list[dict] = []

    code, report = run()
    dims = report.get("acceptance_dimensions") or {}
    cases.append(
        {
            "name": "positive_component_fixture_not_stock37",
            "exit_code": code,
            "status": report.get("status"),
            "acceptance": report.get("acceptance"),
            "acceptance_dimensions": dims,
            "qualification_status": report.get("qualification", {}).get("status"),
            "expected": {
                "exit_code": EXIT_PASS,
                "status": "PASS",
                "acceptance": "TEST_ONLY_COMPONENT",
                "business_correctness": "PASS",
                "independent_source_qualification": "UNVERIFIED",
                "campaign_status": "NOT_ACCEPTED",
            },
            "assert_ok": code == EXIT_PASS
            and report.get("status") == "PASS"
            and report.get("acceptance") == "TEST_ONLY_COMPONENT"
            and dims.get("business_correctness") == "PASS"
            and dims.get("independent_source_qualification") == "UNVERIFIED"
            and dims.get("campaign_status") == "NOT_ACCEPTED",
            "assert_errors": [],
        }
    )

    cases.append(
        case(
            "negative_source_table_absent",
            lambda d: source_mutation(d, lambda s: s.pop("table", None)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["SOURCE_TABLE_MISSING"],
        )
    )
    cases.append(
        case(
            "negative_source_table_other_prefix",
            lambda d: source_mutation(d, lambda s: s.update(table="OTHER." + str(s.get("table", "EV")))),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["SOURCE_TABLE_MISMATCH"],
        )
    )
    cases.append(
        case(
            "negative_source_schema_absent",
            lambda d: source_mutation(d, lambda s: s.pop("schema", None)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["SOURCE_METADATA_MISSING"],
        )
    )

    def stock37_shape(d):
        source_mutation(
            d,
            lambda s: (s.pop("thread", None), s.update(redo_thread=None)),
        )

    cases.append(
        case(
            "negative_stock37_without_external_provenance",
            lambda d: (stock37_shape(d), d.__setitem__("_skip_prov", True)),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_EVIDENCE_MISSING"],
        )
    )

    def bad_window(d):
        d["w"].pop("source_measured_dml_from_utc", None)
        for slot in d["w"]["lanes"].values():
            slot["consumer_start_utc"] = "not-a-date"
            slot["catch_up_after_p099_utc"] = "2026-10-12T04:00:00.000Z"
            slot["consumer_start_utc"] = "2026-10-12T03:00:00.000Z"

    cases.append(
        case(
            "negative_measurement_window_invalid_combo",
            bad_window,
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["MEASUREMENT_WINDOW_INVALID"],
        )
    )

    def receipts_bad(d):
        for r in d["r"]:
            if r.get("receipt_type") == "sql_commit":
                r.pop("sql_exit", None)
            r["utc"] = "invalid"

    cases.append(
        case(
            "negative_receipts_sql_exit_and_date",
            receipts_bad,
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["RECEIPT_SQL_EXIT_MISSING", "RECEIPT_DATE_INVALID"],
        )
    )

    def unbound_q(d):
        q = d["q"]
        for key in list(q.keys()):
            if key not in (
                "contract_version",
                "run_id",
                "mode",
                "pin_ref",
                "dml_artifact",
                "final_state_artifact",
            ):
                q.pop(key, None)

    cases.append(
        case(
            "negative_qev_incomplete_mandatory_set",
            unbound_q,
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["QUALIFICATION_EVIDENCE_INVALID"],
        )
    )

    cases.append(
        case(
            "negative_capture_empty_partitions",
            lambda d: d["m"].update(partitions=[]),
            expect_exit=EXIT_FAIL,
            expect_status="FAIL",
            codes=["CAPTURE_MANIFEST_PARSE"],
        )
    )

    def bad_bounds(d):
        for p in d["m"]["partitions"]:
            p.update(beginning=999999, end=0, final_position=0, first_offset=5, last_offset=1)

    c = case(
        "negative_capture_invalid_bounds",
        bad_bounds,
        expect_exit=EXIT_FAIL,
        expect_status="FAIL",
        codes=["CAPTURE_OFFSET_RANGE"],
    )
    cases.append(c)

    def bad_pc(d):
        d["m"]["partitions"][0]["captured"] += 1

    cases.append(
        case(
            "negative_capture_partition_count",
            bad_pc,
            expect_exit=EXIT_FAIL,
            expect_status="FAIL",
            codes=["CAPTURE_PARTITION_COUNT"],
        )
    )

    def omit_nulls(d):
        for row in d["k"]:
            value = json.loads(row["value_raw"])
            for side in ("before", "after"):
                if isinstance(value.get(side), dict):
                    value[side] = {k: v for k, v in value[side].items() if v is not None}
            row["value_raw"] = json.dumps(value)

    bc = case(
        "negative_actual_null_column_absent",
        omit_nulls,
        expect_exit=EXIT_FAIL,
        expect_status="FAIL",
        codes=["CAUSAL_TRANSITION"],
    )
    cases.append(bc)

    def prov_relabel_one_binding_live(d):
        d["p"]["evidence_class"] = "primary_stock37"
        d["p"]["event_bindings"] = d["p"]["event_bindings"][:1]

    cases.append(
        case(
            "astra_provenance_label_only_one_binding_live",
            prov_relabel_one_binding_live,
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_BINDING_INCOMPLETE"],
            acceptance_mode="live",
        )
    )

    cases.append(
        case(
            "astra_provenance_empty_witness",
            lambda d: d["p"]["event_bindings"][0].__setitem__("srl_witness", {}),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_WITNESS_EMPTY"],
        )
    )

    cases.append(
        case(
            "astra_provenance_xid_spoof_vs_envelope",
            lambda d: d["p"]["event_bindings"][0]["kafka_source"].__setitem__(
                "txId", "0xdead.0001.00001000"
            ),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_TXID_MISMATCH"],
        )
    )

    cases.append(
        case(
            "astra_provenance_wrong_txid_envelope",
            lambda d: source_mutation(
                d,
                lambda s: s.update(
                    txId=OracleXid(usn=0xBEEF, slot=1, sqn=0x1000).stock37_tx_id()
                ),
            ),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_XID_MISMATCH", "PROVENANCE_TXID_MISMATCH"],
        )
    )

    cases.append(
        case(
            "astra_provenance_witness_thread_spoof",
            lambda d: d["p"]["event_bindings"][0]["srl_witness"].__setitem__("thread", 999),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_WITNESS_IDENTITY"],
        )
    )

    cases.append(
        case(
            "astra_provenance_duplicate_op_id",
            lambda d: d["p"]["event_bindings"].append(copy.deepcopy(d["p"]["event_bindings"][0])),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_BINDING_DUPLICATE"],
        )
    )

    cases.append(
        case(
            "astra_provenance_rollback_binding_flag",
            lambda d: d["p"]["event_bindings"][0].__setitem__("rollback_binding", True),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_ROLLBACK_BINDING"],
        )
    )

    cases.append(
        case(
            "astra_provenance_checkpoint_eq_commit_scn_allowed",
            lambda d: None,
            expect_exit=EXIT_PASS,
            expect_status="PASS",
            codes=[],
        )
    )

    def temporal_sampled_after_kafka(d):
        d["k"][0]["timestamp"] = CAPTURE_BASE_MS - 3_600_000

    cases.append(
        case(
            "negative_provenance_temporal_sampled_after_kafka",
            temporal_sampled_after_kafka,
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_TEMPORAL_ORDER"],
        )
    )

    def temporal_archive_before_sampled(d):
        from srl_hashes import sha256_file

        wit_path = st.FIX / "srl-witness.smoke.jsonl"
        lines = wit_path.read_text().splitlines()
        rec = json.loads(lines[0])
        rec["archive_delivered_utc"] = "2020-01-01T00:00:00.000Z"
        lines[0] = json.dumps(rec, separators=(",", ":"))
        wit_path.write_text("\n".join(lines) + "\n")
        new_sha = sha256_file(str(wit_path))
        for b in d["p"]["event_bindings"]:
            b["srl_witness"]["artifact_ref"]["sha256"] = new_sha

    cases.append(
        case(
            "negative_provenance_temporal_archive_before_sampled",
            temporal_archive_before_sampled,
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_TEMPORAL_ORDER"],
        )
    )

    def primary_fake_witness_sha_live(d):
        d["p"]["evidence_class"] = "primary_stock37"
        for b in d["p"]["event_bindings"]:
            b["srl_witness"]["artifact_ref"]["sha256"] = "0" * 64

    cases.append(
        case(
            "astra_provenance_primary_placeholder_sha_live",
            primary_fake_witness_sha_live,
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_WITNESS_ARTIFACT"],
            acceptance_mode="live",
        )
    )

    cases.append(
        case(
            "astra_live_synthetic_class_rejected",
            lambda d: None,
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_NOT_PRIMARY"],
            acceptance_mode="live",
        )
    )

    cases.append(
        case(
            "astra_provenance_oracle_thread_number_spoof",
            lambda d: d["p"]["event_bindings"][0]["oracle"].__setitem__("thread_number", 99),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_ORACLE_FIELD_MISMATCH"],
        )
    )

    cases.append(
        case(
            "astra_provenance_witness_binding_incarnation_spoof",
            lambda d: (
                d["p"]["event_bindings"][0]["srl_witness"].__setitem__("incarnation", 999),
                d["p"]["event_bindings"][0]["srl_witness"].__setitem__("sequence", 888),
            ),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_WITNESS_BINDING_MISMATCH"],
        )
    )

    cases.append(
        case(
            "astra_provenance_xid_raw_contradicts_triplet",
            lambda d: d["p"]["event_bindings"][0]["oracle"].__setitem__(
                "raw_xid_bytes_hex", "0000000000000000"
            ),
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_XID_RAW_INCONSISTENT"],
        )
    )

    def wrong_coord_for_expected(d):
        b0 = d["p"]["event_bindings"][0]
        b1 = d["p"]["event_bindings"][1]
        b0["kafka_coord"] = copy.deepcopy(b1["kafka_coord"])
        b0["kafka_source"] = copy.deepcopy(b1["kafka_source"])

    cases.append(
        case(
            "astra_provenance_wrong_coord_keeps_envelope_mismatch",
            wrong_coord_for_expected,
            expect_exit=EXIT_INPUT,
            expect_status="INPUT_INCOMPLETE",
            codes=["PROVENANCE_EXPECTED_OP_MISMATCH", "PROVENANCE_KAFKA_COORD_COLLISION"],
        )
    )

    m = CaptureManifest(
        True,
        1,
        "read_committed",
        [
            PartitionManifest("x", 0, 0, 3, 3, 0, -1, -1, exclusive_end_reached=True),
            PartitionManifest("y", 0, 0, 1, 1, 1, 0, 0),
        ],
    )
    idx = JsonlCaptureIndex(1, {("y", 0): [0]})
    eos_fails = verify_capture_manifest_against_jsonl(m, idx, lane="baseline")
    cases.append(
        {
            "name": "positive_eos_zero_visible_partition_subgate",
            "assert_ok": not any(f.get("code") == "CAPTURE_EMPTY_PARTITION" for f in eos_fails),
            "failure_codes": [f.get("code") for f in eos_fails],
            "expected": {"no_code": "CAPTURE_EMPTY_PARTITION"},
            "assert_errors": [] if not any(f.get("code") == "CAPTURE_EMPTY_PARTITION" for f in eos_fails) else ["CAPTURE_EMPTY_PARTITION"],
        }
    )

    from validate_lane import main as cli_main

    def cli(argv):
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            code = cli_main(argv)
        return code, buf.getvalue()

    live_argv = [
        "--lane",
        "baseline",
        "--kafka",
        "mem:k",
        "--expected-events",
        str(st.OPS),
        "--expected-final-state",
        str(st.FINAL_PRIMARY),
        "--oracle-pin",
        str(st.PIN),
        "--flink-sink",
        "mem:f",
        "--capture-manifest",
        "mem:m",
        "--execution-receipts",
        "mem:r",
        "--measurement-window",
        "mem:w",
        "--qualification-evidence",
        "mem:q",
        "--provenance-evidence",
        "mem:p",
    ]
    texts = {
        "mem:k": build_base()["k"],
    }
    base = build_base()
    mem_texts = {
        "mem:k": "\n".join(json.dumps(x, separators=(",", ":")) for x in base["k"]),
        "mem:f": "\n".join(json.dumps(x, separators=(",", ":")) for x in base["f"]),
        "mem:r": "\n".join(json.dumps(x, separators=(",", ":")) for x in base["r"]),
        "mem:w": json.dumps(base["w"]),
        "mem:q": json.dumps(base["q"]),
        "mem:p": json.dumps(base["p"]),
        "mem:m": json.dumps(base["m"]),
    }

    def mem_open(path, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode", "r")
        ps = str(path)
        if ps in mem_texts:
            data = mem_texts[ps]
            return io.StringIO(data) if mode == "r" else io.BytesIO(data.encode())
        return real_open(path, *args, **kwargs)

    with patch("builtins.open", mem_open):
        bypass_code, bypass_out = cli(live_argv + ["--allow-missing-runtime-gates"])
    cases.append(
        {
            "name": "cli_rejects_allow_missing_runtime_gates",
            "exit_code": bypass_code,
            "output_snippet": bypass_out[:200],
            "assert_ok": bypass_code != EXIT_PASS and "TEST_ONLY" in bypass_out,
            "assert_errors": [],
        }
    )

    label_cases = run_untrusted_declared_label_cases()
    import contextlib

    from astra_v71_current_attempt import run_all as run_v71_attempt

    with contextlib.redirect_stdout(io.StringIO()):
        v71_exit = run_v71_attempt()
    core_ok = all(c.get("assert_ok", False) for c in cases)
    label_ok = all(c.get("assert_ok", False) for c in label_cases)
    summary = {
        "suite": "mandatory-field-regressions",
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "astra_agent": "76e760df-b914-4d4e-80e0-46a1cbd739c2",
        "overall_pass": core_ok and label_ok and v71_exit == 0,
        "astra_v71_current_attempt_exit": v71_exit,
        "case_count": len(cases),
        "failed_cases": [c["name"] for c in cases if not c.get("assert_ok")],
        "cases": cases,
        "untrusted_declared_label_suite": {
            "case_count": len(label_cases),
            "overall_pass": label_ok,
            "failed_cases": [c["name"] for c in label_cases if not c.get("assert_ok")],
            "cases": label_cases,
        },
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    _archive(EVIDENCE)
    EVIDENCE.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0 if summary["overall_pass"] else 1  # core 30 + untrusted-declared-label 2


if __name__ == "__main__":
    sys.exit(run_all())

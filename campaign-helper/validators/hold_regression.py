#!/usr/bin/env python3
"""In-memory regressions for Astra HOLD findings (same validate_lane code paths)."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from srl_capture_manifest import (
    CaptureManifest,
    JsonlCaptureIndex,
    PartitionManifest,
    verify_capture_manifest_against_jsonl,
)
from srl_fields import compare_field, compare_rows, materialize_oracle_row_for_table
from srl_flink import compare_final_states, flink_rows
from srl_kafka import ParsedBusinessOp
from srl_match import ops_match
from srl_order import verify_per_key_ordered_transitions
from srl_oracle import ExpectedOp, crosscheck_final_state_vs_dml, load_expected_final_state, load_expected_oracle
from srl_oracle_qualify import qualify_oracle
from srl_validate import validate_lane, EXIT_FAIL, EXIT_INPUT, EXIT_PASS

ROOT = Path(__file__).resolve().parent


def _fail(msg: str) -> None:
    raise AssertionError(msg)


def test_uu_swap_same_labels():
    exp = ExpectedOp(
        "E1", "r", "P101", "EV", "u", True, {"ID": 1},
        {"ID": 1, "INST": 1, "TAG": "A", "VAL": 1, "PAD": "a"},
        {"ID": 1, "INST": 1, "TAG": "A", "VAL": 2, "PAD": "b"},
        "kafka_committed_event", seq_on_key=1,
    )
    exp2 = ExpectedOp(
        "E2", "r", "P101", "EV", "u", True, {"ID": 1},
        {"ID": 1, "INST": 1, "TAG": "A", "VAL": 2, "PAD": "b"},
        {"ID": 1, "INST": 1, "TAG": "A", "VAL": 3, "PAD": "c"},
        "kafka_committed_event", seq_on_key=2,
    )
    act1 = ParsedBusinessOp(1, "t", 0, 0, "EV", "u", {"ID": 1},
        {"ID": 1, "INST": 1, "TAG": "A", "VAL": 2, "PAD": "b"},
        {"ID": 1, "INST": 1, "TAG": "A", "VAL": 3, "PAD": "c"}, False, False)
    act2 = ParsedBusinessOp(1, "t", 0, 1, "EV", "u", {"ID": 1},
        {"ID": 1, "INST": 1, "TAG": "A", "VAL": 1, "PAD": "a"},
        {"ID": 1, "INST": 1, "TAG": "A", "VAL": 2, "PAD": "b"}, False, False)
    fails = verify_per_key_ordered_transitions([exp, exp2], [act1, act2])
    if not fails:
        _fail("u/u content swap must fail CAUSAL_TRANSITION")


def test_manifest_eos_gap_pass():
    manifest = CaptureManifest(
        complete=True, records=3, isolation_level="read_committed",
        partitions=[
            PartitionManifest("t.EV", 0, 0, 100, 100, 3, 96, 98),
        ],
    )
    idx = JsonlCaptureIndex(
        row_count=3,
        by_tp={("t.EV", 0): [96, 97, 98]},
    )
    fails = verify_capture_manifest_against_jsonl(manifest, idx, lane="baseline")
    if fails:
        _fail(f"legal gap offsets should pass: {fails}")


def test_manifest_incomplete_fail():
    manifest = CaptureManifest(
        complete=False,
        records=1,
        isolation_level="read_uncommitted",
        partitions=[
            PartitionManifest("t.EV", 0, 0, 1, 1, 1, 0, 0),
        ],
    )
    idx = JsonlCaptureIndex(row_count=1, by_tp={("t.EV", 0): [0]})
    fails = verify_capture_manifest_against_jsonl(manifest, idx, lane="baseline")
    codes = {f["code"] for f in fails}
    if "CAPTURE_INCOMPLETE" not in codes or "CAPTURE_ISOLATION" not in codes:
        _fail(f"incomplete manifest must fail: {fails}")
    if "CAPTURE_ISOLATION" not in codes:
        _fail("read_uncommitted must fail CAPTURE_ISOLATION")


def test_varchar_no_numeric_coercion():
    if compare_field("EV", "PAD", "001", "1") is None:
        _fail("PAD 001 vs 1 must not match")
    if compare_field("EV", "VAL", "10", 10) is not None:
        _fail("VAL int column should normalize 10")


def test_unavailable_not_wildcard():
    err = compare_rows({"PAD": {"__kind__": "unavailable"}}, {"PAD": "x"}, "EV", ("PAD",))
    if not err:
        _fail("unavailable must not auto-pass")


def test_present_without_after_qualify():
    oracle = load_expected_oracle(str(ROOT / "fixtures" / "workload-expected-operations.smoke.jsonl"), None)
    bad_final = {
        "contract_version": "20261012T031500Z-expected-ops-v2",
        "record_type": "final_key_state",
        "run_id": "20261012T031500Z",
        "entries": [{"table": "OLR_RAC_TEST.EV", "key": {"ID": 1}, "present": True}],
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(bad_final, f)
        path = f.name
    load_expected_final_state(path, oracle)
    if not any("present=true requires after" in e for e in oracle.errors):
        _fail("present=true without after must error at load")


def test_empty_sink_fail():
    expected = {"EV": [{"ID": 1, "INST": 1, "TAG": "T", "VAL": 1, "PAD": "p"}], "PP": []}
    errs = compare_final_states(expected, {"EV": [], "PP": []}, {"EV": [], "PP": []})
    if not errs:
        _fail("empty sink vs expected rows must fail")


def test_truncated_oracle_qualify():
    line = json.dumps({
        "op_id": "E10101", "run_id": "20261012T031500Z", "phase": "P101",
        "table": "OLR_RAC_TEST.EV", "op": "c", "committed": True,
        "key": {"ID": 1}, "before": None,
        "after": {"ID": 1, "INST": 1, "TAG": "T", "VAL": 1, "PAD": "p"},
    })
    with tempfile.TemporaryDirectory() as td:
        ops = Path(td) / "ops.jsonl"
        ops.write_text(line + "\n", encoding="utf-8")
        fin = Path(td) / "fin.json"
        fin.write_text(json.dumps({
            "contract_version": "20261012T031500Z-expected-ops-v2",
            "record_type": "final_key_state", "run_id": "20261012T031500Z",
            "entries": [],
        }), encoding="utf-8")
        o = load_expected_oracle(str(ops), None)
        load_expected_final_state(str(fin), o)
        issues = qualify_oracle(o)
        if not any(i["code"] == "ORACLE_TRUNCATED" for i in issues):
            _fail("single-op oracle must not qualify")


def test_primary_smoke_final_coverage_incomplete():
    ops = ROOT / "fixtures" / "workload-expected-operations.smoke.jsonl"
    fin = ROOT / "fixtures" / "workload-expected-final-state.smoke.incomplete.json"
    pin = ROOT / "fixtures" / "workload-oracle-pin.json"
    if not ops.exists() or not fin.exists():
        return
    o = load_expected_oracle(str(ops), str(pin))
    load_expected_final_state(str(fin), o)
    issues = qualify_oracle(o)
    if not any("final_state coverage" in i.get("detail", "") for i in issues):
        _fail("Primary smoke final omitting SHORT rows must fail qualification")


def run_all(*, verbose: bool = True) -> int:
    tests = [
        test_uu_swap_same_labels,
        test_manifest_eos_gap_pass,
        test_manifest_incomplete_fail,
        test_varchar_no_numeric_coercion,
        test_unavailable_not_wildcard,
        test_present_without_after_qualify,
        test_empty_sink_fail,
        test_truncated_oracle_qualify,
        test_primary_smoke_final_coverage_incomplete,
    ]
    for t in tests:
        t()
    if verbose:
        print(json.dumps({"hold_regression": "PASS", "cases": len(tests)}, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run_all())
    except AssertionError as e:
        print(json.dumps({"hold_regression": "FAIL", "error": str(e)}, indent=2))
        sys.exit(1)

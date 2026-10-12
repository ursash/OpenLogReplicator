"""Self-tests: qualified complete oracle for business paths; Primary incomplete kept separate."""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from srl_fields import materialize_oracle_row
from srl_oracle import load_expected_final_state, load_expected_oracle
from srl_oracle_qualify import qualify_oracle
from srl_validate import EXIT_FAIL, EXIT_INPUT, EXIT_PASS, validate_lane

ROOT = Path(__file__).resolve().parent
FIX = ROOT / "fixtures"
OPS = FIX / "workload-expected-operations.smoke.jsonl"
FINAL_PRIMARY = FIX / "workload-expected-final-state.smoke.json"
FINAL_INCOMPLETE = FIX / "workload-expected-final-state.smoke.incomplete.json"
PIN = FIX / "workload-oracle-pin.json"
REC = FIX / "workload-execution-receipts.smoke.jsonl"
MW = FIX / "MEASUREMENT-WINDOW.smoke.json"
QEV = FIX / "ORACLE-QUALIFICATION-EVIDENCE.smoke.json"
BUILDER_SAMPLES_ROOT = Path(
    "/home/shihao/Projects/oracle-cdc-benchmark/olr-rac/evidence/"
    "live-srl-e2e-20261012T031500Z/manifests/samples"
)
EVIDENCE = ROOT / "evidence" / "self-test-last.json"

MANIFEST_OPS = Path(
    "/home/shihao/Projects/oracle-cdc-benchmark/olr-rac/evidence/"
    "live-srl-e2e-20261012T031500Z/manifests/samples/"
    "workload-expected-operations.smoke.sample.jsonl"
)
MANIFEST_FINAL = Path(
    "/home/shihao/Projects/oracle-cdc-benchmark/olr-rac/evidence/"
    "live-srl-e2e-20261012T031500Z/manifests/samples/"
    "workload-expected-final-state.smoke.sample.json"
)
MANIFEST_PIN = Path(
    "/home/shihao/Projects/oracle-cdc-benchmark/olr-rac/evidence/"
    "live-srl-e2e-20261012T031500Z/manifests/samples/workload-oracle-pin.sample.json"
)

MutateFn = Callable[[list[dict], list[dict]], None]
ManifestMutateFn = Callable[[dict, list[dict]], None]


def _topic(table: str, lane: str = "baseline") -> str:
    return f"ocr370.live.srl.20261012T031500Z.{lane}.OLR_RAC_TEST.{table}"


def _capture_row(
    table: str,
    offset: int,
    op: str,
    key: dict,
    before,
    after,
    *,
    lane: str = "baseline",
    value_raw: str | None = None,
) -> dict:
    row = {
        "topic": _topic(table, lane),
        "partition": 0,
        "kafka_offset": offset,
        "timestamp": 1760239221000 + offset,
        "headers_count": 0,
        "key_raw": json.dumps(materialize_oracle_row(key) or key, separators=(",", ":")),
    }
    if value_raw is not None:
        row["value_raw"] = value_raw
    else:
        side = after or before or key or {}
        inst = side.get("INST", 1)
        thread = 2 if int(inst) == 2 else 1
        row["value_raw"] = json.dumps(
            {
                "before": materialize_oracle_row(before) if before else before,
                "after": materialize_oracle_row(after) if after else after,
                "op": op,
                "source": {
                    "db": "ORCL",
                    "schema": "OLR_RAC_TEST",
                    "table": table,
                    "thread": thread,
                    "snapshot": "false",
                },
                "ts_ms": 1760239221000 + offset,
            },
            separators=(",", ":"),
        )
    return row


def build_kafka_from_oracle(ops_path: Path, pin_path: Path) -> list[dict]:
    oracle = load_expected_oracle(str(ops_path), str(pin_path))
    rows: list[dict] = []
    for off, op in enumerate(oracle.committed_kafka):
        rows.append(
            _capture_row(
                op.table,
                off,
                op.op,
                op.key,
                op.before,
                op.after,
            )
        )
    return rows


def build_flink_sink(ops_path: Path, pin_path: Path, final_path: Path, *, corrupt_pp: bool = False) -> list[dict]:
    oracle = load_expected_oracle(str(ops_path), str(pin_path))
    load_expected_final_state(str(final_path), oracle)
    from srl_oracle import build_expected_final_from_records

    final = build_expected_final_from_records(oracle)
    out: list[dict] = []
    for table, rows in final.items():
        for r in rows:
            row = dict(r)
            if corrupt_pp and table == "PP":
                row["CTR"] = 0
            out.append({"table": table, "row": row})
    for table, ids in oracle.absent_ids.items():
        for kid in ids:
            out.append({"table": table, "key": {"ID": kid}, "deleted": True})
    return out


def _manifest_for(rows: list[dict], *, end_slack: int = 0, lane: str = "baseline", java_collector: bool = False) -> dict:
    import re

    from srl_contracts import LANE_REGEX

    lane_re = re.compile(LANE_REGEX[lane])
    parts: dict[tuple[str, int], dict] = {}
    for r in rows:
        if not lane_re.match(r["topic"]):
            continue
        tp = (r["topic"], int(r["partition"]))
        off = int(r["kafka_offset"])
        slot = parts.setdefault(
            tp,
            {"captured": 0, "first_offset": off, "last_offset": off, "beginning": 0},
        )
        slot["captured"] += 1
        slot["first_offset"] = min(slot["first_offset"], off)
        slot["last_offset"] = max(slot["last_offset"], off)
    partitions = []
    total = 0
    for (topic, partition), s in parts.items():
        end = s["last_offset"] + 1 + end_slack
        part = {
            "topic": topic,
            "partition": partition,
            "final_position": end,
            "first_offset": s["first_offset"],
            "last_offset": s["last_offset"],
        }
        if java_collector:
            part["beginning_offset"] = s["beginning"]
            part["end_offset_exclusive"] = end
            part["captured_count"] = s["captured"]
            part["exclusive_end"] = True
        else:
            part["beginning"] = s["beginning"]
            part["end"] = end
            part["captured"] = s["captured"]
        partitions.append(part)
        total += s["captured"]
    doc = {
        "complete": True,
        "records": total,
        "expect_records": total,
        "isolation_level": "read_committed",
        "partitions": partitions,
    }
    if java_collector:
        doc["isolation_level_configured"] = "read_committed"
        doc["collector_source_sha256"] = "selftest-placeholder-sha256"
    return doc


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")


def _failure_codes(report: dict) -> list[str]:
    codes: list[str] = []
    for f in report.get("failures", []):
        c = str(f.get("code"))
        codes.append(c)
        if c == "RUNTIME_GATE":
            detail = str(f.get("detail", ""))
            if ": " in detail:
                codes.append(detail.split(":", 1)[0])
    return codes


def _run_validate(
    tmp: Path,
    name: str,
    *,
    ops_path: Path,
    pin_path: Path,
    final_path: Path | None,
    kafka_rows: list[dict] | None,
    flink_rows: list[dict] | None,
    manifest: dict | None,
    require_capture_manifest: bool = True,
    receipts_path: Path | None = None,
    measurement_window_path: Path | None = None,
    qualification_evidence_path: Path | None = None,
    require_runtime_gates: bool = True,
) -> tuple[int, dict]:
    kafka_path = tmp / f"{name}-kafka.jsonl"
    flink_path = tmp / f"{name}-flink.jsonl"
    manifest_path = tmp / f"{name}-manifest.json"
    if kafka_rows is not None:
        _write_jsonl(kafka_path, kafka_rows)
    if flink_rows is not None:
        _write_jsonl(flink_path, flink_rows)
    if manifest is not None:
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return validate_lane(
        lane="baseline",
        journal_path=None,
        kafka_path=str(kafka_path) if kafka_rows is not None else str(tmp / "missing-kafka.jsonl"),
        expected_path=str(ops_path),
        expected_final_state_path=str(final_path) if final_path else None,
        oracle_pin_path=str(pin_path),
        flink_sink_path=str(flink_path) if flink_rows is not None else None,
        capture_manifest_path=str(manifest_path) if manifest is not None else None,
        contract_version="20261012T031500Z-v1",
        require_capture_manifest=require_capture_manifest,
        execution_receipts_path=str(receipts_path) if receipts_path else None,
        measurement_window_path=str(measurement_window_path) if measurement_window_path else None,
        qualification_evidence_path=str(qualification_evidence_path) if qualification_evidence_path else None,
        require_runtime_gates=require_runtime_gates,
    )


def _run_business_case(
    name: str,
    tmp: Path,
    *,
    ops_path: Path,
    pin_path: Path,
    final_path: Path,
    expected: dict[str, Any],
    mutate: MutateFn | None = None,
    manifest_mutate: ManifestMutateFn | None = None,
    require_capture_manifest: bool = True,
    java_collector_manifest: bool = False,
    receipts_path: Path | None = None,
    measurement_window_path: Path | None = None,
    qualification_evidence_path: Path | None = None,
    require_runtime_gates: bool = True,
) -> dict[str, Any]:
    receipts_path = receipts_path or REC
    measurement_window_path = measurement_window_path or MW
    qualification_evidence_path = qualification_evidence_path or QEV
    kafka_rows = build_kafka_from_oracle(ops_path, pin_path)
    flink_rows = build_flink_sink(ops_path, pin_path, final_path)
    if mutate:
        mutate(kafka_rows, flink_rows)
    manifest = _manifest_for(kafka_rows, java_collector=java_collector_manifest)
    if manifest_mutate:
        manifest_mutate(manifest, kafka_rows)
    code, report = _run_validate(
        tmp,
        name,
        ops_path=ops_path,
        pin_path=pin_path,
        final_path=final_path,
        kafka_rows=kafka_rows,
        flink_rows=flink_rows,
        manifest=manifest,
        require_capture_manifest=require_capture_manifest,
        receipts_path=receipts_path,
        measurement_window_path=measurement_window_path,
        qualification_evidence_path=qualification_evidence_path,
        require_runtime_gates=require_runtime_gates,
    )
    return _pack_case(name, code, report, expected)


def _pack_case(name: str, code: int, report: dict, expected: dict[str, Any]) -> dict[str, Any]:
    qual = report.get("qualification") or {}
    failures = report.get("failures", [])
    codes = _failure_codes(report)
    out: dict[str, Any] = {
        "name": name,
        "exit_code": code,
        "status": report.get("status"),
        "qualification_status": qual.get("status"),
        "qualification_issues": qual.get("issues", []),
        "failure_codes": codes,
        "failures": failures,
        "expected": expected,
    }
    errs: list[str] = []
    if code != expected["exit_code"]:
        errs.append(f"exit_code {code} != {expected['exit_code']}")
    if report.get("status") != expected["status"]:
        errs.append(f"status {report.get('status')!r} != {expected['status']!r}")
    exp_qual = expected.get("qualification_status")
    if exp_qual is not None and qual.get("status") != exp_qual:
        errs.append(f"qualification {qual.get('status')!r} != {exp_qual!r}")
    for fc in expected.get("failure_codes", []):
        if fc not in codes:
            errs.append(f"missing failure code {fc!r} in {codes}")
    if expected.get("failure_codes_exclusive"):
        allowed = set(expected.get("failure_codes", [])) | set(
            expected.get("failure_codes_optional", [])
        )
        extra = [c for c in codes if c not in allowed]
        if extra:
            errs.append(f"unexpected failure codes {extra}")
    out["assert_ok"] = not errs
    out["assert_errors"] = errs
    return out


def _swap_uu_on_key(kafka_rows: list[dict], key_id: int) -> None:
    idxs: list[int] = []
    for i, row in enumerate(kafka_rows):
        val = json.loads(row["value_raw"])
        kid = json.loads(row["key_raw"]).get("ID")
        if val.get("op") == "u" and kid == key_id:
            idxs.append(i)
    if len(idxs) < 2:
        raise RuntimeError(f"need two u ops on key {key_id}")
    a, b = idxs[0], idxs[1]
    kafka_rows[a], kafka_rows[b] = kafka_rows[b], kafka_rows[a]
    for i, row in enumerate(kafka_rows):
        row["kafka_offset"] = i


def _add_transport_tombstone_after_delete(kafka_rows: list[dict]) -> None:
    insert_at = None
    for i, row in enumerate(kafka_rows):
        val = json.loads(row["value_raw"])
        if val.get("op") == "d":
            insert_at = i + 1
            tomb = dict(row)
            tomb["value_raw"] = ""
            break
    if insert_at is None:
        raise RuntimeError("no delete row for tombstone transport test")
    kafka_rows.insert(insert_at, tomb)
    for i, row in enumerate(kafka_rows):
        row["kafka_offset"] = i


def _archive_evidence(path: Path) -> None:
    if not path.is_file():
        return
    hist = path.parent / "history"
    hist.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    shutil.copy(path, hist / f"{path.stem}-{stamp}{path.suffix}")


def _runtime_bundle(tmp: Path, name: str, *, rec_mut=None, mw_mut=None, qev_mut=None) -> tuple[Path, Path, Path]:
    rp = tmp / f"{name}-receipts.jsonl"
    mp = tmp / f"{name}-mw.json"
    qp = tmp / f"{name}-qev.json"
    shutil.copy(REC, rp)
    shutil.copy(MW, mp)
    shutil.copy(QEV, qp)
    if rec_mut:
        lines = [json.loads(ln) for ln in rp.read_text().splitlines() if ln.strip()]
        rec_mut(lines)
        rp.write_text("\n".join(json.dumps(x, separators=(",", ":")) for x in lines) + "\n")
    if mw_mut:
        doc = json.loads(mp.read_text())
        mw_mut(doc)
        mp.write_text(json.dumps(doc, indent=2) + "\n")
    if qev_mut:
        doc = json.loads(qp.read_text())
        qev_mut(doc)
        qp.write_text(json.dumps(doc, indent=2) + "\n")
    return rp, mp, qp


def run_self_tests() -> int:
    """Legacy entry: mandatory Astra regressions + hold in-memory checks."""
    import hold_regression
    from mandatory_field_regressions import run_all as run_mandatory

    hold_regression.run_all(verbose=False)
    return run_mandatory()


def _run_legacy_self_tests_disabled() -> int:
    FIX.mkdir(parents=True, exist_ok=True)
    shutil.copy(MANIFEST_OPS, OPS)
    shutil.copy(MANIFEST_FINAL, FINAL_PRIMARY)
    shutil.copy(MANIFEST_PIN, PIN)
    if not FINAL_INCOMPLETE.exists():
        raise FileNotFoundError(f"missing {FINAL_INCOMPLETE}")
    import importlib.util

    _brt = FIX / "build_runtime_smoke_fixtures.py"
    _spec = importlib.util.spec_from_file_location("build_runtime_smoke_fixtures", _brt)
    if _spec and _spec.loader:
        _mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        _mod.main()

    tmp = Path(tempfile.mkdtemp(prefix="srl-val-self-"))
    cases: list[dict] = []

    try:
        code_q, rep_q = validate_lane(
            lane="baseline",
            journal_path=None,
            kafka_path=str(tmp / "empty.jsonl"),
            expected_path=str(OPS),
            expected_final_state_path=str(FINAL_INCOMPLETE),
            oracle_pin_path=str(PIN),
            flink_sink_path=None,
            capture_manifest_path=None,
            contract_version="20261012T031500Z-v1",
            require_capture_manifest=False,
            require_runtime_gates=False,
        )
        cases.append(
            _pack_case(
                "qualification_intentional_incomplete_final_fixture",
                code_q,
                rep_q,
                {
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": "INCOMPLETE",
                    "failure_codes": ["ORACLE_QUALIFICATION"],
                },
            )
        )

        import hold_regression

        hold_regression.run_all(verbose=False)
        cases.append(
            {
                "name": "hold_regression",
                "exit_code": 0,
                "status": "PASS",
                "assert_ok": True,
                "assert_errors": [],
                "expected": {"exit_code": 0, "status": "PASS", "failure_codes": []},
            }
        )

        std = {
            "ops_path": OPS,
            "pin_path": PIN,
            "final_path": FINAL_PRIMARY,
        }

        o_complete = load_expected_oracle(str(OPS), str(PIN))
        load_expected_final_state(str(FINAL_PRIMARY), o_complete)
        q_issues = qualify_oracle(o_complete)
        cases.append(
            {
                "name": "qualification_builder_smoke_samples_complete",
                "exit_code": 0 if not q_issues else 2,
                "status": "PASS" if not q_issues else "INCOMPLETE",
                "qualification_status": "PASS" if not q_issues else "INCOMPLETE",
                "qualification_issues": q_issues,
                "failure_codes": [i["code"] for i in q_issues],
                "expected": {
                    "exit_code": 0,
                    "status": "PASS",
                    "qualification_status": "PASS",
                    "failure_codes": [],
                },
                "assert_ok": not q_issues,
                "assert_errors": [] if not q_issues else [f"builder samples must qualify: {q_issues}"],
                "note": "copied from manifests/samples; not an accepted integration freeze",
            }
        )

        cases.append(
            _run_business_case(
                "positive_builder_smoke_samples",
                tmp,
                **std,
                expected={
                    "exit_code": EXIT_PASS,
                    "status": "PASS",
                    "qualification_status": "PASS",
                    "failure_codes": [],
                },
            )
        )

        cases.append(
            _run_business_case(
                "positive_java_collector_manifest_fields",
                tmp,
                **std,
                java_collector_manifest=True,
                expected={
                    "exit_code": EXIT_PASS,
                    "status": "PASS",
                    "qualification_status": "PASS",
                    "failure_codes": [],
                },
            )
        )

        def eos_gap(k, f):
            base = 500
            for i, row in enumerate(k):
                row["kafka_offset"] = base + i * 3

        def eos_manifest(m, k):
            for p in m["partitions"]:
                p["end"] = p["last_offset"] + 100
                p["final_position"] = p["end"]

        cases.append(
            _run_business_case(
                "positive_eos_offset_gap_manifest",
                tmp,
                **std,
                mutate=eos_gap,
                manifest_mutate=eos_manifest,
                expected={
                    "exit_code": EXIT_PASS,
                    "status": "PASS",
                    "qualification_status": "PASS",
                    "failure_codes": [],
                },
            )
        )

        def tomb_transport(k, f):
            _add_transport_tombstone_after_delete(k)

        cases.append(
            _run_business_case(
                "positive_delete_plus_transport_tombstone",
                tmp,
                **std,
                mutate=tomb_transport,
                expected={
                    "exit_code": EXIT_PASS,
                    "status": "PASS",
                    "qualification_status": "PASS",
                    "failure_codes": [],
                },
            )
        )

        def drop_first(k, f):
            if k:
                k.pop(0)

        cases.append(
            _run_business_case(
                "negative_drop_event",
                tmp,
                **std,
                mutate=drop_first,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["MISSING_OPS"],
                },
            )
        )

        def dup(k, f):
            if k:
                k.insert(1, dict(k[0]))
                for i, row in enumerate(k):
                    row["kafka_offset"] = i

        cases.append(
            _run_business_case(
                "negative_duplicate_event",
                tmp,
                **std,
                mutate=dup,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["EXTRA_OPS"],
                },
            )
        )

        def dup_coord(k, f):
            if len(k) > 1:
                k[1]["kafka_offset"] = k[0]["kafka_offset"]

        cases.append(
            _run_business_case(
                "negative_capture_duplicate_coordinate",
                tmp,
                **std,
                mutate=dup_coord,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["CAPTURE_DUPLICATE_COORD"],
                },
            )
        )

        def wrong_key(k, f):
            if k:
                k[0]["key_raw"] = json.dumps({"ID": 9999999})

        cases.append(
            _run_business_case(
                "negative_wrong_key",
                tmp,
                **std,
                mutate=wrong_key,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["MISSING_OPS"],
                },
            )
        )

        def wrong_val(k, f):
            for row in k:
                if "1201984" in row.get("key_raw", ""):
                    v = json.loads(row["value_raw"])
                    if v.get("after") and "VAL" in v["after"]:
                        v["after"]["VAL"] = 999
                        row["value_raw"] = json.dumps(v, separators=(",", ":"))
                        break

        cases.append(
            _run_business_case(
                "negative_wrong_value",
                tmp,
                **std,
                mutate=wrong_val,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["MISSING_OPS"],
                },
            )
        )

        def pad_leak_when_oracle_null(k, f):
            for row in k:
                v = json.loads(row["value_raw"])
                if v.get("op") != "c":
                    continue
                if json.loads(row["key_raw"]).get("ID") != 1401984:
                    continue
                aft = dict(v.get("after") or {})
                aft["PAD"] = "unexpected_pad"
                v["after"] = aft
                row["value_raw"] = json.dumps(v, separators=(",", ":"))
                break

        cases.append(
            _run_business_case(
                "negative_pad_value_when_oracle_null_fields",
                tmp,
                **std,
                mutate=pad_leak_when_oracle_null,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["MISSING_OPS"],
                },
            )
        )

        def pad_001(k, f):
            for row in k:
                v = json.loads(row["value_raw"])
                aft = v.get("after") or {}
                if aft.get("PAD") == "run_20261012T031500Z":
                    aft["PAD"] = "001"
                    v["after"] = aft
                    row["value_raw"] = json.dumps(v, separators=(",", ":"))
                    break

        cases.append(
            _run_business_case(
                "negative_varchar_pad_001_vs_1",
                tmp,
                **std,
                mutate=pad_001,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["MISSING_OPS"],
                },
            )
        )

        def reorder(k, f):
            if len(k) > 2:
                k[0], k[1] = k[1], k[0]
                for i, row in enumerate(k):
                    row["kafka_offset"] = i

        cases.append(
            _run_business_case(
                "negative_reorder",
                tmp,
                **std,
                mutate=reorder,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["CAUSAL_TRANSITION"],
                },
            )
        )

        def uu_swap(k, f):
            _swap_uu_on_key(k, 1201984)

        cases.append(
            _run_business_case(
                "negative_same_key_uu_content_swap",
                tmp,
                **std,
                mutate=uu_swap,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["CAUSAL_TRANSITION"],
                },
            )
        )

        def rollback(k, f):
            k.append(
                _capture_row(
                    "EV",
                    len(k),
                    "c",
                    {"ID": 500011},
                    None,
                    {"ID": 500011, "INST": 1, "TAG": "ROLLED_BACK", "VAL": 10, "PAD": None},
                )
            )

        cases.append(
            _run_business_case(
                "negative_rollback_leak",
                tmp,
                **std,
                mutate=rollback,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["ROLLBACK_LEAK"],
                },
            )
        )

        def trunc(k, f):
            del k[-3:]

        cases.append(
            _run_business_case(
                "negative_truncated_capture",
                tmp,
                **std,
                mutate=trunc,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["MISSING_OPS"],
                },
            )
        )

        def bad_flink(k, f):
            f.clear()
            f.extend(build_flink_sink(OPS, PIN, FINAL_PRIMARY, corrupt_pp=True))

        cases.append(
            _run_business_case(
                "negative_flink_state",
                tmp,
                **std,
                mutate=bad_flink,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["FLINK_FINAL_STATE"],
                },
            )
        )

        def wrong_topic(k, f):
            if k:
                k[0]["topic"] = "ocr370.live.srl.20261012T031500Z.baseline.OLR_RAC_TEST.EVV"

        cases.append(
            _run_business_case(
                "negative_wrong_table_topic",
                tmp,
                **std,
                mutate=wrong_topic,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["MISSING_OPS"],
                },
            )
        )

        def schema_history_row(k, f):
            k.insert(
                0,
                {
                    "topic": "ocr370.live.srl.20261012T031500Z.baseline.schema-history",
                    "partition": 0,
                    "kafka_offset": 0,
                    "timestamp": 1,
                    "headers_count": 0,
                    "key_raw": "{}",
                    "value_raw": "{}",
                },
            )
            for i, row in enumerate(k):
                if "schema-history" not in row["topic"]:
                    row["kafka_offset"] = i

        cases.append(
            _run_business_case(
                "positive_schema_history_control_ignored",
                tmp,
                **std,
                mutate=schema_history_row,
                expected={
                    "exit_code": EXIT_PASS,
                    "status": "PASS",
                    "qualification_status": "PASS",
                    "failure_codes": [],
                },
            )
        )

        def manifest_incomplete(k, f):
            pass

        def set_incomplete(m, k):
            m["complete"] = False

        cases.append(
            _run_business_case(
                "negative_capture_manifest_incomplete",
                tmp,
                **std,
                mutate=manifest_incomplete,
                manifest_mutate=set_incomplete,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["CAPTURE_INCOMPLETE"],
                },
            )
        )

        def set_bad_isolation(m, k):
            m["isolation_level"] = "read_uncommitted"

        cases.append(
            _run_business_case(
                "negative_capture_isolation",
                tmp,
                **std,
                manifest_mutate=set_bad_isolation,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["CAPTURE_ISOLATION"],
                },
            )
        )

        def count_mismatch(m, k):
            m["records"] = max(0, m["records"] - 1)

        cases.append(
            _run_business_case(
                "negative_capture_count_mismatch",
                tmp,
                **std,
                manifest_mutate=count_mismatch,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["CAPTURE_COUNT_MISMATCH"],
                },
            )
        )

        def end_not_reached(m, k):
            for p in m["partitions"]:
                p["final_position"] = p["end"] - 1

        cases.append(
            _run_business_case(
                "negative_capture_end_not_reached",
                tmp,
                **std,
                manifest_mutate=end_not_reached,
                expected={
                    "exit_code": EXIT_FAIL,
                    "status": "FAIL",
                    "qualification_status": "PASS",
                    "failure_codes": ["CAPTURE_END_NOT_REACHED"],
                },
            )
        )

        def drop_p103_receipt(lines):
            lines[:] = [ln for ln in lines if ln.get("phase") != "P103"]

        rp, mp, qp = _runtime_bundle(tmp, "rt-missing-phase", rec_mut=drop_p103_receipt)
        cases.append(
            _run_business_case(
                "negative_runtime_missing_phase_receipt",
                tmp,
                **std,
                receipts_path=rp,
                measurement_window_path=mp,
                qualification_evidence_path=qp,
                expected={
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": "PASS",
                    "failure_codes": ["RUNTIME_GATE", "RECEIPT_MISSING_PHASE"],
                },
            )
        )

        def bad_sql(lines):
            for ln in lines:
                if ln.get("phase") == "W101":
                    ln["sql_exit"] = 1

        rp2, mp2, qp2 = _runtime_bundle(tmp, "rt-sql-fail", rec_mut=bad_sql)
        cases.append(
            _run_business_case(
                "negative_runtime_sql_receipt_failed",
                tmp,
                **std,
                receipts_path=rp2,
                measurement_window_path=mp2,
                qualification_evidence_path=qp2,
                expected={
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": "PASS",
                    "failure_codes": ["RECEIPT_SQL_FAILED"],
                },
            )
        )

        def bad_count(doc):
            doc["expected_committed_counts"]["P103"] = 0

        rp3, mp3, qp3 = _runtime_bundle(tmp, "rt-count", qev_mut=bad_count)
        cases.append(
            _run_business_case(
                "negative_runtime_qualification_count_mismatch",
                tmp,
                **std,
                receipts_path=rp3,
                measurement_window_path=mp3,
                qualification_evidence_path=qp3,
                expected={
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": "PASS",
                    "failure_codes": ["QUALIFICATION_COUNT_MISMATCH"],
                },
            )
        )

        def no_catch_up(doc):
            doc["p099_catch_up_proven"] = False

        rp4, mp4, qp4 = _runtime_bundle(tmp, "rt-p099", mw_mut=no_catch_up)
        cases.append(
            _run_business_case(
                "negative_runtime_p099_catchup_not_proved",
                tmp,
                **std,
                receipts_path=rp4,
                measurement_window_path=mp4,
                qualification_evidence_path=qp4,
                expected={
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": "PASS",
                    "failure_codes": ["MEASUREMENT_WINDOW_INVALID"],
                },
            )
        )

        def wrong_schema(k, f):
            for row in k:
                v = json.loads(row["value_raw"])
                if isinstance(v.get("source"), dict):
                    v["source"]["schema"] = "WRONG"
                    row["value_raw"] = json.dumps(v, separators=(",", ":"))
                    break

        cases.append(
            _run_business_case(
                "negative_runtime_wrong_source_schema",
                tmp,
                **std,
                mutate=wrong_schema,
                expected={
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": "PASS",
                    "failure_codes": ["SOURCE_SCHEMA_MISMATCH"],
                },
            )
        )

        def wrong_thread(k, f):
            for row in k:
                v = json.loads(row["value_raw"])
                src = v.get("source") or {}
                side = v.get("after") or v.get("before") or {}
                if side.get("INST") == 2:
                    src["thread"] = 1
                    v["source"] = src
                    row["value_raw"] = json.dumps(v, separators=(",", ":"))
                    break

        cases.append(
            _run_business_case(
                "negative_runtime_dual_instance_thread_collision",
                tmp,
                **std,
                mutate=wrong_thread,
                expected={
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": "PASS",
                    "failure_codes": ["DUAL_INSTANCE_THREAD_COLLISION"],
                },
            )
        )

        trunc_ops = tmp / "truncated-ops.jsonl"
        trunc_ops.write_text(OPS.read_text(encoding="utf-8").splitlines()[0] + "\n", encoding="utf-8")
        cases.append(
            _run_business_case(
                "qualification_truncated_oracle_ops",
                tmp,
                ops_path=trunc_ops,
                pin_path=PIN,
                final_path=FINAL_PRIMARY,
                require_runtime_gates=False,
                expected={
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": "INCOMPLETE",
                    "failure_codes": ["ORACLE_QUALIFICATION"],
                },
            )
        )

        bad_final = tmp / "malformed-final.json"
        bad_final.write_text("{ not json\n", encoding="utf-8")
        code_mf, rep_mf = validate_lane(
            lane="baseline",
            journal_path=None,
            kafka_path=str(tmp / "positive_builder_smoke_samples-kafka.jsonl"),
            expected_path=str(OPS),
            expected_final_state_path=str(bad_final),
            oracle_pin_path=str(PIN),
            flink_sink_path=None,
            capture_manifest_path=None,
            contract_version="20261012T031500Z-v1",
            require_capture_manifest=False,
            require_runtime_gates=False,
        )
        cases.append(
            _pack_case(
                "negative_malformed_final_state_json",
                code_mf,
                rep_mf,
                {
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": None,
                    "failure_codes": ["ORACLE_INVALID"],
                },
            )
        )

        present_no_after = tmp / "final-present-no-after.json"
        present_no_after.write_text(
            json.dumps(
                {
                    "contract_version": "20261012T031500Z-expected-ops-v2",
                    "record_type": "final_key_state",
                    "run_id": "20261012T031500Z",
                    "entries": [{"table": "OLR_RAC_TEST.EV", "key": {"ID": 1}, "present": True}],
                }
            ),
            encoding="utf-8",
        )
        code_pna, rep_pna = validate_lane(
            lane="baseline",
            journal_path=None,
            kafka_path=str(tmp / "positive_builder_smoke_samples-kafka.jsonl"),
            expected_path=str(OPS),
            expected_final_state_path=str(present_no_after),
            oracle_pin_path=str(PIN),
            flink_sink_path=None,
            capture_manifest_path=None,
            contract_version="20261012T031500Z-v1",
            require_capture_manifest=False,
            require_runtime_gates=False,
        )
        cases.append(
            _pack_case(
                "negative_final_present_without_after",
                code_pna,
                rep_pna,
                {
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": None,
                    "failure_codes": ["ORACLE_INVALID"],
                },
            )
        )

        bad_path = tmp / "bad-kafka.jsonl"
        bad_path.write_text("{ not json\n", encoding="utf-8")
        code_parse, rep_parse = validate_lane(
            lane="baseline",
            journal_path=None,
            kafka_path=str(bad_path),
            expected_path=str(OPS),
            expected_final_state_path=str(FINAL_PRIMARY),
            oracle_pin_path=str(PIN),
            flink_sink_path=str(tmp / "positive_builder_smoke_samples-flink.jsonl"),
            capture_manifest_path=str(tmp / "positive_builder_smoke_samples-manifest.json"),
            contract_version="20261012T031500Z-v1",
            require_capture_manifest=True,
            require_runtime_gates=False,
        )
        cases.append(
            _pack_case(
                "negative_kafka_parser_error",
                code_parse,
                rep_parse,
                {
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": "PASS",
                    "failure_codes": ["KAFKA_PARSE"],
                },
            )
        )

        code_no, rep_no = validate_lane(
            lane="baseline",
            journal_path=None,
            kafka_path=str(tmp / "positive_builder_smoke_samples-kafka.jsonl"),
            expected_path=None,
            expected_final_state_path=None,
            oracle_pin_path=None,
            flink_sink_path=None,
            capture_manifest_path=None,
            contract_version="20261012T031500Z-v1",
        )
        cases.append(
            _pack_case(
                "negative_no_expected_events",
                code_no,
                rep_no,
                {
                    "exit_code": EXIT_INPUT,
                    "status": "INPUT_INCOMPLETE",
                    "qualification_status": "unknown",
                    "failure_codes": ["INPUT_INCOMPLETE"],
                },
            )
        )

    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    failed = [c for c in cases if not c.get("assert_ok", True)]
    summary = {
        "suite": "live-srl-validator-self-test",
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "builder_samples_ref": str(BUILDER_SAMPLES_ROOT),
        "primary_final_fixture": str(FINAL_PRIMARY.relative_to(ROOT)),
        "incomplete_final_fixture": str(FINAL_INCOMPLETE.relative_to(ROOT)),
        "smoke_gate": "HOLD",
        "integration_freeze": "not_accepted",
        "overall_pass": len(failed) == 0,
        "case_count": len(cases),
        "failed_cases": [c["name"] for c in failed],
        "expectations": {
            "positive_exit": EXIT_PASS,
            "business_negative_exit": EXIT_FAIL,
            "input_negative_exit": EXIT_INPUT,
            "business_negatives_require_qualification_pass": True,
        },
        "cases": cases,
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    _archive_evidence(EVIDENCE)
    EVIDENCE.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0 if summary["overall_pass"] else 1


if __name__ == "__main__":
    sys.exit(run_self_tests())

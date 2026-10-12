#!/usr/bin/env python3
"""Build smoke runtime fixtures aligned with workload_driver.sh semantics."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import sys

_V = Path(__file__).resolve().parent.parent
if str(_V) not in sys.path:
    sys.path.insert(0, str(_V))

from datetime import datetime, timezone

from srl_contracts import REQUIRED_SOURCE_SCHEMA  # noqa: E402
from srl_hashes import sha256_file  # noqa: E402
from srl_oracle import load_expected_oracle  # noqa: E402
from srl_xid import OracleXid  # noqa: E402
from srl_qualification_evidence import (  # noqa: E402
    oracle_committed_counts,
    oracle_forbidden_counts,
    oracle_table_counts,
)

ROOT = Path(__file__).resolve().parent
OPS = ROOT / "workload-expected-operations.smoke.jsonl"
PIN = ROOT / "workload-oracle-pin.json"
FIN = ROOT / "workload-expected-final-state.smoke.json"
REC = ROOT / "workload-execution-receipts.smoke.jsonl"
MW = ROOT / "MEASUREMENT-WINDOW.smoke.json"
QEV = ROOT / "ORACLE-QUALIFICATION-EVIDENCE.smoke.json"
PROV = ROOT / "PROVENANCE-EVIDENCE.synthetic.json"
WITNESS_JL = ROOT / "srl-witness.smoke.jsonl"
ORACLE_JL = ROOT / "oracle-commit.smoke.jsonl"

RUN_ID = "20261012T031500Z"
CAPTURE_BASE_MS = int(datetime(2026, 10, 12, 3, 20, 10, tzinfo=timezone.utc).timestamp() * 1000)
ARCHIVE_DELIVERED_UTC = "2026-10-12T03:20:24.000Z"
MEASURE_FROM = "2026-10-12T03:20:00.000Z"
CATCH_UP = "2026-10-12T03:19:30.000Z"
CONSUMER_START = "2026-10-12T03:20:05.000Z"


def main() -> int:
    pin = json.loads(PIN.read_text())
    oracle = load_expected_oracle(str(OPS), str(PIN))
    comm = oracle_committed_counts(oracle)
    forb = oracle_forbidden_counts(oracle)
    tables = oracle_table_counts(oracle)
    table_counts = {f"{ph}/{tb}": n for (ph, tb), n in tables.items()}

    partial_key = None
    full_keys: list[int] = []
    for line in OPS.read_text().splitlines():
        o = json.loads(line)
        if o.get("committed") is False:
            kid = int(o["key"]["ID"])
            if o.get("rollback") == "savepoint_partial":
                partial_key = kid
            elif o.get("rollback") == "full_transaction":
                full_keys.append(kid)

    receipts = [
        {
            "run_id": RUN_ID,
            "phase": "P099",
            "receipt_type": "phase_boundary",
            "utc": CATCH_UP,
            "host": "RAC1",
            "note": "prep_rows_outside_measured_dml;lanes_must_catch_up_before_consumer_window",
            "catch_up_proven": True,
        },
        {
            "run_id": RUN_ID,
            "phase": "P100",
            "receipt_type": "phase_boundary",
            "utc": MEASURE_FROM,
            "host": "RAC1",
            "note": "measurement_window_written",
            "path": "state/MEASUREMENT-WINDOW.json",
        },
        {
            "run_id": RUN_ID,
            "phase": "W101",
            "receipt_type": "sql_commit",
            "utc": "2026-10-12T03:20:10.000Z",
            "host": "RAC1",
            "file": "live_srl_workload_burst.sql",
            "sql_exit": 0,
        },
        {
            "run_id": RUN_ID,
            "phase": "W102",
            "receipt_type": "sql_commit",
            "utc": "2026-10-12T03:20:11.000Z",
            "host": "RAC2",
            "file": "live_srl_workload_burst.sql",
            "sql_exit": 0,
        },
        {
            "run_id": RUN_ID,
            "phase": "P103",
            "receipt_type": "key_anchor",
            "utc": "2026-10-12T03:20:20.000Z",
            "host": "RAC1",
            "payload": {
                "partial_rollback_key": partial_key,
                "full_rollback_keys": sorted(set(full_keys)),
            },
        },
        {
            "run_id": RUN_ID,
            "phase": "W104",
            "receipt_type": "sql_commit",
            "utc": "2026-10-12T03:20:25.000Z",
            "host": "RAC1",
            "file": "archive_current.sql",
            "sql_exit": 0,
        },
        {
            "run_id": RUN_ID,
            "phase": "P105",
            "receipt_type": "phase_boundary",
            "utc": "2026-10-12T03:20:30.000Z",
            "host": "RAC1",
            "note": "pingpong_smoke_pairs_complete",
        },
    ]
    REC.write_text("\n".join(json.dumps(r, separators=(",", ":")) for r in receipts) + "\n")

    mw = {
        "run_id": RUN_ID,
        "contract_version": "20261012T031500Z-measure-v1",
        "source_measured_dml_from_utc": MEASURE_FROM,
        "source_phases_in_oracle": ["P101", "P102", "P103", "P107", "P105", "P106"],
        "source_phases_excluded": ["P099"],
        "consumer_scope": {
            "baseline_topic_prefix": f"ocr370.live.srl.{RUN_ID}.baseline",
            "recovery_topic_prefix": f"ocr370.live.srl.{RUN_ID}.recovery",
            "isolation": "read_committed",
            "rule": "events_before_both_lane_connectors_RUNNING_and_post_bootstrap_catch_up_are_out_of_scope",
            "no_offset_topic_reset": True,
            "no_schema_history_reset": True,
        },
        "lanes": {
            "baseline": {
                "consumer_start_utc": CONSUMER_START,
                "catch_up_after_p099_utc": CATCH_UP,
                "p099_catch_up_position": {"topic": "baseline.prep", "partition": 0, "offset": 99},
            },
            "recovery": {
                "consumer_start_utc": CONSUMER_START,
                "catch_up_after_p099_utc": CATCH_UP,
                "p099_catch_up_position": {"topic": "recovery.prep", "partition": 0, "offset": 99},
            },
        },
        "partition_capture_bounds": [
            {
                "topic": "ocr370.live.srl.20261012T031500Z.baseline.OLR_RAC_TEST.EV",
                "partition": 0,
                "capture_start_utc": CONSUMER_START,
                "capture_end_utc": "2026-10-12T03:25:00.000Z",
                "manifest_ref_sha256": "placeholder-partition-bound-sha256",
            }
        ],
        "artifact_hashes": {"measurement_window_sha256": "placeholder-mw-sha256"},
    }
    MW.write_text(json.dumps(mw, indent=2) + "\n")

    qev = {
        "contract_version": "20261012T031500Z-expected-ops-v3",
        "run_id": RUN_ID,
        "mode": pin.get("mode", "smoke"),
        "pin_ref": str(PIN),
        "dml_artifact": str(OPS),
        "final_state_artifact": str(FIN),
        "pin_sha256": sha256_file(str(PIN)),
        "dml_sha256": sha256_file(str(OPS)),
        "final_state_sha256": sha256_file(str(FIN)),
        "expected_committed_counts": dict(comm),
        "expected_forbidden_counts": dict(forb),
        "expected_committed_table_counts": table_counts,
        "capture_manifest_requirements": {
            "complete": True,
            "isolation_level": "read_committed",
            "records_min": 1,
        },
        "phase_receipts": [f"{r['phase']}:{r['receipt_type']}" for r in receipts],
    }
    QEV.write_text(json.dumps(qev, indent=2) + "\n")

    def topic(table: str, lane: str = "baseline") -> str:
        return f"ocr370.live.srl.{RUN_ID}.{lane}.OLR_RAC_TEST.{table}"

    def ms_to_utc_z(ms: int) -> str:
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    witness_lines: list[str] = []
    oracle_lines: list[str] = []
    bindings: list[dict] = []
    for off, op in enumerate(oracle.committed_kafka):
        scn = 2701000 + off
        exec_instance = 1 if off % 2 == 0 else 2
        exec_thread = exec_instance
        xid = OracleXid(usn=0x9001 + off, slot=0x0001, sqn=0x1000 + off)
        tx_id = xid.stock37_tx_id()
        raw8 = f"{xid.usn:04x}{xid.slot:04x}{xid.sqn:08x}"
        sampled_utc = ms_to_utc_z(CAPTURE_BASE_MS + off)
        wit_rec = {
            "run_id": RUN_ID,
            "lane": "baseline",
            "op_id": op.op_id,
            "incarnation": 1,
            "thread": exec_thread,
            "sequence": 100 + off,
            "member": "member1",
            "sampled_utc": sampled_utc,
            "archive_delivered_utc": ARCHIVE_DELIVERED_UTC,
            "growth_state": "archiving",
        }
        ora_rec = {
            "run_id": RUN_ID,
            "op_id": op.op_id,
            "lane": "baseline",
            "schema": REQUIRED_SOURCE_SCHEMA,
            "table": op.table,
            "key": op.key,
            "op": op.op,
            "before": op.before,
            "after": op.after,
            "seq_on_key": op.seq_on_key,
            "xidusn": xid.usn,
            "xidslot": xid.slot,
            "xidsqn": xid.sqn,
            "raw_xid_bytes_hex": raw8,
            "instance_number": exec_instance,
            "thread_number": exec_thread,
            "commit_outcome": "success",
            "commit_scn": scn,
            "commit_scn_source": "oracle_redo_commit",
        }
        witness_lines.append(json.dumps(wit_rec, separators=(",", ":")))
        oracle_lines.append(json.dumps(ora_rec, separators=(",", ":")))
        line_no = off + 1
        bindings.append(
            {
                "op_id": op.op_id,
                "kafka_coord": {
                    "topic": topic(op.table),
                    "partition": 0,
                    "offset": off,
                },
                "oracle": {
                    "xidusn": xid.usn,
                    "xidslot": xid.slot,
                    "xidsqn": xid.sqn,
                    "raw_xid_bytes_hex": raw8,
                    "instance_number": exec_instance,
                    "thread_number": exec_thread,
                    "commit_outcome": "success",
                    "checkpoint_scn": {"value": scn, "source": "kafka_envelope"},
                    "commit_scn": {"value": scn, "source": "oracle_redo_commit"},
                    "commit_evidence_ref": {
                        "path": str(ORACLE_JL),
                        "locator": f"line:{line_no}",
                        "sha256": "pending",
                    },
                },
                "kafka_source": {"txId": tx_id, "scn": scn},
                "srl_witness": {
                    "incarnation": 1,
                    "thread": exec_thread,
                    "sequence": 100 + off,
                    "member": "member1",
                    "sampled_utc": sampled_utc,
                    "archive_delivered_utc": ARCHIVE_DELIVERED_UTC,
                    "growth_state": "archiving",
                    "artifact_ref": {
                        "path": str(WITNESS_JL),
                        "locator": f"line:{line_no}",
                        "sha256": "pending",
                    },
                },
            }
        )

    WITNESS_JL.write_text("\n".join(witness_lines) + "\n")
    ORACLE_JL.write_text("\n".join(oracle_lines) + "\n")
    w_sha = sha256_file(str(WITNESS_JL))
    o_sha = sha256_file(str(ORACLE_JL))
    for b in bindings:
        b["oracle"]["commit_evidence_ref"]["sha256"] = o_sha
        b["srl_witness"]["artifact_ref"]["sha256"] = w_sha

    prov = {
        "contract_version": "20261012T031500Z-provenance-v1",
        "run_id": RUN_ID,
        "evidence_class": "synthetic_component",
        "event_bindings": bindings,
    }
    PROV.write_text(json.dumps(prov, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

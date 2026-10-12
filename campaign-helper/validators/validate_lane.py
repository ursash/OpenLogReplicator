#!/usr/bin/env python3
"""
Strict Live SRL lane validator: Kafka business capture + expected-operations JSONL + Flink sink.

Stdlib only. Self-tests exercise the same validate_lane() paths (no Kafka/Flink clients).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from srl_contracts import CONTRACT_VERSION, EXPECTED_OPS_CONTRACT_VERSION  # noqa: E402
from srl_validate import EXIT_USAGE, validate_lane  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    ap = argparse.ArgumentParser(
        description="Live SRL strict business-event + Flink final-state validator"
    )
    ap.add_argument("--lane", choices=("baseline", "recovery"))
    ap.add_argument("--journal", help="workload-expected-journal.jsonl (intent adapter only)")
    ap.add_argument("--kafka", help="kafka-raw.jsonl capture")
    ap.add_argument(
        "--expected-events",
        help="workload-expected-operations.jsonl (expected-ops-v2 DML)",
    )
    ap.add_argument(
        "--expected-final-state",
        help="workload-expected-final-state.json (record_type final_key_state)",
    )
    ap.add_argument(
        "--oracle-pin",
        help="workload-oracle-pin.json (INST binding for PP pingpong)",
    )
    ap.add_argument("--flink-sink", help="Flink sink rows JSONL")
    ap.add_argument("--capture-manifest", help="capture manifest.json")
    ap.add_argument(
        "--contract-version",
        default=CONTRACT_VERSION,
        help=f"Kafka/Flink integration pin (default {CONTRACT_VERSION})",
    )
    ap.add_argument(
        "--self-test",
        action="store_true",
        help="run mandatory field-deletion regressions (not live acceptance)",
    )
    ap.add_argument("--json-out", help="write machine-readable report path")
    ap.add_argument(
        "--allow-missing-capture-manifest",
        action="store_true",
        help="TEST_ONLY hook (rejected on live CLI)",
    )
    ap.add_argument("--execution-receipts", help="workload-execution-receipts.jsonl")
    ap.add_argument("--measurement-window", help="MEASUREMENT-WINDOW.json")
    ap.add_argument("--qualification-evidence", help="ORACLE-QUALIFICATION-EVIDENCE.json")
    ap.add_argument("--provenance-evidence", help="PROVENANCE-EVIDENCE.json (Primary stock37 for live)")
    ap.add_argument("--workload-mode", choices=("smoke", "campaign"), help="override pin mode")
    ap.add_argument(
        "--allow-missing-runtime-gates",
        action="store_true",
        help="TEST_ONLY hook (rejected on live CLI)",
    )
    args = ap.parse_args(argv)

    if args.self_test:
        from mandatory_field_regressions import run_all

        return run_all()

    if args.allow_missing_runtime_gates or args.allow_missing_capture_manifest:
        print(
            json.dumps(
                {
                    "status": "TEST_ONLY",
                    "acceptance": "rejected",
                    "failures": [
                        {
                            "code": "CLI_BYPASS_FORBIDDEN",
                            "detail": "allow-missing-* switches forbidden for live acceptance",
                        }
                    ],
                    "invoked_argv": argv,
                },
                indent=2,
            )
        )
        return EXIT_USAGE

    if not args.lane:
        print(
            json.dumps(
                {"status": "FAIL", "failures": [{"code": "USAGE", "detail": "--lane required"}]}
            )
        )
        return EXIT_USAGE

    code, report = validate_lane(
        lane=args.lane,
        journal_path=args.journal,
        kafka_path=args.kafka,
        expected_path=args.expected_events,
        expected_final_state_path=args.expected_final_state,
        oracle_pin_path=args.oracle_pin,
        flink_sink_path=args.flink_sink,
        capture_manifest_path=args.capture_manifest,
        contract_version=args.contract_version,
        require_capture_manifest=True,
        execution_receipts_path=args.execution_receipts,
        measurement_window_path=args.measurement_window,
        qualification_evidence_path=args.qualification_evidence,
        provenance_evidence_path=args.provenance_evidence,
        require_runtime_gates=True,
        workload_mode=args.workload_mode,
        acceptance_mode="live",
        invoked_argv=argv,
    )
    report["expected_ops_contract"] = EXPECTED_OPS_CONTRACT_VERSION
    report["invoked_argv"] = argv
    text = json.dumps(report, indent=2, ensure_ascii=False)
    print(text)
    if args.json_out:
        Path(args.json_out).write_text(text + "\n", encoding="utf-8")
    return code


if __name__ == "__main__":
    sys.exit(main())

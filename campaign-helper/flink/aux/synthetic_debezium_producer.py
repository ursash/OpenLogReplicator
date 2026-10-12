#!/usr/bin/env python3
"""Produce schemas.enable=false Debezium JSON (numeric columns as strings) for aux smoke."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from typing import Any

# FLINK-RUNTIME-DELTA-v1 deterministic keys (campaign oracle; reused for aux validation shape).
EV_RAC1 = 1201984
EV_RAC1_DEL = 1201985
EV_RAC2 = 1201994
EV_RAC2_DEL = 1201995
PP_ID = 1
PP_CTR_BASELINE = 100  # pin baseline; +4 after four pingpong increments -> 104


def env_row(table: str, op: str, before: dict | None, after: dict | None) -> dict[str, Any]:
    return {
        "before": before,
        "after": after,
        "source": {"db": "ORCLPDB1", "schema": "OLR_RAC_TEST", "table": table},
        "op": op,
        "ts_ms": int(time.time() * 1000),
    }


def key_id(row_id: int) -> str:
    return json.dumps({"ID": str(row_id)}, separators=(",", ":"))


def produce(bootstrap: str, topic: str, records: list[tuple[str, str | None]]) -> None:
    proc = subprocess.Popen(
        [
            "kafka-console-producer",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            topic,
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=\t",
        ],
        stdin=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None
    for k, v in records:
        if v is None:
            proc.stdin.write(f"{k}\t\n")
        else:
            proc.stdin.write(f"{k}\t{v}\n")
    proc.stdin.close()
    rc = proc.wait()
    if rc != 0:
        sys.exit(rc)


def ev_records() -> list[tuple[str, str | None]]:
    out: list[tuple[str, str | None]] = []
    last: dict[int, dict[str, str | None]] = {}

    def add_ev(eid: int, inst: str, tag: str, val: str | None, pad: str, op: str, tombstone: bool = False):
        nonlocal out
        after = {"ID": str(eid), "INST": inst, "TAG": tag, "VAL": val, "PAD": pad}
        before = None if op == "c" else last.get(eid)
        if op == "d":
            before = last.get(eid, after)
            after = None
        val_json = json.dumps(env_row("EV", op, before, after), separators=(",", ":"))
        out.append((key_id(eid), val_json))
        if tombstone and op == "d":
            out.append((key_id(eid), None))
        elif after is not None:
            last[eid] = after.copy()
        elif eid in last:
            del last[eid]

    add_ev(EV_RAC1, "1", "LSRL_AUX", "10", "rac1", "c")
    add_ev(EV_RAC1, "1", "LSRL_AUX", "11", "rac1-u1", "u")
    add_ev(EV_RAC1, "1", "LSRL_AUX", None, "rac1-null-val", "u")
    add_ev(EV_RAC2, "2", "LSRL_AUX", "20", "rac2", "c")
    add_ev(EV_RAC2, "2", "LSRL_AUX", "21", "rac2-u1", "u")
    add_ev(EV_RAC1_DEL, "1", "LSRL_DEL", "1", "del", "c")
    add_ev(EV_RAC1_DEL, "1", "LSRL_DEL", "1", "del", "d", tombstone=True)
    add_ev(EV_RAC2_DEL, "2", "LSRL_DEL", "1", "del", "c")
    add_ev(EV_RAC2_DEL, "2", "LSRL_DEL", "1", "del", "d", tombstone=True)
    return out


def pp_records() -> list[tuple[str, str | None]]:
    out: list[tuple[str, str | None]] = []
    ctr = PP_CTR_BASELINE
    note = "aux-smoke"
    prev: dict[str, str] | None = None

    def add_pp(op: str, ctr_val: int, inst: str):
        nonlocal out, prev
        after = {"ID": str(PP_ID), "CTR": str(ctr_val), "INST": inst, "NOTE": note}
        before = None if op == "c" else prev
        val_json = json.dumps(env_row("PP", op, before, after), separators=(",", ":"))
        out.append((key_id(PP_ID), val_json))
        prev = after.copy()

    add_pp("c", ctr, "1")
    for i in range(4):
        ctr += 1
        add_pp("u", ctr, str((i % 2) + 1))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic-ev", required=True)
    ap.add_argument("--topic-pp", required=True)
    args = ap.parse_args()
    produce(args.bootstrap, args.topic_ev, ev_records())
    produce(args.bootstrap, args.topic_pp, pp_records())
    summary = {
        "ev_final_ids": [EV_RAC1, EV_RAC2],
        "ev_deleted_ids": [EV_RAC1_DEL, EV_RAC2_DEL],
        "pp_id": PP_ID,
        "pp_ctr_baseline": PP_CTR_BASELINE,
        "pp_ctr_expected_final": PP_CTR_BASELINE + 4,
        "expected_final_rows": {
            "EV": {
                str(EV_RAC1): {"ID": EV_RAC1, "INST": 1, "TAG": "LSRL_AUX", "VAL": None, "PAD": "rac1-null-val"},
                str(EV_RAC2): {"ID": EV_RAC2, "INST": 2, "TAG": "LSRL_AUX", "VAL": 21, "PAD": "rac2-u1"},
            },
            "PP": {
                str(PP_ID): {"ID": PP_ID, "CTR": PP_CTR_BASELINE + 4, "INST": 2, "NOTE": "aux-smoke"},
            },
        },
        "test_semantics": {
            "not_claimed": [
                "Oracle live SRL CDC pass",
                "Kafka log compaction dedupe history",
                "Flink recovery/restart after fault",
                "Debezium transaction END metadata",
            ],
            "tombstone_note": "Delete path uses explicit Kafka null-value record (key\\t) after op=d envelope; not empty-string value.",
            "sink_raw_note": "sink_*.raw.tsv may contain multiple records per key; validator uses last non-null value only.",
        },
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

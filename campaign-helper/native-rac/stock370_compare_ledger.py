#!/usr/bin/env python3
"""Compare intended-operation ledger to Debezium records.jsonl (event sequence, not terminal row only)."""
import json
import sys
from pathlib import Path


def ledger_ops(path):
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("record_type") == "intended_op":
            yield row


def record_ops(path):
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        val = row.get("value") or {}
        if not val.get("op"):
            continue
        src = val.get("source") or {}
        after = val.get("after") or {}
        before = val.get("before") or {}
        key_id = (after.get("ID") or before.get("ID"))
        schema = src.get("schema")
        table = src.get("table")
        yield {
            "op_id": None,
            "table": "{0}.{1}".format(schema, table) if schema and table else None,
            "key": {"ID": key_id},
            "op": val.get("op"),
            "after": after,
            "before": before,
            "redo_thread": src.get("redo_thread"),
            "scn": src.get("scn"),
        }


def main():
    ledger = list(ledger_ops(sys.argv[1]))
    records = list(record_ops(sys.argv[2]))
    missing = []
    matched = 0
    for want in ledger:
        found = [
            r for r in records
            if r["table"] == want["table"] and r["op"] == want["op"]
            and r["key"] == want["key"]
            and (want.get("after") or {}) == (r.get("after") or {})
        ]
        if not found:
            missing.append({"want": want, "reason": "no matching consumer event with full after"})
        else:
            matched += 1
    out = {
        "ledger_ops": len(ledger),
        "consumer_data_ops": len(records),
        "matched": matched,
        "missing": missing,
        "pass": len(ledger) > 0 and not missing and len(records) >= len(ledger),
    }
    print(json.dumps(out, indent=2))
    sys.exit(0 if out["pass"] else 1)


if __name__ == "__main__":
    main()

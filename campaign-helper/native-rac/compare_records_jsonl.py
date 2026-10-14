#!/usr/bin/env python3
"""Fail-closed compare of DbzOlrRunner records.jsonl for bounded native/stock gates."""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

# Prior r2 first-flow business keys; must not reappear as any EV DML in later gates.
FIRST_FLOW_CONSUMED_KEYS = frozenset(
    str(i)
    for i in (
        1807100,
        1807101,
        1807200,
        1807201,
        1807102,
        1807202,
    )
)


def _ev_topic(topic: Any) -> bool:
    return isinstance(topic, str) and topic.endswith(".OLR_RAC_TEST.EV")


def load_records(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise SystemExit(f"missing records file: {path}")
    rows: list[dict[str, Any]] = []
    for i, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError as e:
            raise SystemExit(f"malformed JSON line {i} in {path}: {e}") from e
        if not isinstance(parsed, dict):
            raise SystemExit(f"non-object JSON line {i} in {path}")
        rows.append(parsed)
    return rows


def _row_ev_key(row: dict[str, Any]) -> str | None:
    key = row.get("key")
    if not isinstance(key, dict):
        return None
    kid = key.get("ID")
    if kid is None:
        return None
    return str(kid)


def _row_ev_op(row: dict[str, Any]) -> str | None:
    val = row.get("value")
    if not isinstance(val, dict):
        return None
    op = val.get("op")
    return str(op) if op is not None else None


def ev_inserts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        if not _ev_topic(r.get("topic")):
            continue
        if _row_ev_op(r) != "c":
            continue
        out.append(r)
    return out


def check_expected(
    path: Path,
    *,
    expected_ids: list[int],
    expected_after: dict[int, dict[str, str]],
    forbidden_old_prefix_keys: frozenset[str] | None = None,
) -> dict[str, Any]:
    forbidden = forbidden_old_prefix_keys if forbidden_old_prefix_keys is not None else FIRST_FLOW_CONSUMED_KEYS
    expected_set = {str(i) for i in expected_ids}
    rows = load_records(path)
    ins = ev_inserts(rows)
    ids = [_row_ev_key(r) or "?" for r in ins]
    report: dict[str, Any] = {
        "path": str(path),
        "total_lines": len(rows),
        "ev_op_c_count": len(ins),
        "ev_ids": ids,
        "errors": [],
    }

    if len(ins) != len(expected_ids):
        report["errors"].append(f"expected {len(expected_ids)} EV inserts, got {len(ins)}")

    exp_order = [str(i) for i in expected_ids]
    if ids != exp_order:
        report["errors"].append(f"insert order {ids!r} != declared order {exp_order!r}")

    for eid in exp_order:
        if ids.count(eid) != 1:
            report["errors"].append(f"id {eid} multiplicity {ids.count(eid)}, want 1")

    for r in ins:
        kid = _row_ev_key(r) or "?"
        val = r.get("value")
        if not isinstance(val, dict):
            report["errors"].append(f"id {kid} value is not an object")
            continue
        before = val.get("before")
        if before is not None:
            report["errors"].append(f"id {kid} INSERT.before must be null, got {before!r}")
        after = val.get("after")
        if not isinstance(after, dict):
            report["errors"].append(f"id {kid} INSERT.after must be object")
            continue
        want = expected_after.get(int(kid)) if kid.isdigit() else None
        if want is None:
            report["errors"].append(f"id {kid} not in expected_after spec")
            continue
        for field, wval in want.items():
            if str(after.get(field)) != str(wval):
                report["errors"].append(f"id {kid} field {field} got {after.get(field)!r} want {wval!r}")

    # Any EV DML on forbidden old-prefix keys (any op).
    for r in rows:
        if not _ev_topic(r.get("topic")):
            continue
        kid = _row_ev_key(r)
        if kid is None:
            continue
        op = _row_ev_op(r)
        if op not in ("c", "u", "d"):
            continue
        if kid in forbidden:
            report["errors"].append(f"old-prefix EV DML op={op} on forbidden key {kid}")

    # No extra EV business DML beyond declared inserts.
    for r in rows:
        if not _ev_topic(r.get("topic")):
            continue
        kid = _row_ev_key(r)
        op = _row_ev_op(r)
        if op not in ("c", "u", "d") or kid is None:
            continue
        if kid in forbidden:
            continue  # already reported
        if op == "c" and kid in expected_set:
            continue
        report["errors"].append(f"unexpected EV DML op={op} key={kid}")

    report["ok"] = not report["errors"]
    return report


def _assert_fails(fn, *, msg: str) -> None:
    try:
        fn()
    except SystemExit as e:
        if str(e) == "" and msg:
            raise SystemExit(f"self-test {msg}: empty SystemExit") from e
        return
    raise SystemExit(f"self-test {msg}: expected SystemExit")


def _insert_row(kid: str, after: dict[str, str], *, before: Any = None) -> dict[str, Any]:
    return {
        "topic": "v2p1.firstdata.r2.OLR_RAC_TEST.EV",
        "key": {"ID": kid},
        "value": {"op": "c", "before": before, "after": after},
    }


def self_test() -> None:
    base_after = {"ID": "1807500", "INST": "1", "TAG": "T", "VAL": "1", "PAD": "x"}
    row_a = _insert_row("1807500", base_after)
    row_b = _insert_row(
        "1807600",
        {"ID": "1807600", "INST": "2", "TAG": "T2", "VAL": "1", "PAD": "y"},
    )

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)

        ok_path = root / "records.jsonl"
        ok_path.write_text(json.dumps(row_a) + "\n" + json.dumps(row_b) + "\n")
        r = check_expected(
            ok_path,
            expected_ids=[1807500, 1807600],
            expected_after={1807500: base_after, 1807600: row_b["value"]["after"]},
        )
        if not r["ok"]:
            raise SystemExit(f"self-test baseline 2-row failed: {r}")

        _assert_fails(
            lambda: check_expected(root / "nope.jsonl", expected_ids=[1], expected_after={}),
            msg="missing file",
        )

        bad = root / "bad.jsonl"
        bad.write_text("{not json\n")
        _assert_fails(lambda: load_records(bad), msg="malformed JSON")

        nonobj = root / "nonobj.jsonl"
        nonobj.write_text('"string"\n')
        _assert_fails(lambda: load_records(nonobj), msg="non-object JSON")

        dup = root / "dup.jsonl"
        dup.write_text(json.dumps(row_a) + "\n" + json.dumps(row_a) + "\n")
        r2 = check_expected(dup, expected_ids=[1807500], expected_after={1807500: base_after})
        if r2["ok"]:
            raise SystemExit("self-test duplicate should fail")

        reord = root / "reord.jsonl"
        reord.write_text(json.dumps(row_b) + "\n" + json.dumps(row_a) + "\n")
        r3 = check_expected(
            reord,
            expected_ids=[1807500, 1807600],
            expected_after={1807500: base_after, 1807600: row_b["value"]["after"]},
        )
        if r3["ok"]:
            raise SystemExit("self-test reordered should fail")

        wrong_before = root / "wb.jsonl"
        wrong_before.write_text(json.dumps(_insert_row("1807500", base_after, before={"ID": "1"})) + "\n")
        r4 = check_expected(wrong_before, expected_ids=[1807500], expected_after={1807500: base_after})
        if r4["ok"]:
            raise SystemExit("self-test wrong before should fail")

        wrong_field = root / "wf.jsonl"
        wrong_field.write_text(json.dumps(_insert_row("1807500", {**base_after, "TAG": "NOPE"})) + "\n")
        r5 = check_expected(wrong_field, expected_ids=[1807500], expected_after={1807500: base_after})
        if r5["ok"]:
            raise SystemExit("self-test wrong field should fail")

        old_ins = root / "old.jsonl"
        old_ins.write_text(json.dumps(_insert_row("1807100", {**base_after, "ID": "1807100"})) + "\n")
        r6 = check_expected(
            old_ins,
            expected_ids=[1807500],
            expected_after={1807500: base_after},
        )
        if r6["ok"]:
            raise SystemExit("self-test consumed insert should fail")

        upd = root / "upd.jsonl"
        upd.write_text(
            json.dumps(row_a)
            + "\n"
            + json.dumps(
                {
                    "topic": row_a["topic"],
                    "key": {"ID": "1807100"},
                    "value": {"op": "u", "before": {}, "after": {}},
                }
            )
            + "\n"
        )
        r7 = check_expected(upd, expected_ids=[1807500], expected_after={1807500: base_after})
        if r7["ok"]:
            raise SystemExit("self-test consumed UPDATE should fail")

        dele = root / "del.jsonl"
        dele.write_text(
            json.dumps(row_a)
            + "\n"
            + json.dumps(
                {
                    "topic": row_a["topic"],
                    "key": {"ID": "1807100"},
                    "value": {"op": "d", "before": {}, "after": None},
                }
            )
            + "\n"
        )
        r8 = check_expected(dele, expected_ids=[1807500], expected_after={1807500: base_after})
        if r8["ok"]:
            raise SystemExit("self-test consumed DELETE should fail")

    print("compare_records_jsonl: self-test OK")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--records", type=Path)
    p.add_argument("--expect-ids", type=int, nargs="+")
    p.add_argument("--expect-json", type=Path, help="JSON map id->after fields")
    p.add_argument(
        "--forbid-prefix-ids",
        type=int,
        nargs="*",
        help="Extra old-prefix EV keys to forbid (added to first-flow consumed set)",
    )
    args = p.parse_args()
    if args.self_test:
        self_test()
        return
    if not args.records or not args.expect_ids or not args.expect_json:
        p.error("--records --expect-ids --expect-json required unless --self-test")
    spec_raw = json.loads(args.expect_json.read_text())
    if not isinstance(spec_raw, dict):
        raise SystemExit("expect-json must be a JSON object")
    expected_after: dict[int, dict[str, str]] = {}
    for k, v in spec_raw.items():
        if not isinstance(v, dict):
            raise SystemExit(f"expect-json[{k!r}] must be an object")
        expected_after[int(k)] = {str(f): str(val) for f, val in v.items()}
    extra = frozenset(str(i) for i in (args.forbid_prefix_ids or []))
    forbidden = FIRST_FLOW_CONSUMED_KEYS | extra
    report = check_expected(
        args.records,
        expected_ids=args.expect_ids,
        expected_after=expected_after,
        forbidden_old_prefix_keys=forbidden,
    )
    print(json.dumps(report, indent=2))
    sys.exit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()

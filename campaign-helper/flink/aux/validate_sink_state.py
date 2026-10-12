#!/usr/bin/env python3
"""Validate upsert sink final state (full rows + keyset); aux synthetic scope only."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

INT_FIELDS = frozenset({"ID", "INST", "VAL", "CTR"})


def strict_int_field(name: str, v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, bool):
        raise ValueError(f"{name}: bool is not a strict integer")
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        raise ValueError(f"{name}: float {v!r} rejected (no int truncation)")
    if isinstance(v, str):
        if v.strip().isdigit() or (v.startswith("-") and v[1:].isdigit()):
            return int(v)
        raise ValueError(f"{name}: non-integer string {v!r}")
    raise ValueError(f"{name}: unsupported type {type(v).__name__}")


def norm_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in row.items():
        if k in INT_FIELDS:
            out[k] = strict_int_field(k, v)
        else:
            out[k] = v
    return out


def load_final_rows(tsv: Path) -> dict[int, dict[str, Any]]:
    """Last non-null value per key; tombstone/null value lines remove key."""
    final: dict[int, dict[str, Any]] = {}
    for line in tsv.read_text().splitlines():
        if not line.strip():
            continue
        key_s, val_s = line.split("\t", 1)
        kid = json.loads(key_s)["ID"]
        kid = strict_int_field("ID", kid)
        if val_s.strip() == "null":
            final.pop(kid, None)
            continue
        final[kid] = norm_row(json.loads(val_s))
    return final


def rows_equal(exp: dict[str, Any], got: dict[str, Any]) -> bool:
    return norm_row(exp) == norm_row(got)


def validate_evidence_dir(evid: Path) -> tuple[list[str], dict, dict]:
    expect = json.loads((evid / "input_expectations.json").read_text())
    errors: list[str] = []
    try:
        ev = load_final_rows(evid / "sink_ev.raw.tsv")
        pp = load_final_rows(evid / "sink_pp.raw.tsv")
    except ValueError as e:
        return [f"strict parse: {e}"], {}, {}
    exp_ev = {int(k): v for k, v in expect["expected_final_rows"]["EV"].items()}
    exp_pp = {int(k): v for k, v in expect["expected_final_rows"]["PP"].items()}

    if set(ev.keys()) != set(exp_ev.keys()):
        errors.append(f"EV keyset expected {sorted(exp_ev)} got {sorted(ev)}")
    for eid, erow in exp_ev.items():
        if eid not in ev:
            errors.append(f"missing EV id {eid}")
            continue
        try:
            if not rows_equal(erow, ev[eid]):
                errors.append(f"EV id {eid} row mismatch expected {erow} got {ev[eid]}")
        except ValueError as e:
            errors.append(f"EV id {eid} strict type: {e}")
    for eid in expect.get("ev_deleted_ids", []):
        if eid in ev:
            errors.append(f"EV id {eid} should be absent after delete/tombstone")

    for eid in ev:
        if eid not in exp_ev:
            errors.append(f"unexpected extra EV id {eid}")

    pid = int(expect["pp_id"])
    if set(pp.keys()) != {pid}:
        errors.append(f"PP keyset expected [{pid}] got {sorted(pp)}")
    elif pid in pp:
        erow = exp_pp[pid]
        try:
            if not rows_equal(erow, pp[pid]):
                errors.append(f"PP row mismatch expected {erow} got {pp[pid]}")
        except ValueError as e:
            errors.append(f"PP strict type: {e}")
    else:
        errors.append("missing PP row")

    return errors, ev, pp


def run_selftest() -> int:
    """Deterministic negative cases; each must fail validation."""
    import tempfile

    base_expect = {
        "pp_id": 1,
        "ev_deleted_ids": [],
        "expected_final_rows": {
            "EV": {"1201984": {"ID": 1201984, "INST": 1, "VAL": 1, "CTR": 1}},
            "PP": {"1": {"ID": 1, "INST": 1, "VAL": 1, "CTR": 104}},
        },
    }
    cases = [
        ("pp_float_trunc", {"CTR": 104.9}),
        ("pp_string_decimal", {"CTR": "104.9"}),
        ("pp_extra_key", None),  # special
        ("pp_wrong_ctr", {"CTR": 103}),
    ]
    results = []
    for name, patch in cases:
        with tempfile.TemporaryDirectory() as td:
            evid = Path(td)
            exp = json.loads(json.dumps(base_expect))
            if name == "pp_extra_key":
                sink_pp = '{"ID":1}\t{"ID":1,"INST":1,"VAL":1,"CTR":104}\n{"ID":2}\t{"ID":2,"INST":1,"VAL":1,"CTR":1}\n'
            else:
                sink_row = exp["expected_final_rows"]["PP"]["1"].copy()
                sink_row.update(patch)
                sink_pp = f'{{"ID":1}}\t{json.dumps(sink_row)}\n'
            (evid / "input_expectations.json").write_text(json.dumps(exp))
            (evid / "sink_ev.raw.tsv").write_text(
                '{"ID":1201984}\t{"ID":1201984,"INST":1,"VAL":1,"CTR":1}\n'
            )
            (evid / "sink_pp.raw.tsv").write_text(sink_pp)
            errors, _, _ = validate_evidence_dir(evid)
            passed = len(errors) > 0
            results.append({"case": name, "expected_fail": True, "got_fail": passed, "errors": errors})
    ok = all(r["got_fail"] for r in results)
    print(json.dumps({"selftest": "validate_sink_state", "ok": ok, "results": results}, indent=2))
    return 0 if ok else 1


def main() -> None:
    if len(sys.argv) == 2 and sys.argv[1] == "--selftest":
        raise SystemExit(run_selftest())

    evid = Path(sys.argv[1])
    errors, ev, pp = validate_evidence_dir(evid)

    out = {
        "result": "PASS" if not errors else "FAIL",
        "errors": errors,
        "ev_final_rows": ev,
        "pp_final_rows": pp,
        "scope": "aux_synthetic_only",
    }
    (evid / "SMOKE-OUTCOME.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()

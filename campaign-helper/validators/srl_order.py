"""Per-key ordered full transition verification (not op-letter subsequence)."""

from __future__ import annotations

from typing import Any

from srl_match import ops_match
from srl_oracle import ExpectedOp
from srl_kafka import ParsedBusinessOp


def _sort_actual(ops: list[ParsedBusinessOp]) -> list[ParsedBusinessOp]:
    return sorted(ops, key=lambda o: (o.topic, o.partition, o.kafka_offset))


def verify_per_key_ordered_transitions(
    expected: list[ExpectedOp],
    actual: list[ParsedBusinessOp],
) -> list[dict[str, str]]:
    failures: list[dict[str, str]] = []
    exp_by_key: dict[tuple[str, Any], list[ExpectedOp]] = {}
    for op in sorted(expected, key=lambda o: (o.seq_on_key or 0, o.op_id)):
        kid = op.key.get("ID")
        exp_by_key.setdefault((op.table, kid), []).append(op)

    act_by_key: dict[tuple[str, Any], list[ParsedBusinessOp]] = {}
    for op in _sort_actual(actual):
        act_by_key.setdefault((op.table, op.key.get("ID")), []).append(op)

    for key, exp_list in exp_by_key.items():
        act_list = act_by_key.get(key, [])
        if len(exp_list) != len(act_list):
            failures.append(
                {
                    "code": "CAUSAL_ORDER",
                    "detail": f"key {key}: expected {len(exp_list)} transitions, got {len(act_list)}",
                }
            )
            continue
        for i, (e, a) in enumerate(zip(exp_list, act_list)):
            if not ops_match(e, a):
                failures.append(
                    {
                        "code": "CAUSAL_TRANSITION",
                        "detail": (
                            f"key {key} step {i + 1}: op_id {e.op_id} "
                            f"full row transition mismatch (not just op label)"
                        ),
                    }
                )
    return failures

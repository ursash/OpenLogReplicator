# Validator acceptance matrix (post–Astra HOLD fixes)

| Check | Expected | Evidence |
| --- | --- | --- |
| Per-key causal order | Full transition match (`CAUSAL_TRANSITION` on u/u swap) | `hold_regression.py::test_uu_swap_same_labels` |
| KafkaRawCapture manifest | `partitions[]`, `complete=true`, `isolation_level=read_committed`, captured counts; **no** contiguous offset fill | `hold_regression` gap 96–98 PASS; incomplete/zero/isolation FAIL |
| Field typing | VARCHAR exact; INT normalized; NULL vs absent vs unavailable distinct | `hold_regression` PAD/VAL/unavailable |
| Final state | `present=true` requires `after`; DML rebuild cross-check + key coverage | `hold_regression` + `crosscheck_final_state_vs_dml` |
| Empty Flink sink | FAIL when oracle expects rows | `hold_regression::test_empty_sink_fail` |
| Truncated oracle | `ORACLE_TRUNCATED` / incomplete forbidden set | `hold_regression::test_truncated_oracle_qualify` |
| Primary smoke sample (current) | **INPUT_INCOMPLETE** (final_state omits SHORT/LONG vs DML) | `qualification_primary_smoke_known_incomplete` in self-test |
| Business path (DML-complete final synthesized for test only) | PASS on synthetic kafka+manifest+flink | `positive_v2_smoke` self-test |
| Transport tombstone (`tombstones.on.delete=false`) | Not counted as extra DML | `kafka_transport_tombstones` count; matching via envelope `d` |

Prior freeze checksums: `SHA256SUMS.freeze-hold-v1`. Current: `SHA256SUMS`.

Primary coordination: minimum qualification evidence → `ORACLE-QUALIFICATION-EVIDENCE.schema.json`.

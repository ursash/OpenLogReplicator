# P9 SRL validator — delivery report (HOLD remediation)

Worktree: `campaign-helper/validators/` only.

## Astra HOLD items addressed

1. **Causal order** — `srl_order.verify_per_key_ordered_transitions`: ordered full row match per key (not op-letter subsequence).
2. **Manifest** — `srl_capture_manifest.py` parses KafkaRawCapture `partitions[]`; validates completion, isolation, counts, end reached, unique coords; **allows EOS offset gaps**.
3. **Fields** — typed INT vs VARCHAR; rejects `__kind__: unavailable`; absent ≠ NULL.
4. **Final state** — load rejects `present` without `after`; DML rebuild cross-check + coverage; empty sink fails.
5. **Qualification** — pin-aware P103/P101 counts, forbidden rollback set, truncated oracle detection; Primary evidence schema shipped.
6. **Tombstones** — empty `value_raw` = transport marker when `TOMBSTONES_ON_DELETE_DEFAULT=false`; not multiset DML.

## Tests executed

```bash
python3 -B hold_regression.py          # exit 0
python3 -B validate_lane.py --self-test # exit 0
```

Evidence (path at test time): `evidence/self-test-last.json` — **relocated:** [../../../../../oracle-cdc-benchmark/olr-rac/evidence/source-reorganization-20261014T034848Z/curation/legacy-helper-evidence/campaign-helper/validators/evidence/self-test-last.json](../../../../../oracle-cdc-benchmark/olr-rac/evidence/source-reorganization-20261014T034848Z/curation/legacy-helper-evidence/campaign-helper/validators/evidence/self-test-last.json) (unchanged payload; `SHA256SUMS` / freeze-hold labels **historical** for deleted Git-tree evidence).

## Primary integration

Frozen module + `SHA256SUMS` (previous: `SHA256SUMS.freeze-hold-v1`).

Until Primary fixes generator final_state (SHORT/LONG rows, etc.), **official smoke artifacts remain INPUT_INCOMPLETE** at qualification — not a lane PASS.

CLI unchanged: `--expected-events`, `--expected-final-state`, `--oracle-pin`, `--kafka`, `--flink-sink`, `--capture-manifest` (KafkaRawCapture shape; collector should emit `isolation_level: read_committed`).

# Primary contract gaps (validator-required, not yet in published driver output)

Validator implements **mandatory complete schemas** (`mandatory_schema.py`), **provenance cross-bind** (`srl_provenance.py`), and Astra memory harness (`mandatory_field_regressions.py`). Live EXIT0 requires `evidence_class=primary_stock37` **and** full `verify_provenance_crossbind` PASS (见 `PROVENANCE-MINIMAL-CONTRACT.md`). Primary bounded raw smoke 仍 **HOLD**。

Worktree validator implements gates below; Primary must align shared manifests/driver.

## WORKLOAD-RECEIPT-CONTRACT.json

| Gap | Validator behavior |
| --- | --- |
| `host` required on all receipt types | `P100` boundary in `workload_driver.sh` omits `host` → receipts fail load until fixed |
| `P105` / pingpong completion | Smoke gate requires `phase_boundary` for `P105`; driver only journals `W105` today |
| `sql_exit` on failed SQL | Non-zero `sql_exit` on `sql_commit` → `RECEIPT_SQL_FAILED` / INPUT_INCOMPLETE |
| Campaign `P107` barriers | Requires `barrier` notes covering long tx + log switch + committed/key_anchor |

## MEASUREMENT-WINDOW.json (measure-v1)

Driver writes base doc; validator additionally requires:

| Field | Purpose |
| --- | --- |
| `p099_catch_up_proven: true` | Observed catch-up, not declared-only P099 boundary |
| `lanes.baseline.consumer_start_utc` | Per-lane consumer lower bound |
| `lanes.baseline.catch_up_after_p099_utc` | Must be ≤ `consumer_start_utc` |
| `lanes.recovery.*` | Same for recovery lane |

Published `workload_driver.sh` inline JSON lacks `lanes` and `p099_catch_up_proven` until Builder extends writer.

## ORACLE-QUALIFICATION-EVIDENCE

| Field | Purpose |
| --- | --- |
| `pin_sha256` / `dml_sha256` / `final_state_sha256` | Bind artifacts (validator computes from CLI paths) |
| `expected_committed_table_counts` | Per-phase/per-table (`P101/EV`, …) |
| `expected_forbidden_counts` | Only `rollback_full` / `rollback_partial` (not duplicated phase keys) |

Sample `ORACLE-QUALIFICATION-EVIDENCE.smoke.sample.json` in manifests still mixes phase keys into `expected_forbidden_counts`.

## Kafka envelope

Validator requires `source.schema=OLR_RAC_TEST`, `source.table` matching topic table, and `source.thread` with distinct redo threads per RAC instance when both instances appear in capture.

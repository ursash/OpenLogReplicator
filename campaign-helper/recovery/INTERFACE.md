# Recovery phase operator interface

Recovery lane only: REST **28133**, OLR **39233**, tap **39433**. Baseline **28132/39232** and aux Flink **28140** are rejected as REST targets.

## Per-phase entry

```bash
export RUN_ID=20261012T031500Z
export EV=/path/to/evidence/live-srl-e2e-${RUN_ID}
export PREP=/tmp/olr-connect-live-srl-e2e-${RUN_ID}-prep
export ATTEMPT=$EV/attempts/<attempt-id>
export RECOVERY_EXEC=live          # mock only for module local verify
export RECOVERY_ALLOW_KILL=1       # required for R2 live SIGKILL
export TAP_PATH=/path/to/recovery/tap.data.jsonl   # R2: required for live inflight proof
export BASELINE_READONLY_SNAPSHOT=$EV/state/baseline/connector-status.json  # optional read-only

module/bin/recovery_phase.sh R1
module/bin/recovery_phase.sh R2
module/bin/recovery_phase.sh R3
```

## Outputs (under `$ATTEMPT/phases/`)

- `config-hash.txt`, `register-identity.json`
- `R1|r2|r3/r*-phase-record.json` — verdict `PASS|FAIL|INCONCLUSIVE`, commands, rc
- Per-phase REST snapshots, `connect-offsets.jsonl` (ReadOffsetsTopic), DG logs under `$EV/state/recovery/dg/` for R3

## MAIN hooks (not auto-faked in live mode)

| Hook | Phase | Purpose |
|------|-------|---------|
| `TAP_PATH` | R2 | Prove source/tap progress during inflight window |
| `BASELINE_READONLY_SNAPSHOT` | all | Baseline continuity pointer (GET-only file) |
| `RECOVERY_ALLOW_KILL=1` | R2 | Explicit opt-in to SIGKILL private worker |
| `RECOVERY_CATCHUP_RECEIPT` | all | MAIN JSON: `phase`, `qualified:true`, `offset_watermark` or `business_binding` — required for **PASS** |
| `PREP/scripts/ReadOffsetsTopic.java` + worker-lib | all | Persistent offset-topic raw dumps |
| `bin/dg_r3_recovery_olr.sh` + `bin/dg_r3_procio.py` | R3 | `preflight` (recovery OLR must already run) → snapshot/stop/continue/verify; `/proc` reads via **sudo python**; full OLR identity (loaded exe sha, oracle uid, starttime, listener pid); continue = direct pinned binary (no config rewrite); follower evidence requires baseline+recovery **MATCH** with readable `RUN_DIR` (no follower stop/start; not `dg/dg_live_srl.sh`) |

`offset-progress.json` records fingerprint movement only — **not** phase PASS or next-fault permission. Missing/invalid receipt → `INCONCLUSIVE` even if progress observed.

## Reference lab wiring

Derived from `olr-rac/lab/live_srl_e2e/cdp/recovery_trials.sh` (stop/resume, worker relaunch, `dg_live_srl.sh` **stop+start**). Replaces stub `recovery_trials.sh` fixed sleeps and R3 stop-only.

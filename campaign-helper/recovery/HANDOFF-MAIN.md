# MAIN handoff — campaign-helper/recovery

Copy module to lab (rsync without `--delete`), verify `sha256sum -c FROZEN-SHA256.txt`, then on **CDP recovery worker only**:

```bash
RECOVERY_EXEC=live RECOVERY_ALLOW_KILL=1 \
TAP_PATH="$EV/state/recovery/capture/tap.data.jsonl" \
RECOVERY_CATCHUP_RECEIPT="$ATTEMPT/phases/R1/catchup-receipt.json" \
  ./bin/recovery_phase.sh R1
# MAIN writes receipt (qualified, phase, offset_watermark/business_binding) after business compare.
# Serial advance only after PASS + receipt; offset fingerprint progress alone is not PASS.

RECOVERY_CATCHUP_RECEIPT="$ATTEMPT/phases/R2/catchup-receipt.json" ./bin/recovery_phase.sh R2
# R2 live relaunch: `java -cp "$PREP/worker-lib/*"` (one -cp arg; do not shell-expand `"$PREP/worker-lib"/*`).
# If lab copy predates this fix, patch deployed `recovery_phase.sh` the same way (dated repair OK).

RECOVERY_CATCHUP_RECEIPT="$ATTEMPT/phases/R3/catchup-receipt.json" ./bin/recovery_phase.sh R3
```

R3 uses **`bin/dg_r3_recovery_olr.sh`** (preflight requires recovery OLR already running; continue relaunches **pinned** `OpenLogReplicator` against existing `config.json`/checkpoint — no wrapper config rewrite; **no** `inventory_follow` stop/start). Do **not** call `dg/dg_live_srl.sh stop|start` (global `pkill inventory_follow.sh` / fresh `olr_rac_run`).

Deploy to `$DG_LAB` before R3 live (same pin as `pin_bin` in shell):

1. `dg_r3_procio.py` → `$DG_LAB/dg_r3_procio.py` (`sha256sum` must match module `FROZEN-SHA256.txt`)
2. `dg_r3_recovery_olr.sh` → `$DG_LAB/dg_r3_recovery_olr.sh`

On DG1 (no stdin / no `bash -s` — `BASH_SOURCE` is unbound under `-s`):

`dg_ssh 'env LAB=... RUN_ID=... RUN_NAME=... OLR_PORT=39233 DG_R3_PROCIO=$DG_LAB/dg_r3_procio.py bash $DG_LAB/dg_r3_recovery_olr.sh preflight|snapshot|stop|continue|verify'`

Identity: `CONNECTOR_NAME=ocr370-rac9-live-srl-${RUN_ID}-recovery`, `REST=http://127.0.0.1:28133`, `DG_LAB=/opt/olr-rac/live-srl-e2e-${RUN_ID}-recovery-lab`, `RUN_NAME=ocr370-rac9-live-srl-e2e-${RUN_ID}-recovery-olr`.

Do not invoke against baseline REST or shared Kafka ops from this module.

Local negative/ mock proof: `bin/run_local_verify.sh` (no P9).

Anchor: `FROZEN-SHA256.txt` at module root after copy (Lead re-freeze includes R2 `-cp` fix + Astra R3/procio handoff; rsync module and re-verify).

**Notify MAIN:** new module freeze delivered on branch/worktree — sync `campaign-helper/recovery/`, `sha256sum -c FROZEN-SHA256.txt`, deploy `dg_r3_procio.py` + `dg_r3_recovery_olr.sh` to `$DG_LAB`, apply R2 `-cp` line if lab script is stale.

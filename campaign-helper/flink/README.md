# Live SRL Flink integration helper (frozen)

Stock Flink SQL on CSA parcel: **Kafka `debezium-json` → `upsert-kafka`** for **EV** and **PP**, pinned to `FLINK-INTEGRATION-CONTRACT.json` v1 and `FLINK-RUNTIME-DELTA-v1.json`.

## Primary (Builder) interface

| Step | Command |
| --- | --- |
| Render SQL for a lane | `RUNTIME_ROOT=/opt/oracle-cdc-benchmark/flink-live-srl-20261012T031500Z REST_PORT=28120 ./bin/render_lane_sql.sh baseline /tmp/flink-sql-baseline` |
| Start shared campaign runtime | `RUNTIME_ROOT=... REST_PORT=28120 ./bin/runtime_ctl.sh start` |
| Submit EV+PP jobs | `RUNTIME_ROOT=... REST_PORT=28120 ./bin/submit_lane_jobs.sh /tmp/flink-sql-baseline` |
| Wait RUNNING + checkpoint | `./bin/wait_job_ready.sh 28120 file://.../checkpoints/baseline live-srl-20261012T031500Z-baseline` |
| Dump sink topics | `./bin/dump_upsert_sink.sh 10.129.80.147:9092 <sink-topic> out.tsv` |

Campaign Kafka bootstrap is **`10.129.80.147:9092`** (not `127.0.0.1`). Connect payloads use **`schemas.enable=false`**; treat **ID / INST / VAL / CTR** as JSON **strings** in source DDL and **CAST** to BIGINT in `INSERT`.

Per-lane **`state.checkpoints.dir`** is set in each SQL file (baseline vs recovery). Cluster `flink-conf.yaml` only sets a shared parent under `checkpoints/_cluster` so the first lane to start does not overwrite the other lane’s checkpoint root.

## Auxiliary smoke (Flink worker only)

Isolated topics `ocr370.live.srl.aux.<AUX_RUN_ID>.*`, runtime under `/tmp/flink-aux-smoke-<AUX_RUN_ID>`, **REST ≥ 28140**. Never **28120 / 28132 / 28133**.

```bash
AUX_RUN_ID=aux20261012T032900Z REST_PORT=28140 ./aux/run_aux_smoke.sh
./aux/cleanup_aux_smoke.sh aux20261012T032900Z
```

Deterministic EV keys: **1201984 / 1201985** (RAC1), **1201994 / 1201995** (RAC2). PP **id=1**: baseline CTR **100**, **+4** after four updates → **104**.

## Freeze

```bash
./bin/freeze_manifest.sh
```

Evidence for aux runs lives under `evidence/aux-<AUX_RUN_ID>/` (gitignored from SHA manifest).

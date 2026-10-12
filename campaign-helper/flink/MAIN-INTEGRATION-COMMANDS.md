# MAIN 最小接入（integration HOLD 修复后）

Lead `8e6eafd0` / MAIN `aa4ee83f` — 从 worktree 复制模块，**本轮勿启动 MAIN 28120**。

```bash
SRC=/home/shihao/Projects/github/OpenLogReplicator/campaign-helper/flink
DST=/home/shihao/Projects/oracle-cdc-benchmark/olr-rac/lab/live_srl_e2e/cdp/flink-helper
mkdir -p "$DST"
rsync -a "$SRC/" "$DST/"
(cd "$DST" && sha256sum -c FROZEN-SHA256.txt)
```

Campaign 模式（Builder 在 P9 上、**既定 root + 28120**）：

```bash
export FLINK_RUNTIME_MODE=campaign
export RUNTIME_ROOT=/opt/oracle-cdc-benchmark/flink-live-srl-20261012T031500Z
export REST_PORT=28120
./bin/render_lane_sql.sh baseline /tmp/flink-sql-baseline
./bin/runtime_ctl.sh start
./bin/submit_lane_jobs.sh /tmp/flink-sql-baseline /tmp/flink-sql-baseline/job_ids.json
./bin/wait_job_ready.sh 28120 /tmp/flink-sql-baseline/job_ids.json
# recovery lane: render_lane_sql.sh recovery + second job_ids.json (4 jobs total)
```

端口表见 `config/portmap.json`（campaign REST **28120**，JM RPC **28121**；aux REST **28140**，JM RPC **28141**，避免默认 **6123** 共机冲突）。

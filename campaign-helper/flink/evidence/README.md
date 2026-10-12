# Evidence retention (Flink aux worker)

| Run | Outcome | Notes |
| --- | --- | --- |
| `aux-aux20261012T040200Z` | FAIL (PP slot) | 保留原始 `SMOKE-OUTCOME.json` / raw，勿覆盖 |
| `aux-aux20261012T042200Z` | PASS (Astra 镜像) | 历史冒烟；见 `JOB-ID-HISTORY.json`（同名双 job，旧 raw 不能证新 ID） |
| `fix-hold-selftest` | 负例自测 | `NEGATIVE-RESULTS.json` |
| 修复后 | 见 `EVIDENCE-SEAL.sha256` + `wait_ready_report.json` | 绑定 submit 的 job IDs |

cleanup 前必须先 `aux/cleanup_aux_smoke.sh` 内 tar 归档；MAIN 拷贝完成前不得删除 CDP 上活跃 proof。

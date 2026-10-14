# SRL 测试与验证工具

四个模块已于 2026-10-12 归并。**v1 交付线** 活跃 harness 入口：`/home/shihao/Projects/github/OpenLogReplicator-worktrees/rac-v1/campaign-helper/`（含 **`native-rac/`** 交付 tooling）。共享四模块源码与 main 同源；实验证据在 benchmark `olr-rac/`。

| 模块 | 用途 | 入口 |
| --- | --- | --- |
| `validators/` | 行级事件、字段、顺序、回滚排除和 Flink 最终态验证 | [接口说明](validators/INTERFACE.md) |
| `smoke-business/` | 有界 smoke 业务与采集窗口验证 | [接口说明](smoke-business/INTERFACE.md) |
| `flink/` | Flink SQL、运行身份、作业与证据工具 | [模块说明](flink/README.md)、[集成命令](flink/MAIN-INTEGRATION-COMMANDS.md) |
| `recovery/` | R1/R2/R3 恢复流程与取证工具 | [接口说明](recovery/INTERFACE.md)、[交付说明](recovery/HANDOFF-MAIN.md) |

## 使用边界

OLR 产品源码 lane 与 P9 运行态见 benchmark [`AGENTS.md`](../../../../oracle-cdc-benchmark/AGENTS.md) 与仓库根 ignored `AGENTS.md`。实验报告、冻结合同和 P9 证据仍在 [`olr-rac/`](../../../../oracle-cdc-benchmark/olr-rac/)，按各次 pin 使用。

迁移只验证文件完整性、入口路径和轻量本地控制。原有 `FAIL`、`INVALID`、`HOLD` 和 `NOT_ACCEPTED` 保留；本次没有部署 P9，也没有新增真实链路验收结论。

`HANDOFF-PRIMARY.json`、`NOTIFY-MAIN-LEAD.json` 保留历史 worktree 来源。**当前 v1 lane 入口** 为本 worktree 的 `campaign-helper/`（非 main checkout）。

## 校验与记录

模块内 `SHA256SUMS` / `FROZEN-SHA256.txt` 仍指迁移时 harness **源码** 版本；**不**再声称对当前 Git 树 `MIGRATED-SHA256SUMS` / 已删 localevidence `checksum all OK`（historical / not-all-current-PASS）。

迁移清单与历史 run 输出（迁出 Git，payload 在 benchmark curation，路径含 `campaign-helper/` 嵌套）：

- 根：[`legacy-helper-evidence/campaign-helper/`](../../../../oracle-cdc-benchmark/olr-rac/evidence/source-reorganization-20261014T034848Z/curation/legacy-helper-evidence/campaign-helper/)
- [`evidence/migration-20261012T084302Z/`](../../../../oracle-cdc-benchmark/olr-rac/evidence/source-reorganization-20261014T034848Z/curation/legacy-helper-evidence/campaign-helper/evidence/migration-20261012T084302Z/)
- [`flink/evidence/`](../../../../oracle-cdc-benchmark/olr-rac/evidence/source-reorganization-20261014T034848Z/curation/legacy-helper-evidence/campaign-helper/flink/evidence/)
- [`recovery/evidence/`](../../../../oracle-cdc-benchmark/olr-rac/evidence/source-reorganization-20261014T034848Z/curation/legacy-helper-evidence/campaign-helper/recovery/evidence/)
- [`smoke-business/evidence/`](../../../../oracle-cdc-benchmark/olr-rac/evidence/source-reorganization-20261014T034848Z/curation/legacy-helper-evidence/campaign-helper/smoke-business/evidence/)
- [`validators/evidence/`](../../../../oracle-cdc-benchmark/olr-rac/evidence/source-reorganization-20261014T034848Z/curation/legacy-helper-evidence/campaign-helper/validators/evidence/)

索引与 hash：[`curation/receipts-curation-staging-20261014T040712Z.json`](../../../../oracle-cdc-benchmark/olr-rac/evidence/source-reorganization-20261014T034848Z/curation/receipts-curation-staging-20261014T040712Z.json)。原 `migration-validation.json` 等 **historical** 判定不因 Git 删 evidence 而改写。

原 validator 清单包含缓存条目，源目录核对时有一个 `.pyc` 不匹配。本次原样保存该失败结果；新清单排除 33 个可重建 Python 缓存。

现有自测会改写 fixture 或 evidence。为保留历史结果，本次在临时复制中运行，结束后已删除临时目录。回退时先核对迁移清单，再从 curation 快照恢复所需文件；不要覆盖迁移后他人新增的改动。

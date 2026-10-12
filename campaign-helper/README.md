# SRL 测试与验证工具

四个模块的开发成果已于 2026-10-12 从两个 Paseo worktree 归并到本目录。后续开发使用 `/home/shihao/Projects/github/OpenLogReplicator/campaign-helper/`，同一时间由一个代理写入。

| 模块 | 用途 | 入口 |
| --- | --- | --- |
| `validators/` | 行级事件、字段、顺序、回滚排除和 Flink 最终态验证 | [接口说明](validators/INTERFACE.md) |
| `smoke-business/` | 有界 smoke 业务与采集窗口验证 | [接口说明](smoke-business/INTERFACE.md) |
| `flink/` | Flink SQL、运行身份、作业与证据工具 | [模块说明](flink/README.md)、[集成命令](flink/MAIN-INTEGRATION-COMMANDS.md) |
| `recovery/` | R1/R2/R3 恢复流程与取证工具 | [接口说明](recovery/INTERFACE.md)、[交付说明](recovery/HANDOFF-MAIN.md) |

## 使用边界

OLR 产品源码、主分支和 P9 运行态按仓库根 `AGENTS.md` 管理。实验报告、冻结合同和原始 P9 证据仍在 `../../../oracle-cdc-benchmark/olr-rac/`，已有部署副本按各次实验的 pin 使用。

迁移只验证文件完整性、入口路径和轻量本地控制。原有 `FAIL`、`INVALID`、`HOLD` 和 `NOT_ACCEPTED` 保留；本次没有部署 P9，也没有新增真实链路验收结论。

`HANDOFF-PRIMARY.json`、`NOTIFY-MAIN-LEAD.json` 和历史证据中的旧 worktree 路径记录原交付来源。当前源码入口以本页为准。两个现用 synthetic fixture 的路径已指向新目录，内容中的合同、业务值和判定没有改变。

## 校验与记录

在本目录执行 `sha256sum -c MIGRATED-SHA256SUMS`，应全部显示 `OK`。各模块的 `SHA256SUMS` 或 `FROZEN-SHA256.txt` 标明本次迁移版本；原清单与原文件保存在迁移快照中。

迁移清单与回退快照：[evidence/migration-20261012T084302Z/](evidence/migration-20261012T084302Z/)。其中 `migration-manifest.json` 记录逐文件来源、SHA256、权限与路径改动；`original-helpers.tar.gz` 保留迁移前的 838 个文件。

原 validator 清单包含缓存条目，源目录核对时有一个 `.pyc` 不匹配。本次原样保存该失败结果；新清单排除 33 个可重建 Python 缓存。迁移校验结果见 `migration-validation.json`，不替代实验验收。

现有自测会改写 fixture 或 evidence。为保留历史结果，本次在临时复制中运行，结束后已删除临时目录。回退时先核对迁移清单，再从快照恢复所需文件；不要覆盖迁移后他人新增的改动。

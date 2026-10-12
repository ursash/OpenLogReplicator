# PROVENANCE 最小交叉绑定（MAIN ↔ Validator）

版本：`20261012T031500Z-provenance-v1`。`evidence_class` 仅为声明；**live PASS** 须 artifact 解析交叉验证通过。**HOLD**：无 Primary bounded raw 时不 claim live 正例。

## Kafka `source.txId`（stock37）

格式：`0xUSN.SLOT.SQN`。Validator 将 envelope `txId` 与 commit JSONL 行内 `xidusn` / `xidslot` / `xidsqn`（或 `raw_xid_bytes_hex`）**等值**比较。

## commit SCN 来源（Oracle 独立）

`commit_scn_source` **仅允许**：

- `oracle_redo_commit`
- `oracle_logminer_commit`

**禁止**：`olr_transaction_end`、OLR END/输出流、任意 CDC 非 Oracle 源、`V$TRANSACTION` 活跃视图标签冒充 commit SCN。用户侧 END 记一笔**不**进入本轮 validator 资格门。

## 原记录 JSONL + locator

仅 **`line:N`**（1 起始，整文件 SHA256）。

### oracle-commit.smoke.jsonl

Binding：`oracle.commit_evidence_ref`。`commit_scn` 数值以该行为准。

### srl-witness.smoke.jsonl

Binding：`srl_witness.artifact_ref`。身份字段以该行为准交叉校验（incarnation / thread / sequence / member）。

## 时序：validator **实际检查** vs **未证明**

### 已实现（记录时间基准，逐 binding）

| 关系 | 代码 |
| --- | --- |
| `consumer_start_utc` ≤ `sampled_utc` | `PROVENANCE_TEMPORAL_ORDER` |
| `sampled_utc` ≤ Kafka 行 `timestamp` | 同上 |
| `sampled_utc` ≤ `archive_delivered_utc` | 同上（witness 行内自洽，**非** DG 可用性证明） |

### **未证明**（gap = unknown，不得写进 live 结论）

| 命题 | 说明 |
| --- | --- |
| Kafka 交付 **早于** archive **首次可用** | 需要 catalog/inventory `archive_first_observed_utc`（或 MAIN 实测等价界），**不能**用 `W104` SQL `archive_current.sql` 完成时刻代替 DG archive 首次可用 |
| 全量 event 均为 pre-archive | synthetic smoke **不**对每条 op 声明 pre-archive；仅 witness 行内 `growth_state` / 可选 `pre_archive_claim`（Primary 由 MAIN 填） |
| receipt 即 archive 可用 | receipt 仅可作 campaign **上界/触发点** 文档；provenance **不**再 parse `W104` 与 `archive_delivered_utc` 对齐 |

### pre-archive 见证（Primary / MAIN 范围，本轮 smoke 不验）

若 MAIN 对某 op 声明 pre-archive，需独立原记录证明（同一 incarnation/thread/sequence 下 SRL **增长中**、archive **尚不可用**）且 Kafka 已可见对应 committed 事件；Flink 侧原记录由 MAIN 实测。**Validator synthetic 路径不强制 growth=open/growing。**

## checkpoint / commit SCN

- `checkpoint_scn`：`source` ∈ `kafka_envelope` \| `olr_checkpoint`，`value` = envelope `scn`
- `commit_scn`：来自 commit JSONL + 允许的 `commit_scn_source`；可与 checkpoint 数值相等

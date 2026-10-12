# Live SRL 严格验证器 — 接口说明（expected-ops-v2）

集成 pin：`20261012T031500Z-v1`。期望操作 pin：`20261012T031500Z-expected-ops-v2`。

## CLI

```bash
python3 validate_lane.py \
  --lane baseline|recovery \
  --kafka state/<lane>/capture/kafka-raw.jsonl \
  --expected-events state/workload-expected-operations.jsonl \
  --expected-final-state state/workload-expected-final-state.json \
  --oracle-pin state/workload-oracle-pin.json \
  --flink-sink state/<lane>/flink/sink-rows.jsonl \
  --capture-manifest state/<lane>/capture/manifest.json \
  --execution-receipts state/workload-execution-receipts.jsonl \
  --measurement-window state/MEASUREMENT-WINDOW.json \
  --qualification-evidence state/ORACLE-QUALIFICATION-EVIDENCE.json \
  [--journal state/workload-expected-journal.jsonl] \
  [--workload-mode smoke|campaign]
```

Receipt / measurement / qualification evidence 为 **运行时门**（oracle 资格 PASS 之后）。缺件或矛盾 → `INPUT_INCOMPLETE`（`RUNTIME_GATE` / `SOURCE_*`），非 lane 业务 FAIL。自测可用 `--allow-missing-runtime-gates`。

- DML oracle：JSONL（`emit_expected_operations.py --out`），**不含**最终态重复 `u`。
- 最终态：`workload-expected-final-state.json`，顶层 `record_type: final_key_state`，`entries[]` 用 `present` + `after` 或 absent key。
- `_null_fields`：Oracle NULL 列（如 PAD、删除前的 INST）；比较时视为 NULL，**不**接受通配或缺列。
- 退出码：`0` PASS，`1` FAIL，`2` INPUT_INCOMPLETE（含 oracle 资格未过），`3` 用法错误。

## 计数分离

| 字段 | 含义 |
| --- | --- |
| `counts.oracle_kafka_committed_events` | 须出现在 Kafka 的 committed DML |
| `counts.oracle_final_state_entries` | 最终态 artifact 条目（不计入 Kafka multiset） |
| `counts.oracle_forbidden_records` | `committed: false`（含 i%7 / i%10 精确 key） |
| `counts.kafka_capture_business_ops` | 捕获解析后的业务 op |

## 验收维度（诚实分离，非 campaign 审批）

| 字段 | 含义 |
| --- | --- |
| `acceptance_dimensions.business_correctness` | 行级业务 / Flink / capture  multiset 与因果序 |
| `acceptance_dimensions.provenance_content_consistency` | PROVENANCE 交叉绑定与 artifact 内容自洽 |
| `acceptance_dimensions.independent_source_qualification` | 本 checker **固定** `UNVERIFIED`（不读取输入标签） |
| `acceptance_dimensions.prearchive_status` | 本 checker **固定** `UNKNOWN` |
| `acceptance_dimensions.campaign_status` | 本 checker **固定** `NOT_ACCEPTED` |
| `untrusted_declared_qualifiers` | 若证据 JSON 含 `evidence_class` / `independent_source_qualification` / `prearchive_status` / `campaign_status`，原样 echo 供审计，**不参与** gate |
| `acceptance` | component 为 `TEST_ONLY_COMPONENT`；live CLI 恒为 `not_campaign_accepted`（本模块**从不**输出 `live` / campaign accepted） |

独立来源 / pre-archive / campaign 结论由 Lead/Astra 在 MAIN 实测链路外另行认定；输入 doc 中的 `status=VERIFIED` 等声明与改 `evidence_class` 一样，**不能**在本 validator 内升级上述 computed 维度。

## 资格门

Sidecar 不符合 v2 约束时 `qualification.status=INCOMPLETE` → **INPUT_INCOMPLETE**（非 lane 业务 FAIL）。v2 Primary smoke 样本在自测中应 **PASS** 资格门。

## 自测

```bash
python3 validate_lane.py --self-test
```

使用冻结样本：`manifests/samples/workload-expected-operations.smoke.sample.jsonl`（28 行 DML）、`workload-expected-final-state.smoke.sample.json`、`workload-oracle-pin.sample.json`（`short_tx_n=10`、tombstone_*_before）。

Python 3 标准库 only。

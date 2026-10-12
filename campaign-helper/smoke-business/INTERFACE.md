# Smoke business entry（bounded，无 provenance lane）

**Integration status: INTEGRATION HOLD**（Astra finite review 结论保留于 `evidence/astra-finite-review-results.json`）。本 helper 不替代 MAIN 对 continuous baseline 的独立审计证据；`acceptance_dimensions` 恒 UNVERIFIED / UNKNOWN / NOT_ACCEPTED。

独立 CLI：`smoke_business.py`。不调用 `validate_lane`，不要求 per-op Oracle commit/SRL primary 文件。

## 退出码

| rc | outcome | 含义 |
| --- | --- | --- |
| 0 | `SMOKE_BUSINESS_PASS` | 有界 smoke 业务 + capture + 运行时门通过 |
| 1 | `SMOKE_BUSINESS_FAIL` | 业务 / Flink / capture multiset 失败 |
| 2 | `SMOKE_BUSINESS_INPUT_INCOMPLETE` | 缺输入、slice 非法、资格或 receipt/window 门未过 |
| 3 | USAGE | 参数错误 |

报告内 `acceptance_dimensions` 恒为：`independent_source_qualification=UNVERIFIED`，`prearchive_status=UNKNOWN`，`campaign_status=NOT_ACCEPTED`。不因 JSON 标签宣称 campaign accepted。

## CLI

```bash
python3 smoke_business.py \
  --validator-root /path/to/campaign-helper/validators \
  --lane baseline \
  --kafka state/baseline/capture/kafka-raw.jsonl \
  --expected-events state/workload-expected-operations.jsonl \
  --expected-final-state state/workload-expected-final-state.json \
  --oracle-pin state/workload-oracle-pin.json \
  --flink-sink state/baseline/flink/sink-rows.jsonl \
  --capture-manifest state/baseline/capture/manifest.json \
  --execution-receipts state/workload-execution-receipts.jsonl \
  --measurement-window state/MEASUREMENT-WINDOW.json \
  --qualification-evidence state/ORACLE-QUALIFICATION-EVIDENCE.json \
  --smoke-slice state/SMOKE-BUSINESS-SLICE.json \
  --pre-workload-capture-manifest state/baseline/capture/pre-workload-manifest.json
```

`--validator-root` 仅追加 `sys.path`，不修改 v73 冻结树。

## `--smoke-slice`（MAIN 提供，verify-only）

`contract_version`: `20261012-smoke-slice-v2`（v1 拒绝）

- `pre_workload_cut_ref.measurement_window_sha256`（校验 MW 文件）+ `pre_workload_capture_manifest_sha256`（校验 **原始** full capture manifest 文件）+ `partition_lower_bounds[]`：各分区 inclusive begin 须等于该 manifest 对应 `partition.end`（assigned/end cut）；不得仅信 slice 声明或从 expected 行推断。
- `recovered_capture_binding.capture_manifest_sha256` + `partitions[].end_offset_exclusive`：上界须等于 recovered manifest 的 exclusive end（不可单独缩小 slice.end 藏 EXTRA）。
- `smoke_measurement_bounds[]`：须与上述绑定一致；offset 字段为 strict int（非 bool）。
- 可选 `p099_prep`：`identified_coords[]`（含 `prep_instance` rac1|rac2）。每个 instance 至多排除 **一条** 坐标，且须有对应 **成功** `W099a`/`W099b` `sql_commit`（`sql_exit=0`）+ pin 全字段 INSERT 身份；`P099 phase_boundary` **不能**单独授权排除。超额相同 prep 行留在 smoke 窗口内 → `EXTRA_OPS`。缺 source op → `P099_PREP_SOURCE_OP_UNPROVEN`（INPUT）。Recovered replay **不** retroactive 补造 P099 live catch-up 时间窗。

## 真实 smoke 065738（MAIN 路径示例）

基准键 `BASE_KEY3333984`；期望 committed **EV 21 + PP 4 = 25**，forbidden **3**；最终态 **EV 11 + PP 1**，PP `CTR=680`。

状态目录（由 MAIN 维护，本 worktree 不写入）：

`/home/shihao/Projects/oracle-cdc-benchmark/evidence/attempts/20261012T065738Z-campaign-continuous/active-state/state`

缺任一 artifact 时报告 `INPUT_INCOMPLETE` 与缺失路径，不生成 synthetic primary。

## 自测

```bash
python3 smoke_business.py --self-test
```

证据：`evidence/smoke-business-regressions-last.json`（14 case，同一 `validate_smoke_business` 入口）。

## 冻结清单

`SHA256SUMS`：`INTERFACE.md`、`smoke_business.py`、`smoke_business_regressions.py`、`smoke_selection.py`、`smoke_slice_guards.py`（`20261012-smoke-slice-v2` + P099/cut 绑定守卫）。

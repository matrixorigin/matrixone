# Catalog Metadata Capability Barrier：Wire 与 Snapshot Contract

- **状态**：Draft，等待 design-first approval
- **Owning issue**：#29003（parent #26227）
- **实现系列**：#29003 → #29004 → #29005 → #29006 → #29007
- **基线**：`mo/main@8a4c84f4516d5098de9eef50b8afecc7524f37a2`
- **风险等级**：R3；wire、HAKeeper persistent state、mixed-version、restart/rollback

## 1. 问题与当前合同

main 已有 persisted-expression admission protocol：

- CN heartbeat 通过 `PersistedExpressionProtocolVersion` 广告本地 decoder/authoring capability；
- HAKeeper RSM 持久化 `PersistedExpressionRequiredProtocolVersion` 与 activation pending；
- LogStore 通过 `ViewMetadataAdmissionProtocolV3Supported` 证明可回放 activation entry 和 MOH2 snapshot；
- snapshot 在 protocol floor 为零时使用 legacy raw `HAKeeperRSMState`，floor 非零时使用 `MOH2 + raw HAKeeperRSMState`；
- MOH2 decode 要求 floor 非零，并在完整 decode 成功后原子替换 state。

#26227 还需要一个不同的合同：dependency DDL 或 restore 使 View 持久化 metadata 失效后，必须 seal 新 metadata readers、发布 durable recovery generation、等待 catalog recovery 完成，再开放 public metadata。

Persisted-expression floor、membership epoch 和 catalog recovery generation 相关但不等价。继续复用一个布尔值或重定义现有字段会使旧 binary 将未知状态误判为 terminal。

## 2. Invariant、negation 与 owner

### 2.1 Invariant

对任意 lifecycle-sensitive metadata response，存在 generation `G`，同时满足：

1. HAKeeper admission owner 已授权该 CN generation 参与 `G`；
2. durable catalog 的 `completed_generation >= G`；
3. response 使用的 metadata materialization generation 不小于 `G`；
4. packet/flush/COMMIT terminal validation 时上述 authority 仍有效。

#29003 只定义表达该合同所需的 wire/persistence 格式，不激活 invariant 的运行时 enforcement。

### 2.2 最小 negation

新 binary 写入一个旧 binary 可解码但会按旧含义解释的字段号；旧 HAKeeper/LogStore 随后把未完成的 catalog generation 当作 admission terminal，并允许 stale metadata reader。

### 2.3 第一 owner

| 状态 | 第一 owner | 本 issue 的 producer |
|---|---|---|
| persisted-expression floor | 现有 HAKeeper RSM | 保持不变 |
| service capability payload | 发送 heartbeat 的进程 | 新字段保持零值，不发布 capability |
| catalog barrier phase/epoch | 后续 HAKeeper RSM（#29004） | 本 issue 始终 `DISABLED/0` |
| required/completed generation | 后续 catalog recovery（#29005） | 本 issue 始终 `0` |
| snapshot format/version | HAKeeper snapshot writer | 本 issue 实现 format decode/encode |

## 3. 目标与非目标

### 3.1 目标

- additive、unknown-field tolerant 的 capability/barrier schema；
- 保留所有 main 现有字段号和含义；
- 明确区分 protocol floor、membership epoch、catalog generation；
- 支持 legacy raw、MOH2 和新 versioned envelope；
- decode error 不部分覆盖 live/reused RSM state；
- 为后续 PR 提供稳定合同，而不提前激活任何行为。

### 3.2 非目标

- 不推进 epoch，不 seal reader；
- 不写 catalog marker，不启动 worker；
- 不改变 frontend/planner/DDL/restore；
- 不迁移或删除现有 persisted-expression scalar fields；
- 不承诺本 issue 合入后 View metadata 已刷新。

## 4. 选择的 wire contract

### 4.1 Capability

新增：

```protobuf
message CatalogMetadataCapabilities {
  uint64 persisted_expression_protocol = 1;
  uint64 view_dependency_protocol = 2;
  uint64 recovery_protocol = 3;
}
```

字段分配：

- `CNStoreHeartbeat.catalog_metadata_capabilities = 26`
- `CNStoreInfo.catalog_metadata_capabilities = 28`
- `LogStoreHeartbeat.catalog_metadata_capabilities = 14`
- `LogStoreInfo.catalog_metadata_capabilities = 14`

现有 `PersistedExpressionProtocolVersion` 和 `ViewMetadataAdmissionProtocolV3Supported` 保持 canonical，不改 tag、不改含义。后续迁移期内 nested persisted-expression 值只可镜像现有 scalar；两者同时非零但不一致时必须 fail closed，不能取 max。#29003 不设置 nested 值，因此运行时完全维持当前行为。

### 4.2 Barrier snapshot

新增：

```protobuf
enum CatalogMetadataBarrierPhase {
  CATALOG_METADATA_BARRIER_DISABLED = 0;
  CATALOG_METADATA_BARRIER_PREPARING = 1;
  CATALOG_METADATA_BARRIER_SEALED = 2;
  CATALOG_METADATA_BARRIER_CATALOG_REQUIRED = 3;
  CATALOG_METADATA_BARRIER_RECOVERING = 4;
  CATALOG_METADATA_BARRIER_ACTIVATED = 5;
}

message CatalogMetadataBarrierState {
  CatalogMetadataBarrierPhase phase = 1;
  uint64 membership_epoch = 2;
  uint64 required_generation = 3;
  uint64 completed_generation = 4;
  uint64 required_view_dependency_protocol = 5;
  uint64 required_recovery_protocol = 6;
}
```

`HAKeeperRSMState.catalog_metadata_barrier = 45`。main 当前 43–44 已属于 persisted-expression，不能复用。缺失 field 45 等价于完整 `DISABLED/0`，仅因为本 issue 尚未激活 producer；后续一旦 barrier 曾进入非 DISABLED，降级/缺失 capability 必须由 #29004 明确 fail closed，不能借零值重新开放。

新增 heartbeat snapshot：

```protobuf
message CatalogMetadataBarrier {
  CatalogMetadataBarrierPhase phase = 1;
  uint64 membership_epoch = 2;
  uint64 required_generation = 3;
  uint64 completed_generation = 4;
  bool admitted = 5;
  bool metadata_reads_enabled = 6;
}
```

字段分配：

- `CommandBatch.catalog_metadata_barrier = 6`
- 后续需要 global readback 时新增独立字段，不重定义 `ViewMetadataAdmission`。

#29003 的 producer 始终发送 protobuf 零值/省略字段。

## 5. Snapshot envelope

### 5.1 格式

保留既有 decoder，并新增 magic `MOH3`：

```protobuf
message HAKeeperSnapshotEnvelope {
  uint32 format_version = 1;       // 首版固定为 1
  bytes rsm_state = 2;             // marshaled HAKeeperRSMState
  uint64 required_features = 3;    // decoder feature bitmap
}
```

首版 feature bit：

- bit 0：persisted-expression floor present；
- bit 1：catalog barrier state present and non-disabled。

当前 #29003 在 barrier disabled 时仍可写 MOH3，以证明新 envelope；`required_features` 只按实际 durable state 设置。是否持续写 MOH2 不是 correctness requirement，但 rollout 默认先让新 binary 同时读三种格式，再在 design-approved checkpoint 后切换 writer。

### 5.2 Decode 顺序

1. 无 magic：按 legacy raw `HAKeeperRSMState` decode；
2. `MOH2`：剥离 magic，按当前规则 decode，并要求 persisted-expression floor 非零；
3. `MOH3`：decode envelope，验证 `format_version == 1`、required feature bits 可识别、payload 非空，再 decode payload；
4. 未知 magic/version/required feature：返回 typed invalid-input error；
5. 所有验证在临时 state 上完成；成功前不得写 `s.state`；
6. 成功后规范化 legacy omitted maps，再单次替换 live state。

### 5.3 Writer 规则

- barrier disabled 且 persisted-expression floor 为零：允许继续写 legacy raw，降低无意义 churn；
- barrier disabled 且 floor 非零：继续写 MOH2；
- barrier state 非 disabled 或任一 barrier generation 非零：必须写 MOH3，并设置 bit 1；
- 不允许 barrier durable state 降级写入 MOH2/raw。

本 issue 为验证 MOH3 writer 提供显式 package seam；默认运行时由于 barrier 始终 disabled，仍生成现有 raw/MOH2 格式，不改变 rollout。

## 6. 状态一致性规则

必须拒绝：

- `completed_generation > required_generation`；
- phase 为 DISABLED 但任一 barrier generation 非零；
- phase 为 ACTIVATED 但 required/completed 不相等；
- required feature bit 声明 barrier，但 payload 不含有效 barrier state；
- barrier 非 disabled 但 required feature bit 未设置；
- nested/scalar persisted-expression capability 同时非零但不一致（后续 producer 启用时）。

#29003 decoder 只验证持久格式内部一致性，不执行 membership 或 catalog terminal decision。

## 7. Compatibility matrix

| Writer | Reader | 结果 |
|---|---|---|
| old raw | new | decode；barrier disabled |
| MOH2 | new | decode；保留 persisted-expression floor；barrier disabled |
| MOH3/barrier disabled | new | decode |
| MOH3 | old | 不可读，因此 writer 只在 barrier activation 前置条件证明所有 HAKeeper replicas 支持后启用 |
| new heartbeat | old | unknown fields ignored；现有 scalar 仍 canonical |
| old heartbeat | new | nested capability 缺失；在 #29003 中无行为变化，后续 activation fail closed |
| corrupt/truncated any format | new | error；live state 不变 |

Downgrade 规则：只要 durable barrier 曾非 disabled，旧 HAKeeper 不得成为 replica；由 #29004 的 LogStore capability/membership fence 实现。#29003 仅提供表示该 requirement 的 schema 和测试 fixture。

## 8. Failure containment 与资源预算

- snapshot decode 为 O(snapshot bytes)，只分配一个 input buffer、一个 envelope 和一个 decoded RSM；峰值不超过当前 snapshot payload 的约 2 倍加固定 envelope；
- 不增加 goroutine、timer、queue、retry 或 background I/O；
- heartbeat 增量为固定三个 varint capability 字段，零值时不编码；
- decode 失败保持现有 state，不触发服务关闭或 admission 变化；
- 不新增 metric cardinality或日志循环。

## 9. 安全与租户边界

新字段只承载 cluster-level protocol/generation，不包含 tenant 数据、SQL 文本或 catalog payload。发送 authority 与现有 heartbeat 相同。未知/不一致 capability 只允许 fail closed，不能提升权限或开放 metadata。

## 10. 替代方案

### A. 继续扩展 `ViewMetadataAdmission`

拒绝。它已同时承载 admission 与 persisted-expression floor，再加入 catalog recovery generation 会混淆 terminal predicate，并提高字段号冲突和旧 binary 误判风险。

### B. 为 View lifecycle 建立完全独立的第二套 heartbeat/RSM

拒绝。会复制 membership generation、same-UUID replacement、LogStore decoder gate 和 snapshot recovery owner，形成两个可能漂移的 membership authority。

### C. 通用 barrier + 独立状态维度（选择）

复用唯一 membership authority，但 protocol floor、membership epoch、catalog generation 使用不同字段和 terminal predicate；复杂度最低且可独立验证。

### D. 不加 envelope，只向 raw RSM 追加字段

拒绝。unknown fields 解决结构兼容，但不能表达“旧 decoder 不得恢复此 durable state”的 required-feature contract，也不能稳定区分 corrupt payload 与不支持的格式。

## 11. 验证映射

| 合同 | 最小证明 |
|---|---|
| tag additive/no reuse | descriptor/tag UT + generated diff review |
| old heartbeat/new decoder | byte fixture unmarshal |
| new heartbeat/old decoder | old-shape fixture忽略 unknown fields |
| legacy raw recovery | existing fixture + explicit barrier-disabled assertion |
| MOH2 recovery | existing floor fixture + barrier-disabled assertion |
| MOH3 recovery | byte-level envelope fixture |
| reused state reset | destination 预置非零 barrier，恢复旧 snapshot 后必须 disabled/zero |
| corrupt/truncated atomicity | 每种格式失败后 destination state 完全不变 |
| unknown version/features | typed rejection |
| writer format selection | raw/MOH2/MOH3 table test |
| generated consumer closure | HAKeeper、LogStore、CN focused + full owning-package tests |

无 BVT：#29003 不改变 SQL/public admission，BVT 无法提供比 byte-level protocol/snapshot UT 更强的 oracle。Public SQL proof 属于 #29007。

## 12. Rollout、observability 与 removal

1. #29003 合入 schema/decoder，producer 保持零值，writer 默认保持现有 raw/MOH2；
2. #29004 在所有 HAKeeper/LogStore capability 已证明后启用 barrier/MOH3；
3. #29005–#29006 实现 catalog recovery 与 metadata authority；
4. #29007 才激活 public behavior。

诊断时必须能区分 snapshot format、required feature bits、barrier phase 和 generation；不记录 tenant payload。旧 raw/MOH2 decoder 在至少一个完整升级窗口内保留。删除前需要证明不再存在旧 snapshot/backup/rollback reader。

## 13. Decision log 与开放项

已决定：

1. 保留现有 persisted-expression fields canonical，不重编号、不重定义；
2. 新 barrier 使用独立 message 和 RSM tag 45；
3. MOH3 使用显式 version/features/payload envelope；
4. 默认 runtime behavior 不变；
5. 后续 PR 不得在未更新并重审本文的情况下改变 tags、terminal predicate 或 downgrade contract。

阻塞审批的问题：

1. 是否接受 `MOH3` magic + protobuf envelope，还是统一把现有 MOH2 也迁移为 envelope？本文选择前者以保持 byte compatibility。
2. 是否接受 nested capability 在迁移期镜像现有 scalar，并以不一致 fail-closed？本文选择接受，不在 #29003 启用 producer。
3. RSM tag 45 与 heartbeat tags 26/28/14、CommandBatch tag 6 需由 reviewer 确认未与并行工作保留范围冲突。

上述问题必须在 design approval 中关闭，之后才开始 production schema/snapshot 实现。

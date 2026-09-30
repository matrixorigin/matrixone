# Catalog Metadata Capability Barrier：Wire 与 Snapshot Contract

- **状态**：Approved；[XuPeng-SH review 5221796725](https://github.com/matrixorigin/matrixone/pull/29008#pullrequestreview-5221796725)，批准 revision `5be1ad8802575c558743048443dbf4f7b5a50076`
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
  uint64 hakeeper_barrier_protocol = 4;
}
```

字段分配：

- `CNStoreHeartbeat.catalog_metadata_capabilities = 26`
- `CNStoreInfo.catalog_metadata_capabilities = 28`
- `LogStoreHeartbeat.catalog_metadata_capabilities = 14`
- `LogStoreInfo.catalog_metadata_capabilities = 14`

现有 `PersistedExpressionProtocolVersion` 和 `ViewMetadataAdmissionProtocolV3Supported` 保持 canonical，不改 tag、不改含义。后续迁移期内 nested persisted-expression 值只可镜像现有 scalar；两者同时非零但不一致时必须 fail closed，不能取 max。#29003 不设置 nested 值，因此运行时完全维持当前行为。

#### MOH3 decoder 与 barrier-entry replay 门禁

`LogStoreHeartbeat.catalog_metadata_capabilities.hakeeper_barrier_protocol >= 1` 是唯一新增的 decoder/replay capability predicate；由接收方复制到 `LogStoreInfo`。版本 1 同时承诺：能够读取 MOH3 envelope v1、识别本设计的 required feature bits、验证 barrier state，并回放 #29004 定义的 barrier transitions。只实现 decoder 的 #29003 binary 不得广告版本 1。

此 capability 不由 `ViewMetadataAdmissionProtocolV3Supported`、CN expression version 或 View recovery version 推导；现有 V3 仅证明既有 persisted-expression entry/MOH2 能力，不能授权 MOH3。字段缺失或值为 0 均表示不支持。未来高版本必须保留版本 1 的 decoder/replay 合同，否则必须使用新的 capability，而不能仅提高数值。

首次 barrier entry 提交前，leader 必须证明当前 HAKeeper membership 的所有 replica（含 non-voting）所在 LogStore 均以当前 incarnation 广告版本 >= 1，且无未完成 membership admission。未知或不可达 replica 不计作支持；普通 heartbeat 超时不能替代 Raft membership removal。#29004 必须在 membership 变更入口重复同一 gate，防止检查后加入旧 replica。首次 PREPARING entry 的 durable commit 才允许 MOH3 writer；此后旧 LogStore 不得加入或替换 replica。旧 RSM 必须拒绝新的独立 entry tag，而不能按旧 admission entry 忽略 payload。

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

### 4.3 规范状态转换（#29004–#29007 实现，#29003 固化格式）

记 `E=membership_epoch`、`R=required_generation`、`C=completed_generation`。这些是不同计数域，不能相互比较或从一个推导另一个。非 DISABLED 状态要求 `E>0`、`R>0` 且两项 required protocol 非零。除 ACTIVATED 外要求 `C<R`；ACTIVATED 要求 `C=R`。所有状态变化由 replicated entry 提交，catalog completion 是需要验证的输入而非 CN 单方面授权。

| 当前状态 | 事件/前置条件 | 提交后的状态与动作 |
|---|---|---|
| DISABLED | 初始化、旧 snapshot 恢复 | 全字段为零；不产生新协议行为 |
| DISABLED | 全 replica capability gate 成功，提交首次 barrier entry | PREPARING；分配非零 E，R=1，C=0；从此只写 MOH3 |
| PREPARING | 捕获的 CN/Proxy generation capability 满足目标 | SEALED；发布 E 的 seal 要求 |
| SEALED | 捕获的旧 reader authority 已 drain/expire，catalog owner 成功提交 R 的 required marker | CATALOG_REQUIRED；保持 C<R；重复 marker 请求必须幂等 |
| CATALOG_REQUIRED | catalog owner 成功取得 generation-scoped claim | RECOVERING；不推进 C |
| RECOVERING | durable catalog completion=R，当前 E 的成员/fence 条件均满足 | ACTIVATED；C=R；允许发布 metadata authority |
| ACTIVATED | 新 invalidation 或 membership 变化要求 revalidation | PREPARING；分配新 E，R=R+1，保留 C；停止签发旧 authority，后续仍须 drain |
| 任意非 DISABLED | 新 generation 取代未完成任务 | PREPARING；增加 E、R，保留 C；旧 claim/completion 不得推进新 R |
| 任意非 DISABLED | 请求取消、超时、重试、worker crash | 不退回 DISABLED、不降低 E/R/C；保持当前 phase，重试同一 entry/claim，或由显式新 generation 取代 |
| 任意状态 | 成功恢复合法 snapshot | 原子采用 snapshot 状态；随后按 committed log 顺序回放，不从内存残留推断状态 |

整数耗尽必须拒绝新 transition，不得 wrap-around。重复 entry 不分配新的 generation。失败 decode 不修改任何 live state。

**Ever-non-disabled durable rule**：首次 PREPARING 后禁止任何 replicated transition 返回 DISABLED；非零 R 与非 DISABLED phase 即持久历史标志，无须额外布尔副本。Snapshot 必须保留该状态并使用 MOH3，即使当前无 recovery 工作也不能降级。恢复旧 raw/MOH2 snapshot 的 reused-instance 测试只证明 authoritative replacement；存储层必须随后回放相应已提交 entry，不能把人为选择旧 backup 当作协议降级。激活后的恢复流程若无法恢复 barrier entry/history，不允许重新开放服务，应失败并要求恢复有效备份/日志。这不是依靠保留旧进程内存来实现的保护。

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

#29003 仅在显式测试 seam 中构造 barrier-disabled MOH3 fixture。生产 writer 必须遵循 §5.3，保持当前 raw/MOH2 行为；首次 durable barrier entry 提交前禁止生产输出 MOH3。`required_features` 必须精确反映 payload 的 durable state。

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
- 先按 §6 校验 state；非法状态直接拒绝写入，不通过切换格式修复；
- 合法非 DISABLED barrier state：必须写 MOH3，并设置 bit 1；bit 0 当且仅当 persisted-expression floor 非零；
- 不允许 barrier durable state 降级写入 MOH2/raw。

本 issue 为验证 MOH3 writer 提供显式 package seam；默认运行时由于 barrier 始终 disabled，仍生成现有 raw/MOH2 格式，不改变 rollout。

## 6. 状态一致性规则

Writer 和 decoder 必须共享以下格式验证规则；protobuf enum 能 decode 不代表语义合法：

- phase 仅允许数值 0–5；任何其他值（包括 99、负值）必须拒绝，即使 generations 全零；
- DISABLED 要求 E/R/C、required_view_dependency_protocol、required_recovery_protocol 全部为零；省略 barrier message 与此零值状态等价；
- 非 DISABLED 要求 E、R、两项 required protocol 均非零；PREPARING/SEALED/CATALOG_REQUIRED/RECOVERING 要求 C<R，ACTIVATED 要求 C=R；
- `completed_generation > required_generation` 在任何 phase 均非法；
- MOH3 feature bit 0 **当且仅当** payload 的 `PersistedExpressionRequiredProtocolVersion > 0`；bit=1/floor=0 和 bit=0/floor>0 都拒绝；
- MOH3 feature bit 1 **当且仅当** payload 含合法非 DISABLED barrier；两种不一致方向都拒绝；
- MOH3 不允许 bits 0、1 之外的 required feature；任意未知 bit 都拒绝；
- raw/MOH2 不得携带非 DISABLED barrier；MOH2 仍要求 floor 非零；不把新 MOH3 bit 校验反向套到历史 raw 格式；
- nested/scalar persisted-expression capability 同时非零但不一致（后续 producer 启用时）必须拒绝。

Negative fixtures 必须独立覆盖：phase=99 且零 generation、phase=-1、每个 DISABLED 非零字段、每个非 DISABLED 缺失字段、各 phase 的 C/R 边界，以及 bits 0/1 与 floor/barrier 的完整真假组合。每个失败 fixture 都检查 reused destination 完全不变；合法 raw/MOH2/MOH3 fixture 是对应 positive control。

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

- 不承诺 serialized payload 的固定 2x 峰值。记 S 为输入序列化长度、P 为 envelope 中 rsm_state 长度、D 为新 decoded RSM 的 maps/strings/slices 实际 heap、L 为旧 live state heap。原子恢复期间两份 state 必须共存；live memory 模型为 `L + cap(input) + cap(envelope.rsm_state) + D + envelope/decoder 临时开销`。generated bytes decoder 若复制 payload，P 会额外常驻；input 增长和 GC 尚未回收的旧 buffer 还会提高分配峰值，不能用 S 推导 D 的固定系数；
- snapshot reader 拥有 input buffer；临时 envelope 拥有其复制的 payload/unknown bytes；临时 decoded state 拥有新 maps/strings。只有验证完成才转移 decoded state 给 RSM，函数返回后不得缓存 input/envelope，也不得让新 state 引用可复用 input。失败时临时对象全部失去引用，旧 live state 不变；GC 回收不是同步内存释放承诺；
- 时间成本按输入扫描和 decoded 元素计为 O(S + 元素数)，不新增额外遍历副本或后台保留。实现必须采用有长度检查的 framing，不能按未验证的 envelope length 直接分配；本 issue 不新增未经容量评估的硬性 snapshot 大小限制，现有 raw reader 的大输入风险不宣称已解决；
- 实现验收需记录 raw/MOH2/MOH3 在相同 RSM payload 下的 `allocs/op`、`B/op`，并分别测 fresh/reused destination、map/string-heavy payload 和截断失败。使用小规模边界 UT 证明 ownership/atomicity，独立 benchmark/heap profile 测容量；报告 S/P/D/L 与额外 payload copy，禁止将 B/op 误报为峰值 RSS。性能证据未完成前不得声称存在固定倍数 memory bound；
- 不增加 goroutine、timer、queue、retry 或 background I/O；
- capability 最多包含四个 uint64 varint，scalar 编码最多 44 bytes，另加外层 message tag/length；nil nested message 不编码，生产 producer 在本阶段保持 nil；
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
| unknown version/features/phase | typed rejection；phase=99/-1、每个未知 feature bit |
| feature/payload 双向一致性 | bit 0 × floor、bit 1 × barrier 完整真假矩阵 |
| MOH3 capability gate | 缺失/0/V3-only/支持新协议，以及 voting/non-voting 与 incarnation 替换；#29004 执行 transition 测试 |
| generation/phase transitions | §4.3 每条边、duplicate、abort/retry、overflow 和禁止回退；#29003 格式 UT，#29004 状态机 UT |
| snapshot allocation/ownership | raw/MOH2/MOH3 fresh/reused benchmark + heap profile；失败无保留引用 |
| writer format selection | raw/MOH2/MOH3 table test |
| generated consumer closure | HAKeeper、LogStore、CN focused + full owning-package tests |

无 BVT：#29003 不改变 SQL/public admission，BVT 无法提供比 byte-level protocol/snapshot UT 更强的 oracle。Public SQL proof 属于 #29007。

## 12. Rollout、observability 与 removal

1. #29003 合入 schema/decoder，producer 保持零值，writer 默认保持现有 raw/MOH2；
2. #29004 在所有 HAKeeper/LogStore capability 已证明后启用 barrier/MOH3；
3. #29005–#29006 实现 catalog recovery 与 metadata authority；
4. #29007 才激活 public behavior。

诊断时必须能区分 snapshot format、required feature bits、barrier phase 和 generation；不记录 tenant payload。旧 raw/MOH2 decoder 在至少一个完整升级窗口内保留。删除前需要证明不再存在旧 snapshot/backup/rollback reader。

## 13. Decision log 与审批结论

已决定：

1. 保留现有 persisted-expression fields canonical，不重编号、不重定义；
2. 新 barrier 使用独立 message 和 RSM tag 45；
3. MOH3 使用显式 version/features/payload envelope；
4. 默认 runtime behavior 不变；
5. 后续 PR 不得在未更新并重审本文的情况下改变 tags、terminal predicate 或 downgrade contract。

审批已关闭以下问题：

1. 接受 `MOH3` magic + protobuf envelope，不迁移既有 MOH2，以保持 byte compatibility。
2. 接受 nested capability 在迁移期镜像现有 scalar，并以不一致 fail-closed；#29003 不启用 producer。
3. 确认 RSM tag 45、heartbeat tags 26/28/14 与 CommandBatch tag 6 在审批基线未被占用。

实现若改变上述结论，必须更新设计并重新获得审批。

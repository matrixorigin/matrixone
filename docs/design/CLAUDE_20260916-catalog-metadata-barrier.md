# Catalog Metadata Capability Barrier：Wire 与 Snapshot Contract

- **状态**：以下 §1–13 为 #29003 冻结批准范围；末尾 #29004 仲裁修订仍为 DRAFT、待 reviewer 接受。原批准：[XuPeng-SH review 5221796725](https://github.com/matrixorigin/matrixone/pull/29008#pullrequestreview-5221796725)，批准 revision `5be1ad8802575c558743048443dbf4f7b5a50076`
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

\#26227 还需要一个不同的合同：dependency DDL 或 restore 使 View 持久化 metadata 失效后，必须 seal 新 metadata readers、发布 durable recovery generation、等待 catalog recovery 完成，再开放 public metadata。

Persisted-expression floor、membership epoch 和 catalog recovery generation 相关但不等价。继续复用一个布尔值或重定义现有字段会使旧 binary 将未知状态误判为 terminal。

## 2. Invariant、negation 与 owner

### 2.1 Invariant

对任意 lifecycle-sensitive metadata response，存在 generation `G`，同时满足：

1. HAKeeper admission owner 已授权该 CN generation 参与 `G`；
2. durable catalog 的 `completed_generation >= G`；
3. response 使用的 metadata materialization generation 不小于 `G`；
4. packet/flush/COMMIT terminal validation 时上述 authority 仍有效。

\#29003 只定义表达该合同所需的 wire/persistence 格式，不激活 invariant 的运行时 enforcement。

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

\#29003 的 producer 始终发送 protobuf 零值/省略字段。

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

\#29003 仅在显式测试 seam 中构造 barrier-disabled MOH3 fixture。生产 writer 必须遵循 §5.3，保持当前 raw/MOH2 行为；首次 durable barrier entry 提交前禁止生产输出 MOH3。`required_features` 必须精确反映 payload 的 durable state。

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

\#29003 decoder 只验证持久格式内部一致性，不执行 membership 或 catalog terminal decision。

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

#### A. 继续扩展 `ViewMetadataAdmission`

拒绝。它已同时承载 admission 与 persisted-expression floor，再加入 catalog recovery generation 会混淆 terminal predicate，并提高字段号冲突和旧 binary 误判风险。

#### B. 为 View lifecycle 建立完全独立的第二套 heartbeat/RSM

拒绝。会复制 membership generation、same-UUID replacement、LogStore decoder gate 和 snapshot recovery owner，形成两个可能漂移的 membership authority。

#### C. 通用 barrier + 独立状态维度（选择）

复用唯一 membership authority，但 protocol floor、membership epoch、catalog generation 使用不同字段和 terminal predicate；复杂度最低且可独立验证。

#### D. 不加 envelope，只向 raw RSM 追加字段

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

## 14. #29004 仲裁、证据与恢复修订（DRAFT）

本节回应 review 5232882733（基于 `d6ac371788`），仍待 reviewer 接受。
§1–13 的 #29003 wire/snapshot 批准不自动覆盖本节的运行时协议、维护切换、
独立 decoder floor 和显式退休。对于 #29004，本节取代早期候选选择；旧
\#29003 raw/MOH2/MOH3 格式仍按其版本恢复，不能据旧 bit 0/1 规则忽略新 bit 2。
本节不是实现完成或运行时安全证明；`MetadataReadsEnabled` 仍须 false。
[原修订及其审查上下文](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/CLAUDE_20260917-catalog-barrier-review-resolution.md)
保留在不可变历史中。

### A. 成员仲裁：能力检查不能代替升级栅栏

#### A.1 两个不同的安全问题

1. 升级前已经投递的旧命令：不携带 reservation，新 RSM 无法证明其已失效。
2. 全部 executor 升级后的新命令：可以强制使用 replicated reservation。

不能用第二项的解决方案宣称解决了第一项。当前没有足够证据证明现有系统支持无停机切换到新仲裁。

**本次选择的保守合同**：允许滚动部署新 binary，但首次启用新 entry/reservation 协议需要一次受控维护切换。混合版本自动激活不支持。这个约束改变了原计划的上线能力，必须由设计 reviewer 明确接受；若 #29004 必须提供无停机首次激活，则此方案不能通过，不能以文档修改冒充完成。

维护切换步骤：

1. 外部部署 owner 关闭 scheduler/人工 membership mutation，停止所有可能执行 HAKeeper add/start 的旧进程，禁止旧 binary 重启。仅失去心跳不算停止证明。
2. 停止所有 HAKeeper replica进程，随后只用新 binary 恢复现有 membership；不加入新成员。旧进程内存中的 response/command 不得恢复为可执行 work。
3. 等待 Raft 完成恢复并取得线性一致 membership/config-change-index。崩溃前成功提交的 config entry会反映在恢复结果中；未提交的操作仍由 Raft日志处理，不能通过缓存 checker state推测结果。
4. 所有重启后的 executor默认拒绝无 reservation 的 HAKeeper add/start，包括旧 durable schedule队列恢复出的命令。调度 owner基于恢复后的 authoritative membership重新生成命令，而不是重新授权旧 epoch命令。
5. 在外部旧 binary启动禁令仍生效时，检查全部 voting/non-voting replica的当前 incarnation capability，启用新的仲裁模式。此后才能提交首次新 barrier entry。任一步失败保持维护状态，不回退到无reservation执行模式。

这里的进程停止证明及启动禁令属于部署控制面的信任前提，不是 SQL/RSM boolean。当前仓库尚无该维护流程实现，因此不能在 #29004 中悄悄添加自动 Begin 开关。

#### A.2 升级后共同线性化点

HAKeeper RSM持有唯一 `MembershipArbitration` owner。状态至少包括 `Mode`、`NextOperationID`、单个 `Reservation` 和 `BarrierSubmissionFence`。**Reservation只用于Raft membership mutation（add/remove voting或non-voting），不用于本地StartReplica。** reservation绑定 operation ID、当前 config-change-index、完整目标 membership摘要、目标UUID/replica ID/incarnation、action。本地start使用A.4的独立permit。

- `ReserveAdmission` 与 `FenceBarrierSubmission` 是同一 RSM上的互斥转移。后者只在无 reservation时成功；前者在 fence存在时拒绝。这两个 entry 的 apply顺序是共同线性化点。
- fence提交后重新做线性一致 membership读取和 incarnation能力检查。检查通过才提交 Begin；失败保留decoder floor，显式释放submission fence，但不撤销已经持久化的protocol floor。
- executor调用Dragonboat add/remove前必须获得匹配reservation；本地start前必须获得A.4的permit。无对应token的调用拒绝；普通数据shard不受此新合同影响。
- reservation存在期间不得推进首次Begin，也不得授权另一个membership mutation，包括remove操作，避免结果归属歧义。
- token仅允许一个指定的mutation及expected config-change-index。重试不分配新token，不刷新expected index；旧token不能授权新目标。
- capability只证明指定incarnation；replacement需要独立reservation。外部进程启动门禁必须继续禁止低于durable decoder floor的binary使用既有replica身份启动。

#### A.3 Membership mutation的未知结果与恢复

本节仅适用于改变membership的add/remove，不适用于StartReplica。RPC success、timeout或executor ack都不直接删除reservation。reconciler从线性一致Raft membership取得结果：

| 观察 | 行为 |
|---|---|
| index已推进且目标membership符合预期 | 标记APPLIED；旧index命令不能再次改变membership；释放reservation |
| index已推进但membership不符 | 标记CONFLICT，封闭调度，要求重新核对；不得自动改写token |
| index未推进 | 结果可能尚未提交；保留reservation，允许相同token重试 |
| 无法读取/leader变化 | 保留reservation，重试只做读取或相同token操作 |
| executor崩溃 | 新executor接管同一durable token；不能因旧进程失联清理 |
| 请求取消/超时 | 只结束调用者等待，不撤销Raft操作、不释放reservation |

不提供未经证明的自动abort。无法确认结果时允许阻塞membership/首次activation，普通SQL不受影响。活性依赖quorum恢复及目标操作最终得到确定结果；运维不能靠清空map解除阻塞。确需取消未决操作时，必须用维护切换停掉所有executor后核对Raft结果，不能实现TTL清理。

#### A.4 Local start：独立permit，不等待membership index推进

StartReplica只启动已经admitted的本地replica，不是membership mutation。RSM核对authoritative membership确实包含该UUID/replica ID后签发 `StartPermit(PermitID, UUID, ReplicaID, StoreIncarnation, DecoderFloor)`。Bootstrap初始建群不通过这个已admitted成员入口；首次协议启用必须在bootstrap完成后。

- permit每个当前member最多一个pending项，签发时验证对应incarnation capability符合durable floor。pending start期间禁止首次submission fence及该member的remove/replacement；不占用全局mutation reservation，但两者的冲突检查由同一RSM执行。
- executor的本地supervisor必须持久保存permit high-water及STARTING/STARTED/REVOKED状态。启动和撤销在该supervisor中串行执行；只有`StartReplica`返回并核实本地实际shard/replica身份后才持久记录STARTED。
- supervisor将绑定PermitID/incarnation的STARTED结果重发给RSM；RSM校验pending permit精确匹配后，将其转成当前member的有界start证明并清除pending。**不要求membership/config-change-index改变。** 正常start完成后允许新的admission。
- RPC timeout不等于启动失败。executor或RSM leader重启后，使用同一permit查询supervisor的durable状态及实际本地replica；已经启动则补发STARTED，尚未启动则重试同一permit，不新分配token。
- 取消需supervisor持久记录REVOKED，并确认没有该permit的启动正在执行；若已启动，先StopReplica成功，再记录REVOKED。RSM收到精确token的撤销证明才清pending。迟到start被supervisor的high-water/tombstone拒绝；不能仅靠调用前远程读取permit来避免TOCTOU。
- supervisor永久丢失时不能清pending。需A.1的部署停止/旧incarnation禁止重启证明才能退休旧permit。新incarnation不能替旧incarnation发送成功ack。
- high-water及当前状态每个当前/未退休member只存一项，旧permit号不可复用。member退休后可清本地记录，但必须先禁止其旧incarnation重新执行；不能凭清本地文件重新取得旧身份。

该supervisor持久执行合同是新增实现要求，不声称当前StartReplica wrapper已经提供。不存在该owner时生产start-permit路径保持不可达。

#### A.5 BarrierSubmissionFence：完整终止与接管合同

RSM保存单个 `Fence(Token, OwnerIncarnation, ExpectedPhase, E, R)` 以及全局单调`NextOperationID`。token与reservation/permit同源分配，永不复用；溢出拒绝新操作。fence token不是leader term，leader变化不会隐式释放它。

1. **Acquire**：CAS无reservation、无pending start且无fence，记录expected phase/E/R。已识别的新协议entry依C.2持久提高decoder floor，不能等到Begin成功才保护后续admission。
2. **Begin/consume**：请求必须携带完整fence token、owner及expected tuple。RSM再次检查成员/能力条件；成功推进PREPARING和消费fence在同一次apply中完成。消费后迟到同token请求只能返回ALREADY_CONSUMED或STALE，不能再次递增E/R。
3. **业务拒绝**：保留fence及floor，由owner修正证据后重试或显式Release；不因一次checker read失败开放membership。
4. **Release**：仅接受当前token/owner的CAS，在一次apply中清除fence。随后延迟Begin因token不存在/不匹配拒绝；先后顺序由Raft apply确定。若Begin已先成功，则Release不能撤销phase。release不降低floor。
5. **Takeover**：经认证的控制面新owner以当前token CAS接管，分配更大的token并保留冻结状态；不依赖旧owner存活或主动释放。旧owner的Begin/Release从此STALE。新owner必须重新取得线性一致membership和当前incarnation能力证据，不能继承旧checker read。
6. **崩溃恢复**：fence、next ID、最后一个consumed token及结果进入同一snapshot。proposer死在Acquire后，由successor执行Takeover再Begin或Release；死在Begin提交后通过readback确认结果，不重新Begin。

仅保留最后一次consumed token用于精确幂等响应；更老token返回STALE并要求readback，不保存无限历史。请求处理遇到旧token必须先拒绝，不能因为phase恰好又回到PREPARING就重放旧操作。Release后不要求保留每个tombstone，单调计数器+当前fence精确匹配即可拒绝旧token。

### B. Catalog证据：提交后outbox与claim身份

#### B.1 Owner及可信边界

HAKeeper拥有E/R与transition，catalog transaction owner拥有marker/claim/completion。RSM不读取数据库、不接受普通CN heartbeat的completion声明。

新增内部proposal入口只接受经认证的catalog recovery coordinator；普通SQL用户及普通heartbeat不能调用。该coordinator属于可信集群控制面，不提供拜占庭容错。未经身份验证的任意RPC携带transaction ID不构成证据。

每个catalog marker记录 `(E, R, ClaimID, Kind, Sequence)`，证据outbox与对应业务修改在同一事务提交；dispatcher只能读已提交outbox。不能先发proposal再提交catalog事务。

#### B.2 精确流转

1. SEALED以后，coordinator获得当前E/R，事务锁定catalog barrier row，校验其generation不会倒退，写required marker和对应outbox。提交成功后发送 `CatalogRequired(E,R,Sequence)`。
2. RSM验证当前phase=SEALED、E/R精确匹配、发送者角色合法、sequence未处理；推进CATALOG_REQUIRED。超时重发同一evidence identity。
3. RSM分配单调、不可复用的ClaimID（绑定当前E/R）。catalog owner在事务中CAS claim row，写claim及outbox；提交后发送 `RecoveryStarted(E,R,ClaimID,Sequence)`。旧claim不能覆盖较新的claim。
4. worker每次catalog写入都在同一事务中校验claim row仍匹配；final transaction在锁定该row时验证所有恢复条件、写completion marker和outbox。提交后才能发送 `Complete(E,R,ClaimID,Sequence)`。
5. RSM只接受当前E/R/ClaimID且phase=RECOVERING的completion，再检查member/seal gates；成功才设置C=R和ACTIVATED。

supersession先在RSM推进E/R，立即禁止旧completion授权。新catalog worker只有在安装新claim后才能写；旧worker若在安装前提交，仍属于旧generation，不能开启新generation；安装后被事务CAS拒绝。public authority必须依据当前RSM E/R与catalog generation共同验证，不能仅凭某条旧completion row开放。

同一evidence重试幂等；相同identity但不同内容为错误；future E/R、旧claim、未知kind、未认证sender全部拒绝。catalog commit成功后publisher崩溃，由durable outbox重发。RSM已接受但响应丢失，重发读到相同结果。没有outbox/owner实现时该生产transition不可达，不通过test-only fixture偷渡成产品入口。

这些wire/认证/outbox/claim owner属于#29005接口合同；#29004仅实现consumer时不能宣称跨服务集成已验证。

#### B.3 Evidence清理、去重与容量

证据不是永久事件日志。每个协调范围只允许一个当前E/R/ClaimID；RSM保存三个阶段的固定receipt槽（required/started/complete）、当前claim及一个退休watermark，不保存逐generation历史。Sequence是该阶段的固定序号；同一identity不同内容必须拒绝，不生成新Sequence规避错误。

清理握手：

1. catalog事务提交outbox，publisher按同一identity发送。
2. RSM apply后返回 `ACCEPTED(identity,digest,result)`；该结果必须来自已提交状态，不能在propose之前或仅进入queue时返回。
3. publisher收到ACCEPTED，或通过线性一致readback得到相同receipt，才能事务删除对应outbox。删除失败重试；删除前崩溃重发仍幂等。
4. supersession使旧E/R/claim的证据永久不可授权后，RSM返回 `RETIRED(identity,currentWatermark)`。publisher验证watermark严格越过该证据后事务删除outbox；不能因timeout/普通STALE字符串删除。
5. 当前tuple下身份冲突或future证据不能删除为“已处理”；保留一个有界失败槽，停止该scope生产并告警，等待修复，不持续生成相同失败事件。

retirement watermark由单调E/R和不可复用ClaimID构成，随snapshot保存。claim replacement必须先在RSM提升claim ID并使旧claim永久无效，再由catalog CAS安装；旧claim outbox于是可获得RETIRED。恢复不得回退watermark后直接开放producer，必须完成committed log replay。已接受但receipt槽被新generation替换的重试，用watermark返回RETIRED，不需要保留旧payload。

容量是提交前的硬门禁：每个scope最多3条未确认outbox、每条编码后最多4 KiB；控制面部署固定全局上限1024条/4 MiB（两者分别计数），最多一个活动claim。额度计数和outbox插入在同一catalog事务中完成；相同identity重试不重复扣额度。超限回滚该控制事务并返回明确backpressure，不提交业务marker后丢掉outbox，不影响普通SQL事务。

supersession若旧outbox占满scope额度，先通过RETIRED握手回收；否则新marker事务等待/拒绝，不绕过额度另建scope。同一scope身份必须稳定，不能每generation新建quota桶。RSM receipt为固定3槽，publisher每scope最多一个in-flight RPC；全局dispatcher最多16个in-flight，失败采用有上限退避并受context取消，绝不把durable backlog全部读入内存。

required/completion marker不是outbox：每scope覆盖保存当前generation的固定row，历史generation不无限追加。旧claim/recovery工作数据只有在worker事务CAS已被新claim fencing后才允许分批清理；每批有固定条数上限，尚未清理的数据计入catalog recovery预算。#29005必须实现并验证这项预算，不能通过提前删marker释放额度。

retirement record每UUID仅保留当前/尚未退休owner；老record清理后依单调身份allocator及部署旧incarnation禁令拒绝复用，不保留每代tombstone。证据服务不可用时积压最多达到上述上限，随后暂停新的metadata lifecycle工作；不设置丢弃安全证据的TTL。

#### B.4 Authority退休及背压

不把heartbeat timeout/store deletion当作退休证明。选择显式drain：issuer先在durable issuance owner中停止为旧E签发新的authority，旧进程关闭该E的新metadata response/COMMIT授权入口并排空已经进入的授权操作，之后才发送绑定generation/E/R的seal ack。终止点定义为authorization linearization point，不承诺网络中已经发送的字节被收回。

generation替换不能替旧owner ack。每个UUID最多一个未退休owner；replacement可以运行普通SQL，但在旧owner退休前不得获得新的metadata authority。无权签发者不加入旧-owner target集合。由此不累积无界replacement历史。

故障进程没有ack时，选安全优先：保持SEALED并对该UUID的metadata authority背压。人工恢复需要受信部署owner确认旧进程已终止且不能重启，再由issuer发布durable retirement record；不能由heartbeat失联推导。普通SQL继续运行。若要求自动故障恢复，需要另行设计带明确时钟假设的bounded lease；本文不臆造现有系统没有的lease到期证明。

上述issuer/drain/retirement实现属于#29006；#29004不签发authority，MetadataReadsEnabled固定false。

### C. Snapshot及entry接受矩阵

#### C.1 定义

- F：新增durable decoder floor，当前支持0/1；大于1拒绝恢复。
- P：phase，仍遵守既有phase/E/R/C/protocol校验。
- V：runtime evidence schema version，0为旧格式，1为本文格式。
- I：evidence initialized；I=false时targets必须为空，所有runtime推进拒绝。
- B0：expression floor非零；B1：P非DISABLED；B2：F=1。
- V=1可以是未初始化的封闭状态，不以空targets自动推导ready。

required features必须精确等于B0|B1|B2，无多余已知bit，也不能缺少任何bit。未知bit先于RSM payload decode拒绝；envelope version仍为1。

| F | P | V/I/targets | 接受/运行行为 |
|---|---|---|---|
| 0 | DISABLED | V=0,I=false,无targets | 旧raw/MOH2正常；按原规则允许精确features的MOH3 |
| 0 | 非DISABLED | V=0,I=false,无targets | 兼容#29003 MOH3；仅恢复，不允许runtime推进/签发authority |
| 0 | 任意 | V=1或任何新evidence | 拒绝：缺少decoder floor |
| 1 | DISABLED | V=1,I=false,无targets | 合法；MOH3+B2；覆盖业务拒绝的Begin |
| 1 | DISABLED | I=true或有targets | 拒绝：未开始却有active evidence |
| 1 | 非DISABLED | V=1,I=false,无targets | 合法封闭状态；不得推进，适用于旧state仅提高decoder floor |
| 1 | 非DISABLED | V=1,I=true,合法targets | 合法；I=true的空集必须来自原子捕获，不能由decoder补齐 |
| 1 | 任意 | V=0或未知V | 拒绝 |
| >1 | 任意 | 任意 | 拒绝：不支持的decoder floor |

targets含重复身份、零generation、未知service type、不匹配E/R的ack、future phase ack均拒绝。完整新runtime evidence还包含claim/outbox消费identity与仲裁state；不得只序列化targets却丢失这些恢复条件。

F=1强制MOH3，即使P=DISABLED。raw/MOH2携带F=1拒绝。旧decoder不支持B2，必须fail closed。F=0与非DISABLED的旧MOH3只允许只读恢复/受控升级，不能默认为member gates已成立。

#### C.2 Entry语义及拒绝的Begin

新tag是decoder义务：**只要识别到新tag，就持久记录F=1，再解析payload**。这是对前稿“只有合法entry才提高floor”的修正；否则malformed entry已经进入日志仍可能被降级binary回放。

- 长度不足以读取tag：沿用现有外层entry framing错误处理，不解释为barrier entry。
- 可识别的新tag但payload损坏、未知request version/action、业务条件失败：F提高到1，初始化V=1/I=false（若之前无runtime evidence）；P/E/R/C及既有targets/claim保持不变，返回确定性的拒绝结果。
- request声明未知floor/version不能把F任意提高：当前tag的decoder最低义务固定为1；未知版本返回unsupported，不接受其desired state。
- duplicate valid request不得再次增加E/R；同一结果由expected tuple与保存的accepted operation identity判断。
- 未知tag仍沿用unknown-tag处理，不猜测其协议floor。

因此“拒绝原子性”是业务状态不变，不是整个RSM字节不变。测试分别比较index、F/V以及独立复制的P/E/R/C/targets/claim。SaveSnapshot在拒绝后也必须输出MOH3+B2；恢复后不能回到旧decoder可加入的状态。

F=0旧非DISABLEDstate首次遇到新tag时可以升级成F=1/V=1/I=false，但不生成ready。重新捕获必须经显式维护/owner退休证明，不能用decode过程自动重启barrier。

### D. 对应验证与交付状态

- 成员调度：用阻塞点复现投递后未执行、Begin争用reservation；executor崩溃/timeout/leader change后reservation不得消失；旧token在index推进后拒绝。
- Local start：成功start且config index不变时清pending，随后新admission成功；STARTED丢ack/restart补发，cancel与start交错，旧permit迟到、incarnation替换不得错误清pending。
- Submission fence：Acquire后死亡、Begin提交但响应丢失、Takeover与旧Begin/Release交错、Release后延迟Begin、snapshot恢复接管、token overflow与幂等receipt有界性。
- Evidence容量：提交后/ACCEPTED后/删除前各崩溃点；superseded outbox获RETIRED后回收；当前identity冲突不得删除；连续generation运行中receipt恒定3槽、outbox条数/字节不越界，满额时marker事务原子回滚。
- 升级：维护切换前无自动Begin；新executor拒绝旧无token命令；部署owner未提供停止/启动禁令证明时不启用新协议。
- Catalog：outbox提交前不发送、提交后崩溃可重放、重复幂等、旧E/R/claim拒绝、完成响应丢失重试；事务CAS阻止旧worker写入。
- Authority：g2不替g1退休，delete/timeout不完成seal；backpressure不影响普通SQL；无issuer时没有生产authority。
- Snapshot：上述矩阵全组合、malformed/unknown-version新tag、rejected Begin立即snapshot/recover、旧MOH3缺targets封闭恢复；所有拒绝使用独立oracle。

原修订记录当时只有文档修改；这不是对当前源码不存在 runtime 实现的判断，也不证明当前实现完整或通过验证。C节补齐了P2所需的明确接受矩阵。A/B节给出可审查的保守决策，但带来维护切换及显式退休的活性取舍；P1是否可关闭取决于reviewer是否接受这些需求约束，不能宣称已证明无停机升级或自动故障恢复。

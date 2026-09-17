# #29004：Catalog Metadata Barrier RSM 补充合同

- 状态：Draft；新增合同尚未获得设计审批。
- Review 修订：[仲裁、证据与恢复合同](CLAUDE_20260917-catalog-barrier-review-resolution.md)。若与本文候选方案冲突，以修订文档为准；其中维护切换与显式退休约束仍待 reviewer 接受。
- 父设计：`CLAUDE_20260916-catalog-metadata-barrier.md`。
- 基线：`e4511d6af419354f76f413df7602d24bfffe5a54`。
- 交付：设计与实现放在同一个 #29004 Draft PR；不另开 documentation PR。
- 审批边界：#29008 的审批覆盖父设计，不自动覆盖本文的 wire、target 及 completion evidence。

## 1. 缺口与不变量

父设计的 phase/E/R/C 足以表达 snapshot 格式，不足以证明运行时 transition。当前只有 View-specific observed epoch；Proxy 没有 generic capability，也没有独立 seal ack。不能复用 View admission 的 readiness/maps 来完成 catalog barrier。

HAKeeper 是唯一 epoch/phase owner。成员 heartbeat 提供证据，不直接修改完成 generation。要求同时满足：

- 证据绑定 service kind、UUID、进程 generation、barrier E/R；
- ack 必须产生于该 E 的对应阶段被观察之后；
- replacement 不能使旧进程持有的 authority 自动消失；
- required/completion marker 必须来自 catalog owner 的事务提交结果，不能把任意 heartbeat 声明视作 catalog proof；
- 新 barrier 不修改旧 persisted-expression 的 phase、floor、targets 或 readiness。

反例：捕获 A/g1 后，A/g2 heartbeat 覆盖 store record；若以当前 store 已更新为条件删掉 g1 target，就可能在 g1 仍返回 metadata 时提前完成 seal。

## 2. Wire 提案（字段号需最终复核）

保持 #29003 既有 tags 与意义。新增以下独立字段，不复用旧 View observed epoch：

| Message | 新字段 | 候选 tag |
|---|---|---|
| CatalogMetadataCapabilities | BarrierParticipantProtocol | 5 |
| CNStoreHeartbeat | CatalogMetadataAck | 27 |
| CNStoreInfo | CatalogMetadataAck | 29 |
| ProxyHeartbeat | CatalogMetadataCapabilities / CatalogMetadataAck | 7 / 8 |
| ProxyStore | CatalogMetadataCapabilities / CatalogMetadataAck | 9 / 10 |
| CatalogMetadataBarrier | RecipientGeneration | 7 |
| CatalogMetadataBarrierState | Targets | 7 |
| HAKeeperUpdateType | CatalogMetadataBarrierUpdate | 23 |

`BarrierParticipantProtocol=1` 表示实现独立 E/R/generation 的观测与 ack 合同，不表示已实现 View dependency tracking、recovery worker 或 public metadata authority。因此 #29004 不得广告 ViewDependencyProtocol/RecoveryProtocol=1 来冒充后续能力。LogStore 的 HAKeeperBarrierProtocol=1 仍同时承诺 MOH3 decode 和 barrier entry replay，不能由旧 V3 bool 推导。

新增 `CatalogMetadataAck`：

| 字段 | tag | 含义 |
|---|---|---|
| Generation | 1 | 当前进程 generation，非零；沿用现有 allocator 的身份来源，不沿用 View readiness |
| MembershipEpoch | 2 | 精确匹配当前 E，不接受大于 E 的值 |
| RequiredGeneration | 3 | 精确匹配 R |
| ObservedPhase | 4 | 观察到的阶段，不允许未知数值 |
| SealComplete | 5 | 只有本进程确实不再持有/签发该 barrier 之前的 metadata authority 才能置真 |

本阶段没有 public authority producer。即便发送 participant ack，也不能凭此替代尚未实现的 catalog marker/claim/completion。CN/Proxy 接收的 RecipientGeneration 必须匹配本地身份，否则丢弃整个新 barrier response，不更新新协议本地状态。

新增 `CatalogMetadataBarrierTarget`，Targets 为 repeated message：

- ServiceType=1、UUID=2、Generation=3：复合身份；不能仅按 UUID 索引。
- CapturedTick=4：捕获时 replicated tick；不因 g2 heartbeat 延长 g1 deadline。
- ObservedPreparing=5、SealComplete=6：本 E/R 的 durable ack；进入新 E 后清空。

Targets 序列必须按 kind/UUID/generation 排序，拒绝重复身份；snapshot 必须验证目标字段合法。不能把历史 #29003 中不含 targets 的非 DISABLED fixture 解释为“成员已全部确认”；需要显式区分缺失 target evidence 与合法空目标集，见 §6。

## 3. Transition 与原子发布

请求不携带任意 desired state。定义受限 action，携带 expected phase/E/R、目标 protocol floors；先完整验证，再一次性修改 state。未知 action、缺失 expected 条件、倒退 floor、整数耗尽均拒绝。重复 request 必须能判断已应用与过期，不以“再递增一次”实现 retry。

| Action | 前置条件 | 提交效果 |
|---|---|---|
| Begin | DISABLED；全 voting/non-voting decoder gate；无 pending admission | E=1/R=1/C=0，PREPARING，捕获目标 |
| Seal | PREPARING；目标 participant capability 及 preparing ack 全部满足 | SEALED；清空 seal ack，不借 preparing ack 完成 seal |
| CatalogRequired | SEALED；所有旧 authority 已明确撤销；catalog required marker 已提交 | CATALOG_REQUIRED，C 不变 |
| RecoveryStarted | CATALOG_REQUIRED；catalog owner 取得当前 R 的 durable claim | RECOVERING，C 不变 |
| Complete | RECOVERING；当前 R 的 durable completion，E/member gates 仍有效 | ACTIVATED，C=R |
| Supersede | 非 DISABLED；expected E/R 精确匹配，计数不溢出 | E/R 增加，C 保留，PREPARING；未退出旧 owner 仍须保留 |

同一 E/R 不能跳过阶段。任意失败不改变 protocol state；正常的 RSM applied index 前进与业务状态拒绝需分开断言。

## 4. Membership 与失败处理

- 首次新 entry 在 propose 前证明所有 voting/non-voting replica 支持 decoder/replay；RSM apply 时再检查 replicated membership。检查失败不得开始 barrier。
- 新 entry 一旦进入 Raft log，即使 apply 拒绝，它仍要求新 decoder。仅靠 phase 非 DISABLED 作为历史标记不充分：必须在提交前阻止不兼容 membership 进入，并规定 rejected-entry 的持久 decoder floor。此点列为 §6 的审批决策，不能静默套用父设计。
- LogStore capability 必须绑定匹配的 StoreIncarnation；旧 incarnation heartbeat 不得恢复新 incarnation 的 readiness。
- 对 CN/Proxy，g2 不能替 g1 ack；迟到 g1 ack 只能处理仍存在的 g1 target，不能替换当前 UUID owner。
- store deletion/heartbeat timeout 不等于 reader authority 已撤销。#29004 不实现基于 timeout 的 seal 成功捷径；没有明确撤销证据时保持 SEALED。
- 显式取消、leader 变化、worker crash 不回退 phase、不重置 floor；没有有效 evidence 时停止推进。
- snapshot 恢复采用 snapshot+committed log，不混入被复用对象的旧内存。恢复 targets 后必须仍能区分 preparing ack 与 seal ack。

## 5. 默认关闭与资源预算

#29004 没有自动 Begin proposer，也不新增面向普通 SQL 的手动激活入口。HAKeeper/LogStore decode/replay 与 test-only command driver 可以实现；catalog marker/claim/completion 的生产 producer 留给 #29005，public authority 留给 #29006。

不能以开放的 RPC 参数 `CatalogComplete=true` 冒充 durable proof。测试可使用受控 owner fixture，但产品路径在缺少 owner 时必须不可达。

`MetadataReadsEnabled` 在本阶段始终 false；不改变旧 CN startup/readiness 或普通 SQL availability。

Targets 若保留每次 replacement 会无界增长。提案：每 UUID 至多保留一个尚未撤销的旧 owner，并阻止再次授予新 metadata authority，直到旧 owner 退出；尚未获 authority 的进程不进入旧-owner target 集合。具体与 #29006 lease owner 的衔接需见 §6。不得用删除旧 target 来满足内存上限。

无新 goroutine、timer、catalog I/O；transition 扫描 O(replica+targets)，heartbeat 更新按目标身份查找。测试记录按成员数量缩放的分配/耗时，不声称与 cluster size 无关。

## 6. 必须关闭的设计决策

1. **Rejected entry 的 decoder floor**：是否新增独立 durable barrier-entry floor，使 rejected Begin 也不能导致旧 decoder 重新加入；以及该 floor 对 snapshot format 的影响。父设计仅以非 DISABLED phase 表示历史，不能覆盖此情形。
2. **旧 MOH3 的缺失 target evidence**：采用新增 required feature bit 2 的提案，不更换 magic 或 envelope format_version。该 bit 要求 reader 同时理解独立 decoder floor 与 runtime target evidence；旧 #29003 decoder 会明确拒绝。缺失 evidence 不等于合法空目标集，运行时不得凭空恢复 readiness。
3. **Catalog proof 的生产入口**：#29005 的 owner 如何绑定 generation、claim identity 和 committed transaction；#29004 可实现哪些不可自动到达的 transition，不能暴露伪造 completion 的入口。
4. **旧 owner 的退出与有界性**：在 #29006 尚无 authority lease 实现时，只允许显式证据，不采用 liveness timeout 当作 seal proof；最终 target 上限/背压需与 lease issuance owner 一起定义。

这些是协议正确性问题，不是单纯字段补齐。本文未关闭这些决策前，不广告新的运行时 capability，不更改生产 wire/snapshot 或提交可自动推进的状态机。

### 6.1 成员变更执行链的代码证据

`pkg/hakeeper/rsm.go:getCommandBatchFiltered` 对非 bootstrap 的 AddReplica/AddNonVotingReplica 在投递后删除 pending。`bootstrapReplicaCommandStatus` 的保留逻辑仅覆盖 bootstrap StartReplica。Log heartbeat 返回后，`pkg/logservice/service_commands.go:handleAddReplica/handleAddNonVotingReplica` 才调用 `store.addReplica/addNonVotingReplica`，最终进入 Dragonboat membership API。

最小反例：config-change-index=K 时下发 add，pending 清空；Begin application entry 提交但不改变 K；迟到 add 仍携带 K。`OrderedConfigChange=true` 本身无法排除此交错。仅在投递时或实际执行前多读一次 capability 都不能提供共同线性化点。

因此候选方向是 durable membership-operation reservation，从命令投递前保持到对应 Raft membership 结果确定；首次 barrier 提交与 admission 使用同一 replicated 仲裁 owner。超时不能释放未决 reservation。**尚未解决升级前已下发、没有 reservation 的旧命令如何退出，以及 authoritative membership result 如何进入仲裁 owner。**不得仅给新命令添加 reservation 就宣称支持 rolling upgrade。

### 6.2 独立 decoder floor 提案

候选新增 `HAKeeperRSMState.CatalogMetadataBarrierRequiredProtocolVersion`（tag 46，生成前再次核对）。合法新协议 entry 即使业务条件不满足，也持久保留 decoder floor；phase/E/R/C不变。非零 floor 强制 MOH3 + required-feature bit 2。malformed entry 与未知 version 的处理需单独定义，不允许垃圾请求任意提高 floor。

这显式修订父设计“只在非 DISABLED 时强制 MOH3”的合同。业务 rejection 测试必须分别断言 floor、业务 state 与 applied index，不能误用全对象字节不变断言。

## 7. 验证矩阵

- 每阶段合法与非法 transition、重复/过期 expected tuple、overflow；生产路径保持 disabled。
- voting/non-voting、pending admission、incarnation replacement、旧 decoder、Begin 被拒绝后 restart/downgrade。
- preparing 之前的 ack、未来 E、旧 R、g2 替 g1、same-UUID 连续 replacement。
- 所有 phase 的 snapshot recovery、缺失 target evidence、未知 feature/version、reused-state 原子性。
- persisted-expression activation 与新 barrier 交错，旧协议字段独立保持。
- catalog owner 不存在/claim 过期/旧 completion 时不得进入 ACTIVATED。
- ack 只表示新协议证据，不能改变旧 routing ready；MetadataReadsEnabled 保持 false。
- target 保留、背压与清理；不得通过成员超时、删除或计数 wrap-around 伪造成功。

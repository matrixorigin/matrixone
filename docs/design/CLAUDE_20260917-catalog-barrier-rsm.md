# #29004：Catalog Metadata Barrier RSM 补充合同

- 状态：Draft；新增合同尚未获得设计审批。
- 当前选定的 DRAFT 仲裁规则：[父设计 §14](CLAUDE_20260916-catalog-metadata-barrier.md#14-29004-仲裁证据与恢复修订draft)。维护切换与显式退休仍待 reviewer 接受；不继承 #29003 批准。
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

请求不携带任意 desired state。定义受限 action，携带 expected phase/E/R、目标 protocol floors；先验证业务条件，再一次性修改业务 state；识别新 tag 必须先持久提高 decoder floor，详见父设计 §14。未知 action、缺失 expected 条件、倒退 floor、整数耗尽均拒绝。重复 request 必须能判断已应用与过期，不以“再递增一次”实现 retry。

| Action | 前置条件 | 提交效果 |
|---|---|---|
| Begin | DISABLED；全 voting/non-voting decoder gate；无 pending admission | E=1/R=1/C=0，PREPARING，捕获目标 |
| Seal | PREPARING；目标 participant capability 及 preparing ack 全部满足 | SEALED；清空 seal ack，不借 preparing ack 完成 seal |
| CatalogRequired | SEALED；所有旧 authority 已明确撤销；catalog required marker 已提交 | CATALOG_REQUIRED，C 不变 |
| RecoveryStarted | CATALOG_REQUIRED；catalog owner 取得当前 R 的 durable claim | RECOVERING，C 不变 |
| Complete | RECOVERING；当前 R 的 durable completion，E/member gates 仍有效 | ACTIVATED，C=R |
| Supersede | 非 DISABLED；expected E/R 精确匹配，计数不溢出 | E/R 增加，C 保留，PREPARING；未退出旧 owner 仍须保留 |

同一 E/R 不能跳过阶段。业务失败不改变 phase/E/R/C、targets 或 claim；识别新 tag 即使 payload 损坏也提高 decoder floor。applied index、floor/evidence 与业务 state 分别断言。

## 4. Membership 与失败处理

- 首次启用新协议须先执行父设计 §14 的维护切换，排空无 reservation 的旧命令；随后由 durable reservation/submission fence 与权威 config index 协调 membership，能力检查不能替代共同线性化点。
- 新 entry 一旦进入 Raft log，即使 apply 拒绝，它仍要求新 decoder。仅靠 phase 非 DISABLED 作为历史标记不充分：必须在提交前阻止不兼容 membership 进入，并规定 rejected-entry 的持久 decoder floor。此点按父设计 §14 的独立 floor/MOH3 bit 2 规则处理，仍待审批。
- LogStore capability 必须绑定匹配的 StoreIncarnation；旧 incarnation heartbeat 不得恢复新 incarnation 的 readiness。
- 对 CN/Proxy，g2 不能替 g1 ack；迟到 g1 ack 只能处理仍存在的 g1 target，不能替换当前 UUID owner。
- store deletion/heartbeat timeout 不等于 reader authority 已撤销。#29004 不实现基于 timeout 的 seal 成功捷径；没有明确撤销证据时保持 SEALED。
- 显式取消、leader 变化、worker crash 不回退 phase、不重置 floor；没有有效 evidence 时停止推进。
- snapshot 恢复采用 snapshot+committed log，不混入被复用对象的旧内存。恢复 targets 后必须仍能区分 preparing ack 与 seal ack。

## 5. 默认关闭与资源预算

\#29004 没有自动 Begin proposer，也不新增面向普通 SQL 的手动激活入口。HAKeeper/LogStore decode/replay 与 test-only command driver 可以实现；catalog marker/claim/completion 的生产 producer 留给 #29005，public authority 留给 #29006。

不能以开放的 RPC 参数 `CatalogComplete=true` 冒充 durable proof。测试可使用受控 owner fixture，但产品路径在缺少 owner 时必须不可达。

`MetadataReadsEnabled` 在本阶段始终 false；不改变旧 CN startup/readiness 或普通 SQL availability。

Targets 若保留每次 replacement 会无界增长。选定 DRAFT 规则：每 UUID 至多保留一个尚未撤销的旧 owner，并阻止再次授予新 metadata authority，直到旧 owner 退出；尚未获 authority 的进程不进入旧-owner target 集合。由 #29006 的 issuer/drain/retirement owner 衔接，不能假定已有 lease 到期证明。不得用删除旧 target 来满足内存上限。

无新 goroutine、timer、catalog I/O；transition 扫描 O(replica+targets)，heartbeat 更新按目标身份查找。测试记录按成员数量缩放的分配/耗时，不声称与 cluster size 无关。

## 6. 选定 DRAFT 规则与审批边界

父设计 §14 是仲裁、证据和恢复的唯一完整定义。旧候选方案已由以下选择替代：

- 新 tag 一经识别就持久提高 floor，包括 malformed、未知版本与业务拒绝；
  floor=1 强制 MOH3 + bit 2，旧无 evidence 状态仅封闭恢复，不凭空变 ready。
- 维护切换排空旧命令；membership reservation 由 authoritative config index
  确认结果。local start permit 单独记账，超时、取消、未知结果不释放安全所有权。
- submission fence 使用不可复用 token；Acquire/consume/Release/Takeover
  由 replicated owner 仲裁，snapshot/readback 恢复结果，不依赖 proposer 存活。
- #29005 提供已提交的 outbox、认证 claim 与退休回执。每 scope 三槽/每条
  4 KiB，全局 1024 条/4 MiB；全局最多 16 个 in-flight，不能将 backlog 全装入内存。
- #29006 使用显式 drain/retirement；失联、删除或 timeout 不能代替旧 authority
  撤销。没有 producer 时产品 transition 不可达，MetadataReadsEnabled 固定 false。

这些选择不再留给实现临时二选一，但维护激活与安全优先的活性代价尚待 reviewer
接受。不得把 source 中已有 floor/runtime 部分当成整套协议已经完成的证明。

## 7. 验证矩阵

- 每阶段合法与非法 transition、重复/过期 expected tuple、overflow；生产路径保持 disabled。
- voting/non-voting、pending admission、incarnation replacement、旧 decoder、Begin 被拒绝后 restart/downgrade。
- preparing 之前的 ack、未来 E、旧 R、g2 替 g1、same-UUID 连续 replacement。
- 所有 phase 的 snapshot recovery、缺失 target evidence、未知 feature/version、reused-state 原子性。
- persisted-expression activation 与新 barrier 交错，旧协议字段独立保持。
- catalog owner 不存在/claim 过期/旧 completion 时不得进入 ACTIVATED。
- ack 只表示新协议证据，不能改变旧 routing ready；MetadataReadsEnabled 保持 false。
- target 保留、背压与清理；不得通过成员超时、删除或计数 wrap-around 伪造成功。

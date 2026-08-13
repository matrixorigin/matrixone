# CN 事务 Workspace 重构设计

## 1. 目标

CN 事务 Workspace 负责保存事务内尚未提交的写入，并向语句读取、重复键检查、
Ranges、Reader、CN transfer、spill、回滚和提交提供同一份事务视图。

本次重构的目标是消除“切片位置等于事务语义”的隐式契约。`txnOffset`、
`snapshotWriteOffset`、`offsets []int`、`batchSelectList` 和调用方直接访问
`txn.writes` 都不再是生产接口。Workspace 只暴露稳定标识、不可变读视图和受控
Payload lease。

## 2. 不可违背的事务语义

- 同一事务内，语句只读取其 `WorkspaceReadView` 发布时已经可见的 mutation。
- 普通新语句发布上一语句的全部有效写入；当前语句后续产生的写入不可穿透该视图。
- 禁止递增 Statement 的内部 SQL 可以读取调用者当前 attempt 已产生的写入，但不得推进
  调用者的公开语句边界。
- statement retry 必须撤销失败 attempt 的全部逻辑 mutation、表操作、物理对象和选择集，
  且不得撤销更早 statement 的结果。
- rollback、spill 和 compaction 可以改变物理布局，不能改变已经发布视图的逻辑可见集合。
- RC snapshot 前进和 CN tombstone transfer 必须以 statement 边界为单位，不能依赖可变切片位置。
- commit 只提交仍为 active 的 mutation，并保持现有 TN 请求顺序和 DDL/DML 约束。

## 3. 核心模型

### 3.1 StatementJournal

`StatementJournal` 是 statement 与 retry attempt 的唯一 owner：

- `StatementID`：事务内单调递增的逻辑语句标识。
- `AttemptID`：同一 Statement retry 时单调递增。
- `WriteMark`：执行开始时记录的
  `(StatementID, AttemptID, MaxMutationID, WriteScopeID)`；`WriteScopeID` 只用于
  Journal 校验嵌套执行的完成顺序，不暴露 mutation 位置。
- 普通 Compile 发布的 protocol-order epoch 是提交顺序分组边界。它不等于 StatementID：
  StatementID 只负责 attempt rollback/visibility；同一普通 Compile 内即使顺序打开多个 write scope
  或执行内部 SQL（例如先产生用户表对象写，再写入 `mo_tables` 元数据），仍继承同一 epoch 并统一
  遵守 catalog 优先和 mutation type 降序。内部 SQL 只捕获当前 ReadView，不得推进 epoch；write
  scope 只按 LIFO 校验执行所有权，不参与提交排序。
- 正常推进 Statement 边界前必须关闭该 Attempt 的全部 write scope；否则边界发布失败，且
  mutation transition、RC boundary 和 payload 状态均不得变化。rollback/retry 按 Attempt
  整体废弃未完成 scope，新 Attempt 不继承旧 scope。
- attempt 状态：open、completed、rolled-back。Journal 只保留当前 Attempt；正常完成的
  Attempt 不作为可增长历史继续驻留，事务级资源由独立索引维护。
- frontend statement execution 是否已开始、当前 execution 是否已经推进过边界；这些调用时序守卫
  与 Statement/Attempt 身份属于同一个状态机。
- RC statement snapshot、最近完成 transfer 的 snapshot，以及下一边界是否需要 transfer。
- attempt 级外部状态恢复动作，例如 ALTER 对事务内 `txnTable` schema cache 的修改。
- attempt 级外部物理资源，例如 LOAD TABLE 已写入但事务尚未提交的对象文件。

回滚通过 `(StatementID, AttemptID)` 选择 mutation，不再截断某个 slice 前缀。

RC 的 snapshot 历史与 transfer 游标只能由 StatementJournal 持有，不能在
`Transaction` 上保留第二份镜像状态。statement 完成时，mutation 状态转换、
StatementID 推进和 RC 边界发布必须在同一个 Workspace 临界区内完成；rollback 时也必须在
同一个临界区内同时撤销 attempt mutation 和 RC 边界。外部 TxnOperator 的 snapshot 可以先于
本地发布完成，但本地 Workspace 不允许出现“mutation 已推进而 RC 游标未推进”或相反的半状态。

外部状态恢复动作必须由当前 Attempt 登记。rollback 时 Workspace 先完成内部状态撤销，再将动作
移交调用方，在 Workspace 锁外按登记逆序执行；statement 边界完全成功后才释放动作。不得在
merge、spill 或 snapshot 更新尚可能失败时提前清空恢复状态。

LOAD TABLE 文件也必须由创建它的 Attempt 精确登记。statement rollback 只清理失败 Attempt 的
文件；正常完成边界后，文件由 Journal 的 transaction 级顺序与引用计数索引继续持有，供
transaction rollback 清理，不保留完整的已完成 Attempt 对象。物理删除成功后才移除 Journal 中的
所有权，删除失败时必须保留所有权以便后续 transaction cleanup 重试。全量清理、存在性检查和
删除不得扫描完整 Attempt 历史，Attempt 归属与 transaction 级索引必须由同一 Journal 操作原子更新。
克隆事务的对象保护状态只能在 Journal 中已经不存在该文件 owner 后移除。`Transaction` 不得再
持有平行的 statement ID、statement execution guard 或 LOAD 文件集合。

### 3.2 WorkspaceReadView

`WorkspaceReadView` 是不可变逻辑 token，包含：

- Workspace 身份；
- 发布 revision；
- 最大可见 `MutationID`。

ReadView 提供稳定字段的构造器和 getter，供 Workspace owner、诊断与跨模块传递使用；opaque
表示调用方不得把字段解释成物理数组位置、据此自行推导可见性，或绕过 Workspace 解析 mutation。
Workspace 校验视图属于自己，随后通过统一索引解析可见 mutation。

零值与 `NoWorkspaceReadView()` 在运行时是同一个值，只表示“没有事务内 mutation 可见”，不得被
静默替换成当前视图。生产调用方必须使用 `NoWorkspaceReadView()` 表达这个意图，使
committed-only、远程 shard 和漏传视图能够在源码与代码审查中区分；这不是运行时额外状态。

`EndStatement` 原子推进最小可读 revision，使本次 execution 发布的 ReadView 不能再获取新的
Payload lease。此前已经通过该 ReadView 得到的 `workspaceEntrySet` 持有 generation lease，仍可安全
使用到 owner 调用 `Close`；延迟关闭只会延迟物理回收，不得产生悬空指针。Reader、
LocalDataSource、Ranges、transfer 和 CommitBuilder 必须持有受控 EntrySet/lease，不能持有未受保护
的裸 Batch，也不能用过期 ReadView 重新解析或静默改读最新 generation。

### 3.3 TableOverlay

TableOverlay 的 key 是完整 `(accountID, databaseID, tableID)`。它是所有表级事务读取的
唯一入口，维护：

- insert mutation；
- rowid/block tombstone mutation；
- uncommitted data/tombstone object；
- PK candidate；
- 表级 active mutation、PK、delete 与 object owner 索引，以及当前 execution 所需的 retired ID
  历史。

Mutation 的 `active`、`createdRevision`、`retiredRevision` 和不可变 commit order 由 Workspace
全局 mutation registry 唯一持有。全局 `activeMutations` 是 commit 和 transaction-wide current
state 的有序索引；TableOverlay 只保存稳定 MutationID 的表级 membership/current/history 索引，
不得复制 mutation 的可见性状态或成为第二事实源。发布和退休必须在同一个 Workspace 临界区内
同时更新全局 registry、全局 active 索引与相关 TableOverlay。

Mutation 发布时一次性从 payload 提取不可变分类事实，Overlay 据此维护 PK candidate、内存
compaction、BlockMeta 和按物理形态分类的 delete mutation 索引。CN transfer 通过 delete
索引定位存在可见内存 tombstone 或对象 tombstone 的表；compactor 和 duplicate-key check
也只查询自己的索引，不得为寻找候选项扫描、pin 全事务或无关表 payload。索引只保存稳定
MutationID；active/revision、generation 和 selection 仍在读取时由统一 ReadView 路径校验，因此
spill、rewrite、rollback 不会留下可见性错误。

全局和表级当前态 mutation 索引必须同时维护 MutationID membership 与不可变 TN commit order。
当前 Reader、LocalDataSource 和 compactor 按表级 active 索引顺序遍历；CommitBuilder 按全局
`activeMutations` 顺序遍历。索引可以生成本次消费所拥有的 ID/Entry 快照，但不得为每次读取重新
扫描 mutation 历史并排序。历史 ReadView 需要合并 active 与 retired revision 时，允许在该
statement execution 的有限历史范围内恢复 commit order；该成本不能回流到最新 revision 的热路径。

当前 statement execution 内已经发布的 ReadView 使用不可变的 statement-scoped 索引历史；只处理
当前 workspace 状态的 memory compaction、BlockMeta compaction 和 CN transfer delete planning
使用独立 active set。发布与唯一退休入口同时维护两类索引；`EndStatement` 使该 execution 的
ReadView 过期并回收无 rollback owner 的历史元数据，不能让索引历史随整个事务持续增长。当前态
planner 不得从历史记录中过滤 active mutation。

Workspace 的 active mutation 基数是发布/退休状态转换共同维护的内部不变量。Statement 边界只读
取该 O(1) 基数，不得为判断当前写集大小遍历完整 mutation 历史。全量扫描只允许出现在显式一致性
校验中，并必须同时验证该基数、Payload usage 与 active mutation 实际集合一致。

Transaction-local object delete 同样维护独立的 active ID 集合。当前 compaction snapshot 只能遍历
该集合，不能按单调增长的历史 ID 上界扫描所有已退休 delete；发布与唯一退休入口必须原子维护集合。
历史记录保留 created/retired revision，直到当前 execution 的 Statement ReadView 全部失效；active
索引不替代也不改变 execution 内的历史版本语义。

用户表 INSERT/DELETE 的 PK candidate 分类必须在 mutation 发布前闭合：无主键写入发布空
descriptor；有主键写入发布精确的 payload vector 位置。表定义不可用、主键列缺失或 delete
payload 不满足形态约束时，写入必须在发布前失败。Workspace 不存在“尚未解析”的 PK 状态，
duplicate-key check 也不得在 commit 阶段重新查 TableDef 或猜测 payload 布局。

Transaction-local object owner 是 TableOverlay 的当前态索引。索引由 mutation 发布和唯一退休入口
原子维护，object-delete compaction 直接按 ObjectID 获取当前 owner；同一个 ObjectID 存在多个
active owner 时必须明确失败。它不遍历该对象的 mutation 历史，也不承担历史 ReadView 解析。

物理对象名引用是 transaction 级当前态索引，因为 clone cache 和 GC 以 ObjectName 为身份，而不以
表为身份。索引保存发布时提取的 ObjectName 到 active MutationID 集合，支持排除本次 GC 自身的
Entry；retire 必须同步移除引用。不可变 mutation 仍独立保留 object facts 和 revision，历史
ReadView 不依赖当前态引用索引。clone 保护、rollback GC 因而既不打开 payload，也不扫描已退休引用。

Reader、LocalDataSource、Ranges、TableMetaReader、CN transfer 和 commit 不再扫描全局写集。

未提交的 persisted tombstone 只存在于对应表的 DELETE mutation 中。消费方必须通过
`WorkspaceReadView` 获取 generation-pinned payload，并应用该 generation 的 selection；
`Transaction` 不得另建 object-stats map、slice 或其他镜像注册表。这样旧 ReadView 不会看到
后续 statement 追加或改写的 tombstone，rollback/selection 也只有一个事实源。

### 3.4 DDL Catalog Overlay

事务内 database/table 名称绑定、table ID 反向名称查询、create-in-transaction 标记和 drop 状态
由 Workspace 内的 DDL Catalog Overlay 统一持有。每个操作记录精确的
`(StatementID, AttemptID)` owner；rollback 只移除该 Attempt 添加的栈后缀，不扫描或截断其他
statement 的操作，也不回退已经完成的 create、drop 或 rename。

`Transaction` 不再持有平行的 table/database operation chain。ALTER 修改事务内 `txnTable`
对象时，通过 StatementJournal 登记恢复动作；DDL Overlay 负责名称与生命周期语义，恢复动作负责
对象 cache 的内存状态，两者在同一 Attempt rollback 中完成。

### 3.5 PayloadStore

逻辑 mutation 只保存 `PayloadID`。PayloadStore 管理物理内容：

```text
Memory -> Spilling -> Object -> Retired
```

读取方通过 generation-aware lease pin payload，不能持有未受保护的裸 `*batch.Batch`。
Payload 只有在没有 lease 后才能释放。selection 与 payload 同生命周期归 PayloadStore 管理，
不再使用以裸 Batch 指针为 key 的全局 map。

`EndStatement` 回收已经 retired、没有 lease 且不再参与 statement rollback 的 generation。
selection-only generation 可以共享同一个物理 Batch；只有全部保留 generation 都不再引用该
Batch 时才能释放。当前 Attempt 的 rewrite undo 仍可能在下一条语句边界推进失败后用于恢复旧
payload，因此其物理 Batch 保留到边界成功推进或 rollback 完成。pin 未及时释放只会延迟回收，
不能导致悬空指针；物理 Batch 的 `Clean` 必须在 PayloadStore 和 Workspace 锁外执行。

spill 使用三阶段协议：

1. 锁内选择 mutation、pin payload、登记 spill generation；
2. 锁外执行文件 IO；
3. 锁内校验 generation 和 mutation 仍有效，原子发布 Object payload；失败则撤销状态并清理
   未发布对象。

任何阶段都不得在 Workspace 全局锁内执行远程 IO。

### 3.6 CommitBuilder

CommitBuilder 从 Workspace 全局有序 `activeMutations` 构建现有 TN commit request，不逐个扫描
TableOverlay。构造时在同一个 Workspace 读锁临界区内捕获最新逻辑 ReadView、按 commit order
复制 active MutationID，并 pin 全部可见 Payload generation；锁释放后的 Build 只消费这份冻结的
`workspaceEntrySet`，不再读取可变 Workspace 状态。Builder 是该 EntrySet 的 owner，所有成功和
失败路径都必须 `Close`。

构建过程必须满足：

- 使用显式 commit order，不依赖 slice index；
- 新逻辑 mutation 在创建时一次性获得不可变顺序
  `(protocol-order-epoch, catalog-rank, descending-type-rank, append-ordinal)`；
  `Adjust` 只校验 WriteMark 的归属和精确一次关闭，不得事后改写顺序。write scope 之前的 mutation 因此
  不形成独立排序前缀；整个普通 Compile 及其内部 SQL 保持 catalog 优先、mutation type 降序及
  同类追加顺序，而不同普通 Compile 之间绝不重排；
- commit order 属于逻辑 mutation 而不是物理 payload 或追加时刻。一对一 rewrite 原位继承；
  一对多 rewrite 在原位置下生成稳定的层级子序，嵌套 rewrite 继续细分该子序；没有逻辑来源的
  新 mutation 才追加到提交序列末尾。这样 catalog 的相邻协议项不会因 transfer、spill 或
  compaction 被拆开；
- 多对一 spill/object rewrite 必须显式携带全部 source mutation ID，校验它们属于同一物理
  spill group，并继承最早 source 的逻辑位置；一次 source 只能被一个 replacement 认领。
- 保持 delete/insert、catalog/user table 和 DDL/DML 的既有排序；
- 只通过 Payload lease 读取物理内容；
- build 完成前 Payload 不得释放。

### 3.7 并发、锁与资源所有权

Workspace 的逻辑状态与物理 Payload 分层加锁：

1. `Transaction` 锁保护事务编排、table cache 和 Workspace 之外的事务状态；同时需要时先取得
   `Transaction` 锁，再进入 Workspace。
2. `txnWorkspace.mu` 是 StatementJournal、mutation registry、TableOverlay、DDL Overlay、revision、
   current-state 索引和 usage accounting 的唯一逻辑状态锁。一次发布、退休、边界推进或 rollback
   的所有可见性变化必须在这个临界区内原子完成。
3. `PayloadStore.mu` 只保护 PayloadID、generation、pin、selection、状态转换和 reclaimable 集合。
   Workspace 在持有 `txnWorkspace.mu` 时可以进入 PayloadStore；PayloadStore 不得反向调用或获取
   Workspace 锁。lease `Close` 只进入 PayloadStore，因此可以在 Workspace 锁外独立释放。

外部 IO、物理对象删除、ALTER 恢复回调和 Batch `Clean` 不得在 Workspace 或 PayloadStore 锁内
执行。需要跨锁和 IO 的操作必须显式转移 owner：

| 资源 | 稳态 owner | 临时 owner | 完成边界 |
| --- | --- | --- | --- |
| mutation 可见性与 commit order | Workspace registry + active 索引 | 无 | 同一 Workspace 临界区内发布/退休 |
| 表级 current/history membership | TableOverlay | 无 | 同一 Workspace 临界区内发布/退休/边界回收 |
| 表级读取结果 | 无 | statement-scoped EntrySet | EntrySet `Close` |
| Payload generation | PayloadStore | generation lease / EntrySet | lease `Close` 后才可 reclaim |
| spill source 与未发布对象 | Workspace mutation + PayloadStore generation | `workspaceSpillAttempt` | 原子 publish 或 abort 后 `Close` |
| attempt rollback mutation、回调和 LOAD 文件 | StatementJournal | `workspaceRollback` | 锁外 GC、`RunActions`、文件清理与 `Close` |
| commit 输入 | Workspace active 索引 + PayloadStore | `workspaceCommitBuilder` | Build 返回后 `Close` |

任何临时 owner 都必须在错误、取消和 retry 路径释放；不得用超时回收或 finalizer 替代明确的
`Close`。逻辑状态已经发布而外部 IO 尚未完成时，必须通过 generation/attempt 身份重新校验，不能
凭裸指针或先前数组位置继续提交结果。

### 3.8 复杂度与性能契约

定义：`A` 为事务当前 active mutation 数，`A_t` 为目标表 active mutation 数，`C` 为某个当前态
候选索引（例如 PK、compaction、BlockMeta 或 object delete）的 active mutation 数，`H` 为已退休历史，
`R_b` 为自上一 execution 边界以来的 reclaim 候选，`T` 为事务触及的 TableOverlay 数，
`A_attempt` 为当前 Attempt 拥有的 mutation、DDL、object delete、undo 和外部资源总数。

- 当前表读取为 `O(A_t)`；PK、compaction、BlockMeta 和 object-delete 等当前态候选解析为 `O(C)`。
  两类路径都不得依赖 `H`，也不得扫描与本次语义无关的 mutation。
- CommitBuilder 冻结输入为 `O(A)`；ordered set 插入/移除为 `O(log A)`，快照为 `O(A)`，不得重新
  扫描或排序 `H`。
- active mutation 计数和 RC 当前边界读取为 `O(1)`。
- `EndStatement` 的逻辑 metadata 回收只处理 `O(R_b + A_attempt + T)`；其中 `T` 只用于清空各
  Overlay 的 execution-scoped retired ID journal。Payload 回收只处理已进入 reclaimable 集合的
  generation，不扫描全部 Payload 历史。
- statement rollback 与 retry 只与 `A_attempt` 成正比，不得随已完成 Statement/Attempt 数增长。
- LOAD 文件存在性与引用计数更新为 `O(1)`；实际清理只与本次选择的文件数成正比。

benchmark 的稳定基线不是固定 `ns/op`，而是输入维度不变性：以下 benchmark 分别以
0/1,000/10,000 条无关历史比较热路径，增加 `H` 时对应操作的工作量和分配不得线性增长；
`BenchmarkOrderedMutationSetSnapshot` 单独验证快照只随 `A` 线性增长。

- `BenchmarkTxnWorkspaceCurrentStateIgnoresRetiredHistory`
- `BenchmarkTxnWorkspaceObjectDeleteSnapshotIgnoresRetiredHistory`
- `BenchmarkWorkspacePayloadReclaimIgnoresUnrelatedHistory`
- `BenchmarkTxnWorkspaceRCBoundaryStateIgnoresHistory`
- `BenchmarkTxnWorkspaceLoadFileRemovalIgnoresAttemptHistory`
- `BenchmarkOrderedMutationSetSnapshot`

## 4. 模块边界

- `pkg/txn/client`：定义逻辑 ReadView/WriteMark 值、构造器、只读 getter 与 Workspace 生命周期接口。
  这些 getter 只用于传递、归属校验和诊断；调用方不得自行解释可见性。
- `pkg/vm/engine/disttae`：拥有 Journal、Overlay、PayloadStore、CommitBuilder。
- `pkg/sql/compile`：捕获并向 Scope 传播 ReadView/WriteMark，不解释内部字段。
- Reader/LocalDataSource/Ranges/transfer：只接受 ReadView，通过 Relation/TableOverlay 查询。
- TN 协议保持不变；重构仅改变 CN 内部组织与构建方式。

公开 API 与内部状态机的对应关系：

| 公开 API | owner 内部动作 | 不承担的职责 |
| --- | --- | --- |
| `StartStatement` | `StatementJournal.beginExecution`，打开 frontend execution guard | 不推进 StatementID，不发布 ReadView |
| `IncrStatementID` | 合并/必要 spill，原子完成 Journal attempt transition 与 RC boundary publication | 不结束 frontend execution，不回收该 execution 的 ReadView |
| `PublishReadView` | 推进 protocol-order epoch，并将当前 revision 与 MutationID frontier 发布为普通 Compile 边界 | 不改变 StatementID |
| `CurrentReadView` | 捕获当前 revision/frontier，供内部 SQL 或隔离 Workspace 使用，并继承当前 epoch | 不发布普通 Compile 边界、不推进 epoch |
| `BeginWriteAttempt` / `Adjust` | 注册/关闭 attempt 内的 active write scope，返回/校验稳定 WriteMark | 不按物理 offset 重排 mutation |
| `RollbackLastStatement` | rollback 当前 Attempt，返回锁外清理资源并把 Journal 标记为 retry pending | 不直接创建下一 Attempt |
| retry 后的 `IncrStatementID` | 保持 StatementID、递增 AttemptID，创建下一 open Attempt | 不重复清理上一 Attempt |
| `EndStatement` | 关闭 execution guard、使本次 ReadView 不能再新 pin、回收未 pin 的 retired 状态 | 不等同于 Statement 完成；已 pin lease 继续有效 |

## 5. 生命周期

```text
frontend execution
  -> StartStatement
  -> IncrStatementID(commit=false)
       -> merge / spill if required
       -> advance previous Attempt and RC boundary atomically
  -> PublishReadView                         // ordinary statement compile
     or CurrentReadView                      // internal SQL / isolated workspace
  -> BeginWriteAttempt
  -> append / rewrite mutations
  -> Adjust(WriteMark)                       // close this active write scope exactly once
  -> readers / EntrySets release their leases
  -> EndStatement
       -> expire this execution's ReadViews for new resolution
       -> reclaim unpinned retired metadata/generations

retry after failed execution
  -> cancel and wait for running scopes
  -> RollbackLastStatement
       -> retire failed Attempt state
       -> transfer rollback entries/actions/LOAD files to workspaceRollback
       -> perform object/file IO and callbacks outside Workspace lock
  -> IncrStatementID(commit=false)
       -> owner-preserving compaction / spill of completed Attempts is allowed
          while the failed Attempt remains rolled back and retry pending
       -> logical rewrites remain forbidden until the retry Attempt is open
       -> keep StatementID, increment AttemptID, open retry Attempt

commit
  -> finalize current statement boundary and spill if required
  -> create CommitBuilder from global activeMutations
       -> atomically freeze commit order and pin payload generations
  -> encode existing TN precommit request without reading mutable Workspace
  -> CommitBuilder.Close
       -> release the frozen EntrySet and payload leases
  -> TN commit
  -> FinalizeCommit / FinalizeCommitWithUnknownResult releases CN-local state
```

Retry preparation deliberately separates physical payload maintenance from
logical mutation ownership. `IncrStatementID` may compact or spill mutations
owned by an earlier completed Attempt before it opens the successor retry
Attempt. Such a replacement must preserve the original
`StatementID`/`AttemptID`, commit order and visibility, and it must not register
rollback state in either the failed Attempt or its successor. Mutations owned
by the failed Attempt have already been retired and cannot participate.
Ordinary logical rewrite APIs continue to require an open Attempt.

CommitBuilder 只活到现有 TN precommit request 编码完成：protobuf request 已拥有编码后的数据，随后
不再依赖 Workspace payload。它必须在请求交给 TxnOperator 提交前释放冻结 EntrySet；TN commit 和
最终 CN 状态释放不由 Builder 持有 lease。

`EndStatement` 是 execution 生命周期边界，不是 mutation 提交边界。正常路径应尽早关闭 Reader、
EntrySet 和 CommitBuilder；即使 owner 延迟 `Close` 到 `EndStatement` 之后，既有 lease 仍保持物理内容
有效，但不得再使用旧 ReadView 获取新的 lease。

## 6. 删除条件

迁移完成必须满足：

- 生产代码不存在 `txnOffset`、`snapshotWriteOffset`、`adjustWriteOffset`、
  `offsets []int`、`batchSelectList`；
- 生产代码不存在对 `txn.writes` 或 Workspace 内部 mutation slice 的直接访问；
- Reader、Ranges、LocalDataSource、TableMetaReader、CN transfer、spill、rollback、commit
  均通过新接口工作；
- 没有双实现、兼容 fallback 或按 offset 回退的路径。

## 7. 验证

- statement 可见性、内部 SQL 可见性；
- retry/rollback 与多 attempt；
- database/table create、drop、rename、ALTER 的成功边界、rollback 与 retry；
- ALTER 外部恢复动作逆序执行、Workspace 锁外执行及成功边界释放；
- RC snapshot 与 transfer；
- spill 与 rollback/commit 并发、generation 失效、IO 失败；
- payload lease 与 GC；
- 多 account 同 table ID 隔离；
- commit request 与重构前语义等价；
- mutation 数增长时，表级读取和 rollback 不再退化为全局前缀扫描。
- mutation 历史增长时，Statement 边界的 active mutation 计数保持 O(1)，且 spill、rewrite、
  compaction、rollback、drop/retire 后与实际 active 集合严格一致。
- object delete 历史增长、当前 active 数固定时，compaction snapshot 只与 active delete 数量相关；
  consume/rollback 后 active 索引与 revision 历史必须同时正确。
- object owner 和 ObjectName 引用历史增长、当前 active 引用固定时，object-delete compaction 与
  clone GC 只遍历 active owner/reference；rewrite、spill、rollback 和 retire 后当前态索引与不可变
  mutation 历史必须同时正确。
- `EndStatement` 后旧 ReadView 新解析必须失败，而边界前已经 pin 的 EntrySet 必须可读取到 `Close`；
  `Close` 后 retired generation 才允许回收。
- CommitBuilder 构造与并发 rewrite/spill/rollback 之间必须得到单一冻结视图，Build 期间不得重新读取
  Workspace 或观察半发布状态；成功、错误和取消路径都必须释放 lease。
- 锁顺序和 owner 转移必须通过并发/故障注入测试验证：Workspace/PayloadStore 锁内无外部 IO，
  rollback 回调逆序且锁外执行，spill publish 前重新校验 generation/Attempt。
- 上述复杂度契约对应的 benchmark 必须保留 history-dimension 对照；性能评审以复杂度退化和分配增长
  为门禁，不用单机绝对 `ns/op` 代替语义判断。

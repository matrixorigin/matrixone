# #28238 / #30107：接受感知的 AUTO_INCREMENT 与约束仲裁设计

## 0. 修订、范围与审查状态

- 修订：**r1，2026-10-14，待独立设计审查**。
- 问题：[#28238](https://github.com/matrixorigin/matrixone/issues/28238)；实现系列：[PR #30107](https://github.com/matrixorigin/matrixone/pull/30107)，承接已合并的 #28349。
- 实现对照：`6bbac783645040a2ee97e017b196240d83755b9c`；该实现的 main 基线：`a53390c57de48ac280249ff0e88831e502e20fb0`。本文不描述尚未合并的后续 main 行为。
- 本文是对缺失设计门禁的补件，**不声称设计先于已有实现获批**。设计修订以 Git commit 固定，PR 正文必须链接该精确版本；修订变化重新审查受影响决策。
- 完整范围是跨 allocator、PRE_INSERT、行仲裁、FK 存储读取、最终键锁及 wire 的所有权/协议重构，满足规模和复杂度门禁，不能因 PR 类型为 BUG 而豁免。
- [旧 IGNORE 设计](20260908-auto-increment-boundaries.md)只提出后续 acceptance cursor；[#23371 设计](CLAUDE_AUTO_INCREMENT_23371.md)明确排除本问题。其批准、实现 APPROVE、CI 成功均不构成本文的设计批准。
- 当前审查记录：`设计 r1 / 尚无独立 PASS / 实现批准保持阻塞`。不得提前 resolve 设计门禁线程。独立审查应记录设计 commit、决定、阻塞项及接受的权衡，不要求额外 human-approval 标签或 RFC 转换。

## 1. 问题、依据与成功条件

旧链路先在 PRE_INSERT 发号，再过滤 CHECK/FK，最后执行 PK/UK 仲裁。被提前过滤的行丢失候选来源；被拒绝的正显式大 ID 已推进共享分配器，甚至使后续有效行提前溢出。下游不能恢复这些信息，更不能回退全局高水位。

问题讨论给出的四行 CHECK witness 为 `(NULL,1,-1),(NULL,2,1),(NULL,2,1),(NULL,3,1)`：先前 MO 接受 `(id,uk)=(2,2),(3,3)`，MySQL 8.4.10 接受 `(1,2),(2,3)`。另一个 TINYINT witness 中，因重复 UK 拒绝的显式 127 不应使后续有效生成行尝试 128。两者不是要求全局无间隙。

参考 MySQL 8.4 的 [AUTO_INCREMENT](https://dev.mysql.com/doc/refman/8.4/en/innodb-auto-increment-handling.html)、[INSERT](https://dev.mysql.com/doc/refman/8.4/en/insert.html)、[LOAD DATA](https://dev.mysql.com/doc/refman/8.4/en/load-data.html) 和 [外键约束](https://dev.mysql.com/doc/refman/8.4/en/create-table-foreign-keys.html)。MySQL 行映射是兼容性参照；MO 的分布式独占号段、事务快照和锁协议仍是实现约束，不复制 InnoDB 自增锁模式。错误文本和 warning 完整兼容不在本次目标内。

### 必须保持的不变量

1. 已消费的共享号段永不归还、倒退或与其他语句/CN 重叠；业务 rollback 不回收共享租约。
2. 同一串行仲裁器内，被拒绝的生成候选可以重用；只有完整通过约束的行推进候选游标和显式正 ID 下界。负数、显式零及 `NO_AUTO_VALUE_ON_ZERO` 继续遵循既有规则。
3. CHECK 等可提前判断的表达式只物化一次，不为重排重新执行 volatile 表达式。自动 ID 依赖的约束不能拿临时值当最终值。
4. 拒绝行不拥有任何 PK/UK/self-parent 键；当前行 self-FK 匹配不是向 accepted-parent 集合投机发布。
5. 输出、最终 PK 锁、普通及同步维护索引读取同一接受行映像；不得只改主表 ID。最终输出批次未完成时不发布本批生成 ID 的语句结果。
6. 真正 allocator/reader/表达式/容量/取消错误使语句失败，不能伪装成 FK 不存在或普通 IGNORE 跳行。
7. 新执行元数据必须同时通过发送方、接收方与实际目的 CN 的能力门禁；protobuf 可忽略未知字段不等于语义兼容。

成功标准：问题 witness、首/中/末/全部拒绝、接受/拒绝显式高 ID、整数终端、会话序列、跨批次及 prepared reuse 都符合这些不变量；base/index 一致，取消/失败后可清理；不增加逐行 allocator 事务，不宣称跨 CN 全局连续。

## 2. 边界与替代方案

自动路径仅用于纯 IGNORE、一个真实单列整数 AUTO_INCREMENT PK，并存在其它有效 UNIQUE 或 CHECK/FK。排除 ODKU、fake/composite PK、cluster-by/不可靠 remapping、可跳过 dedup 的内部路径，以及会依赖重写 ID 的生成表达式/非冗余唯一表达式。直接包含该 PK 的冗余 UNIQUE 不得把未生成的值当最终键。独立 self-FK 元数据还覆盖无 AUTO_INCREMENT 的 IGNORE。

普通 FK 保留向量化 join；只有 ID-dependent FK 和 self-FK 转入有序路径。普通非 IGNORE 自引用 INSERT/LOAD 保留语句检测 SQL；IGNORE 必须逐行拒绝，不能用检测 SQL 把整个有效语句回滚。`foreign_key_checks=0` 沿用现有禁用语义；prepared/catalog dependency 变化必须触发既有失效/重绑。

不新增 catalog 属性、allocator 持久表字段、后台 worker 或通用 retry 框架；不实现 gapless sequence、全局行顺序、新 FK 动作或新的 charset/collation、分区、UDF、存储过程、warning 兼容。

| 方案 | 正确性、成本及兼容性 | 决策 |
|---|---|---|
| 保持先发号后过滤 | 无新协议，但丢失来源，拒绝显式 ID 仍推进/溢出 | 不能满足问题契约 |
| 全部约束预过滤 | 普通 CHECK 可用且便宜；ID-dependent FK/生成键必须先知道最终 ID，循环依赖仍未解决 | 只用作可独立判断部分 |
| 回退 allocator / 按接受行重新发号 | 回退破坏跨 CN 租约独占；逐行 allocator 事务增加 I/O/争用，取消恢复复杂 | 拒绝 |
| 批量投机 ID 加向量 FK、失败再重算 | 有机会降低 FK I/O，但拒绝改变后继 ID，需迭代重算/撤销所有键、结果和 volatile 求值 | 暂不选择，不能无界重试 |
| 语句私有租约游标 + 完整接受仲裁 | 保留共享 allocator，准确延后显式推进；需要新所有权/wire 和部分逐候选 FK 读取 | 选择；接受下述扫描和表锁权衡 |

选择不是吞吐保证。未来向量化必须保持接受顺序、最终键、self visibility 和单次求值，不可作为静默语义 fallback。

## 3. 数据与控制流

```text
SQL/session snapshot + typed table/epoch
  → binder：物化 CHECK eligibility；拆分 ordinary / ordered FK
  → PRE_INSERT：CHECK 选行、生成来源标志，延迟自动 ID/显式推进
  → ordinary FK join：保留资格位，不丢候选来源
  → 串行 PRE_INSERT_UNIQUE：候选 → eligibility → PK/UK → ordered FK
      → 接受：键集合、生成游标/显式下界、self-parent、最终输出
      → 拒绝：不发布、不消耗当前生成候选
  → lease TS 主键历史检查 → 最终 PK LOCK_OP
  → 接受行 SINK / MULTI_UPDATE / index consumers → 业务事务 commit
```

### CHECK、来源与 LOAD 策略

`retainInsertIgnoreCandidates` 把过滤条件转为 PROJECT 中一次求值的 bool，logical/physical pruning、deepcopy 和 codec 均保留位置。PRE_INSERT 的 CHECK selector 仅复制选中行；选中输出不可 shrink 借用的 child vector。全部 CHECK 失败且尚未进入生成阶段时，不启动 reservation。VALUES 已知输入数仍作为后来启动 reservation 的需求预算；SELECT/LOAD 未知长度按块请求，不能承诺与 VALUES 相同的未用号段形状。

生成来源标志在把零转 NULL 的既有 sql_mode 规则后捕获。deferred 路径对 AUTO_INCREMENT 的暂时 NULL 不提前做最终 NOT NULL 判断，其它列保持既有检查。FK eligibility 在 arbiter 处消费；资格为 NULL/false 的行不发布键。

LOCAL（非 REPLACE）的行拒绝与显式 IGNORE 的 decoder 放宽是两种策略。`tree.IsIgnoreStatement` 供行/表达式 profile 使用；两个 LOAD binder 从语法重算 `ExternParam.LoadExplicitIgnore`，通过 Createsql JSON 传递。compiler 对 LOCAL 只因显式 IGNORE 放宽严格输入解析；非严格模式仍非严格。TIME 的原始文本/assignment cast 是上游独立契约，不可把 LOCAL row-profile=true 当作所有输入列都应宽松解析。复用 AST 重绑必须重置该标志。

## 4. 租约、游标与线性化点

`AutoIncrementReservation` 是内部 context request，不是 SQL API：`ColumnIndex, Type, Count, Runs`。调用 `InsertValues` 时必须为空向量、rows=0、estimate=0、Count>0、Runs 初始为空，目标列有效；表/epoch 继续由原 service admission 校验。`AutoIncrementRun` 为 `(First, Count, Step, AllocatedAt)`，不用不可表示的 exclusive end 描述类型末端。

| 状态/事件 | 所有者与动作 | 失败/下一状态 |
|---|---|---|
| 已有 shared cache 段 | 原 table cache acquire/release 与 column mutex；等待原在途分配时释放 mutex | ctx 取消/allocator 错误直接终止 |
| 消费 lease → request.Runs | 持列锁消费/推进 cache cursor；此点号码已不属于 cache | 部分成功后错误也不回收号码 |
| request → candidate stream | arbiter 将段复制进 mpool-backed stream，保留最老 TS | append 失败终止；所有已消费 lease 保持消耗 |
| 候选未接受 | 私有 stream peek，PK/UK/FK 依次判断 | 拒绝仍用同一候选；不发明新号码 |
| 接受生成行 | pop 一个合法候选，记录最终 PK/UK 与输出 | 后续容量/锁错误使整个语句失败，不回滚租约 |
| 接受正显式 ID | 必要时用既有显式 InsertValues 推进 allocator；discardThrough 保留私有下界 | 拒绝的显式 ID 不做此操作 |
| 输出批次完成 | 按 lease 最老 TS 检查历史 PK，发布本批 first-generated statement result | 检查发现并发占用返回既有 txn retry |
| Reset/Free/abort | 清除私有 run、下界、类型、TS、键集合、tuple、executor | 新执行是新 generation，不复用失败游标 |

三个点不能混淆：**allocator lease 消费**保证号码所有权；**arbiter 接受**只是语句内部键发布；**业务事务 commit**才是存储可见性。键集合更新中间可能失败，此时不能返回半批输出，原 txn rollback 和 operator cleanup 负责撤销数据/释放私有状态。

已提交表的 allocator 高水位沿用其既有独立事务，不受业务 rollback 回退；未提交 CREATE/private table 仍用原 owning txn，整个创建失败的既有例外不改变。业务重试重新 bind/admit/申请当前租约，不重放旧 Runs；可能留洞，不能把 retry 承诺成同一 ID。

VALUES 的首次请求使用 `ValuesRows`；未知流按 1、2、4…扩张，块上限 **65535**。剩余预算按到达 arbiter 的行维护，candidate 消耗只随接受发生。连续段合并，exhaustion compaction 不清除显式下界；只有 Reset/Free 清除。实际 allocator 仍按 CACHE/会话增量与 offset 选值，checked 加乘及 int 转换，unsigned all-ones 不作生成 ID；接近界限允许 reservation 缩短/饱和，但真实请求无合法下一值必须报错。

无进展分配至多沿用 **8** 次 guard，不新增无限重试。retire/epoch 更新返回 definition-change retry；旧 callback 不复活 retired cache。Wait-for 链仍为 caller → 原在途 allocator 完成或 ctx cancellation，不能把取消改成普通约束拒绝。

## 5. FK 可见性、锁与最终键

### 有序 FK 读取

1. 将待查 child tuple（必要时替换为最终生成 ID）绑定进完整 parent 等值 predicate；任意 child NULL 按 MATCH SIMPLE 通过。
2. parent relation 使用当前 txn、workspace snapshot-write offset。`Ranges`/reader pushdown 仅用于 pruning；每个返回批次必须再执行完整 SQL predicate，不能把“reader 返回了行”当父键匹配。
3. 逐个 reader 读到匹配或终止；ctx/Read/Eval 错误向上返回，所有已建立 readers 统一 Close，临时 batch/vector/executor 释放。
4. 只缓存单列生成 auto-ID 非 self-FK 的最后一次 `(value, exists)`，每个约束一项，避免同一拒绝候选反复读取；不是跨语句或无限值缓存。

self-FK 的查找顺序：完整当前行 child/parent tuple 用 SQL `=` 与 `and` 比较 → 同语句已接受 parent 集合 → txn 存储读取。当前行比对不发布键；多列不能只比较其中的自动列。通过所有 CHECK/PK/UK/FK 才记录 self-parent，所以拒绝的父行不能支持后继子行；后输入的父行不会倒流改变此前拒绝结果。此前接受行尚未写入存储时由语句集合提供可见性。

### 并发与锁闭环

pessimistic txn 对 ordered FK 父表按 TblId 排序、去重获取共享表锁，一次获取后复用，最长存活至原 txn lock owner 释放；optimistic txn 不新增这组锁，继续依赖既有 MVCC/commit 冲突协议。排序只约束这组 acquisition，**不保证整个 DML 图无死锁**；与其它事务/最终 PK 锁的冲突、升级、取消、deadlock victim 和 retry 交给现有 lock service/txn context。

共享父表锁是粗粒度代价，可能降低父更新/删除并发。它不能被文档包装成无成本 point lock，也不能把“sort”当作所有 wait 的释放证明。部分取锁失败时已取锁归原 txn cleanup，不由 reader.Close 释放。

generated PK 的旧输入为 NULL，不能沿用旧键锁。arbiter 完成最终行后追加 `LOCK_OP`，使用最终输出 physical ordinal；显式行保留此前的锁。原 UK 锁、约束优先级与 index-key remapping 仍保持，ID-dependent 生成表达式受 eligibility guard 排除。最老租约 TS 的 `PrimaryKeysMayBeUpserted` 检查覆盖可能已被另一个 CN 显式 writer 占用的旧租约，临时表沿用既有例外；缺少 txn/workspace 不伪造安全结果。

锁触发 snapshot refresh/retry、历史检查失败、输出分配失败均不能提交已接受的私有映像；重试/rollback 清理所有 maps，已消费 lease 不回收。不能声称仅看 LAST_INSERT_ID 或主表 count 就证明 index 与锁正确。

## 6. 精确 wire 契约与版本兼容

最低能力为 **MORPC 113**；112 已归 signed WEEK/DAYOFYEAR，不能复用。当前 schema 对既有 messages 是 **7 个字段**，不是 PR 早期正文中的六个；另有两个新 messages。

| producer / 字段号 | 内容与 consumer |
|---|---|
| plan.PreInsertCtx 15 / 16 | `check_eligibility` / `defer_auto_increment` → compile PRE_INSERT |
| pipeline.PreInsert 19 / 20 | 上述两字段的 operator wire → receiver PRE_INSERT |
| plan.PreInsertUkCtx 19 | `row_eligibility` → arbiter 输入资格列 |
| plan.PreInsertUkCtx 20 | `ordered_auto_increment` → table/epoch/column/VALUES 预算/FK |
| plan.PreInsertUkCtx 21 | `ordered_foreign_keys` → 无 AUTO_INCREMENT 的 self-FK arbiter |

`OrderedAutoIncrement` 字段 1–5：ref、table_def、column_index、values_rows、foreign_keys；`OrderedForeignKey` 字段 1–6：ref、table_def、child_columns、parent_columns、predicate、self_parent_columns。child/self positions 是最终物理输入位置；parent positions 是 parent TableDef ordinals，predicate relation 1 为待绑定 child tuple slot。Deepcopy、引用收集、pruning/remapping、encode/decode 必须同改；不能只追加 protobuf。

发送 encoding 及 receiver instruction admission 用 `validateRemoteInsertIgnoreEligibilityProtocol`；发送前递归 pipeline 校验再通过 `validateRemoteInsertIgnoreEligibilityDestination` 查实际 Node.Id/Addr。任一 metadata 存在即要求 113；未知/缺失能力或低版本拒绝，不能因为本机 latest=113 就推断 worker 也支持。缺失 metadata 的普通旧路径保持既有契约；格式错误由 operator validation/运行列界检查失败。

不新增 WAL/catalog/storage schema。这些字段是执行计划/临时 operator 数据，不能在旧 CN 上丢字段后继续执行，也不跨重启恢复 Runs/maps/prepared execution。降级不转换在途计划。

## 7. 资源、复杂度与安全预算

令 N 为待处理行、A 为接受行、U 为参与唯一键数、F 为 ordered FK 数、T≤F 为不同父表数、R 为当前租约段数、P 为一次查找最坏需读的父表行、W 为相关键/tuple 宽度。

| 资源 | 峰值/代价、owner、终止 |
|---|---|
| 私有候选段 | O(R)，非逐 ID 对象；R≤请求 Count。未知流 Count≤65535，VALUES Count≤已解析输入行数；mpool-backed stream 在 exhausted 时复用/compact，Reset/Free 释放 |
| 临时 request.Runs | Go heap O(R)，一次同步申请存活；部分结果仅由 caller 持有，**不宣称全部 Go heap 都已纳入 mpool admission**；不能把上述上界说成常数空间 |
| PK/UK 与 self-parent sets | O(A·U·W + A·F·W)，复用 hashAllocation/mpool admission，resize 峰值含旧+新容量；容量错误终止；Reset/Free 释放 |
| FK 固定缓存/tuple/executor | O(F·W + predicates)，末次 point cache O(F)；部分初始化立即挂到 operator，失败也能清理 |
| FK reader 工作区 | 一个 lookup 同步完成才开始下一 lookup；读批次遵循 engine 现有 batch/byte admission，Close + batch/executor Free；不跨行保留整个父表 |
| table locks | 至多 T 组本路径共享父锁，归 txn；ctx/deadlock/retry/txn end 终止等待与所有权 |
| 输出 | 一个正常批次 O(batch rows·row width)，借用输入不 shrink，最终 PK 只构造一次 |
| 分配 I/O | 按实际 cache/需求块摊销，拒绝生成候选不要求每行新的 allocator txn；显式接受仍可能有推进 I/O |

ordered FK 的最坏读取是 **O(N·F·P)**，不是保证 O(N·F) 的索引点查。完整 PK equality/序列化复合 PK 可帮助 pruning，但没有专用新 FK 索引，也没有每个 probe 的固定扫描行数上限；通过既有 query admission、deadline/cancellation、engine 批次预算终止工作，不静默跳过大型父表。父表锁生命周期与长事务相同，不只持续一行。

接受这些可测成本，而不是编造常数 I/O 或端到端提速。普通非有序 FK/path 不应被新增 probe/table-lock 覆盖；该 guard 是成本的第一隔离边界。运维容量评估必须包括 Go heap transient Runs、native hash resize 峰值及长 txn lock wait，而不是只报 B/op。

本期不新增每行日志、无界缓存、后台 queue 或线程。租户/身份/表权限沿用 binder 与 existing authenticated pipeline/process context；parent metadata 来自合法 FK 绑定，relation lookup 仍使用同一 txn/context，不新开跨租户 privileged SQL。执行元数据属于可信 compiler/远端执行协议，不是用户可直接提交的 SQL API；版本和列界拒绝不代替既有鉴权。

## 8. 上线、回退、恢复与观测

- 无新配置开关，planner 的 eligibility 与 113 capability 是准入。先升级执行相关 CN，再放行命中该路径的 workload；旧 direct CN 仍有旧语义，不承诺升级期间任意入口一致。可以先停止旧入口/定向到升级入口；不能仅改本机 latest 值冒充全量升级。
- 先运行本文矩阵、协议 rejection 和真实多 CN 功能检查，再对有 ordered FK 的代表性 workload 观察 allocator 请求数、reader rows/bytes、native/Go 峰值、锁等待/取消尾部。锁/读取代价超过既有资源预算时暂停该 workload 的扩大，不取消约束或降级成丢 metadata 路径。
- 启用后没有 rollback migration：停写并 drain 新执行/txn，结束 prepared contexts 后可回旧 binary；持久 allocator 高水位不回退。旧版本重新运行将恢复原 compatibility gap，必须明确是功能回退，不宣称同一结果。
- restart/backup/restore 只沿用已有数据和 allocator 持久高水位；未用 leased slots 和私有 maps 丢弃，允许洞。禁止把 transient stream 纳入备份或恢复后复用。无新的 schema/WAL format，但现有 AUTO_ID_CACHE/epoch 等独立版本要求仍适用。
- 复用 query/operator analyzer、allocator 请求、mpool/account 错误和 lock-service 诊断。没有新增专属生产指标；定位时保留 head/plan、版本、txn snapshot/lease TS、查询取消或 retry 类型。不能把错误重试后 count 正确当作无争用证明。

## 9. 确定性验证与证据分层

| 不变量/边界 | 最便宜的独立 oracle |
|---|---|
| reservation ownership/series/bounds | `TestStatementReservation`、`TestStatementReservationSessionSeries`、`TestStatementReservationConcurrentOwnership`；enumeration 对照整数界、step/offset、无交集 |
| cancel/retire/partial failure | `TestStatementReservationWaitLifecycle`、`TestStatementReservationPartialFailure`、`TestStatementReservationTypedTableBoundary`；barrier 注入、已消耗段不归还、准确错误类型 |
| CHECK 选行及借用输入 | `TestPreInsertDeferredCheckSelection`，零/全/部分资格、pool cleanup 与输入不变 |
| 最终候选/显式推进/溢出 | `TestOrderedAutoIncrementAllocation`、malformed context 控制；tinyint、session series、先拒绝后接受、across batches |
| FK reader 与 residual | `TestOrderedForeignKeyProbe`；返回行不匹配、NULL、read/range/build/eval error、reader Close |
| current self / accepted parent | `TestOrderedForeignKeyCurrentRow`、`TestOrderedSelfReferenceCompositeTuple`；完整 tuple、当前行、自根/后继、拒绝 parent、未投机发布 |
| logical→physical 与消费者 | plan owning UT、deepcopy/remapping、最终 LOCK_OP ordinal 与 indexes，共用接受行而非临时 ID |
| protocol | `TestInsertIgnoreEligibilityRemoteProtocol`：encode/decode、112 拒绝/113 接受、实际旧 destination、递归和独立 metadata |
| LOAD/prepared策略 | explicit JSON、两 binder TIME rebind/profile UT，既有 prepared/FK setting 失效矩阵；普通与 LOCAL/IGNORE 相邻控制 |
| SQL 行/事务/索引 | mo-tester `insert_ignore_constraints.sql`、`insert_ignore_reservations.sql`、`issue_25366.sql`、`external_time_assignment.sql`；准确值、错误/零提交、cleanup、同实例第二轮 |

共享 fixture 的各场景恢复 session/runtime/global 状态；资源成功获取后立即登记清理，FailNow/ctx cancellation 同样执行；不新增 sleep、skip 或大数据概率 witness。allocator shared-state 改动需 focused race（按测得耗时决定重复次数）及 owning race；跨 CN 要检查真实 final row/PK/UK/FK 和 lease 不相交，不只比 count。

### 可追溯的已知证据与缺口

- 实现 head `6bbac78364` 的 [CI run 38531158789](https://github.com/matrixorigin/matrixone/actions/runs/38531158789)：Ubuntu UT、SCA、PROXY BVT、pessimistic BVT、coverage、CI Required 成功；不是设计批准。skipped Darwin/upgrade jobs 不计通过。
- 相同 source tree 的本地 owning 8 包、静态检查、build 与上述 4 个 SQL 各两轮已通过（每轮 59/121/95/128），本地日志不在 Git 中；不能要求外部 reviewer 仅凭本机路径复核。
- PR 早期正文报告 allocator race 100 次、MySQL 8.4.11、16,384 输入的真实 two-CN 结果；它们没有在 reviewer 的冻结输入中携带 receipts，本文只标为**作者报告、待独立验证证据**，不升级为独立通过。
- 未部署旧版本 CN binary；混版现有证据是确定性 codec/admission rejection，不宣称真实 upgrade/topology 演练通过。
- 未提供该新 ordered FK 路径相对基线的吞吐/reader-I/O/锁争用 benchmark，旧 #28349 benchmark 不可移植为本方案性能证据。

上线接受条件：功能与 terminal/resource 矩阵非空且真实退出成功；变更代码覆盖率≥75%；所有真实失败可清理且无 partial publication；所需 race/真实 topology receipts 能被复核；mixed-version refusal 保持；allocator 仍按块请求，probe 工作区不会随 N 累积父表数据，pool/ctx 的容量和取消路径有效。另需为 ordered FK 的目标 workload 记录对照的扫描量、锁等待和峰值，明确接受其粗粒度锁成本；**在此证据补齐前不声明性能或全面生产 rollout 就绪**。

## 10. 实现索引与决策记录

- [reservation](../../pkg/incrservice/statement_reservation.go)、[unused saturation](../../pkg/incrservice/unused_reservation.go)、[table admission/lifecycle](../../pkg/incrservice/table_cache.go)：消费/transfer owner 与失败不回收。
- [planner eligibility/final locks](../../pkg/sql/plan/bind_insert.go)、[资格/ordered FK](../../pkg/sql/plan/insert_ignore_candidates.go)、[physical remap](../../pkg/sql/plan/query_builder.go)、[deepcopy](../../pkg/sql/plan/deepcopy.go)。
- [PRE_INSERT](../../pkg/sql/colexec/preinsert/preinsert.go)、[CHECK buffer](../../pkg/sql/colexec/preinsert/check_reservations.go)、[row acceptance](../../pkg/sql/colexec/preinsertunique/preinsertunique.go)、[candidate/TS](../../pkg/sql/colexec/preinsertunique/ordered_auto_increment.go)、[FK reader/self](../../pkg/sql/colexec/preinsertunique/ordered_foreign_key.go)、[cleanup](../../pkg/sql/colexec/preinsertunique/types.go)。
- [schema](../../proto/plan.proto)、[operator schema](../../proto/pipeline.proto)、[三向 gate](../../pkg/sql/compile/remoterun.go)、[LOAD decoder](../../pkg/sql/compile/compile.go)。生成文件由工具维护，不手写 descriptor。

| 决策 | 理由/代价 |
|---|---|
| 共享 lease 不回滚；statement cursor 私有 | 保证跨 CN 独占，错误/重试留洞；不追求全局连续 |
| 只提前独立 CHECK，普通 FK 保持批路径 | 单次求值和来源不丢失，避免扩大 I/O blast radius |
| 有序 FK 完整 residual + 当前/已接受 self 可见性 | 不依赖 pruning 的假阳性；只缓存固定最后值与实际接受 parent |
| pessimistic shared parent table locks | 用既有并发保护换取写争用；不宣称更细粒度或绝无 deadlock |
| 最终输出后 PK lock + lease TS 检查 | 不锁临时 NULL、不制造未验证号码；失败使用既有 txn retry |
| 113 三向拒绝而非降级丢字段 | 混版不能 silent orphan/错误 ID 映射；没有新的存储 migration |

设计级阻塞：**本文精确修订尚待独立设计审查/PASS**。功能代码不能因补了一份文档就自动获批。benchmark/旧 binary 演练的证据缺口由 PR 作者与实现 reviewer 在 rollout 决策前处理；它们不是本文暗中承诺的成功结果。审查对所有权、性能预算或兼容提出新阻塞时，更新 r2 并重审，而不是沿用 r1 的批准。

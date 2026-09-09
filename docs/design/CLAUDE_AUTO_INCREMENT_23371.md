# #23371：AUTO_INCREMENT 增强设计

- 修订：**r4 + rollout-A/r1，2026-09-09，均经用户 go ahead 批准；实现、验证与最终自审 PASS；交付分支 `issue-23371-main`，目标 `main`**。
- 交付基线：`mo/main@3d22c694a8a3224c03a88f94327439304de23607`。完整验证基线为 `cd04bb4c1af5bc595e2147dc645dfa754f4c395b`；交付前再同步无重叠的 FIND_IN_SET/SET 修复，重新通过 plan/compile owning 与公开 lifecycle，其余未改变调用路径的证据复用。后续合并 `d3e8aced87`，上游 V58 已用于 binary-string/runtime-domain；本 PR 的 CACHE 门槛顺延为 V59，字段/opcode 编号和 rollout 契约不变。
- r4 已替代原 P1/P2 重实现计划；CACHE 语法、typed 元数据、分配策略与[已批准方案 A 发布边界](CLAUDE_AUTO_ID_CACHE_ROLLOUT_23371.md)已实现。公开双 CN、dump/load、BVT、关闭节点与全集群重启均有通过证据。
- 范围始终为 [#23371](https://github.com/matrixorigin/matrixone/issues/23371)，不合并 #28237/#28238/#28239 的独立修复。
- 归属实现 PR：`ck89119:issue-23371-main` → `matrixorigin/matrixone:main`，关联 #23371；Draft PR 正文链接本设计与 rollout-A/r1 的确切提交修订。TODO 不提交。

## 1. 结论与范围变化

**保留“共享独占号段 + 语句会话序列”的架构，复用新 main 实现，主要新增 AUTO_ID_CACHE 表选项及其持久化/生命周期闭环。**

| #23371 需求 | 当前 main | 本任务剩余工作 |
|---|---|---|
| ALTER TABLE AUTO_INCREMENT | #26658 系列已有 SQL/MAX/epoch fence | 复用；验证与 CACHE 组合，不重写 reset 协议 |
| session increment/offset | #28349 已加入 DML 快照、区间对齐、远端 V56，移除 CREATE/COPY 的 session offset 固化 | 复用源码和现有用例；补 CACHE 与两类 session 的组合验证，不重复新增快照/算法 |
| AUTO_ID_CACHE | pkg/proto/BVT 尚无对应实现 | 本期主要生产改动：语法、typed metadata、冷加载、预取/按需预算、展示/复制/恢复 |
| 基础显式/自动兼容 | 上游已修负数误转 uint64；全自动主键历史探测存在 | 验证新 CACHE 不破坏显式推进和查重；其余失败须先证明与 #23371 的具体关联，不 blanket 重构 DML |

### 用户已澄清的范围边界

- #28237 REPLACE 结果发布和 #28238 IGNORE 行映射不属于本任务。即使 #28349 已包含这些修改，也只作为上游依赖/回归控制，不重新修复或宣称本任务完成它们。
- #28239 的会话复现用于核对 #23371 原始需求；不单独追踪/关闭该 issue。
- #28349 描述使用了 Closes 三个 issue，但本次查询三个 issue 仍为 OPEN。以源码确认已有能力，不据此擅自更新 GitHub 状态。
- 上游公开承认的“IGNORE 被拒绝的正显式大 ID 提前推进/溢出”继续留给后续工作。本期不引入 acceptance cursor、不移动 dedup、不回滚共享分配器。
- 默认不扩展分区、charset/collation、存储过程、UDF、warning 兼容。

## 2. 上游证据与不可再沿用的 r3 假设

已完整读取 [上游 AUTO_INCREMENT 设计与限制](20260908-auto-increment-boundaries.md)，并核查相关生产代码及合入差异。

| 新边界 | 证据 | 对本设计的约束 |
|---|---|---|
| 语句参数已存在 | `incrservice/types.go:AutoIncrementOptions/WithAutoIncrementOptions`，frontend 每语句刷新，process SessionInfo/codec | 沿用 context 携带的 typed options，不为同一需求再修改整个 public service API |
| 区间对齐已存在 | `range_cache.go:nextFor`，unit-step 快路径与 legacy 非 unit congruence 算法 | 撤销 r3 的拒绝非 1 step 计划，不替换 solver |
| 非默认序列关闭预取 | `column_cache.go:preAllocate/maybeAllocate` | CACHE>1 也不能重新开启非默认 session 的背景预取 |
| 两类写入共享基础跨度 | `column_cache.go:allocateLocked`，`autoIncrementAllocationCount` | CACHE 必须作用于最终有效 Config，不能只修改 async prefetch 分支 |
| 主键检查仍先取 TS 后发号 | `preinsert.go:genAutoIncrCol` | CACHE=1 去掉冷构建预取后，必须重新闭合检查窗口，不能使用未知 TS 假装精确 |
| 结果发布可被延后 | `TrackAutoIncrementGenerated`，preinsertunique 已接受行边界 | 本任务不得无条件提前发布 LAST_INSERT_ID，不能绕过现有仲裁与索引共用最终行 |
| 会话协议使用 V56 | `defines/const.go`，compile remote validation | 复用会话 V56；新 CACHE 元数据能力单独门禁，不能假设 V56 认识新字段 |
| PK I/O 取消已修复 | #28361，`PKPersistedBetween(ctx,...)` | 传递语句 ctx，保留取消为终止而非冲突的语义，不增加查重 SQL fallback |
| 临时 DDL 所有者变化 | #28324/#28332/#28356，`compile/temporary_ddl.go` | 顶层临时 CREATE 与内部 COPY 的事务归属不同，CACHE 需跟实际物理表 metadata 走 |
| TRUNCATE 已隐式提交 | #28414；#28374 保留 cluster table 默认 | 保留 CACHE，不把 TRUNCATE 写成可由外层 rollback 撤销的 DDL，不重新构造丢失其它属性的 table definition |
| 输入向量新增分配优化 | #28405，vector known-source append | 不加逐行 provenance 对象/反复 sidecar 归一；需要生成来源时使用既有 metadata，保持批量成本 |

原 P1 算法、P2 会话快照/codec、CREATE/COPY 去 session offset 的步骤已经由上游实现，删除本任务重复实现项。上游测试报告仅为历史证据；本任务当前基线的独立验证见自审记录和当日 TODO，不用上游结果替代。

## 3. SQL 契约（保留 r3 的 CACHE 选择）

参考：[TiDB AUTO_INCREMENT](https://docs.pingcap.com/tidb/stable/auto-increment/)，[MySQL 会话序列规则](https://docs.oracle.com/cd/E17952_01/mysql-8.4-en/replication-options-source.html)。

- 语法：`CREATE TABLE ... AUTO_ID_CACHE [=] unsigned_integer`。本期不新增 ALTER CACHE 状态机。
- 0/省略：继承 CN 已有 CountPerAllocate/LowCapacity；保持当前 main 的默认分配策略。
- 1：关闭建表、只读 CurrentValue 和 batch 结束时的投机预取，按当前请求真实自动行需求分配；不是全局单 owner 发号，也不承诺全局单调/无间隙。
- 2～1000000：覆盖基础原始数值跨度 C；大批次可以按需扩大。不是内存容量，也不是保证生成行数。session stride 只决定区间内选值。
- 负数、非整数、超上限、重复选项拒绝，不静默截断。
- 非零选项要求普通/临时表有用户可见自增列；无此列时 CACHE=0 可归一为默认。ALTER COPY 在所有条款处理完后，若最终目标已无可见自增列（MODIFY 移除属性或 DROP 移除列），将继承策略归一为 0；仍有可见自增列则保留原策略。直接 CREATE 的非法组合仍拒绝，SHOW 不隐瞒持久属性。
- 正数 SHOW CREATE 稳定呈现；0 省略。与 AUTO_INCREMENT=N 同时生效，N 是原始下界，实际候选继续由现有会话序列算法选择。
- 沿用当前 main 的 O>S 归一为 1、零/NULL/sql_mode、显式正数推进、负数不推进的行为，不重新定义它们。
- session 参数仍不是 catalog 属性；CACHE 才是表级持久策略。内部索引表不无条件继承用户表 CACHE。

**仍保留 r3 明确的产品边界：CACHE=1 不等价于 TiDB 特殊全局单调模式。** 如需该模式必须另行设计；不能通过缩小默认 CountPerAllocate 谎称实现。

## 4. 不变量与所有者

1. 全局号段由持久 allocator 高水位事务独占，跨 CN 不重叠。修改 C 不改变取号线性化点，不回收业务回滚的保留区间。
2. 会话 S/O 只读选择候选，不改 `AutoColumn.Step` 或共享 cache 的序列定义；legacy 非 1 step 继续使用上游算法。
3. CACHE 的唯一持久所有者为表 metadata，CREATE/冷 CN/重启/LIKE/COPY/CLONE/dump/restore 的读取一致。
4. 默认 0 的 SQL 行为、预取与发号热路径请求次数保持 main 行为；冷加载在同一 SQL 快照额外读取已有表属性，不增加 executor 往返，但不宣称该 catalog 读取成本为零。新策略不引入 session cache、worker、广播、retry 层。
5. CHECK、PK/UK 锁、base/index 共用最终行、上游结果发布边界保持；CACHE 不能制造未探测/未锁定的新候选。
6. TS 随实际保留区间保存，查重下界不晚于本批任何候选来源；末端值、skipped、跨段与错误清理同样成立。
7. 加法、乘法、int 转换、末端值均受既有边界检查；新预算计算不能在已有 checked helper 之前溢出。
8. 旧回调不能复活退休 cache；缓存替换继承既有 acquire/release/builders/epoch 机制，不新增 generation 所有者。

## 5. 元数据闭环

- 使用 `api.SchemaExtra` 新增 `uint64 auto_id_cache`，0 表示继承；`plan.TableDef` 具有对应 typed 字段。原始 createsql 或普通未知 property 不能成为唯一持久来源。
- 路径：grammar/AST → plan → engine PropSchemaExtra → TN schema/WAL/checkpoint → logtail catalog → 冷 TableDef。
- 更新手写 deep-copy、SHOW CREATE、LIKE、COPY、CLONE、dump/restore；无关 ALTER 与 TRUNCATE 保留配置，不重置其它 schema 属性。
- incrservice 构造必须获得 allocator columns 与表策略，CREATE/私有 reset 读本事务 metadata，committed 冷加载读已提交 metadata。避免只在 CREATE 的本 CN 生效。
- 优先将 cold metadata 读取收敛为一次同事务的 typed table state，不新增 allocator 表列、策略表或 per-row catalog 查询。是否调整 GetColumns 的返回类型在该闭环 implementation map 中冻结；不为会话参数再改它。
- CACHE CREATE 与 allocator 状态同一实际建表事务；临时表使用新 main 的独立 DDL owner，内部 COPY 沿用 executor transaction。外层 rollback 后的可见性按各自 DDL 契约验证。
- 所有 parser/proto/mock 由仓库工具生成，字段编号和协议版本以实施时最新基线分配。

## 6. 分配策略接入

### 6.1 默认与非默认 session 两条路径

| 表策略 | 默认 session | 非默认 session |
|---|---|---|
| C=0 | 完全继承现有构造预取/低水位/按需逻辑 | 保留按需逻辑和现有 CountPerAllocate 最小跨度，禁背景预取 |
| C=1 | 禁止构造/低水位/估算投机预取，按自动行需求取号 | 同样只按需；不被全局默认 10000 或并发放大覆盖 |
| C>1 | 用表级有效 Config 控制基础跨度和低水位，保留单列单预取在途 | 基础跨度为 C，实际需求较大时扩大；仍不背景预取 |

- CACHE=1 在 rows=0/全显式时不取新号段，仅保留必要的显式高水位推进。
- 使用实际待填自动行数和上游 checked span helper；缓存不足才申请。估算行数不是跨越表策略的通行证。
- 0 默认路径不借机重构全部缓存布局。显式策略的预取和需求增长要有预算；不得引入按行大小分配的 ID 数组。现有端点/TS 表示和锁 owner 保持。
- 默认路径和非默认路径对窄类型、uint64 terminal、legacy step、mixed explicit 采用同一现有发号器，不能复制第二套 arithmetic。

### 6.2 CACHE=1 的 CurrentValue 与查重窗口：实施前必须闭合

当前冷构建会预取，`current()` 无段时返回 0；`genAutoIncrCol` 在 InsertValues 前取得最早 allocation TS。单纯禁预取会同时影响 SHOW/CurrentValue 的意义和历史探测下界。

因此 CACHE=1 不能只有一个 `if count==1`：

- CurrentValue 需要不预留号码的 metadata 读取语义，不能把空缓存的 0 当成“无自增/耗尽”，也不能为 SHOW 再隐式申请号段；只能声称观察到的下一原始下界，不保证跨 CN 的下一次发号。
- r3 提出的 typed allocation result（实际消费段 TS）是可选解决途径，但**不再预设一定扩大 InsertValues API**。先核对能否复用既有保守窗口而不退化到全历史扫描，或由同一 cache 获取原子地返回候选与最早 TS。
- 若需要结果 API，必须保持现有 context options 和 deferred LAST_INSERT_ID 发布；不引入新逐行来源对象。skipped 和 terminal TS 同步保留。
- 不允许用“当前时间”填补未知来源，也不允许把取消转为 retry 冲突。

2026-09-09 边界核查决策（详细步骤见根目录 `CLAUDE_TODO_2026-09-09.md`）：

1. 不扩展 InsertValues；有剩余段继续使用上游 oldestAllocateAtLocked，含 terminal/skipped 来源。CACHE=1 有可用段但 TS 未知时 fail closed。
2. 持列锁观察无可用段时，无投机分配在途；调用事务 SnapshotTS 可作为后续新保留的保守下界。新共享保留在该快照之后提交，私有保留采用 owning snapshot。缺少有效 snapshot 时拒绝，不使用零或 wall clock。
3. CurrentValue 有本地游标/terminal 则直接读；无段时由实际 table cache owner 执行 SQL 读取 offset+step，checked overflow，不用创建 cache 时的陈旧 offset，不触发分配。未提交 CREATE 使用该 cache 的 txnOp；commit 后 owner 清空为 nil，使用已提交快照。lazy private wrapper 转交到同一实际 owner，并在整个查询期间保留 acquire/release；不持 cache 锁跨 SQL I/O。
4. mixed batch 的自动行真实需求在 manual 位移之前同步保留，使 `(NULL,100,NULL)` 的前一自动行仍能沿用上游 skipped-range 顺序。全显式不预留，estimate/low-water 不参与。
5. cold GetColumns 同 SQL 快照读取 allocator rows 与 SchemaExtra；Reset 的旧 catalog 行可能已被 TRUNCATE 删除，因此该次读取明确指定新物理表为 metadata owner。无新增 public service/store API。

## 7. 兼容、失败与资源

- 会话沿用 V56 gate；CACHE 使用已批准方案 A：缺省关闭，非零策略只允许在受控全量升级后显式开启。0 的旧表和缺失字段继续默认行为。
- CACHE 使用新 V59；V56/V57/V58 保留上游原含义。追加 `PreInsertAutoIDCache` wire-only opcode，不改变已有 opcode 数值；接收端先验证，再还原为普通 PRE_INSERT。
- `SetupServiceBasedRuntime` 的本机 latest 不是 CN/TN 最低版本证明。V59/新 opcode 只补充局部及 PRE_INSERT 传输拒绝；旧 TN/旧直连 CN 必须由运维先停止，混合版本启用/运行和旧节点重入均不支持。该约束已经由用户批准，不增加平台 admission。
- 保留 ALTER exact-TN epoch-fence，不拿一般最低协议版本取代它。
- 重启丢弃未用段，从持久高水位重建配置；降级旧二进制不承诺保留新策略，需停写后兼容逻辑导出或恢复升级前备份，不降低高水位。
- account context/CREATE 权限/GLOBAL SET 权限走现有链路，冷 metadata 查询仍按租户隔离。CACHE 上限在 parser/plan 校验。

| Q1～Q3 | 要求 |
|---|---|
| metadata Result 所有权 | 每次 SQL 结果关闭；失败不发布半构造 cache |
| cache retire/close | 复用单 owner/reference lifecycle；旧预取回调不得重新发布 |
| 等待终止 | 保留 allocator ctx/队列/close-drain；无新增 service.mu 下 I/O；PK 检查使用语句 ctx |
| 增长预算 | 配置为标量，号段为端点；无 session map/逐 ID 对象；新显式策略预取保持单个在途 |
| 错误/回滚 | 未成功输出的 batch 不发布结果；业务 rollback 不回收共享保留段；TRUNCATE 不错误套用事务 rollback |

## 8. 验证地图与实施顺序

| 闭环 | 风险 | 新证据 |
|---|---|---|
| grammar/AST/plan/metadata | R3：公开语义、持久化 | 合法/非法/重复选项、typed roundtrip、deepcopy、SHOW |
| cold table policy | R3：跨 CN/事务/restart | 新 CN 冷加载、CREATE 失败/rollback、私有 reset 与已提交策略隔离 |
| CACHE 两路预算 | R3：共享热点 | C=0/1/2/上限；S=1/3/65535；小 batch、全显式、溢出、交错 writer、legacy step |
| 无预取观察与历史检查 | R3：正确性/扫描成本 | CurrentValue 不发号、冷/跨段/skipped/terminal TS、陈旧范围与取消 |
| metadata 生命周期 | R3：DDL/恢复 | ALTER reset/MAX/rename/COPY、LIKE/CLONE、TRUNCATE、临时 DDL、dump/restore |
| 上游消费者兼容 | R2/R3 按实际改动 | 复用 #28349 现有公开测试，保持 deferred result 与 base/index 一致；不新增相关 issue 的修复用例 |

1. review r4 收敛范围，先解决 6.2 的 API/观察语义 blocker。
2. 删除旧 P1/P2 重复开发，复用已有 arithmetic/options/session/codec。
3. 完成 CACHE parser/metadata 的 typed 闭环，随后接入有效 Config 和 cold loading。
4. 完成 CACHE=1 的无投机分配、观察与历史窗口；只作必要的消费者改动。
5. 验证配置复制/恢复、两类 session、多 CN、new gate；最终 self-review。

### 测试与性能要求

- 使用现有 `TestColumnCacheConcurrentSessionSeries`、`BenchmarkColumnCacheStatementSeries`、`TestIssue28349AutoIncrementPublicPaths` 和 AUTO_INCREMENT BVT 作为基础，不重造 fixture 或复制其大批量 case。
- 新功能通常 1～6 行，小 C 确定性跨阈值，barrier 控制预取/退休，无 sleep/skip/弱化断言。
- C=0 的 allocation 请求数与 Go allocations 对照当前 main，不能回退到非默认小写入每行一次 SQL；C=1 允许明示的请求次数增加，不能因此宣称默认路径退化合理。
- `.result` 用 mo-tester 生成后审阅并正常比较；test-owned 实例检查清理并重跑。
- owning UT/race、public consumer、restart/mixed-version 必须与改动映射；上游作者历史结果不冒充本任务 exact-head pass。原生构建输入已有变化，重新走 CGo wrapper provenance 判定。
- push 前改动覆盖率至少 75%，self-review 无 blocker；未验证项记录 pending，不以 issue 标题或 PR 合入代替证据。

## 9. 修订记录

- r1：完整增强草案，识别 session 与共享计数器隔离。
- r2：曾误纳入 REPLACE/IGNORE 修复，用户否决，已废止。
- r3：恢复 #23371-only，用户授权；本任务尚未改生产代码。
- r4：#28349 等已在 main 实现多个原计划闭环，复用上游并聚焦 CACHE；恢复 legacy step 支持、保留新 DML 仲裁 owner、更新 DDL 事务边界。后经用户 go ahead 进入 CACHE 第一阶段实施；不执行额外 issue 修复。
- 2026-09-09 第二轮：再次 go ahead 后同步新 main，纠正 CACHE=0 无自增列的实现与设计不一致；双 CN SQL、67 条 BVT 两次比较、完整服务重启、schema/catalog UT 通过。
- 2026-09-09 第三轮：用户批准 rollout A 后实施默认关闭/远端 opcode 门禁（初版 V58，合并后顺延 V59）；增加公开 dump/load 与永久独占集群重启/禁用测试，补冷 metadata 错误回收、取消、terminal TS、AST owner 回收证据。默认 benchmark 对照、owning UT、各关键 race 用例100次及 owning race 通过，变更行覆盖率250/271=92.25%，最终自审 PASS。配置关闭不额外禁止既有的无号段分配 ALTER 起点维护。

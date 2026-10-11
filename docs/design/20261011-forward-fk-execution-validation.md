# 前向外键执行校验：正确性、等待范围与性能约束

状态：设计 v2，替代 v1；源码级候选方案，尚未实施或通过性能验收。v1 的“所有普通 CREATE 增加 G S”方案撤回。下面的锁冲突策略及目录查询成本尚须通过第 8 节门禁，不能把本文当作已经证明无退化的实施批准。

所属 issue：[#29852](https://github.com/matrixorigin/matrixone/issues/29852)。
基线：`4022729da24d71b082c22b52aaed3faba2b77ea5`，包含已合并的 #29837。
目标为生产悲观 RC；覆盖内部 DDL、现有 SI/乐观调用者及停机升级，不设计混合版本协议。

## 1. 硬约束与范围

已确认的故障：父表 CREATE 的计划保存旧 child 的前向 FK；执行前另一个事务 DROP child 并同名重建无 FK 的 child；执行器按名字找到了新对象，把旧 FK 写进去。main 和原 PR 都有相同调度的复现。修复契约是：**写入使用的 FK 声明、子表身份、列绑定必须来自同一有效对象代际，且验证到写入之间受相关锁保护。**

本轮增加三项同等重要的交付条件：

1. 普通 SELECT/DML 不增加工作；普通无 FK CREATE 不加入全局生命周期锁队列；scalar TRUNCATE 不重复准入、扫描或推进快照。
2. 不新增由局部热点向无关数据库/租户传播的锁等待，不排队升级持有中的锁，不引入无限重试。必须同时验证成功提交吞吐、尾延迟和失败率，不能把等待改成报错后宣称性能改善。
3. 大库 DROP DATABASE 保持已有整库处理路径，本修复不增加逐表 FK 校验、逐表全局准入、逐表读屏障或逐表重新规划。表数、索引数、FK 边数增长时，检查增量成本及混合负载影响。

“无死锁”在本次变更的可证明范围内，指新增锁请求不产生等待环，不依赖超时/死锁检测器补救本方案引入的锁序反转。任意用户多语句事务本来就可能构造行锁环；不能把有限测试或语句内排序当作整个数据库永不死锁的证明。

修复还需挑战同 ID 的 FK 变化、incoming 空→非空、计划中缺失的父表先被创建。后几项目前是契约反例，不能描述成全部已经复现的生产故障。不新增历史数据追溯验证、自动元数据修复或通用 DDL 调度框架。

## 2. 已有职责与源码约束

| 职责 | 所有者及处理原则 |
|---|---|
| catalog 布局、列序、动作来源 | `plan/build_dml_util.go:getFkReferredToWithCatalogLayout`，继续共用，不复制 SQL/解码 |
| FK 类型、列和父键绑定 | `buildFkDataOfForwardRefer` / `checkFkColsAreValid`，抽取必要纯函数供执行校验复用 |
| 元数据和物理定义互斥 | `lockMoDatabase` / `lockMoTable` / `lockTable`，复用相同键编码和锁服务 |
| 全局生命周期准入 | `admitForeignKeyCreate` / `admitLifecycleRC`，不扩大到无 FK 的 CREATE |
| 事务回退和重建 | `prepareRetryTransition` / `buildRetryCompile` 及 frontend 事务所有者，不在 FK helper 内循环重试 |
| 内部替换的准入复用 | 现有 `dropLifecycleReceipt`，不新建另一个凭据/状态机 |
| 整库删除 | `DropDatabase` / `dropDatabaseRelations`，继续复用 resolved relations 和已准入的内部 DROP |

代码已经提供可复用的同步点：ALTER ADD FK 会锁父表名，包括父表不存在的情形。CREATE 自身的目标表名也已有锁。前向声明应补齐这个契约，而不是让所有 CREATE 都进入 G。

三个不能忽略的限制：

- 原地 ALTER 不统一持有 G，因此 G X 不能替代 child 表名和定义锁。
- `mo_foreign_keys` 写入的 ID 字段可能为 0；物理身份取自实际关系，不能把 catalog 的零 ID 当身份验证。
- incoming 查询虽按父名称过滤，但现有主键不是父名称前缀；最坏可能扫描租户 FK 总量 M，不能称作常数点查。

## 3. 选择与明确否决

采用：**保留既有计划语义，在执行写入前，利用目标表名保护引用发布，验证当前完整绑定。变化交还既有事务/重试所有者。**

否决以下做法：

- 普通 CREATE 新增 G S 或统一 G X：扩大与无关 DROP、TRUNCATE、快照操作的联系。
- 持有 G 后无限等待新引入的 child/parent 锁：把局部争用传播给全局。
- 全部锁一律 FastFail：既有正常等待会变成大量失败，不满足性能目标。
- 用超时变长、重试次数增加、失败请求不计入统计掩盖退化。
- 为本修复增加 FK 缓存、schema epoch、持久注册表或专用索引迁移。
- 仅核对 ID，或仅遍历计划里已有的 FK：漏掉同 ID 语义改变以及新引用。

将 incoming 绑定完全从 planner 移到 executor 可以避免两次扫描，但会影响 PREPARE/EXPLAIN 的报错时机、计划消费者及内部 CREATE。它目前只是性能门禁失败时要评估的替代方向，不预先引入第二条执行路径，也不声称已经设计完成。

## 4. 最小计划与执行数据

- `ForeignKeyInfo` 增加 `child_table_id`（拟用字段 6），并填充已有 `cols`。用于核对子表代际、列名/顺序以及已解析列 ID；不复制完整 TableDef。
- `CreateTable` 增加 `unresolved_fk_parents`（拟用 repeated ObjectRef 字段 16；14 reserved，15 RawSQL），表达出向 FK 对尚不存在父表的依赖。已有 materialized `FkDbs/FkTables/Fkeys` 不变；不要解析 `UpdateFkSqls` 来猜依赖。
- 执行校验产物为本次调用内的计划条目与已验证 `engine.Relation`，不修改缓存/共享计划，不跨事务存活。
- LIKE/CLONE、PREPARE、proto/deepcopy、内部 CREATE 均须完整传递。停机升级后重新生成计划；不为缺失身份加按名字猜测的兼容回退。

父表新 ID/列 ID 仍在现有实际 CREATE 后填充。前向执行循环改用已验证关系，删除写入阶段重新按名字挑选 child 的旧路径。

## 5. 生产悲观 RC 协议

C/G 为现有生命周期身份/全局锁；D 为数据库名锁；T 为表名锁。锁都由原事务拥有。

### 5.1 先锁域，后验证，再写入

1. 计划确定会发布 FK 的 CREATE 保留既有 G X 准入。无 FK 计划不获取 C/G，使用已有目标 D/T 锁。
2. CREATE 发布出向 FK 时，补齐全部父表名锁，包含尚不存在的父表名；并验证存在/不存在预期。这样 CREATE 与已有 ALTER ADD FK 都在同一个父名称上序列化。对象不存在也不能省略名称锁。跨库依赖使用对应 actor 的 D 键。
3. incoming 非空时，在写入前取得每个不同 child 的名称锁与物理定义锁；保持 `changeDef=true` 的定义变更通知。锁域去重、同类键排序，但排序只是降低冲突，不是跨语句死锁证明。
4. 新增的依赖锁不得在持有 G 后形成阻塞队列。候选策略是对这些新增请求使用请求级 FastFail，冲突走事务终止；不更改 process 的共享等待策略。现有 metadata admission API 有请求级策略；物理定义锁入口需要小幅透传该参数，不能另写锁实现或丢失定义通知。
5. 所有相关锁取得后，在任何 CREATE 写入前使用既有 TN applied-frontier 屏障，重新解析实际关系。锁授予时间戳不能当作 TN 屏障，早先 frontend 缓存不能当作实时定义。
6. 对目标完整 incoming 集合做一次执行校验，包括计划为空的情况。先对比参与者/声明，再对已有锁域中的关系验证身份、列和父键。新出现且未锁的参与者不能直接加入执行列表。

校验必须早于 `dbSource.Create`、catalog SQL、约束更新及索引表 ID 分配。`IF NOT EXISTS` 保持已有的无副作用行为。

### 5.2 负向证明

当目标 T X 已取得，所有新增 FK 声明的 CREATE/ALTER ADD FK 都必须先取得同一个 T。最终 TN 读屏障后查询为空，才能证明本次创建期间没有外部待绑定 FK。

删除/更改已有声明仍要受对应 child 的锁保护。RENAME、COPY ALTER、restore/内部写入等其他 catalog producer 必须逐一核对：若可能把原来不属于该父名称的声明移入集合，也必须受同一父名称边界约束，不能仅凭两个公开入口推定闭环。源码已确认的 producer 分类如下，实施前仍要把各内部调用者映射到这些入口：

| producer | 对同步边界的要求 |
|---|---|
| CREATE / ALTER ADD FK | CREATE 补缺失父名称依赖；ALTER 已有父名称锁，复用该语义 |
| RENAME TABLE | `getSqlForRenameTable` 同时改 child 名和 refer_table_name；现有 ALTER 锁旧/新名称。父名称改写必须被这两个名称锁覆盖 |
| COPY ALTER | `GetSqlForTransferAlterCopyFk` 只转移 child 名，明确不改父名称；外层 source/replacement 锁域必须覆盖转移，内层不得自行重试 |
| RENAME/CHANGE COLUMN | 改列名不新增父目标，但会改变已有绑定；通过 child/parent 定义锁和最终语义校验保护 |
| DROP TABLE/DATABASE | 删除声明不凭空产生 incoming；已有成员的删除/同名替换由 child 锁和最终集合检查保护，不向整库删除追加新扫描 |
| restore / clone 等内部 DDL | 必须复用上述生产入口与外层事务；snapshot 的 system catalog restore 列表跳过 mo_foreign_keys，不能据此推定所有恢复入口已经审完 |


若原无 FK 计划在最终检查中发现 incoming 非空，它已经持有低层 D/T：**不能直接返回普通重规划错误，随后排队请求 G X。** 候选处理为写前返回既有 `ErrLockConflict`，让事务所有者完整回退，再由调用方重试。必须在真实 frontend 和 internal executor 验证终止所有权；不能假定函数返回就释放事务锁。源码对此已有明确区分：frontend 的 whole-txn 错误集合包含 `ErrLockConflict`；`sql_executor.go:txnExecutor.rollback` 在 `ExistsTxn()==true` 时只返回错误，并不会回滚借来的事务。因此内部路径必须把错误送达真正的外层所有者，禁止吞错/改成普通语句失败后继续；验证必须覆盖这个分支。

这会增加特定 CREATE 竞态下的可见冲突，是明确的行为成本。若现实并发负载下频繁发生，则方案不能通过验收，应调整准入/绑定边界；不能靠自动无限重试掩盖。

### 5.3 完整绑定和重试

比较子表库名/表名/物理 ID、约束名、按 constraint_id 排列的子列和父列、动作及其来源、原有选键规则产生的绑定。集合遍历顺序无关，复合列顺序有关。

对已锁 child 使用实时定义，复用唯一的 planner 绑定规则；不能只比较 version，也不能自行实现第二套类型规则。新声明与 SQL 本身不兼容时返回原语义错误；读取失败/取消不能被当作空集合。

在锁域及全局准入仍适用、且尚无写入时，绑定代际变化可使用 `ErrTxnNeedRetryWithDefChanged`。需要扩大锁层级、写后刷新或内部替换已经有副作用时，必须由拥有外层事务的路径终止，不能局部重试。

本协议不增加新的阻塞依赖边，但 FastFail 对进展的代价必须单独测量；不能据此声称永不饥饿。既有 G 准入或既有 DDL 锁链是否本身有问题，与新增边区分取证。

### 5.4 必须手工推演的等待图

| 调度 | 候选协议的处理与验收点 |
|---|---|
| A 持 G，B 持 child T；A 修复 child，B 随后请求 G | A 的新增 child 请求不等待；必须验证 A 事务及时释放 G，B 能推进 |
| ALTER 持 child T 请求 parent T；CREATE 已持 parent T 请求 child T | 新增反向请求不排队，不能靠死锁检测解环 |
| 无 FK CREATE 持 parent T，最终发现引用；另一个事务持 G 请求该 T | 前者不能带 T 排队升级 G，按完整事务失败处理 |
| 显式事务前一条 SQL 已持其他表的锁 | 不以本语句排序证明安全；检查实际保留锁和终止后的释放 |
| 正常 DML 长时间持 child 数据锁 | 检查 FK CREATE 的失败成本及无关库 DROP 的进展，不能只测快速失败耗时 |
| 持续读/写与有限 FK 发布竞争 | 检查完成率与等待/失败分布，不以有限实验宣称无限流量下公平 |

原子可见点仍是事务提交：父表、FK catalog、子约束和父反向边一起提交或回退，无提前发布标志。

## 6. 内部替换与大库 DROP

### 内部 CREATE

scalar TRUNCATE 复用原 FK-free 证明和目标锁。通过现有 receipt 做 actor/TxnOperator/名称/操作匹配，内部 CREATE 不新增准入、目录扫描或快照推进。旧物理 ID 只表示来源，不能要求已 DROP 的旧关系仍存在。

broad TRUNCATE/COPY ALTER 的新增 FK 依赖准备必须在外层第一笔写入前完成，使用同一校验函数及 receipt；内层不另开快照、不扩域、不单独重试 CREATE。凭据仅在同步调用内有效，退出失效。若复用需要维护第二份独立状态机，应返回设计评审。

### DROP DATABASE 的专门约束

现有 `DropDatabase` 先取得整库生命周期域，收集 `resolvedRelations`，再由 `dropDatabaseRelations(..., lifecycleAdmitted=true)` 调用已准入的内部 DROP，并统一处理后续 catalog/branch 清理。保留这个职责划分。

本修复不能：

- 将数据库删除展开成 N 次完整公开 DROP/CREATE 准入；
- 对每张表再运行一次 incoming 目录扫描或强制 TN 屏障；
- 每删一张表重扫剩余表、整个 FK 图或租户 catalog；
- 把所有 FK 连通分量无限展开成整库/跨库锁域；
- 为方便大批量锁把精确名称锁升级为整张系统目录表锁。

不声称现有 DROP DATABASE 本身只有一次扫描或整体必然 O(N)：它已有 FK 清理、索引/branch 和对象删除成本。本次要求是**不引入新的按表重复的工作，不改变已有增长趋势**；原有瓶颈依据 profile 单独归因。

还要测间接影响：其他数据库正在等待 child 的 CREATE 不能因为本修复新增的全局锁持有而拖住大库 DROP。即使 DROP 文件零改动，也不等于已经证明无退化。

中途取消/出错时，验证整个数据库删除回退、锁释放、原表可用、之后能重试；不能只检查返回了错误。

## 7. 成本、兼容与清理

令 U 为相关不同子表数，F 为相关 FK 数，C 为引用列数，M 为租户 FK catalog 总量，N 为被删库表数。

| 路径 | 增量预算 |
|---|---|
| 普通 SELECT/DML | 无新增查询、锁、每行校验 |
| 普通无 FK CREATE | 无新增 G；一次最终 incoming 校验及必要读屏障，成本必须测量 |
| FK CREATE | 相关 D/T/定义锁去重，最多按 U 处理；目录按父目标读取，不能逐 FK 扫描 |
| scalar TRUNCATE 内部 CREATE | 不新增重复准入/扫描/屏障 |
| DROP DATABASE | 不调用新增 CREATE 校验；不新增随 N 增长的准入/扫描/屏障 |

验证集合的内存按 U/F/C 增长；名称排序 O(U log U)，实际目录查询最坏仍可能 O(M)。锁等待阶段的查询、RPC、持锁时间都计入成本，不能只数函数调用次数。

当前方案保留 planner incoming 读取，因此通常新增一次执行读取。**这里尚未证明满足 CREATE 性能预算**；不通过时必须调整查询/绑定归属，不能凭“DDL 低频”豁免，也不能未经收益分析先加缓存/索引。

SI/乐观事务不调用 RC 快照推进；已有语义仍需控制测试。停机升级不做持久格式迁移。旧/新 FK catalog 布局继续由现有适配器处理。请求 actor/事务贯穿读取、验证和写入；全局锁的 SYS 身份不能泄漏到租户 catalog 查询。临时同名表不能成为持久 FK 的绑定对象。

必须同步清理：晚期按名字选择 child 的旧执行路径、重复取同一 child 的锁/定义、失去调用者的 helper。保留真正有消费者的兼容布局，不借本修复做全库整理。

## 8. 验证与通过条件

### 正确性和终止

复用现有 authenticated 2-CN fixture；用 observable waiter 或已有注入 seam 控制顺序，不靠 sleep 猜调度。

最小反例组：旧 child 同名替换；同 ID 声明/列变化；incoming 空→非空；missing parent 先创建；跨库/同事务先 child 后 parent；复合列/多 FK；PREPARE 和 scalar/broad 内部 CREATE。注入父创建后及首个 child 更新后失败，验证全部元数据原子回退。覆盖锁等待取消/超时、临时表遮蔽和租户隔离。

每个错误路径都验证拥有事务的终止、锁释放和下一次成功；仅 helper 返回指定 error 不充分。既有诊断 red 不是候选实现通过证据。

### 性能和大库 DROP

与上述 main 基线使用同机、同构建参数、同数据/拓扑做交替 A/B 测量；先 A/A 测噪声。初始化和重建数据库不算进 DROP 计时，但取消/回滚成本单独报告。

| 维度 | 有区分力的负载 |
|---|---|
| 库大小 | 100、1,000、10,000 表，资源允许再扩；用实际物理对象数记录隐藏索引表 |
| 库形态 | 无 FK；库内 FK 链/星形；合法跨库引用；有/无历史、branch、PITR 的实际支持组合 |
| 删除中并发 | 同库有锁长事务；其他库/租户 SELECT/DML、CREATE、TRUNCATE；child 热点与 FK 发布 |
| CREATE 成本 | 无 FK 快路径、不同相关 U/F、固定 U/F 增大无关 M、持续而有限的并发写 |
| 终止 | 等待时 cancel/deadline、删除中途失败、失败后重新执行 |

报告成功提交吞吐、p50/p95/p99、错误率及错误类别、每个成功操作的尝试数、锁等待、RPC/查询数、读行量、CPU 和峰值内存。尾延迟的样本不足时报告样本数及原始值，不制造精确 p99。

交付门禁：

1. 无新增等待环/无关库级联阻塞；重复观察到的未完成请求必须解释和修复。
2. 普通 DML/独立 scalar TRUNCATE/大库 DROP 无可重复的性能退化；不能以降低成功率换吞吐或延迟。
3. 大库 DROP 的新增逐表准入、FK 扫描及屏障次数为零；N 扩大时增量不能形成 N×M 或 N² 趋势。
4. CREATE 的新增查询和冲突必须有明确预算证据；相关热点下失败率/完成率显著恶化即拒绝方案，而不是调大 timeout。
5. 统计波动通过 A/A 和重复对照解释；任何稳定退化都需回到设计，不能预设一个允许豁免的百分比。

这些是待执行门禁，不是已完成测试。普通 owning-package normal/race、公开 SQL/BVT、增量 SCA 在实现后按变更闭包执行；文档修改不重复运行生产测试。

## 9. 实施前的剩余决策

当前 v2 解决了 v1 明确不合理的全局锁扩面，但以下事项必须先闭合：

1. 完整 catalog producer 清单，证明目标名称边界覆盖所有能新增 incoming 的路径。
2. FastFail 冲突在 frontend/internal executor 的实际事务终止与进展；验证它没有把 convoy 转成失败风暴。已有锁等待路径与新增路径分开归因。
3. 一次额外目录扫描和读屏障的成本；若无法通过 CREATE 预算，优先重新安排现有绑定工作，不增加持久状态。
4. 内部替换外层锁域及 receipt 复用能否保持小改动、没有新增逐表 DROP 成本。

结论：当前可采用的方向是目标名称同步、写前完整校验和既有事务所有权；不能直接实施 v1 的全局共享准入。v2 是带硬门禁的候选设计，不宣称已经满足“零死锁、零退化”。本轮为源码自审，没有启动 subagent，没有生产代码变更或新增运行测试结果。

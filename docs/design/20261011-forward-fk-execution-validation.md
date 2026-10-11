# 前向外键在执行边界的完整校验

状态：设计 v1，待实施；已依据源码完成自审，尚未通过修复后的运行验证。

所属 issue：[#29852](https://github.com/matrixorigin/matrixone/issues/29852)。
基线：`4022729da24d71b082c22b52aaed3faba2b77ea5`，已包含 #29837。
本设计的并发正确性目标是生产悲观 RC；兼容性检查包括现有 SI、乐观事务、内部 DDL 和停机升级。没有混合版本运行要求。

设计门槛：虽然由 bug 触发，方案涉及 planner/执行器边界、瞬态计划字段及 CREATE 锁契约，按并发与协议复杂度触发设计先行。本文完成的是方案，不是实施交付批准。

## 1. 问题和明确的目标

已确认的失败是：父表 CREATE 计划保存了旧子表的前向 FK；执行前，另一事务删除并同名重建了不带 FK 的子表；父表 CREATE 按名字找到新子表，把旧 FK 写进去。最终 `mo_foreign_keys` 无对应记录，SHOW CREATE 和 INSERT 却按该 FK 工作。此前 main 与 PR 的相同调度均复现。

修复必须满足：**只有当前事务在锁保护下确认仍然有效的 FK 声明和物理对象，才能参与本次元数据写入。** 名字相同、曾在计划中出现、获取了全局锁，都不单独构成有效性证明。

目标包括三个不可分割的反例：

1. 子表被同名替换，旧绑定不能落到新对象。
2. 子表 ID 没变，但 FK 被删除、重建或改了列/动作，旧计划不能继续写。
3. 计划中的引用集合为空，执行前出现新引用；或者缺失的父表在子表执行前已创建，不能永久漏绑定。

第 1 项有运行证据；第 2、3 项是从契约推导的必测反例，不在没有运行证据时描述为新的已复现故障。

保持既有 SQL 语义：`foreign_key_checks=0` 仍允许前向声明；本修复不新增对历史存量行的追溯校验，不自动修复已经损坏的元数据，也不引入 FK 注册表或新的重试框架。

## 2. 已有职责归属

| 职责 | 当前所有者 | 本次使用方式 |
|---|---|---|
| 读取前向声明、列顺序、动作来源、选择父键 | `plan/build_dml_util.go:getFkReferredToWithCatalogLayout`；`plan/build_ddl.go:buildFkDataOfForwardRefer` / `checkFkColsAreValid` | 保留为唯一语义实现，抽取必要的可复用绑定函数 |
| CREATE 生命周期准入 | `compile/lifecycle_fk_scope.go:admitForeignKeyCreate`；`lifecycle_admission.go:admitLifecycleRC` | 扩展现有准入边界，覆盖空集合的有效性 |
| 数据库、表名及表定义互斥 | `lockMoDatabase`、`lockMoTable`、`lockTable` | 在元数据写入前锁定完整参与对象 |
| DROP/CREATE、约束和反向引用更新 | `Scope.createTable`、`AddFkeyToRelation`、`AddChildTblIdToParentTable` | 使用已校验对象继续现有执行路径 |
| 内部替换的锁借用 | `dropLifecycleReceipt` / `withDropLifecycle` | 扩展同一短生命周期凭据的 CREATE 校验，不增加第二套凭据状态机 |
| 语句回退、重建计划、事务终止 | `prepareRetryTransition`、`buildRetryCompile`、事务/workspace | 返回既有错误类别，不在 FK 函数内循环重试 |

源码中的两个限制直接影响方案：

- 并非所有原地 ALTER 都持有 G；ADD FK 会锁父表名，DROP FK/改列/改名也会使用子表元数据锁。不能用 G X 替代参与表锁。
- `mo_foreign_keys` 的 ID 列在现有插入路径中可以是 0；不能直接拿它们当可靠物理身份。现有按被引用名称查询也不等价于有专用索引的点查。

## 3. 选择与取舍

| 方案 | 判断 |
|---|---|
| 只给旧子表加一次 ID 检查 | 不足：不能发现同 ID 的声明变化，也不能发现新增参与者 |
| 所有 CREATE 一律 G X，随后重新规划全部 SQL | 不选：扩大全局串行化，重复整个规划流程，且仍需处理原地 ALTER 的表锁 |
| 执行器临时重写失效的 FK，继续旧计划 | 不选：容易让 catalog UPDATE SQL、约束、列 ID 使用不同代计划，并复制 planner 语义 |
| 在现有准入/表锁边界验证完整绑定；变化时重建整个计划 | 采用：保持一个绑定规则、一个执行器和一个重试所有者 |

这里有真实成本：没有 FK 的普通永久 CREATE 也需要证明“当前没有待修复引用”。采用现有 G S 和目标表名锁保护这个负向检查；有 FK 发布的 CREATE 保留 G X。新增共享持有者可能增加独占等待，因此需要测量，但不能为规避测量而省略正确性检查。

## 4. 最小计划数据补全

只补现有计划无法表达的事实，不复制完整子表定义或增加 schema epoch。

1. `ForeignKeyInfo` 增加 `child_table_id`（建议新字段号 6），由现有 planner 解析到的子表物理 ID 填写。填充已有 `cols` 字段，使子列名/顺序与 `def.Cols` 的 ID 一起成为可检查输入。数据库和表名继续使用现有字段。
2. `CreateTable` 增加 `unresolved_fk_parents`（建议 `repeated ObjectRef`，新字段号 16；14 已 reserved，15 为 RawSQL）。它仅表达 CREATE 自身指向尚不存在父表的依赖；Obj=0 是明确的“不存在”预期。去重后按已有规范保存数据库/表名。
3. 正常已解析的出向 FK 继续使用 `FkDbs/FkTables/Fkeys`；自引用保留现有 sentinel 处理。不要把未解析 FK 塞进 materialized Fkeys，也不要从 `UpdateFkSqls` 反解析依赖。
4. 新字段由真实 planner 路径生成。LIKE/CLONE、PREPARE、内部 CREATE、proto 深拷贝和往返序列化都要覆盖。前向条目的物理 ID 缺失应成为明确内部契约错误，不增加“旧计划按名字猜测”的兼容分支。

计划数据是单次执行输入；不写入 catalog，不跨重启保存。停机升级后重新生成计划，不需要数据迁移。已有 catalog 的 legacy/extended 布局适配仍由原读取函数负责，不因本修复删除仍在使用的布局支持。

## 5. 生产悲观 RC 的执行协议

记 C 为生命周期 registry 物理身份锁，G 为现有生命周期行，D 为数据库名锁，T 为表名元数据锁。

### 5.1 在任何 CREATE 写入前建立校验域

1. 对 FK 可用数据库中的永久 CREATE，按计划选择 G：有出向/前向发布用 X，否则用 S。使用现有 `C → G → D` 准入及 applied frontier；临时表、FK 禁用的系统库沿用各自已有路径。
2. D 域包含目标数据库及计划中涉及的数据库，排序去重。T 域包含目标表名、已解析及缺失的父表名、前向子表名，按 `(account, db, table)` 排序去重。缺失对象仍锁名字，不能省略。
3. 对会修改现有约束的每个前向子表，解析关系并核对计划物理 ID；使用现有表定义变更锁语义取得物理表锁，保留 `changeDef=true`。同一子表多个 FK 只取得一次锁。这样同时排斥原地 DDL，并给使用旧定义的 DML 提供正确的变更通知。
4. 所有可能等待的相关锁取得后，执行既有 TN applied-frontier 屏障，再重新解析关系。不可把锁返回的时间戳当作 TN 读屏障。最终检查不能使用 frontend 中早先缓存的 TableDef。
5. 此阶段不得执行 `dbSource.Create`、`UpdateFkSqls`、约束更新、反向引用更新或 index-table ID 分配。目标已存在时维持现有 `IF NOT EXISTS` 的无副作用语义；不得为了忽略的定义去修改现有表的 FK。

### 5.2 一次完整检查

在同一个事务/workspace、当前 actor 上读取目标的完整 incoming FK 集合。即使计划集合为空也执行；排除自引用的规则复用已有查询。

比较内容为：子表数据库/表名、约束名、按 `constraint_id` 排列的子列与父列、动作和动作来源，以及按照已有选键规则生成的绑定。约束之间的 map 遍历顺序不属于语义；复合列顺序属于语义，不能排序抹掉。

先比较参与者和声明集合；发现新增、删除或变化即返回重规划，不继续解析未被当前锁域覆盖的新子表。只有集合匹配时，才对已锁成员做列绑定验证。

对每个计划子表核对物理 ID，并使用锁后的实时列定义复用 planner 的类型/列/父键校验。采用共享的绑定函数生成可比较结果；不要在 compile 中另写一套 FK 类型或选键规则。使用关系的当前定义，不能仅依赖全表 version：同事务对象可能 version=0，而无关列变化也不应产生永久重试。

在同一边界执行：

- 现有出向父表验证和定义变更处理；
- `unresolved_fk_parents` 的预期不存在验证，复用 `validateForeignKeyParentGeneration(..., expectedID=0)`；父表已出现则重建计划，使此次 CREATE 改走正常已解析 FK 绑定；
- 完整 incoming 集合比较，包括消失、新增和空→非空。

任何关系/集合变化均在写入前返回 `ErrTxnNeedRetryWithDefChanged`。当前快照下仍有效但与 SQL 不兼容的 FK，重新规划后返回既有语义错误。目录读取失败、取消、权限问题不能被当成空集合。

若负向检查发现新引用，重新规划下一代，再按正向路径获取 G X；沿用 retained-owner 的非等待升级规则，冲突走既有事务失败路径。禁止持有较低层锁排队升级 G，也禁止添加内部无限重试。

### 5.3 为什么检查之后不会再失效

| 并发动作 | 排斥/检测边界 |
|---|---|
| 新 CREATE 子表发布 FK | 其 G X 与目标 CREATE 的 G S/X 冲突；取得锁后还须检查原“父表缺失”预期 |
| ALTER ADD FK 到该父表 | 既有 ADD FK 对父表名的 T X，与目标 CREATE 的 T X 冲突 |
| 已知子表 DROP/同名重建、COPY ALTER | G 与相关 D/T；物理身份再次核对 |
| 已知子表原地 DROP FK、改名、改列 | 子表 T 和物理表定义锁，不能只依赖 G |
| 父表名被别的 CREATE/RENAME 占用 | 目标 T；沿用定义变更重试及表已存在语义 |
| 同事务先前未提交的 DDL | 同一 workspace 读己之写，不能开启旁路事务或 historical snapshot |

空 incoming 集合下，删除或改名一个不引用该目标的子表不能凭空产生引用；能新增引用的 CREATE/ADD FK 已分别被 G 或目标 T 覆盖。

### 5.4 执行与提交

校验返回仅本次调用存活的绑定列表：计划条目与已锁定、已核对的 `engine.Relation`。继续现有 CREATE、catalog 更新、子表约束更新及父表反向引用路径。

删除现有 forward-repair 循环在写入之后再次“按名字随便取一个子表”的逻辑，消费已校验的关系。不要只在旧逻辑旁边增加检查，然后仍让旧逻辑选择执行对象。

父表物理 ID/列 ID 仍在实际创建之后由现有逻辑填充。不要修改共享/缓存计划中的 FK 定义。多个 FK 对同一 child 的锁/解析去重；约束写入是否合并为一次，以复用现有约束合并函数为前提，不引入通用批处理层。

真正的外部可见点仍是事务提交。父表、catalog 行、子约束、反向边必须共同提交或共同回退。不能把“已经校验”变成提前发布标记。

## 6. 内部 DROP/CREATE 和重试所有权

这是设计的必要部分：不能让新的 CREATE 准入在外层 DROP 已执行后无条件推进快照。

- **scalar TRUNCATE**：其外层已经证明 FK-free，并持有 G S 和目标表名/物理锁。内部 CREATE 复用同一个现有 receipt 的明确 CREATE 校验：actor、TxnOperator、数据库/表名和 FK-free 计划必须匹配；旧物理 ID 用于标明来源，不要求已删除的旧关系仍存在。CREATE 期间凭据只在同步回调中有效，退出立即失效。避免重复全局准入和负向目录扫描。
- **broad TRUNCATE / COPY ALTER**：外层在第一笔替换写入前，使用同一参与对象收集/加锁函数，把可能在内层修复的子表域锁全。沿用现有 receipt 传递这次执行的已锁对象域，内部 CREATE 按外层 workspace 检查实际关系。锁域和语义验证共用函数，不维护第二份 catalog。
- 内层不可自行扩大凭据授权范围，不可凭 `IgnoreForeignKey`、`disableLock` 或一个布尔值跳过验证。若发现外层未覆盖的对象，或必须刷新快照才可继续，按锁准入失败返回既有 `ErrLockConflict`，让事务所有者回退；不得仅重试 CREATE 并留下外层 DROP。当前 frontend 的 `errCodeRollbackWholeTxn` 明确包含该错误，而普通内部错误或 `ErrTxnWWConflict` 不能仅凭名称推定具有相同回滚效果。
- 普通顶层 CREATE 的代际不匹配发生在写入前，可以使用现有 RC 语句重建。写入后的注入失败走已有语句/事务回滚。不得把所有错误都转换成可重试错误。

receipt 只保存这次调用必要的对象域，不增加持久状态、后台 worker 或跨事务缓存。旧的 DROP 校验继续有效；CREATE 的验证不能错误调用“旧关系还存在”的 DROP 前置条件。

## 7. Unhappy path、资源和兼容

| 路径 | 结果要求 |
|---|---|
| 等待 G、T 或子表物理锁时取消/超时 | 返回对应终止错误，无新 FK/父表提交；在拥有事务的正确边界完成回滚 |
| 查询、分配或写入中途失败 | 关闭结果/释放临时向量；锁由事务终止释放；无局部提交 |
| 多子表修复中第二次更新失败 | 所有子表、父表及 catalog 更新一起回退；下一次正常执行可成功 |
| 计划条目缺少身份、列数组不一致、重复同名定义 | 明确内部契约错误；不能 panic、静默丢条目或无限重试 |
| 真实子表消失/换代、FK 集合改变 | 写前重建计划；重建仍遇到持久目录不一致时返回真实错误 |
| 多语句事务先前已经有写入 | 保留 prior workspace；不能用独立事务重新读元数据或回退错误的语句边界 |
| 重启/崩溃 | 沿用事务原子提交恢复；无新恢复记录或后台修复任务 |

内存与锁数量按相关子表数 U、FK 数 F、引用列数 C 线性增长；排序 O(U log U)。结果流读取并按请求归还；沿用已有锁容量上限，不能为了容量把名称域升级成整张目录表锁。

SI/乐观事务保留其快照与提交冲突语义，不调用 RC 的快照推进 helper；新增计划数据及纯校验需可兼容这些调用者。本设计不声称修复这些模式下所有并发 FK 异常。若实现必须改变它们的隔离协议，需单独扩展设计。

租户隔离：FK catalog 和子表读取始终属于请求 actor；仅既有全局 G/C 锁使用 SYS 身份。禁止使用 SYS 查询结果替代 tenant 的 FK 集合。临时表同名遮蔽不能把持久 FK 绑定到 session 临时关系。名称规范化复用已有解析规则，不自行全部转小写。

交付采用停机切换，不加用户开关。诊断复用既有错误、锁等待和重试统计，不添加按表名维度的无限基数指标。新版本阻止再次错误绑定；已经受影响的数据需依据 catalog、SHOW CREATE 和实际约束单独核对，不能据此自动删除约束。回退程序版本不会转换 catalog，但会重新暴露旧竞态，应记录这一限制。

## 8. 成本和三项附加观察

### 8.1 性能预算

- 普通 SELECT/DML 不增加准入、目录查询或每行工作。
- 无 FK 的普通永久 CREATE：新增负向保护及一次 incoming 集合读取；必须实测额外 C/G 准入、frontier 与查询的实际开销。
- 有 FK 的 CREATE：复用 G X，增加最终集合验证和每个不同子表的必要定义锁。查询按父目标批量读取，禁止每个 FK 单独查询 catalog。
- scalar TRUNCATE 内部 CREATE：借用外层证明，要求不重复新增整套 FK 扫描/快照推进。
- 不能将 `where refer_db_name/table_name=...` 直接写成 O(1) 点查：现有 catalog 主键没有以这两个字段开头。最坏扫描成本可能随租户 FK 总量 M 增长；本修复先测量，不顺手加索引/缓存/新存储。

对照分别回答两个问题：`dd38098853` 与已合并 #29837 基线比较原 TRUNCATE 优化；`4022729da2` 与本修复比较新增校验成本。使用同机、相同构建/拓扑/数据，记录中位和尾延迟、查询数/读行量、锁等待、重试和分配。覆盖无争用 CREATE/TRUNCATE、目标 convoy、不同相关 FK 数与无关 FK catalog 规模。报告绝对耗时和差值，未测前不承诺更快。

### 8.2 锁公平性

保留现有 `WriterFair=false` 和 retained promotion 规则。使用专门的有限竞争实验观察共享流量下独占进展/等待分布，并与基线对照；一轮有限测试通过不能证明任意无限流量下绝不饥饿。没有证据时不修改全局公平策略。若新 CREATE 共享锁导致可重复的进展退化，回到准入方案评审，不能靠放大 timeout 掩盖。

### 8.3 #29837 的覆盖缺口

同一 fixture 增加目标表锁等待时的 cancel/deadline 分支，验证终止、原 ID/数据及后续可用性；增加 PITR 已保护时的 broad fallback，并验证恢复行为。它们独立标记为生命周期覆盖补充，不冒充 #29852 已确认的故障，也不据此扩大 FK 修复的业务语义。若暴露旧基线问题，先做相同基线比较再归因。

## 9. 最小但有区分力的验证计划

复用 `pkg/tests/issues/issue_29400_test.go` 的 authenticated 2-CN fixture，以命名 subtest 隔离场景；不因新 issue 另启动一套 cluster。planner/compile 各在现有测试文件扩展对应契约。

| 契约 | 最便宜的证据和关键断言 |
|---|---|
| 计划携带有效身份/缺失依赖 | planner UT：真实 forward producer、Cols 顺序、非零 child ID、pending parent；proto/deepcopy 往返 |
| 比较不受遍历顺序影响、保留复合列顺序 | 纯函数 UT：重排集合相等，列序/动作/参与者变化不等；malformed 输入明确错误 |
| 原始 bug | 2-CN 实际 SQL：旧 child→无 FK 同名新 child；两条 CREATE 成功后，新 child 无 FK、catalog 无旧行、INSERT 成功 |
| ID 检查不是唯一证据 | 同 ID 删除/重建 FK，或修改相关列；旧计划不得写入，合法新定义由重规划执行 |
| 负向证明和反向缺失依赖 | 计划 empty→新增 child；另一个调度为 child 计划 parent absent→parent 先提交；最终均正确绑定且非法 INSERT 被拒绝 |
| 非等价正常控制 | 无并发正常 forward FK；同名新 child 声明了合法的新 FK；跨库、两个 FK/复合列保序 |
| 检查后的排斥 | 在最终检查后暂停，竞争 DROP/ALTER 按相关锁等待；释放后只能出现合法串行结果 |
| 写入失败原子性 | 父 CREATE 后/首个 child 约束更新后注入错误，检查无局部父表、FK 或反向边；随后重试成功 |
| 锁等待终止 | observable waiter 后 cancel/deadline；显式事务按协议回滚后无锁残留、无 metadata 变化 |
| 真实消费者 | PREPARE/EXECUTE、同事务先建 child 再建 parent、LIKE/CLONE 和 scalar/broad TRUNCATE 内部 CREATE |
| 隔离及相容性 | 两租户同名对象、临时同名遮蔽；SI/乐观正常语义；legacy/extended catalog 读取 |

同步用已有 lock-wait observer/执行器注入 seam；必要的计划后 barrier 只承担调度，不能替代生产判断。测试只用一至两个区分结果的行；要求观察 SQL、物理身份、catalog、约束执行和 rollback，而非仅验证新 helper 的返回值。

已有诊断 red 可复用为复现材料，但不算新基线/修复的通过证据。实现后先跑区分性 red→green，再跑变更拥有包 normal/race、所选公开 SQL、相关 BVT 和增量 SCA。并发重复次数按已有有限预算计算。禁止用覆盖率数字替代行为验证。

## 10. 实施顺序和设计自审

1. 补齐计划身份、缺失父依赖及共享绑定/比较函数；明确 malformed 输入行为。
2. 在现有 CREATE 准入边界锁定并验证完整域，复用现有 retry；把 forward 执行循环切换到已校验关系，删除晚期按名字重新选择对象。
3. 同时完成内部 scalar/broad 借用、外层写前锁域收集和终止规则，不能将这一项留给“后续兼容”。
4. 加入核心反例与原子性测试，完成性能对照；再处理第 8.3 节独立生命周期测试补充。

生产改动预期局限于 plan/compile 和相应 protobuf 生成文件。没有新增 catalog、服务接口、worker、缓存或用户配置。若实现开始修改 txn/lockservice 协议、需要新持久字段，或为了绕过冲突引入第二套 retry，应暂停并重新评审设计。

自审已逐项挑战：ID-only、G-only、只检查计划中已有成员、遗漏缺失父依赖、在 DROP 后推进快照、逐 FK 查询、跨租户读以及复合列乱序，均在上述协议中有明确处理。

设计结论：采用现有锁保护下的完整绑定校验与计划重建。新增共享 CREATE 准入和 child 定义锁是明确维护/运行成本，必须以真实对照验证；尚未实施，不宣称性能或修复验证已通过。设计审查是本轮源码自审，没有启动 subagent 或声称新增独立模型评审。

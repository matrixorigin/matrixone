# 4.2-dev 向量标量来源与等值 JOIN：重新实现方案 v1

- 日期：2026-10-12。
- 状态：**待用户 review，未批准实施**。用户已选择“重新实现”，本版将具体实现与兼容取舍提交确认。
- 基线：`mo/4.2-dev`，`276938065e831d66e01027a2290f18780563571b`；两个 worktree 均已 pull，Go `1.26.4`。
- 需求：[#23158 的 SQL1](https://github.com/matrixorigin/matrixone/issues/23158)、[#29702](https://github.com/matrixorigin/matrixone/issues/29702)。
- 行为参考：[#29493](https://github.com/matrixorigin/matrixone/pull/29493)、[#29767](https://github.com/matrixorigin/matrixone/pull/29767)。这是新实现，不声称 diff 等价的 cherry-pick，不沿用主线设计批准或测试结果。
- 实现 PR：尚未创建；拟分别使用 `issue-23158-4.2-dev`、`issue-29702-4.2-dev`，最终 base 均为 `4.2-dev`。

## 1. 范围与设计门禁

实现两项能力：

1. 独立、PK/UK 可证明至多一行的标量子查询，提供非 NULL 查询向量时使用既有 IVF-FLAT/HNSW；空来源和 NULL 保留原关系语义。
2. 普通等值 INNER JOIN 过滤向量表时，允许 IVF-FLAT 在候选 Top-K 前应用精确资格集合，最终保留 JOIN 重数、右侧投影和分页。

建议排除：

- 不搬入 #27009、#27297、#29058、#27939，不引入新的 vector-index-scan 执行架构、通用 UNION 惰性调度或 CDC initial snapshot 框架。
- #29767 混入的 CDC executor 锁修复不属于本次两个向量功能，**本方案建议不迁移**；这是重实现范围选择，不伪称原 PR 完整迁移。批准本方案才视为接受该范围。
- 不扩展 #23158 SQL2/SQL3、GPU、相关子查询、任意动态向量、分区、charset/collation、存储过程、UDF、warning 兼容性。
- 不新建完整 JOIN 成本模型、不承诺加速倍数或 ANN 精确召回率。

跨 planner、compile、算子生命周期及计划协议，并新增按需启动状态，触发强制设计门禁。主线方案明确要求旧分支另行评估，不能复用其批准。

## 2. 已核实的 4.2 能力与差异

| 当前代码 | 已有能力 / 差异 |
| --- | --- |
| `pkg/sql/plan/apply_indices_vector.go` | 单行非 NULL INNER provider；SEMI membership；PK/UK 至多一行证明；候选预算 LIMIT+OFFSET |
| `pkg/sql/plan/apply_indices.go` | `detectVectorGuard` 和普通插件 `CanApply/ApplyForSort`；没有主线 logical-search 入口 |
| `pkg/sql/plan/apply_indices_ivfflat.go` | `FUNCTION_SCAN/ivf_search` 可接 provider 子节点；membership 的 build/probe 使用 `MustApply`，并保留部署协议门禁 |
| `pkg/sql/colexec/table_function/ivf_search.go` | required membership 缺失、PASS、取消时拒绝无过滤继续；已有结果游标与资源生命周期 |
| `pkg/sql/internal/materialized/source.go` | 带预算和代际检查的语句局部物化来源；merge reader 已有释放协议 |
| `pkg/sql/compile/scope.go:MergeRun` | 一次启动全部 PreScopes；结果计数假定全部启动，不能只漏启动一个分支而沿用计数 |
| `prePipelineInitializer/InitAllDataSource` | 全树初始化数据源；仅推迟 goroutine 不足以保证未选分支不初始化 reader |
| `output/output.go`、`compile/compile2.go` | AUTO 在最终输出为空时触发整语句重试并改写 AST，不是子树局部 fallback |

因此可以复用现有 TVF 与 membership，但必须新设计标量 selector 的局部执行所有权；不能机械删除缺失接口后认为功能完整。

## 3. 语义不变量与最小反例

- 所有求值沿用同一语句的租户、权限、事务/快照；不在规划期读 provider、不新增旁路 SQL 连接。
- provider 每个执行代际最多读取一次；PREPARE 缓存结构不缓存数据。既有事务重试先结束旧代际，再建立新代际。
- LIMIT 0 不执行 provider、ANN 或 fallback；外层 LIMIT/OFFSET 不复制到两个内部 reader 子树。
- 只启动被选结果分支。空来源和一行 NULL 不能被等同于“ANN 返回零行”。
- ANN 错误不得自动切换 fallback；EOF 后仍检查 producer 完成结果，不能吞掉清理阶段错误。
- JOIN 候选来自资格集合，最终仍是原 INNER JOIN；资格集合允许去重，最终输出不允许去重。
- FORCE 留在原精确计划。未通过证明或插件能力检查的形状留原计划，不能悄悄变更结果/错误语义。
- 保持结果列顺序、类型、来源和 prepared 表/索引依赖。

反例：

1. 主表两行、provider 查不到 PK。原标量子查询为 NULL，但仍可返回主表两行；改成 provider 驱动 INNER 会错误清空结果。
2. B 对同一 join key 有两行，A 有三个合格对象。原 JOIN 返回重复行，替换成 IN 不等价。
3. 全局最近对象均不满足 B 资格。先全局取 K 再 JOIN 会漏掉距离稍远但合格的对象。
4. 上层 JOIN 附加 hash-build scope。把它误纳入 selector 的未选分支会永久等待其输出。

语义依据：[标量子查询](https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-SCALAR-SUBQUERIES)、[JOIN 的关系与重数语义](https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-JOIN)、[pgvector 的近似查询说明](https://github.com/pgvector/pgvector#querying)。仅用于语义对照，不引入 PostgreSQL InitPlan 架构。

## 4. 方案比较与选定路径

| 方案 | 正确性 / 成本 | 决策 |
| --- | --- | --- |
| 保持现状或应用拆两次查询 | 不满足单条 SQL 索引能力，快照责任外移 | 不采用 |
| 整串依赖 backport | 可复用主线，但扩大执行与 CDC 变更面 | 用户已选择不走此路径 |
| 放开 SINGLE/NULL guard 或 INNER 改 SEMI | 代码少但立即违反反例 1/2 | 拒绝 |
| 编译期求值 / 通用分布式 InitPlan | 前者缓存陈旧结果；后者扩张协议与执行框架 | 不采用 |
| **4.2 原生 TVF + 局部 selector + 原 membership** | 实现两项能力，代价是局部生命周期适配和保守单 CN | **推荐** |

### 4.1 模式边界：避免旧版 AUTO 整语句重放

本版建议明确限定新索引路径：

- 普通 LIMIT 与显式 PRE：资格证明通过且索引可用时，私有候选使用现有 PRE 路径；不让其被默认 AUTO 配置升级为整语句重试。
- 显式 FORCE：不启用新优化。
- 显式 AUTO/POST/INCLUDE 等其它模式：首版不新增这两个形状的索引支持，保留已有计划与已有模式行为。
- 同一语句其它区域会启用 AUTO 整语句回退时，保守不启用本次新改写；不能仅修改结果 Output 来掩盖 AUTO 标记。
- 模式限制仅作用于本次新支持形状及其私有候选，不改变已有 literal、INNER provider、SEMI membership 查询的模式策略或会话配置。

这是相对主线重实现的**显式兼容取舍，需批准**。不复制 #29058，也不以 AUTO 零结果重放代替 selector 的 NULL 选择。若要求这些形状的 AUTO/其它模式也完全对齐主线，应扩大设计并重新审阅，而不是实施时偷偷缩减范围。

### 4.2 标量来源：局部一次物化与选择

准入：独立 provider、PK/UK 全键等值至多一行、唯一性证明不跨非单射转换、单个支持距离的升序排序、有 LIMIT；拒绝相关来源、volatile/副作用表达式、未证明多行和 SQL_CALC_FOUND_ROWS。现有非 NULL INNER provider 不改变。

计划：

```text
整条查询固定在入口协调 CN，保留 CN 内并行
  普通 merge 包装（上层 JOIN 的附加依赖位于包装外层）
    selector 私有域
      P：provider -> 独立保留零或一行向量 M
      A：M reader 0 -> 现有 IVF/HNSW TVF 路径
      E：M reader 1 -> 原 SINGLE/LEFT 关系与原排序
```

- 保留 selector/source 逻辑节点，ANN 分支使用 4.2 的 FUNCTION_SCAN，而非搬入 VECTOR_INDEX_SCAN。
- 在私有副本上尝试索引，成功后才替换原根。插件拒绝时原树、binding tag、runtime-filter tag 和 remap 不受污染。
- source 零行仍保持零行，由 E 的原 SINGLE/LEFT 执行 NULL 扩展；一行 NULL 保留该行。
- source 只存一列向量；Compile 持有总生命周期，selector 写入封存，两个 reader 各自完成释放。未启动 reader 由私有域清理路径释放一次。
- selector 的输出和 result-column provenance 指向结果分支，不能误读 provider schema。FUNCTION_SCAN 输出使用自身 TableDef。
- 计划节点使用主线已分配的 `VECTOR_QUERY_TOP=58`、`VECTOR_QUERY_SOURCE=59`、`vector_query_source_id=93`，基线这些位置未占用；不复用主线其它编号。VM 新操作码对齐 `VectorQuery=67`，不重排既有值，检查新的枚举上界及空洞消费者。
- 只修改 `.proto` 后生成 bindings；同步 deepcopy、remap、统计、EXPLAIN、compile、VM 枚举消费者。

### 4.3 按需启动：仅 selector 私有域，不扩展通用调度框架

在 Compile/Scope 增加 selector 专用执行域，通过 merge 输入消费的受控接口请求启动与等待；不让 colexec 导入 compile。

状态：

```text
未启动 --非零需求--> provider 活动 --EOF + 完成成功--> source 封存
  --选择一次--> ANN 或原关系活动 --EOF + 完成成功--> 完成
任意阶段 --失败/取消--> 封闭启动入口 -> 取消活动分支 -> 等完成 -> 清理
未启动 --LIMIT 0--> 清理并完成
```

- receiver 先安装，再允许 producer 启动；完成通知具有固定容量，不能被不再读数据的消费者阻塞。
- `InitAllDataSource` 对私有域不递归初始化全部孩子，只在分支启动时初始化该子树。已有锁/权限检查仍沿用语句边界，不能旁路。
- 普通 `MergeRun` 路径不改变。私有域按实际启动数等待，不等待未启动的两个/一个分支。
- provider 的数据必须消费至 EOF，且 producer 终态成功后才能选择；意外超过一行立即失败，不截取第一行。
- 提前结束、启动失败、部分 Prepare、取消、下游错误均先封闭新启动并取消活动分支，再等待真实完成；不能先持锁等待、不能启动未选分支来“排空”。
- 清理区分“已启动并有 pipeline cleanup 所有者”和“从未启动但持有编译资源”的分支，防止重复 Reset/Free/reader release。
- 上一代运行者与回调全部退出后才能 Reset/复用；新代际清空选择、完成状态、向量及 LIMIT executor。
- 私有域由普通 merge 包装，上层 JOIN hash-build 与其它依赖按现有机制并发启动，不加入三个私有分支的启动计数。

这部分是 R3 新实现，必须有真实 Scope/管道的生命周期验证；不能只靠 selector mock 测试通过。

### 4.4 等值 INNER JOIN：资格过滤加重数保留

准入：普通表两侧直接 scan、等值 ON、索引向量可证明非 NULL、查询向量可折叠为非 NULL 常量；无跨侧残余条件/volatile、受支持距离与排序模式。参数向量未被现有绑定阶段证明时保持原计划，不为了命中索引在规划期读取数据。

```text
私有复制 A、B 并重绑定
  A SEMI JOIN B -> 精确合格主键集合（MustApply）
  IVF 在集合内选 K+OFFSET 个 A 候选
  候选 A INNER JOIN 原 B -> 原投影/排序 -> 最终 OFFSET/LIMIT
```

- 复用 `detectVectorGuard` 在普通索引递归改写前保留必需 scan 形状；探测只操作私有节点。
- 提取/复用现有插件 dispatch，调用 `CanApply/ApplyForSort`，不增加按算法名称分支。
- 不依赖 #29493 的辅助函数；以插件成功结果和实际 TVF/索引元数据确认访问路径，不能把任意 FUNCTION_SCAN 当向量索引。
- 候选内只计算 K+OFFSET 预算，最终 OFFSET 只应用一次；原 B、ON、右侧列和重复展开保留。
- eligibility 副本与最终 JOIN 使用不同节点、binding/runtime-filter tag，不共享可变计划状态；保持相同 statement snapshot。
- 延用已有 membership 协议版本检查、资源预算、生产失败/取消传播和 ForceOneCN 约束，不改变过滤编码。
- 先按能力优先，不引入未经校准的成本阈值。B 双读和额外资格 JOIN 可能让小表/高重数查询更慢；FORCE 是已有退出路径。

## 5. 资源、等待、代际与兼容

| 对象 | 唯一有效所有者与终态 | 上界 / 等待解除 |
| --- | --- | --- |
| provider 向量 M | Compile Begin/Close；selector 写入；reader 各释放一次 | O(D)，零或一行，现有物化内存/溢写预算 |
| 私有三个分支 | 专用 Scope 域；最多启动 provider + 一个结果分支 | 固定数量；取消先于 join；不等待未启动分支 |
| batch/TVF reader | 已有管道/TVF Prepare/Reset/Free | 不新增结果全量缓存，沿既有预算与批次边界 |
| membership | 原 hash-build/message/IVF 消费链 | 复用现有容量与取消协议；不新增全表 Go map 缓存 |
| prepared/重试代际 | 原 Compile 代际所有者 + 私有域完成屏障 | 旧回调退出后发布新代际，不保存跨执行向量 |

Q1 验证覆盖初始化失败、未启动分支、正常结束、提前结束、重复清理；Q2 覆盖 receiver 安装、provider 完成、取消与 JOIN 附加依赖；Q3 覆盖向量复制、批次、消息容量与多次 EXECUTE 后的资源回落。

- 不改变索引磁盘格式、catalog、DDL/DML、CDC/ISCP；无需重建索引、迁移数据或备份格式变更。
- selector 所在整条查询在入口 CN 执行，新算子/source 不进入远端 pipeline；仍可使用既有 TN/fileservice RPC。IVF 内部既有 SQL 读取不携带本地 source 指针。
- 升级/降级不共享新内存计划；既有 session/prepared 生命周期重建计划。不能仅凭 protobuf 兼容忽略未知节点后继续远程执行。
- membership 保留已有部署协议门禁；不新增其编码，不绕过混合版本 guard。没有专项混合版本实测前不声明全链路混合版本验证通过。
- 同租户权限/事务上下文不变，不打印查询向量，无新后台 worker、全局 cache 或配置项。
- EXPLAIN 展示计划与选择器结构但不求值 provider；实际启动计数/物理 CN 地址另作验证。
- 回退使用已有 FORCE 或撤回优化；unsupported 保留既有关系路径，不改变全局配置。

## 6. 变更地图与验收

| 闭包 / 拟改位置 | 风险 | 必需证据 |
| --- | --- | --- |
| scalar/equi planner、guard、plugin dispatch | R2 | public planner 正例、最邻近拒绝例、私有副本不污染、模式边界 |
| proto、计划消费者、输出来源、EXPLAIN、VM | R3 协议消费者 | 生成一致性、枚举稳定、类型/metadata、serialization roundtrip、EXPLAIN 不求值 |
| selector、Scope 私有域、merge reader/物化接入 | R3 生命周期 | 真实 Scope/HashJoin 成功/失败/取消/LIMIT 0/Reset；资源回落；focused 及 owning-package race |
| 既有 IVF/HNSW provider、membership 消费 | R2/R3 消费边界 | 真实 TVF consumer、required membership 失败、原查询路径无回归 |
| SQL 公共行为、协调 CN 布局 | R2/R3 | mo-tester、两次同实例、两 CN 分别入口、物理地址与结果双 oracle |

测试复用调查：已有 `apply_indices_vector_join_test.go` provider/membership fixture、`ivf_search_test.go` required membership/参数 LIMIT、`scope_test.go` 资源与代际/错误收敛、`vector_ivf_membership.sql`、`vector_ivf_mode*.sql`、`vector_ivf_retry.sql`。实现前检查对应结果及具体 oracle，只新增缺失语义格，不重复重型 fixture。

最小矩阵：

- 标量：PK/UK 命中、空来源、NULL、多行未证明、隐式转换、相关/volatile 拒绝、内外 LIMIT 0/OFFSET、inline CTE JOIN、HNSW/IVF、列类型。
- EXECUTE：来源值更新、空/NULL/非 NULL 切换、零/非零 LIMIT 复用；旧代际读者与回调不触达新向量。
- JOIN：唯一/重复/NULL/未匹配 key、空两侧、最近对象不合格、少于 K、距离边界、VARCHAR PK、反向 JOIN、右侧投影、OFFSET。
- 模式：普通/PRE 命中；FORCE 不命中；默认 AUTO 开启与显式 AUTO/POST/INCLUDE 按第 4.1 节保留边界；现有 literal/provider/membership AUTO 不回归。
- 生命周期：provider/ANN/完成屏障错误、启动失败、初始化失败、结果提前关闭、取消、未选分支零执行与 reader 清理。

验证顺序：

1. 先最小 public/typed 回归，证明确实命中或拒绝路径；最小数据独立 oracle，不用性能数据集。
2. Go `1.26.4`，CGo 使用仓库 `mo-cgo-test` 和 worktree 构建的原生产物；按最终 diff 推导 owning packages。
3. 聚焦 UT 后 owning packages 正常模式；生命周期测试用屏障注入，按耗时预算单项 race 重复，生产生命周期所属包跑一次 race。
4. 增量 gofmt/vet/CI 对齐 lint，修改生产行覆盖率至少 75%；build 与生成一致性检查。
5. 新/修改 BVT 用 mo-tester 生成结果、逐项独立审阅、正常比较，检查 teardown 后同实例再跑一次；无 skip/ignore 掩盖新行为。
6. 最小两 CN，从两个入口分别验证整查询协调 CN 和结果；原 literal 查询作布局对照。不借旧主线证据声称 4.2 已通过。
7. 完整 self-review 与交付 diff 核对。所有必需证据完成前不 push/交付。

当前只有源码检查，没有运行测试、构建或性能验证；上述均为待执行验收。

## 7. 执行顺序与审批记录

建议先实现风险较低且可独立提交的 JOIN，再做 scalar selector。两个分支均以 `4.2-dev` 为基线，避免 JOIN 人为依赖 scalar；共享触点发生变更时通过 merge 整合并补验证，不强推。

1. 用户审阅本版范围、模式边界、单 CN 与成本取舍；批准后记录版本/hash。
2. `issue-29702-4.2-dev`：独立 JOIN 实现与 UT/BVT。
3. `issue-23158-4.2-dev`：scalar selector 与 4.2 生命周期、计划消费者适配及验证。
4. 按最终共同基线核对两项组合行为；公共触点通过 merge 处理。
5. PR 标注为 4.2 原生重新实现并链接源 PR，不用 cherry-pick 标题或原样迁移声明。提交前按一般流程确认 issue：#23158 仅 Related，不关闭整个 issue；#29702 关联对应 JOIN 功能。
6. push 到 origin，在 matrixorigin/matrixone 使用模板创建 draft PR；设计文档可交付，TODO/本地证据不提交，不擅自评论或跟踪 CI。

### 待批准的关键决定

- 只重实现上述两个向量功能，不迁移混入的 CDC 改动与整串依赖。
- 新路径支持普通 LIMIT/PRE；FORCE 和未支持模式维持旧路径，防止旧 AUTO 整语句重放进入新 selector。
- 接受整查询协调 CN 和能力优先、无完整 JOIN 成本模型的首版边界。
- 接受新增局部 selector 执行域，而非重写全局惰性调度/向量执行框架。

审批状态：**待批准**。发现需要扩展这些边界时先更新设计并重新 review；不能以“修编译”为由引入未批准功能。

# #29437 S2 设计修订：按需只读绑定的最小闭包

修订：v2.1（2026-10-05），用户在本次对话批准“复用现有解析和绑定路径，保证取消、错误、结果与依赖正确；资源沿用现有查询限制并做必要防护和测试，不承诺逐对象的 128 MiB 硬证明”。本修订覆盖 `CLAUDE_20260922-view-metadata-on-demand.md` 的 S2 §11 描述工作内存 128 MiB 精确先准入要求及 `CLAUDE_S2_BOUNDED_PARSE_BIND_2026-09-29.md` 为实现该要求新增的全链路替换方案；其余 S2 的语义、安全及按需边界继续有效。S3–S9 及公共 SQL 的默认入口保持未激活。

## 问题与不可变条件

读取普通 View 输出列时，持久化列可能不是可见基表/嵌套 View 下的当前列真相；权威结果来自 CREATE VIEW 的既有 SQL 解析、绑定及来源/空值推导，不应读后写回 ViewSql 或目录。不执行 View 数据查询、类型探测用户函数、持久化刷新、统计采集，不新增后台任务、跨请求缓存或修改公共 SQL 解析路径。

请求固定当前可见目录、同事务 DDL、租户/权限/订阅、SQL mode/default DB/历史快照和对象身份。根授权每次检查；解析和绑定在请求取消/更早父 deadline 下终止（必要底层 I/O 要传播取消）；错误区分无效定义、未支持协议、源对象不可用、取消与可重试目录变化。输出列及完整根/中间 View/基对象依赖独立所有权，结果在请求后可安全读取，不共享可变 TableDef。共享嵌套依赖可使用 statement-local memo，命中不能改变语义/授权，也不得跨请求或跨不相容事务/snapshot。缓存关闭是结果/错误的参考 oracle。

资源拒绝采用既有 `moerr` 资源耗尽类；可见性和生命周期拒绝采用既有无效状态类，调用者仍以具名错误的 `errors.Is` 匹配区分重试、关闭和忙碌。父取消及 deadline 原因保留；错误展示使用 `moerr` 标准前缀和错误码。

## 修订后的资源契约

- **删除**“描述整个调用链存活内存精确 ≤128 MiB、任一 Go/CGo/SDK 分配逐对象先计费”的 S2 保证、逐叶替代解析器/binder/外部库的要求，以及原 §11 把这一数值用作 S2 验收的性能断言。该数字不是本次 API 或磁盘存储格式的一部分。
- **保留**现有查询/CN 内存 admission、mpool 和执行代的资源控制；不绕开或关闭它们。限制输入持久 SQL 大小及递归深度、扩张/逻辑工作、memo 项数和保留字节、单次输出/依赖容器大小；上限选现有合理边界并写入实现和确定性 UT，失败返回明确资源错误，取消/关闭释放请求局部状态。避免依赖数×SQL 长度、深链递归和无界缓存；不声称对 opaque helper 做无法证实的逐对象收费。
- 新增内存只在本边界直接创建的可控结构限制（例如 memo、输出、依赖），临时 AST/绑定对象沿既有查询预算与 Go runtime 生命周期；超限不得转为静默少列、错误分类吞没、备用未计费全局缓存。通常 SQL 成本不得因这次描述激活而显著回退；用代表性 UT/benchmark 验证。

## 架构与比较

1. 维持普通元数据的持久列：不满足可见源变化时输出新鲜性，否决。
2. 为精确 128 MiB 拷贝 grammar/binder、fork 第三方依赖及 FileService/RPC 全链：对按需列推导过度，改变普通 SQL 热路径并显著增加分叉维护/正确性风险，否决。
3. **选定**复用当前 parser→binder→只读列推导及目录 resolver，构建一个请求局部适配，必要时只提取已有 CREATE VIEW 共同语义；归属请求的必要结果和 memo 独立持有；老 SQL 入口不改。读路径的 catalog I/O 走同快照读事务，写操作按最邻近接口检查不可达。

## 状态与验证

生命周期：Open（无工作）→首次 Describe 固定上下文→单活动绑定→发布已完成只读结果/错误→下次 Describe 或 Close。部分初始化、解析失败、绑定失败、取消、错误、panic 与 Close 必须释放已创建请求状态，不抢先关闭借用的事务/执行代；Close 与结果最后读者退场同步。memo 的可选失败允许不缓存但不能屏蔽描述本体失败。有限制的工作计数在缓存命中仍按等价逻辑展开计费，循环先检测。

按风险从小到大验证：权威 CREATE VIEW 与只读推导的列类型/宽度/精度/nullability/default、显式列名及多层依赖差分；当前/历史/同事务 DDL/跨账户权限/订阅、cache-off 与 memo-hit 对照；取消/错误/关闭 barrier、预算 N/N+1、慢 I/O 的退出边；owning CGo 包、按需 race、静态工具及保留生成器幂等；若改默认公共 SQL 输出则 mo-tester BVT 生成并验证，否则具体记录默认未激活路径。修改代码覆盖率仍须满足仓库 ≥75% 的合适口径，交付前自审零 blocker。不存在“先导出半成品然后补资源门禁”的例外。

## 决策日志

- 用户确认删除精确 128 MiB 逐对象证明，不删除资源有界、防 OOM、取消、结果/依赖/权限正确性。
- 用户确认普通按需解析/绑定为主，不创建期限调度后台 worker。
- 保留 #29437 issue 规定的 rollout 隔离；S3–S9 不纳入本次。

## v2.1 实现约束澄清

嵌套memo采用保守准入：首批复用透明单源列投影链的完整schema边界；复杂查询、函数/常量、索引hint、临时/外表、订阅特例及ENUM/SET边界继续普通绑定。所有形状的根结果仍按完整权威路径推导，支持范围不由memo资格决定。原因是外层成功可来自裁剪未用MATCH等表达式，不能证明该中间View任意输出都可跨根复用；也不能以独立预描述给父绑定新增错误边界。缓存是否命中与真实bind次数分别记录和测试，不宣称所有DAG只绑定唯一对象一次。后续扩大资格必须新增对应语义证明和差分。

## 当前请求边界与交付验证口径

`TxnCompilerContext.NewViewSchemaRequest` 仅供拥有当前statement/事务的调用者显式使用，要求调用者提供每根授权函数；现有公共SQL入口不自动构造它。首次 Describe 借用查询执行代并冻结绑定变量、账户/角色、事务快照/workspace代与协议能力；使用独立child compiler，关闭不结束借用事务。provider失效返回可重试目录变化，解析/绑定原错误和父取消原因不吞并。

实际硬界限：每份持久定义/根输入/快照16MiB、每次描述输入副本合计64MiB；每次解析32768 token和64MiB扫描工作；View深度64、每根4096次View展开及4096列槽；请求最多65536根/逻辑展开；每根binder递归256和65536次入口工作；memo最多4096项且编码保留字节（含固定开销）16MiB，完整编码结果16MiB，冻结变量16MiB。请求工作deadline为首次Describe后30秒或更早父deadline。缓存命中重放逻辑深度/展开/列槽；可选缓存满额继续权威绑定，必要内存准入先逐出缓存。直接保留的输入副本、变量、memo和结果使用现有查询执行代；这些上限不等价于整个调用链RSS上限。

结果包含完整有序列、来源元数据与View边界CTAS default策略、完整依赖、推导及持久化定义要求的最大协议版本。返回的各字段图独立持有；不导出旧QueryBuilder槽位。句柄须Release，Close等待句柄退场；调用者已取得的独立列/依赖/来源副本在请求关闭后仍有效。取消不破坏已发布结果，错误不进入memo。opt-in请求在根及每个嵌套View绑定或memo复用前要求明确持久化的创建时名称大小写模式及`DefaultDatabase`字段；任一必要上下文缺失（含null）返回`LEGACY_CONTEXT_UNAVAILABLE`受控不支持错误，不从调用者、父View或对象所在库猜测历史环境。CREATE VIEW正常保存的`DefaultDatabase=""`表示已知no-USE环境，允许完整限定的表来源及无表表达式；该环境下未限定且需要数据库的来源返回NoDB，不继承外层默认库。缺失字段与明确空值是不同状态，不能混为一谈。订阅发布库身份取可见解析对象`ObjectRef.SchemaName`，不是创建时`DefaultDatabase`；发布账户、发布库、订阅别名三者一致才能复用当前订阅上下文。每次describe在entry root lookup前暂时清空child继承的subscription，正常frontend查找和根依赖捕获使用同一subscriber域；普通local根的来源绑定/memo维持该域，只有真实解析的published根才能从obj identity建立publisher域。原继承值只在identity完全匹配时用于保留publication membership，整个root作用域在成功/错误/取消/memo早退时恢复，parent不变。进入持久化publisher SQL后，创建默认库和显式限定的数据库名称始终保留原名，包括恰好与subscriber别名同名的真实publisher库。该来源命名空间不做SubName↔DbName字符串改写，root/nested、DB-ID（含historical source库存在性校验）和UDF元数据解析保持一致；publication库在源snapshot时尚未创建不影响已存在的publisher来源库。普通SQL/Regenerate历史别名兼容分支不在本次opt-in修复范围；以显式root-resolver边界区分entry与source阶段，不能靠已有active subscription或名称拼写猜测。no-USE空值保留。私有frontend child的按名、ById和index的订阅来源表解析均借用publisher账户域，跨库名称保持原样，不依赖来源数据库本身也是订阅才能切换账户；历史snapshot克隆并指定publisher tenant，实际读取及返回后的依赖账户计算一致，系统表仍遵循最终system-account override。subscriber临时表及index名字映射不进入外账户的读取域，同账户own-DDL保持原语义；上下文在成功/错误/取消后恢复，root授权与parent Session身份不改变。nested memo键包括各定义自身保存的名称大小写模式；复杂形状完整支持但旁路nested memo。旧公共路径的兼容fallback保持不变，等待单独的consumer激活阶段。

共享目录SQL及入口授权中的目录读只在本请求上下文设置既有内部statement标记，读取当前写集但不推进父workspace边界；普通SQL没有该标记。临时跨账户解析上下文传给私有child，返回或失败时恢复，依赖账户与实际物理读取一致。可选nested memo先检查列/来源的protobuf编码大小，超限直接旁路；避免宽中间View把同一大default在准入前重复复制和序列化。

单元测试使用权威CREATE VIEW差分、cache-off对照及实际binder/查询预算边界。真实目录验证位于frontend的integration标签测试，复用单CN fixture，通过真实引擎、同事务DDL、历史快照、共享目录SELECT及订阅/RBAC校验所有权；fixture接线只编译入_test产物。公共入口虽未激活S2，保留的scanner/数值范围错误、JSON_ROW错误传播及共用View推导需要现有公共View/解析BVT回归。最终证据须以合并mo/main后的源码为准，未运行检查不得标成通过。

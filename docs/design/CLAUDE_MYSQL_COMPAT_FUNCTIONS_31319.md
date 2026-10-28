# MySQL 兼容函数与运算符补齐设计

版本：v6，2026-10-28。归属：#31319（原始报告 #23010、#23024、#23025、#23035）。STATEMENT_DIGEST 的兼容目标为 MySQL 8.4 逐字节一致的摘要，而非近似指纹。

## 范围、不变量与设计门槛

本功能扩展 SQL 公开契约，涉及 parser、AST、planner、函数求值及错误映射，设计门槛适用。目标是完整闭合四项能力，不以自定义指纹替代 MySQL 摘要，不改变裸 DEFAULT 的写入契约。默认不扩展分区、存储过程、UDF 或 warning 兼容性。

- `a SOUNDS LIKE b` 等价于 `SOUNDEX(a) = SOUNDEX(b)`，各操作数仅求值一次，参数顺序、NULL、二进制/字符域保持不变。
- 对支持解析的 MySQL 8.4 SQL、相同 SQL mode 和默认摘要预算 1024 字节，两个摘要函数的文本与哈希必须与独立 MySQL 8.4 oracle 相同；不能哈希格式化文本或 MO 的 token 编号。
- `DEFAULT(col)` 读取所引用列的默认值元数据，而非当前行值或赋值目标列默认值；必须保留列来源与无来源表达式的区别，适用的 MySQL 错误不能被 NULL/零值掩盖。

## SOUNDS LIKE

parser 将其降低为既有两个 SOUNDEX 调用的等值比较。SOUNDS 是非保留关键字；采用上游 #31373 已合并的 scanner 上下文识别：下个真实 token 不是 LIKE 时恢复 ID，从而保留 MO 已支持的隐式别名。前瞻快照继承 SQL mode、可执行注释状态及解析工作/取消预算，并用已有递归防护标记避免连续 SOUNDS 触发递归。删除本分支重复的 lexer 前瞻；普通 LIKE、显式别名和限定列名不受影响。

该方案优于新增 ComparisonOp 或独立执行函数：既有 AST、格式化、参数遍历、binder、执行器和持久化 view 消费者均可复用，不新增函数 ID 或 plan 字段。语法生成器必须无新增冲突，生成物必须可重复生成。

## STATEMENT_DIGEST / STATEMENT_DIGEST_TEXT

### 契约与编码

参考 MySQL 8.4 文档及本机 MySQL 8.4.11 的独立返回值。`SELECT 1` 的规范化文本是 `SELECT ?`，摘要是 `d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae`；直接 SHA256 文本会得到不同值，明确禁止。

共享核心位于 MySQL parser 包，函数包仅负责输入转换、向量选择/NULL、会话模式以及结果包装：

1. 使用现有 MySQL parser 校验单条语句，不执行 SQL，不解析表/catalog，不创建背景会话。空输入、多语句、裸参数占位符和语法错误拒绝；NULL 参数返回 NULL。此无目录边界也意味着不复制 MySQL 的系统变量注册表：未知系统变量的服务器相关存在性错误不作为纯语法兼容保证。
2. 使用现有 scanner 获取词法单元，翻译为冻结的 MySQL 8.4 token 兼容编号。token 用小端 uint16 编码；标识符附加小端字节长度与原始标识符字节。兼容编号和关键字/原生函数别名是互操作数据，不使用 MO goyacc 编号。
3. 归约数值/字符串字面量、合法 NULL 字面量、单值/多值列表、IN 列表以及数值的一元符号；保留结构、标识符、运算符、BOOL 常量和语法位置中的 NULL。NULL 的字面量角色由语法动作在本次可选摘要解析状态中标记，不用正则猜测 `IS NULL` 或 DDL 的 `NOT NULL`。
4. 普通注释不参与摘要；可执行注释继续按 scanner 规则处理；可识别 optimizer hints 按 MySQL hint token 域处理，不当普通注释静默删除。
5. 采用 MySQL 默认的 1024 字节 token 存储预算；新 token/完整标识符无法容纳时停止采集，不能截断单个编码项。即使摘要前缀已满，完整输入仍须通过语法校验。
6. TEXT 从同一规范化 token 流生成，采用 MySQL 的标识符引号、空格及列表占位符；DIGEST 对该编码字节流作 SHA256，返回 64 位小写十六进制。两个输出共享一个规范，不维护相互漂移的两个算法。

兼容数据生成工具只提取公开 token 数值/关键字拼写等互操作事实，输入版本和来源可追溯；不把 MySQL GPL 源码或实现复制进仓库。实现使用独立的 Go token 模型与现有 MO 解析器。独立 oracle 固定在测试 fixture 中，不依赖在线 MySQL 才能跑 UT。

### 备选、生命周期与预算

- 拒绝 SHA256(AST 文本)：可直接用 `SELECT 1` 证伪。
- 拒绝引入完整第二个 SQL 引擎、调用外部 MySQL 或无限缓存：增加依赖、权限边界、网络和状态所有者，不符合标量函数契约。
- 选择现有 parser/scanner 加兼容编码层：语法和转义已有所有者，兼容编码有单独独立 oracle。

每次调用拥有 scanner、语法树、可选 NULL 位置集及摘要缓冲；立即注册释放。数据不进入全局可变缓存，失败/取消路径同样释放。解析资源通过现有 ParseLimits 和调用者 context 限制；输入/工作/词法状态必须有界，最终实现记录具体预算，不能因摘要只保留 1024 字节就跳过后半段错误。向量常量每批只处理一次，NULL 和 selection mask 不解析；列向量逐行处理，不保留跨行状态。SQL mode 在执行期解析，因此函数不能错误地在 prepare 时常量折叠。

## DEFAULT(col)

### 所有者与数据流

`DEFAULT(col)` 与裸 DEFAULT 含义不同。选择独立的列默认值 AST 表达式，以免已有 INSERT/UPDATE/生成列处理把它误当作目标列的裸 DEFAULT。它只接受列引用（可限定名），不接受一般表达式。

binder 在正常命名空间中解析列，复用已有 `OutputColumnProvenance` / `SourceColumnMetadata` 传递的 planner-local 来源快照。透明投影、派生表、CTE 和 view 复用同一来源；语义表达式、聚合/集合边界按独立 MySQL oracle 决定是否采用输出类型默认值，不能随意追到任意底层列。只在确实缺少所需元数据时扩展现有本地快照，不新增 catalog/proto/持久格式。

VALUES/REPLACE/ON DUPLICATE KEY UPDATE 通过本次 DML 已解析的目标列定义提供默认值命名空间；SELECT/WHERE/UPDATE 的普通列作用域复用原有鉴权和歧义/未知列错误。不得运行绕过会话权限的目录 SQL。

### 已有独立事实与必需边界

- 显式字面量默认值读取其值及类型；可空且无默认值返回 NULL。
- 无默认值的 NOT NULL 列和生成列按 MySQL 1364 拒绝；自增列返回类型零值而非分配序号。
- 表达式默认值按 MySQL 3773 拒绝，不能复用 INSERT 中执行表达式的行为。
- 动态时间默认值必须按 MySQL DEFAULT 函数的列元数据结果处理，不运行 CURRENT_TIMESTAMP。
- 原始报告中的 `(SELECT 1 AS col)` 结果为 0，不是 1。
- `UPDATE t SET b=DEFAULT(a)` 读取 a 的默认值，不是 b 的默认值。
- 表别名、限定名、外层相关列、无来源派生列、外连接、view、UNION/CTE、参数计划复用等分别由最小 oracle 与 UT/BVT 验证，不能只验证物理表成功路径。

不在运行期创建新的状态或资源。默认值表达式来自不可变元数据快照，返回前复制；不污染共享表定义，不破坏普通写入默认值或 prepared plan 的 schema 校验。

## 兼容、交付与验证

- 所有新函数 ID 追加，既有 ID 不重排。无 wire/catalog 格式变更，不新增 MORPCVersion、capability gate 或迁移步骤；MO 使用全停机升级。
- 不新增权限、跨租户或外部网络路径；SQL 文本只解析，错误不应暴露输入中其他行的敏感数据。
- parser 生成物与 token 兼容表由各自生成器维护，不手改状态表；兼容表升级必须同时更新来源版本与独立 oracle。
- UT：parser/typed AST、兼容 token/hash/text、非法输入/预算/取消、NULL/selection/常量与列向量、DEFAULT 来源/错误和真实 binder 消费者。
- BVT：用本任务服务和隔离 mo-tester workspace，独立运行新增/扩展 case；生成结果后逐项对照 MySQL，正常比较两次并验证资源清理。SQL 注释不写 issue 号。
- 证据顺序：聚焦回归 → 所有者包/直接消费者 → 增量 gofmt/vet/lint → 服务构建/BVT。只有可达并发/持久化/分布式风险才增加 race/重启/多 CN 证据。大输入成本用专门 benchmark 或最小预算边界，不把普通 BVT 变成负载测试。
- 最终自审必须读取完整变更地图，关闭全部语义与证据阻塞项，确认修改代码覆盖率至少 75%。TODO、原始参考源码、临时工具、日志和证明叙述不提交；本设计及维护性回归 fixture 可以交付。

## 设计决策记录

- 相同 UNION 分支的 DISTINCT 折叠属于优化，不是 SQL 类型默认值边界的删除。折叠前捕获 provenance，保留 UNION 的三态时间可空性、纯 NULL 与整数精度；内部 grouping-set 的共享输入 UNION 保留原投影的整数精度身份，物化字段不套用户 UNION 的默认值策略。
- HAVING 的 DEFAULT 虽不依赖行值，仍受 HAVING 可见列约束：已声明的源分组列优先于同名 SELECT 别名，其次匹配输出别名和直接投影列；不因表达式或聚合引用了底层列就授予可见性。分组列可见性使用 HavingBinder 的逻辑分组注册表，不使用仅表示小计分支函数依赖的 active grouping 位图，因此 ROLLUP/CUBE 的非活跃声明列仍能读取元数据。复用 HavingBinder 所有的名称/列身份索引，不创建伪造行引用或聚合；聚合参数及外层相关列仍使用各自原有作用域。
- 系统变量的 scoped name 按词法引号域编码：反引号和 ANSI_QUOTES 双引号为标识符，普通单/双引号字符串为占位符；无 scope 的字符串语法拒绝，不把它伪装成注册表存在性校验。
- CREATE 和 ALTER ADD/MODIFY/CHANGE 共用列属性上下文化规则；生成列拒绝 DEFAULT（包括裸 NULL）、AUTO_INCREMENT、ON UPDATE、COLUMN_FORMAT 和 STORAGE；COMMENT、NULL/NOT NULL、索引/字符属性及普通列上的属性通过独立控制组保留，不按建表执行期的键约束误拒绝其他合法摘要。
- 可执行注释在 digestMode 中按普通 SQL 字符串规则扫描，不沿用 MO 普通解析对注释内单引号的特殊跳过/拼接规则；普通执行 parser 的既有契约不变，版本过滤/工作预算仍由原 scanner 所有。
- hint 尺寸后缀区分大小写：大写 K/M/G 为数值，小写为名称。小数点之后没有数字（例如 1.）的前缀丢弃，不产生虚假数值 token；所有 text/hash 控制组来自独立 MySQL oracle。

- 一元 `+` 的列默认值引用须在投影 AST 边界保留，包括嵌套括号、派生表物化、HAVING/ORDER BY 别名。MO 的数值降低可能改变行值类型，因此使用既有 provenance 中单独的 `ColumnDefaultSource`，只供 DEFAULT 命名空间读取，不伪造通用透明来源或扩大 CTAS 默认值继承；它复用不可变来源快照，无新的持久状态。负号、显式 CAST 和算术表达式仍不是来源列。
- 字符集引入符必须来自非限定 ID token，不能由普通字符串内容或限定标识符触发；国别字符串只认紧邻的单引号。数字前缀标识符保留原始大小写，已完成的 SQL 指数数值与后续别名分开；这些兼容分支只在 digestMode 生效。
- hint 数字域独立于 SQL：`1e2`、`1abc` 是名称，`16K` 是尺寸数值。第一项有效 token 到来时才写 hint 开始标记，避免空白、被跳过的点或首项词法错误产生虚假的空 hint；满额前缀仍不可变。
- CREATE TABLE 只模拟列属性上下文化，不执行表定义校验：内联 PRIMARY KEY 按属性顺序设置非空，表级 PRIMARY KEY 不在此阶段应用；AUTO_INCREMENT 拒绝非裸 NULL 的显式默认值。有效控制组与拒绝样本均来自独立 MySQL 返回值，不能按最终建表合法性扩大拒绝范围。

- 兼容基准为 MySQL 8.4 逐字节一致的摘要，不使用近似或自定义哈希。
- 已评审：共用规范化流、现有 parser/scanner、正常列命名空间及已有来源快照，优于新引擎/远程查询/重复状态。
- 实现过程中发现的语义差异必须先补充 oracle 与本设计；不会以关闭测试、放宽断言或隐藏已知差异通过验收。
- 实际服务对照发现：显式 DATE/TIME CAST 的派生列默认值为 NULL，但 DATE/TIME 类型字面量和 CURRENT_DATE/CURRENT_TIME 的默认值为零时间；不能粗暴把全部时间类型判成 NULL。来源信息应在投影 AST 尚未被常量折叠/恒等 CAST 消除时捕获，并通过既有 provenance 传递。
- 实际服务对照发现：多赋值 UPDATE 会创建顺序行映像的临时 Binding，若只维护值类型而没有原始列来源，DEFAULT(a) 会误报无默认值。物理表和顺序 UPDATE 的 Binding 共用同一个来源初始化入口；当前行值可以变化，原始默认值元数据不随赋值变化。
- ENUM/SET UPDATE 不在没有列作用域的临时 DefaultBinder 中预先绑定 RHS；删除重复 AST 转换，统一由既有 normalizeUpdateAssignment 在真实 RHS 绑定完成后转换。
- DEFAULT 的输出字段声明与“透明行值来源”不同：CTAS 需要保留 ENUM/SET 定义，并为非空输出构造类型默认值（整数 0、字符串空串、ENUM 首成员、SET 空集）；但合并派生表达式 DEFAULT(DEFAULT 的别名) 仍须拒绝。通过既有输出 provenance 的结果类型及 CTASDefaultPolicy 表达，不伪造 SourceColumn。物化 ENUM 临时字段的 DEFAULT 为索引零/空串，不能误用 CTAS 的首成员默认值。
- Prepare 的内部重新规划可能重复 binder。DEFAULT 的 strict 判定须随解析 AST 捕获，复用既有 prepared schedulingSQLMode 的克隆解析快照，而非每次重绑读取实时会话；DIGEST 仍明确读取执行期模式。无需新增前端 prepare 状态或 wire 字段。
- 时间字面量与同名转换函数必须在 AST 中保留区别，并在 view 格式化/重新解析后仍然保留；临时输出字段使用三态可空性来源（继承、可空、非空），不能从已折叠的值倒推声明属性。
- ROLLUP/CUBE 内部 UNION 的物化输出使用普通临时字段默认值，不得引入只属于用户 SQL UNION 的 strict 时间错误。仍按全部分支的真实 nullability 区分日期常量（零日期）、CAST（NULL）和实际分组列的小计 NULL。
- UNION 临时字段与普通无 FROM 派生字段还需保留来源种类：strict 模式下非空 DATE/DATETIME/TIMESTAMP 的 UNION 输出没有隐式默认值，报 1364；TIME/数值、可空转换结果不受影响。MySQL 将这条规则固定在 PREPARE/bind 时，后续仅切换 strict 模式不会改写已准备的默认值；不为此改变 prepare 的 SQL-mode 缓存协议。
- 独立 oracle 确认 HAVING 的 SELECT 别名优先于同名的未分组物理列；已声明的同名源分组列优先于别名。ORDER BY 表达式中的列优先于别名；相关作用域允许在内层同名表无该列时继续解析外层。元数据解析必须分别保持这些命名空间规则。
- hint 有独立词法域，不能把其内容当普通 SQL 表达式扫描：其中的 `?` 是标点而非参数，复合运算符按单字符编码，`--`/`#` 不开始行注释，字符串前的 N/字符集/X 保留为标识符；标识符与 @ 的空白也影响 token。复用现有字符串、数字和标识符扫描器，但用局部 hint 分派入口，避免给全局 scanner 增加状态。
- 摘要达到 1024 字节后进入不可变状态：追加和归约均不得修改已保存前缀；仍扫描后缀以拒绝参数占位符。长前缀后接字符集引入符或 hint 的独立 oracle 反例已加入 fixture。
- 摘要返回类型为可空 VARCHAR(64)/LONGTEXT，字符域复用 MO 已支持的显式 utf8mb4_general_ci 标识（与既有文本函数一致）；本任务不新增 MySQL utf8mb4_0900_ai_ci 的排序引擎。文本及 hash 的内容兼容不依赖这一现有元数据边界。
- 摘要解析的资源预算固定为每个参数最多 1 MiB 输入、65536 个 parser token 和 8 MiB scanner 工作量（语法校验和编码扫描各有独立同额预算）；尊重更紧的上层 ParseLimits。错误统一使用 MySQL 3677 的不披露输入版本，取消原因和资源超限原样传播。

# View 元数据按需生成：统一列行源设计

版本：v1.0；状态：APPROVED（用户明确选择按需绑定方案并授权完整实施）。
所属：#26227；替代 #29005 的 View 派生元数据恢复机制；关联 Draft PR #29139。
研究代码基线：a939ed2abd。日期：2026-09-22。
用户已批准研究、原型和本轮设计整理，不等于对本文新增契约的最终批准。

## 1. 决策摘要

推荐 View 语义定义为唯一权威，读取时绑定生成完整输出描述。DESC 与 I_S 使用同一生成器，CTAS 复用来源语义但保留独立 default 规则。不新增持久依赖图、worker、lease、COMPLETE 或跨请求 schema 缓存。

首版迁移目标是**不再信任派生列作为当前元数据**，不是立即物理删除所有 View 列记录。遗留结构删除另设迁移门禁。

停止研究版 sys/dump 白名单、按 SQL 特例改写及旧列骨架覆盖的扩展：它们只提供可行性证据，不能成为生产路径。

## 2. 证据与非证据

已证明：
- planner 在 View.Cols 为空时可为已测试的直接投影、CTAS 和 LEFT JOIN 产生描述。
- 真实 SQL overlay 下，DESC 与定向 I_S 随源宽度 5→60→90 变化，mo_columns 仍为旧 5；失效兄弟 View 不影响定向查询。
- 原始 star 与规范化 star 的升级行为不同。
- 简单完整绑定的 mock benchmark 约 0.104–0.109ms/View、66.5KB 分配；不含目录 RPC。

未证明：完整字段/权限/历史快照、真实全库 I_S、所有缓存入口、移除旧列后的全部引擎消费者、混合版本。前期 benchmark 不作为生产容量承诺。

原型及日志索引：根目录 `CLAUDE_VIEW_METADATA_ON_DEMAND_STUDY_2026-09-21.md`、`CLAUDE_VIEW_METADATA_IS_PROTOTYPE_2026-09-22.md`。

## 3. 不变量及反例

对同一语句可见目录状态 S 与绑定上下文 C：
`CurrentColumns(v) = Describe(Bind(SemanticDefinition(v), S, C))`。

- 完整类型、default、nullability、特殊类型来源规则共用一个权威推导，不按输出名字回查源列。
- 绑定前，候选必须通过该入口的可见性/权限检查。不可见对象不得导致绑定、源目录访问或泄露其失败。
- 读操作不修改 View、源表、权限或恢复状态；不启动独立事务读取“最新”状态。
- 无可用描述时单对象受控失败；批量 I_S 依第 8 节跳过并告警，不回退旧列。
- 合法旧事务按旧快照观察，不能要求不同快照跨 DDL 结果一致。
- 重启/请求失败只丢弃请求状态；下一请求重算，不维护完成真相。

反例：先全库绑定再 WHERE 过滤，既会被无关坏 View 阻断，也可能泄露不可见对象；把 DESC 结果常量化后直接复用缓存计划，会重新引入陈旧 schema。

## 4. 现有边界核查

- `sysview/predefined.go` 的 `informationSchemaMetadataVisibilityCTEWithActiveRoles` 已定义完整 active-role 可见对象集合；应抽出可复用候选查询构造器，不复制第二套手写 RBAC。
- `frontend/compiler_context.go:GetSubscriptionMetadata` 使用调用者事务或历史 snapshot clone，在访问 publisher 前过滤 subscriber 可见集合；复用该语义，不假设 subscriber role ID 在 publisher 有意义。
- `table_function/subscription_metadata.go:start` 当前创建流式 producer，并传调用者 TxnOperator 给 sqlexec。它证明目录行源有先例，**不证明可安全在该 goroutine 中复用 session compiler context**。
- `process.types.go:sqlHelper.GetCompilerContext() any`、`compile/sql_executor_context.go` 已有桥接；返回的是现有 compiler context，不能直接并发调用其可变 binding/subscription 状态。
- `plan/subscription_metadata.go:PreparedPlanDependsOnSubscriptionMetadata` 已识别规划期固化可见集合的缓存风险；新行源应执行时枚举而不是规划时生成全库 UNION 常量。

## 5. 数据与控制流

### 5.1 一个列描述生成器

从 genViewTableDef/bindView 中复用完整绑定结果和 OutputColumnProvenance，提取无目录写入的 `DescribeView` 内核。

输入：原始对象身份、语义定义、绑定上下文与指定 snapshot。
输出：有序完整列描述及该次绑定的依赖引用（仅请求内，用于现有计划验证或诊断）。

不要求生成新的 ViewData JSON、不写 Dependencies、不写 TypeDef、不进行恢复。首版可以保留现有优化步骤确保推导一致，再用数据决定是否裁剪；不能先假设 parse-only 足够。

对象的不可变语义定义保留 SQL mode、默认库、显式列名、规范化投影、名称模式、安全属性和已有表达式协议标记。

### 5.2 动态行源

逻辑行源称 `CurrentMetadataColumns`，名称为设计占位，不新增用户可调用特权 API。

执行过程：

1. 创建当前执行代的只读绑定环境，捕获同一事务、快照、用户/活动角色、订阅上下文。
2. 在原始 mo_tables/授权目录上分页枚举**可见**候选。禁止通过 I_S.COLUMNS 枚举，避免自省递归。
3. 普通表读取现有列记录；用户 View 调用 DescribeView，直接生成全部列行，**不 join 旧 View mo_columns**。
4. 将统一列行交给现有展示函数，产生 I_S 字段；残余 SQL 条件、排序、聚合由正常关系算子执行。
5. 成功、失败、取消或上游提前停止均关闭 cursor，释放所有请求级内存。

系统 View 的固定 schema 由系统定义注册边界提供；绑定 I_S 行源的 schema 不执行该行源。用户 View 引用 I_S 时，只绑定静态行源 schema，不在绑定期递归枚举它代表的所有 View。

### 5.3 DESC

保留现有 SHOW 入口权限检查，然后通过同一 DescribeView 返回列；FULL/LIKE/WHERE 使用共享描述上的过滤和展示，不再查询旧 View 列。

SHOW CREATE、DROP、ALTER 修复失效 View 必须能获取原始定义，不强制 DescribeView 成功；因此不把动态绑定无条件塞进底层 Resolve。

### 5.4 CTAS

复用本次绑定的来源与 nullable 边界，不从旧 View.Cols 取当前 nullability。保留 View default 与 CTAS default 的独立转换；研究中 7 与 0 的差别是正确行为，不统一成相同文本。

## 6. 实现归属与请求级接口

拟定职责：

| 所有者 | 职责 |
|---|---|
| planner | DescribeView 推导、固定行源 schema、安全候选约束提取、缓存标记 |
| frontend/context provider | 调用者权限、活动角色、snapshot、订阅，创建独立只读绑定上下文 |
| origin CN 执行行源 | 拉取候选、绑定、批输出、取消与清理 |
| catalog/engine | 原始定义及普通表列存取，不加入 worker |

概念接口（非已实现 API）：

```
Open(executionContext, boundCandidateConstraints) -> cursor
cursor.Next(ctx) -> owned batch | EOF | error
cursor.Close() -> idempotent release
```

约束：
- 一个 execution generation、一个 cursor、串行 Next；不跨查询/租户复用。
- 独立 child compiler context，不共享父 QueryBuilder、可变 subscription 状态或 cached background executor。
- 明确借用调用者 txn；Close 不提交、不回滚、不关闭父 txn。历史 clone 的释放规则按实际 TxnOperator API 审核，不能猜测所有权。
- origin-CN-only，禁止远端重建缺少用户身份的 provider。plan 只携带可序列化约束及固定 schema，不携带 session pointer 或已枚举对象集合。
- 内部 SQL executor 也必须有同等 provider；不能在无 session 时默认 sys 或默默退回旧列。
- 推荐同步 pull 首版，避免新增流式 goroutine/channel 所有权；若底层 internal executor 只能流式，则单独审查 stop/drain/cancel，不直接复制现有 producer。

**落地前阻塞项：验证 origin-only operator 到独立 frontend/compiler-context provider 的依赖与生命周期，不将 GetCompilerContext() 类型断言当作已解决。**

## 7. 筛选与鉴权

候选约束只作为原 SQL 谓词的必要条件，永远保留完整残余谓词：

- 优先支持列源上的 TABLE_SCHEMA/TABLE_NAME 等值、有限 IN 与执行时已绑定参数。
- AND 可提取确定成立的约束；OR 只有所有分支都可安全表达时才提取，否则不剪枝。
- 不把 unsafe cast、volatile 函数或可能抛错表达式提前求值。
- 不跨外连接 null-extension、LIMIT、聚合等语义边界盲目下推。
- 不能先输出 LIMIT 数量候选后再过滤；LIMIT 只有在等价性已证明时才影响候选停止条件。
- 对不支持的筛选：扫描所有可见候选，不能返回旧元数据或漏行。

授权发生在绑定之前。I_S 行可见性与 DESC 的数据库 SHOW 权限是两个现有契约，不应强行合并。目标获准并不意味着调用者需直接 SELECT 每个源表；按既有 View definer/invoker 语义建立绑定上下文，跨租户访问必须先通过订阅规则。

不可见对象的失败不能成为旁路。候选分页、名称解析、诊断都必须携带 account 与对象身份。

## 8. 失败及一致性

经 MySQL 8.0.45 对照与用户在 2026-09-24 明确选择方案 2 后，首版契约：

- 单 View 绑定失败：错误，不返回旧描述。
- 批量 I_S 遇到可见且位于候选范围内的失效 View：仅当错误明确是缺失源表、源库或来源列时跳过该 View 的所有列，向当前语句的诊断 sink 提交 `Warning 1356 (HY000)`；其他合法 View 仍返回。warning 数量精确计数、可供客户端 `SHOW WARNINGS` 查看，诊断记录按现有容量上限保留；不返回旧列。
- 超出已确认的失效依赖错误集合、定义格式错误、协议不支持、资源上限、权限错误等保持原失败语义，不能把内部故障伪装成无效 View。View 源库探测不得使用会吞掉目录/存储错误的布尔 `DatabaseExists`：应使用可返回错误的目录查询，只有权威缺失（`OkExpectedEOB` / `ErrBadDB`）可转为可跳过的依赖缺失，其他错误原样向上传播。
- 不可见或被安全候选条件排除的坏 View：不绑定，不报错也不产生 warning。
- 超时/取消：停止工作，清理 cursor；不自动换快照重试。
- DDL 冲突：交由既有语句重试规则，新执行代丢弃所有前代描述；禁止混合重试前后结果。
- 引用循环：绑定链检测失败；深度/工作量预算防非循环放大。

MySQL 8.0.45 对照实测（独立 `mysql:8.0.45` 容器，`SELECT VERSION()` 确认；2026-09-24）：

复现命令：

```sql
CREATE DATABASE claude_view_metadata;
CREATE TABLE claude_view_metadata.src (x VARCHAR(5), keep_col INT);
CREATE VIEW claude_view_metadata.good AS SELECT keep_col FROM claude_view_metadata.src;
CREATE VIEW claude_view_metadata.broken AS SELECT x FROM claude_view_metadata.src;
ALTER TABLE claude_view_metadata.src MODIFY COLUMN x VARCHAR(60);
SELECT table_name,column_name,character_maximum_length FROM information_schema.columns
 WHERE table_schema='claude_view_metadata' ORDER BY table_name,ordinal_position;
ALTER TABLE claude_view_metadata.src DROP COLUMN x;
SELECT table_name,column_name FROM information_schema.columns
 WHERE table_schema='claude_view_metadata' ORDER BY table_name,column_name;
SHOW WARNINGS;
DESCRIBE claude_view_metadata.broken;
```

观测结果：

1. 创建 `src(x varchar(5), keep_col int)`、`good AS SELECT keep_col FROM src`、`broken AS SELECT x FROM src`，查询 `information_schema.columns` 返回两个 View 的列。
2. `ALTER TABLE src MODIFY COLUMN x varchar(60)` 后，再查 I_S，`broken.x` 宽度变为 60。
3. `ALTER TABLE src DROP COLUMN x` 后，`SELECT TABLE_NAME,COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA='claude_view_metadata' ORDER BY TABLE_NAME,COLUMN_NAME` 成功返回 `good.keep_col`、`src.keep_col`，**跳过 `broken`**；`SHOW WARNINGS` 返回 `Warning 1356 (HY000)`，提示 View 引用失效。同样只筛选 `TABLE_NAME='broken'` 时返回空结果与同一 warning。
4. `SELECT * FROM broken`、`DESCRIBE broken`、`SHOW FULL COLUMNS FROM broken` 均返回 `ERROR 1356 (HY000)`。

此前实现对批量 I_S 的可见失效 View **使整个查询报错**，与 MySQL 8.0.45 的“跳过并发 warning”不一致；不能将单对象 DESC 的 MySQL 行为推断为批量 I_S 契约。实验原始输出保留在本机 `/tmp/claude-pr29139-mysql8045-evidence.log` 与 `/tmp/claude-pr29139-mysql8045-warnings.log`；SQL 和关键结果已记录于此，临时容器已清理。**产品选择：用户在 2026-09-24 的本 PR 修复会话中明确选择方案 2，并在询问 warning 机制后再次确认继续实施。** 按对照结果修改批量扫描，保留单对象 DESC/SHOW 的错误。

## 9. 资源预算与性能

资源属于请求；无全局缓存、持久队列、后台任务。

拟定验证起点（不是已测容量承诺）：候选页 32 个；单个 View 描述至多 16MiB；单次请求 View 绑定展开次数至多 65536、嵌套深度至多 64。普通表枚举不计入 View 绑定次数。实际数值需结合现有 planner 限额和生产目录规模统一，不能重复冲突限额。

- 深度及工作量限制必须在递归入口/展开前扣减；生成完整描述后才检查大小不能防 OOM，构造中也要计费。
- 输出 batch 复用现有内存账户与行/字节阈值；一页有界不能代替全部中间 AST/优化器分配的预算。
- 超额报错，不截断、不返回假完整结果。
- 串行绑定首版无并发扇出；普通表热路径不得调用 View binder。
- 请求内 memo 非首版必要条件；只有 benchmark 证明需要后增加有界复用，不提前引入新的缓存正确性问题。

验收测量：单对象、定向/全库元数据、共有子 View 与深链、普通表控制组、权限稀疏目录、取消后的保留内存。已有 128 个简单 View 约 13ms 的 mock 结果不能代替真实服务扫描对照。

## 10. 缓存与协议

- 公共元数据读取标记执行时生成；不得缓存描述值或候选列表。准备语句每次执行重新枚举授权集合。
- 首版若暂不能证明缓存安全，明确禁用相关动态元数据计划复用；不影响普通表 SQL 缓存。
- 同一执行代冻结 snapshot/语义上下文；RC snapshot 推进、重试与 provider 要有明确协调，不能一半新一半旧。
- origin-only metadata operator 的 wire/调度标记需要混合版本支持检查。
- 新旧 CN 混跑仍需要发布边界，但没有全目录 COMPLETE/recovery 的必要。已有共享表达式协议/HAKeeper 功能不可一并删除。

## 11. 迁移与回滚

阶段 A：保留 catalog 结构及历史列行，所有受支持公共入口转向动态描述；这时不能宣称 mo_columns 原始读取也最新。
阶段 B：完成内部直接列消费者、备份/restore/clone、View.Cols 使用审计和旧 star 规范化，证明它们不以旧派生列为当前真相。
阶段 C：另审物理去列/停止写列格式；旧二进制回滚兼容必须先解决。现研究不授权执行该步骤。

遗留 raw star 缺失历史绑定信息时可能无法无损恢复；不能猜测映射或悄悄改变输出列集合。保留兼容记录或明确受控失败策略，需产品决定。

整群升级到支持版本后再切换共享 I_S 定义；门禁不满足时不得让新 CN 写入旧 CN 不认识的定义。回滚阶段 A 会恢复原缺陷，不可将其描述为仍保证当前元数据；阶段 C 前才可谈旧格式可读回滚。

PR #29139 的 View 专用恢复机制在替代设计获批后决定撤换；暂不删除任何已合并共享协议，不改变远端历史、不关闭 issue。

## 12. 备选方案

- 保留 durable recovery：读取便宜，但 mutation/restore 闭包、迟到 proposal、租约、恢复完成证明均是持续负担。
- 每次绑定并写回：若每次都绑定，写回无助读取成本；若以后信任写回，重新需要完整失效协议。不选。
- 规划期全库 UNION：现有订阅统计有类似受限先例，但大量 View 会膨胀计划并固化对象/权限集合，不选作为通用列源。
- 定向 SQL 改写：原型可用，但无法覆盖所有 SQL 形状/权限/全枚举，不作为产品实现。
- 执行期有界行源：推荐；把复杂度收敛为一个读取算子和一个绑定服务，代价是读取 CPU 与 context 生命周期设计。

参考兼容契约：MySQL 8.0 CREATE VIEW 文档（定义冻结、无效依赖等）与 INFORMATION_SCHEMA COLUMNS 文档；具体错误与默认值以 8.0.45 对照实测为准，不以引用文档替代执行证据。
https://dev.mysql.com/doc/refman/8.0/en/create-view.html
https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html

## 13. 实施/验证拆分及门禁

1. 独立 provider 与 cursor：证明同事务、同租户、串行 owner、取消、EOF/重复 Close、无父 txn 清理、内部 executor 与 origin-only 调度。
2. 纯完整描述生成与 CTAS 边界：不含旧 View.Cols，类型/default/来源正反例覆盖。
3. 真实 I_S 与 DESC：不保留旧列骨架；先授权，再绑定；谓词/参数/外连接/全库扫描测试。
4. 兼容矩阵：MySQL 错误、raw star、snapshot/订阅、升级及缓存。
5. 原 mo-tester 生成并比对 BVT，变更生产覆盖率≥75%，风险映射 race/static/容量证据及最终自审后才提交替代实现。

## 14. 设计审查记录

范围：跨 planner/frontend/执行器/系统视图的架构调整，触发权限、持久化兼容、生命周期及热路径设计门禁。

决定：**PASS（含 2026-09-24 批量错误契约补充审批）**。用户选择 definition-only/按需绑定方向，并针对第 8 节 MySQL 8.0.45 对照证据明确选择批量扫描跳过无效 View + warning；该批准不授权将取消、协议、内部或资源错误降格为 warning。

关闭项：
- B1：执行期 provider 由请求级 `SessionInfo.CompilerContext` 所有，APPLY 强制 origin CN 串行执行；frontend、internal executor、subscription 和双 CN 实测通过。
- B2：单对象 View 描述失败仍报错；批量 I_S 在可见且进入候选集合的 View 失效时跳过该 View，向客户端产生 warning 1356；不可见或被安全筛选排除的 View 不绑定、不产生 warning。产品选择、MySQL 8.0.45 对照与公开 SQL 证据见第 8 节。
- B3：CREATE VIEW 已有 star 固化定义继续作为权威；按需绑定不读取旧派生类型。遗留规范化行为由现有 star 测试保持。
- B4：4.0.9 + protocol 97 控制混合版本切换（94 已由 CDC 占用，95 已由 prepared scalar precision 占用，96 已由 vector scan 占用）；每 scan 最多 65,536 个 View、每 View 4,096 列、定义 16 MiB，并响应 context 取消。

已作决策：拒绝先全库绑定后鉴权；拒绝全局 schema 缓存；拒绝管理员 SQL 特例；保留旧 `mo_columns` 仅作兼容存储，公共 DESC/I_S 不再将其作为用户 View 当前类型真相。

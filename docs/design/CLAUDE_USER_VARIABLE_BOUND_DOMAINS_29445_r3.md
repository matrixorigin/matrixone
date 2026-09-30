# 用户变量双轴绑定合同 r3：PR #29445 的 CI 修复设计

> 状态：具体设计已批准，进入实施。2026-09-28，用户在收到本稿及字段/迁移限制摘要后回复“go ahead”。批准快照 SHA-256：`04f9af860ef2e08cb40d09f9cd4fd380ae551c7c6043e487a2da719dc3af80ee`（添加本审批记录前）。这是本次具体方案的批准，不是借用先前的笼统继续授权。
>
> 本稿取代 r2 中“把有效求值域写进 Expr.Type.Charset”的实现选择，只覆盖共享用户变量前置修复。不是完整 REGEXP 设计的批准，也不关闭 #27217。

## 1. 身份、范围和门禁

- 实现 PR：https://github.com/matrixorigin/matrixone/pull/29445
- 关联问题：https://github.com/matrixorigin/matrixone/issues/27217
- 原 CI head：`d75b8498e06040a430ad29038376c119bf4755c3`。
- 本次设计基线：合并 `mo/main@d99187d7b3bfda8744088b3ae8cf16739b690aa0` 后的 `3dfe05cd32904f62db948f126dee82b81670303b`。
- 原前置修复曾按普通 bug fix 分类，不要求独立设计审批。**本次方案扩大到计划 schema 元数据和连接迁移行为，须重新审查这部分范围，不能沿用原豁免。**
- 保持原有 SQL 功能、session 保存值和 marker 规则；不修改通用 CHARSET/COLLATION 函数、不增加 REGEXP 特判、不改变向量格式、catalog 格式或共享客户端结果序列化。
- 本文明确需要确认的新边界：VarRef 一个独立字段；连接迁移对有冻结绑定的 prepared statement 采取拒绝迁移策略。不得无记录扩大为完整 prepared-state 迁移框架。

## 2. 问题与证据

### 2.1 已确认的 CI 回归

Run `36397790774`、job `108851328976` 中，`dtype/binary_string_function_semantics.test` 只有三个失败：SET、SELECT INTO、prepared SET 产生的 selected-text 用户变量。

```sql
SET @s = IF(false, X'ff', '你a');
SELECT CHARSET(@s), COLLATION(@s), CHAR_LENGTH(@s), HEX(LEFT(@s, 1));
```

| 观察 | CHARSET / COLLATION | CHAR_LENGTH / LEFT HEX |
|---|---|---|
| 仓库原 BVT 合同 | binary / binary | 2 / E4BDA0 |
| 当前 PR 的 CI 实际结果 | utf8mb4 / utf8mb4_general_ci | 2 / E4BDA0 |
| 独立 MySQL 8.4.10 实测 | binary / binary | 4 / E4 |

仓库已有“静态共同类型”与“选中行求值域”两个轴。它在后两列上与 MySQL 不同，本文**保留这项既有测试合同，不声称本 PR 消除了这项 MySQL 差异**；不能将前两列的回归改成错误预期来让 CI 通过。

原实现将已选中行的 text override 写入表达式静态 Charset，导致 CHARSET/COLLATION 再也看不到赋值时的 binary 静态身份。不能靠 OID 推回原身份：合法状态可以是 text OID + binary Charset。这已通过一次真实 BVT 否定；试探性 OID 特判已完全撤销，未 push。

### 2.2 绑定代际，而非永远不允许重绑定

新的独立 MySQL 8.4.10 证据 `CLAUDE_R3_ORACLE_27217.log`：

```sql
CREATE TABLE t(i INT);
INSERT INTO t VALUES (1);
SET @s = _binary'你';
PREPARE p FROM 'SELECT CHARSET(@s), HEX(LEFT(@s,1)) FROM t';
EXECUTE p;                       -- binary / E4
SET @s = '你';
EXECUTE p;                       -- binary / E4
ALTER TABLE t ADD COLUMN j INT;
EXECUTE p;                       -- utf8mb4 / E4BDA0
```

由此限定：普通重复 EXECUTE 不能受 SET 改变绑定；真正重新 PREPARE 或 schema 导致的自动 reprepare 建立新绑定代际。不能为“永久保存原变量类型”去干预既有 schema reprepare 机制。

参考：MySQL 8.4 user variables 与 statement caching 文档，以及上述固定 8.4.10 的实测；MySQL 并不是 MO 现有 selected-row 扩展的依据，二者必须分开报告。

- https://dev.mysql.com/doc/refman/8.4/en/user-variables.html
- https://dev.mysql.com/doc/refman/8.4/en/statement-caching.html

## 3. 不变量和首个 owner

定义每一个已绑定字符串 VarRef 的合同为 `(S, D)`：

- `S`：赋值静态类型的副本，包括 OID、Charset、Width 等；只归属该表达式，不写回 session。
- `D`：绑定时冻结的 RuntimeStringDomain，取 INHERIT / TEXT / BINARY。INHERIT 相对于不变的 S，不意味着执行时重新读 session 域。
- `V(t)`：执行时读取的最新 session 值；允许 SET 更新。

不变量：

1. 同一绑定代际求值使用 `S + D + V(t)`。SET 只影响 V(t)，不改变该表达式的 S/D。
2. CHARSET/COLLATION 与静态结果推导只看 S；字符串行消费者使用 S 与 D 合成的有效域。
3. 新 SQL / 新 PREPARE / 正式 reprepare 读取当时赋值的 S/D，旧 prepared 表达式不被回写。
4. marker 仍走原 per-execution 参数域；数值、BIT/IsBin、参数来源 kind、JSON/array、系统变量按原路径处理。
5. session 保存的值、类型和 runtime override 不被 binding 改写。
6. DeepCopy、序列化、远端折叠不能丢掉任何一轴，也不能在复制或构造 executor 时重新向 session 取域。

首个 owner 是 binder 创建的 VarRef；executor 只消费该快照，不拥有重新绑定权限。

## 4. 方案比较

| 方案 | 判断 |
|---|---|
| 继续改写 Type.Charset / 修改 BVT 前两列 | 丢失 S，已经证明错误，拒绝 |
| CHARSET/COLLATION 或 REGEXP 根据变量来源特殊取 session 类型 | 多 owner、不稳定，旧语句会读到新赋值，拒绝 |
| 全部用户变量只继承静态类型，不保留 selected-row 域 | 可以与本次 MySQL 三例一致，但会改变仓库既有行执行合同；不是本次选定方向 |
| AuxId / Width / Scale / Charset 高位编码 D | 污染已有字段合同，复制/优化/协议难以验证，拒绝 |
| executor 首次 Eval / 首次构造才捕获 D | 构造可能晚于 SET，每次 EXECUTE 也可能重新构造，不能证明绑定时冻结，拒绝 |
| 复用 PreparedNumericMetadata.string_domain_source 放伪表达式 | 该字段已有真实来源表达式的重写语义；增加伪节点/额外读取，不如直接标注 owner，拒绝 |
| **VarRef 一个明确的标量域字段，S 不变** | 最小可解释的双轴表示，选定 |

## 5. 精确表示和行为

### 5.1 计划 schema

拟在 `proto/plan.proto` 的 `VarRef` 新增 tag 4：

```protobuf
uint32 bound_string_domain = 4;
```

明确编码，不借用其他字段，不依赖截断 uint8：

| 值 | 意义 |
|---|---|
| 0 | 旧计划/没有绑定域；保持旧 resolver 路径 |
| 1 | 已绑定 INHERIT（host RuntimeStringInherit + 1） |
| 2 | 已绑定 TEXT（host RuntimeStringText + 1） |
| 3 | 已绑定 BINARY（host RuntimeStringBinary + 1） |
| >3 | 非法元数据；在 consumer 构造边界返回错误，禁止 uint8 截断成有效值 |

0 和 1 必须区分，否则旧计划和明确冻结的 INHERIT 无法分辨。只为非系统 MySQL 字符串 VarRef 生成非零值。若非字符串/系统变量携带非零字段，按非法计划拒绝，不能静默改变既有处理分支。

生成文件通过仓库 proto 工具链重新生成，保留 `proto/postprocess/plan_string_literal_form` 后处理，不手改 `pkg/pb/plan/plan.pb.go`。只交付本 schema 变化应产生的输出。

### 5.2 binder

- 继续使用既有类型 resolver 复制 S。
- 继续使用本 PR 的可选 UserVariableStringDomainResolver 获取 D；校验返回值。
- 没有可选 resolver 时，新绑定取 INHERIT，不新增强制接口和批量 mock 改造。
- 不再按 D 改 S.Charset。将 `D+1` 写入 VarRef。
- resolver 错误直接中止构建，不返回半绑定表达式。

### 5.3 executor

- `newExpressionExecutorWithAllocation` 先检查合法性，再获取池对象；将标量快照复制进 VarExpressionExecutor。
- 非零绑定字段的字符串路径只读快照，绝不调用当前 session 的 string-domain resolver；value / IsBin / prepare-param-kind 的处理不改。
- 0 值使用原动态 resolver 分支，兼容旧计划及明确未绑定的调用者。
- INHERIT 的有效域基于 S；遇到已有 vector 的 binary-OID fallback 与 S 的显式 text Charset 不一致时，仍使用既有 explicit TEXT override 桥接，不全局改 vector 规则。
- `SetRuntimeStringDomainWithMP` 继续承担 vector sidecar 写入；不增加每行 session 查询。
- Free/池复用清零、ResetForNextQuery、NULL、masked、错误恢复必须有对应测试。Reset 不重新绑定；新 executor 则从新计划读取自己的快照。

### 5.4 复制、重绑定、远端执行

- `pkg/sql/plan/deepcopy.go:DeepCopyExpr` 的 VarRef 分支显式复制新字段。只改已有复制，不添加额外整树 DeepCopy。
- ResetParamRefRule / prepared specialization 不得把 VarRef 当 marker 重新绑定；补证据证明执行副本保留两轴。
- `prepareRemoteRunSendingDataWithVectorProtocol` 已先调用 `foldVarExprsInRemoteRunScope`：在 coordinator 使用正确 S/D 求当前值。
- `foldVarExprsInExprInPlace` 保留 Expr.Typ，并通过 `rule.GetConstantValue` 产生带 LiteralForm/来源的 literal；复用现有 TEXT/BINARY override 的线格式，不发新的 vector 侧带格式。
- 对新绑定的 VarRef，远端发送前必须成功折叠；若仍有待远端求值的新绑定 VarRef，则在发送端失败关闭，不能让老 CN 忽略字段后读当前 session 域。检查须覆盖实际发送的表达式 owner，而非仅检查一个投影列表。
- UT 必须对真实 outgoing pipeline 做 marshal/unmarshal 后在独立 process 消费，验证 S/D 和结果；仅检查内存字段存在不算跨节点证明。

## 6. 连接迁移：保守拒绝，而非悄悄重新绑定

已查到现有流程：

- `Routine.migrateFrom` 只把 prepared name、SQL、ParamTypes 放入迁移消息。
- `Session.migrateTo` 先安装当前用户变量，再逐条重新 PREPARE。
- 因此，仅在 VarRef 上加字段不能让迁移自动保留旧 S/D；这条路径根本不传原 prepared plan。

本修订选择：当 source session 的 prepared plan 含新冻结的用户变量字符串绑定时，返回既有 `OkExpectedNotSafeToStartTransfer`，让连接继续留在 source CN。使用现有 cursor/long-data 迁移保护的位置和错误合同，不增加 query.proto 字段或迁移重放表。

实施要求：

- 通过现有 `VisitExpressionsInOwner` + `VisitExprTree` 检查完整 PreparePlan，包括嵌套表达式/已有来源元数据，而非 SQL 文本搜索。
- 必须证明 cached PreparePlan 仍保留绑定身份：覆盖执行前、执行后、常量折叠/参数特化后状态。若有路径会抹掉身份，先修 owner 的信息保持，不能在“未查到”时放行。
- 不增加 session 级缓存/map 来重复记录可从原计划得到的状态。
- marker-only、无该绑定的 prepared statement 仍可迁移。
- 拒绝不应破坏 source 的 prepared statement、用户变量、锁/事务或已有返回缓冲；客户端可继续执行。
- DEALLOCATE/关闭最后一个受保护语句后，现有迁移应恢复可用。pool reset 移除 prepare 后不可残留拒绝状态。
- 代价：相关连接不再参与透明迁移/负载搬迁，直到语句关闭。这是明确需要批准的行为变化；不把它包装为零影响。

## 7. 兼容、代际和发布

### 7.1 格式兼容与语义兼容分开

- 新 reader 读无字段的旧 VarRef：走 legacy 路径。
- 老 reader 忽略 tag 4：只说明 protobuf 可解析，**不保证绑定语义正确**。
- 远端正常执行使用既有 literal 形式；现有 binary-string 协议门槛为 MORPC v58。保留现有门禁，测试低版本拒绝，不宣称只增加 tag 就可任意滚动升级。
- 新 source 的连接迁移保护适用于任意 target 版本；旧 source 不知道此保护，不能承诺 old→new 迁移保持旧冻结合同。
- 此改动不写新的表数据/catalog 状态；临时 executor/vector 不跨重启保留。真正新 PREPARE 建立新绑定，断线重连同理。

### 7.2 选定的保守交付限制

本修订不承诺跨版本保留活跃 prepared 绑定。发布/回滚需要排空受影响 prepared 连接，协调 CN 更新后让客户端重新连接并 PREPARE；不得依靠跨版本迁移来保留这些旧会话。

跨 CN 协议 UT 和当前版本真实多 CN BVT是合并前证据。若要宣称活跃会话无损滚动升级，则须另做带能力协商的逐语句绑定迁移设计，不属于本方案。

## 8. 生命周期、资源和安全

- 一个 VarRef 标量 + 一个 executor 标量，不增加全局状态、cache、goroutine、锁、timer 或周期性任务。
- 每次绑定 O(1) 域解析；每次执行 O(1) 读取冻结值，当前赋值的 value 解析照旧。不按 batch 行数重复查询域。
- 复制/序列化的增量按 VarRef 数量 O(n)；proto 非零小值约两字节，实际 Go struct 对齐须以生成后布局为准，不声称零开销。
- 迁移检查按待迁移计划总大小遍历，复用现有 visitor，有界于现有 prepared statement 的数量和 AST/plan 大小；不放入每行热路径。
- vector 字节和侧带继续由既有 mpool/allocation selection 拥有；失败时按现有 Free 路径释放。
- 非法字段在执行前拒绝；不读取其它 session 的状态，不新增 tenant/权限边界。
- pool reuse 测试覆盖先绑定 TEXT、释放、后 legacy/system/其它绑定，证明不残留上一个 executor 的标量状态。

## 9. 实施顺序与验收矩阵

先最小失败反例，再生产修复，再分层验证。旧通过证据不能替代新字段和新迁移保护的验证。

| 闭包 | 必要证据 |
|---|---|
| binder 两轴 | typed 表驱动覆盖 S(text/binary OID × Charset) 和 D 的三种值；S 完全不变、两个 PREPARE 不互改、session 不变、无 resolver、resolver 错误/非法域 |
| executor | 每次赋值改变域但当前表达式 S/D 不变；最新值变化；NULL/masked/error→success；Reset、Free、池复用；legacy 0；非法 raw 4/256/大值不可截断 |
| 用户可见回归 | 原 `binary_string_function_semantics.test` 67/67，三个旧 binary/binary 预期保持；不改其 result 掩盖问题 |
| 前置用例 | `prepare_user_variable_string_domain.test` 既有用例正常通过；新增 selected-text/binary 分支的 prepared 冻结，直接 SQL/marker 对照，CHARSET/COLLATION 与行值同时断言 |
| 复制/协议 | DeepCopyExpr 与 Expr/Plan protobuf round trip 保留 S/D；旧消息默认 0；真实 remote fold→literal→marshal/unmarshal→独立消费者保留静态类型、行域和来源 |
| 远端拒绝路径 | 新绑定 VarRef 未折叠不能发往未知/旧解释器；v58 既有 gate 保持；原计划不被远端执行副本回写 |
| session migration | 含新绑定时拒绝且 source 仍可用；执行前后/嵌套/折叠后覆盖；marker-only 正常；deallocate/reset 后恢复迁移 |
| 代际 | 同一计划 EXECUTE 冻结；真正 reprepare 按新赋值重绑。MySQL schema-reprepare oracle 已补充，不建立额外永久绑定缓存 |
| 集成/静态 | owning packages：plan/pb、plan、colexec、compile、frontend，及直接受影响 consumer；匹配工具链 gofmt/vet/lint；changed-code 覆盖率 ≥75% |
| BVT 形态 | 先本地现有隔离服务的确定性对照，再真实多 CN / PROXY 路径。单 CN BVT 即使出现 MULTICN explain，也不冒充多节点运行证据 |

新增 SQL 的 result 只用 mo-tester 生成，先与独立合同逐格比对，再普通比较。目标是不增加跳过、重试或 sleeps。

## 10. 审查决定与交付

用户已确认的具体选择：

1. 同意新增 VarRef tag 4，把 S 与 D 分离，撤销 binder 改写 S.Charset 的做法。
2. 同意在现有迁移保护点拒绝含冻结字符串用户变量绑定的 prepared 连接迁移，直到语句关闭；不扩展迁移协议。
3. 同意保留既有 selected-row 执行合同，与 MySQL 在该合同上的已知差异不在此 PR 中顺带统一。
4. 同意按受影响 prepared 会话排空/重建的发布边界交付，不宣称无损混版本迁移。

在当前 Draft PR 中提供这个版本化设计与实际批准范围，并实施对应修复；不把这次事后修正伪装成原实现已经经过完整双轴设计审批。TODO 和本地原始日志仍不提交。

审批结论：批准上述 1–4 项范围及取舍；实施和验证以本稿为依据。设计批准不等于实现或 CI 已通过，交付验证单独记录。

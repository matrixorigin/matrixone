# FIELD 类型历史设计 D3：动态验收结果与待决修订

**状态：设计阶段未通过；实现交付仍阻塞。本文是对D2的补充，不是已批准的实现方案。**

- 归属：[issue #25301](https://github.com/matrixorigin/matrixone/issues/25301)、[PR #30096](https://github.com/matrixorigin/matrixone/pull/30096)。
- 完整问题、备选、所有权、状态机和范围以同目录[冻结D2](CLAUDE_PREPARED_FIELD_TYPE_HISTORY_DESIGN_D2.md)为基础。
- D2 SHA256：`b2df6187d3f952284c92374d38e78f30ab04ca958bb49e241bcc0d78f2c42c8c`；保留原文，不把后来的授权倒签到草案。
- 授权记录：2026-10-19，本地交互用户在收到D2及其缺口摘要后回复原文 `go ahead`。据此继续设计验证及后续实施准备；这不是GitHub维护者的独立设计APPROVE，也不代表对下述新发现和兼容选择的批准。
- 被测源码H：`a0e585b3090302fc57d9a8eabbe954b98d6d1aee`；配对基准B：`def799e1260dd547cae2e0e54323ea20ec1b7685`。最新fetch的`mo/main`为`57fb9530859d92791100098d312ea8cacc671910`，尚未merge。
- 本阶段只用本地诊断overlay做技术验证，没有修改产品或正式测试。本设计文档的版本化不是实现交付或设计门禁PASS。

## 1. R1：真实声明改变的retry不能继续盲用旧H

### 已确认反例

```sql
CREATE TABLE t(n_name VARCHAR(25));
INSERT INTO t VALUES ('a');
PREPARE s FROM 'SELECT FIELD(CASE WHEN ? THEN n_name ELSE ? END, ''A'', ''a'') FROM t';
SET @c=1, @v='a';
EXECUTE s USING @c,@v; -- 1
EXECUTE s USING @c,@v; -- 1，未变更声明对照
ALTER TABLE t MODIFY n_name VARBINARY(25);
EXECUTE s USING @c,@v; -- MySQL 8.4：2
```

独立MySQL观察为`1 / 1 / 2 / 2`（最后一个2来自相同语义direct SELECT）。这是DDL前后同handle的公开兼容对照，**不是MySQL内部retry竞态重放**。

MO诊断使用真实binder、`buildPlanForCompileRetry`及表达式执行器，仅用既有mock替换catalog观察。绑定旧VARCHAR声明产生H；随后修改列为VARBINARY并递增TableDef.Version，既有`checkModify`确定返回true。结果：

| 绑定 | FIELD结果 |
|---|---:|
| 旧声明初次绑定 | 1 |
| 新声明，但retry携带旧H | **1** |
| 新声明，以nil H重新绑定 | **2** |

原plan及输入H未被原地改变。将这三份真实计划序列化后交给Compile的实际`validateRetryResultMetadata`：metadata冻结前、冻结后均不拒绝；三者的FIELD输出列元数据相同。终端`completeCompileExecution`的成功/错误路径都会标记下一EXECUTE需要rebuild，但不会修正本次已算出的1。

因此这不是仅缺一个测试：**D2的“旧H沿用至本次retry结束”不涵盖真实依赖类型改变，可能得到错误结果。** 当前未运行并发DDL到完整physical Compile/Run的端到端注入，不把上述分层证据冒称为完整服务复现。

### 需要明确选择的修订

**建议A：对history相关的真实依赖变化fail-closed。** 在重建回调中用当前事务的catalog观察验证原声明代际；变化时返回既有definition-change/reprepare终端错误，不执行带旧H的rows。Compile先标记generation rebuilt的既有顺序保证frontend在错误终端使cache失效；下一EXECUTE由正常owner重建并发布新H。保持“retry不是第二个handle历史发布者”，不加共享缓存、goroutine或迁移协议。

该方案必须覆盖：未变更/仅物理重试保H、真实列类型及对象身份变化、metadata冻结前后、解析失败、终端失效、下一EXECUTE正确重建。应复用已有依赖检查边界，不再维护一份依赖白名单；重试闭包捕获的代际证据须有明确owner，不能借用会被下一次执行覆盖的handle字段。

**可见代价：** 同样的DDL变化，MySQL在两次EXECUTE之间可以透明重准备；MO若在当前执行的definition retry阶段发现变化，建议A会要求客户端重试。这一可见错误策略是D2尚未决定的事项，需要维护者明确接受，不能由agent当成已经获批。

备选B是在retry内重建并携带新的私有H/依赖代际直到物理编译成功，下一轮retry复用该代际，仍不写handle。它更接近透明重试，但改变D2“capture H不更新”的生命周期合约，增加候选发布/失败回滚及下一retry的状态验证。若选择B，需要先修订并审查这条状态机，不能只把`WithPreparedFieldTypes(..., nil)`写死。

## 2. R2：普通绑定有已测开销，不能称零回归

配对实验在同工作树、Go1.27.1、darwin/arm64、同一原生库、GOMAXPROCS=2和相同SQL/fixture进行。基准overlay恢复B的全部PR变更Go文件；当前组使用H。每项`-benchmem -benchtime=100ms -count=3`，表中为中位数。共享机器未做CPU隔离，时间只能作样本观察；稳定的B/op/allocs差异不是调度噪声。

| 普通REVERSE嵌套层数（列为VARCHAR） | B ns/op | H ns/op | B→H B/op | B→H allocs/op |
|---:|---:|---:|---:|---:|
| 1 | 29,783 | 29,835 | 27,176→27,752 | 333→338 |
| 8 | 54,908 | 62,831 | 37,361→41,969 | 519→559 |
| 32 | 212,654 | 327,094 | 96,139→160,012 | 1,209→1,485 |

32层样本增加约**54%绑定时间、63,873 B和276次分配**。这些是不含FIELD/CASE的普通SQL，不得归咎于用户启用了history。源码中的逐consumer声明解析/局部resolver、重复子树访问可解释随深度上升的成本；样本不是全局复杂度上界或OOM证明，也没有证明某一个分配点独占全部差额。

无CASE的source-binding miss在P=1/16/64时，分别增加128/248/632 B、4/7/9次分配；计入额外AST格式化，不声称没有成本。三个时间中位数约为基准的0.985/1.009/1.004倍，不能据此声称普遍加速。

实际`initExecuteStmtParam`logical cache hit另以plan指针同一且无新cache候选作断言，未将miss当hit：

| 路径 / 参数数 | B→H B/op | B→H allocs/op | 时间比H/B |
|---|---:|---:|---:|
| 无history / 2 | 1,896→1,896 | 19→19 | 0.997 |
| 无history / 16 | 13,032→13,032 | 65→65 | 1.003 |
| 无history / 64 | 49,896→49,896 | 212→212 | 0.870 |
| 有history形状 / 2 | 1,536→1,664 | 18→19 | 1.068 |
| 有history形状 / 16 | 12,680→13,704 | 64→65 | 1.121 |
| 有history形状 / 64 | 49,544→53,640 | 211→212 | 0.966 |

基准没有本PR的H；“有history形状”指相同FIELD/CASE SQL。另一次不计作性能对照的结构探针明确断言：H组确实非nil且CASE map=1、param map=P，无H组为nil；每格50次真实cache hit保持同一H和plan指针，六格均通过。H组多一次key字符串分配，与D2的key复制成本吻合。不能把64参数的噪声性时间下降写作优化收益。

**待决：** D2没有预先批准可接受的退化阈值。维护者需选择接受上述深嵌套绑定代价并明确验收边界，或要求先优化普通TEXT路径。建议不要宣称“无性能影响”，也不要在没有失效模型的情况下直接加常驻memo。首次绑定/类型进展/数值模式重建、共享CTE、依赖数增长的完整计数矩阵仍需在选定策略后补齐；本次配对样本不是整个R2的性能PASS。

## 3. R3：pool清理方案有确定性红绿证据

测试直接持有私有Compile，不依赖sync.Pool是否返回原指针：

1. 设置一个捕获对象的buildPlanFunc。
2. 标记prepared并调用Release，确认闭包仍可执行且返回同一捕获对象，这是有意缓存的对照。
3. 转为非prepared并直接执行pool的clear seam，要求buildPlanFunc为空。

H在第3步失败；只在诊断overlay的`Compile.clear()`清理区加入`c.buildPlanFunc = nil`后，同一测试通过。metadata guard对照也通过。产品源文件尚未改动，不能把这个局部green视为正式修复的owning-package/race验证。

该修复方向符合已审D2：把清理放在真正回pool的owner，不在缓存prepared的每次Release中清除。仍须正式UT、真实owner消费者和按生命周期风险选择的race验证；没有证明无限泄漏，也不声称修复了无关所有闭包字段。

## 4. R4：已补当前版本的计划序列化消费者证据

在D2所列8格DEFAULT测试上，以真实CREATE TABLE绑定计划执行：

`DeepCopyPlan → Plan.Marshal → 新Plan.Unmarshal → Default.Expr.DeepCopy → colexec.Eval`

逐格验证完整表达式/Type与原计划相等、HEX/NULL结果正确、执行不改变表达式；8格均通过。覆盖了新增NULLIF witness随完整计划编码与DEFAULT消费者传递，非仅用手造摘要测试protobuf。

限度：是当前H的编码/解码及表达式执行，不是落盘重启、旧版本CN、Proxy/MULTICN或全部持久化拓扑。没有新增proto字段的事实不意味着旧FIELD执行器已获得新比较语义；发布时不能保证混合新旧执行器的查询有一致新语义，需与部署/升级约束一同审阅。

## 5. 当前决策记录与后续门禁

| 项目 | 状态 |
|---|---|
| D2推荐架构的继续推进授权 | 用户`go ahead`已记录；不冒充过去已有design-first批准 |
| R1声明变化重试 | 具体错误结果已确认；待选择A fail-closed或B私有新代际 |
| R2成本 | 已有12组配对样本；待接受实测预算或要求优化，再补剩余验收矩阵 |
| R3终端清理 | 局部诊断红绿已证明；正式实现及owning/race验收待执行 |
| R4计划摘要 | 当前版本roundtrip/DEFAULT消费通过；拓扑/版本外推明确不覆盖 |
| 完整设计/实现审批 | **仍阻塞**，不关闭review 5567395398，不请求无阻塞实现批准 |

选定R1/R2及兼容/部署边界后，记录精确批准revision；再merge最新mo/main，按批准的设计做最小产品修复和永久测试，复核影响范围、coverage及最终源码一致性。保持原PR OPEN/ready/main，不以文档提交或CI绿灯代替技术闭合。

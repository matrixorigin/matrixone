# FIELD 声明解析与 prepared 类型历史设计（D2）

**状态：待审草案；没有设计批准，也不代表实现可交付。**

- 问题：[#25301](https://github.com/matrixorigin/matrixone/issues/25301)；实现：[PR #30096](https://github.com/matrixorigin/matrixone/pull/30096)；设计门禁：[review 5567395398](https://github.com/matrixorigin/matrixone/pull/30096#pullrequestreview-5567395398)。
- 被解释实现 H：`a0e585b3090302fc57d9a8eabbe954b98d6d1aee`；固定基准 B：`def799e1260dd547cae2e0e54323ea20ec1b7685`。下文源码链接固定到 H，不随分支变化。
- 本次重新审计于2026-10-19。D1保留为历史；本文取代D1中的技术描述。最新main另记在审计清单中，尚未merge，不能以 H 的证据代替集成验证。
- 本地文档内容hash/Git blob identity由`CLAUDE_design_D2_manifest.json`记录；正式发布后还须提供可访问的Git commit链接。批准人、批准revision、批准记录均为空。

## 1. 决策摘要

建议保留“**声明解析 + handle拥有不可变类型快照 + 复用既有单槽specialization cache**”方案，不增加历史列表、共享cache或迁移wire格式。

完整改动不是仅给FIELD加`bytes.Equal`：32个文件中14个生产文件，生产增删合计1861行；同时触及普通SQL绑定、prepared状态、缓存准入、重试与迁移。按完整范围触发设计门禁，不以历次小补丁豁免。

D2将两类工作分开：

1. **当前源码事实与提议取舍**：下面给出可核实的激活范围、发布/失败状态、缓存和迁移策略。
2. **批准前技术闭合项**：真实依赖改变时的Compile retry语义、普通绑定遍历成本仍需针对性证据；另外已在源码确认pool-reset没有清除捕获H的buildPlan闭包，需关闭这一具体生命周期缺口。不能用“接受风险”代替正确性证明；残留引用不等于已经证明无限泄漏或OOM。

已有实现先于本次设计审查。补救只能是现在暂停实现交付，建立独立设计阶段，再恢复实现对齐和审查；不能把补写文档记作过去已有事前批准。

## 2. 问题与外部合约

### 2.1 不只修比较器

`FIELD(_binary X'FF', _binary X'FE', _binary X'FF')`应返回2。二进制比较不得把不同无效UTF-8序列解码为同一个替代字符；文本保持已有字符比较，NULL、首次匹配和数值重载保持原规则。

实际进入FIELD的subject可能是CASE、COALESCE、关系列、aggregate/window或参数。**FIELD字符串路径按subject解析后的静态域选择比较，不按每一行的runtime-domain override选择**；普通LEFT/REVERSE等消费者则有不同的声明/逐行来源契约。只根据当前payload、或只改最末端比较器，不能统一决定这些语义。

prepared兼容还取决于执行顺序：NULL-first、SQL变量的binary声明、COM参数的transport类型及后续数值reprepare可能产生不同事实。不能用每轮孤立direct SQL作为全部历史测试的oracle。

### 2.2 必须成立的不变量

- **域分离**：transport、来源、消费者比较域、结果声明各有owner；隐式运输CAST不取得SQL显式CAST的权威。
- **身份与不可变性**：CASE事实按同一prepared AST的稳定ordinal标识；发布后快照不变。需要新事实时COW，不改旧执行/retry可见对象。
- **范围克制**：只有读写型FIELD解析实际收集的参数相关CASE事实才激活history。普通字符串声明预览为只读，不能顺便冻结历史。
- **两种发布点**：成功绑定/metadata/AST clone后发布history；成功Compile后才安装runtime cache。查询执行成功不是history发布条件；“整个EXECUTE完全事务式回滚”不是当前合约。
- **代际分离**：物理计划失效与SQL类型历史失效不同；真实类型环境改变不能复活旧事实。失败组合要么保持旧有效状态，要么被dirty标志禁止复用。
- **缓存安全**：来源签名、同handle历史revision、值敏感reprepare及diagnostic证明都满足才命中；不能按wire类型相等盲目命中。
- **迁移安全**：不能省略history后重放SQL并称等价。现有payload不传history时源CN拒绝导出相关prepared handles。

### 2.3 兼容范围

MySQL 8.4文档为公开依据：[FIELD](https://dev.mysql.com/doc/refman/8.4/en/string-functions.html#function_field)、[PREPARE](https://dev.mysql.com/doc/refman/8.4/en/prepare.html)、[statement caching](https://dev.mysql.com/doc/refman/8.4/en/statement-caching.html)。文档没有穷尽这里的CASE历史边界，必须用同协议、同声明、同执行序列独立验证。文本EqualFold不等于完整MySQL collation权重实现。

不扩展charset/collation算法、分区、存储过程、UDF、warning兼容。既有CASE+空binary literal及NULLIF第一值无长度BINARY的已记录差异不作为本PR“完全MySQL兼容”的证据。已有其他issue的binary-string/common-type设计作为背景，其批准不能跨scope借用。

## 3. 完整变更图：新增与复用分开

| 闭包 / 风险 | 本PR生产文件 | 新增合约及复用边界 |
|---|---|---|
| FIELD执行 / R2 | `function/func_binary.go` | 静态subject域、原始bytes比较、选择掩码；原数值重载/文本路径复用 |
| 声明与来源 / R2、绑定成本R3 | `plan/base_binder.go`、`prepared_field.go`、`collation_expression.go`、`query_builder.go`、`types.go` | 普通SQL与DEFAULT也可达；query-block/HAVING边界、NULLIF返回来源、紧凑witness；不只prepared路径 |
| CASE历史 / R3 | `plan/prepared_field_types.go`、`prepared_binding.go`、`tree/expr.go`、`tree/format.go` | 新snapshot/COW/ordinal、参数合约进展和有条件二次optimize |
| frontend生命周期 / R3 | `frontend/computation_wrapper.go`、`prepared_execution.go`、`routine.go`、`types.go` | 新history指针/resetPending、key revision/reprepare守卫、发布延后、retry携带和migration拒绝；**单槽cache、retired compile清理、请求互斥本身已有** |

另10个Go测试文件、8个BVT case/result文件，共32文件。全生产hunk已读；测试按第9节的合约断言核对，本轮不是再做一次全PR所有测试hunk的实现批准。完整路径和内容hash见审计附件。

## 4. 数据模型与激活边界

### 4.1 四种不同数据不能混写为“历史”

| 对象 | 内容 / 存活owner | 边界 |
|---|---|---|
| H=`PreparedFieldTypes` | revision、CASE→`types.Type`+inferredBinary、param→type+inherited；handle当前指针 | 不含值、AST/Expr/vector；一处当前指针不代表进程只存活一个历史版本 |
| `preparedFieldTypeBinding` | ids/producers/originals/collected/actual等绑定期图索引 | 仅本次binding context；defer恢复context后不进入handle |
| `StringDomainSource` witness | 计划里的既有Expr-shaped元数据；P/V标识、标量域、必要声明证明 | 不同于H；本PR生成紧凑摘要而不保留原值图，但仍属于plan并可能随plan复制/传输/保存 |
| retry capture | H引用、bindings/paramVals的slice副本、必要的原prepare plan引用 | slice容器复制不是任意`any`载荷deep copy；其有效期由当前/缓存Compile闭包及pool回收链决定，不是仅局部栈变量 |

H保存runtime `types.Type`，不是完整`plan.Type`。完整nullable/collation/coercibility/conflict/version等仍由计划声明和CAST契约承担，不能声称H携带所有plan元数据。运输CAST复制完整`arg.Typ`，只调整必要表示；BINARY→VARBINARY保宽避免padding，零容量binary VARCHAR→无界BLOB避免截空。

### 4.2 精确激活条件（[S1]、[S2]、[S3]）

1. frontend对有参数的Query source-binding路径传入history context；无该context时没有history binding。
2. `newPreparedFieldTypeBinding`枚举AST CASE；没有语法CASE直接返回nil。存在CASE只创建临时binding，**不是已经发布history**。NULLIF后续降低产生的synthetic CASE不等于语法CASE ordinal。
3. `bindPreparedFieldCase`登记producer；读写FIELD resolver追到producer且其返回值计算依赖参数时，才收集事实。参数依赖与类型来源不同：内部函数的控制参数也可令返回表达式依赖参数，不能把它们都赋成结果类型。
4. `bindStringSourceDeclaration`的resolver为readOnly；普通`REVERSE(CASE...)`不能仅因预览建立H。显式CAST等权威边界阻止向内取得FIELD历史。
5. `H==nil`既可能是未解析，也可能是**不适用/从未激活**；`cases[ordinal]`缺失又是另一个维度。非nil快照中的`T_any`/TEXT事实不能因NULL值或看似默认类型被抹去。
6. revision是候选快照变化标识，不是EXECUTE计数。首次收集后又statement-wide reprepare可在一次绑定内再次递增；代际重置后可重新从低revision开始，因此revision不是全局epoch。

### 4.3 普通消费者与关系边界（[S2]、[S8]）

- NULLIF比较用第一/第二值，但返回来源只属第一值；QueryBuilder和无QueryBuilder DEFAULT分支都记录来源。条件/比较不能冒充真正CASE声明。
- 输入subject的逐行选择与函数结果是否自行拥有声明是两个维度；复用单一source/result contract，不能为FIELD再造第二份函数白名单。
- 关系列引用本身不自动等于物化；按真实query block、DISTINCT/window/set、GROUP/HAVING、LIMIT等判断；常量HAVING消除后保留owner信息。scalar行选择与derived materialization不混同；严格唯一键证明可消除某些单行GROUP边界，不能借用逻辑FD过度推断。
- 图遍历用局部Expr memo与路径上的(nodeID,output-position)守卫终止别名回边；它们不是跨执行memo，也不能据此推论整个bind仅遍历一次。
- 未识别的binary producer先问registry在全TEXT输入下是否仍固有binary，避免把opaque hash/tuple表示加错CAST；不是无条件移除优化器CAST。

## 5. 状态转换与发布合约

P表示prepare seed/metadata，H表示handle当前history，K表示runtime cache槽。下列名称只是说明，不新增flags。

| 事件 | 候选与发布行为 | 失败/终端行为 |
|---|---|---|
| 首次相关绑定 | 读P、来源bindings，私有收集H'；必要时再bind应用CASE事实 | 任何优化/lowering失败返回，旧H不变 |
| 兼容后续执行 | 读H；兼容类型保持同一snapshot pointer；K命中仍做额外准入 | 准入失败不冒充命中，转私有bind |
| 类型不兼容/完整整数拼写越界 | statement-wide更新参数环境，重建相关CASE facts；不改输入bindings | 不向旧map原地写入；若后续metadata失败不发布H' |
| metadata/AST clone成功 | `initExecuteStmtParam`末尾设置H=H'并清resetPending；这是历史发布点 | 之后Compile或Run失败不承诺回滚H |
| Compile成功/失败 | 成功才安装cache候选；失败丢弃候选 | 原有K可能仍留存但key/dirty守卫不得允许错误复用；不把新H发布等同新K已安装 |
| 仅物理/保守计划失效 | rebuild P并清K，尽可能保留H | 若新P已经安装而后续失败，标dirty强制下一次重建 |
| 真实类型代际改变 | 用nil历史绑定新环境；成功再替换H | 新P已安装且失败时保留旧H指针但置resetPending，下一次不能恢复旧事实 |
| Close/同名替换/DEALLOCATE | 清H/resetPending；既有cache/compile/params/AST清理 | 未释放的执行/Compile引用按各自owner回收，不宣称`H=nil`等于立刻释放所有旧快照 |

### 5.1 reset判定（[S4]）

无条件重置理由是已验证的schema变化、NativeMode变化、已有非零protocol版本不匹配、或pending-reset。重建后还比较依赖ObjectRef多重集合：以`proto.Equal`匹配身份/版本/snapshot等完整字段，忽略顺序但保留重复数量，不按指针相等判断。

仅session DDL/temp版本、stats等保守失效不等于真实类型变化。数值模式改变且H存在时，先build(nil)，比较实际`-`、`/`重载/返回类型/隐式CAST契约以及bool SUM/AVG影响；有改变则采用该候选，否则再build(H)。只剥离推断CAST0，不能跨执行CAST1或typed CAST0(P)。旧模式字段直到成功绑定后才更新，避免失败后失去比较基线。

**失败原子性不是所有字段同一次store。** `rebuildCommitted && !rebuildPublished`的defer把状态置为不可复用；只有确实需要reset时才置resetPending。seed尚未发布就失败时，旧环境字段留存供下次重新判断。绑定失败是返回错误的具体终端；context取消只有被下层观察并返回时才走该路径，不能声称这里有额外取消线性化屏障。

### 5.2 K的key与成本边界（[S5]）

复用原有来源签名：位置、类型/width/scale/charset、协议/域类别等；本PR加`field:<revision>`。只有同handle cache槽能解释该revision，真实plan重建清槽后才能重用低revision。

同key还需`!H.NeedsReprepare`与diagnostic证明；完整整数拼写超signed/unsigned界限可触发DECIMAL进展，而非完整数字的尾缀继续既有prefix规则。inherited显式参数不发生reprepare，但当前实现求actual/范围检查后才检查inherited，不能在成本估算中假设它零扫描。

缓存仍为一个logical specialization槽；TP可留Compile，AP留logical plan而不长期留AP topology。ValueDependent、诊断不安全、pagination/percentile和EXPLAIN等已有准入限制继续生效，不另加按值索引的map。

### 5.3 retry不是第二个handle发布者（[S6]）

- `preparedExecutionRetry`引用当次已发布H，复制binding/参数slice；新建Compile和cache命中Reset均设置本次buildPlan闭包。
- `buildPlanForCompileRetry`把H传入binder，只返回`bound.Plan`；**不把`bound.FieldTypes`回写handle，也不更新capture H**。恢复context/database依赖defer；window/pagination检查重做。
- Compile的definition retry会检查已冻结result metadata是否变化，成功physical compile后才替换当前执行plan；终端`completeCompileExecution`发现generation rebuilt会使frontend缓存无效，下一次EXECUTE走正常重建/reset判断。
- 同一来源/声明下retry沿用H的值等价已有UT；但“真实依赖类型改变且FIELD仍返回同一UINT64 metadata”的路径不能仅靠result metadata比较证明中间CASE历史正确。**第10节R1必须明确并验证：本次retry允许沿用原类型契约，还是应返回需reprepare；不能悄悄用新事实执行却不持有对应history。** 当前没有足够证据把此问题判为已闭合或已确认bug。

## 6. 所有权、等待与存活界限

### 6.1 Q1：引用寿命，而不是只找`=nil`

```
绑定context → 私有H' → 成功后handle.H
                          └→ 当前/缓存Compile的retry闭包 → Reset替换或释放
cache替换的旧Compile → wrapper.retired → Clear → releaseRuntimeCompile
handle移除/同名替换/Session.Close → PrepareStmt.Close → 清cache/params/AST
```

已缓存Compile的retry闭包可比一次请求活得更久；新H发布但新Compile失败时，旧K也可能仍保留旧H。因此D1“旧快照只存活到当前执行结束”的描述不成立。设计要求是**持有者数量有界且无按执行次数追加的历史链**，不是任何时刻只有一个live snapshot。

**已确认的反向引用缺口（[S9]）**：非prepared `Release → reuse.Free → clear → sync.Pool.Put`中，`Compile.clear()`清理plan/proc等字段，却没有清`buildPlanFunc`；`NewCompile`也不清该字段。闭包可继续引用retry capture/H以及原session。GC或pool丢弃、后续SetBuildPlanFunc覆盖都可能最终切断引用，因此不能称必然无限泄漏；但“Release/Close终端切断history引用”及“只由活handle界定存活数量”的证明目前不成立。缺少clear本身是既有代码，本PR给这条保留链新挂了H，不能作为无关代码完全排除。

提议合约：只有有意缓存的prepared Compile可以跨请求持有retry闭包；真正返回reuse pool时必须清除闭包，下一owner不能看到旧session捕获。实现如何补齐先经设计决定，后续用直接clear seam断言closure为空，不依赖sync.Pool一定返回同一指针或反复GC。

`releaseRuntimeCompile`临时detach/restore当前参数vector；retired compile延后至wrapper.Clear，避免清掉新compile共享Process状态。这些机制是既有所有权，本PR只给其闭包增加H引用。GC回收H不需要新析构goroutine；但峰值内存、重试载荷及cache引用需第10节R3的容量证据。

### 6.2 Q2：migration拒绝不是无等待的fast path（[S7]）

```
迁移调用 → migrateController的request/operation准入等待
         → context.AfterFunc广播 / endRequest/endOperation广播 / startClose广播
         → 取得operation owner → 读session与已有snapshot → 检查H → 拒绝/导出
         → defer endOperation
```

controller锁不跨实际业务I/O持有；取消callback可广播唤醒cond。`startClose`封闭准入并取消active operation。已有的snapshot/远端功能检查在H判断之前，所以H拒绝不能许诺固定低延迟或“先于所有session读取”。新增判断本身不引入等待；本轮只闭合这条新增边及其既有admission，不泛称迁移全流程任意I/O均无挂起风险。

H非nil时拒绝所有目标，包括同版本CN，因为payload没有history。失败不导出prepared handle列表且源会话继续可用；不声称response其他字段从未填充。DEALLOCATE最后一个相关handle之后仅解除这一拒绝条件，还需满足其余既有migration条件。

### 6.3 Q3：有限结构不等于已有byte quota

H常驻内容至多C个CASE事实、P个参数合约；C、P来自该statement，而非执行次数。初始化params覆盖全部参数，不仅FIELD直接读取的参数；statement-wide reprepare会处理这些合约。

现有`getMaxPrepareStmtCountLocked`按session限制handle数量（内部上限与配置取较小值），不是进程总history字节配额。没有新history专用byte cap，也不能把Go heap maps称为mpool已计费。暂不提议另建持久化/全局quota系统；先验证固定语句不同值序列无增长链，再由维护者接受“按获准语句复杂度线性持有”的资源策略。无法满足该界限时是设计/实现问题，不可用“仅缓存一个指针”跳过。

## 7. 成本模型：完整范围，不只history

记S为格式化SQL字节数，C/P为语法CASE/参数数，D为依赖数，E为绑定表达式访问规模，L为本轮需范围判定的字符串字节数，Klen为原有来源key长度，W为所有计划witness的总标记/声明大小。以下是源码推导，不是测量结果。

| 路径 | 当前可证明的工作 / 未证明的上界 |
|---|---|
| 无H cache命中 | history nil guard常数；本PR不新增CASE枚举、history map或optimize；原有key构造/diagnostics不是零成本 |
| 有H cache命中 | dense bindings时O(P+L)检查；key拼接会复制原key，新增分配O(Klen)，不能只记revision的O(log r)。非dense lookup可退为O(P²)，生产frontend当前生成dense位置 |
| source-binding miss，即使最终无CASE/无H | `CaseExpressions`实际调用Format并写strings.Builder，额外遍历和文本分配至少按S计；不能写成仅O(C)或没有成本 |
| 初次CASE binding | ParameterCount/seed遍历、binding maps，首次事实应用最多再optimize一次；单次BuildPreparedExecutionPlan最多2次optimize |
| 数值模式变化且H存在 | build(nil)最多2次；若契约不变，再build(H)为1次；该分支上限**3次**，不是笼统4次；另计seed重建等外层既有工作 |
| 依赖比较 | rebuild时嵌套匹配，最坏O(D²×ObjectRef比较成本)，另有O(D)matched；H=nil也可走到，不是所有普通语句完全免额外成本 |
| 普通函数声明绑定 | 每次binder函数入口可能重新收集来源、遍历关系/参数证明、问registry。局部memo不跨调用，嵌套链的累计工作可呈二次量级，不能声称全statement为O(E) |
| witness与表达式复制 | 每个摘要是扁平的，不等于全plan只O(P)。不同producer可各有witness，总体记W；新增CAST也有执行期vector转换/字节复制成本，只有“无新增逐行图遍历”成立 |
| 参数范围检查 | prefix扫描按L；DECIMAL256用固定76字节系数scratch，不按指数的数值大小分配。`[]byte→string`及错误格式化仍可能按L分配，不能称零分配 |
| 快照/COW | 每候选O(C+P)；峰值乘以实际live持有者，另加临时maps/格式化文本/witness及既有plan/Compile成本 |

**结构验收预算**：cache命中新增optimizer/CASE枚举/快照map分配均为0；单次build≤2次optimize、上述numeric-mode分支≤3次；H map项数≤C/P；cache槽=1；固定AST反复执行不增加history版本集合。对通用声明递归暂不承诺线性上界，必须测量/计数深度增长，若不可接受则在设计层决定局部算法改进，而不是先加常驻cache。

性能实验必须用同toolchain/平台/配置的配对baseline，区分无CASE miss、无H hit、有H hit、首次绑定、类型进展、模式重建、深层普通字符串函数/共享CTE；报告ns/op、B/op、allocs/op与访问/优化次数。一般SQL也受影响，不能只benchmark FIELD。现有UT耗时/覆盖率不能证明无性能回归；没有预先同意的“可忽略退化百分比”，显著退化需解释及明确决策。

## 8. 兼容、运维、安全与方案比较

### 8.1 备选与推荐

| 方案 | 正确性/复杂度/成本/迁移 | 决定 |
|---|---|---|
| 仅bytes比较并拆走所有历史变更 | 易审/成本低；不能承诺完整prepared历史兼容，可作为缩小PR范围的真实备选 | 需明确变更目标并移除依赖，不称等价完成当前scope |
| 每轮无历史重绑 | 不保留历史语义且损失cache，无法用执行当前值替代声明 | 不选 |
| 在共享prepare plan原地记录 | 减少复制但污染旧retry、失败回滚与metadata；锁与别名复杂 | 不选 |
| 紧凑不可变H、绑定局部COW、复用单槽cache | 正确性可分解；有额外binding/扫描成本与本地持有；本轮可保守禁止迁移 | 推荐，受R1–R4及设计审批约束 |
| 序列化H到proxy/CN迁移payload | 可消除驻留，但引入wire版本、identity/代际重放、混合版本与升级测试 | 另案，不在本PR临时扩展 |

### 8.2 运维与安全边界

- 长期不释放的相关handle可令连接长期留在源CN；这是明确可迁移性损失。已有history不只是某个旧目标不支持，同版本目标也拒绝。不能通过清H后迁移“恢复可用性”。
- 没有新增H的wire/catalog/on-disk字段；但plan中新增/调整了既有witness和CAST，可能流经现有plan序列化、DEFAULT持久化及远端执行。**“H不持久化”不推出整个PR无持久化/远端消费者。** 兼容依赖已有字段与能力gate，必要的copy/roundtrip/旧计划缺字段行为需单独验收。
- CN重启/断线的prepared handle原本不恢复；备份不包含H。回滚要drain相关会话并重新PREPARE，不宣称活跃history可透明降级。已生成的合法表列/DEFAULT仍需按原计划编码规则解释，而不是做history数据迁移。
- 不新增配置flag/worker；按常规版本发布，先小范围观察现有查询错误、延迟、heap、prepared数量及迁移拒绝。当前没有FIELD专属监控指标，不能在验收中假称已部署；需要指标时只用有界reason，不能以SQL/handle作为label。
- H不保存用户值，但计划/Compile retry本来可能保留参数载荷；不能把整个执行系统称为“不持有敏感值”。所有状态只在原session/handle，不跨租户复用。输入增长维度是SQL长度、CASE/参数/依赖和载荷长度，DoS评估不能只看单槽数。

## 9. 证据矩阵：已读断言与未覆盖内容

本轮只读审计。通过脚本核对review18工作树同为H、全部32个变更文件字节一致、原始normal测试终态非空，以及BVT报告；没有新增Go/service/CI执行。

| 断言 | 核实的现有测试与证据 | 不可外推 |
|---|---|---|
| 快照事实/源类型不变、兼容同pointer | `TestPreparedFieldTypeHistory`、`TestPreparedFieldHistoryRelations`；读取具体Same/旧type/bindings断言，normal已pass | 全部可能的类型组合或history live-heap界限 |
| CASE身份、无关语句不激活 | `TestPreparedFieldCaseIdentity`、`TestPreparedFieldUnrelatedStatementsKeepNoHistory`、`TestPreparedFieldStringConsumerWithoutHistory`，normal已pass | Format无额外分配/全绑定线性 |
| metadata失败、resetPending、真实cache hit、执行/迁移竞争及释放handle恢复迁移 | `TestPreparedFieldHistoryCacheAndMigration`，有hook/barrier、Same/NotSame/PrepareStmts空、源继续执行断言，normal已pass | 本轮重跑race或真实Proxy跨CN迁移 |
| retry沿用相同H并得到同值 | **`TestPreparedFieldCandidateHistory`**：Same(retry.fieldTypes)、调用`buildPlanForCompileRetry`并执行结果，normal已pass | 真实依赖类型改变的Compile retry；D1错用的`TestPreparedFieldProtocolHistory`只验证packet类型/NULL/type-vector复用 |
| 无关计划失效保H，数值模式改变/不改变合约与错误后重试 | `TestPreparedFieldPlanInvalidationKeepsHistory`、`TestPreparedFieldNumericModeGeneration`及dependency tests，normal已pass | 任意失败点/所有schema变化均覆盖 |
| 整数范围准入 | `TestPreparedFieldOverflowAdmission`，边界/尾缀/大指数/继承CAST不reprepare，normal已pass | 完整cache-path压力或零分配 |
| 等待取消/active operation关闭 | 既有`TestLifecycleControllerWaitingForRequestHonorsContext`、`TestLifecycleControllerCloseCancelsActiveOperation`，normal已pass | 前者取消可能发生在实际等待前，不是“已证明blocked-phase取消”；这不是本PR新改测试 |
| DEFAULT真正执行与来源保持 | `TestExpressionDefaultNullIfStringSource`，8格、hex/NULL/type width、执行前后DeepCopy相等，normal已pass | 任意持久化/升级拓扑 |
| 公共SQL | 单CN dtype93与history492各两轮、FIELD97、variadic104；DEFAULT三行MO/MySQL原始输出相等 | 全492组MySQL逐项差分、本轮Proxy/MULTICN、全PRchanged coverage或新base验证 |

现有reviewer报告的race/CI通过不抹除局限；本轮未下载验证其原始race终态，不将其写成本轮实测。review18增量2/2覆盖率只适用于那三行修复，不是1861行全PR的覆盖率。

## 10. 技术闭合、审批与交付顺序

### 10.1 需要完成的技术项（不能让用户“同意即跳过”）

| ID | 缺口与最小下一步 | 通过标准 / 责任 |
|---|---|---|
| R1 retry遇真实类型依赖变化 | 构造有H→故障触发definition retry→只改变中间CASE来源而FIELD输出metadata仍UINT64；覆盖metadata frozen前后。同步核对MySQL同语义序列，确定原历史沿用或需reprepare的明确合约 | 当前执行、终端frontend invalidation、下一EXECUTE都一致；不得只检查result metadata。实现作者给证据，维护者审兼容选择 |
| R2 普通绑定与cache成本 | 用第7节小型计数/benchmark覆盖非FIELD语句、深嵌套/共享来源、S/D/P增长；保留baseline/模式/终态 | optimizer/遍历次数满足结构预算；报告额外字节/时间，解释二次增长的实际可达性与容量，不宣称未测线性。作者补证 |
| R3 pool闭包清理与容量 | 为已确认的buildPlanFunc残留补直接clear/reset合约测试，区别prepared缓存Release与真正回pool；再用固定AST类型切换、Compile失败、cache替换、Close核对实际live owner | 回pool不留旧session/retry/H闭包；cache项数与H maps有界，无按EXECUTE次数追加链。作者修正实现并提供证据，当前未改代码 |
| R4 计划摘要兼容 | 对新增NULLIF witness及完整Type复制核对现有copy/serialization/DEFAULT消费者测试，缺失则补最小验证 | 声明/来源经相应边界不丢。复用已有覆盖要记录具体断言，不机械新增集群 |

R3的闭包残留为源码已确认的生命周期缺口，尚未执行新增UT；R1/R2/R4是未闭合的合约/证据。均不夸大为已经复现的无限泄漏、OOM、错误结果或hang。本轮不修改产品或创建Issue。若只读证据或现有测试已足够，后续先复用，不为打勾重复跑大套件。

### 10.2 需要维护者批准的取舍

- 选定第8节方案及scope，明确不以逐轮无history替代MySQL历史。
- 认可绑定发布早于Compile成功的合约，但R1的兼容正确性仍必须证明。
- 接受相关handle可长期阻止迁移的运维代价，或另案实现可迁移协议。
- 接受按statement复杂度持有、无独立H byte quota的资源策略；R2/R3仍需完成，回pool清旧闭包的正确性不因批准而豁免。
- 确认正式设计发布与精确revision审批渠道。没有发布链接前不能称外部reviewer已取得稳定设计。

### 10.3 后续阶段

先关闭设计技术阻塞并记录对精确D2或后继revision的明确批准，再merge最新mo/main，检查实现偏差与证据失效范围，完成实现自审/正常push/请求复审。设计-only文档可在获得发布授权后单独版本化；批准设计与批准实现是不同事件，不需要伪造过去的设计阶段。

## 固定源码索引

[S1]: https://github.com/matrixorigin/matrixone/blob/a0e585b3090302fc57d9a8eabbe954b98d6d1aee/pkg/sql/plan/prepared_field_types.go#L29-L406
[S2]: https://github.com/matrixorigin/matrixone/blob/a0e585b3090302fc57d9a8eabbe954b98d6d1aee/pkg/sql/plan/prepared_field.go#L99-L686
[S3]: https://github.com/matrixorigin/matrixone/blob/a0e585b3090302fc57d9a8eabbe954b98d6d1aee/pkg/sql/plan/prepared_binding.go#L1014-L1053
[S4]: https://github.com/matrixorigin/matrixone/blob/a0e585b3090302fc57d9a8eabbe954b98d6d1aee/pkg/frontend/computation_wrapper.go#L1480-L2023
[S5]: https://github.com/matrixorigin/matrixone/blob/a0e585b3090302fc57d9a8eabbe954b98d6d1aee/pkg/frontend/prepared_execution.go#L95-L135
[S6]: https://github.com/matrixorigin/matrixone/blob/a0e585b3090302fc57d9a8eabbe954b98d6d1aee/pkg/frontend/computation_wrapper.go#L3013-L3098
[S7]: https://github.com/matrixorigin/matrixone/blob/a0e585b3090302fc57d9a8eabbe954b98d6d1aee/pkg/frontend/migrate.go#L99-L235
[S8]: https://github.com/matrixorigin/matrixone/blob/a0e585b3090302fc57d9a8eabbe954b98d6d1aee/pkg/sql/plan/base_binder.go#L6335-L6604
[S9]: https://github.com/matrixorigin/matrixone/blob/a0e585b3090302fc57d9a8eabbe954b98d6d1aee/pkg/sql/compile/compile.go#L475-L604

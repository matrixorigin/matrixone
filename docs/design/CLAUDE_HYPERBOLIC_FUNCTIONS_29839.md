# SQL 双曲函数支持设计（v1，已批准并验证）

- 关联问题：https://github.com/matrixorigin/matrixone/issues/29839
- 历史相关请求：https://github.com/matrixorigin/matrixone/issues/2095
- 实现 PR：https://github.com/matrixorigin/matrixone/pull/29987 （draft，目标 main，关联 #29839）。
- 日期：2026-10-12。
- 设计基线：`mo/main`，`5a5501b3e1048f3d19d96824cc68a62b7f0fcb07`。
- 用户已批准功能范围：TANH、COSH、ASINH、ACOSH、ATANH。
- 状态：用户于 2026-10-12 以“go ahead”批准 v1 设计及边界语义；实现和验证已完成。
- 批准基准 SHA-256：`d4999a39085def42bc0ea996d8128cdf55aa86d42c1859bdcabb05d379c81c6f`。本文随后仅更新状态、必要的可空元数据闭包与验证记录，公开语义和算法不变。

## 1. 问题与成功标准

当前 SQL 内置函数注册表不包含上述五个名称，名称解析会返回“不支持函数或操作符”。SINH 已存在，其它双曲/反双曲函数缺失，阻碍科学特征转换与聚合下推。

成功标准：五个函数通过现有 SQL 解析、类型匹配、常量折叠与向量执行路径可达；普通结果、NULL、定义域、极小/极大数值和聚合满足下述契约；既有函数 ID、重载 ID 与既有数学行为不变。

非目标：不修改 SINH，不补其它数学函数，不改变通用转换/错误策略，不引入新的特殊值 SQL 语法，不覆盖 warning 数量或文本、分区、字符集/排序规则、存储过程或 UDF。

## 2. 类型与公开结果契约

五个函数均为严格的一元函数，输入经既有数学函数类型匹配规则转为 DOUBLE，返回 DOUBLE。整数、浮点、decimal、布尔数值上下文及其它既有可接受转换保持与相邻数学函数一致；不新增隐式转换规则。NULL 输入返回 NULL。错误参数个数或不支持的类型由现有重载解析拒绝。

| 函数 | 有限实数输入契约 | 边界或失败 |
| --- | --- | --- |
| TANH(x) | 所有有限 x 有效；奇函数，零为零，结果在 [-1,1]；微小值保留一阶值，大有限值饱和到 ±1 | 不因 EXP 中间计算而溢出 |
| COSH(x) | 所有有限 x 在数学上有效；偶函数，COSH(0)=1；可表示结果返回有限 DOUBLE | 真正 DOUBLE 结果溢出时返回现有 `OutOfRange` 类错误，与 SINH 的溢出策略一致 |
| ASINH(x) | 所有有限 x 有效；奇函数，零为零；微小与极大有限输入均数值稳定 | 不使用会溢出的朴素 x*x 公式 |
| ACOSH(x) | x≥1 时有效，ACOSH(1)=0；靠近 1 与极大有限输入数值稳定 | x<1 返回 NULL，与现有 ACOS/ASIN/SQRT 的 SQL 定义域失败策略一致 |
| ATANH(x) | -1<x<1 时有效；奇函数，零为零；微小与靠近 ±1 的合法输入数值稳定 | x≤-1 或 x≥1（包含 ±1）返回 NULL，不向 SQL 发布边界无穷大 |

底层原生向量如果含 IEEE 特殊值，不新增统一清洗策略：TANH/ASINH 的特殊值及有效 ACOSH 输入沿用 Go 数学行为；ACOSH(-Inf)、ATANH(±Inf) 按定义域返回 NULL；COSH(±Inf) 返回范围错误；NaN 沿用现有数学函数的传播惯例。主要 SQL 验收面为有限数值与 NULL，不宣称新增 NaN/Inf 字面量支持。

## 3. 首个所有者与执行闭包

名称注册、函数 ID 与重载是公开可达性的首个所有者：

`SQL 名称 -> functionIdRegister -> GetFunctionByName/现有转换 -> 编码重载 ID -> 执行器 -> 结果向量 -> 投影/聚合/过滤消费者`。

在 `pkg/sql/plan/function` 中追加五个稳定函数 ID、名称与重载，使用 `Function_STRICT`、标准函数布局、`fixedTypeMatchWithBoolNumericCast`、唯一 DOUBLE 重载及现有执行器构造方式。当前结束标记为 591；若实现前分配未变化，追加 ID 为 591–595，结束标记改为 596。最终编号须以新鲜基线为准，不重编号已有函数。

可空元数据也是上述闭包的消费者：ACOSH/ATANH 可在非 NULL 输入上生成定义域 NULL，必须在 `DeduceNotNullable` 中保留 nullable 属性，避免 COUNT 或 NULL 谓词被错误优化。TANH/COSH/ASINH 的成功返回仍沿用依赖输入可空性的既有契约。

复用现有固定长度一元模板：

- TANH、ASINH：`opUnaryFixedToFixed`。
- ACOSH、ATANH：`opUnaryFixedToFixedWithNullCheck`，定义域判断先于计算，不为逐行预期 NULL 创建随后丢弃的错误对象。
- COSH：`opUnaryFixedToFixedWithErrorCheck`，真正溢出保留现有范围错误。

模板已有常量广播、输入 NULL、选择掩码、全掩码与零长度边界。不复制 SINH 的手写循环，不修改这些通用模板，不引入额外状态或扩展接口。

资源归属不变：调用方拥有输入向量；结果包装器与既有调用方拥有输出向量；每次调用无缓存、后台 goroutine、锁、等待、重试、外部 I/O 或持久化副作用。错误直接返回调用方既有清理路径。NULL 与掩码禁止对该行进行数值计算；复用结果向量的重置仍由既有框架负责。

## 4. 数值算法与替代方案

TANH、ASINH、ACOSH、ATANH 使用 Go 1.27.1 的 `math.Tanh`、`math.Asinh`、`math.Acosh`、`math.Atanh`。标准库已按微小、近边界与大数输入分段实现，优于直接使用 EXP/LOG/SQRT 代数公式。

COSH 有一个必须补上的标准库边界：Go 1.27.1 通用 `math.Cosh` 在 |x|>21 时计算 `Exp(|x|)*0.5`。因此 `COSH(710)` 的数学结果仍可表示为 DOUBLE，标准库却会因中间 EXP 溢出而返回 Inf。在当前 darwin/arm64 上通用实现适用；不能把这一结果误判为真正 COSH 溢出。

选用局部修正：正常调用 `math.Cosh`；若结果为 Inf 且输入有限，用 `e=math.Exp(abs(x)/2)`、`(e/2)*e` 重算。乘法顺序不能写成 `e*e/2`，否则重新产生中间溢出。重算结果仍为 Inf 时才返回范围错误。该分支只在普通计算报告 Inf 后进入，正常范围不会多做指数计算，且不会改变既有 SINH。

替代方案比较：

1. 保持不支持或由客户端转换：实现成本低，但无法满足 SQL 聚合下推目标。
2. SQL/内部朴素 EXP/LOG 公式：表面简单，但存在 TANH 微小值相消、COSH 中间溢出、反双曲函数 x*x 溢出及近定义域端点精度问题，不采用。
3. 完全手写数值算法或引入 CGo 库：维护、平台及验证成本明显高于本需求，不采用。
4. 直接无条件使用五个 Go 标准函数：其它四个满足数值算法需求，但 COSH 的已知有限结果提前溢出不满足本设计；只为它增加上述局部重算，不建设通用数值框架。

参考惯例：Go 1.27.1 的 `src/math/{tanh,sinh,asinh,acosh,atanh}.go`；MO 的 `momath.Acos/Asin/Sqrt/Sinh` 与 `builtInACos/ASin/Sqrt`。这些是数值算法与 MO 错误策略依据，不宣称这些函数全都属于 MySQL 兼容要求。

## 5. 兼容性与交付

新增函数沿用现有计划/重载编码格式，不修改协议、目录字段或存储格式。所有已有函数和重载 ID 必须保持稳定；追加 ID 同步进入稳定 ID 测试。

旧二进制不识别新增函数 ID/名称。使用新函数前，参与计划生成与执行的节点须全部升级；不增加自动混合版本能力。含新函数的视图/其它存储表达式也依赖具备支持的版本，降级前需移除或停止使用相应表达式。已有查询与数据无需迁移，回滚普通查询路径无需修改数据。

无新权限、配置项、特性开关、后台状态或新租户边界。正常错误沿用现有 SQL 诊断，不新增每行日志、指标或标签。允许错误诊断显示实际函数名，但不纳入 warning 兼容性范围。

## 6. 成本与性能约束

普通向量执行 O(n)，常量输入只计算一次后广播。输出沿用已有包装器的 O(n) 空间；不新增逐行分配，NULL/掩码直接跳过。正常 COSH 输入只做一次标准 Cosh；有限 Inf 结果的局部重算最多再做一次指数计算。新函数属于新路径，不增加其它函数的运行开销；无需规模测试或基准测试才能证明这一结构性成本。

若实现新增共享框架、跨包数值层、缓存或影响既有热路径，则超出本版设计，必须重新审核。

## 7. 变更与验证映射

| 闭包 | 文件/消费者 | 风险与证明 |
| --- | --- | --- |
| 注册、解析、稳定编码 ID | `function_id.go`、`function_id_test.go`、`list_builtIn.go`；名称解析、编码 ID 执行及 SQL binder | R2；验证已有 ID 不变、新 ID 唯一，DOUBLE 返回及转换、错误参数/类型、按编码 ID 执行 |
| 向量与数值执行 | `func_builtin.go`、新 `func_hyperbolic_test.go`；投影与聚合 | R2；固定独立参考值与容差、最近边界/负向控制，常量/普通向量/NULL/空输入/掩码与结果重置 |
| 可空元数据 | `function.go:DeduceNotNullable`、`function_test.go`；COUNT 与 NULL 谓词 | R2；ACOSH/ATANH 在 NOT NULL 输入下仍保持可空，SQL 以主键算术输入验证实际消费者结果 |
| 布尔数值上下文 | 复用 `bool_math_test.go` 的既有表驱动 fixture | R2；五个函数注册及转换目标仍为 DOUBLE |
| SQL 公共契约 | 新 `func_math_hyperbolic.test`/`.result`；mo-service 与 mo-tester | R2；常量折叠、列向量、COUNT/AVG、类型转换、定义域 NULL、COSH 溢出、稳定舍入/容差及清理 |
| 本设计 | 本文 | R0 文档；审核准确版本与实现一致性 |

UT 最小独立 oracle：

- TANH：issue 提供的 ±1、±2 固定数值，零、±1e-20、±最大有限值与奇函数对称；微小输入不能只用宽绝对误差，否则错误的零也会通过。
- COSH：0、±1/±2 的独立固定数值，偶函数，±710 的可表示结果，±711 的范围错误；掩码/NULL 包含越界载荷时不得计算该行。
- ASINH：0、±1/±2、±1e-20、±最大有限值，避免朴素公式溢出；独立固定值与奇函数对称。
- ACOSH：1、2、略大于 1 的最近有效 DOUBLE、大有限值；0、略小于 1 的最近 DOUBLE 与负数为 NULL。
- ATANH：0、±0.5、±1e-20、接近 ±1 但仍合法的 DOUBLE；±1 与定义域外数值为 NULL。
- IEEE 特殊值、负零、空输入、常量广播与选择掩码使用向量 fixture 的确定性表驱动 UT；不使用墙钟或进程。

固定参考值取自独立高精度计算或可信外部实现，不仅将同一个待包装的 Go 函数当作唯一预期值。数值容差按相对误差/极小值的非零与保真约束设置，精确 NULL/错误类另行断言。

BVT fixture 决策：已检查 `func_math.sql`/`.result`、`func_math_asin.test`/`.result`、`func_math_trig.test`、SINH 用例清单。现有脚本包含其它函数的大量场景及通用表名；五函数共用一个新的自包含双曲函数用例，比扩张已有混合脚本更清晰。一个特定名称表、八行（±2、±1、±0.5、0、NULL）足以覆盖正常列向量与不同定义域；极小、极大、溢出与参数错误使用常量语句，不增长 fixture。

BVT 使用 mo-tester 生成 `.result`，逐行审查值与 DOUBLE 元数据，再正常比较；验证本用例资源清理后，同一实例重复运行。使用测试专属干净单机实例，等待 SQL 与查询服务注册就绪，不删除其它实例的数据，不终止其它进程。现有编码格式和分布式协议未改变，单机公共路径加编码 ID 执行 UT 为本次契约证据；不声称验证混合版本或分布式新能力。

执行顺序：精确命名 UT -> owning-package UT 与覆盖率 -> 变更包增量 gofmt/vet/lint -> 当前 worktree 的 mo-service 构建 -> 精确 BVT 生成/审核/比较/清理/重复 -> 完整交付 self-review。匹配 `go.mod` 的 Go 1.27.1，CGo 间接依赖使用 `mo-cgo-test` 和匹配的原生产物。修改代码覆盖率要求 ≥75%。

没有新共享状态、等待链或生命周期，因此不额外要求 race、故障注入、重启/升级、多 CN 或负载测试；追加 ID 的兼容性由明确部署限制与稳定编号证明，不能据此宣称旧节点支持新函数。

## 8. 审核与风险记录

- 设计门禁触发：新增 SQL 公开能力及函数 ID 的兼容性边界；规模虽小，仍记录正式版本化设计并先审核。
- 已确认：范围扩展为五个函数；基线更新不修改本任务所需函数包、momath、Makefile、CGo/thirdparty 或 Go 模块输入。
- 已批准：ACOSH/ATANH 定义域外（含 ATANH(±1)）返回 NULL；COSH 真溢出保留范围错误，而不误报可表示的 COSH(710)。
- 实现按 v1 执行，新增 ID 为 591–595，结束标记为 596；不修改已有 ID、SINH、通用向量模板或转换策略。
- 验证：精确 UT、函数包正常模式 UT、gofmt、go vet、增量与完整 owning-package golangci-lint、worktree 服务构建均通过。变更行相交覆盖块为 28/28 语句，100%；五个新执行函数均为 100%，不声称整个包达到 100%。
- BVT `.result` 由 mo-tester 生成并逐条审核；同一干净专属实例正常比较两次，各 33/33 通过，无 ignored 或 abnormal。域 NULL 的主键算术 COUNT/谓词、掩码、准备参数和错误后继续执行均验证；每次用例结束表数量为 0，实例正常停止退出 0。
- 额外 molint 的两处旧 unsafe 诊断与准确干净基线报告逐字一致；不将它们归因于本次修改，也不擅自修复。
- 详细命令、输入签名、报告路径与完整变更审查见本 worktree 的 `CLAUDE_REVIEW_2026-10-12.md`；本记录不声称全仓 CI、多 CN 或混合版本验证。
- 任何实质偏离仍需更新设计并重新审核。

### PR 交付前的新鲜基线验证

用户已确认关联 #29839。功能提交 `d4695eadf2` 后，以 merge/pull 合并最新 `mo/main` 的 `7af631bdf7`，验证 head 为 `871f59109c`。上游新增 join ownership、truncate lifecycle 测试与 Sirius SQL campaign；本任务的函数包源/测试、SQL case/result、Go 模块、Makefile、CPU 原生输入均不变，上游变化 Go 包与函数包依赖闭包交集为空，因此复用原 UT、覆盖率及静态证据。

鉴于 frontend/compile/plan 公共消费者有变化，重建合并版本服务并用新的干净任务专属实例正常 BVT 两轮，各 33/33、failed/ignored/abnormal=0，分别 0.135s/0.119s。预期结果没有重新生成或放宽；每轮表数量 0，实例正常停止 0，源/case/result 与服务 SHA 校验一致。详细验证摘要已附在 draft PR；本次文档交付记录更新没有改变数值代码或批准语义。

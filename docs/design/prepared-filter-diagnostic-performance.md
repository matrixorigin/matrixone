# Prepared 参数诊断与性能：根因和修复

关联：[性能问题 #29429](https://github.com/matrixorigin/matrixone/issues/29429)、[修复 PR #29462](https://github.com/matrixorigin/matrixone/pull/29462)、[复合键语义问题 #29463](https://github.com/matrixorigin/matrixone/issues/29463)。

## 根因

引入提交为 `a99db843c7b4630a86883addc9424433d93fc2d2`（#28851），直接正常 parent 为 `1575e70c3db676d3ed00abc080aadc029b0e1435`。单 CN 即可复现。

原修改要保证转换告警的执行所有权：下推、常量折叠和 JOIN 改写不能提前或重复发出告警，也不能激活空输入和未选中分支。Prepared 的整数参数经字符串向量传输，计划中同样有 implicit CAST，因此被 `MayDiagnoseStatementParameter` 覆盖。退化来自把 PREPARE 时的“可能产生诊断”直接当作本次 EXECUTE 的最终结论：

1. reader/block 谓词在绑定和折叠前被排除，整数点查失去存储选择性。
2. JOIN 的诊断屏障阻止关系改写，单独放行扫描不能恢复原计划。
3. 扫描和中间结果增加，使持锁时间变长；锁等待是放大后的症状。
4. 最初逐次重新规划虽恢复部分选择性，却增加 frontend CPU。静态 traits 的重复全计划遍历和 `serial(CAST(?), CAST(?))` 在两份模板中的重复隔离求值，又带来额外成本。

## 执行契约

### 计划、绑定与缓存

- 参数化 QUERY 的正常执行和 compile retry 都从原始 AST 进入 `BuildPreparedExecutionPlan`。先绑定协议或 SQL 变量的真实源类型，再选择比较域、函数重载和键/索引改写；PREPARE 计划只承担未绑定阶段的检查，不作为执行后备模板。
- 原 AST 的参数 ordinal 在优化前固定。执行表达式始终保留 ParamRef，当前值只供诊断探测及确实依赖值的配置消费者使用。
- 每个 QueryBuilder 在关系改写前复制诊断候选并取得本次证明。子查询证明不能授权父查询；取消、资源和内部错误传播，探测 warning 不发送给客户端。实际 SQL 诊断保留原执行所有权。
- SQL 优化完成后，非字符串源参数降成已有的 `CAST(TEXT ParamRef AS source)`，兼容旧 CN。字符串域直接保留参数引用及运行时来源元数据，避免额外 CAST 丢失字符/二进制语义。
- 每个 PREPARE generation 沿既有 owner 保留绑定计划、优化前诊断副本和可复用 TP compile。缓存身份还包含当前来源类型和 FIELD 声明历史修订；不能把它描述成只看物理传输类型的缓存。命中时重新证明当前绑定；值相关配置、分页、percentile、EXPLAIN 不进入该缓存。不存在第二份优化模板或 QUERY late-Fill 后备入口。
- 物理编译成功后才发布缓存；旧 compile 在本次 statement 清理后释放，避免清空共享 Process。schema retry 重建绑定和证明，错误路径恢复上下文。DDL/强制 SET 继续使用其原有物化入口。

### 快速证明的边界

成功的 `ParseExecuteData` 已把固定宽度整数解码成规范文本。直接 implicit `CAST(ParamRef AS integer)` 只在 SHORT/LONG/LONGLONG、目标同符号且不窄化时跳过隔离求值。目标范围以整数 OID 为准。NULL 也先检查来源与目标；long data 按参数位置排除。其他情况继续隔离求值。

`serial`/`serial_full` 仅当每个分量都是上述直接 CAST 时使用同一证明。编码本身不产生 SQL 转换告警；执行仍构造键并传播资源错误。删除了协议解码后重复 parse/format 的校验，不再为手工伪造的“整数类型 + 非整数文本”重复建立协议校验层。

### reader、block 与远端

本次证明只授权登记的参数诊断，literal、row-scoped、volatile 诊断仍按原规则处理。普通 scan 可从获准的 reader 谓词补充可 zonemap 的 block 谓词；`blockFilter=2` 的规划决定保存在既有 `ExtraOptions` 中，非空特殊 scan options 不补全。

可复用的 `Source.node` 保持不可变。`Source.remoteBlockFilters` 保存本次获准的原始谓词：nil 表示懒初始化尚未完成，非 nil 空列表表示本次全部排除。远端收到原始表达式，在自己的 Process 中折叠，不接收协调端 Fold ID。

深审发现旧实现覆盖 `Source.node` 后，safe → unsafe → safe 的 block 数量为 1 → 0 → 0。直接 compiler 回归测试要求恢复为 1 → 0 → 1，并检查每步实际发送的远端列表。

## 深审发现的语义错误

### 复合键 #29463

已在 PR 前的 `a99db843c7` 和 PR head 复现。优化器把分量比较合并为隐藏序列化键比较，随后参数特化错误地把内部编码传播到 DOUBLE 比较域，返回错误行。只阻止外层 DOUBLE 转换仍不充分：分量类型变化会改变编码，小数范围比较和再次绑定合法字符串仍可能错误。

先前按协议类型禁用键改写、逐次重建的原型已撤回：干净 TPCC 实测为 1124.06 对 parent 5542.42 tpmC，无法验收。它还遗漏 VARCHAR 键绑定数值的比较域，不能作为系统性修复。

当前实现将类型选择前移到 AST 表达式绑定：先以真实源类型解析比较，再由普通优化器决定键/索引改写。参数物理 TEXT 传输与 SQL 源类型分开；可复用规划全程保留引用，禁止先填字面量再试图恢复。整数窄化必须对完整来源类型域无损，值相关优化另需本次绑定的证明。不得在 `serial` 或索引规则中新增协议白名单。

前置类型绑定和单缓存路线的历史审查与验证见文末固定版本记录；这些结果不能覆盖之后的修订。`Expr.Typ` 保存 SQL 源类型，执行器复用已有 CAST 处理非字符串 TEXT 传输；不借用 `SyntaxExplicitCast` 或增加包装身份表。

源 AST 的最大参数 ordinal 在 PREPARE 优化前固定，用它初始化归一化映射，优化删除一个参数不能移动剩余参数的执行槽。现有 `FillValues` 使用优化后的列坐标，不能直接搬到绑定阶段。

执行入口遵循以下约束：

- 每个 generation 只保留一个类型描述符键、优化计划、物理 compile 和优化前诊断表达式副本。每次命中重新证明本次绑定；不能只检查优化后仍然存在的谓词。
- 源描述符来自协议解码或 SQL 变量的真实类型、字符串域及来源，不根据字符串数值前缀推断类型。SEND_LONG_DATA、typed/untyped NULL 和 BIT_COUNT 类型演进不能丢失。
- 需要当前值才能确定类型或配置的消费者，在自己的绑定/编译阶段解析值。GENERATE_SERIES 的输出 schema 和 geometry SRID 必须在父表达式绑定前确定；percentile 配置在物理聚合编译时求值，不能复用旧物理配置。
- 普通参数全程保留引用，禁止整条查询填字面量再恢复；失败构建和 schema retry 必须恢复上下文、保持原始 ordinal，并重新取得诊断证明。

原型验证：键改写前的等值/范围/IN 类型域、非恒等参数布局、同一逻辑及物理计划连续换值、TIME/JSON/DECIMAL 比较、UPDATE 写入类型，以及执行器 NULL/掩码/错误后复用均通过。所属包 plan、colexec、parsers/tree 全量测试通过。这些证据不替代 frontend、真实协议和性能验收。

回归 oracle 按 SQL 数值比较定义，包括键值 0、NULL、小数、字符串后缀、等值和范围；不能把旧版本的错误行/内部编码告警写进 golden。

### 浮点协议比较 #29464

普通整数列的 `k > ?` 绑定二进制 FLOAT 2.5，在 PR 前的 a99db843c7 上也会报 cast-to-int 错误。原执行入口只识别 text 比较特化，未识别 FLOAT。曾加入的 FLOAT 参数位置特例随上述原型一并撤回；当前方案在共同的类型绑定阶段修复，不能再增加独立类型分支。

### prepared EXPLAIN

原 CI 的 `index 1 not exists` 来自普通 PREPARE 与 EXPLAIN 采用不同的规划入口。普通入口清理不可达节点，EXPLAIN 留下的旧节点没有参与参数序号归一化，却被候选收集访问。让 EXPLAIN、ANALYZE、PHYPLAN 使用共同 PREPARE optimizer；删除仅排除 EXPLAIN 诊断探测的补丁。展示结果仍按当前绑定生成。

## 普通消费者与兼容边界

前置绑定使普通函数收到真实源类型。消费者准入在共同入口修正，不增加参数专用重载：

- 字符串与整数/浮点比较采用数值比较域；普通整数列的精确字符串常量按完整范围证明保留原有索引能力。
- JSON 算术使用 DOUBLE，保留小数（[#29470](https://github.com/matrixorigin/matrixone/issues/29470)）。JSON CONCAT/CONCAT_WS 在 kernel 序列化；JSON_DEPTH 的 binary 输入在 NULL/掩码之后报告 charset 错误。已有远端表达式能力检测覆盖 placement、发送重检、接收和持久化，两种新输入契约要求 MORPC 101（原问题 [#28907](https://github.com/matrixorigin/matrixone/issues/28907)）。全局 JSON CAST 行为另由 [#29471](https://github.com/matrixorigin/matrixone/issues/29471) 跟踪，本 PR 不声称修复。
- 含完整 UINT64/BIT 来源域的乘法在公共重载入口使用 Decimal128，避免 TIME/Decimal64 路径先缩窄成负数（[#29469](https://github.com/matrixorigin/matrixone/issues/29469)）。显式 CAST 不被穿透。
- HLL/bitmap 的序列化状态由共同 checker 接收 MySQL 字符串并规范为无宽度限制的 VARBINARY，保持旧聚合 ABI；大状态不能被 SQL 默认宽度截断。已删除 PREPARE 专用 opaque cast。
- 源变量保存 bare NULL 的 ANY 类型，显式 typed NULL 保留完整类型；SET 的内部 SELECT 不得用展示类型覆盖逻辑类型或修改源 AST。
- Temporal 与 FLOAT/字符串算术在公共入口选择 DOUBLE，经已有 Decimal128 转换保留 packed value 和小数秒；比较规则和显式 CAST 边界保留；同规则用于乘除、加减、取余和 DIV（[#29473](https://github.com/matrixorigin/matrixone/issues/29473)）。
- REGEXP 使用共同运行时字符串域解析（[#29465](https://github.com/matrixorigin/matrixone/issues/29465)），保留字符/二进制元数据及 NULL 来源。

## 删除的机制

- 保守/优化双模板、无绑定 QUERY 模板构建、其 pending/publish 状态和判定 helper。
- QUERY late-Fill 和 PREPARE compile 后备路由，独立的直接结果/数值前缀/geometry 值键及缓存 traits。
- context 上的第二份诊断证明，以及缓存从优化后计划补造候选的 fallback；证明只由 builder 和本次执行拥有。
- 重复参数归一化检查、整份 optimizer hints 快照、无生产调用的 percentile helper。
- 恒真分支、永远返回 nil 的 error 和协议解码后的重复整数 parse/format 校验。

## 相邻消费者与错误恢复

早期审查之后的公共回归还要求保留以下边界：字符串来源的数值运算沿实际来源域转换；NULL 参数保持可执行，同时关系结果列具有具体物理类型；YEAR→BIT 赋值复用共同转换入口。SQL EXECUTE 可见的 TEXT 展示不改变内部数值消费者，展示类型也不能反写源 AST。

无符号精确比较必须保留可整除的十进制／科学计数字符串的精度，不能先经 DOUBLE 丢失整数位。显式 DOUBLE 来源的 CHAR 转换保持其舍入语义，词法推断仅适用于字符串来源。同类型的 inexact/error→exact 再绑定必须恢复，不能复用失败前的值证明。

当前缓存和重准备边界由 [prepared execution](../../pkg/frontend/prepared_execution.go) 与 [binding](../../pkg/sql/plan/prepared_binding.go) 拥有。schema、来源类型或 FIELD 历史修订变化沿现有失效／重准备路径处理；诊断证明仍属于本次执行，值和折叠结果不因此进入缓存。已有失败、替换、重试和公共协议测试保持各自职责。

## 历史验证和适用范围

[原设计及性能记录](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/prepared-filter-diagnostic-performance.md)
保留原型回退、完整配对方法、吞吐和 CPU 数据；[审查及后续修复记录](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/prepared-filter-diagnostic-performance-review.md)
保留审查发现、公共回归修复、失败与各代验证范围。其独有消费者约束已合入本文。

记录中的 102.59% 吞吐比和双 CN BVT 结果早于后续公共回归修复，不能作为修复后版本的性能／分布式验收。后续修复有其单独的本地 UT／协议记录，未重新执行那组完整双 CN 性能验收。全局 JSON CAST、启动 placement 的独立问题与原变更范围分开；本文不判断其当前 issue 状态，也不把历史 pending CI 写成当前结果。

# REGEXP 操作数域分离设计 r1

状态：**待独立设计审批；实现验收仍阻塞**。

归属：[issue #27217](https://github.com/matrixorigin/matrixone/issues/27217)，[实现 PR #28534](https://github.com/matrixorigin/matrixone/pull/28534)。参考需求：#26907、#25299。设计版本 r1，2026-09-12。现有实现审查起点 bff80fa30846707eaab4de137cf49d4c2d39ed35，review base a0770f5dc4a296f8407bbebf6a2664bac9972e53。

本稿是补做的独立设计阶段，不追认早期“go ahead”为本版本审批，不声称设计先于已有实现。批准必须记录审批人、决定、确切提交 SHA 和链接；批准后才继续实现对照与验收。

## 1. 问题、范围与门禁

同一个二进制标记不能同时回答：SQL 是否允许这两个操作数、每个输入如何解码、位置计量单位是什么、输出编码是什么。混用这些问题会造成 PREPARE/EXECUTE 错误阶段不同、binary pattern 改变 text subject 的字符位置、prepared reuse 元数据漂移，以及完整非法 UTF-8 被误当作可截断尾部。

这是跨 binder、prepared specialization、函数执行和错误协议的核心兼容契约重构；完整生产改动约700行，触发设计门禁，不因 BUG 标签豁免。

目标是 REGEXP 家族的操作数兼容；独立 oracle 固定为 MySQL 8.4.8。参考 [MySQL 8.4 REGEXP 文档](https://dev.mysql.com/doc/refman/8.4/en/regexp.html) 和 [prepared protocol](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase_ps.html)。文档不能覆盖的 malformed-input/NULL 顺序由该版本实测决定，不外推到所有 MySQL/ICU 版本。MO 仍使用现有 regexp 引擎，不宣称实现 ICU 全语法。

非目标：全局 charset/collation、通用参数转换、protobuf/wire/存储格式改动、TEXT/LONGTEXT 全局元数据修复、materialized alias/derived-table 的一般 provenance 传递、非 REGEXP 消费者语义调整。

## 2. 必须保持的不变量

1. 静态不合法组合在绑定/PREPARE 报3995，运行时 BLOB 描述符不能使它合法。
2. subject、pattern、replacement 独立解码；pattern 不取得 subject 位置单位的所有权。
3. StringSource 仅表示来源/所有权，不证明 UTF-8 合法；列、表达式、字面量都可能含任意字节。
4. 执行转换只作用于 REGEXP 局部视图，原参数 vector 不改写；BIT/JSON/数值/普通字符串消费者继续看到原值。
5. 返回域独立于匹配域；直接 prepared marker 的返回元数据不随执行包类型改变。
6. 错误优先级是外部契约。缓存命中不豁免输入验证；正常非 NULL 行不重复执行仅服务 NULL 早退的优先级验证。

否定例：用 `subject.IsBin || pattern.IsBin` 同时决定全部四项；以 StringSourceExpression 跳过扫描；首次执行 BLOB 后固定后续元数据；NULL subject 无条件吞掉 malformed pattern。这些都不可接受。

## 3. 四个独立矩阵

### 3.1 静态合法性

| 来源 | 静态职责 | EXECUTE 职责 |
|---|---|---|
| 普通 text 表达式/列 | 保持 text 分类 | 验证实际字节 |
| 显式 BINARY cast/literal、fixed binary/BLOB 列 | 按函数静态兼容规则判定；不合法 binary/text 配对报3995 | 不重新放宽静态规则 |
| bare 用户变量 | 保留变量来源与当前域 | 按实际值解码并决定允许的返回域 |
| SQL PREPARE / COM_STMT marker | 保留 marker 身份及上下文约束；PREPARE 拒绝可静态证明不合法的配对 | 接受上下文允许的 text/BLOB 值，不改变静态承诺 |
| typed NULL | 保留类型参与静态检查 | NULL 值按函数优先级处理，不以值空消除类型约束 |
| nested REGEXP | 消费内部函数的返回域 | 不直接继承内部 matcher 的域 |

这不是“所有 binary/text 都非法”的泛化规则；列、变量、marker、显式转换身份必须区分。具体交叉格以 `computation_wrapper_test.go` 和 `regexp_sources_test.go` 的静态/协议矩阵为验收清单；若实现出现矩阵未定义的来源组合，需补设计决策而非猜测映射。

### 3.2 解码与匹配

| 输入 | 转换 |
|---|---|
| text | 按 MySQL UTF-8 扫描规则验证/截断，再匹配 |
| binary 与 text 混合 | binary 按 Windows-1252 facade 解码；text 仍按自己的 UTF-8 规则 |
| 合法同域 binary 配对 | 可保留 byte matcher；ASCII 不额外复制 |
| text replacement 用于 binary matcher | 先验证原 text，再转换为 matcher 表示 |

UTF-8 lead C2–DF需2字节，E0–EF需3字节，F0–FF需4字节。剩余输入不足所需宽度时只保留合法前缀；输入长度充足但 continuation、overlong、surrogate 或范围非法时报3854。错误展示至多六个源字节，非打印字节用大写 `\\xNN`，保留0x20–0x7F，超长加省略号。

### 3.3 位置、偏移和 anchor

subject 决定字符/字节位置；不允许 binary pattern 把多字节 text subject 的位置改为字节位置。INSTR 的显式 pos 将输入重定基到 suffix：只验证/匹配 suffix，丢弃前缀不参与转换；anchor 必须遵守该重定基规则。SUBSTR/REPLACE 的终点、occurrence 规则独立于 INSTR，不合并成一个未经证明的公共位置策略。索引转换和输出切片必须位于合法字符边界；用多字节字符、前缀非法字节、末尾位置和 anchor 对照证明。

### 3.4 返回编码

| 来源/匹配 | SUBSTR/REPLACE 返回 |
|---|---|
| 直接 SQL/COM_STMT marker | text 元数据，重复 EXECUTE 稳定 |
| bare binary 用户变量等非 marker binary 来源 | 按 REGEXP 返回域规则保留 binary |
| matcher 域不同于返回域 | 结果完成后显式编码转换 |

Windows-1252 逆转换中不可表示字符按 oracle 用 `?`；不得修改原输入或把 matcher 内部表示泄漏为结果。谓词/INSTR 返回布尔/整数，不套用字符串编码策略。

## 4. 来源与所有权流

`binder 分类 → fold/prepare 保留来源 → 每次执行局部 specialization → REGEXP 参数视图 → 独立解码/匹配 → 返回编码`。

binder 是静态合法性的首个 owner；prepare/rebind 是执行来源恢复的 owner；函数实例拥有 conversion 临时状态和 regexp cache；result vector 拥有最终输出。fold 必须保留已有来源而非把变量/marker 强行变成“已验证字面量”。prepared reuse 每次恢复来源，不能把上次 BLOB 编码状态残留给下一次 text；错误后复用也须成功。类型化 NULL 与 null bitmap 各司其职。

不新增 wire 字段或共享可变来源状态。物化后无法保留来源的一般问题明确不在本闭包，不能宣称 alias 等价性已经解决。

## 5. NULL / 错误顺序

- 非 NULL pattern 的解码/编译错误不能被 NULL subject 吞掉。
- SUBSTR 显式位置：验证非 NULL pattern → position/occurrence 自身 NULL 返回 NULL → position<=0 报1583 → 处理 NULL subject/pattern → subject 验证/匹配。全非 NULL 行直接执行核心路径；仅 NULL 早退路径调用 precedence helper。
- REPLACE：非 NULL pattern 解码/编译 → optional NULL → 非正 position → replacement 原域转换错误 → 剩余 NULL → subject 验证/执行。NULL pattern 不豁免 malformed replacement。
- INSTR 保持独立终点/位置规则，不因 SUBSTR 重构改变。

失败不发布本行成功结果；上层沿现有错误路径丢弃失败结果。无新 worker、RPC、锁、重试、持久化 commit 点，取消/超时继承现有同步函数与执行器生命周期；本设计不承诺扫描中新增 cancellation poll。测试持有的 rows/statement 在成功获取后立即注册同作用域清理，循环使用逐次 helper，避免 defer 积累。

## 6. 成本、边界和安全

设批次 N 行，输入长度 S/P/R，结果 O，缓存条目 K。新增域验证/转换成本按每行 O(S+P+R+O) 计，不声称早匹配可跳过全部文本验证。SUBSTR 正常显式位置路径不得增加一次 P 扫描和 cache lookup；常量 pattern 的输入验证仍可能是 N×P，缓存只省编译。

现有函数实例 cache 上限100个条目，插入满额时淘汰已有条目；它不是全局、跨租户或持久 cache。**条目数上限不等于字节上限**：内存取决于100个 pattern 及编译对象尺寸；本次不新增 byte-budget cache。转换临时值按实际输入分配，binary→UTF-8 单字节最多扩到三字节；输出按实际替换结果增长。匹配引擎本身的复杂度和替换膨胀并不因此变成常数；不得宣称已有硬性输出/CPU 配额。本设计接受继承现有执行内存/查询治理的限制，不扩大为新的缓存框架。

不新增认证、租户可见状态或跨租户共享。用户可提交长 pattern/高膨胀 replacement 的既有容量风险仍存在；错误消息至多暴露当前请求操作数六字节，不新增全文日志/metrics cardinality。若评审要求绝对字节/CPU硬上限，应作为明确设计阻塞决定，而非把100条当作已解决。

## 7. 替代方案与权衡

| 方案 | 正确性/复杂度/运维 |
|---|---|
| 维持统一 IsBin 策略 | 最小代码量，但无法满足独立解码、marker 元数据、NULL 顺序，拒绝 |
| 全局来源/类型系统和 wire 重构 | 可解决物化 alias 等更大范围，但影响非 REGEXP 消费者、混合版本与序列化，超出本问题，拒绝 |
| 局部四域分离（选用） | 无 wire/存储迁移，影响面可审计；代价是来源分类和局部转换代码，需要交叉矩阵证明 |
| helper 返回已编译 matcher | 可消除重复扫描，但扩大 helper/核心 API 和状态传递；本轮优先采用仅 NULL 路径预验证，后续有独立性能证据再评估 |

## 8. 升降级与发布

无 catalog/on-disk 格式变化，不需数据迁移；重启清空进程内缓存，备份恢复不含它。回滚二进制恢复旧 REGEXP 行为，但不会修复此前以不同语义写出的业务结果，需要业务重算评估。

**无格式变化不意味着混合 CN 的 SQL 语义一致**。新旧 CN 可能对同一非法字节/来源返回不同错误或结果；本设计不保证混合版本阶段的等价结果。发布约束：受影响查询固定路由到同版本 CN，升级完成后重新建立连接/prepared statement；不要跨版本复用 prepared 执行上下文。需要跨版本分发的 REGEXP 工作负载在同版本切换后启用；若现有部署无法保证此约束，发布仍阻塞，需另作版本能力门禁设计。

不新增 feature flag。以小范围同版本环境验证后切换；观察3854/3995错误及已有查询耗时/内存指标。异常时撤回版本或路由受影响查询，不添加静默吞错降级。没有证据证明混合 CN 集成已通过。

## 9. 验证和实现对照计划

| 不变量 | 最小证据/责任文件 |
|---|---|
| 静态合法性/typed NULL/folding | binder、visit_plan_rule、function/type_check；frontend computation_wrapper UT |
| 每个操作数独立解码/输出 | regexp_operands UT、func_builtin_regexp UT |
| malformed UTF-8/顺序 | regexp_invalid_operands UT + MySQL8.4.8 oracle |
| prepared reuse、packet252/254、元数据、错误恢复 | TestRegexpPreparedProtocolSources 真实 COM_STMT |
| 公共 SQL/session 状态 | func_regexp_sources、regular_instr 及相关 BVT |
| wrapper 热路径 | BenchmarkRegexpSubstrWrapperPosition：1000行、1001-byte常量、短subject，arity2/pos1对照 |

既有证据截至826a355564：相关包/COM_STMT、构建、9组BVT报告通过，wrapper约1.26ms而非1.9ms。它们是历史记录，不冒充本次 merge 后的 exact-head PASS。既有覆盖率统计也须检查 profile 与源版本一致性，不能无条件沿用91.14%。设计文档本身无需重复 native suite；批准后按实际依赖差异选取有效复用或补跑。

对照顺序：先审本设计的矩阵与边界；批准后检查完整16文件与合并后依赖，列出偏差；偏差影响契约时修订设计并重新审批。生产代码不因为这份草案自动获得通过。

## 10. 待决事项与审批记录

- 阻塞：独立审批人尚未批准 r1；需要确认静态来源矩阵、继承现有资源限制的权衡、同版本发布约束及两项明确排除。
- 阻塞：设计批准后的实现对照尚未完成；不得请求实现重新批准或标记review已解决。
- 版本定位：发布设计文档时以文档提交 SHA 构造永久链接，并在 PR 正文引用；任何实质修改生成新修订，不覆盖已审批含义。

审批人：待定。审批版本 SHA：待提交。决定：REQUEST_CHANGES（设计待审批）。审批链接：待记录。

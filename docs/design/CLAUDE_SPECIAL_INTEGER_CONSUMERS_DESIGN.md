# FORMAT / MAKEDATE / MAKETIME 特殊整数参数兼容性补充设计

- 状态：Approved；用户于 2026-09-26 在补充设计 review 后回复 `go ahead`。原候选 v97；完成 74 个 open PR 的分配审计后避让已被其它 PR 使用的 v97，最终采用 v98；合并前仍需核对协议分配。
- 任务：#28981；父任务：#28893；共享基础：#28977。
- 基础设计：`CLAUDE_INTEGER_PARAMETER_BINDING_CONTRACT_DESIGN.md`。
- 检查基线：`3f0a68bd8034d37e098765920f328985c308c9ff`。
- 实现 PR：待实现与验证完成后创建 draft PR，关联 #28981。

## 1. 为什么需要补充 review

基础设计明确要求：迁移引入新的 execution identity、协议版本或持久化机制时，必须返回设计 review。本次不是只添加整数 metadata 即可：

- FORMAT 仅有 overload 0/1，precision 物理参数是 varchar；不能在旧 identity 下发送 INT64 precision 给旧执行器。
- MAKEDATE 仅有 overload 0，物理参数均是 varchar；旧内核先执行 `int32(day)`，无法满足新 calendar 契约。
- MAKETIME 旧 overload 0 支持 INT64 三参数，但其余旧 signature 没有 INT64 hour/minute 与 FLOAT/varchar/UINT64 seconds 的组合。复用旧 FLOAT signature 会再次让 hour/minute 经过 FLOAT。
- 当前 `appendIntegerArgument` 在源已是目标 INT64 时不插入 CAST。因此 `MAKEDATE(bigint_column, bigint_column)` 等表达式可能完全没有 CAST 5–8。
- v85 的 `IntegerParameterCoercion` scanner 仅识别 CAST 5–8；它不能证明目标 CN 认识本次新增 overload。即便强制插入 CAST，已经发布的 v85–v96 节点仍不认识新 overload。
- 当前最新 MORPC 为 v96。v85 已发布，不能扩写其 capability 含义并假设旧节点支持。

## 2. 决策提案

保留共享整数转换语义不变，追加必要的特殊执行 identity，新增独立 capability **SpecialIntegerConsumers**。协议编号以 Vspecial 表示，最终分配 **v98**。提交前扫描全部 74 个 open PR 的 `pkg/defines/const.go` 差异，发现 #29241、#29139、#29024、#28851、#28521 均曾使用 v97，未发现 v98 分配。当前 main 为 v96；不在本 PR 定义或实现其它 PR 的 v97 capability。合并前须按最新 main 再核对编号，避免不同 feature 共用一个发布版本。

### 2.1 执行 identity

| 函数 | 保留旧 identity | 新 identity 提案 | 说明 |
| --- | --- | --- | --- |
| FORMAT | 0/1 | 2/3 | 分别为两/三参数，precision INT64；第一参数保留当前数值/文本域选择；locale 沿用现有规则 |
| MAKEDATE | 0 | 1 | INT64 year/day，返回 varchar；在完整整数/calendar 域检查后才窄化 |
| MAKETIME | 0–35 | 按需从 36 顺序追加 | hour/minute INT64；seconds 保留近似、精确文本及 unsigned 域；全 INT64 可继续复用 0 |

不得按 source 类型笛卡尔积新增 hour/minute overload。仅 seconds 的真实执行域允许不同 signature。旧 identity 的参数、实现和返回类型保持不变；binding subset 排除仅承担旧 source coercion 的 entries。

整数参数舍入/溢出继续交由 CAST 5–8，无 executor-local 第二套转换规则。不存在“为兼容先 INT64 再 FLOAT”的新路径。

### 2.2 日历与时间

MAKEDATE：

1. 共享转换检查 INT64 范围；转换溢出是 error，不是 NULL。
2. day <= 0、year < 0 或 year > 9999 返回 NULL。
3. 按现有规则映射 year 0–69 到 2000–2069，70–99 到 1970–1999。
4. 用已合法年份的年首日期与 9999-12-31 差值确定最大 day；在 INT64 中比较，然后进行受证明安全的日期加法/窄化。
5. 超 calendar 范围返回 NULL；不让 int32 wraparound 产生貌似正常的日期。

MAKETIME：复用已有时间构造及 seconds 解析规则；INT64 合法但超 838 的 hour 继续按现有结果范围处理，minute 越界返回 NULL。共享转换本身溢出仍报错。seconds 不标记为整数参数，保留 microsecond carry、精确文本/DECIMAL、unsigned 以及结果 scale。

FORMAT：只迁移 precision，输出格式与 exact/approximate 第一参数域维持现状。precision 的最终显示位数范围继续由已有 formatter 的规则负责。

## 3. 协议与持久化 owner 闭环

- `pkg/pb/plan` feature scanner 按稳定 function ID + 新 overload 检测 SpecialIntegerConsumers，不依赖函数名、源类型、是否有 private CAST。
- scanner 校验新 signature 的参数数量和 canonical integer 位置，未知/畸形输入失败；folded expression 的来源树使用已有遍历边界。
- `pkg/sql/compile` 沿用已有特征准入模式：placement 避开不支持节点，实际 send 再检查 destination，receiver 解码后在执行前拒绝不支持 identity。
- Vspecial 以下（包括 v85）不能接收新 identity；unknown capability 失败关闭。CAST 5–8 仍独立要求 v85。
- 持久化需求取整棵 owner 的最高 floor。MAKEDATE/MAKETIME 新 identity 使用 Vspecial authoring/read admission，保留 origin SQL 与 protobuf round-trip；旧 binary 不得静默执行。
- 不新增 protobuf 字段、状态机、后台任务或独立持久化格式；复用现有 durable floor 和 owner traversal。

## 4. FORMAT pre-v59 bridge

目录 FORMAT 继续使用历史语义，与 transient query 不同，不把 catalog precision 私有转换简单替换成 CAST0：两者的舍入不同。

现有 `preservePersistedFormatCompatibility` 在 folding 前执行。扩展此边界：

1. 不再只检查第一参数是否 numeric；所有新 FORMAT identity 都需要桥接，包括第一参数已是 varchar 的情况。
2. 第一参数沿用现有 numeric -> varchar bridge。
3. precision 只撤销其自身 integer-context 插入的转换，恢复真实 source，再按 legacy precision varchar 规则绑定。
4. CASE/IF/NULLIF 已下推转换到选择分支：只沿 precision 的透明选择分支恢复 source，然后重新进行普通 selector 公共类型调和；不得保留失配的 INT64 selector metadata。
5. 用户显式 CAST 是 value boundary，保留原样。非透明 value-producing 函数是边界；不得递归删除其自有整数参数转换。例如 FORMAT 的 precision 中调用 SUBSTRING_INDEX，其内部 count 仍要求 v85。
6. 显式选择 FORMAT legacy 0/1，不重新走新 binding subset；在 folding、protobuf round-trip 和 origin SQL 重建中保持同一 catalog 行为。
7. 桥接后整棵表达式再次扫描 capability：简单 legacy FORMAT 不应残留本次新增 Vspecial 或其 precision 自有 v85 转换；若其它真实子表达式需要新版本，保留要求，不虚假宣称整个 owner 可降级。

不通过保存绑定前可执行快照来恢复 source，避免恢复已失效的列位置或子查询计划。不修改已持久化的 legacy FORMAT 数据。

## 5. 备选方案与代价

- 仅加 metadata，INT64 再转 varchar 调用旧内核：不能满足 MAKEDATE 安全边界，且保留重复解析，不采用。
- 在旧 overload 下悄悄接收新物理类型：旧 CN 不认识 signature，legacy compatibility 不可靠，不采用。
- 只依赖 v85 private CAST gate：纯 INT64 输入没有 CAST，且 v85 不包含新 overload，不采用。
- 全部转 FLOAT 再复用旧 MAKETIME：损失精确域，违反 hour/minute 契约，不采用。
- 对所有 FORMAT catalog 表达式一律提高 floor：破坏明确要求保留的 pre-v59 bridge，不采用。
- 专用 identity + 独立 capability：新增协议分配和有限兼容测试成本，但 ownership 清晰、对已发布 binary 的行为可证明。

## 6. 验证与资源预算

| 契约 | 独立证明 |
| --- | --- |
| 参数 metadata | 正常/speculative resolver；constants/columns；确认只规范五个参数位置 |
| 专用 identity | 新 binding canonical；旧 identity 直接执行；参数数量/类型负例 |
| 日历 | 两位年份边界、闰年跨年、9999 最大日期、day 超 int32、INT64 极值 |
| FORMAT | exact/approximate 第一参数相同输入对照；precision 文本/DECIMAL/FLOAT/explicit REAL；NULL/overflow |
| MAKETIME | fractional seconds/scale、unsigned seconds、hour/minute 不经 FLOAT、合法 INT64 大 hour 与转换 overflow 的差别 |
| prepared | SQL EXECUTE/binary typed parameters、NULL-first、重复执行/类型变化、失败后再次执行 |
| 协议 | 无 CAST 的 INT64 列表达式也被检测；Vspecial-1 拒绝、Vspecial 接收；unknown destination；send-time downgrade |
| catalog | DEFAULT/generated/CHECK，fold 前后、序列化/origin SQL、restart、FORMAT precision selector 与嵌套其他整数 consumer |

先 focused UT，再 owning/dependent packages 与增量静态检查；公开 SQL 通过 mo-tester 生成并复核 result，再正常比较。restart/升级证据不能由纯函数 UT 代替。

每 row 只做一次共享转换，执行器线性扫描；不新增整 vector 副本、goroutine、lock、queue、cache、日志或高基数 metrics。结果 allocation/append error 沿现有 result owner 清理；prepared 使用现有 template deep-copy，失败不得污染下一次执行。catalog rewrite 的副作用只限当前构建拥有的表达式，不写回共享存储。

## 7. 审批结论

用户已批准：增加必要的 append-only execution identity，并分配独立 MORPC capability（最终为 v98，合并前再次核对）；共享 CAST v85 不改号、不扩写已发布含义。按本文实现、验证并创建 #28981 的 draft PR。

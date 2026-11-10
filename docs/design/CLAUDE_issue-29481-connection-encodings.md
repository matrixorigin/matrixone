# C02：连接编码与协议边界设计草案

状态：r3，2026-10-08，**待评审，不批准生产实现或上线**。保留 r2 已评审的架构决策；本修订修复新增 P2 指出的参考请求/响应配对损坏，提供同一 send buffer 的可复现探测及紧接部分结果错误的成功请求。

所属任务：[#29481](https://github.com/matrixorigin/matrixone/issues/29481)；上位任务 [#29479](https://github.com/matrixorigin/matrixone/issues/29479)；基础 [C01](CLAUDE_issue-29480-collation-metadata.md) / [#29503](https://github.com/matrixorigin/matrixone/pull/29503)。基线：`d9eb0e0ce88bec403dce4cd6c7fbdf15f19e1b03`。

本文先提交设计评审，不声称 C02 已修复，不替代 C11 升级或 C12 发布验收。本轮仅交付本文，生产代码、测试及默认值不变。阻断决策见第 8 节，必须在实施前关闭。

## 1. 已核实的问题与兼容约束

- `pkg/frontend/charset_admission.go` 负责新增请求准入；latin1 已在会话设置、握手和 change-user 等入口拒绝。C02 必须扩展行为而非另建名称白名单。
- `MysqlProtocolImpl.HandleHandshake`、`parseChangeUserRequest` 已解析协议 collation ID；前者字段为 uint8，后者为 uint16。编号 309 不可截断后接受为其它编号。
- `ExecRequest` 的 COM_QUERY 直接将收到的字节转为 Go string；这不是按 client/connection 编码转换的证明。
- `doSetVar`（由 `handleSetVar` 调用）的 SET NAMES 分支逐项设置 client/connection/results，有 TODO，未在该分支应用显式 `COLLATE`。C02 需要把相互依赖的更新作为一个经过预检的状态转换，而非多次部分发布。
- `ParseExecuteData` 将参数放入复用的 text vector，long-data 先物化并释放源 buffer；必须结合真实参数类型/来源追踪字节语义，不能把 vector 的 T_text 或 Go string 当作文本编码证据。现有注释指出 binary/varbinary 也使用 MYSQL_TYPE_VARCHAR。
- `setCharacter` 默认对字符串列写协议 ID 33；`makeColumnDefinition41Payload` 输出列上的 uint16 ID。实际结果字节和列元数据必须一起验证，不能只更新 ID。
- planner 的 `bindConvertUsingCharset` 与 evaluator 的 `builtInConvertUsingCharset` 使用既有兼容准入；scanner/binder 已有 introducer 路径。需要保留并扩展这些共同边界，而非只修一种 SQL 拼写。

**已有明确决定优先于本草案：** C01 文档第 2.2、8 节记录用户要求 utf8/utf8mb3 继续静默映射 utf8mb4，包含新增同名声明；严格三字节需要独立兼容性/发布决定。#29481 的 utf8mb3 验收与此冲突。不能由本次实现隐式撤销该决定，也不能因 #29480 已关闭而认为严格 utf8mb3 已获准。

## 2. 范围、不变量与否定例

可实施首批保留 binary/utf8mb4 及 utf8/utf8mb3→utf8mb4 兼容映射，闭合会话联动、协议字节/metadata、binary 可观测域与失败清理；补 ascii client/results 的边界转换，不为此启用 ascii 列/index 比较域。ascii connection/SET NAMES ascii 的默认 collation 若尚无完整执行消费者，继续拒绝，直到相关 gate 通过。严格 utf8mb3 与 GBK 不作为首批实现前提；严格三字节拒绝的 issue 验收项明确延期，不能将首批交付标为完整修复。codec 可内部验证严格三字节，但不公开激活。

完整 C02 跟踪范围仍包含握手、COM_CHANGE_USER、SET、introducer、CONVERT USING、文本/预处理输入与所有输出边界，以及 C10 可接入的有界 GBK 接口。编码能力与 collation 比较能力分别准入，不能因为协议转换可用就启用新的 column/index 域。

不改变产品默认值、既有对象身份、存储格式或比较/key 算法；不实现 GBK、latin1、其它未列字符集、分区表、存储过程、UDF 或 warning 数量/文本兼容。既有 UTF32 的 DDL 兼容拼写不授权协议/表达式转换。

不变量：

1. 会话对每个方向只有一个有效编码状态；协商值、sysvar 展示与实际消费者一致。
2. `client → connection → 内部文本 → results` 在明确的语义边界执行；仅对 wire/SQL 可观测的 binary 源域承诺原字节保留（NUL、0xff），不承诺恢复客户端语言层 string/[]byte 身份。相同 wire、相同 session 与 SQL 必须产生相同行为。
3. connection 编码不等于列 repertoire。连接转换成功不豁免列写入校验；连接失败不得进入列写入。
4. 准入/转换失败不能留下半更新的相互依赖会话状态、半更新绑定或静默 fallback。
5. 新 latin1/latin1_* 请求始终拒绝；历史恢复能力不是新请求准入。
6. NULL、chunk、重复绑定、reset、取消和断开都有既有所有者负责清理；无无限 session cache。

否定例：SET NAMES 的前三个变量已变而 COLLATE 失败；一个 BLOB 参数因 vector 为 text 被改写；UTF-8 多字节序列跨 long-data chunk 被逐 chunk 拒绝；结果标成 ascii 却仍发送高位字节；一个 multi-statement 请求前面的 SET 改变后面语句时，后者仍使用旧 connection 状态。

## 3. 选择、所有者与数据流

### 3.1 可选方案

| 方案 | 正确性与代价 | 结论 |
| --- | --- | --- |
| 维持现状，只修改 sysvar/列 ID | 未产生真实转换，不能满足字节契约 | 不采用 |
| 在每个 SQL 函数、算子、驱动层分别转码 | 重复解码；破坏 binary 来源；所有权与热路径成本扩大 | 不采用 |
| 由共同 codec 执行显式边界转换，前端/表达式保留来源与状态 | 复用准入、能区分方向和 binary；需要协议、lexer 与绑定共同验证 | 建议，待阻断决策关闭后批准 |

复用 `pkg/common/collation` 静态身份，codec 放在其无 frontend 依赖的 `encoding` 子包，不添加动态 registry。拟冻结两个窄 API：

```go
// src/dst 来自已有字符集枚举；policy 仅区分 identity、connection、result、CONVERT。
Convert(ctx context.Context, pool *mpool.MPool, src, dst collation.Charset,
    policy Policy, input []byte, limit int64) (output []byte, owned bool, isNull bool, err error)
Validate(ctx context.Context, charset collation.Charset, input []byte) error
```

实际目录枚举名以 `metadata.go` 的 `Charset` 为准。成功 `owned=false` 表示借用 input（不修改，生命周期不得超过 input）；`owned=true` 表示唯一 owner 为调用者，使用同一个 `pool.Free(output)` 一次释放，移交到 vector/packet 时先复制后释放，不能把 mpool slice 当作普通 Go backing array 留在 cache。CONVERT 的 NULL 成功返回 `nil,false,true,nil`；空字符串返回 isNull=false，与 NULL 不混淆；SQL NULL 输入由 caller 直接传播，不调用 codec。失败返回 `nil,false,false,error`，helper 自行释放尚未移交的分配。`Validate` 无 payload 分配；身份相同的 conversion 与 repertoire 校验不是同一操作（见第 7.1 节）。

使用调用所属 `proc.Mp()`，通过 `MPool.Alloc(size,true)` 分配并注册 cleanup，不能偷偷改用无会计的 make。先计算所需输出长度，再分配精确长度；检查 `limit=min(max_allowed_packet,mpool.MaxAllocationSize(),调用者剩余 vector-area 限额)` 和 int 溢出。首批输出长度不超过输入字节数，替换为单字节 0x3f；取消每最多 4096 个输入字节以及分配/发布前检查 context。后续 GBK 的实测最坏扩张上界必须先补设计，当前 GBK 仍拒绝。无 payload cache、goroutine 或重试。

### 3.2 会话状态

`Session` 是 client/connection/results 和 connection collation 的有效状态所有者；protocol 对象只保存协议协商所必需的值，不成为 SET 后的第二份编码权威。优先从现有 sysvar 状态构造不可变、请求内快照，不新增跨请求转换缓存。若现有快照无法表达严格/兼容策略，则必须在批准的迁移契约中显式扩展，而非只改本地结构体。

握手先校验并构造候选状态，认证成功后与 session 初始化一致发布；认证响应、salt 和密码材料不可转码。COM_CHANGE_USER 复用现有认证/重置状态流程，不在认证失败后发布候选编码，保留当前既定的连接关闭策略。COM_RESET_CONNECTION 必须恢复经过批准的初始状态，不能残留结果编码或转换内存。

`doSetVar` 是发布 owner：在既有赋值顺序中计算相互依赖的编码 tuple，复用正常变量转换/scope/权限检查；不由 scanner 发布。SET NAMES 同时设置 client/connection/results，connection collation 取显式匹配值或该有效字符集默认值；SET CHARACTER SET 设置 client/results 为请求字符集，connection/collation 取当前数据库默认值。直接修改 character_set_connection 选择其默认 collation；修改 collation_connection 同时派生对应 connection 字符集；client/results 赋值不改变 connection。DEFAULT 按现有有效默认解析，不能用历史快照入口绕过新请求检查。

先预检完整编码 tuple，再把 sysvar 值和 migration replayability 作为一个发布单元更新；编码变量本身不调用未知外部副作用 hook。发生其它 SET runtime hook 错误时不发布候选编码 tuple。同一 SET 同时含 sql_mode 时，sql_mode 复用现有 `SetSessionSysVar` 的类型转换、`sesSysVars.Set` 与 `updateSqlModeNoAutoValueOnZero` 路径，成功返回后下一个 statement 才读取两者；失败不会解析下一条。这里不承诺混合 SET 的其它非编码副作用整体回滚，也不改变既有事务错误 owner。

全局默认值继续由现有账户级持久化和缓存所有者负责；新请求拒绝与历史加载保持分开。SET GLOBAL 的既有权限、scope、成功发布点不能被 codec 绕过。

### 3.3 SQL 输入与表达式

保留整个 COM_QUERY 的原始字节，不在请求入口一次性转码。第一条 statement 使用请求开始时有效 client/connection/sql_mode；剩余原字节在每条成功 statement 后重新按当前 session 字段解释。请求快照只用于第一条和不随 SET 改变的 rewrite-policy 等已有请求来源，不冻结整个请求的 client 编码。

直接扩展 `prepareSQLModeStagedExecution` / `nextSQLModeStatementInput` 和 `doComQuery` 的 staged loop，将“影响后续解析的 SET”识别扩为编码变量及 SET NAMES/CHARACTER SET（含与 sql_mode 混合的 SET）。这只是扩大既有 owner 的输入与 parser 参数，不引入独立 parse/execute 状态机；未来 GBK 下不能用初始模式 parse-all 找分号，必须由该 owner 用当前状态 ParseFirst 取得字节 offset。`newSQLStatementInput` 保留原请求 provenance；`rewriteSQLStatementInput`、`refreshStatementScopedSessionInfo`、hasMore/result flags、事务错误和 parse-error 记录仍用原路径。

具体 trace（第 7.1 节 raw-wire 参考）：

1. 初始 client/connection/results=utf8mb4。发送 `SET NAMES utf8mb4; SET character_set_connection=ascii,sql_mode='NO_BACKSLASH_ESCAPES'; SELECT HEX('a\nb'); BAD SQL; SELECT 1`。前两条仅含 ASCII 语法字节；第二条成功发布 connection=ascii 与新 sql_mode，client/results 仍为 utf8mb4。第三条在 next-statement 边界读取这四项，反斜杠不再处理，结果是 ASCII 字节 `615C6E62`（该 HEX 结果本身的 wire hex 为 `3631354336453632`）。
2. 第四条用同一新状态解析，返回 1064/42000；第五条不执行。第二条成功的编码/sql_mode 状态保持，不因后续 parse error 回滚；下一请求可查询这组值。
3. `SET NAMES ascii; SELECT '<c3a9>'` 中第二条以**新 client=ascii** 解读其原字节，而不是请求开始的 utf8mb4。MySQL 的 identity 路径保留 c3a9；而只 SET connection=ascii、保留 client=utf8mb4 时 c3a9→3f。两者不可混为一个“先整包 decode”的路径。首批 MO 只执行已准入组合；ascii connection gate 未通过则第一条拒绝，不提前执行后续语句。
4. COM_RESET_CONNECTION 进入已有 session reset owner，释放 prepared/转换资源、恢复现有 reset 默认 tuple 和 sql_mode；下一请求重新从该 tuple 开始。参考 MySQL 的 reset 回到服务器 utf8mb4/0900 默认，而非握手 ID 45；MO 保持自己的既有产品 reset 默认，不顺带改成 MySQL 默认。

scanner 先在当前 client/sql_mode 下处理语法与 escape，再由 introducer 指定文字源编码；introducer 不更改 escape 规则。sql_mode='' 下 `_binary'a\nb'` 与 `_utf8mb4'a\nb'` 都得到 `610A62`；NO_BACKSLASH_ESCAPES 时两者都保留 `615C6E62`。binary introducer 保留的是 escape 后的 literal 字节，不保证原 SQL token 一字不变。hex/bit literal 的显式字节沿现有路径解码。认证、长度前缀、数值不转码；标识符/COM_INIT_DB 纳入文本路径。SQL PREPARE 和 COM_STMT_PREPARE 复用相应语义入口。

CONVERT USING 使用 SQL 可观测源域和目标编码，复用同一个 policy；不能借该路径启用 C01 禁用 collation，也不扩大 C04/C06 比较范围。

### 3.4 预处理参数与结果

wire type/flag 与 SQL 是唯一可用证据：go-sql-driver/mysql v1.9.3 的非 nil []byte 和 string 都发送 type=254、flag=0、同一 lenenc payload；相同 payload 不可区分，**统一按 client 编码的文本参数处理**。不建立应用语言 provenance 或依赖 Go string/vector.T_text 猜测来源。unsigned flag 只控制数值，不选择 binary。复用现有准备参数 kind/domain 表示实际协议分类，但不得填入客户端未传送的身份。

明确 binary wire 类型 BLOB/TINY_BLOB/MEDIUM_BLOB/LONG_BLOB/GEOMETRY 的 payload 是 byte 域；BIT 保留现有 typed bit 路径、不作文本转换；STRING/VAR_STRING/VARCHAR 是文本域。SQL `_binary`/hex literal 或显式 binary 类型运算是其它可观测 byte 域；它们保留已取得的字节，不追回输入阶段已经发生的文本转换。给 binary 列写入并不授权在协议阶段猜测原应用类型。用户用 driver 的 []byte 想传任意字节时必须使用可观测 binary wire 类型、显式 binary client 模式或明确的 SQL 字节路径；单靠 []byte 不提供保证。JSON/DECIMAL 仍走各自已有 typed 校验，不放入二进制逃生分支。

NULL bitmap 是值缺失，不引发转换；MySQL 的既有 long-data 优先级（包括零长度 chunk）保留。long-data 只累积字节，不决定类型；EXECUTE 的成功新 type vector 决定解释，new-params-bound=0 复用上一成功 type vector（缺失则拒绝），不会复用上次语言类型。按当前执行 client/connection tuple 处理，不冻结 SEND_LONG_DATA 时编码。`c3|a9` 两块在完整值上按文本处理，与单块 c3a9 相同；显式 BLOB 下 ff 保持 ff，STRING 下遵循参考转换行为而非 binary 承诺。

失败重绑沿已有 owner 收敛：`ParseExecuteData` 先借用旧 ParamTypes、在调用内暂存新 type vector；candidate vector 由该次 proc.Mp() 拥有且暂不挂到 PrepareStmt/process。逐参数从 packet/longDataBuffers 借用源，转换后复制到 candidate，立即释放 owned 转换输出；成功复制某 long-data 值后释放对应 buffer。完整解析/转换后一次挂接 candidate、再提交新 ParamTypes，并释放被替代参数 vector；运行 compile 只能在该提交后读取参数。

失败时释放 candidate 与剩余转换输出，通过 `ExecRequest` 现有 `clearBinaryParamState` 清理本次 long-data、参数和 process 引用；失败新 type vector 不覆盖上一成功 ParamTypes。**不承诺保留上一执行参数值**：现有每次执行结束会清理 params，但保留 type vector 以便下一次复用。已消费的 long-data 不恢复、不隐式重试；SEND_LONG_DATA 自身错误仍按 `latchLongDataError` 保留首错至 reset/close。Close、COM_STMT_RESET、disconnect 沿原 PrepareStmt/session owner 清理；取消/分配失败同样进入清理。

峰值为旧参数 area（若尚存在）+剩余 long-data容量+candidate area+一个最大转换输出+小型 type vector，而不是所有参数的转换副本同时驻留。这些 payload 共用 proc.Mp() 的会计上限；每参数/总 area 预检继承 max_allowed_packet/MaxAllocationSize。不得为原子性另存 long-data 的历史副本，或要求无界地保留旧执行值。

所有结果生产入口共用序列化边界：普通文本行、binary row、ColumnSlices 快速路径、cursor/fetch、列定义及 prepare metadata。文本值/字段名称按有效 results 策略输出；binary 值和数值/null bitmap 不转码。results=NULL 不转换，但元数据必须按 oracle 保留来源，不能把 NULL 当作声明 binary。字节长度与最大字符宽度须对应输出编码。准备时 metadata 与执行时 results 变化的关系必须通过客户端验证。

输出前完成字段转换再写其 lenenc 字节长度；ColumnDefinition41 的 length 则为声明最大字符容量×目标编码最大字节数（binary/数值按原 typed 路径），不是当前行长度。results=ascii 的文本元数据采用默认 ascii 协议 ID 11；协议 ID 11 作为 transport-only 默认投影纳入已有静态能力目录（不是第二套 registry），不参与 `ResolveSQL`；这不批准 ascii_general_ci 的 SQL 比较域。results=NULL 保留来源 ID，不改为 63。

不可表示输出按第 7.1 节替换为 0x3f；转换中的畸形源按该入口 policy 处理，不能用 generic strict error 代替参考规则。取消/OOM 等实际错误：尚未写出的当前 row 丢弃，已完整发送的列定义/行不能回滚，发送一个正常 ERR 包终止结果（无成功 EOF、不再执行后续 statement），编码消息按 results 策略；SQLSTATE/error number 为 ASCII 固定字段、不转码。第 7.1 节的 3141/22032 部分结果实测证明 ERR 后连接可复用。网络短写、context 导致无法完整发送 ERR 或 packet 序列已经破坏时由原连接 owner 关闭，不在坏流上重试。

## 4. 资源、失败、安全与性能预算

- 输入借用 packet 仅在当前请求有效；需要跨阶段保存的值由现有 statement/vector/packet owner 接管，禁止存储悬空借用 slice。
- 转换使用现有请求内内存会计，分配前检查输出上界、整数溢出、最大 packet、vector area/单次分配约束；不能先无限扩容再拒绝。数值预算复用当前配置限额，具体分配器和最坏扩张系数须在实现设计修订中固定。
- binary 和已验证且同编码的路径目标为零额外 payload 分配；需要校验的文本进行 O(n) 扫描，真正转换为 O(n+m)，不在逐行路径解析 charset 名称或整段 SQL。
- 首批编码为 ASCII/UTF-8 子集，文本通过 repertoire 校验后无需扩张；替换/错误策略可能产生不同输出大小，须纳入 oracle 与预算，不能在本文假定。
- long-data 由 PrepareStmt 的现有有界累积器所有；不复制全历史 chunk，不新增 session scratch 高水位缓存。临时转换资源在错误、取消、reset、close、断开释放，注册 cleanup 后才能进行下一步资源获取。
- 禁止新等待链、重试、worker、后台 cache 或动态指标标签。大输入若按块扫描/转换，必须检查 context，取消不依赖下游发送成功。
- 不记录原始参数、密码或 payload；跨租户复用 buffer 必须遵循原 owner 的销毁/隔离契约。错误不能降级到未经校验的编码，避免多字节和转义边界绕过。
- 针对 direct/ColumnSlices 路径、prepared 重绑和边界大小文本做 allocations/benchmark 前后对比；未经测量不承诺端到端性能。大数据只用于性能证据，不扩大功能 BVT。

## 5. 兼容、交付与运行

首批基础 codec 可以在无新公开准入情况下合入，但不标记 issue 完成。会话/SQL 能力启用必须服从 C01 的显式兼容决定、C11/#29490 的升级/旧 writer 保护及 C12/#29491 验收；不添加无法跨 CN 强制执行的本地开关来绕过门槛。

不改变默认编码、旧 Type/collation identity、持久值或物理 key。连接转换不重写旧数据。历史 sysvar/snapshot 仍沿既有恢复入口；若严格模式/有效连接状态需要跨 proxy/CN 迁移，必须有版本、reader 拒绝及混合版本保护；否则禁用该能力或在具备可执行路由保证时拒绝迁移，不能静默丢失。无保障时只允许同构升级。

连接为 transient 不代表可以忽略迁移、prepared reuse、result metadata 或 proxy。重启后按已批准默认重新协商；已启用能力的回滚必须验证没有向旧节点传递无法理解的状态，不涉及用旧 collation 重新解释持久数据。

实施 owner 为 C02；GBK 为 C10/#29489；升级/迁移为 C11；发布 oracle/上线证明为 C12。现有请求错误和内存指标作为初始诊断，若需新观测只允许固定编码/方向枚举，不记录输入内容。latin1 拒绝不能作为 UTF-8 fallback。

## 6. 验证地图与实施顺序

先盘点并扩展已有 `charset_admission_test.go`、protocol/prepared/long-data 测试、scanner introducer 测试、binder CONVERT 测试及 evaluator 测试；公开 SQL 复用 `test/distributed/cases/charset_collation/` 与相应 result。wire 字节/握手与参数来源需要真正客户端测试，mo-tester 的展示字符串不充分。

| 契约 | 最便宜内部证据 | 必须的公开证据 |
| --- | --- | --- |
| repertoire 与 codec | ASCII 0x7f/0x80；严格 U+FFFF/U+10000 仅内部；畸形 identity/跨编码/CONVERT 分开，空、NUL、NULL、binary 0xff；原子错误与预算 | 原始字节、error number/SQLSTATE、结果 metadata 与固定 MySQL 对比，兼容四字节仍接受 |
| 会话发布 | SET 候选状态成功/失败，显式 COLLATE、DEFAULT、results=NULL、scope | 握手/change-user/SET 后逐项状态和行为；latin1 拒绝后原状态不变 |
| 文本输入/表达式 | scanner/token 来源、escape/introducer/CONVERT 与近邻控制 | COM_QUERY/INIT_DB/PREPARE、SQL PREPARE、多语句 SET、literal 字节 |
| prepared 参数 | 同编号不同来源、NULL、类型复用/重绑、拆分 chunk、转换/分配失败 | 真驱动字符串/字节绑定及 raw-wire 对照，不只调用 helper |
| 结果 | text/binary/ColumnSlices/cursor 的共同 byte oracle | prepare/execute/fetch metadata、results 改变、二进制不变 |
| 生命周期 | error/cancel/reset/close/断开和两次重绑，内存 owner 归零 | 已有客户端/服务 fixture 上的有界失败路径，适用时 focused race |
| 升级/迁移 | 状态 reader/writer 及拒绝旧版本 | 只有实际改动迁移契约才运行对应 proxy/multi-CN/恢复场景 |

首选 0–2 行和最小边界字节，不用 sleep、重试、随机 chunk 或新重型集群替代明确输入。UT 先 focused 再 owning package；Go 版本跟随 go.mod，CGo 使用 mo-cgo-test；增量 gofmt/vet/lint。新增可执行逻辑覆盖率至少 75%，不能以覆盖率代替 wire/生命周期证明。

SQL result 由 mo-tester 生成、人工审查再 comparison，清理后同实例重复；协议 oracle 保存真正的输入/输出字节与 metadata。基线 oracle 未固定前，不写由当前实现自产的“期望结果”。

建议分批：①批准本设计与实际 oracle；②按兼容 utf8mb4/binary 闭合状态/staging/metadata 与 UT；③token/表达式/prepared 输入；④结果与 ascii client/results；⑤公开协议、失败与性能证据；⑥通过适用发布门槛激活；严格 utf8mb3、ascii connection 比较域和 GBK 另行满足各自契约。实现 PR 链接批准设计具体 commit；偏离先更新并重新评审。

## 7. 规范参考

- [MySQL 8.0 连接字符集](https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html)
- [MySQL 8.0 introducer](https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html)
- [MySQL 8.0 CONVERT](https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html)
- [MySQL 协议](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html)
- [TiDB 范围参考](https://docs.pingcap.com/tidb/stable/character-set-and-collation/)，只限定范围，不复制已知 repertoire/PAD SPACE 缺陷。

C01 冻结 backend 的已有 MySQL fixtures 不是连接转换 oracle；版本或语义测试不同不能自动复用。

### 7.1 已固定且实际运行的参考

参考服务器为本机 **MySQL Community 8.4.11（Homebrew macos arm64）**，运行进程的 mysqld SHA-256 为 `b2521ed46ab48f2d840daf2bc8073a8c5fa5dc3aec77cf1fac8230a8dd36de37`；不是声称运行了 MySQL 8.0。默认 utf8mb4/utf8mb4_0900_ai_ci，max_allowed_packet=67108864，sql_mode 为 `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`。需要其它参考版本时另取得其证据，不混用 golden。

参数 driver 源固定 [go-sql-driver/mysql v1.9.3](https://github.com/go-sql-driver/mysql/blob/v1.9.3/packets.go#L1103)，检查 []byte/string 的 type=254、flag=0 与 lenenc 分支；source digest 见 [参考字节记录](CLAUDE_issue-29481-oracle.json)。这是 driver 源代码证据，不声称已运行完整 driver matrix。参考执行使用 Python 标准库 socket 构造 Protocol 4.1 包，不经 mysql CLI/Unicode 重新编码；登录 collation=45，capability mask/每个 query_hex、列名 hex、charset、length、type、row/error 包均在 JSON。无 CREATE/DDL/全局赋值，只有独立连接中的 SET/SELECT/PREPARE/EXECUTE/RESET/CLOSE，连接在 finally 关闭；未启动或删除已有 MySQL 实例。每个 COM_QUERY 重置 seq=0；prepared EXECUTE 构造一参数的 null bitmap/new-types flag/type pair/value，long-data 构造两次 param-index=0，二进制行记录完整 payload hex（含 header/null bitmap）。这是参考探测快照，不是 MO acceptance test。

**r3 配对修复：** r2 在汇总生命周期记录时又通过嵌在 shell 字符串中的 Python 常量重构了两条 SQL，导致单引号/美元号消失、`5c6e` 变成换行 `0a`。原 JSON 配对不成立，不能作为这两个行为的证据。本次没有只编辑 query_hex 或期望响应：使用 [定向 probe](CLAUDE_issue-29481-oracle-probe.py) 在同版本/digest MySQL 的新连接上重新采集完整请求/响应及状态/reset，连跑两次输出一致；query_hex 只取真正传给 send 的 `payload[1:]`，response_payloads_hex 取对应 recv 的 payload。原 quote-corrupted JSON 会被 offline check 拒绝。未受影响的其它记录保留，新增记录与旧记录的来源由 recaptured_traces 区分。

```sh
# 不通过 shell 拼接 SQL；capture 只支持本地无密码参考账户，且不覆盖已有文件。
python3 docs/design/CLAUDE_issue-29481-oracle-probe.py --capture /tmp/CLAUDE_c02_new_capture.json
# 无需 MySQL：核对确切 SQL、引号/美元号/单反斜杠、对应输出、错误及后续成功请求。
python3 docs/design/CLAUDE_issue-29481-oracle-probe.py --check docs/design/CLAUDE_issue-29481-oracle.json
```

定向序列包含一次版本/配置查询，再执行五个 COM_QUERY 和一个 COM_RESET_CONNECTION，socket 操作超时 5s、连接 finally 关闭。部分结果请求为 `SET NAMES utf8mb4; SELECT JSON_EXTRACT(x,'$') FROM (SELECT '1' x UNION ALL SELECT 'bad') t`，其后**不插入其它命令**发送 `SELECT 1` 并记录成功列定义/行/EOF；连接复用不再靠叙述或另一条失败请求推断。上面的 inline `a\nb` 表示 SQL 中一个反斜杠后跟 n（hex `615c6e62`），不是两个反斜杠，也不是实际换行。

| 入口/输入 | 实测输出及设计 policy |
| --- | --- |
| utf8mb4 identity literal `ff`、`_binary'ff'`、`_utf8mb4'ff'` | SELECT 均成功返回 ff；两种文本列 ID=255，binary ID=63。identity 不额外施加 strict repertoire 校验，列写入校验仍独立 |
| `CONVERT(_binary 0xff USING utf8mb4)` | NULL；不能把这个入口实现成 identity 或笼统错误 |
| client=utf8mb4，connection=ascii，literal c3a9 | 转为 3f；HEX 为 3346。connection 跨编码替换不可表示字符，不是假装所有输入非法都报错 |
| SET NAMES ascii 后 identity literal c3a9 | 保留 c3a9，列 ID=11；是参考行为的事实，不声称 ascii 严格 repertoire 验收已经满足；严格列校验不由此放开 |
| results=ascii，畸形 utf8mb4 literal ff | 空字符串；prepared/text 两种输出都不发 ff，也不是 NULL |
| results=ascii，utf8mb4 literal c3a9f09f9880，别名 c3a9 | 行为 3f3f，别名 3f，ID=11、length=2；输出替换而非查询 ERR |
| results=NULL，literal/别名 c3a9 | 两者保持 c3a9，ID=255、length=4，连接可复用 |
| prepared `SELECT ?`，STRING(254) c3a9 / 复用类型 ff，results=ascii | binary row `0000013f` / `000000`；后者是空字符串，不是 NULL 或原 ff，不能以简单 Validate+error 取代该入口行为 |
| 同一 prepared 新类型 BLOB(252) ff / NULL bitmap | `000001ff` / `0004`；ff byte 不被 results 转换。该参考的参数列 metadata 仍 ID=11，不可用结果列 ID 反推输入域 |
| long-data c3、a9，最终 type STRING | 行 `0000013f`，与单次 c3a9 相同；不逐 chunk 解码 |
| NO_BACKSLASH_ESCAPES、encoding SET、后续 BAD SQL | 前序结果成功，BAD SQL 为 1064/42000；后续 SELECT 1 未执行，成功发布的 tuple/mode 保留；RESET 后回服务器默认 |
| results=ascii，查询不存在的 mysql.é | 1146/42S02，错误消息以 ASCII 转义 `\00E9` 表示名称，而不是发送未转换 c3a9；error number/SQLSTATE 不变 |
| SELECT JSON_EXTRACT(x,'$')，x 依次为 '1'、'bad' | 列定义和第一行 31 后 ERR 3141/22032，无成功终止，下一请求成功；采用已有结果流 owner 发送 ERR，不关闭仍完整的连接 |

codec 需针对 identity、connection、result、CONVERT 保持这些不同 policy；本参考 prepared/text 的 ff→空是同一个 result policy 的畸形源处理，不是基于客户端语言身份的差别。不得将结果替换、CONVERT 的 NULL 与结果畸形转为空收敛为同一个 Validate 失败。该表固定已测边界；更复杂畸形序列、实际列写入/握手/cursor metadata 仍需扩充最小 reference fixtures 后实现，不能把未测组合归结为“通用框架以后处理”。warning 数量/文本仍不在范围。严格 repertoire 适用的列/显式校验与连接兼容 policy 分开验证。

## 8. 决策与仍未取得的批准

r2 不撤销既有兼容决定：严格 utf8mb3 验收延期，但不阻断兼容首批。参数按 wire 可观测域分类，不恢复客户端语言身份；复用现有 staged owner；codec/发布/失败释放顺序已在第 3 节明确。错误/替换不再以“所有畸形字节都拒绝”猜测，参照第 7.1 节的入口分工。

仍需设计批准及有针对性的证据：

- reviewer 批准首批 API/state/metadata、错误与内存闭包；本作者不自签 PASS。
- 新的 ascii client/results 准入、proxy/session snapshot 实际 reader/writer 与 C11/C12 的可执行上线条件需在激活 PR 给出；首批只迁移现有可表达的兼容 tuple，新域若无法版本化则拒绝迁移/不开启。sysvar 已有 replayability/snapshot owner，不新增第二份状态 wire。
- reference JSON 为实际 MySQL 探测，不是 MO 通过证据，且不覆盖所有 prepare/cursor/列写入/取消排列。实际消费者 UT/raw-wire/BVT 仍按地图补齐；ascii repertoire acceptance 不能由 MySQL identity 保留畸形字节推导为已满足。
- 如后续要求严格 utf8mb3，必须由用户单独批准兼容/发布合同，不能通过本次 review 修复顺带改变。

评审范围：整个 C02 workstream；触发：client/server 协议、配置兼容、多 owner 边界及生命周期。r3 未批准，生产实现仍等待设计批准；可以继续评审本草案。#29481 保持未修复，验收 checkbox 不改变。

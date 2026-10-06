# C02：连接编码与协议边界设计草案

状态：r1，2026-10-07，**待评审，不批准生产实现或上线**。

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

范围为 binary、ascii、utf8mb3、utf8mb4 的显式连接/表达式转换，以及供 C10 接入 GBK 的有界接口；包含握手、COM_CHANGE_USER、SET、文字 introducer、CONVERT USING、文本/预处理输入与结果输出。编码能力与 collation 比较能力分别准入，不能因为协议转换可用就启用新的 column/index 域。

不改变产品默认值、既有对象身份、存储格式或比较/key 算法；不实现 GBK、latin1、其它未列字符集、分区表、存储过程、UDF 或 warning 数量/文本兼容。既有 UTF32 的 DDL 兼容拼写不授权协议/表达式转换。

不变量：

1. 会话对每个方向只有一个有效编码状态；协商值、sysvar 展示与实际消费者一致。
2. `client → connection → 内部文本 → results` 在明确的语义边界执行；binary 值始终按长度保留原字节，包括 NUL 和非法 UTF-8。
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

复用 `pkg/common/collation` 的静态字符集身份目录，不引入第二个动态 registry。codec 位置建议为该共同包或其无 frontend 依赖的子包；包/API 最终签名在设计批准时冻结。它接收源/目标编码、明确的文本/binary 来源、输出预算和调用上下文；返回借用输入或调用方拥有的转换结果及明确错误，不缓存 payload，不创建 goroutine。GBK 后续使用同一预算/错误/所有权契约，当前请求仍拒绝。

### 3.2 会话状态

`Session` 是 client/connection/results 和 connection collation 的有效状态所有者；protocol 对象只保存协议协商所必需的值，不成为 SET 后的第二份编码权威。优先从现有 sysvar 状态构造不可变、请求内快照，不新增跨请求转换缓存。若现有快照无法表达严格/兼容策略，则必须在批准的迁移契约中显式扩展，而非只改本地结构体。

握手先校验并构造候选状态，认证成功后与 session 初始化一致发布；认证响应、salt 和密码材料不可转码。COM_CHANGE_USER 复用现有认证/重置状态流程，不在认证失败后发布候选编码，保留当前既定的连接关闭策略。COM_RESET_CONNECTION 必须恢复经过批准的初始状态，不能残留结果编码或转换内存。

SET NAMES 预先解析字符集、显式/默认 collation 和所有受影响 sysvar；SET CHARACTER SET 的 connection 继承数据库默认规则由固定 MySQL oracle 决定。单个直接赋值触发的 charset/collation 联动也由同一候选状态计算。完成现有类型/scope/权限/runtime hook 预检后一次发布；中途失败保留原状态。这里不承诺任意混合 SET 语句的所有非编码副作用整体回滚。

全局默认值继续由现有账户级持久化和缓存所有者负责；新请求拒绝与历史加载保持分开。SET GLOBAL 的既有权限、scope、成功发布点不能被 codec 绕过。

### 3.3 SQL 输入与表达式

不得对整个 SQL packet 无差别解码，否则 `_binary`、hex/bit literal 及 introducer 会丢失字节来源。scanner 应先区分语法、文字 token 和 introducer，再按各自编码解释字节；SQL escape 处理、client 解码和 connection 转换顺序由 oracle 固定。认证、长度前缀及数值编码不经过字符转换。标识符、数据库名与 COM_INIT_DB 也纳入文本路径检查。

multi-statement COM_QUERY 的词法 client 编码和每条语句执行时的 connection 状态必须分别建模；SET 在请求中的生效时点是设计阻断问题，不能靠 parse-all 后统一替换猜测。SQL PREPARE 文本与 COM_STMT_PREPARE 复用相应公开输入路径，不能只支持 COM_QUERY。

introducer 选择文字源编码；CONVERT USING 使用表达式实际源域和目标编码；binary 来源不先假定 UTF-8。两者不改变 C01 的 collation 原生准入、不自动扩大 C04/C06 比较/函数范围。

### 3.4 预处理参数与结果

在执行绑定边界明确区分文本、binary、数值与 NULL；不能仅凭 MYSQL_TYPE_VARCHAR 将 binary 参数当文本，也不能仅凭 BLOB 编号猜测全部应用语义。使用现有 prepare provenance/domain 与固定驱动 oracle 决定可表达的分类；没有可靠来源时的策略须先定案。

long-data 累积原字节，物化完整值后再转换，使跨 chunk 字符合法。执行时使用该次有效类型绑定和编码快照；新类型向量和新参数值在完整解析/转换成功后才能替换上一成功状态。错误后清理该执行拥有的临时值；保留/清除未执行 long-data 的规则遵守既有协议和 oracle，不新增隐式重试。

所有结果生产入口共用序列化边界：普通文本行、binary row、ColumnSlices 快速路径、cursor/fetch、列定义及 prepare metadata。文本值/字段名称按有效 results 策略输出；binary 值和数值/null bitmap 不转码。results=NULL 不转换，但元数据必须按 oracle 保留来源，不能把 NULL 当作声明 binary。字节长度与最大字符宽度须对应输出编码。准备时 metadata 与执行时 results 变化的关系必须通过客户端验证。

输出前完成字段转换再写其长度，避免已写长度与字节不匹配；一旦已有结果包发出，不能假装回滚整条查询。转换失败后的 ERR/关闭策略及消息编码在第 8 节定案，保持报文流可解析。

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
| repertoire 与 codec | ASCII 0x7f/0x80；U+FFFF/U+10000；畸形、空、NUL、NULL、binary 0xff；原子错误和分配预算 | 原始字节、error number/SQLSTATE、结果 metadata 与固定 MySQL 对比 |
| 会话发布 | SET 候选状态成功/失败，显式 COLLATE、DEFAULT、results=NULL、scope | 握手/change-user/SET 后逐项状态和行为；latin1 拒绝后原状态不变 |
| 文本输入/表达式 | scanner/token 来源、escape/introducer/CONVERT 与近邻控制 | COM_QUERY/INIT_DB/PREPARE、SQL PREPARE、多语句 SET、literal 字节 |
| prepared 参数 | 同编号不同来源、NULL、类型复用/重绑、拆分 chunk、转换/分配失败 | 真驱动字符串/字节绑定及 raw-wire 对照，不只调用 helper |
| 结果 | text/binary/ColumnSlices/cursor 的共同 byte oracle | prepare/execute/fetch metadata、results 改变、二进制不变 |
| 生命周期 | error/cancel/reset/close/断开和两次重绑，内存 owner 归零 | 已有客户端/服务 fixture 上的有界失败路径，适用时 focused race |
| 升级/迁移 | 状态 reader/writer 及拒绝旧版本 | 只有实际改动迁移契约才运行对应 proxy/multi-CN/恢复场景 |

首选 0–2 行和最小边界字节，不用 sleep、重试、随机 chunk 或新重型集群替代明确输入。UT 先 focused 再 owning package；Go 版本跟随 go.mod，CGo 使用 mo-cgo-test；增量 gofmt/vet/lint。新增可执行逻辑覆盖率至少 75%，不能以覆盖率代替 wire/生命周期证明。

SQL result 由 mo-tester 生成、人工审查再 comparison，清理后同实例重复；协议 oracle 保存真正的输入/输出字节与 metadata。基线 oracle 未固定前，不写由当前实现自产的“期望结果”。

建议分批：①固定兼容和 oracle、批准设计；②无新准入 codec/状态基础及 UT；③token/表达式/prepared 输入；④所有结果生产者；⑤公开协议、失败与性能证据；⑥通过适用发布门槛后激活。实现 PR 链接批准设计的具体 commit；任何契约偏离先更新并重新评审。

## 7. 规范参考

- [MySQL 8.0 连接字符集](https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html)
- [MySQL 8.0 introducer](https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html)
- [MySQL 8.0 CONVERT](https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html)
- [MySQL 协议](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html)
- [TiDB 范围参考](https://docs.pingcap.com/tidb/stable/character-set-and-collation/)，只限定范围，不复制已知 repertoire/PAD SPACE 缺陷。

C01 冻结 backend 的已有 MySQL fixtures 不是连接转换 oracle；版本或语义测试不同不能自动复用。

## 8. 阻断决策与评审记录

| 阻断事项 | 所有者与批准时点 |
| --- | --- |
| 严格 utf8mb3 是否允许撤销已确认的静默 utf8mb4 映射；若只在新的显式模式启用，其准入、展示、协议 ID、迁移与 release 门槛如何表达 | 用户/兼容策略 owner；生产编码行为实施前 |
| 固定 MySQL 参考二进制精确版本/digest、sql_mode、默认字符集、驱动版本；逐入口确定畸形输入/不可表示输出是错误还是替换，error number/SQLSTATE、状态与部分输出策略 | C02 与 C12；codec 行为实施前 |
| MYSQL_TYPE_VARCHAR/BLOB 无法独立证明参数来源时，真实驱动和现有 provenance 可以表达哪些语义，以及不可区分时的可互操作规则 | C02；prepared 输入实施前 |
| multi-statement SET 的 client/connection 生效时点、文字 escape 次序、SET CHARACTER SET 默认继承及直接变量联动 | C02/C12；lexer/状态设计批准前 |
| codec 具体签名/分配 owner/取消粒度/输出扩张上界、session reset 与 proxy snapshot 的具体 reader/writer 闭包，以及可执行激活条件 | C02/C11；基础接口设计批准前 |

这些是实质设计阻断，本文不替它们假造决策。后续批准版本必须补齐 oracle、具体 API、状态 reader/writer 和门槛，再开始生产实现。用户要求继续创建 PR 只授权交付 draft，不意味着这些互相冲突的行为已获得批准。

评审范围：整个 C02 workstream；触发：client/server 协议、配置兼容、多 owner 边界及生命周期。设计 r1 未批准；实现状态 BLOCKED。文档可作为 design-first draft 提交；#29481 保持未修复，验收 checkbox 不改变。

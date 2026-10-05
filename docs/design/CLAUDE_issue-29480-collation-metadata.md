# C01：排序规则身份与版本元数据

状态：r2，2026-09-29，用户已明确确认 utf8/utf8mb3 保留静默兼容映射，撤销 r1 的拒绝建议。所属任务：[#29480](https://github.com/matrixorigin/matrixone/issues/29480)，属于 [C01–C12](https://github.com/matrixorigin/matrixone/issues/29479)。工作方案已获准继续；本版本记录已确认的兼容性决策，先于生产实现编写。本设计不批准原生新域上线。

## 1. 问题与边界

基线 `d8152cb08d7e73316034c33aaf0e7bc01c176f91` 将 ascii、latin1、utf8/utf8mb3/utf8mb4 折叠成一个内部值；0900 名称是 general-ci 历史别名。`Type.Charset` 实际保存有效排序规则身份，不是 MySQL 字符集或协议编号。另有 sysview 表和独立名称 switch，使准入、协议与展示容易漂移。

本任务交付身份和版本的可表达性、传播和拒绝边界，不交付新的比较、转码、索引或迁移算法。复用 #29054 的冻结后端；从尚未合并的 #29055 复用元数据字段分工，不合入整支实现栈。禁用的新域不得产生生产计划或新持久对象。

### 不变量

1. 字符集、SQL 名称、MySQL ID、有效语义、修订、物理格式和表达式 coercibility 不是可互换的编号。
2. 已有持久身份 0–3、零值及 legacy key 字节不变。不存在从旧名称推导 native 语义的升级。
3. 每一个已知字段通过 Type、plan、pipeline、catalog、clone/cache 时保真；未知值或非法组合不得截断后成为 legacy。
4. 一个固定能力所有者同时服务准入和元数据。知道名称或拥有权重后端不代表 SQL 已获准执行。
5. 所有新域和非零语义修订/物理格式在本 PR 的生产边界保持关闭；没有绕过此约束的测试全局开关。

否定例：protocol ID 309 被 uint8 截断；版本在 plan→Type 中丢失；0900 native 在重启后按 general-ci 读取；SHOW 声称 ascii/GBK 可执行但实际按 UTF-8 处理。

## 2. 所有者与表示

### 2.1 单一静态能力目录

所有者为现有 `pkg/common/collation`。定义使用独立的字符集、语义和协议类型；sysview 仅投影该目录，不保存另一份权威映射。目录按值返回，外部不能修改全局准入状态。

原生字符集定义：binary、ascii、utf8（utf8mb3 别名）、utf8mb4、gbk。原生定义的最大单字符编码字节数分别为 1、1、3、4、2。大小写不敏感；在原生定义查找中仅 utf8mb3→utf8 是同一家族归一化。这个原生定义空间与当前 SQL 兼容解析分开：后者继续把 utf8/utf8mb3 静默解析为 utf8mb4，而不激活三字节限制。不能因为目录能够表达严格 utf8mb3，就改变已有 SQL 的含义。

| 内部紧凑身份 | canonical 名称 | MySQL ID | 家族 | 新语义 padding |
| --- | --- | --- | --- | --- |
| 0 | 未指定的历史对象 | 不据此猜测原声明 | 未指定 | 历史字节比较 |
| 1 | binary | 63 | binary | NO PAD |
| 2 | utf8mb4_bin | 46 | utf8mb4 | PAD SPACE |
| 3 | utf8mb4_general_ci | 45 | utf8mb4 | PAD SPACE |
| 4 | utf8mb4_0900_ai_ci | 255 | utf8mb4 | NO PAD |
| 5 | utf8mb4_0900_bin | 309 | utf8mb4 | NO PAD |
| 6 | ascii_bin | 65 | ascii | PAD SPACE |
| 7 | utf8_bin | 83 | utf8 | PAD SPACE |
| 8 | utf8_general_ci | 33 | utf8 | PAD SPACE |
| 9 | utf8_unicode_ci | 192 | utf8 | PAD SPACE |
| 10 | utf8mb4_unicode_ci | 224 | utf8mb4 | PAD SPACE |
| 11 | gbk_bin | 87 | gbk | PAD SPACE |
| 12 | gbk_chinese_ci | 28 | gbk | PAD SPACE |

0–3 来自 main，4–5 沿用 #29055 分配，其余为新增、尚未准入的身份。内部身份是对有限合法描述的紧凑引用，不是动态注册器，不是 protocol ID，不会写入每条物理 key。当前十二项 canonical collation 和历史对象一共十三种身份，uint8 容量上限 256；255 已被现有数值 CAST 私有标记使用，永不分配给 collation。

有效比较语义由身份与语义修订共同确定：修订 0 保留旧 0–3 的行为；修订 1 描述冻结/待集成的命名语义。general-ci、UCA400、UCA900、字节 PAD、字节 NO PAD、GBK Chinese 独立。字符编码不从这些语义类别反推。

### 2.2 历史别名和新请求

2026-10-05 合并上游 #29590：`utf32`、`utf32_general_ci`、`utf32_bin` 仅作为 DDL 兼容拼写，由现有能力 registry 的 DDL 专用入口归一化到既有 utf8mb4 general-ci/bin 身份。它们不增加原生身份、编码转换、协议编号或目录广告；SHOW CREATE 继续输出支持的 utf8mb4 身份。普通 session/SET 准入仍拒绝 UTF32 请求，避免把上游的 DDL 兼容修复扩大成原生能力启用。

`utf8mb4_0900_ai_ci` 的现有默认拼写在 legacy 准入策略下仍解析成身份 3、修订 0，effective metadata 为 general-ci/PAD SPACE；native 查找结果则是身份 4、修订 1、NO PAD，两者由明确的 API 和修订区分。旧对象没有保存原始拼写，不能凭空恢复。

latin1/latin1_* 一律拒绝新增 DDL、会话/全局设置、握手/change-user 和 CONVERT 请求。SET NAMES/SET CHARACTER SET 在赋值前检查字符集及显式 collation；本任务不顺带重构其既有赋值/转码行为。保留 collation_connection 的既有 `default` 占位值。已验证的历史 session snapshot 使用独立恢复入口保留原设置，仍执行既有变量校验及 runtime hooks，不重新按新请求解析名称；这不是 C11 的新格式迁移或激活机制。不会猜测历史 latin1 字节，也不将它们重编码。

**utf8/utf8mb3 保留静默兼容映射，这是用户明确确认的行为契约：** 现有以及新增的同名 SQL 声明继续采用 MO 原有 utf8mb4 有效语义；`utf8_bin`/`utf8mb3_bin` 保留到 utf8mb4_bin 的映射，`utf8_general_ci`/`utf8mb3_general_ci` 保留到 utf8mb4_general_ci 的映射。不得新增三字节限制，不得拒绝原先接受的这些名称，也不通过告警改变现有调用行为。未支持的 unicode-ci 等规则不会因这个兼容决定变成已支持。

`information_schema.CHARACTER_SETS` 的 utf8 行 `MAXLEN` 按有效兼容编码显示 4，而非原生严格三字节定义的 3；这与四字节数据的实际准入一致，不修改默认排序规则。原生定义查询仍返回严格 utf8 的容量 3。

准入返回的是兼容解析后的有效身份，而不是同名原生定义的身份。SHOW、information_schema 和协议消费者必须使用同一能力定义，区分兼容拼写、协议编号与有效语义；不得把接受 utf8 名称作为已实现严格 utf8mb3 的证据。原有 0–3 持久值和默认行为不变。

ascii、GBK、unicode-ci、native 0900 和严格三字节 utf8mb3 的原生身份尚未通过相应 workstream 的全链路验证，仅在内部元数据层可表示，生产保持禁用。严格 utf8mb3 的未来启用需要独立兼容性/发布决定，不能在本任务中悄悄替换已确认的映射。

## 3. 存储与传输布局

### 3.1 Runtime Type

沿用 #29055 的小型版本槽：原 `dummy2` 字节变为 `CollationVersion uint8`。`Oid`、`Charset`、`notNull`、版本仍各占一字节；`Size` 仍在 offset 4，Width/Scale 不移动，原生 `Type` 保持 16 字节，无指针、无逐对象堆分配。

手工 20 字节 codec：oid 位于 [0:2]；[2:4] 是 `(version << 8) | identity`；notNull 位于 [4:6]；[6:8] 仍写 0；Size/Width/Scale 各四字节。旧值 version=0 的字节完全不变。解码先读到临时值、验证，再发布接收者，失败不保留半初始化类型。原生固定编码同样需要消费者拒绝未知版本，不能只保护这个手工 codec。

该表示并不能保护旧读者：旧读者会忽略版本高字节。安全性来自本 PR 不发布新语义，及 C11 未来的升级所有者，而不是 padding 字节本身。

原生解码新增带 error 的 checked 入口，在检查长度和元数据后才返回值。当前未校验的外部字节消费者位于 `pkg/objectio/legacy_column.go`、`pkg/vm/engine/readutil/pk_filter_base.go`、`pkg/vm/engine/tae/containers/batch.go` 的三个格式读者、`pkg/vm/engine/tae/catalog/schema.go`、`pkg/container/vector/versions.go` 的版本探测和解码、`pkg/container/vector/vector.go`，以及 `types.ReadType`。这些入口必须传播正常错误，不能通过 panic 或 silent fallback 处理不可信元数据。原有无 error helper 仅留给已验证的内部数据；测试必须覆盖实际读者而非只测试新增 helper。

### 3.2 Protobuf

保留现有编号，复用 #29055 的字段安排：

- `plan.Type`: charset=8 不变；coercibility=10、presence=11、merge_conflict=12；collation_version=13。
- `plan.IndexDef`: key_format=15；既有 reserved 14 不动。
- `plan.TableDef`: key_format=41、collation_version=42，default_charset=39 不变。
- `api.SchemaExtra`: default_charset=19、auto_id_cache=20 不变；key_format=21、collation_version=22。main 已占用 #29055 当时拟用的 20，必须顺延而不能重用现有 wire identity；本分配取代旧草案的 20/21。

coercibility 合法范围 0–6；显式 COLLATE 的 0 与“未提供”由 presence 区分。它是表达式 provenance，不伪装成 runtime Type 的编码属性。Type↔runtime 的契约只传输运行时相关字段；plan↔plan 必须保留全部 provenance。

protobuf 中的 uint32 身份/修订必须先验证范围，再降到 uint8。物理格式目前识别 legacy=0 和冻结的 collation tuple V1=1，但只允许 legacy 生产准入。未知版本/格式显式报错。

正常使用仓库 protoc/gogo 生成流程；若需生成后校验注入，修改已有 postprocess 工具并为幂等性/选择边界添加测试，绝不直接编辑生成绑定。

### 3.3 传播矩阵

| 生产者 | 传输/副本 | 消费者 |
| --- | --- | --- |
| 名称解析和绑定 | plan.Type、DeepCopyType、expr/plan cache | 本地编译、向量类型、协议列信息 |
| runtime Type | vector proto、pipeline 类型列表 | remote 接收端和算子初始化 |
| TableDef 默认与格式 | engine SchemaExtra、disttae catalog/cache、TAE schema extra | clone、重启目录读取、表定义恢复 |
| IndexDef 格式 | constraint protobuf 和 deep copy | schema 与 index 消费者 |
| 表达式 provenance | plan protobuf、deepcopy、hash/equality | prepared/cache 和后续 C04 绑定策略 |

完整 change map 以实际 diff 更新。所有手工 Type 转换点与手工相等比较均需审查，不能只更新 constructor 然后宣称全部保真。

## 4. 准入、失败与升级

结构验证允许已知但禁用的元数据正常往返，以便后续组件逐步接入。生产准入是独立的拒绝边界：新 DDL/绑定、计划编译（包含缓存计划）、remote sender/receiver、目录创建。未知值总是拒绝，已知新域也不因本地无 service ID 而放行。

单表达式、批量表达式及 join/常量列表工厂均在资源分配前完成全输入预检；递归构造器不重复做整树扫描。LiteralVec、PK IN 和远端 batch 的不透明字节不能由外层 legacy Type 代为准入：实际消费者在解码后、进入执行/比较前检查内部向量类型，拒绝时释放尚未移交的向量或整个 batch。通用 codec 仍保留结构合法的元数据。Substrait 不能表达新版本/格式，其 Export、Build 和 CanonicalSchema 边界拒绝这些元数据，而不是删字段导出。

失败发生在会话/目录状态发布、算子资源分配或数据写入之前。请求取消、重试不增加持久状态；本任务没有新 worker、锁、队列、事务协调器或重试循环。对接既有边界时保留它们原有清理责任。

C01 负责表达、传播和本地拒绝。C11/#29490 负责可以强制执行的集群能力、旧 reader/writer 隔离、迁移、恢复和回滚；C12/#29491 负责 release acceptance，当前三项 assignee 均为 ck89119。只有完成适用的 SQL/index/storage/upgrade/release gate 才能修改准入策略。没有可靠混合版本保护时要求完整同构升级，而不是依赖旧节点忽略的 table flag。

本 PR 不修改旧对象语义或已写 key，不支持通过关开关回滚新格式，不实现迁移、collision 合并或 in-place 转码。历史目录加载与新请求策略分离。已有旧身份仍恢复原行为。

## 5. 容量与性能

- 固定十三个内部身份、十二个 canonical collation；名称 lookup 为 O(13)，仅绑定/元数据阶段使用。
- 热路径继续携带两个 uint8，Type 原生大小及复制成本不变，不添加每行解析或 per-key metadata。
- plan protobuf 新增四个标量字段；零值省略，coercibility/版本各值受界限约束。在当前 64-bit Go/gogo 布局下，plan.Type 从 88 增至 104 字节，包含它的 Expr 从 168 增至 184 字节；新增预算为每个计划类型/表达式 16 字节，并非每行或每条 key。既有 prepared-numeric 仍使用可选指针，不增加其 resident 标量。schema/index 仅新增标量，不含随行数增长的结构。
- 不引入新全局可变 cache、goroutine、I/O、日志标签或指标维度；目录和缓存的所有者不变。
- legacy/binary round trip 增量堆分配预算为零；以 allocation 测试验证，不以文档声称代替。

## 6. 选择与规范

保留现状不能满足身份及 unknown-version 契约；扩大 Type 结构体会改变原生 object/vector/catalog 固定布局并迫使本任务引入另一套存储升级协议；动态 sidecar/registry 增加持久所有权且被总任务明确排除。因此采用有限不可变描述 + 既有固定宽度身份/版本 + schema 物理格式。

MySQL 协议 ID 按官方命名 collation 编号处理，不等于 MO 内部值。参考 [MySQL 8.0 coercibility](https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html)、[TiDB 范围](https://docs.pingcap.com/tidb/stable/character-set-and-collation/) 及仓库 `issue-28164-collation-key-reuse.md`、`issue-28164-weight-key-v1.md`。TiDB 仅限定范围，不作为已知 repertoire/PAD SPACE 缺陷的实现依据。

## 7. 验证与完成门槛

1. 目录独立 oracle：十二名称/协议 ID/家族/修订/padding；alias 与 native 区分；latin1 和未知名称拒绝。
2. 16 字节布局及 20 字节历史 golden，known-domain/version 往返；未知/越界/短输入失败且不改变接收者。
3. plan/runtime/vector/pipeline/catalog/cache/clone 对所有相关字段的表驱动往返；coercibility presence、unknown fields 不丢失；缓存不同语义不等价。
4. 真实公开入口验证拒绝，不只调用 lookup；会话失败不留变更；协议 309 不能截断成 53。
5. SHOW/information_schema/列协议与同一 enabled/legacy 策略一致；mo-tester 生成结果后检查并实际 comparison 验证。
6. Go 1.26.4 定向 UT、owning-package、增量静态检查；CGo 使用仓库 wrapper。改变的非生成逻辑覆盖率至少 75%。
7. 元数据实际持久化改动必须补恢复/重启消费者证据；不宣称 native SQL/index 或混合版本上线通过。
8. 最终执行 mo-self-review；发现未覆盖生产入口、传播丢字段或旧读者风险则阻断交付，不降低断言掩盖问题。

## 8. 设计审查与决策记录

复杂性触发为 wire/persistence/upgrade 与多子系统边界。保留旧布局、不增加生产启用开关、复用单一能力所有者为工作方案已选择方向。

2026-09-29 用户明确纠正：MO 原有 utf8/utf8mb3 实际就是 utf8mb4，所以应继续静默映射。该决定适用于现有兼容入口，包括新增同名声明，而不只是历史目录加载。

r1 把“能独立表示严格 utf8mb3 身份”误解成“必须立即改变现有 utf8 声明语义”，进而提出全面拒绝和不必要的阻断，现撤销这两项结论。C01 通过区分原生定义与有效兼容身份满足可表达性；严格 utf8mb3 原生能力不因此启用。保留这个明确的兼容政策不是字段传播缺陷，也不自动意味着只能做部分交付。

兼容性阻断已解除。实际完成声明仍取决于 C01 的类型/计划/目录往返、未知版本拒绝、统一能力、latin1 拒绝及所需测试证据；不得把本决策当作这些验证已通过，也不得把接受兼容名称当作原生三字节语义通过。

## 9. 2026-09-30 准入与展示升级补充

用户 review 发现的三条遗漏均属于 C01，不能由既有测试通过或 C11/C12 边界豁免：

- **ALTER 表级请求**：在共享 ALTER 路由、COPY/INPLACE 选择之前复用统一字符集/排序规则解析。parser 保留组合选项的 COLLATE，并区分 FORCE/KEYS/TABLESPACE/VALIDATION 的旧占位语法。支持的表级选项仍是兼容 no-op，本轮不实现转换；latin1/禁用域/家族不匹配不能因此成功。
- **旧租户展示升级**：注册 `4.0.11` tenant handler，最低前序为 `4.0.10`，沿用其协议 floor 100。新语义版本使已完成旧版本的租户也能刷新，并避免旧 worker 把同版本 offset-only 任务当完成。目录刷新由既有升级事务和 account-scoped executor 拥有；精确完成谓词与初始化复用同一有效容量/default 来源，旧 `MAXLEN=3` 不再完成。只重建三行 CHARACTER_SETS 展示元数据，不重写用户类型/key，不启用新物理格式。
- **聚合 opaque 状态**：普通聚合状态、兼容结果和参数向量在实际解码后校验内层 type，早于重排、merge 和发布；通用 vector codec 继续结构保真。compact spill 不携带 type tag，继承执行器声明，其参数在共享聚合工厂准入。reader 保持既有错误清理 owner；真实 MergeGroup 的 ExtraBuf 反例包含先成功的聚合、后拒绝的聚合及 legacy 重试，普通 bat.Vecs 校验不替代它。

验证补充：Go 1.26.4 全部 37 个受影响包完整测试通过，整个 C01 修改可执行行覆盖率 `800/850=94.12%`，本轮修复 `81/83=97.59%`；mo-tester 修改用例 `207/207`、邻域 `709/709`。干净旧 main `76862df5c8` 生成真实 `4.0.10/MAXLEN=3` 系统及普通租户，当前二进制原数据升级后均为 `4.0.11/MAXLEN=4`，SHOW 与目录一致，四字节用户数据保持不变，再次重启结果不变。此证据是展示元数据升级，不声称原生格式混合版本激活或回滚验收通过。

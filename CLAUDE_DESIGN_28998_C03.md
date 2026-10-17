# #28998 / C03：数据库、表、列 charset/collation 继承与显式转换

修订：r1，2026-10-10。状态：**用户已用 `go ahead` 批准实施**。批准草案 SHA-256：`075264245dfa4bd5a921cf2b7d3ba8d28dfe2a190b9639e2d6d5f7232e06cfea`。

任务：[#28998](https://github.com/matrixorigin/matrixone/issues/28998)，承接 [C03/#29482](https://github.com/matrixorigin/matrixone/issues/29482) 的全部范围；上位任务 [#29479](https://github.com/matrixorigin/matrixone/issues/29479)。接续既有 PR [#29374](https://github.com/matrixorigin/matrixone/pull/29374)，批准时为draft，当前ready状态保持不变，不另做重复 no-op 修复。

当前维护编号（2026-10-17，冻结main `a322466757b9b0165c59fbda565608653d923b2a`）：保留main协议112/113，以及native Unicode UNIQUE **114**、calendar interval **115**、scalar division warning **116**、collation-aware LEAST/GREATEST **117**和`TO_CALENDAR_INTERVAL=598`。未发布C03保持协议 **118**、函数顺延`INTERNAL_TABLE_COLLATION=600`、`FUNCTION_END_NUMBER=601`；保留main文件访问鉴权函数`CHECK_ASYNC_DATALINK_SOURCE=599`；独立迁移 **4.0.15**、`CreateTable.database_defaults#18`不变。下文r1批准时事实及各历史补充保留各自时间范围，不代表当前分配或main最新能力；历史112下SHOW binary展示已由SHOW NULL/information_schema binary合同纠正。

开发基线：`e5dd4724f782067e1c76c38c9d2f55cebceda4a9`，已经在独立 worktree pull `mo/main`。旧 PR 的设计批准仅覆盖其原有范围，不自动批准此次 C03 扩展。本文批准后记录内容 SHA-256，并在交付 revision 中保留可追溯设计。

## 2026-10-15 后续评审与语法合并协调

- 新评审5527576615的两个inline针对旧f5a7：生成表达式会话精度及main113不具备C03。二者已由170a5d1aa83d修复，当前typed replay、114准入及113负控保持；不是未解决的新生产缺陷。
- 后续正常合并main501c85ecdd，包括POINT别名、表达式CHAR CHARACTER SET及CREATE INDEX后缀USING、UT调度。保留上游语法/绑定/错误/结果，POINT复用ST_POINT既有ID，不占用598。表达式字符域CAST继续由CONVERT USING及显式宽度owner执行，不误用DDL二进制填充规则；C03作者CAST/目的赋值边界不变。
- 唯一文本冲突在生成parser；从合并grammar执行owner强制双次生成一致，不手改生成源码。该维护不增加C03功能范围或新状态/锁/持久化格式；验证按受影响parser/binder及真实SQL消费者重采集，未变历史资格不无条件扩张。

## 1. 问题、事实与不变量

原始见证是 Gitea 启动执行：

```sql
ALTER DATABASE `gitea_mo` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
```

成功必须使后续对象实际继承该库的默认值，而不是仅通过 parser 或返回 OK。C03 还要求表/列声明、CREATE LIKE、CTAS、ADD/MODIFY COLUMN、prepared DDL、元数据展示与恢复一致。

### 已核实事实

- `buildCreateDatabase` 仅校验 charset/collation 名称；当前 `plan.CreateDatabase` 没有默认值字段。
- `build_util.go:tableDefaultCharset` 在无显式表选项时读取 `collation_server`。
- `build_alter_table.go:validateAlterTableCharsetOptions` 明确将表级 charset 请求视为 no-op。
- `mysql_sql.y` 的 CONVERT 与表默认值修改生成同类 `TableOptionCharset`，丢失转换意图。
- 当前 main 已合入 C01、C02 codec 基础及 #29601 的 fenced UCA400 子集。旧草案/旧 PR 的名称白名单和完成声明不能覆盖这些新事实。
- #29601 仍拒绝 native Unicode PRIMARY/UNIQUE，native 0900、ascii 列域、GBK 与新 key format 不因本任务获得准入。
- 当前 protocol 最新编号 108，最新 tenant handler 为 4.0.13；旧 PR 的 95/96、4.0.9 不能直接复用。

### 不变量

1. 对普通新对象：显式列声明优先于显式表声明，之后才是**目标数据库**默认值；旧库无默认记录才走已定义的 legacy 回退。
2. CREATE 时解析并保存完整有效类型。ALTER DATABASE 只影响以后对象；ALTER TABLE 默认值只影响以后的未显式声明列，均不重解释既有列或索引。
3. `CONVERT TO CHARACTER SET` 是显式数据/列/索引转换，与默认值修改不能使用同一 no-op。转换失败时原表、数据与索引仍可用。
4. charset 家族、有效 collation 身份、语义修订、物理 key format 和默认值变更代际互不混用。已持久化对象不根据名字重新猜测语义。
5. SHOW、information_schema、CHARSET/COLLATION、runtime 比较与实际索引访问观察同一有效域；不能只改展示。
6. 权限、对象存在性、账户、代际及能力在执行时仍成立；prepared/cache/background 不能绕过。
7. 未支持组合、latin1、未知版本在对象发布或数据写入前拒绝；错误、取消和回滚不得留下半对象。

最小否定例：修改 B 库后在 B 新建未声明列，得到 A 库/session/server 的 collation；或默认值 ALTER 改变旧列比较结果；或 CONVERT 失败后原表消失。

## 2. 范围与兼容决策

### 本次负责

- CREATE/ALTER/DROP DATABASE 及 DATABASE/SCHEMA 同义语法、完整选项校验与持久默认值。
- CREATE TABLE 的数据库→表→列继承与显式覆盖；qualified target、跨会话、后台 SQL、prepared DDL。
- CREATE LIKE 保留源表默认值/列域；CTAS 按现有 SELECT 来源类型契约保留输出域，显式声明按 MySQL 契约覆盖；不把所有 SELECT 类型强制改成目标库默认值。
- ADD/MODIFY/CHANGE COLUMN 使用目标表当前默认值与显式声明；临时表使用既有 session 所有者。
- ALTER TABLE 默认值持久更新；CONVERT 的显式列/数据/索引重建、错误保持和约束预检。
- 展示、数据库变量、恢复、clone、DDL replay 与升级。

### 能力边界

复用 `pkg/common/collation` 的唯一静态能力所有者及当前 DDL 解析政策，不建立第二份名字白名单。数据库默认值和继承只产生当前已准入的有效域；known-but-disabled 仍拒绝。

- 保留 utf8/utf8mb3 的已批准 legacy→utf8mb4 静默兼容映射，不顺带引入严格三字节限制。
- native utf8_unicode_ci / utf8mb4_unicode_ci 是 revision 1 的独立 UCA400 域，遵循 #29601 已批准的 repertoire/非唯一索引边界，不能退化为 general-ci。
- utf8mb4_0900_ai_ci 的既有 legacy 别名仍是 general-ci；不宣称 native 0900 上线。
- native Unicode PRIMARY/UNIQUE 及其转换仍拒绝；不绕过 C07 锁/去重闭环。
- 保留当前已有的 UTF32 **DDL-only 兼容拼写**，不新增原生 UTF32、转码或协议能力。
- 新 latin1/latin1_* 始终拒绝。GBK、ascii 列域、native 0900、新物理 key format 由相应 workstream 与 C11/C12 gate 负责。

产品默认值、历史对象行为不变。分区、存储过程、UDF、warning 文本/数量、未列出的原生字符集与额外全文语言学语义仍排除。

**完成口径：** C03 的继承/DDL机制与当前准入域都要验收；总任务中未来域的上线由它们的 workstream 承担。任何 C03 必需验收尚受依赖阻塞时明确列出，不以该划分暗中删掉验收或直接关闭 #28998。

## 3. 方案与替代项

| 方案 | 判断 |
| --- | --- |
| 维持/扩大 ALTER no-op | 成功不对应状态变化，不能通过 C03，淘汰 |
| 只改 session/global 变量 | 跨会话/目标库/恢复所有权错误，淘汰 |
| 解析或修改数据库 CREATE SQL 作为权威默认值 | SQL 字符串不适合作为可变类型状态；catalog/cache/恢复协议成本更高，不选 |
| 在核心 mo_database 增加字段 | 可行但扩大核心存储与旧 reader 协议；本次不优先 |
| 复用 #29374 的每库一行 catalog 与既有 DDL/COPY/replan 所有者 | 选用；补足 C01 身份修订、表默认值及转换闭环，不新增 cache/worker/registry |

旧 PR 以 merge 整合到最新 main，保留历史；代码按本文修订，parser/protobuf 从当前源重新生成。旧证据仅在相关输入未变且能证明相同合同的条件下复用，不能直接复用旧 PR “全部通过”。

## 4. 元数据与所有者

### 数据库默认值

沿用旧 PR 的 `mo_catalog.mo_database_defaults` 概念，每账户每数据库 ID 最多一行。建议持久结构：

```text
account_id          uint32
database_id         uint64
collation_id        uint32
collation_revision  uint32
version             uint64
PRIMARY KEY (account_id, database_id)
```

charset/canonical 名称从 C01 有效定义推导，不保存另一份可能与 ID 冲突的权威 pair；请求 alias 不变成新语义。`version` 是默认值修改代际，与 collation_revision 不同。

旧 PR catalog 尚未合入 main，因此不假设其字符串 schema 已正式部署。如发现真实部署过该实验格式，必须补独立迁移设计，不能直接读取或覆盖为新布局。

数据库 DDL 是唯一写入者；普通用户不能直接改受保护 catalog。账户及数据库 ID 是所有读写的过滤条件。DROP/CREATE 同名库不能继承旧代际。DROP 同事务删除默认值；DROP ACCOUNT 由原租户 catalog 所有者清理。

表继续使用现有 `TableDef.DefaultCharset`、`CollationVersion`、`KeyFormat` / `SchemaExtra`；列继续使用现有 Type 元数据，不再添加逐列 sidecar。数据库默认值不替用户表决定新的物理 key format。

### 解析与准入

共享解析返回一个有效身份/修订及 canonical metadata，先校验 family pair、重复/冲突选项和能力，再构造计划。单 charset 使用该有效字符集默认；单 COLLATE 推导有效 charset。全无选项时新库记录当时有效 server 默认值。

CREATE/ALTER DATABASE、CREATE TABLE、列声明和 ALTER 路由共用解析合同；保留 native UCA 与 legacy alias 的区别。错误读 catalog、缺表、损坏值不是“旧库缺记录”；只有确证无记录或历史快照早于表的情形允许 legacy 回退。

## 5. DDL、锁、事务与计划新鲜度

### 数据库生命周期

解析/预检 → 前端鉴权 → 既有 DDL 事务 → 数据库锁 → 执行时存在性/账户/可写性/能力校验 → 原子写入完整默认值及代际 → 原有提交/回滚 → 响应。

CREATE 的默认值和库对象共用同一事务。ALTER 采用数据库排他锁；CREATE 继承在已有数据库共享锁下验证默认值代际。DROP 清理共用生命周期事务。相同有效值可以不增加 version，但仍校验权限与对象；version 溢出明确拒绝。

数据库默认值操作的新增锁序只有“数据库锁→默认值行”，不增加反向拿库锁的 catalog 回调。表级 ALTER 使用现有表 DDL 锁，不为改变表默认值再获取数据库默认值行锁。实际整合时逐条核对现有 COPY/DDL 的锁顺序，新增反向等待视为设计阻塞。

prepared EXECUTE 重新检查数据库权限，包含 PREPARE 后撤权、角色成员变化与缓存失效。订阅库、系统库、readonly/不可写环境沿现有 admission，不借 background context 绕过租户。

继承计划保存 `(database_id, defaults_version, effective identity/revision)` 依赖；锁后不匹配走现有 definition-change retry，完整重建 plan/列/default expression/index 依赖，而不是落库前修改一个字段。LIKE/显式表选项不建立无关库默认依赖。

### 表默认值与列操作

parser 增加明确的转换意图（独立 AST 或等价不可混淆标记），Format/Free/clone 保真；FORCE/KEYS/TABLESPACE 等历史占位不是 charset 请求。

只改变表默认值时复用已有 schema replacement/ALTER 持久化协议，不扫描、转换或重建既有行/index。不能因 SHOW 重放时省略旧列属性而间接转换旧列。

ADD/MODIFY/CHANGE 在目标表当前有效默认值上绑定，保留显式属性及语义修订，随后走已有类型/index/constraint admission。组合 ALTER 必须按选定 MySQL 参考行为处理选项先后，不能由 AST 格式化顺序决定；失败整体不发布新增列或半默认值。

### CONVERT 与数据/索引原子性

显式 CONVERT 进入现有 COPY/rebuild 生命周期：构造完整目标 schema → 验证所有转换列与索引/约束准入 → 用已有有界数据复制/类型校验生成目标行和索引 → 在既有 DDL 事务内发布 → 清理临时表/资源。

- 支持域之间的文本变更复用现有类型转换及已实现的 codec/validator，不把 connection conversion 当作 column repertoire 校验。
- binary charset 对字符列的类型变化与仅选择 _bin collation 区分，按固定 MySQL oracle 验证。
- 现有 PRIMARY/UNIQUE/FK 等约束在目标域重新检查；不允许为了成功复制跳过 dedup 或合并/删除冲突行。
- 当前不能正确支持的目标域/索引组合在副作用前拒绝；native Unicode 主键/唯一键沿当前 guard。已准入域若发现 index/full-scan 不一致，必须修闭环或记录真正阻塞，不能以“是 C07”当作通过。
- 失败、cancel、commit error 与重试不得发布新 schema，原数据/index/默认值保持；已创建临时目标由 COPY 原 owner 清理。
- 不改变 index-plugin dispatch 或为某算法新增 SQL 分支；重建由已有统一机制拥有。遇到未覆盖 writer 路径明确拒绝，不作 silent legacy fallback。

## 6. 资源、等待与成本

| 维度 | 所有者与约束 | 所需证明 |
| --- | --- | --- |
| Q1：catalog result/内部 executor | 创建成功即注册 Close；不移交到 cache | 读/写/解码失败注入，已有 owner 终止路径 |
| Q1：COPY 临时表、vector、batch | 复用 COPY/事务 owner，成功移交或失败清理一次 | 转换中途/提交错误/cancel 后原表可用且无残留 |
| Q2：数据库/table DDL lock | 复用既有锁序和事务 cancel/timeout；不另加重试循环 | 屏障控制的竞争、timeout/cancel、后续请求成功 |
| Q3：默认值元数据 | 每数据库固定一行；DROP/账户清理；无常驻 cache/worker | DROP/CREATE 同名与账户隔离 |
| Q3：转换 workspace | 沿既有 batch/mpool/packet 上界，不物化全表/全部转换 key | 分配失败与清理，按修改消费者记录峰值 |

普通继承只增加定点 catalog 查询/计划依赖，不增加 DML 逐行名称解析或新后台工作。表默认值 ALTER 不能变成 O(rows)；CONVERT 是显式 O(data+index rebuild)，不承诺与 metadata ALTER 同成本。UCA scratch 遵守 #29601 的既有 per-value bound，不宣称它已纳入 mpool。

## 7. 展示、恢复与升级

- SHOW CREATE DATABASE/TABLE 从权威有效元数据生成可重放 DDL；旧列域必须显式保留，不能被新表默认值覆盖。
- SCHEMATA/COLUMNS、CHARSET/COLLATION 与协议消费者使用同一有效定义；增加默认值 JOIN/读取不得改变原授权可见性。
- `character_set_database` / `collation_database`、USE 与 SET CHARACTER SET 的现有消费者读取实际库默认值；不替换 server/connection owner，不扩大 C02 转码范围。
- snapshot/PITR 读取源历史默认值，在恢复子对象前按新账户/新数据库 ID 重建；表级恢复到现有库不覆盖库默认值。
- database clone/data branch 与 DDL replay 复制有效域并重映射 ID；LIKE 只复制表，不修改目标库默认值。
- 新表由 bootstrap/租户初始化与**新的独立升级版本**幂等创建；禁止修改已完成的 4.0.9 migration 冒充新增升级。当前建议 4.0.14 与 protocol 109，实施前及合并上游后重新核对占用并顺延。
- 在全体相关节点完成能力协商、catalog 升级前拒绝新的数据库默认值持久写入和依赖计划，不能复用已被上游占用的协议号。
- 重启/恢复保持原 semantic identity、revision、key format；旧对象无记录不猜测历史被忽略的 CREATE 选项。
- 直接降级到忽略新 catalog/ALTER 合同的版本不支持；必须使用能够保留语义的备份/恢复或独立迁移流程。新域的 C11/C12 gate 不被此基础设施替代。

## 8. 变更与风险地图

| 闭环 | 所有者/主要组件 | 级别 | 必须证据 |
| --- | --- | --- | --- |
| AST、名字与计划字段 | parsers/tree、mysql_sql.y、common/collation、plan/proto | R2/R3 | parse/format/free、pair/alias/禁用域、修订/deepcopy/正常生成一致性 |
| 数据库持久默认值 | catalog、compile DDL、frontend、bootstrap | R3 | CREATE/ALTER/DROP、权限/账户、事务、缺失/损坏、代际、升级 gate |
| 继承/复用计划 | plan build_ddl/build_util、compiler contexts、prepared | R3 | qualified target、完整 replan、LIKE/CTAS/ADD/MODIFY、后台/双 CN |
| 表默认值与 CONVERT | build_alter_*、compile ALTER、既有 COPY/schema/index owner | R3 | 默认值不改旧列、显式转换、失败原子性、约束与 cleanup/竞争 |
| 展示与反向恢复 | SHOW/sysview/变量、snapshot/PITR/clone/replay | R3 | 可见性、重放、重启、历史/跨账户/不同恢复粒度 |
| 公共测试与交付 | owning UT、database/charset BVT、sqlintegration | R2 | 真实结果/index 对照、result review、comparison、清理/重复 |

设计 gate：新增跨 subsystem 的持久/catalog、权限/租户、恢复/升级、prepared 代际与数据转换合同，必须先批准版本化设计；不是仅修 parser 的普通局部 bug。

## 9. 验证矩阵与证据规则

| 验收 | 最小 UT / 白盒 | 公共验证 / 独立 oracle |
| --- | --- | --- |
| 原始 Gitea 语句生效 | parser→plan→catalog 执行 | 默认配置下原始 ALTER + 新 VARCHAR + `a/B` 比较/MIN/MAX/SHOW |
| 库→表→列及显式优先级 | 目标库/有效域/修订 typed 断言 | 两库两连接；CHARSET/COLLATION、比较、索引与 scan 对照 |
| 默认值变更不转旧对象 | ALTER plan 不改旧列/无 COPY | ALTER 前后旧列结果不变，后建表/ADD 使用新默认 |
| LIKE / CTAS / MODIFY | 来源类型、默认值、表达式与修订 | 源/目标不同默认、显式列控制、qualified/prepared/background |
| CONVERT 成功与失败 | 转换意图、COPY admission、失败注入/cleanup | 最小两行边界数据；repertoire/约束拒绝后数据、schema/index 不变 |
| 无半对象/非法请求 | pair、重复选项、latin1、禁用域全输入预检 | 组合 ALTER 有 ADD 的失败后检查无新增列、无临时对象 |
| 权限/代际/隔离 | execute-time grants、ID/version、取消屏障 | PREPARE 后撤权、跨租户同名库、DROP/CREATE、跨 CN ALTER 后 EXECUTE |
| 事务与持久化 | catalog/result/commit 错误、幂等升级 | COMMIT/ROLLBACK、真实旧版本数据升级/重启、普通/系统租户 |
| SHOW/恢复一致 | history lookup、ID 重映射与完整元数据 | snapshot/PITR/clone/DDL 重放；表级恢复不覆盖库默认 |

实施前扩展现有 `build_ddl_test.go:TestCreateTableInheritsEffectiveServerCollation`、`build_alter_*_test.go`、`build_show_util_test.go`、frontend/prepared、upgrade 及 sqlintegration fixture。旧 PR database_defaults UT/BVT 作为可复用素材，先核对 distinct oracle，避免重复起服务。

BVT 优先复用 database 默认值与 charset_collation 文件，分别读取 sql/result；权限、恢复、双 CN 独立生命周期确有不同才拆文件。普通核心数据 0–2 行，语义冲突/顺序边界才增加最少值；不同场景按名称辨识，不用 sleep/随机重试。

MySQL oracle：实际连接并记录版本/digest/config，再固定默认值 ALTER、CONVERT、CTAS、组合 ALTER 与 binary 的最小差分；文档链接不当作运行证据。可复用已固定 8.4.11 参考工具，但 C02 的 connection golden 不是 C03 DDL oracle。

顺序：聚焦 UT/生成检查 → owning packages → 增量 gofmt/vet/lint → 公共 mo-tester comparison → mapped race/双 CN/重启/upgrade/恢复。工具链跟随 go.mod，当前 Go 1.27.1；CGo 只用仓库 mo-cgo-test，native 来源通过 provenance 校验。

新增/改动非生成可执行逻辑覆盖率至少 75%。生成 result 先审每行/列 metadata，再正常 comparison；检查 teardown 后同实例第二次运行。只清理本 worktree/test 拥有的进程、mo-data 与临时资源。

## 10. 决策记录与完成门槛

### 提请批准的决定

1. 接续 #29374，复用其持久/事务/恢复框架，按完整 C03 而非原始 parser 症状交付。
2. 以当前唯一能力目录与有效身份/修订承载数据库默认值，不复活旧白名单，不擅自激活未就绪域。
3. 表默认值 ALTER 只改默认值；CONVERT 明确走现有 COPY/rebuild，不以兼容 no-op 替代。
4. 新独立 catalog 升级及未占用 protocol gate；旧 schema/新域 gate/降级约束单独明确。

### 开发阶段必须关闭的证据缺口

- C03 DDL MySQL oracle 尚未运行；组合 ALTER 与 CTAS 的具体边界以实际参考固定后写测试，若与本文架构冲突先修订设计。
- 旧 PR 合并后的当前 baseline/range 尚未建立，旧验证不声称新实现通过。
- 现有索引/COPY/恢复的所有反向路径在实施 review 中完成逐 hunk change map；当前未进行完整代码审计，不声称 Q1–Q3 全部通过。
- 当前禁用域与 native Unicode PK/UNIQUE 继续拒绝；不能因 issue 关闭、名称被接受、后台库表存在而宣称其 gate 通过。

完成必须具备：批准设计的可追溯修订、全 C03 验收结果、所有 changed hunks/反向消费者映射、有效测试终态与平台/工具链记录、零未解决 blocker 的 mo-self-review、交付 diff 无 TODO/本地残留。需要依赖其他 workstream 才能满足的未完成项明确上报，不伪造完成状态。

2026-10-10 用户批准本设计 r1，进入 merge、实现与验证阶段。审批前仅创建本文与 TODO；审批不代表功能或测试已经通过，也不授权虚构完成状态。

本文保留批准时的基线、事实与待验证项。2026-10-11 交付合并上游后按已批准顺延规则采用 protocol110 / 4.0.15；实际实现、自审和最终成功/失败证据以 `CLAUDE_VALIDATION_28998_C03.md` 为准，不把本文的批准或历史建议编号当作验收结果。

## 2026-10-12 review反例闭包补充（既有r1合同，不新增feature）

- **最终列域决定算法**：同一ALTER里的DEFAULT与MODIFY/CHANGE组合，算法admission与实际binding必须共享effective final table default。原schema只读，私有浅拷贝只改default/revision字段；已有COPY继续拥有字节校验、约束/索引及原子发布。仅DEFAULT仍不重写旧列；显式列域覆盖继承。域/修订变化不得接受metadata-only INPLACE/INSTANT或LOCK=NONE。
- **TS恢复定义包含durable defaults**：cluster快照的TS loader与account/PITR loader一致，从同一source tenant/TS读取database identity与defaults；普通CREATE重放携带合法identity/revision的声明，由目标CREATE分配新DB ID。legacy缺关系/缺行保持既有fallback；坏generation、读取失败与cancel不被伪装成无default。
- **能力不是数据有效性**：protocol110不能跳过source preflight。所有可恢复定义必须在account等不可逆操作前校验；TABLE-only仍遵循原能力检查与不覆盖DB default的边界。复用已有context/BackgroundExec/transaction owner，不增加cache/state/worker、row scan或生产retry；额外metadata读取按DB数量有界。
- 原批准设计/历史验收不自动覆盖这些组合。新增typed UT、真实emoji/cluster恢复反例与公开case保留原fixture、正负oracle、teardown和普通比较，具体失败→修复结果见验证记录。

2026-10-12 上游080365f0已使用591–595作为TANH/COSH/ASINH/ACOSH/ATANH。按r1的未占用编号顺延规则，本PR尚未发布的`INTERNAL_TABLE_COLLATION`从历史591移至**596**，`FUNCTION_END_NUMBER=597`；保留上游全部编号及588–590。本补充不改变SQL函数名字、C03 classifier0展示、wire tags、protocol110或4.0.15迁移，历史591证据不当作当前编号。

## 2026-10-13 上游协议、生成合同和展示交互

- 正常合并cd8d5abf、175a645a及21f19ae0，保留已发布MEDIUMINT协议110、IndexSearchScan协议111、FORMAT_BYTES596/FORMAT_PICO_TIME597。仅将本PR未发布能力顺延112、internal函数顺延598/END599；协议生产、消费、恢复、sysview和4.0.15升级门及负控同步，不把111视为已拥有C03能力。
- 完整PB descriptor相对175只允许C03 DatabaseDefaults及CREATE DATABASE#5/ALTER DATABASE#3/CREATE TABLE#18；上游IndexSearchScan全部layout、reserved、enum和pipeline保持。parser/PB正常生成，不选择整文件ours/theirs或手改生成字节。生成噪声仅在源schema不变时从精确预像恢复。
- Binary COPY同域stored ColRef保留历史bytes的优化不得覆盖MEDIUMINT Width24目标赋值边界。共用严格目标集合保留双方binary/varbinary及int32/uint32，原MORPC110 authoring/remote-expression fence不变。
- SHOW FULL COLUMNS的上游投影已不再使用null占位符，因此不能沿用末端字符串替换。建query前按C03能力选择既有ColumnCollationSQL或上游selector，两种账户路径共享投影；112下binary classifier2仍展示binary，低版本保持上游合同。复用现有binary转换fixture作真实SQL正控并加一条公开SHOW，零新增fixture/数据，既有失败/显式CAST/清理控制保留。
- 21f19的qualified-identifier scanner上下文由单次调用defer/reset拥有，DEFAULT/CONVERT及CURRENT_USER grammar不改；mixed整数comparison域与assignment域分离；prepared LIMIT=0 materialization仅对SELECT顶层参数limit生效，Compile退出恢复borrowed plan，不覆盖C03 CREATE/ALTER freshness admission。相关新owner/真实消费者在当前验证中单独定界。
- 本轮不新增持久状态、cache、worker、goroutine、锁序、retry或数据扫描；协议检查、ID lookup及常数规模SQL投影构造由原owner持有。历史race/restart证据只维持已证明不变的合同，不冒称本轮全部重跑；当前验证状态见配套验证记录。

## 2026-10-13 EXPLAIN JSON主干交互（冻结35c8dd5854）

- 延续r1和既有ready PR，正常合并EXPLAIN JSON及SDK CI两提交。唯一文本冲突是生成parser；grammar保留双方规则后经仓库goyacc正常生成并两次hash复现，不手改生成代码。EXPLAIN JSON/CURRENT_USER与DEFAULT/CONVERT在已有三行零fixture UT中前后交替解析，AST类型、format选项、转换意图及format/reparse保持。
- 新JSON上游代码仍引用旧VectorIndexScan，fresh完整explain编译证明与已发布111不兼容。只把新JSON消费端对齐现行INDEX_SEARCH_SCAN/IndexSearchScan及QueryPayload、AlgoOptions、AlgoExprs；不恢复旧字段、不改proto/generated/wire、111或C03的112/4.0.15/598/END599。
- 诊断输出使用既有indexSearchScanSettings/SearchPlugin ExplainHooks获取说明并传播codec错误，完整hex保留算法opaque bytes，按当前names/values渲染表达式并拒绝缺项/空名/重复/空表达式；query payload、candidate k、方向、range、prefilter与include列仍typed。没有在SQL/catalog加入算法分支、复制IVF decoder、改hook/registry、GPU/ISCP/CDC或执行reader。
- 旧NProbe/first-round语义由当前plugin payload拥有，不把已移除的fields当零值。公开既有JSON脚本只同步两个payload标签及NProbe说明标签，bytes/16/first-round2与3/界限/control assertions和result不变。复用原16完整C03闭包加完整JSON，普通同实例双轮与清理的资格见验证记录。
- JSON说明仍走原authorization与query-only检查，不执行DML；ANALYZE/PHYPLAN/CHECK不支持在原owner提前拒绝。新增renderer/UT state仅本次调用局部且按输入规模有界，不新增生产生命周期或持久状态；历史race/restart/upgrade保留历史范围。

## 2026-10-13 后续共享索引说明接口协调（冻结8586fa0871）

- 主干f14a1fa9ee实现同一IndexSearchScan兼容修复并扩展详细说明接口；正常逐hunk合并，JSON使用`indexSearchScanSettings(spec,true)`，保留主干canonical settings、named `name=value`、vector的query_vector及fulltext的query_payload。保留本分支完整opaque bytes hex与duplicate/空白名/NULL/layout拒绝；不并存两套算法loop，不复制decoder或恢复旧PB。
- 上游compact text稳定、VERBOSE/JSON详细设置、IVF/HNSW search threads/expansion、classic/fulltext2 mode/include predicates/score/zero guard和损坏bound类型控制原样保留；全部原typed及公共控制不弱化。只恢复两个vector payload断言标签到主干的canonical名称，SQL/result现已与8586精确相同，不重新生成golden。
- 47bbf28c05只在legacy域不提前提取varlen bytes，native Unicode仍原Validate admission；8586 row-variable NULLIF在raw scan到目标列赋值时继续使用forceAssignmentCastExpr(target.Typ)，C03 defaults/domain owner不改。通过完整vector/plan及新增完整LOAD公开owner验证交互，数据仅其已有5行资源。
- CI38377757841的两UT相关失败发生在精确6a3的checkout TLS（exit128），执行/coverage采集未开始；汇总缺producer不是百分比判负。不能用产品改动、retry/skip/覆盖门豁免掩盖；新正常push触发正常新CI，不声称未完成远端CI成功。
- 当前分配仍112、4.0.15、defaults#18、598/END599；proto、grammar、native、modules未变，无本轮新执行lifetime/locks/workers/waits/格式。所有历史race/restart资格保留其输入范围，当前验收见验证文档。

## 2026-10-14 CI消费者与协议冲突协调（冻结fcad62a8ff）

- 保留main有符号WEEK/DAYOFYEAR协议112，未发布C03按获批顺延规则迁至113；authoring/loader/restore/sysview/4.0.15门槛与全部正控同步，109–112旧协议拒绝，混部113/112升级在写入前拒绝。4.0.15/min4.0.14/offset4、defaults18、598/END599不变。
- CI38421272142的原Unicode消费者证明SHOW FULL COLUMNS二进制类型必须NULL，而information_schema仍须binary。新增纯SQL投影helper区分这两个展示合同，保留所有字符域/native/revision分类；原CI断言不改。C03真实SQL双检查、原public三处SHOW binary单元格纠正，其余SQL/数据/metadata断言不变。此前历史112展示binary不再代表当前合同。
- PB/parser均正常生成。普通make因时间戳跳过parser的本地编译红证据保留；最终仓库make -B强制双次生成并核对一致哈希及无冲突标记。完整descriptor对比新main只增加批准DatabaseDefaults与5/3/18，上游RankOption.nprobe2与全部其余类型/编号/reserved/options/enum原样保留。
- 逐项协调main的live-source COPY宽度/replay、view admission、LOAD NULLIF重绑定、共享CTE/set-operation ordinal/effect保护、OP_TYPE引用及procedure gate、subscription标识符/ADD列表/RESTORE拒绝规则。复用既有owner，不新增本轮生产state/cache/worker/锁序/retry；原C03严格赋值、生成列内层CAST、默认版本新鲜度与恢复身份边界不变。
- 新鲜11 owner、10实际SQL、14包静态、严格609/756=80.56%、23完整public同实例双轮通过；历史race/restart/upgrade/GPU范围不扩张，精确成功与失败见验证文档。本地通过不推断远端Linux CI绿色。

## 2026-10-15 生成表达式评审修复与main协议协调（冻结17a6200f59）

- main已占用113作为DECIMAL variance/stddev DOUBLE合同。保持该合同及全部消费者，未发布C03顺延至114：初始化、DDL authoring/loader、逻辑恢复和4.0.15升级门禁同步迁移；新增main113前驱拒绝及114/113混部写入前拒绝。独立迁移版本、最小版本/偏移、PB字段18和函数598不变。
- 评审反例：精度10创建DECIMAL除法→VARCHAR STORED，插入后把会话精度改为0，再同域CONVERT，旧值从0.333333333333变成0.33。变化原因是MODIFY重新绑定持久化表达式，不是字符域转换本身；不允许用该次会话设置重新创作未受影响的计算。
- CONVERT先构建完整目的类型骨架，再复用既有typed replay的输入域兼容性判定。可保留的DEFAULT、ON UPDATE和生成表达式从原始列位置快照复制；不是从重建SQL或可变会话推导。转换专用CompilerContext只在同步规划调用内存活，并转发已有内部SQL executor；普通显式MODIFY仍按其声明和当前会话绑定，输入域确实变化的表达式仍走原绑定路径。
- 同域表达式原样保留；换域只重定向根字符串cast_strict赋值owner，绝不剥离作者plain CAST。若原来没有赋值owner则增加一层，之后复用该owner；往返转换不线性堆积持久化CAST。COPY重建阶段必须证明目标表达式与这一精确typed变换相等，才允许replay；改变的表达式或不兼容依赖不会被SQL文本等价误判。
- 保留MODIFY原有类型、FK、索引、生成列环检测、约束、nullability和严格赋值检查，以及COPY的事务、索引重建、发布与失败清理。新状态只包含当前调用内的schema/表达式副本，不增加生产缓存、goroutine、锁、等待或重试。
- 公共SQL正反例检查已有行和之后插入行，并保留未改会话、普通COPY控制；typed检查同域/legacy/native域、DEFAULT/ON UPDATE、作者CAST、schema不可变及多次往返深度。正常合并两个冲突；pipeline/parser由owner生成两次一致，完整pipeline descriptor与main相同，plan descriptor仅含批准的defaults增量。最终资格见验证文档，历史证据不扩张。

## 2026-10-15 新增P2：函数598表达式兼容性闭包（冻结main bb2b725d0a）

- `internal_table_collation` 的执行身份598纳入共享表达式feature walk及Any，统一要求114：持久化authoring/加载、worker选择、发送目标与接收前验证。按编码ID判断，不依赖可伪造显示名；596/597及main113统计合同不变，不新增protobuf字段或函数编号。
- 普通/预编译视图在113拒绝且不发布目录行，114允许并写入最低版本marker；reader重申114，下限不足时marked/unmarked定义均拒绝。旧普通视图仍可在113创建读取。实际RPC模拟能力边界不冒充真实旧二进制混部资格。
- 新门禁揭示fresh bootstrap尚未admit时不能创建598消费者。复用既有阶段：prerequisite事务先安装legacy TABLES；catalog admission达到114后，在原derived事务中DROP/CREATE新版TABLES并创建四个trace completion views，再开ingress。完整旧集群仍走skip，不因本轮重写系统视图；普通tenant初始化保持原114模板。
- 新Exec结果逐次Close，既有事务独占commit/rollback；失败、取消与不确定提交沿原框架处理，无新等待循环/worker/cache。实际SQL证明先成功DROP再被CREATE门禁拒绝时原rel_id不变，114重新初始化成功。新增开销仅每expr常量比较和缺失derived schema初始化的两条DDL，不改COPY/恢复/索引等既有所有权。
- 任务启动时正常合并官方bb2的只读cache不可变性修复并验证其normal/race。验证期间main前进到461bc8e349，未追逐其无关24文件；该轮PR差分与证据基于bb2，前进main合并树无文本冲突。准确资格及历史限制见验证文档。

## 2026-10-16 CI/冲突修复设计（冻结main 76deafc806）

- 保留main114原生Unicode新建普通持久表UNIQUE及canonical key format、115 calendar normalization执行ID598、116 division warning合同；C03及其TABLES消费者统一117/599，升级/恢复/作者/reader/发送接收/worker选择都同步。新增114–116前驱拒绝及598/599身份正交控制，不把旧未发布C03的114误当main114能力。native Unicode PRIMARY、已有表索引mutation和CONVERT边界继续交由上游owner拒绝，不回退main的新建UNIQUE能力；历史正文全局拒绝UNIQUE的说法仅适用于当时main。
- derived bootstrap需要替换legacy TABLES，而DROP跨越main lineage writer的pessimistic/RC门槛。该内部ExecTxn显式拥有pessimistic RC选项，沿用既有commit/rollback/retry/结果Close；不改变用户/集群默认事务模式，也不绕过lineage锁。原optimistic CDC负控继续以optimistic集群启动并在target create前被拒绝。
- migration在两个请求之间没有reqCtx。有效默认值读取改为显式接收migration operation context，保留其取消/截止期/值；background reader租户来自session认证TenantInfo，不来自last-query accounting、migration RPC或已结束请求；缺失认证身份先拒绝。活动用户事务仍走原compiler/transaction视图，不创建第二个提交owner。普通无请求getter使用connection context，断连/请求取消在reader创建前终止；非默认变量仍走原session存储。新增无请求/已结束请求/活动请求、认证租户7/last-query99/RPC42、operation/connection context传播、取消/缺失身份前零reader及成功/错误逐次Close控制，并用真实prepared-owner SQL migration回归。
- Issue28909降级71的无ghost断言改读原始mo_catalog.mo_tables，不依赖最低117的TABLES视图；保留错误码、已有数据/普通SQL、恢复后同名视图创建断言，不放松view能力门槛。
- main114原生UNIQUE以KeyFormatV1/CollationVersionV1保留schema级证明，不能与独立表默认字符域revision混为一谈。DEFAULT只更新继承身份，已有V1物理索引仍保留schema marker1和原列/索引；optimizer-pruned count(*)不能因此失去准入。TABLES formatter沿SchemaExtra.Unmarshal验证已知identity/revision/format，仅展示默认字符域：已知format1/schema marker1且legacy默认域时，用域revision0解析名称；这不是索引执行准入，不改全局RequireLegacy或TableDef/index owner。未知format2/256、revision、identity、缺失native revision、0/1历史身份及非支持字符域仍拒绝。保留完整source metadata在frontend recoverLegacyTinyText入口、ALTER重写之前的既有RequireLegacyCollations门；不把最终marker当原始损坏数据的修复手段。有效RED证明已知format1展示误拒绝及DEFAULT把marker1清零；typed pruning与真实SQL的TABLES/COLUMNS、DEFAULT后count(*)/重复1062验证修复，已有表index mutation仍拒绝。
- legacy partition fixture保留main两列seed和普通列模板clone、4098列typed注入及全部边界/回滚断言；保留本分支分阶段180秒/600秒总预算。pipeline冲突从合并proto owner双次生成且字节一致，不手改生成块。完整解压descriptor审查证明pipeline差异仅SourceCodeInfo，字段/类型/编号/reserved/enums/options与main一致；plan仅批准DatabaseDefaults及CreateDatabase#5/AlterDatabase#3/CreateTable#18。当前执行结果及限制另记验证文档。

## 2026-10-16 新一轮评审修复（冻结 main b3d6b21fe9）

- 本轮是既有 r1 设计内的正确性修复，不引入新功能/存储格式。新反例为 `concat(v,a/b)`：字符列 v 换域使旧 replay 丢弃整个表达式，导致无关除法被当前 div_precision_increment 重新创作。原始实际 SQL 中，改变字符依赖、带作者 CAST、经数字 CAST 的三个反例失败，保持域不变等控制通过。必须同时保持无关计算并真正更新受影响字符操作，而不是把旧树只贴上新标签。
- 首个 replay owner 仍按列身份建立原始/可见坐标及物理类型兼容性；只有物理类型、宽度、scale、enum/自增均不变的 CHAR/VARCHAR/TEXT 依赖允许字符域适配。按 typed 树自底向上更新字符引用，仅在参数域变化时重新绑定该操作；不受影响的除法等子树保留原执行身份/返回类型/参数。作者 CAST 保留其目标，根目标赋值由原有 owner 重定向；新声明和物理类型变动仍走原绑定。对现有 CAST0 且非 syntax_explicit_cast / EXPLICIT COLLATE(rank0) 的编译器字符串coercion，在受影响字符操作绑定前刷新其输入域：非字符格式化输入使用 NUMERIC coercibility5，不向原数字子树写标记；仅合并物理类型/宽度/pad相同的隐式层，保留窄宽度、作者/数字/assignment边界，使 native 混合域不会被旧 IMPLICIT 标记误拒绝且重复转换有界。CHECK 与数字结果生成表达式共享此首个 owner，避免仅修 VARCHAR 输出。
- replay 仅在目标仍为原表达式，或等于已证明的 typed 适配/根赋值变换时保留；不接受 SQL 文本等价作为权威。CONVERT 从 replay 的适配结果恢复可见→完整 catalog 坐标，再交给 MODIFY；不再丢弃适配结果而取旧 raw 表达式。typed 适配失败直接拒绝 CONVERT，不回落到整表达式重新创作。所有 schema/表达式/坐标/error 状态均为调用内副本，无新缓存、worker、锁或等待；成本按 schema/表达式大小有界，重复转换不得堆积 cast。
- main117 已发布 collation-aware LEAST/GREATEST overload4；保留其 identity、feature walk、author/read/worker/send/receive 门禁与旧 overload0。C03 顺延118，INTERNAL_TABLE_COLLATION599/END600、升级4.0.15/min4.0.14/offset4与 PB18 不变。新增真实 main117 拒绝，118 允许；TABLES 初始化/升级/恢复及独立599表达式门禁同步，不把117误当作C03。
- 合并交互逐项保留：SET NAMES DEFAULT 及 collation_connection 的独立服务端解析/迁移状态、qualified ALTER VIEW 目标库、window interval 执行前检查、COLLATE AST、两个独立表达式feature。合并 grammar 按 owner 强制生成两次，与自动合并的 parser 及彼此 SHA 一致；没有 protobuf 生成冲突或本轮 wire/schema 增量。
- 验证复用现有 planner、SQL、restart 与 public case fixture：每个 SQL 子例仅两个已有/未来行，已有 public 脚本内增加字符依赖、作者 CAST 与真正 native 比较域的独立断言，新增 public 场景仅三行，不更改旧 oracle 单元格、不 genrs、不增加 skip，不改变原有场景的数据或超时预算。当前有效结果、资源回收证据及未验证平台见验证文档。

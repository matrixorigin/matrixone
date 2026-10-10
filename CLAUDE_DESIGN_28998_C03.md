# #28998 / C03：数据库、表、列 charset/collation 继承与显式转换

修订：r1，2026-10-10。状态：**用户已用 `go ahead` 批准实施**。批准草案 SHA-256：`075264245dfa4bd5a921cf2b7d3ba8d28dfe2a190b9639e2d6d5f7232e06cfea`。

任务：[#28998](https://github.com/matrixorigin/matrixone/issues/28998)，承接 [C03/#29482](https://github.com/matrixorigin/matrixone/issues/29482) 的全部范围；上位任务 [#29479](https://github.com/matrixorigin/matrixone/issues/29479)。拟接续 draft PR [#29374](https://github.com/matrixorigin/matrixone/pull/29374)，不另做重复 no-op 修复。

开发基线：`e5dd4724f782067e1c76c38c9d2f55cebceda4a9`，已经在独立 worktree pull `mo/main`。旧 PR 的设计批准仅覆盖其原有范围，不自动批准此次 C03 扩展。本文批准后记录内容 SHA-256，并在交付 revision 中保留可追溯设计。

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

# Issue #28998：数据库默认字符集与排序规则

修订：R1，2026-09-24，用户已用 `go ahead` 批准。原审批版本 SHA-256：`8be757dff9840829a4ff76cb6acc428c8d0f8319133364cda5c7f2981c290a34`。

基线：`3063f44fe5527dc74dbb3887822214e6fefcc80e`。

## 1. 问题与验收契约

Gitea 初始化执行 `ALTER DATABASE d CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`，当前 main 无此语法，ORM 初始化失败。PR #29023 的兼容 no-op 在默认配置下返回 OK，却没有改变随后 CREATE TABLE 采用的默认值。

本次必须满足：对存在且获授权的目标数据库，支持的 ALTER 在事务提交后使数据库默认值实际生效；后续未显式指定字符集/排序规则的字符列、元数据展示和现有执行器行为一致。错误、取消或回滚不能留下半份配置。已有表保留其原有默认值和列类型。

最小反例与验收见证：

```sql
CREATE DATABASE d;
ALTER DATABASE d CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
CREATE TABLE d.t(v VARCHAR(10));
INSERT INTO d.t VALUES ('a'), ('B');
SELECT COLLATION(v) FROM d.t LIMIT 1;
SELECT MIN(v), MAX(v) FROM d.t;
SHOW CREATE DATABASE d;
SHOW CREATE TABLE d.t;
```

预期列为 `utf8mb4_bin`，MIN/MAX 为 `B/a`；同数据的 general-ci 对照为 `a/B`。还需跨连接执行 CREATE TABLE，以证明默认值属于数据库。此处为待运行的验证设计，不是已获得的测试结果。

MySQL 参考：[ALTER DATABASE](https://dev.mysql.com/doc/refman/8.0/en/alter-database.html)、[数据库字符集与排序规则](https://dev.mysql.com/doc/refman/8.0/en/charset-database.html)。采用其数据库默认值、选项推导、权限及建表继承契约；事务处理遵循 MatrixOne 已有 DDL 规则。

## 2. 当前源码证据

| 边界 | 当前实现与影响 |
|---|---|
| Parser | `pkg/sql/parsers/dialect/mysql/mysql_sql.y` 的 `alter_stmt` 只有数据库兼容模式配置分支；CREATE DATABASE 已能保存 `CreateOptions` AST |
| CREATE DATABASE | `pkg/sql/plan/build_ddl.go:buildCreateDatabase` 没有解析这些字符集选项，plan 仅携带库名、SQL、subscription 等字段 |
| 表类型 | `pkg/sql/plan/build_util.go:tableDefaultCharset` 对无表选项的建表读取 `collation_server`；`buildTableDefs` 将结果写入表默认值和字符列 |
| 执行语义 | `pkg/sql/colexec/aggexec/minmax2.go` 区分 `CharsetUTF8MB4Bin`；现有 `minmax2_test.go` 有 general-ci/bin 对照 |
| 展示 | `pkg/sql/plan/build_show.go:buildShowCreateDatabase` 对普通库合成只有库名的 DDL；`pkg/util/sysview/predefined.go:InformationSchemaSchemataDDL` 固定输出默认值 |
| 存储接口 | `engine.Database` 有 `GetCreateSql`，无通用数据库选项读写 API；不能假设更新 `dat_createsql` 就完成 TAE/catalog/cache 一致性 |
| 现有配置表 | `mo_mysql_compatibility_mode` 只有自增配置 ID 主键，未对数据库选项建立唯一键，恢复策略为 Skip |
| 恢复 | `snapshot.go` / `pitr.go:restoreToDatabaseOrTable*` 对普通库合成 CREATE DATABASE，必须显式传递历史默认值 |

## 3. 范围与替代方案

首批数据库默认值只承诺当前已有明确语义的 `utf8mb4_general_ci` 和 `utf8mb4_bin`，charset 为 `utf8mb4`。大小写归一；单独 CHARACTER SET 使用其默认 general-ci，单独 COLLATE 推导 charset。其他 charset/collation 或不匹配组合返回明确错误，不通过别名冒充新增语义。

解析支持 DATABASE/SCHEMA、DEFAULT、可选等号、两类选项顺序，以及省略库名时使用当前库；没有当前库时报错。重复冲突选项明确拒绝。READ ONLY、ENCRYPTION 不通过新 ALTER 入口宣称支持。

CREATE DATABASE 的对应受支持选项一起落地，保证 SHOW/导出能重建同样的默认值。数据库 collation 生效复用现有字符列类型和执行器，不在此实现一套新的比较、排序或 Unicode 算法。分区表、存储过程、UDF、warning 兼容仍排除。

| 方案 | 判断 |
|---|---|
| 无条件兼容 no-op | 违背成功响应契约，淘汰 |
| 仅在当前 server/session collation 相等时 no-op | 可作局部拒绝策略，但默认配置下仍无法解决原始 Gitea 场景，也不能给数据库建立跨会话持久契约，不选 |
| 只改 session/global 变量 | 作用域错误；会影响其他库或在重连后丢失，不选 |
| 更新核心 `mo_database` 字段或复用 `dat_createsql` | 需要改变核心数据库 catalog 更新、缓存和恢复协议，且 SQL 字符串不是理想的可变配置所有者，不选 |
| 复用 `mo_mysql_compatibility_mode` | 缺少目标选项唯一性、数据库 ID 代际和恢复契约；需对已有宽泛配置表增加这些机制，不选 |
| 独立、每数据库一行的 catalog 元数据 | 选用；有明确主键和生命周期，可复用普通 catalog 表事务与版本升级机制 |

## 4. 元数据与所有权

拟新增租户内 catalog 表 `mo_catalog.mo_database_defaults`：

```text
account_id     uint32
database_id    uint64
character_set  varchar(64)
collation_name varchar(64)
version        uint64
PRIMARY KEY (account_id, database_id)
```

数据库 ID 是对象代际，DROP 后同名 CREATE 不能继承旧值；库名通过权威 `mo_database` 解析，不再维护第二份可变名称索引。每库最多一行，无新缓存、worker 或重试队列。数据库 DDL 是唯一写入所有者；普通用户不能直接读写系统表绕过权限。所有访问包含当前账户与库 ID。

新建用户库在 CREATE DATABASE 的同一事务保存默认值：显式选项优先，否则记录建库时的有效 server 默认值。只有真正缺失记录的旧库沿用当前兼容回退；读取失败、缺表、损坏值均不得当作缺记录吞掉。系统库/初始化 bootstrap 走明确的内建默认路径，避免查询元数据表时递归触发自身创建。

旧库首次 ALTER 建立记录；无需在升级时猜测其历史创建参数。对于历史创建 SQL 中已被忽略的 charset 选项，不回溯改变现有语义。DROP DATABASE 同事务删除记录；DROP ACCOUNT 通过租户 catalog 生命周期清理。重复设置同一默认值可保持版本不变，但必须重新校验权限、数据库存在性和能力。

## 5. 写入、锁与失败路径

采用真正的 `tree.AlterDatabase` 与 `plan.AlterDatabase`，不继承旧 `CompatibilityNoOpStmt`。proto 仅追加字段，parser/protobuf 统一生成。

执行顺序：解析/校验 → 数据库权限检查 → 标准 DDL 事务 → 数据库排他锁 → 再校验数据库 ID、订阅/只读约束和能力 → 原子写入完整 pair/版本 → 标准事务结束/响应。沿用 `lockMoDatabase` 对 CREATE/DROP 的数据库生命周期串行化，锁顺序为数据库再默认值行。临时结果与内部执行器由创建者在同一作用域关闭。

`ALTER` / `DATABASE ALL` / `OWNERSHIP` 使用现有数据库权限模型；未显式指定库名时，鉴权和执行使用同一解析结果。订阅库和不可写共享库沿用既有写入限制。仅把明确不存在映射为数据库不存在错误，其余 catalog、锁、存储、取消和提交错误原样传播。

采用 MatrixOne 当前允许显式事务中数据库 DDL 的规则，配置和调用者共用事务，不启动独立的后台提交。成功 autocommit 返回零 affected rows；显式事务中的成功在 COMMIT 后对新事务可见，ROLLBACK 恢复旧值。PREPARE 只准备，不改元数据；每次 EXECUTE 重新做必要的权限与目标校验。

## 6. 消费与计划新鲜度

增加明确的数据库默认值读取契约，输入包括目标库及 snapshot，返回库 ID、是否存在配置、标准 pair、版本。前台 `TxnCompilerContext`、内部 SQL executor 的 compiler context 都实现该契约；不让后台执行静默使用另一份默认值。

继承优先级：显式列选项 → 显式表选项 → 目标数据库默认值 → 无记录旧库的现有 server 回退。限定库名 `CREATE TABLE other.t` 使用 other，不能使用当前 USE 库。表建成后，ADD COLUMN/重建依赖既有表默认值；CREATE LIKE 保留源表契约；CTAS 继续遵循现有来源列类型规则，仅新建定义中需要默认值的位置使用目标库。

默认值在规划阶段影响类型，不能只在落库前替换 `DefaultCharset`。凡继承数据库默认值的建表计划记录 `(database_id, defaults_version/absence)` 依赖，执行时在已有数据库共享锁下校验；不一致走现有 statement replan/retry 边界，重新绑定完整计划。prepared、后台、同会话事务内 ALTER、其他 CN 提交 ALTER 都覆盖。未继承数据库默认值的计划不引入无关失效。

持锁 ALTER/CREATE/DROP 不形成新的反向锁顺序；锁等待受已有事务取消与超时约束。通过确定性屏障验证竞争，不使用 sleep。若实施发现现有 retry 边界无法完整重新绑定，此处属于设计阻塞，先修订方案，不能用晚期修改列类型绕过。

## 7. 展示、恢复与升级

- `SHOW CREATE DATABASE` 从同一权威配置生成可重放 charset/collation 子句，正确引用标识符。
- `information_schema.SCHEMATA` 在原有可见性过滤结果上连接该账户/数据库的默认值，避免因新 JOIN 暴露其他数据库。
- `character_set_database` / `collation_database` 的读取与 USE 行为反映当前目标库；不改 `collation_server` 代替数据库设置。
- 逻辑 dump 使用更新后的数据库 DDL。Snapshot/PITR 按原快照读取默认值，在目标库创建后、恢复子对象前应用；新账户/新库 ID 重映射，不能直接拷贝源 ID。表级恢复到现有库不覆盖库默认值。历史快照确实早于新表时走有证据的旧版本回退，其余读取错误报错。
- 数据库 CLONE/data branch 复制源快照默认值到新库 ID；CREATE TABLE LIKE 等仅复制表定义的路径不覆盖目标数据库默认值。
- 新表在新集群初始化和独立的 4.0.9 租户升级中幂等创建，恢复策略由数据库 DDL 重建并在系统表批量复制中跳过，不直接 Copy 全表。现有 `mo_database` 物理结构不变。
- 以新的集群 protocol capability 门槛启用默认值写入和依赖语义；全部相关节点与 catalog 升级完成前，ALTER 明确拒绝，不能让旧 CN 忽略已写配置。使用仓库现有 persisted-protocol/version 机制，并覆盖旧版本负例。
- 重启依赖普通 catalog 持久化；不依赖进程内变量。降级前必须清理/转换新数据库默认值契约或使用支持该能力的备份恢复路径；不得把“旧节点忽略新表”当作兼容降级。上线说明必须写明这一限制。

## 8. 变更风险图

| 闭环 | 主要文件/组件 | 风险与证据 |
|---|---|---|
| 语法与 DDL 携带 | parsers/tree、mysql_sql.y、proto/plan.proto、生成物、plan deepcopy | R2；parse/format/free、合法/非法选项、proto/deepcopy、生成一致性 |
| catalog 与升级 | catalog 定义、新表访问、frontend 初始化、bootstrap 当前版本 | R3；唯一行、租户隔离、初始化顺序、幂等升级、旧库回退、协议门槛 |
| 权限/事务/执行 | frontend authenticate/stmt_kind、plan build、compile DDL | R3；直接/预处理/后台、授权前后、错误保持、原子性、取消和竞争 |
| 类型继承与计划复用 | build_util、build_ddl、compiler contexts、prepared 依赖检查 | R3；目标库解析、完整 replan、跨连接/跨 CN、显式选项优先级 |
| 展示和持久闭环 | build_show、sysview、数据库变量、snapshot/pitr、clone/dump | R3；可见性、可重放 DDL、重启和不同恢复粒度、ID 重映射 |
| 公共测试/交付 | owning UT、test/distributed、生成器 | R2；真实字符结果、负例、清理与正常 result 比较 |

这是新增跨层持久化能力，触发 `mo-dev` 的 feature-design gate。用户已批准 R1，进入实施阶段。实施前记录批准的 R1 内容散列；交付时提供可追溯设计版本并链接，TODO 不入 git。

成本预算：每用户数据库一行固定大小元数据；单次建表/ALTER 仅定点读取或写入一行，不增加 DML 每行开销。无新增后台进程或 session 缓存。测试优先使用现有 UT fixture；公共 SQL 用两个数据库、两个连接、两条可区分的字符值即可覆盖核心契约。多 CN/重启只承担相应独立证明。

## 9. 验证矩阵

| 不变量/边界 | 主要验证 |
|---|---|
| Gitea 原始语句可达且实际生效 | 原始 SQL + VARCHAR + COLLATION + MIN/MAX + SHOW；默认配置运行，修复前应在 parser 失败 |
| general-ci 与 bin 确实不同 | 同值 `a/B`、显式表 collation 对照及 review 的六值见证；预期来自语义而非复制生成结果 |
| 拒绝错误且不写状态 | 缺库、无当前库、未知/不匹配 pair、无权限、订阅只读；后续查询旧值不变 |
| 权限是执行时契约 | CONNECT-only 用户拒绝，授予数据库权限后成功，PREPARE 后撤权再 EXECUTE 拒绝 |
| 默认值属于目标库 | 当前库 A 修改 B；另一连接在 B 建表；A 不受影响；USE/变量与 SCHEMATA 一致 |
| 继承优先级正确 | 无选项继承；显式 table/column 覆盖；既有表及 ADD COLUMN；CREATE LIKE 与 CTAS 控制 |
| 事务性 | COMMIT、ROLLBACK、设置同值、内部读写/提交错误注入；直接、后台和 prepared 共享规则 |
| 不消费过期计划 | 先 PREPARE CREATE，再其他会话 ALTER 后 EXECUTE；同事务变化；锁竞争后的重建计划 |
| 对象/租户隔离 | DROP/CREATE 同名库、账户同名数据库、伪造或损坏配置拒绝 |
| 持久与迁移闭环 | 服务重启、数据库 snapshot/PITR、表级 restore 不覆盖库默认值、跨账户 ID 重映射、clone、dump roundtrip |
| 旧版本兼容 | 无记录旧库、旧快照、新租户初始化、升级重复执行、混合版本写入拒绝 |
| 资源/等待终止 | 锁竞争确定性屏障、取消释放、内部结果 Close、相关 race 检查；不新增计时型 UT |

现有可复用测试：`build_ddl_test.go:TestCreateTableInheritsEffectiveServerCollation`、`aggexec/minmax2_test.go`、`frontend/authenticate_test.go` 的数据库配置场景、数据库 create_metadata/config BVT，以及既有 snapshot/PITR/clone fixture。新增 BVT 使用独立的 charset/defaults 契约文件，避免塞入只测建库名称的旧用例。

执行顺序：聚焦 UT/生成检查 → owning packages → 增量 gofmt/vet/lint → mo-tester 公共回归 → 与持久化/分布式风险对应的重启、恢复和双 CN 场景。Go 1.26.4，CGo 走仓库 wrapper，覆盖率按 CLAUDE.md 要求。最终 `mo-self-review` 核对所有 changed hunks 和反向消费路径，零未解决阻塞才交付。

## 10. 实施结果与交付约束

R1 已获用户 `go ahead` 批准并实施。独立 catalog、事务所有者、协议 95 门槛和两种 collation 的支持范围均按批准方案落地；迁移独立编号为 4.0.9。

验证时发现并修复了两个实际反例：LIKE 重放时 general-ci 源表错误继承目标库 bin；撤权后连接缓存允许 prepared ALTER。前者通过保留重放 DDL 的显式表 collation 修复，后者在 ALTER 的每次鉴权前清理权限缓存，重新检查角色成员关系和数据库授权。

完整证据与最终自审见 [CLAUDE_VALIDATION_28998.md](CLAUDE_VALIDATION_28998.md)。

支持范围明确限定为 `utf8mb4` 配合 `utf8mb4_general_ci` / `utf8mb4_bin`；包括旧 CREATE DATABASE 曾忽略的 `CHARACTER SET utf8` 在内，未支持的选项现在报错。ALTER 不转换既有表；新建表按目标数据库继承。启用协议 95 并完成 4.0.9 租户升级后才能使用此持久化契约；直接降级到不读取该元数据的旧版本不受支持。

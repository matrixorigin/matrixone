# 保留默认行为的 CI 数据库：范围与验收设计

后续显式general-ci扩展见[独立设计](CLAUDE_issue-25096-general-ci-design.md)及其附录，须单独审批。本文件的UCA400范围和历史批准不覆盖该扩展；下文保留原设计记录。

状态：**r2-draft.1，2026-10-26，待维护者设计评审**。关联 [#25096](https://github.com/matrixorigin/matrixone/issues/25096)；替代已关闭、未合并的 [#30684](https://github.com/matrixorigin/matrixone/pull/30684) 的交付方案，不认可或继承其实现审批。本 PR 只有设计文档；**本设计获批并合并之前，不修改实现或测试代码**。

源码基线：`mo/main@83c1e23526fc0ed31188abd7fc2ca87cd3a6d4b3`。下文“已有”来自源码/测试定义检查，不表示本轮执行过测试。实现时重新核对当时 main，仅修复经复现确认的缺口，不整支 cherry-pick 旧 PR。

## 1. 评审要求与重新定界

[fengttt 的评审](https://github.com/matrixorigin/matrixone/pull/30684#pullrequestreview-5646922664) 要求：事务边界修复独立；collation 应相对局部，不与 snapshot 等流程混合；重点测试 CREATE DATABASE 的默认值及行为；不改变当前默认 “C”，但支持显式创建默认 “CI” 的库。

| 评审点 | 本提案的处理 |
| --- | --- |
| 事务与 collation 分开 | [#31120](https://github.com/matrixorigin/matrixone/issues/31120) 单独跟踪；fault/commit-wait/shutdown 的修改不进入本系列。旧 PR 关闭不等于该缺陷已修复到 main。 |
| 保留默认 “C” | 不改 server/connection/handshake 默认值，不重解释历史目录或兼容别名，不因新建普通库启用新语义。 |
| 显式默认 “CI” 库 | 首选复用 main 已有 `utf8mb4_unicode_ci`，先验收库→表→列→表达式的完整语义链；不新增名为 `C`/`CI` 的 SQL collation。 |
| 变更局部化、测试为主 | 先补行为契约，缺陷在现有公共 owner 修复；恢复流程作为元数据往返消费者验收，不复制一套 collation 专用 snapshot/PITR 状态机。 |
| 先完成设计审批 | 原 [aptend](https://github.com/matrixorigin/matrixone/pull/30684#discussion_r4358707072)、[iamlinjunhong](https://github.com/matrixorigin/matrixone/pull/30684#discussion_r4365958199) 和 [XuPeng-SH](https://github.com/matrixorigin/matrixone/pull/30684#pullrequestreview-5644313301) 的设计门不因关闭 PR、CI 或本提案而自动解决；见第 9 节。 |

**最重要的范围选择尚待确认：** fengttt 没有指定 “CI” 必须是哪种算法。本提案建议首期采用已有 UCA 4.0 的 `utf8mb4_unicode_ci`，而非激活 `utf8mb4_general_ci` 的新修订。它们不是同义词；例如 `ß` 与 `ss` 的关系不同。如果首期必须修复原 issue 的显式 `general_ci` 示例，需在本设计合并前明确选择并扩展设计，不能把 UCA 方案冒充完成。

## 2. 当前事实、目标与不变量

当前 [database_defaults.go](../../pkg/sql/plan/database_defaults.go) 已保存数据库 collation identity/revision/version，继承由 [build_ddl.go](../../pkg/sql/plan/build_ddl.go) 和 [build_util.go](../../pkg/sql/plan/build_util.go) 消费；[metadata.go](../../pkg/common/collation/metadata.go) 已准入 UCA400。现有 [数据库继承集成测试](../../pkg/tests/sqlintegration/database_defaults_test.go) 包含 UCA 默认继承、prepared CREATE、UNIQUE 和覆盖优先级；[重启测试](../../pkg/tests/issues/isolated/database_defaults_restart_test.go) 已检查持久默认值。这意味着本工作首先是**契约梳理及缺口验收**，不假定需要另一套元数据、绑定或激活框架。

“C” 在本文仅指**未选择新 collation 时的现有兼容行为**，不是承诺所有旧算子都等价于 POSIX C。例如当前 server 默认显示 `utf8mb4_general_ci`，却不能由名称推断所有旧比较都执行 MySQL general-ci；历史 CHAR、MIN/MAX 也有各自行为。本提案既不全局修正这些旧行为，也不把显示名称改成 `C`。必须同时验证元数据和结果。

目标：用户显式选择支持的 CI 默认库后，普通新表/新字符列正确继承，查询结果不随物理执行计划或 CN 改变；未选择 CI 的对象不受影响。

不变量：

1. **定义时继承，执行时按类型。** 默认值只在对象定义时参与解析；已存在的列不在每次比较时查询当前库或会话默认值。
2. **已解析的域是权威。** 字符集、collation identity、semantic revision、表达式 coercibility 各司其职；库默认值的 `version` 是元数据代际，不是 collation revision。
3. **同一 CI 域关系一致。** 相等、IN、hash join/group、DISTINCT、shuffle、排序/peer、MIN/MAX 和已支持的物理 key 使用一致关系；权重相等的原值仍可不同，不能用权重替代返回值。
4. **无静默降级。** 不支持的名称/域组合、能力不足、损坏元数据或 CI 非法文本须在相应准入边界报错；不能退成旧字节比较而返回成功。
5. **兼容边界不扩大。** 原有原生 UCA 的可用能力和拒绝条件保留；旧身份、修订、key 字节、会话默认及历史对象不因本系列升级。

非目标：全局改为 MySQL 8 默认；启用 native general-ci/bin/0900 新修订；更换权重库或 key format；新增迁移/在线重标记能力；新 LIKE/REGEXP 语义；分区、存储过程、UDF、warning 兼容。既有能力不因列入非目标而删除。#25096 的 general-ci、默认切换诉求及更广泛兼容范围不会由本设计关闭。

## 3. 用户可见契约

### 3.1 默认与覆盖矩阵

下表针对当前支持的语法和域，不增加新配置开关：

| 操作 | 预期 |
| --- | --- |
| 默认配置下 `CREATE DATABASE d` | 保留当前 server 默认的有效身份及行为；不是新增 CI 激活入口。管理员已有的显式全局配置仍按原规则生效。 |
| `CREATE DATABASE d CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci` | 库保存 UCA400/修订 1；`SHOW CREATE DATABASE`、SCHEMATA 和数据库变量展示一致。仅 COLLATE 的等价形式也覆盖。 |
| CI 库下 `CREATE TABLE t(v VARCHAR(20))` | 表默认和列类型继承库的已解析域；从别的当前库执行 `CREATE TABLE d.t(...)` 也使用目标库 d。 |
| 表或列显式 COLLATE | 本层声明覆盖上层默认；同层 charset 与 collation 必须兼容，属性书写顺序不影响选择。 |
| 表/列只有显式 CHARACTER SET | 使用当前该 charset 的默认解析规则，不暗中继续继承被覆盖的 CI 默认。`CHARACTER SET binary` 的类型转换与文本 `_bin` 不混同。 |
| `ALTER DATABASE ... COLLATE ...` | 仅影响之后需要继承的定义，不改已有表/列；`ALTER TABLE ... DEFAULT COLLATE` 同理只改表默认。重新声明列的 MODIFY/CHANGE 仍遵守现有规则，不当成纯默认值修改。 |
| `USE d` / `SET NAMES` / 跨库查询 | 不重写已解析列；不把 USE 当成修改 `collation_connection` 的新机制。裸字面量仍按现有 session 绑定；列与字面量比较由 coercibility 决定。 |
| `CREATE TABLE ... LIKE` / CTAS | LIKE 保留源定义；CTAS 的已有输出表达式类型不能被目标库默认覆盖。显式目标列声明走其已有覆盖/转换规则。 |
| prepared CREATE 与并发 ALTER DEFAULT | CREATE 执行时通过已有代际验证重新规划或报错，不发布基于过期默认的表。普通已绑定 SELECT 不因库默认变化重解释其列。 |

`ALTER ... CONVERT` 的现有功能、重建和失败语义保持不变；本系列不增加转换模式。`IF NOT EXISTS` 对已存在库不能借重复执行重新设置默认。

### 3.2 最小示例及独立预期

以下是设计验收示例，不是本轮执行结果；fixture 必须恢复原会话设置并清理创建的库。

```sql
CREATE DATABASE ci_example CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE TABLE ci_example.t (v VARCHAR(20));
INSERT INTO ci_example.t VALUES ('abc'), ('ABC'), ('abc ');
SELECT COUNT(*) FROM ci_example.t WHERE v = 'ABC';
-- 3
SELECT COUNT(DISTINCT v) FROM ci_example.t;
-- 1
SELECT HEX(v) FROM ci_example.t ORDER BY HEX(v);
-- 414243, 616263, 61626320：保存及返回原字节，不返回权重
CREATE TABLE ci_example.raw_t (v VARCHAR(20) COLLATE utf8mb4_bin);
INSERT INTO ci_example.raw_t VALUES ('abc'), ('ABC'), ('abc ');
SELECT COUNT(*) FROM ci_example.raw_t WHERE v = 'ABC';
-- 1：显式旧 bin 覆盖仍生效
DROP DATABASE ci_example;
```

另用最少成对数据验证 CI 的 `café=cafe`、`ß=ss`、组合/分解重音等价与 PAD SPACE。NULL 保持 SQL 三值逻辑；空串、内嵌 NUL 和 binary 分别设置对照：UCA 可以忽略某些字符，权重相等不代表原字节相同，不能断言空串与 NUL 在所有域都不相等。依据是明确指定 `utf8mb4_unicode_ci` 的 MySQL oracle，不是 `LOWER()`。issue 中对 MySQL 默认 0900 的尾空格预期不能直接作为 oracle：0900 为 NO PAD，本提案 UCA400 为 PAD SPACE。

跨域表达式复用现有 [collation_expression.go](../../pkg/sql/plan/collation_expression.go) 的规则：显式 COLLATE 优先，列强于普通字面量；同字符集、同级非显式 `_bin` 的既有优先规则保留；不能解决的冲突报错，不因当前库为 CI 就强转所有输入。显式 COLLATE 是值域转换，不原地修改共享源列节点。

排序只要求 collation 意义的顺序/peer 正确；对 `a`、`A` 等同权值不承诺任意固定原字节先后。#25096 收录的 IF-derived ORDER BY 场景必须覆盖，检查 IF/CASE/CTE/UNION 和常量折叠不丢域，而非在 golden 中规定等价值的偶然顺序。

## 4. 责任边界：复用公共所有者

```text
SQL 显式选项 / 目标库持久默认
  → planner 的一次解析与继承
  → TableDef / column Type / expression provenance
  → 公共比较、key、排序及类型传播
  → 本地算子 / 远端算子 / 已支持的索引消费者

catalog、clone、export、restore
  → 原有 schema 元数据读写与验证
  → 同一已解析域（不是按当前默认重新猜测）
```

| 权威 owner | 职责与允许的局部修复 | 不应引入 |
| --- | --- | --- |
| `pkg/common/collation` | 已有静态名称/identity/revision 与冻结 UCA 权重；一处定义有效能力 | 新全局可变 registry、按库配置的 comparator、第二份别名表 |
| `pkg/sql/plan` | 默认解析/继承、表达式 coercibility、类型保真、元数据代际 | 每个 SQL 入口各写一套 CI 判定；隐式 general-ci 修订升级 |
| `pkg/container/types`、现有比较/key owner | 消费完整已解析类型；公共执行消费者保持同一域 | 在每行查询 catalog、用字符串名称判断算法、用 lowercasing 代替 collation |
| 现有 compile/remote/存储准入 | 复用已存在的 UCA/default/index 能力边界；验证输出/输入和 opaque 类型 | 将旧 PR 的 native132 当成上游能力，或凭 sender 的一次探测绕过 receiver 检查 |
| 原有 schema/catalog owner | 保真读写类型与默认代际；创建/恢复仍使用现有事务、权限和发布边界 | collation 专用 commit-wait、恢复事务、重试/关闭协议 |

派生表达式和向量/计划的 deep copy、序列化、聚合状态、remote 返回、cache 必须保持原有契约。当前 UCA 身份只有已准入的修订 1；现有能够证明保真的 identity helper 不需要为本设计机械改造签名。若验收发现字段丢失，在真实公共转换 owner 补齐，不靠在 snapshot、CDC、View 等调用点分别补丁解决。恢复可以有元数据适配，**局部化不等于免测恢复或丢弃来源信息**。若必须新增持久来源字段、wire 字段或通用恢复机制，停止该实现切片，先更新并评审设计，不将其藏在“小修复”内。

原生 UCA 的现有物理能力不能套用旧 PR 的 blanket rejection。例如基线测试已有 CREATE 新表时的 UCA UNIQUE 去重，以及既有原生表 ADD INDEX 的拒绝断言。保留这些正/负边界。索引探测、回表、剪枝不能返回与扫描不同的结果；不兼容原字节域的优化只能在既有安全策略下保守退出，不能借本系列静默禁用所有索引。

## 5. 持久化、分布式与失败路径

本方案不增加 catalog/wire/key 格式，也不分配新的 MORPC 数字。复用当前数据库默认、UCA、UNIQUE 等独立能力，不能因某个总协议版本够高就省略操作自己的准入。旧 PR 的修订激活、CTAS 新字段和 native132 不进入本方案。

- **创建/变更**：默认值和对象由现有 DDL 事务发布；缓存对照 account/database identity 与代际。失败、取消或代际失效不留下半建对象；DROP/recreate 同名数据库不能复用旧身份。缺行的历史默认与损坏/未知 metadata 区分，不能都 fallback。
- **执行与传输**：同一有效域穿过 binder→fold→plan→vector→remote。未知或不支持的类型在消费/发布前拒绝；已分配的 batch/向量由既有 owner 清理。空批次、NULL-only 和常量不得绕过必要的类型准入。
- **保存与恢复**：已有对象保留 identity/revision，数据库默认只影响未来定义。重启、clone、View、snapshot/PITR、DUMP/restore、CDC 分别验收原有支持范围内的元数据往返和未来写入，不能只断言命令成功。普通 SQL 导出若无法保真，沿用对应 owner 的明确拒绝，不能删字段后继续成功。
- **混合版本/升级**：未达到现有各操作能力要求时拒绝；不以本地开关启用。本系列不扩大旧 binary 可读/可写范围，不把 revision=1 降为 0。正常升级保留旧表/索引字节；真正的新持久能力需要另行设计。
- **回退**：因为没有新格式，回退到基线能力内的版本不新增格式迁移义务；仍必须服从该版本已有的对象/索引兼容下限，不能声称任意旧版本都可读。更改库默认也不是数据降级或索引重建。
- **权限/租户**：仍通过当前账户权限、目标库解析和事务快照读取默认；不能只按数据库名查缓存或使用 sys 会话代替租户身份。错误时不暴露其他租户的默认/表定义。

不引入新 worker、锁、queue、timer、retry 或持久 cache。临时 key/scratch 借用现有执行器预算，原值仍归向量 owner；取消、失败、Reset/Free 不跨查询保留缓存。长度计算必须校验溢出，不能按声明的最大 VARCHAR 宽度逐行填充权重；大值拒绝/内存限制复用现有准入。无需新增逐行日志或高基数指标；诊断使用已有 collation 展示、类型/协议错误和执行计划。

## 6. 方案比较与成本

| 方案 | 好处 | 成本/问题 | 结论 |
| --- | --- | --- | --- |
| 完全不变、不补契约 | 无代码风险 | 用户不知道哪些 CI 名称真正有效；继承与跨算子缺口难以验收 | 不选 |
| **复用已有 UCA400 CI 默认，契约/测试先行** | 复用现有默认、类型、执行与持久 owner；无新格式；缺口可独立修复 | 不解决 general-ci 新语义及全局默认切换；需明确告知范围 | **建议首期** |
| 原生 general-ci 新修订、显式绑定激活 | 直接覆盖原 issue 中 general-ci 示例 | 同名旧对象语义、混合版本、表达式来源、物理 key 和导出要重新闭环；不能只是改 lookup | 若为首期硬需求，先扩展本设计，不沿用旧实现已完成的假设 |
| 将全局默认改为 CI，或给每次比较增加数据库开关 | 看似无需显式选择 | 改变历史结果/唯一性，跨库表达式歧义，执行热路径引入状态查询 | 与评审要求冲突，排除 |

成本预算：纯契约/测试切片不改生产路径；缺陷修复不得新增每行 catalog I/O、同步或后台状态。CI 权重处理成本随输入/权重长度增长，scratch 受执行器内存预算约束，不按行数无界保留。旧路径目标为不新增分配、不改变 key 字节；若需修改公共热路径，做同模式 before/after 基准，覆盖 ASCII、多字节、PAD、长公共前缀和代表性 group/join/order，列出吞吐/内存和扫描回退成本。已有旧 PR 的测试或数字不能充当本系列性能证据。

## 7. 验证矩阵与完成条件

先复用已有 fixture 和场景，再补缺失的语义格子；不为每个 issue 复制嵌入式集群。除独立性能测试外，正常场景只需 0/1/2 个边界值或上述 3 行，不扩大数据量制造概率复现。

| 契约 | 最小证明 / 现有入口 |
| --- | --- |
| 默认未变、显式 CI 和覆盖 | planner 表驱动 UT + `charset_collation_defaults_conversion.sql` 的 metadata/结果对照；无选项、仅 charset、显式 COLLATE、目标库、表/列覆盖各独立命名 |
| CI 关系一致 | 独立成对权重 oracle + 比较/IN/group/distinct/join/order/MIN/MAX 的共享数据；强制不同可用计划，避免一个错误 key helper 同时生成 expected |
| 表达式派生、参数与 fold | 现有 expression/planner UT + `charset_collation_expressions.sql`；IF-derived ORDER BY、CASE/UNION/CTE、prepared 参数和纯字面量对照 |
| 默认代际与失败原子性 | `TestDatabaseCharsetInheritanceContract` 扩展；确定性地控制 prepare/ALTER/execute、权限失败、同名重建，失败后检查对象与默认均未半发布 |
| 物理索引 | 继承 CI 的现有 UNIQUE 正例/1062/NULL、原值回表、扫描对照；保留 ADD INDEX 等已知拒绝及无残留断言，不授权新格式 |
| 远端/opaque 类型 | 现有序列化/consumer UT；有实际跨 CN 行为变化才补最小双 CN SQL，同结果并拒绝不支持 peer |
| 持久往返 | 复用 `database_defaults_restart_test.go`、[恢复集成测试](../../pkg/tests/sqlintegration/database_defaults_recovery_test.go) 和对应现有 clone/View/export/CDC fixture；检查恢复后现有行、默认继承、未来写入和适用的索引等价 |
| 兼容/非法输入/资源 | 旧 revision/key golden、未知 metadata、CI 非法 UTF-8、binary 任意字节、NULL/空 batch、错误后复用和内存清理；当前已支持能力的边界不回退 |

oracle 分工：MO 旧路径与经核对的基线结果比较；命名 UCA400 语义与固定版本 MySQL 比较。实现前固定具体 oracle binary/version 和输出；不能用原 issue 的默认值推测代替。参考 [MySQL 数据库默认规则](https://dev.mysql.com/doc/refman/8.0/en/charset-database.html)、[列默认规则](https://dev.mysql.com/doc/refman/8.0/en/charset-column.html)、[coercibility](https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html) 与 [collation 实现差别](https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-sets.html)。既有 [MO 元数据设计](CLAUDE_issue-29480-collation-metadata.md) 和 [权重选型](issue-28164-collation-selection.md) 仅供历史语义参考，准入现状以本节源码基线为准。

每个后续切片的完成条件：非空精确测试选择；被改公共 owner 的 UT/实际 consumer；SQL 变化有正常比较模式 BVT；`.result` 仅工具生成并独立审查；同实例两轮与清理后置检查；只有触及的协议/持久/共享状态维度增加 topology/restart/race 证明。生产改动的自身变更语句覆盖率至少 75%，不是要求测试-only 或本设计跑 Go。通过测试不替代维护者设计决定。

本设计 PR 只做文档一致性、源码引用、链接及交付范围检查，**不宣称 UCA 全链路、原 issue 或旧实现已重新验证**。

## 8. 小 PR 交付与止损边界

| 切片 | 前置条件 | 独立交付内容 | 禁止混入 |
| --- | --- | --- | --- |
| D：本设计 | 无 | 决定首期命名 CI、默认契约、公共 owner 与验收矩阵；合并后才开始下面工作 | 实现、测试代码、旧分支 merge |
| T：默认与语义契约测试 | D 获批且合并 | 在当时 main 验证第 3/7 节，优先补复用现有 fixture 的通过场景；给确认缺陷提供最小复现 | 修改默认值、扩大准入、提交故意失败的 CI 测试 |
| F：经确认的局部缺陷 | T 定位的证据 + D 范围内 | 每个行为闭包独立提交：公共 owner 修复 + 反例 + 最近控制 + 必要 consumer/SQL 验证；无缺陷则没有该 PR | 全量旧实现、无关事务修复、为维持旧 PR 的冲突修复 |
| A：验收与使用说明 | 所有必要 F 已合并 | 汇总有效 CI 名称、明确限制、默认继承示例及源码对应验收；复用已有效证据，不重跑无关套件 | 声称 general-ci/default-switch 已完成或自动关闭 #25096 |
| X：general-ci 或新持久/物理能力 | 单独设计决定 | 若需要，另立小范围设计与 issue；不是 D 的隐含批准 | 将未批准扩展作为 F 偷渡 |

[#31120](https://github.com/matrixorigin/matrixone/issues/31120) 与本表无依赖；需要交付时另提针对当时 main 的独立修复 PR。本轮不开始该修复。旧 #30684 保留为参考，不恢复其维护循环。

切片按行为闭包而非文件数拆分：不能先开放入口、以后再补持久/远端安全。一个缺陷若要求新增协议/格式、改事务边界或扩成跨恢复体系重构，先停止 F、更新设计并重新评审；不能用“小 PR”标签绕过设计门。

## 9. 必须在设计合并前完成的决定

建议由 fengttt 确认产品方向、适用模块维护者确认职责边界；这是评审请求，不声明任何人已接受责任或批准。

| 决定 | 推荐值 | 状态 |
| --- | --- | --- |
| D1：首期 “CI” 的确切含义 | 复用 `utf8mb4_unicode_ci` / UCA400 / PAD SPACE；general-ci 另行设计，本系列不关闭整个 #25096 | **待确认，阻塞设计批准** |
| D2：默认兼容 | 保持当前默认行为、名称及旧对象语义，不统一改成 MySQL 8 默认，也不新增 C/CI SQL 名称 | **待确认，阻塞设计批准** |
| D3：实现/测试边界 | 接受 T→必要 F→A；事务独立；持久消费者做保真验收而不复制恢复机制；新格式须再设计 | **待确认，阻塞设计批准** |

审批记录必须链接本文件的实际评审 commit 和决定；合并前关闭上述问题并更新状态。后续实现 PR 引用该合并版本。**本草案、自审和旧 PR 的测试都不是设计批准，也不是提前实施许可。**

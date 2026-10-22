# S4 范围对账:#29439 关闭与 #30578 交付的一致性记录

本记录回答 [QA 跟进 #30668](https://github.com/matrixorigin/matrixone/issues/30668) 的两个问题,并补齐 S1 设计要求的 S4 关闭性归类审计。设计依据为 [S1](CLAUDE_20260922-view-metadata-on-demand.md)([S1 批准记录](CLAUDE_20260928-view-metadata-s1-review.md))、[S2 v2.1 修订](CLAUDE_20261005-view-s2-scope-revision.md)、[S3 消费者实现与审计](CLAUDE_20261018-view-s3-consumers.md)(#30551 随附)与 [S4 消费者接线说明](CLAUDE_20261019-view-metadata-s4-consumers.md)(#30578 随附)。

源码核查基线:`c0e826888b`(mo/main,2026-10-21;首轮提交基线为 `76e152a1dd`,修订见第 8 节)。本记录只做源码核查与范围对账,不声称任何新的运行时证据,也不重新声称 #30551/#30578 已报告的测试证据。

## 1. 范围确认(问题 1)

**#30578 的意图是关闭 S4 全部的 opt-in 消费者接线,而不是只完成一项 S2 附带工作。** 但它正确声明了两条边界:默认 SQL 行为不变、不激活完整 v2 rollout、不吸收 S3。判断 S4 是否"关闭",对照物应是 [#29439](https://github.com/matrixorigin/matrixone/issues/29439) 的交付项和验收标准,以及 S1 第 15 节为 S4 划定的消费者范围(C04–C07、C11–C13、C16、C17;S1 正文第 382 行),而不是把 S3/S5 的内容算进来。

对账结论:**S4 范围内没有发现未迁移的必需消费者;#29439 除两项关闭性记录外均已交付。** 两项缺失的关闭性记录是:

1. S1 正文第 66 行要求(S3/S4 关闭时须给出最终源码搜索的每个 Cols/mo_columns 消费者归类)——本记录第 4 节补齐。
2. D03 raw-catalog 策略落实的显式声明(物理语义已文档化于 S4 接线说明"raw catalog 与逻辑元数据的区别"一节,但 #30578 PR 描述未声明策略本身)——本记录第 3 节补齐。

## 2. S4 交付项与验收标准逐条对照

引用均为基线 `c0e826888b` 的源码位置。

### 交付项

| #29439 交付项 | 交付状态与位置 |
| --- | --- |
| Update DESC/SHOW COLUMNS, information_schema.columns, and subscription-column metadata | 已交付。`buildShowColumns`/`buildShowColumnNumber`/`buildShowTableValues`([build_show.go](../../pkg/sql/plan/build_show.go) L639/L483/L556)经 `isUserViewMetadata`(L626)对普通 View 走 `describeViewForMetadata` / `viewDescriptionRelation`([view_description.go](../../pkg/sql/plan/view_description.go) L109/L177);I_S COLUMNS 当前模板由 `informationSchemaDerivedColumnsDDL`([predefined.go](../../pkg/util/sysview/predefined.go) L382)生成,用户 View 行从持久 `mo_columns` 分支剔除并改走 `cross apply mo_view_columns(...)`;订阅 View 行走 `__mo_visible_subscription_views cross apply mo_subscription_view_columns(...)`(L511)。opt-in 语句经 `TxnCompilerContext.NewViewMetadataRequest`([view_schema_provider.go](../../pkg/frontend/view_schema_provider.go) L68)共享 S2 请求,三参数签名在 [query_builder.go](../../pkg/sql/plan/query_builder.go) L14402 接线,由 `ViewMetadataColumnsDDL`(L338)切换 |
| Cover additional protocol/internal introspection consumers identified by S1 | 已覆盖 S4 部分:C11 COM_FIELD_LIST 保持未支持(回归控制,`doCmdFieldList` 列枚举自始为注释);C12 SHOW CREATE 保持持久定义(`buildShowCreateView` L205),无效 View 仍可导出;C13 DUMP TABLE 保持拒绝 View(`validateTableDumpSchema`,[table_dump.go](../../pkg/frontend/table_dump.go) L524)。C02/C03/C08–C10/C14 归 S3(#29438),不在 S4 承诺内;S3 已由 #30551 交付关闭(见第 5 节) |
| Keep base-table metadata reads on their existing efficient path | 已保持。普通表/系统 View 仍走原持久列分支(derived DDL 仅对 `relkind='v'` 且非系统库的普通用户 View 走推导(L395 的 `userView` 谓词),系统 View 固定列路径不变);SHOW COLUMNS/COLUMN_NUMBER 普通表分支仍直接读 `mo_catalog.mo_columns`(build_show.go L763/L765/L541/L544),普通表持久列即物理权威;`buildTableDefFromMoColumns`([util.go](../../pkg/frontend/util.go) L2991)对普通表保留物理快径 |
| Implement the approved mo_catalog.mo_columns compatibility/adapter policy | 策略本体即 S1 D03(设计第 110 行):raw mo_columns 保持物理快照语义,不虚拟化;要求当前列的内部调用者迁出。落实=物理契约文档化(S4 接线说明)+ 内部调用者全部迁出(第 4 节归类审计无遗漏)。**第 3 节给出显式声明** |
| Preserve predicate/projection/JOIN/aggregation/ORDER/LIMIT semantics; push safe filtering before derivation | 已交付。候选筛选/行列可见性/JOIN/聚合/ORDER BY/LIMIT 由原关系计划执行;订阅分支在 APPLY 左侧以 `__mo_visible_subscription_views` CTE 收缩候选,避免不可见 View 消耗预算(derived DDL 注释,L399–404);数值签名/可跳过错误判定在 [view_columns.go](../../pkg/sql/colexec/table_function/view_columns.go) |
| Apply approved visibility/authorization and row-column rewrite rules; no privilege escalation | 已交付。授权由 opt-in 调用方注入(`NewViewMetadataRequest(ctx, authorize)`),不缓存授权决定;订阅侧 publisher 身份定义绑定、subscriber 名称展示,授权谓词见 `informationSchemaSubscriptionColumnAuthorizationPredicate`(L288);私有 TVF 所有者白名单 `subscriptionMetadataOwnerAllowed`([subscription_metadata.go](../../pkg/sql/plan/subscription_metadata.go) `subscriptionMetadataOwnerAllowed`)限制为 information_schema 系统模板 |
| Define and implement single-object vs bulk invalid-view behavior | 已交付。单对象描述失败即报错;批量扫描仅对 binder 确证的缺库/表/列错误跳过(`skippableViewDescriptionError`,view_columns.go),授权/provider/根查找/取消/协议/预算错误保持 fatal,并沿用既有 ER_VIEW_INVALID warning |
| Bound batches, derived objects, nested work, response memory | 已交付。`maxViewDescriptionsPerScan`、`MaxViewMetadataColumns`、opt-in TVF 页 1024 行/1 MiB/单行 16 MiB(view_columns.go);APPLY 保留源批次边界(`ApplyPreserveBatch`,[apply/apply.go](../../pkg/sql/colexec/apply/apply.go) L72);S2 请求级 memo/工作量/截止时间沿用 |

### 验收标准

| #29439 验收标准 | 状态 |
| --- | --- |
| DESC/SHOW, information_schema, subscription surfaces, and the approved raw-catalog interface agree with planner output under equivalent schema visibility | 前三个表面:#30578 报告的 native SQL 集成验证覆盖(本记录不重新声称)。"approved raw-catalog interface"按 D03 的批准含义是**物理快照接口**,其与 planner 输出的一致性承诺被 D03 明确排除并以逻辑入口替代;该替代已文档化(S4 接线说明)。验收中的 raw 项按批准文本成立,不需要第四份推导表面 |
| An unrelated invalid or invisible view cannot affect a narrowly scoped metadata query | 已由授权先行筛选 + 可跳过错误白名单覆盖(#30578 native SQL 集成与 UT;skip 判定见 view_columns.go) |
| Public SQL and unit tests cover filtered/full-schema enumeration, composite SQL shapes, invalid views, empty results, authorization/rewrite visibility, cancellation, bounded allocation | #30578 已随附(plan/table_function/apply/frontend UT 与 native SQL 集成);现存回归:`TestViewMetadataShowConsumersRejectInvalidView`、`TestViewMetadataShowConsumersTrackSourceAndTarget` 等([view_metadata_consumers_test.go](../../pkg/sql/plan/view_metadata_consumers_test.go)) |
| No metadata read writes lifecycle state or starts a view data query; activation remains gated | 描述路径只读(view_columns.go TVF 无写路径);默认行为不变,opt-in 由语句调用者显式创建请求,无 session variable/持久协议/全局开关(S4 接线说明"激活及所有权") |

## 3. D03 raw-catalog 策略落实声明

S1 D03(经 S1 批准记录特别条款 1 批准)选择:**直接 SELECT `mo_catalog.mo_columns`(含投影、JOIN、聚合、快照查询及用户在其上建立的 View)保持物理目录/MVCC 语义;不虚拟化,不恢复持久刷新;View 行是创建/历史维护时刻的列快照,源 DDL 后可能陈旧。** 需要当前列的调用者必须使用逻辑入口(SHOW COLUMNS / information_schema.columns)。

落实状态:

1. **物理契约文档化**:S4 接线说明"raw catalog 与逻辑元数据的区别"一节给出同一 `v2 -> v1 -> t` 例子的两类查询语义,并声明描述读取不持久刷新 `mo_columns`、不排队修复、不执行 View 数据查询。
2. **内部当前列消费者已全部迁出**:第 4 节对基线的最终源码归类中,不存在仍以持久 View 列充当当前列权威的被要求消费者。
3. **不声称刷新**:描述路径不写回推导结果(与 D03"不恢复持久刷新"一致);`mo_columns` View 行的陈旧性由 D03 契约显式覆盖。
4. **#26227 替代已闭环**:D03 要求同步认可的持久化验收替代已由 #26227 owner 于 2026-09-30 的 scope clarification 确认(按需推导替代持久/缓存列),2026-10-09 完成回归验证,#26227 于 2026-10-10 关闭。本记录不代做任何 issue 编辑,仅登记既有决策。

## 4. 最终消费者归类审计(S1 关闭性 artifact)

方法:在 `c0e826888b` 上执行以下搜索,并对**每个命中文件**逐一人工核对调用点语义(反引号、常量拼接与未限定表名的命中会被逐文件覆盖,不再只依赖精确字面模式):

```
rg -ln "mo_columns|MO_COLUMNS" --type go
rg -n "select.*mo_columns|FROM .*mo_columns" --type go
rg -n "BuildTableDefByMoColumns" --type go
rg -ln "SystemViewRel" pkg/frontend/*.go pkg/sql/plan/build_show.go
rg -n "getTableColumnDefSql|extractTableDefColumns|resolveTableVisibleColumns" --type go
```

### A. `mo_catalog.mo_columns` 可执行查询调用点(全部,含物理兼容)

| 调用点 | 用途 | 归类 |
| --- | --- | --- |
| [authenticate.go](../../pkg/frontend/authenticate.go) L1647 `getTableColumnDefFormat`(经 L2113 `getTableColumnDefSql` → util.go L2991 `buildTableDefFromMoColumns`) | 普通表 columns-only 快径;L3026 起对 View 只返回 kind,丢弃持久列,由 planner 走正常 View 绑定 | C16→D 守卫;普通表物理快径保留;该接口在本基线上无生产调用方,C14 处置由 S3 记录为保留的历史物理 helper(见表 B 末行) |
| [build_show.go](../../pkg/sql/plan/build_show.go) L763/L765/L769/L771(SHOW COLUMNS/FULL 普通表分支)、L541/L544(SHOW COLUMN_NUMBER 普通表分支) | 普通表列读取直接取自 `mo_catalog.mo_columns`;持久列即普通表物理权威 | C04 的普通表物理路径,View 由 `isUserViewMetadata` 改写数据源(L783) |
| [build_show.go](../../pkg/sql/plan/build_show.go) L318(`buildShowSequences`,L309 起)、L1053(`buildShowIndex`,L995 起) | SHOW SEQUENCES 读取序列值列类型(`relkind='s'`,序列非 View);SHOW INDEX 经 mo_indexes 左连 mo_columns 取列属性(attnotnull/hidden/generated),仅普通表存在索引行,View 无输出 | C17 物理兼容(S1 明确归属;序列/索引与普通表元数据不因本系列变成 View 输出) |
| [predefined.go](../../pkg/util/sysview/predefined.go) L277 `informationSchemaColumnsLocalFromSQL`(及模板内的索引/约束源:L529 KEY_COLUMN_USAGE 经 mo_indexes,L825 STATISTICS 经 mo_indexes 连接 mo_columns tcl) | I_S 系统模板中的持久列/索引源;用户 View 行由 derived DDL 剔除,普通表/系统 View 持久列即权威;View 在 mo_indexes 无行,不产生 STATISTICS/KCU 输出 | C06/C17 物理权威(普通表/系统 View) |
| [subscription_metadata.go](../../pkg/sql/colexec/table_function/subscription_metadata.go) L787 `buildSubscriptionColumnsQuery`(`mo_subscription_columns` TVF) | 订阅发布方普通表行源;I_S COLUMNS 订阅分支对 `relkind='v'` 非系统库用户 View 显式剔除(derived DDL) | C07 普通表部分保留;View 已迁出至 `mo_subscription_view_columns` |
| [publication_subscription.go](../../pkg/frontend/publication_subscription.go) L3388 `checkColExists` | sys 账户 catalog 表列存在性检查(升级兼容),非租户 View 行 | C15 物理兼容 |
| [snapshot.go](../../pkg/frontend/snapshot.go) L2729 `getCreateSequenceSQL` | restore/clone 按快照 TS 读取序列值列类型以重建 CREATE SEQUENCE;序列为物理对象,非 View | C15 物理兼容 |
| [cdc/sql_builder.go](../../pkg/cdc/sql_builder.go) L347/L358 `CDCCollectTableInfoSqlTemplate`(`collectTableInfoSQL` 以 `catalog.SystemOrdinaryRel` 过滤,L1356) | CDC 源候选表的用户主键探测(EXISTS 子查询);候选集按 `relkind='r'` 限定普通表,View 不参与 CDC 复制 | 物理兼容(同 C16 分类;View 由 relkind 排除) |
| [incrservice/store_sql.go](../../pkg/incrservice/store_sql.go) L536 | RESET 归属校验:按 rel_id 统计 hidden fake-PK 列;自增分配仅存在于普通表 | 物理兼容(同 C16 分类;仅普通表物理记录) |
| [vectorindex/sqlexec/metadata_shape.go](../../pkg/vectorindex/sqlexec/metadata_shape.go) L72 `HasProvenanceColumns`(L97 执行 mo_columns count 探测) | per-index 内部元数据表的形状探测(CREATE/REINDEX/CDC 写路径混合版本窗口);探测对象为索引内部物理表,非 View | 物理兼容(同 C16 分类;仅索引内部物理表) |
| [bootstrap/versions](../../pkg/bootstrap/versions):v4_0_0 `upg_mo_columns_add_attr_*` 升级项、v4_0_6 升级清单、[v4_0_7](../../pkg/bootstrap/versions/v4_0_7/index_metadata_provenance.go) L101 `hasIndexMetadataColumn`、upgrade_strategy.go L344/L359 版本列校验 | 版本升级中的 catalog 结构检查与迁移(ALTER/存在性计数),sys 或结构层,不读取租户 View 列作为当前权威 | C15 物理兼容 |
| [txn_database.go](../../pkg/vm/engine/disttae/txn_database.go)、[logtail_consumer.go](../../pkg/vm/engine/disttae/logtail_consumer.go)、disttae cache/db、tae/logtail、catalog tuplesParse/tuplesGen | mo_columns 物理复制、日志回放与引擎记录编码(含 FIND_TABLE 诊断) | C15 物理兼容 |
| [tools/checkpointtool](../../pkg/tools/checkpointtool/table_dump.go)、[cmd/mo-object-tool](../../cmd/mo-object-tool/ckp/checkpoint.go) | 离线工具从 checkpoint 的 mo_columns 记录重建表 schema | C15 物理兼容(离线记录层) |

名称常量/谓词/注释引用(无可执行 View 列读取,非消费者):[defines/type.go](../../pkg/defines/type.go) L341、build_show.go L389/465/535/757/835(catalog 名过滤谓词)、[util/executor/options.go](../../pkg/util/executor/options.go) L318、[sql/util/util.go](../../pkg/sql/util/util.go) L230、[plan/function/func_mo.go](../../pkg/sql/plan/function/func_mo.go) L802/L978、[sql/plan/dml_context.go](../../pkg/sql/plan/dml_context.go) L30、snapshot.go L109(restore 跳过清单)、util.go L3020(注释)。

### B. View 列/定义消费者(S4 范围)

| 消费者 | 调用点 | 归类 |
| --- | --- | --- |
| DESC / SHOW [FULL] COLUMNS、SHOW COLUMN_NUMBER、SHOW TABLE_VALUES | build_show.go L639/L483/L556 | C04/C05 → D(`isUserViewMetadata` 守卫;系统库 View 固定持久列) |
| I_S COLUMNS(本地,当前模板) | predefined.go L382 | C06 → D(用户 View 行剔除持久源,cross apply 推导) |
| I_S COLUMNS(订阅 View) | predefined.go L511 | C07 → D(可见订阅 View CTE + cross apply;publisher 身份/subscriber 名称) |
| I_S COLUMNS 历史模板 | predefined.go L312 `AdaptLegacyInformationSchemaColumnsDDL`(V41/V46/V58 仅改列权威;V100/V109 冻结) | C07 → D 只读适配 |
| COM_FIELD_LIST | `doCmdFieldList`(mysql_cmd_executor.go L1219) | C11 回归控制:列未枚举,未虚报支持 |
| SHOW CREATE TABLE/VIEW、定义导出 | build_show.go L119/L205 | C12 持久定义,无需推导成功 |
| DUMP TABLE / 查询结果导出 | table_dump.go L524;query_result.go | C13 View 拒绝保持;结果导出用当次描述 |
| ANALYZE 可见列 | `resolveTableVisibleColumns`(mysql_cmd_executor.go L2561),`bindAnalyzeTables` 拒绝 View | C17 物理统计消费者,不支持范围未扩 |
| I_S TABLES/VIEWS/STATISTICS/KEY_COLUMN_USAGE 等 | predefined.go L529/L825 等(STATISTICS 订阅分支由 [subscription_metadata.go](../../pkg/sql/plan/subscription_metadata.go) 提供可见性与预算守卫) | C17 身份/定义字段;View 无索引/约束行,不虚构源表元数据 |
| SHOW PUBLICATION 表清单 | publication_subscription.go L2399(`relkind != 'v'` 名称列表) | C17 身份字段 |
| restore/clone/PITR、snapshot restore | snapshot.go L2594/L2729/L2808、snapshot_catalog_restore.go | C15 物理记录,目标侧按其 schema/权限重新绑定(S5/S9) |
| `BuildTableDefByMoColumns` 接口 | frontend/compiler_context.go L1782、compile/sql_executor_context.go L165;本基线上无生产调用方,仅测试替身 | C14 已由 #30551 处置:S3 消费者文档第 109 行记录为"历史物理 helper;当前 query binder 不调用,不恢复快路",保留且非当前权威 |

S3 所属消费者(C02 查询/嵌套 View、C03 CTAS/default、C08 计划缓存、C09 PREPARE/reprepare、C10 cursor、C14)不在本表展开;S3 #29438 已由 #30551 交付关闭,C14 处置见表 B 末行。

## 5. 残余项与跟踪(问题 2)

| 事项 | 归属 | 状态与处置 |
| --- | --- | --- |
| planner/CTAS/计划缓存/PREPARE/cursor 派生元数据迁移(C02/C03/C08–C10/C14) | S3 [#29438](https://github.com/matrixorigin/matrixone/issues/29438)(已关闭,2026-10-21) | 已由 [#30551](https://github.com/matrixorigin/matrixone/pull/30551)(2026-10-21 合入)交付:SELECT/CTAS/文本及类型化 PREPARE 的显式 opt-in 派生查询规划,持久列不再决定该路径的 admission/binding/CTAS 元数据;**默认 SQL、PREPARE 与 cursor 路由不变**,不声称默认 binary/cursor 激活或 S5/S6/S8/S9 门禁;完整消费者审计与 C14 处置记录于 S3 消费者文档 |
| #26227 持久化验收的 D03 替代认可 | [#26227](https://github.com/matrixorigin/matrixone/issues/26227)(已关闭,2026-10-10) | 已闭环:owner 2026-09-30 scope clarification 确认以按需推导替代持久/缓存列验收,2026-10-09 回归验证通过;本记录仅登记,未代做编辑 |
| 跨组件/快照/恢复/混合版本证据 | S5 [#29440](https://github.com/matrixorigin/matrixone/issues/29440)(open) | 其前置依赖 #29438 与 #29439(+本记录)均已交付;剩余为 S5 自身范围,由 #29440 正常推进 |

**#29440 的前置依赖状态**:#29438 已由 #30551 关闭;#29439 的消费者范围由本记录完成最终归类与对账。本记录合入后,#29440 不再因 S4 侧缺记录而受阻,剩余门禁为第 5 节第三行所列的 S5 自身范围。

## 6. 证据与非证据

- 已执行:第 4 节所列 `rg` 源码搜索(基线 `c0e826888b`)及每个命中文件的人工语义核对(含反引号、常量拼接、未限定表名字面);全部引用行号在本基线上复核;`git diff --check`;文档结构检查(表格列数一致、代码围栏闭合、相对链接存在)。
- 未执行:Go UT/BVT/race/多 CN/混合 binary/恢复测试——本次仅文档,无生产或测试代码改动;#30551/#30578 已报告的验证证据本记录不重复声称。
- 本记录不批准或宣称 S5–S9 的任何门禁已通过;除登记既有决定外,不改变任何 issue 状态,未编辑 #26227、#29005/#29006/#29007。

## 7. 结论

回答 #30668:

1. **#30578 意图为关闭 S4 全部 opt-in 消费者接线**(C04–C07、C11–C13、C16、C17),其"S2 opt-in"表述指复用 S2 请求边界而非缩小 S4 范围;"默认行为不变/不激活 rollout/不吸收 S3"是正确边界声明,与 S4 交付不矛盾。
2. **S4 无未交付的代码范围**:D03 raw-catalog 策略的批准含义即"物理语义 + 内部调用者迁出",二者均已落实(第 3、4 节)。剩余事项按批准路线归属:S3 范围已由 #30551 关闭;#26227 的 D03 替代已由 owner 闭环;#29440 的前置依赖随本记录合入而满足,仅剩其自身范围。

## 8. 修订记录

- **R1(2026-10-21)** 依据 PR #30739 审查意见修订:
  1. 首轮将 `metadata_shape.go` 误判为仅注释引用,并漏列 CDC、自增存储、v4_0_7 等 mo_columns 读取点;根因是未对 broad sweep 的全部命中文件逐一核对,字面模式未覆盖反引号、常量拼接与未限定表名。本版对 `rg -ln "mo_columns|MO_COLUMNS" --type go` 的每个命中文件完成人工分类,补齐物理兼容读者(表 A)并修正该误分类;以上读者均已核实早于首轮基线合入。
  2. 更新已交付事实:S3 #29438 已由 #30551 交付并于 2026-10-21 关闭;#26227 已于 2026-10-10 关闭,D03 替代认可闭环;残余项收缩为 S5 #29440 自身范围。
  3. 核查基线由 `76e152a1dd` 更新为 `c0e826888b`,文档内全部源码行号在新基线上重新校验。

- **R2(2026-10-21)** 依据 PR #30739 审查意见补齐表 A 两条 C17 物理兼容路径:SHOW SEQUENCES(build_show.go L318,序列值列类型,序列非 View)与 SHOW INDEX(L1053,mo_indexes 左连 mo_columns,仅普通表存在索引行);同时将 SHOW COLUMNS FULL 变体(L769/771)并入原 SHOW COLUMNS 行,并把 build_show.go 中其余 `mo_columns` 字面(名过滤谓词)登记为非消费者。

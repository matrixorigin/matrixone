# S4 范围对账:#29439 关闭与 #30578 交付的一致性记录

本记录回答 [QA 跟进 #30668](https://github.com/matrixorigin/matrixone/issues/30668) 的两个问题,并补齐 S1 设计要求的 S4 关闭性归类审计。设计依据为 [S1](CLAUDE_20260922-view-metadata-on-demand.md)([S1 批准记录](CLAUDE_20260928-view-metadata-s1-review.md))、[S2 v2.1 修订](CLAUDE_20261005-view-s2-scope-revision.md) 与 [S4 消费者接线说明](CLAUDE_20261019-view-metadata-s4-consumers.md)(#30578 随附)。

源码核查基线:`76e152a1dd`(mo/main,2026-10-20)。本记录只做源码核查与范围对账,不声称任何新的运行时证据,也不重新声称 #30578 已报告的测试证据。

## 1. 范围确认(问题 1)

**#30578 的意图是关闭 S4 全部的 opt-in 消费者接线,而不是只完成一项 S2 附带工作。** 但它正确声明了两条边界:默认 SQL 行为不变、不激活完整 v2 rollout、不吸收 S3。判断 S4 是否"关闭",对照物应是 [#29439](https://github.com/matrixorigin/matrixone/issues/29439) 的交付项和验收标准,以及 S1 第 15 节为 S4 划定的消费者范围(C04–C07、C11–C13、C16、C17;S1 正文第 382 行),而不是把 S3/S5 的内容算进来。

对账结论:**S4 范围内没有发现未迁移的必需消费者;#29439 除两项关闭性记录外均已交付。** 两项缺失的关闭性记录是:

1. S1 正文第 66 行要求(S3/S4 关闭时须给出最终源码搜索的每个 Cols/mo_columns 消费者归类)——本记录第 4 节补齐。
2. D03 raw-catalog 策略落实的显式声明(物理语义已文档化于 S4 接线说明"raw catalog 与逻辑元数据的区别"一节,但 #30578 PR 描述未声明策略本身)——本记录第 3 节补齐。

## 2. S4 交付项与验收标准逐条对照

引用均为基线 `76e152a1dd` 的源码位置。

### 交付项

| #29439 交付项 | 交付状态与位置 |
| --- | --- |
| Update DESC/SHOW COLUMNS, information_schema.columns, and subscription-column metadata | 已交付。`buildShowColumns`/`buildShowColumnNumber`/`buildShowTableValues`([build_show.go](../../pkg/sql/plan/build_show.go) L639/L483/L556)经 `isUserViewMetadata`(L626)对普通 View 走 `describeViewForMetadata` / `viewDescriptionRelation`([view_description.go](../../pkg/sql/plan/view_description.go) L109/L177);I_S COLUMNS 当前模板由 `informationSchemaDerivedColumnsDDL`([predefined.go](../../pkg/util/sysview/predefined.go) L370)生成,用户 View 行从持久 `mo_columns` 分支剔除并改走 `cross apply mo_view_columns(...)`;订阅 View 行走 `__mo_visible_subscription_views cross apply mo_subscription_view_columns(...)`(L499)。opt-in 语句经 `TxnCompilerContext.NewViewMetadataRequest`([view_schema_provider.go](../../pkg/frontend/view_schema_provider.go) L68)共享 S2 请求,三参数签名在 [query_builder.go](../../pkg/sql/plan/query_builder.go) L14287 接线,由 `ViewMetadataColumnsDDL`(L326)切换 |
| Cover additional protocol/internal introspection consumers identified by S1 | 已覆盖 S4 部分:C11 COM_FIELD_LIST 保持未支持(回归控制,`doCmdFieldList` 列枚举自始为注释);C12 SHOW CREATE 保持持久定义(`buildShowCreateView` L205),无效 View 仍可导出;C13 DUMP TABLE 保持拒绝 View(`validateTableDumpSchema`,[table_dump.go](../../pkg/frontend/table_dump.go) L523)。C02/C03/C08–C10/C14 明确归 S3(#29438),不在 S4 承诺内 |
| Keep base-table metadata reads on their existing efficient path | 已保持。普通表/系统 View 仍走原持久列分支(derived DDL 仅对 `relkind='v'` 且非系统库的普通用户 View 走推导(L383 的 `userView` 谓词),系统 View 固定列路径不变);`buildTableDefFromMoColumns`([util.go](../../pkg/frontend/util.go) L2991)对普通表保留物理快径 |
| Implement the approved mo_catalog.mo_columns compatibility/adapter policy | 策略本体即 S1 D03(设计第 110 行):raw mo_columns 保持物理快照语义,不虚拟化;要求当前列的内部调用者迁出。落实=物理契约文档化(S4 接线说明)+ 内部调用者全部迁出(第 4 节归类审计无遗漏)。**第 3 节给出显式声明** |
| Preserve predicate/projection/JOIN/aggregation/ORDER/LIMIT semantics; push safe filtering before derivation | 已交付。候选筛选/行列可见性/JOIN/聚合/ORDER BY/LIMIT 由原关系计划执行;订阅分支在 APPLY 左侧以 `__mo_visible_subscription_views` CTE 收缩候选,避免不可见 View 消耗预算(derived DDL 注释,L387–392);数值签名/可跳过错误判定在 [view_columns.go](../../pkg/sql/colexec/table_function/view_columns.go) |
| Apply approved visibility/authorization and row-column rewrite rules; no privilege escalation | 已交付。授权由 opt-in 调用方注入(`NewViewMetadataRequest(ctx, authorize)`),不缓存授权决定;订阅侧 publisher 身份定义绑定、subscriber 名称展示,授权谓词见 `informationSchemaSubscriptionColumnAuthorizationPredicate`(L276);私有 TVF 所有者白名单 `subscriptionMetadataOwnerAllowed`([subscription_metadata.go](../../pkg/sql/plan/subscription_metadata.go) `subscriptionMetadataOwnerAllowed`)限制为 information_schema 系统模板 |
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
2. **内部当前列消费者已全部迁出**:第 4 节对基线 `76e152a1dd` 的最终源码归类中,不存在仍以持久 View 列充当当前列权威的被要求消费者。
3. **不声称刷新**:描述路径不写回推导结果(与 D03"不恢复持久刷新"一致);`mo_columns` View 行的陈旧性由 D03 契约显式覆盖,#26227 的对账见第 5 节(属 owner 级动作)。

## 4. 最终消费者归类审计(S1 关闭性 artifact)

方法:在 `76e152a1dd` 上执行(结果均另行人工核对调用点语义):

```
rg -ln "MoColumnsAllQueryFormat|mo_catalog\.mo_columns|FROM mo_catalog\.mo_columns" --type go
rg -ln "mo_columns" --type go
rg -n "BuildTableDefByMoColumns" --type go
rg -ln "SystemViewRel" pkg/frontend/*.go pkg/sql/plan/build_show.go
rg -n "getTableColumnDefSql|extractTableDefColumns|resolveTableVisibleColumns" --type go
```

### A. raw `mo_catalog.mo_columns` SQL 直读者(全部)

| 调用点 | 用途 | 归类 |
| --- | --- | --- |
| [authenticate.go](../../pkg/frontend/authenticate.go) L1647 `getTableColumnDefFormat`(经 L2113 `getTableColumnDefSql` → util.go L2991 `buildTableDefFromMoColumns`) | 普通表 columns-only 快径;L3026 起对 View 只返回 kind,丢弃持久列,由 planner 走正常 View 绑定 | C16→D 守卫;普通表物理快径保留;该接口在本基线上无生产调用方,最终处置归 S3/C14 |
| [publication_subscription.go](../../pkg/frontend/publication_subscription.go) L2965 `checkColExists` | sys 账户 catalog 表列存在性检查(升级兼容),非租户 View 行 | C15 物理兼容 |
| [subscription_metadata.go](../../pkg/sql/colexec/table_function/subscription_metadata.go) L787 `buildSubscriptionColumnsQuery`(`mo_subscription_columns` TVF) | 订阅发布方普通表行源;I_S COLUMNS 订阅分支对 `relkind='v'` 非系统库用户 View 显式剔除(derived DDL) | C07 普通表部分保留;View 已迁出至 `mo_subscription_view_columns` |
| [txn_database.go](../../pkg/vm/engine/disttae/txn_database.go)、[logtail_consumer.go](../../pkg/vm/engine/disttae/logtail_consumer.go) | mo_columns 物理复制/日志消费(含 FIND_TABLE 诊断),引擎记录层 | C15 物理兼容 |
| [pkg/bootstrap/versions/v4_0_6](../../pkg/bootstrap/versions/v4_0_6)、upgrade_strategy.go | 升级脚本/租户升级清单中的 catalog 结构检查与系统 View 模板版本控制 | C15 物理兼容(版本门) |
| [pkg/vectorindex/sqlexec/metadata_shape.go](../../pkg/vectorindex/sqlexec/metadata_shape.go) L39、[pkg/defines/type.go](../../pkg/defines/type.go) L341 | 注释引用 mo_columns,无可执行直读 | 非消费者 |

### B. View 列/定义消费者(S4 范围)

| 消费者 | 调用点 | 归类 |
| --- | --- | --- |
| DESC / SHOW [FULL] COLUMNS、SHOW COLUMN_NUMBER、SHOW TABLE_VALUES | build_show.go L639/L483/L556 | C04/C05 → D(`isUserViewMetadata` 守卫;系统库 View 固定持久列) |
| I_S COLUMNS(本地,当前模板) | predefined.go L370 | C06 → D(用户 View 行剔除持久源,cross apply 推导) |
| I_S COLUMNS(订阅 View) | predefined.go L499 | C07 → D(可见订阅 View CTE + cross apply;publisher 身份/subscriber 名称) |
| I_S COLUMNS 历史模板 | predefined.go L300 `AdaptLegacyInformationSchemaColumnsDDL`(V41/V46/V58 仅改列权威;V100/V109 冻结) | C07 → D 只读适配 |
| COM_FIELD_LIST | `doCmdFieldList`(mysql_cmd_executor.go) | C11 回归控制:列未枚举,未虚报支持 |
| SHOW CREATE TABLE/VIEW、定义导出 | build_show.go L119/L205 | C12 持久定义,无需推导成功 |
| DUMP TABLE / 查询结果导出 | table_dump.go L523;query_result.go | C13 View 拒绝保持;结果导出用当次描述 |
| ANALYZE 可见列 | `resolveTableVisibleColumns`(mysql_cmd_executor.go L2561),`bindAnalyzeTables` 拒绝 View | C17 物理统计消费者,不支持范围未扩 |
| I_S TABLES/VIEWS/STATISTICS/KEY_COLUMN_USAGE 等 | predefined.go(STATISTICS 订阅分支由 [subscription_metadata.go](../../pkg/sql/plan/subscription_metadata.go) 提供可见性与预算守卫) | C17 身份/定义字段;View 无索引/约束行,不虚构源表元数据 |
| SHOW PUBLICATION 表清单 | publication_subscription.go L2299(`relkind != 'v'` 名称列表) | C17 身份字段 |
| restore/clone/PITR、snapshot restore | snapshot.go L2594/L2808、snapshot_catalog_restore.go | C15 物理记录,目标侧按其 schema/权限重新绑定(S5/S9) |
| `BuildTableDefByMoColumns` 接口 | frontend/compiler_context.go L1755、compile/sql_executor_context.go L165;本基线上无生产调用方,仅测试替身 | C14 残余,最终处置归 S3 #29438(本 PR 不删除接口,避免接管 S3 范围) |

“S3 所有”的消费者(C02 查询/嵌套 View、C03 CTAS/default、C08 计划缓存、C09 PREPARE/reprepare、C10 cursor、C14 LIMIT 0 完整迁移)不在本表展开,其关闭归 #29438;S3 未完成前,#29440 不视为解除阻塞。

## 5. 残余项与跟踪(问题 2)

| 残余项 | 归属 | 处置 |
| --- | --- | --- |
| planner/CTAS/计划缓存/PREPARE/cursor 派生元数据迁移(C02/C03/C08–C10/C14,含 `BuildTableDefByMoColumns` 最终处置) | S3 [#29438](https://github.com/matrixorigin/matrixone/issues/29438) | 保持 open,本记录不接管 |
| #26227 持久化验收的 D03 替代认可 | #26227 owner | D03 批准时已声明需同步认可替代(S1 批准记录特别条款 1);编辑/关闭 #26227 是显式独立动作(设计第 16 节、批准记录第 5 节),本记录不代为评论或关闭 |
| 跨组件/快照/恢复/混合版本证据 | S5 [#29440](https://github.com/matrixorigin/matrixone/issues/29440) | 依赖 #29438 完成;S4 消费者范围以本记录交付 |

**S5 解除阻塞条件**:`#29438` 完成且本对账记录合入。在本记录合入前,S4 的关闭性归类审计缺失,#29439 与 #30578 的范围差异无权威落点。

## 6. 证据与非证据

- 已执行:上述 `rg` 源码搜索(基线 `76e152a1dd`)及每个命中调用点的人工语义核对;`git diff --check`;文档结构检查(表格列数一致、代码围栏闭合)。
- 未执行:Go UT/BVT/race/多 CN/混合 binary/恢复测试——本次仅文档,无生产或测试代码改动;#30578 已报告的验证证据本条不重复声称。
- 本记录不代表对 #29438/#29440 的批准,不改变任何 issue 状态;对 #26227、#29005/#29006/#29007 不做编辑。

## 7. 结论

回答 #30668:

1. **#30578 意图为关闭 S4 全部 opt-in 消费者接线**(C04–C07、C11–C13、C16、C17),其"S2 opt-in"表述指复用 S2 请求边界而非缩小 S4 范围;"默认行为不变/不激活 rollout/不吸收 S3"是正确边界声明,与 S4 交付不矛盾。
2. **S4 无未交付的代码范围**:D03 raw-catalog 策略的批准含义即"物理语义 + 内部调用者迁出",二者均已落实;缺失的两项关闭性记录(最终归类审计、D03 显式声明)由本记录补齐。剩余适配/契约工作全部归 S3 #29438 与 S5 #29440,已在本记录第 5 节显式跟踪;在 #29438 完成前,#29440 不应视为解除阻塞。

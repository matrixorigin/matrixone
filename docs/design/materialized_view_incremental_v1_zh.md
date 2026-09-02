# 增量维护物化视图 V1（中文对照）

状态：草案，邀请设计评审

归属 Issue：https://github.com/matrixorigin/matrixone/issues/24553

实现 PR：https://github.com/matrixorigin/matrixone/pull/27615

本文描述 PR #27615 当前已有代码。该分支在设计正式批准前已经实现，因此 reviewer
应先评审本文定义的用户契约和正确性不变量，再进入实现细节评审。第 3、12 节明确
区分当前已实现能力与后续设计提案。若中英文有歧义，以获批的英文 revision 为准。

## 1. 问题与用户契约

MatrixOne 已有普通视图，但缺少面向 trace/metrics firehose、可持久化且持续维护的
预聚合结果。主要场景是 append-heavy 的 Dashboard 和告警查询，同时也要保证过期
数据删除以及迟到/纠正事件 update 的正确性。

V1 把 MV 存成物理表，支持两种刷新时机：

- `ON CHANGE`：通过 ISCP 异步初始化和持续维护；
- `ON DEMAND`：仅在用户显式执行完整刷新时初始化或替换结果。

支持三种刷新策略：

- `FAST` / `INCREMENTAL`：只有当前实现支持的增量 SQL 才能创建；运行期 delta
  失败时失败关闭，不能静默改成全量语义。
- `COMPLETE` / `FULL`：每次都执行完整定义查询。
- `FORCE` / `AUTO`（默认）：能生成增量计划就增量，否则完整刷新；运行期增量
  事务失败时，先完整回滚，再在同一边界尝试全量刷新。

`ON CHANGE` 是最终一致的，源 DML 不等待 MV。成功发布 tail watermark W 后，目标
结果和内部状态必须等于定义 SQL 在 W 的结果。当前实现不承诺统一的最大 freshness
lag。

## 2. SQL 接口

标准语法：

```sql
CREATE MATERIALIZED VIEW mv AS SELECT ...;

CREATE MATERIALIZED VIEW mv
  REFRESH FAST ON CHANGE
AS SELECT ...;

CREATE MATERIALIZED VIEW mv
  REFRESH COMPLETE ON DEMAND
AS SELECT ...;

REFRESH MATERIALIZED VIEW mv;
DROP MATERIALIZED VIEW mv;
```

默认值是 `REFRESH FORCE ON CHANGE`。Parser 也接受 `INCREMENTAL`、`AUTO`、
`FULL` 别名；省略时机时默认 `ON CHANGE`。不支持 `ON COMMIT`。

只有 `COMPLETE/FULL ON DEMAND` 允许手动 `REFRESH`，其他模式拒绝手动刷新，
避免与 ISCP owner 并发更新。

本 PR 同时接入 parser AST、plan protobuf、statement 分类、数据库 remap、prepared
statement schema 收集和权限提取。REFRESH 当前复用 ALTER VIEW/database ownership
权限路径，没有新增 MV 专属权限。

## 3. 定义范围与刷新选择

### 3.1 源关系

MV 必须是顶层 select，包含 1～16 个直接、持久化的普通基表。注册 ISCP job 时解析
并持久化源表 ID；同一个物理源重复出现时在 job source set 中去重。

外表、临时表、cluster/source/subscription 等特殊关系、普通 view、MV 和内部 MV
状态表都不能作为源。这些关系没有统一的普通表历史 change/snapshot 契约，且 MV
作为源会形成递归依赖。

### 3.2 当前已实现的增量 SQL

增量计划当前要求恰好一个直接基表和顶层 `SelectClause`，支持：

- 可选的确定性 row-local `WHERE`；
- 普通 `GROUP BY`，或把 `SELECT DISTINCT` 改写成 grouping；
- 代数 delta：`COUNT(*)`、`COUNT(expr)`、`SUM(expr)`、`AVG(expr)`；
- 仅重算受影响分组的 `MIN(expr)`、`MAX(expr)`；
- 通过持久 value multiplicity 状态实现精确 `COUNT(DISTINCT expr)`；
- insert、delete、update tail。

每个 group 表达式必须在输出中恰好出现一次。增量标量表达式白名单包含列引用、
字面量、算术/一元运算、比较、布尔/NULL/range 条件、cast、`CASE`，以及
`date_trunc`、`coalesce`、`ifnull`、`abs`、`floor`、`ceil`。未知或易变函数、
子查询、窗口、嵌套聚合和不支持的表达式节点不能生成增量规格。

当前增量规划既支持顶层单表 `SelectClause`，也支持包含 2～16 个分支的顶层
`UNION ALL`：每个叶子必须是
单个直接普通表上的聚合分支，并且各分支的输出列和隐藏状态 schema 必须兼容。
`UNION ALL` 使用稳定 branch ID 组成隐藏 group identity，因此不同分支产生的相同可见
行不会被错误合并；同一物理源只订阅一次，delta 会路由到所有匹配分支。

当前 PR **尚不支持以下增量能力**：

- `HAVING`；
- `SUM(DISTINCT)`、`AVG(DISTINCT)`；
- `UNION DISTINCT`、`INTERSECT`、`EXCEPT`、分支内部嵌套集合操作，或输出/聚合
  状态 shape 不兼容的 `UNION ALL`；
- JOIN、CTE、子查询、窗口、`ORDER BY ... LIMIT`、ROLLUP、CUBE、GROUPING SETS、
  Top-K、percentile/quantile、bitmap/HLL、用户自定义聚合状态。

查询必须先通过 3.1 的直接普通源 admission。通过 admission 但无法生成增量规格时，
`FAST` 在创建阶段拒绝；当前错误能说明“不属于受支持的单表增量聚合”，但还不能
对每种 SQL construct 返回独立原因。`FORCE` 不保存增量规格并走完整刷新。未通过
source admission 的 derived table、普通 view 和特殊 relation 会直接拒绝，而不是完整
刷新。兼容的顶层 `UNION ALL` 可走 FAST；通过 source admission 但不能增量编译的
`UNION ALL` 可由 FORCE/COMPLETE 完整刷新。

### 3.3 当前已实现的完整刷新

`FORCE` 和 `COMPLETE` 接受普通 planner 能执行、包含 1～16 个直接普通源的定义，
包括多表 JOIN。保存的 refresh SQL 中所有源引用都会重写到同一个 ISCP `toTS`，
因此结果不会混合不同源边界。

多源当前只支持完整刷新。多源 job 通过引入其序列化 ISCP 结构的 MORPC 版本门控；
旧服务拒绝创建，不能把它误解成旧单源 job。

### 3.4 当前刷新边界

| 输入/策略 | 当前行为 | 原因 |
| --- | --- | --- |
| 任意 `ON CHANGE` MV 的初始 snapshot | 完整构建 | 尚无 target/operator state，必须从一致性 snapshot hydration |
| `FAST/FORCE ON CHANGE` 且存在 version-2 增量规格的 tail | 增量 | consumer 可把 insert/delete/update 转成有符号 group delta |
| `FORCE ON CHANGE` 且查询通过 source admission、但无增量规格 | 完整刷新 | 当前没有表达该查询的可撤回 operator state |
| `FORCE` 增量事务失败 | 回滚后在同一 `toTS` 完整刷新 | 不能发布部分 target/state 或越过失败 boundary 的 watermark |
| `FAST` 增量事务失败 | 报错，不 fallback | FAST 是“必须增量”的用户契约 |
| `COMPLETE/FULL ON CHANGE` | 每个变化 boundary 完整刷新 | 用户显式选择完整替换 |
| `COMPLETE/FULL ON DEMAND` | 手动完整刷新 | 当前唯一允许的 ON DEMAND 组合 |
| 未通过 source admission | 创建失败 | 缺少统一 snapshot/change/source identity 契约，或无法安全提取依赖 |

完整刷新在这里指：在共同 boundary 删除旧 target、重新执行完整定义查询、重建所需
state 并原子提交。MatrixOne 当前不支持 PCT，因此完整刷新不能缩小为“仅刷新变化
分区”。

## 4. 物理结构与持久化元数据

MV 存为普通物理 relation，而不是逻辑 view。Catalog property 持久化：

- MV 标记、刷新策略和刷新时机；
- 源数据库/表名以及 1～16 个源列表；
- source SQL 和可执行 refresh SQL；
- 满足条件时保存带版本号、base64 编码的增量规格。

当前增量规格版本是 2，记录 source columns、filter、group expressions、aggregate
kinds、可见输出列、隐藏状态列、序列化 group-key 列、策略和内部状态表身份。
Consumer 遇到未知版本时失败关闭。

增量 target 使用二进制 `serial_full(...)` group key 作为隐藏主键，并保存 group
row count、SUM/AVG sum/count 等隐藏状态。只支持全量刷新的 target 使用 MatrixOne
隐藏 auto-increment fake primary key；创建时在 refresh 前初始化 sequence。

MIN/MAX 和精确 COUNT(DISTINCT) 使用 consumer 拥有的内部状态表，表名由数据库和
MV 名称的确定性 hash 生成。该 namespace 保留，普通用户不能创建、修改或单独
删除状态表。

## 5. ISCP 生命周期与多源扩展

V1 新增独立的 `ConsumerType_MaterializedView`，明确不使用
`ConsumerInfo.InitSQL`，注册前也不执行 CTAS。

`ON CHANGE` 创建时使用 `startFromNow=false` 注册一个 ISCP job：

```text
注册 job
  -> ISCP 一致 snapshot
  -> MaterializedViewConsumer 完成初始全量构建
  -> 从 snapshot 边界继续 tail
  -> 增量或完整维护
```

Job 保留 `SrcTable` 作为兼容 anchor，并通过 `SrcTables` 保存完整源集合。Dirty
table 检测覆盖所有源；一次 iteration 在同一个 `[fromTS,toTS]` 收集所有源。
每个 batch 带 `SourceTableID`，多源 schema 不同时使用各自 TableDef 解析 CDC batch
索引。只需要边界的 full-refresh consumer 会 drain stream，但不保留表规模 payload。

Executor 使用独立的一秒 change poll，不再只依赖较大的 task-sync tick。Iteration
只有在 worker admission 成功后才标记 pending。新 executor generation 不可能仍由
旧 worker 持有任务，因此持久化的 Pending/Running 会恢复成可调度状态。LSN 恢复、
generation fence、cancel/drain 和 final-status 处理共同防止废弃 generation 覆盖当前
generation。

Watermark 继续沿用现有 ISCP ownership：

- tail 中 consumer 在同一个 SQL 事务更新 target/state，并调用普通 retriever
  watermark update；
- 初始 snapshot 的 retriever watermark update 是 no-op，consumer 结果事务成功后，
  由 iteration finalization 持久化 completed snapshot watermark。若在两步之间崩溃，
  最多重复一次原子全量替换，不能在结果事务成功前声明 snapshot 已完成。

没有新增 MV 私有 watermark 或进度 cache。

## 6. 刷新算法

### 6.1 Snapshot 与完整刷新

Consumer 在一个 refresh 事务中删除旧结果、在共同边界执行所有源查询、插入新
结果、重建需要的 exact-distinct 状态并提交。Delete/insert 同事务，查询者不会看到
半替换结果。

手动 `ON DEMAND` refresh 获取数据库 shared lock 和 MV table exclusive lock，在
调用者事务 snapshot 上执行同样的原子替换，不添加 `MO_TS`。

### 6.2 增量 tail

ISCP insert row 提供引用到的源值；CDC tombstone 只有 row identity 和主键，没有
全部旧值。因此本 PR 新增 engine `RowIDReader` contract 及 DistTAE 实现，按每条
tombstone commit 之前的 snapshot 恢复删除行。如果一行在同一个 tail interval 内
被插入或多次 update，统一从 iteration `fromTS` 读取会得到错误结果。

Update 因而等价于旧行负贡献加新行正贡献；进入/离开 filter 以及移动 group 都复用
这条规则。Native value extraction 保留 NULL 和 temporal type precision。

对 group G 的有符号行 S：

- `COUNT(*)` 加 S；
- `COUNT(expr)` 只在 expr 非 NULL 时加 S；
- `SUM(expr)` 维护 sum 与 non-NULL count，删除最后一个值后恢复 SQL NULL；
- `AVG(expr)` 维护 sum 与 non-NULL count，再计算商；
- `MIN/MAX` 记录 G，并在 `toTS` 只重算受影响 group；
- 精确 `COUNT(DISTINCT expr)` 更新 `(aggregate,G,encoded value)` multiplicity，
  仅在 0→1 或 1→0 时修改可见 count；
- group row count 归零时删除该结果行。

Planner 证明 non-null 的 group key 使用普通 equality，否则使用 NULL-safe equality。
Distinct/group identity 保存序列化值，不依赖未经碰撞校验的 hash。

Delta 渐进处理：逻辑 chunk 最多 32,768 行，生成 SQL 最多 8 MiB；过大 chunk 递归
拆分。每条内部 SQL 推进 statement boundary，但共享同一事务。

### 6.3 失败与 fallback

Parse、bind、row lookup、state、target、watermark、cancel 或事务错误都会回滚整个
增量事务。`FAST` 直接返回错误；`FORCE` 只有在 delta 事务确定回滚后，才允许在
同一 `toTS` 开新事务完整刷新；COMPLETE 从不尝试 delta。

## 7. DDL、DML 与依赖行为

只有内部 refresh context 能写 MV 隐藏列。Planner/binder 与 DistTAE 同时阻止用户
对 MV target 或 state relation 执行 INSERT、UPDATE、DELETE。

删除 MV 时按 target identity 注销所有活跃 ISCP generation，验证内部 relation
marker，并与 target 一起删除 state table。不能只依赖 job name，因为下划线拼接
可能碰撞。

源表被删除或 rename 后，即使 job anchor 在另一个源上，也会把所有引用 job 标为
error。查询物理 MV 时会重新验证持久定义中的源；源缺失或已变成不支持的 relation
时查询直接报错，不返回陈旧结果。
通用源表 DDL 也会运行在尚未初始化可选 ISCP catalog 的部署和测试环境中。此时缺少
`mo_iscp_log` 意味着不可能存在 MV job，因此依赖失效处理直接 no-op；其他 catalog
或 executor 错误仍然中止 DDL 事务。

当前不支持 ALTER MATERIALIZED VIEW、源 schema 变化后自动重建增量规格、明确的
CASCADE/RESTRICT policy 或 MV 作为源。

## 8. 兼容性与发布

SQL grammar 和 plan protobuf 都增加了公开持久结构，PR 中同时重新生成
`mysql_sql.go` 和 `plan.pb.go`。旧单源 ISCP job 继续使用 `SrcTable`；多源扩展是
additive 的，并受 protocol gate 保护。

当前没有 catalog feature-version negotiation、自动 downgrade 或 migration tool。
不支持运行无法理解 MV metadata/多源 consumer 的旧 binary。Target、state、job
log 和 source identity 作为一个逻辑对象的 backup/restore/PITR 尚未验证，V1 不
声称已支持。

回滚方式是 DROP MV，它会注销 job 并清理自有 state。当前没有 optimizer 自动
query rewrite，因此删除 MV 不会改变普通查询计划。

## 9. 可观测性与性能行为

本 PR 注册以下 metrics：

- 按 `incremental|full` 和 result 标记的 refresh transaction duration；
- 按 `insert|delete` 标记的增量 source rows；
- FORCE 从增量 fallback 到全量的次数；
- 成功 watermark 的 wall-clock lag histogram。

当前还没有 per-MV label、state cardinality/bytes、affected-group count、chunk bytes、
retry class 或 SQL status 接口。

代数热路径目标复杂度与 changed rows、distinct-key transition 和 affected MIN/MAX
groups 成正比；完整刷新仍与源数据量成正比。Freshness 还受 ISCP polling、事务和
planner 开销、target write amplification 以及磁盘吞吐限制。

## 10. PR 当前验证范围

已提交 UT 覆盖：

- parser/formatter 语法和 refresh alias；
- source validation、物理 marker、state namespace、DML 拒绝、source dependency
  validation、refresh policy 和增量规格规划；
- 单源/多源共同边界 SQL rewrite；
- initial/full payload skipping 和 FAST 不 fallback；
- signed delta SQL、batch/size limit、NULL join、temporal precision、exact-distinct
  spec/state 和 tombstone 前 row lookup；
- 多源 dirty detection/batch index、retained row ID、job-log JSON、snapshot/tail
  status 与 watermark；
- executor restart recovery、admission ordering、fencing、cancellation、rollback 和
  protocol-version gate。

该分支也使用本地/远程 SQL 脚本和长时间 append benchmark 验证过；这些脚本按要求
不提交为本 PR 的 BVT。Benchmark 数字应在 PR summary 中附 exact revision 和机器，
它们是证据，不是可移植 SLA。

合并前，获批设计应要求补充确定性 public SQL test：snapshot、insert/delete/update
tail、complete multi-source refresh、FAST reject、FORCE 路径选择、source-drop 后
查询失败、direct-DML reject、ON DEMAND refresh 和 restart recovery。BVT polling
必须有界，避免测试套件超时。

## 11. 业界对标与目标能力并集

“主流 MV”并没有统一契约。MatrixOne 的目标是吸收下列公开模型中有价值的能力
并集，不表示每个数据库都支持表中所有能力。

| 系统 | 公开能力 | 对 MatrixOne 的参考意义 |
| --- | --- | --- |
| [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/26/dwhsg/basic-materialized-views.html) | FAST/FORCE/COMPLETE、ON COMMIT/ON DEMAND、log/PCT refresh、aggregate/JOIN/UNION ALL、嵌套 MV、query rewrite | refresh policy、能力解释、依赖 DAG 和 query rewrite 的参考；PCT 只用于业界对比，不在本 MatrixOne 设计范围 |
| [PostgreSQL](https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html) | 通用 SQL 全量手动刷新、`CONCURRENTLY`、`WITH [NO] DATA`、普通表存储/index 参数 | 通用 fallback、延迟填充、非阻塞替换、物理设计 |
| [ClickHouse](https://clickhouse.com/docs/materialized-view/incremental-materialized-view) | 实时 append 的 insert-trigger incremental MV，以及支持依赖和原子 replace/append 的定时 refreshable MV | append 快路径和定时全量；不能作为 delete/update 正确性基线 |
| [Snowflake](https://docs.snowflake.com/en/user-guide/views-materialized) | 自动单表维护、query rewrite、clustering、常见聚合、variance/stddev、bitwise、HLL；不支持 JOIN/HAVING/window/ORDER BY/LIMIT | 单表聚合广度、optimizer 集成、clustering 和维护成本 |
| [Materialize](https://materialize.com/docs/transform-data/optimization/) | insert/update/delete 下持续维护 JOIN、aggregate、DISTINCT、MIN/MAX、grouped Top-K；arrangement、group-size hint、temporal filter、freshness | 可撤回 operator state、keyed arrangement、资源 hint 和 freshness 语义 |
| [RisingWave](https://docs.risingwave.com/sql/commands/sql-create-mv) | 持续 backfill/维护、JOIN、group Top-N、tumble/hop/session window、window-close、级联 MV、在线控制 | streaming operator、event-time、级联 pipeline、backfill admission |

MatrixOne 最终应覆盖下面所有能力族，但必须按依赖顺序交付。某个算子没有状态、
失败恢复和资源契约前，不能仅因为语法可解析就标记为 FAST。

上表描述的是被对标数据库的能力和未来设计输入，不代表 MatrixOne 当前已经支持。
特别地，本 PR 实现的是基于 ISCP/CDC log 的维护，**不支持** Oracle 风格的 Partition
Change Tracking（PCT），也不支持 MV 的分区级刷新；二者都不是本设计的目标。

## 12. 其余主流能力的实现设计

### 12.0 从完整刷新迁移到增量维护

目标不是把每个 SQL construct 都直接写进 ISCP，而是把当前扁平 version-2 规格升级
为 version-3 增量 operator graph。Planner 对每个定义输出三态结果：

- `INCREMENTAL(spec, cost)`：具备正确的 insert/delete/update 算法、持久 state schema
  和资源上界；
- `COMPLETE(reason)`：查询可执行且 source contract 成立，但尚无安全或有界的增量
  operator；
- `REJECT(reason)`：source/lifecycle/安全契约不成立，完整刷新也不能保证正确。

`FAST` 只接受 `INCREMENTAL`；`FORCE` 优先使用 `INCREMENTAL`，否则使用
`COMPLETE`；`COMPLETE` 不编译 delta operator。错误必须带稳定 construct/reason code，
例如 `MV_FAST_UNSUPPORTED_HAVING`、`MV_FAST_UNBOUNDED_JOIN_STATE`，不能只返回一个
通用“单表聚合不支持”。

#### Operator graph 与中间状态

每个 operator 有稳定 ID、kind、输入边、typed key/payload schema、retract 能力、
state relation ID、估算 rows/bytes 和版本。中间表由 consumer generation 拥有，放在
保留 namespace，普通 SQL 不能读写或单独删除。初始 snapshot 使用同一 operator graph
hydration target/state，tail 不再维护另一套逻辑。

最少需要下列 state family：

| 查询能力 | 持久中间状态 | 增量动作 |
| --- | --- | --- |
| 全局 aggregate | 一个固定 zero-dimensional group key | 所有 delta 合并到同一 group |
| 无 aggregate 的 GROUP BY/DISTINCT | `(group key)->row multiplicity` | 0↔1 时发布或撤回 visible row |
| HAVING | 完整 group aggregate state 与 visible bit | 重新求 predicate 并处理 false/true 转换 |
| SUM/AVG DISTINCT | `(operator,group,value)->multiplicity` 加 distinct sum/count | 仅 0→1/1→0 修改聚合 |
| UNION ALL | stable branch ID 加 branch row/group identity | source delta 路由到每个匹配 branch，保留重复 |
| UNION/INTERSECT/EXCEPT | output row 在每个 branch 的 multiplicity | 根据 set predicate 更新 visible row |
| JOIN | 每个输入的 keyed arrangement、payload、multiplicity、match count | 一侧 delta probe 其他侧并产生 join-product delta |
| ROLLUP/CUBE/GROUPING SETS | `(grouping-set ID,group key)` | 一个输入 delta fan-out 到有限多个 grouping set |
| Top-K/window | `(partition,order key,row identity)` 有序 multiset | 更新受影响 partition 与 K/rank 边界 |
| percentile/quantile/HLL/bitmap/UDAF | 声明 merge/retract/serialize 契约的 typed state | 只有具备 mutable-source retract 契约才可 FAST |

所有 target/state 变更和 ISCP tail watermark 必须在同一 SQL 事务提交。失败前不能发布
任何一部分；旧 generation 被 fence 后不能继续写 target 或 state。DROP/REBUILD 由
generation owner 清理全部 operator state，restart 从 catalog 中的 spec、state relation
和 watermark 恢复，而不是依赖内存 cache。

#### ISCP 通用扩展

ISCP 保持“提供变化和一致性 boundary”的职责，不理解 aggregate/JOIN/HAVING。为支持
operator graph，需要在现有多源能力上补充：

1. 每个 batch 保留 `SourceTableID`，job spec 保存 `source -> operator input/branch`
   route；同一物理源只订阅一次，但可 fan-out 到多个 operator input。
2. 一个多源 iteration 必须为所有 source 提供共同 `[fromTS,toTS]`，只有全部 source
   都到达 boundary 后才能运行/提交 operator graph。
3. delete/update 优先携带 planner 投影列的 before-image；协议版本不支持或 payload
   过大时继续使用当前 `RowIDReader` 按 tombstone commit 前 snapshot 回读。两条路径
   必须产生同一 typed row delta。
4. snapshot 可以按有界 chunk 喂给同一 graph，降低 hydration 峰值。每个 chunk 只写
   未发布的 shadow generation，并可独立提交；全部 chunk 成功后，再用一个事务原子
   发布 target generation 和 snapshot watermark。崩溃后可以续建或丢弃 shadow，绝不
   暴露部分 hydration 的 target。
5. job status 持久化 spec version、generation、各 source progress 和最后成功 boundary。
   老 CN 遇到 version-3/multi-source operator job 必须拒绝接管，不能按 version-2 解释。

不选择“每种 MV 创建一组独立 CDC job”，因为多源共同 boundary、故障恢复和 watermark
原子性会被拆散；也不把完整 base rows 永久复制进 consumer 内存，因为状态无界且
restart 后不可恢复。普通持久表加有界 cache/spill 是默认 state storage。

#### 哪些完整刷新可以逐步消除

交付顺序按收益和状态复杂度排列：

1. 固定 group key 的全局 aggregate，以及 multiplicity GROUP BY；
2. HAVING、SUM/AVG DISTINCT（顶层兼容 UNION ALL 及 FORCE/COMPLETE source
   admission 已在本 PR 实现）；
3. unique-dimension inner equi-join，再扩展 non-unique/multi-way join；
4. ROLLUP/CUBE/GROUPING SETS 和可证明有限 fan-out 的子查询 decorrelation；
5. Top-K、有界 window、event-time TUMBLE/HOP；
6. 具有明确 retract/accuracy/memory contract 的高级 aggregate；
7. dependency DAG 完成后支持级联 MV。

CTE 本身不是必须全量的理由：non-recursive CTE 应先 inline/编译成 operator graph。
scalar/correlated subquery 能 decorrelate 成 join/aggregate 时复用对应 state；不能证明
有界影响范围的相关子查询继续 COMPLETE。确定性 scalar expression 逐步扩大白名单；
volatile/session-dependent expression 永远不能 FAST。

#### 应长期保留完整刷新或拒绝的边界

下表区分“天然需要完整构建”“当前实现 fallback”和“必须拒绝”三类情况：

| 场景 | 当前不能增量的原因 | 长期处理 |
| --- | --- | --- |
| 没有可信 state 时的首次 hydration/rebuild | tail delta 生效前，每条 source row 必须且只能贡献一次 | 逻辑上天然是完整构建；用 shadow-generation 分块与并行限制资源峰值 |
| 显式 `COMPLETE/FULL`，含手动 ON DEMAND | 用户主动选择 replacement semantics | 保持完整；优化调度、coalescing 和原子 shadow replacement |
| FORCE 遇到未实现的 delta operator | 持久 graph 还不能表达该查询 | 按上面的交付顺序增加 operator/state family，之后自动选择 INCREMENTAL |
| 估算 state/fan-out 超过预算 | 算法正确，但无法满足准入资源上界 | 增加 index、spill、compaction、cardinality hint 或显式扩大 quota；绝不能静默执行无界 FAST |
| 增量事务失败 | target、state 和 watermark 不能分叉 | 优先幂等重试同一 boundary；FORCE 可在同一 `toTS` rebuild，FAST 不推进并停止 |
| spec/state checksum、version 或 generation 校验失败 | 现有 state 不可信，不能据此生成下一批 delta | fence 后 rebuild/shadow-migrate，不能从损坏或不兼容 state 继续增量 |
| non-equi/cross 或爆炸型 many-to-many JOIN | 单行变化可能需要无界扫描或产生无界输出 fan-out | 只有具备有界 index/probe plan 和 state admission 后才增量，否则 FORCE 完整刷新 |
| 会影响无界 suffix 的 window | 单个变化可能改写无界数量的已发布 rank/value | 使用 bounded frame/Top-K 或重建受影响内部 window，否则 FORCE 完整刷新 |
| mutable-source UDAF、HLL、percentile 或 sketch 没有 retract | 已保存 state 无法减去旧值 | 增加 retractable/counting state、精确 ordered state 或 immutable logical-window state，否则 FORCE 完整刷新 |
| volatile/current-time/random/session expression | 重新计算旧行不能复现其原始贡献 | FAST 始终拒绝；FORCE/COMPLETE 只有满足普通查询 reproducibility contract 才准入 |
| 外表、临时表、特殊/state relation 或循环依赖 | 缺少 snapshot、change identity、lifetime、安全或无环调度保证 | 直接 REJECT，不是 COMPLETE；补齐 source/lifecycle contract 后再开放 |
| PCT/分区级 MV refresh | 本设计没有 MatrixOne source-partition change contract | 不在范围内，不能宣称为增量维护 |

因此，只有首次 hydration/rebuild 和用户显式选择 COMPLETE 在语义上天然需要完整处理。
大部分 SQL shape fallback 属于 operator 实现或资源有界性缺口，应逐步迁移到 operator
graph；source/lifecycle contract 无效的定义则不能用完整刷新伪装成已支持。

#### Resource、兼容性与验证 gate

Planner/admission 必须记录 estimated state rows/bytes、最大 join fan-out、hot-group
cardinality、spill threshold 和每 iteration work budget。超过预算时 FAST 明确拒绝，
FORCE 选择 COMPLETE；运行中超过 hard limit 必须回滚，不能 OOM 后发布 watermark。
FORCE 的完整 fallback 也必须有 full-refresh cost budget 和 change coalescing policy；
如果重复全表扫描无法追上变化，应报告 backpressure 或明确暂停，不能形成无界 refresh
队列。

Version-2 MV 保持原 consumer 和 state schema。Version-3 通过 MORPC feature gate 创建；
混合版本中旧节点不能调度新 job。升级不原地解释旧 state，默认保留 version-2，用户
REBUILD 或 shadow generation 才迁移；回滚只能继续读兼容 version，或完整重建。

每个新 operator 至少要有：signed-delta UT、与同 boundary 完整查询比较的 public SQL
BVT、insert/delete/update/NULL/duplicate、事务 rollback、consumer restart、重复 delivery
和 state cleanup。多源 operator 还要覆盖 source 交错提交、任一 source 卡住、共同
boundary 恢复。性能 gate 同时报告 source throughput、freshness p50/p95/p99/max、
state bytes、write amplification、CPU/IO 和 backlog drain；仅结果最终一致不代表增量
实现可交付。

### 12.1 Aggregate、HAVING、DISTINCT 与集合操作

下一版持久化规格升级为 version-3 operator graph，用带类型状态算子和稳定 operator
ID 代替扁平 aggregate list。

- **HAVING**：完整 group state 始终存在内部 relation 中，即使当前不满足 HAVING、
  不出现在 target。每次 group delta 合并后求 HAVING；false→true insert，true→false
  delete，true→true update，false→false 只改 state。
- **SUM/AVG DISTINCT**：复用 `(operator,group,encoded value)->multiplicity`；只有
  0→1/1→0 改 distinct sum/count，AVG 由二者计算。
- **Variance/stddev**：维护可撤回 count、sum、sum-of-squares，并使用扩大后的
  numeric type；无法保证溢出或误差的类型退化为 affected-group rebuild。
- **Bitwise aggregate**：维护每个 bit 的 one/zero count，不能只存一个无法撤回的
  bitmask。
- **Approximate distinct**：append-only HLL 可以 merge，但普通 HLL 不能 delete。
  可变源需要 counting/retractable sketch，或 immutable logical-window sketch 加受影响
  operator-state bucket 重建；这里的 bucket 是内部状态，不是源表 PCT。禁止把不安全
  append-only state 用于 mutable FAST。
- **Percentile/quantile/histogram**：append/window-close 使用可 merge 的 partition
  sketch；任意 delete/update 使用可撤回有序 state 或 affected-group rebuild，并把
  accuracy/memory 参数持久化。
- **UNION ALL**：每个 branch 独立编译并分配稳定 branch ID；隐藏 identity 为
  `(branch ID,row/group key)`，保留跨 branch duplicate。同一源只注册一次，但变化
  要送到所有匹配 branch。
- **UNION DISTINCT/INTERSECT/EXCEPT**：维护每个 output row 在各输入的
  multiplicity，再按 SQL set predicate 决定可见性；state 与 distinct input rows
  数量成正比。

当前代码已实现顶层兼容 UNION ALL；下一批仍是 HAVING、SUM/AVG DISTINCT 和
construct-specific FAST error。不兼容但通过 source admission 的定义只有 FORCE 能
选择完整刷新。

### 12.2 增量 JOIN

每个输入维护 durable keyed arrangement，保存 row identity、projected payload、
multiplicity 和 source progress。一侧变化时 probe 其他侧 arrangement，生成带符号
join-product delta。一次多源 delta 在共同 ISCP boundary 计算，target、arrangement、
tail watermark 同事务提交。

首批只做 immutable equality key 的 inner equi-join；唯一维表键可优化为单次 lookup。
后续扩展：

- non-unique many-to-many join，并按估算 cardinality 做 admission；
- left/right/full outer join，用 per-row match count 在 0↔1 时发布/撤回 NULL-extended
  row；
- semi/anti join，用 match multiplicity；
- key update 等价 delete-old + insert-new；
- multi-way delta join，optimizer 选择 probe order 并复用 index。

Cross join、无有界索引策略的非等值 join、预计爆炸的 many-to-many 必须走 FORCE，
除非用户显式提高 state budget。外键只用于估算和 unique-side 选择，正确性不能依赖
未强制约束。

### 12.3 Top-K、ORDER BY/LIMIT 与窗口函数

Grouped Top-K 使用 `(partition key,order key,stable row identity)` 持久有序 multiset。
每个变化只更新一个 partition，并发布 K boundary 附近 membership/rank 差异；tie 和
NULL order 必须精确编码。Group-size hint 和 spill threshold 防止热点 group 无界占用
内存。

第一批 window 支持可下推到有序 state 的 partitioned `row_number`、`rank`、
`dense_rank`、`first_value`、`last_value` 和有界 `lead/lag`。一次插入会改变无界
suffix 的普通 window，在有界算法出现前走完整刷新或重建受影响的 operator-state
window；这不是源表分区刷新。

### 12.4 Event-time window、淘汰与迟到数据

增加带 event-time column、持久 watermark 和 allowed-lateness policy 的 `TUMBLE`、
`HOP`，后续支持 `SESSION`。发布模式包括：

- emit-on-update：开放窗口持续更新；
- emit-on-window-close：event watermark 越过窗口后只发布一次 final result。

窗口关闭时生成 synthetic negative delta 回收 input/operator state。Processing-time TTL
是另一种 policy，不能冒充 event-time correctness。允许范围内的 late event 重开/更新
state，超出范围必须明确选择 drop、error 或 correction。

### 12.5 级联与在线替换

只有 catalog dependency 构成 DAG 后才允许 MV 作为源。一个 logical commit boundary
按 bottom-up 顺序传播，child progress 不能新于任一 parent。共享 intermediate
arrangement 引用计数，仍有 consumer 时不能清理。

`CREATE REPLACEMENT MATERIALIZED VIEW` / `ALTER ... APPLY REPLACEMENT` 在一致边界
建立 shadow target/state，追平 active watermark 后原子切换 logical ownership，保持
dependent object ID。DROP 默认 RESTRICT；CASCADE 在事务内枚举和删除依赖 DAG。

### 12.6 Refresh 生命周期对齐

- **BUILD IMMEDIATE / WITH DATA**：使用当前 ISCP snapshot。
- **BUILD DEFERRED / WITH NO DATA**：保存 unscannable object，不启动 consumer；
  REFRESH/RESUME 前查询明确报错。
- **REFRESH CONCURRENTLY**：建立 shadow generation、追平后原子切换；要求稳定
  unique row key，同一 MV 只允许一个 refresh generation。
- **REFRESH EVERY/AFTER**：持久化 scheduler、jitter、dependency 和 replace/append
  policy。Replace 使用一致 snapshot；append 必须声明不重叠 range key 和
  idempotency key。
- **ON COMMIT**：独立同步模式，不能作为异步 ISCP alias。源 DML 事务内执行 planner
  生成的 delta，并按 dependency order 加锁。首版限制单源 algebraic aggregate；
  多源 ON COMMIT 需要额外 deadlock/distributed transaction 设计。
- **PAUSE/RESUME/REBUILD/CANCEL**：fence 当前 generation。Resume 只从有效
  watermark 继续；rebuild 创建新 snapshot generation。

### 12.7 Query rewrite 与 freshness-aware read

Optimizer 为新鲜 MV 注册 canonical relational expression，按 source identity、
predicate、grouping、aggregate 和 projection 匹配 query subgraph。可以从细粒度 MV
roll up 到粗粒度 query，但不能仅根据 target contents 推断 freshness。

Read policy 包括 `FRESH`（等待合格 boundary）、`BOUNDED STALENESS interval` 和
`STALE OK`。没有满足 policy 的 MV 时，按显式 hint 选择 base table 或报错。Plan
展示所选 MV、watermark、compensation predicate 和拒绝其他候选的原因。

### 12.8 Partition、index、storage 与资源控制

本节仅是后续设计。MatrixOne 当前不支持 PCT 或分区级 MV 刷新。当前实现要么对
FAST 子集应用逐行 ISCP/CDC delta，要么完整替换 MV 结果；不会根据源表分区变化
元数据把刷新范围限制到受影响分区。

MV DDL 应接受普通 index、clustering、distribution、partition、tablespace/storage 和
retention option，但普通物理分区/存储选项不表示支持 PCT。exchange/drop/truncate
必须转换为 ISCP 能表达的 row delta，或使 FORCE 执行完整 rebuild；不能只根据 source
partition metadata 声称完成了增量刷新。

每个 stateful operator 报告 estimated/actual rows/bytes、spill、hot-key skew 和 write
amplification。Admission 使用 per-MV memory/disk budget、backfill rate/parallelism、
maintenance priority 和 overload policy。State 使用普通 durable storage 加有界 cache，
不能假设 arrangement 全部驻留内存。

### 12.9 管理、安全与恢复

增加 `SHOW MATERIALIZED VIEWS`、`SHOW MATERIALIZED VIEW STATUS`、
`EXPLAIN MATERIALIZED VIEW`，展示 definition、strategy、unsupported reason、source/
target ID、hydration、watermark/freshness、last success/error、retry/fallback、state
bytes 和 generation。

增加 CREATE/ALTER/REFRESH/DROP 专属 privilege，并在 DDL 与 background execution
同时检查 invoker/definer security。Backup/restore/PITR 把 definition、target
generation、auxiliary state、dependency DAG、job log、watermark 当成一个对象；恢复
只能从验证过的共同边界 resume，否则 rebuild，不能混合不同 generation。

## 13. 公开 Benchmark 方案

没有一个公开 benchmark 同时覆盖 snapshot、mutable IVM、streaming SQL 和 serving
latency，因此组合多个可复现 benchmark，并禁止把不同语义直接排成性能名次。

### 13.1 ClickBench 与 TSBS：observability append

[ClickBench](https://github.com/ClickHouse/ClickBench) 提供 10M/100M/1B 真实分布的
web event 和 43 条 dashboard/ad-hoc query。保持官方 schema/data 不变，额外定义并
明确标记下列 MV：

- minute/hour bucket 和 service-like dimension；
- row-local error predicate、`SUM(CASE WHEN ...)`；
- COUNT/SUM/AVG/MIN/MAX、精确/近似 distinct；
- high-cardinality 多维 group 和 Top-K。

官方 43 queries 继续测 base/MV serving；分块 continuous loader 测 maintenance。
结果必须叫 “ClickBench-derived MV workload”，不能冒充官方 ClickBench 成绩。

[TSBS](https://github.com/timescale/tsbs) 提供 DevOps time-series generator、ingestion
tool 和 dashboard query。复用它的 scale、agent、time-window、group 分布，测试持续/
突发 append、window rollup、retention delete 和并发 dashboard read。TSBS 更接近
metrics，ClickBench 的 event skew 更真实。

### 13.2 Nexmark：高级增量 SQL

[Nexmark](https://github.com/nexmark/nexmark) 是 continuous stream operator 覆盖的
主 benchmark。把 persons/auctions/bids 映射为 MatrixOne table，保留公开 generator
和 query semantics，用 projection/filter、aggregate、join、window、Top-K、multi-way
join query 作为能力 gate。测量 source commit 到正确 MV result 可见的时间，不能只
测 barrier latency。

RisingWave 有公开可复现的
[Nexmark 结果表](https://docs.risingwave.com/get-started/rw-benchmarks-stream-processing)，
包含 throughput/per-core/CPU/memory。其数字仅作为背景；只有相同机器、durability、
query subset、generator rate、checkpoint/refresh semantics 和 correctness check 下
重跑，才能与 MatrixOne 横向比较。

### 13.3 TPC-H refresh 与 DBToaster：mutable relational state

SF1 做正确性，SF10/SF30 做容量；使用 Q1-derived aggregate、join/aggregate query 和
RF1/RF2-style insert/delete batch，覆盖 snapshot、append、delete、update-as-delete+
insert。完整 TPC-H score 不是 MV score，不能这样报告。

[DBToaster 实验方法](https://dbtoaster.github.io/papers/2013-dbtoaster-report.pdf) 使用
TPC-H/SSB query 的 update stream，是 delta-heavy relational IVM 参考。SQL 兼容时
复用其 query/update shape，分别比较 algebraic delta、affected-group rebuild 和
complete refresh。

### 13.4 统一测试协议与时长

每个 workload 必须报告：

- exact git revision、数据库版本/配置、机器、storage、topology、durability、dataset、
  writer、batch size、duration；
- snapshot/hydration 和 catch-up drain 时间；
- 无 MV、一个 MV、完整 MV set 的 source throughput；
- 从 marker commit 到第一次读到正确结果的 freshness p50/p95/p99/max；
- 与 freshness 分开统计的 MV query latency p50/p95/p99/max；
- CPU、RSS、state/target bytes、disk throughput/utilization、network、retry、fallback、
  backlog；
- 最终结果及采样 boundary 与 definition query 的精确比对。

运行四档：确定性 correctness、10 分钟 smoke、1 小时 steady+burst、持续增压直到
freshness/backlog 无法恢复。额外覆盖 snapshot/tail restart、hot-key skew、百万 group、
retention delete、dimension update 和 disk saturation。任一 boundary 错误、停止输入后
backlog 不 drain、资源仍无界增长都算失败。

横向比较时：PostgreSQL 只比较 complete/manual/concurrent refresh；ClickHouse 只
比较一致的 append-trigger 或 scheduled semantics；Materialize/RisingWave 比较 mutable
continuous SQL。Vendor 公开数字不能与 MatrixOne 不同机器结果混在同一排名。

## 14. 交付 Gate 与评审问题

能力并集明显大于一个安全 change，交付顺序为：

1. 稳定当前 PR 子集并补 public SQL lifecycle test；
2. HAVING、SUM/AVG DISTINCT、construct-specific FAST error；
3. inner/unique-dimension JOIN 和 operator arrangement；
4. Top-K、event-time tumble/hop window；
5. cascading/replacement、scheduled/concurrent refresh、status control；
6. query rewrite、同步 ON COMMIT 和高级 state。

每个 gate 都必须升级 persistent-spec/compatibility table，补 failure/restart test，并通过
对应 benchmark 后才能宣称 FAST。

邀请 reviewer 重点决定：

- snapshot finalization 与 tail transaction 的 watermark contract 是否可接受；
- FORCE 运行期 fallback 是否应该改为 operator-controlled；
- version-2 spec 应作为稳定格式合并，还是发布前替换成 version-3 operator graph；
- 当前实现早于设计批准的情况下，哪些 gate 应放在 PR #27615，哪些拆 follow-up；
- ON COMMIT 和 optimizer rewrite 是否应单独设计，因为它们分别改变 source-DML 与
  optimizer ownership。

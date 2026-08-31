# 增量维护物化视图 V1（中文对照）

状态：草案

归属 Issue：https://github.com/matrixorigin/matrixone/issues/24553

实现 PR：https://github.com/matrixorigin/matrixone/pull/27615

本文件是 `materialized_view_incremental_v1.md` 的中文对照版本。评审和实现
发生分歧时，以英文原文及其获批 revision 为准。

## 1. 问题与用户契约

MatrixOne 已有普通视图，但缺少面向高吞吐事件表、可以持久化并自动维护的
预聚合结果。目标场景持续写入 trace/metric 事件，并由 Dashboard 和告警系统
反复查询分组聚合；部分场景还会删除过期数据，或更新迟到、纠正后的事件。

V1 提供物理物化视图和两种刷新时机：

- `ON CHANGE`：通过一致的 ISCP snapshot 初始化，并异步消费后续 tail。
- `ON DEMAND`：用户显式执行事务化全量刷新。

V1 提供三种刷新策略：

- `FAST` / `INCREMENTAL`：只有定义能够生成受支持的增量计划时才允许创建；
  运行期增量维护出错时失败关闭，不允许静默全量重算。
- `COMPLETE` / `FULL`：每次刷新都重新执行完整定义查询。
- `FORCE` / `AUTO`（默认）：能增量则增量，否则全量刷新。运行期增量失败时，
  必须先回滚 delta 事务，再在一致边界执行全量刷新。

对每个已提交的 MV watermark W，查询 MV 的结果必须等于在所有源表 W 时刻
执行定义 SQL 的结果。目标行、内部状态和 W 必须在同一事务中推进，禁止发布
W 对应的新结果却保留旧 watermark。

V1 是最终一致的，不承诺源 DML 与 MV 同事务提交，不提供统一的最大陈旧时间，
也不包含自动查询改写、定时刷新、分区刷新、窗口、Top-K 或级联 MV。

## 2. 范围

### 2.0 当前基础能力与仍缺少的经典功能

实现 PR 已经提供：物理 MV 生命周期、独立 ISCP consumer、一致 snapshot 初始化、
异步 tail 刷新、目标结果与 watermark 原子发布、1～16 个源表的变化跟踪、复杂
查询的一致全量刷新，以及单表 `WHERE`、计算型 `GROUP BY`、`COUNT`、`SUM`、
`AVG`、按受影响分组重算的 `MIN/MAX`、`SELECT DISTINCT` 和精确
`COUNT(DISTINCT)`。insert/delete/update tail 都转换成带正负号的行 delta。

本次 V1 增量扩展包括：

- 单表 `HAVING`，且不满足 HAVING、暂时不可见的组仍保留完整维护状态；
- 精确 `SUM(DISTINCT)` 和 `AVG(DISTINCT)`；
- 2～16 个直接源的 branch-local 增量 `UNION ALL`；
- 上述查询对应的 insert/delete/update；
- `FAST` 对不支持的构造返回明确原因，只有 `FORCE` 可以退化到全量刷新。

以下经典能力不在本次实现范围内，需要单独评审设计。

#### 增量 SQL 广度

- 等值 JOIN、外连接和 fact-to-dimension JOIN 的增量维护；
- `UNION DISTINCT`、`INTERSECT`、`EXCEPT`；
- CTE、子查询、递归查询和相关表达式；
- `ROLLUP`、`CUBE`、`GROUPING SETS`、`GROUPING`；
- `ORDER BY ... LIMIT` Top-K、窗口函数、event-time/session window；
- HLL、bitmap、percentile/quantile、median、histogram、用户自定义聚合等
  可合并或近似聚合状态；
- 嵌套和级联 MV，包括把 MV 作为另一个 MV 的源；
- 时间窗口淘汰、TTL 状态回收和迟到事件策略。

#### 刷新方式与一致性

- 在源 DML 事务中同步维护的 `ON COMMIT`；
- 按间隔、cron 或日历执行的定时刷新；
- `CREATE WITH DATA / WITH NO DATA` 和延迟初始构建；
- 并发/非阻塞手动刷新及显式取消刷新；
- bounded staleness/freshness SLA：按用户策略等待、报错或读取旧结果；
- pause、resume、rebuild、retry 和管理端任务重新分配。

#### 优化器与存储集成

- 自动把普通查询改写为使用合适的 MV；
- 分区级刷新、partition change tracking 和 partition exchange；
- 用户配置 MV 目标表/状态表的索引、聚簇、分布、保留期和存储位置；
- 在存储与维护预算下自动推荐、选择 MV；
- 同时考虑 freshness 与维护代价的统计信息和成本模型。

#### DDL、依赖与运维

- `ALTER MATERIALIZED VIEW` 和在线替换定义；
- 完整支持源表 ALTER/RENAME，并提供明确的 CASCADE/RESTRICT 依赖策略；
- 超出 V1“拒绝执行”规则的混合版本迁移与自动降级；
- 专门的 SHOW/EXPLAIN 状态：定义、刷新策略、watermark、wall-clock lag、
  最近成功/错误、重试、状态大小和 fallback 原因；
- 每个 MV 的准入、磁盘/内存配额、状态压缩、背压和过载隔离；
- 把目标、状态、源身份和 watermark 作为一个逻辑对象验证备份、恢复和 PITR；
- 更细粒度的 CREATE/REFRESH/ALTER 权限和租户级运维控制。

这些是 roadmap 差距，不是 V1 的隐含承诺。增加任一能力都必须更新本设计，或
通过独立后续设计明确所有权、兼容性、资源和验证契约。

### 2.1 可增量维护的定义

第一类增量查询是顶层单表查询，源必须是直接普通基表，可包含 row-local
`WHERE`、普通 `GROUP BY` 和以下聚合：

- `COUNT(*)`、`COUNT(expr)`、`SUM(expr)`、`AVG(expr)`；
- `MIN(expr)`、`MAX(expr)`，只重算受影响分组；
- 使用值 multiplicity 状态的精确 `COUNT(DISTINCT expr)`、
  `SUM(DISTINCT expr)`、`AVG(DISTINCT expr)`；
- 把 `SELECT DISTINCT` 当成无聚合输出的 grouping；
- 可选 `HAVING`，表达式只能依赖 group 表达式和受支持的聚合输出。

增量标量表达式白名单必须是确定、row-local 的，包括列引用、字面量、算术、
布尔/比较/NULL 条件、`CASE`、cast、`date_trunc`、`coalesce`、`ifnull`、
`abs`、`floor`、`ceil`。易变函数、子查询、窗口、嵌套聚合和未知函数不能生成
FAST 计划。

第二类是包含 2～16 个 branch 的顶层 `UNION ALL`。每个 branch 都必须满足
单表子集，只能有一个直接普通源，并且输出类型 union-compatible。相同源可以
出现在多个 branch 中，但物理源只注册一次；一条变化要在所有匹配 branch 中
分别求值。状态和隐藏行身份使用 `(branch ID, group key)`，因此不同 branch
产生的相同可见行仍保留为多行，符合 SQL bag semantics。V1 不支持不带 ALL 的
`UNION`。

insert、delete、update 都受支持。update 等价为：在 iteration from-boundary
读取旧行形成负贡献，在 commit boundary 读取新行形成正贡献，因此 filter、group、
distinct value 和 HAVING membership 的变化都能统一处理。

### 2.2 全量刷新定义

FORCE 和 COMPLETE 可以接受 1～16 个直接普通基表上的顶层查询，前提是普通
planner 能执行并且每个源都支持 snapshot read。JOIN 等不能增量的定义在共同
ISCP 边界全量重算。FAST 对 2.1 以外的定义返回稳定的 `NotSupported` 错误，
并指出第一个不支持的构造。

外表、临时表、cluster/source/subscription relation、普通 view、MV 和内部状态表
不能作为源，避免未定义的 snapshot、生命周期、所有权和递归维护语义。

## 3. 架构与所有权

### 3.1 Planner 负责持久化规格

SQL planner 校验定义，并把带版本号的增量描述序列化到表元数据，其中包括：

- 规格版本和策略；
- 有序源身份、每个源的 branch 描述及稳定 branch ID；
- source alias 和求 delta 所需列；
- group 表达式、filter、HAVING；
- 聚合类型、输入、可见输出列和隐藏状态列；
- exact-distinct 状态索引和内部状态表身份。

序列化规格是持久化兼容契约。未知版本必须失败关闭；新增可选字段必须兼容旧
版本解码；改变已有语义必须升级版本并定义混合版本规则。

Planner 是 SQL 增量资格的第一责任方。Consumer 不能在运行期猜测任意 SQL
是否可增量。

### 3.2 ISCP 负责源进度

一个 ISCP job 持有有序源集合及每个源的 watermark。初始构建从历史数据开始，
取得共同一致 snapshot，并调用与 tail 相同的 consumer。禁止另走 CTAS 或
InitSQL 初始化目标结果。

多源 job 只有在所有源都推进到共同 `toTS` 时才允许发布 iteration。由于目标
变化与 watermark 原子提交，重复投递不会重复应用。重启从持久 watermark 恢复，
不能重新创建或跳过 snapshot generation。

ISCP executor 负责调度、取消、重试、源消失和投递 buffer；consumer 不维护
独立进度 cache。

### 3.3 Consumer 负责目标和状态

Consumer 每个 iteration 只拥有一个 refresh transaction。它消费所有源 batch，
在 from-boundary 恢复删除行，计算带符号的 branch delta，并在调用普通 ISCP
watermark update 之前更新状态和可见行，所有操作在同一事务内。

物理 MV 目标保存可见列、隐藏聚合状态和序列化 group key。内部状态表保存：

- 按 aggregate、group、序列化 value 索引的 distinct multiplicity；
- 用于重算和 HAVING 发布的 affected group key。

禁止用户直接修改目标表和状态表。删除 MV 时注销所有 job generation 并删除
状态。源表被删除后，维护任务和 MV 查询都返回 source missing，不能把陈旧结果
当成有效结果返回。

## 4. Delta 语义

每个满足条件的源行经 planner 生成的 branch projection 得到 branch B、group G、
aggregate input 和符号 S（-1 或 +1）。目标 DML 前按 `(B,G)` 合并 delta；不能仅
因为可见值相同就合并不同 UNION ALL branch。

- COUNT：`COUNT(*)` 加 S；`COUNT(expr)` 仅在输入非 NULL 时加 S。
- SUM：维护 `(sum, non_null_count)`，删除最后一个非 NULL 输入后结果恢复 SQL
  NULL，而不是 0。
- AVG：维护 `(sum, non_null_count)` 并计算可见商值。
- MIN/MAX：记录 G，并在共同 `toTS` 只重算受影响组。
- exact DISTINCT：更新 multiplicity M(G,V)。只有 0→1 和 1→0 时才改变可见
  COUNT/SUM 状态；AVG(DISTINCT) 由 distinct sum 和非 NULL distinct count 得到；
  NULL 遵循普通 SQL 聚合语义。
- group row count 为 0 时删除目标行和内部状态。

HAVING 在 post-delta 聚合状态上求值。只在 HAVING 中出现、没有出现在 SELECT
中的受支持聚合也要规划为隐藏状态。非空 group 即使暂时不满足 HAVING，也必须
保留完整维护状态，因此不能只用用户可见目标行承载状态：HAVING 定义把完整 group
状态放在内部状态表，仅把满足条件的 group 投影到用户目标。false→true 插入，
true→false 删除，true→true 更新，false→false 只改状态。

group key 和 distinct key 必须使用保留类型并带显式 NULL marker 的编码；相等性
必须符合 SQL grouping 语义。除非同时保存编码值并解决冲突，否则不能只用 hash
作为身份。

## 5. 事务与失败行为

目标 DML、状态 DML、watermark update 共用一个事务，commit 是唯一发布点。

- parse、bind、row lookup、delta size、state、target 或 watermark 任一错误都
  回滚整个 iteration；
- FAST 记录错误并按 ISCP 策略重试，不能 fallback 到 COMPLETE；
- FORCE 只有在失败 delta 事务确定回滚后，才能开启新事务并在同一边界全量重算；
- cancellation/timeout 必须回滚并释放所有 batch 和 reader；
- 同一边界重试必须产生同样的最终状态；
- 源或目标消失时，维护任务以持久错误终止；
- 改变源列身份/类型的 DDL 必须被拒绝或使 job 失效，禁止旧规格静默绑定新列。

Delta SQL 按行数切块，并限制序列化大小。Consumer 不能把整个无界 iteration
保存在内存里，必须渐进处理和释放 batch。内部状态随活跃 `(group, distinct value)`
基数增长，multiplicity 归零或 group 为空时必须回收。

## 6. 语法与兼容性

标准形式：

```sql
CREATE MATERIALIZED VIEW mv
  REFRESH FAST ON CHANGE
AS SELECT ...;

CREATE MATERIALIZED VIEW mv
  REFRESH COMPLETE ON DEMAND
AS SELECT ...;

REFRESH MATERIALIZED VIEW mv;
DROP MATERIALIZED VIEW mv;
```

省略 refresh 子句保持 FORCE ON CHANGE。已有 V1 metadata 必须继续可解码。新规格
记录最低 executor capability；只有所有可执行 CN 都报告该能力时才能创建，旧
executor 必须拒绝任务，不能解释或降级 FAST。目标、状态、源身份和 watermark
作为一个逻辑功能参与 backup/restore。缺少 catalog feature-version 协商时，降级
到不认识 MV metadata 的二进制前必须先删除 MV。

## 7. 备选方案

### 所有定义都全量刷新

实现简单且 SQL 通用，但会反复扫描 firehose，无法满足 freshness/cost 目标。
它保留为 COMPLETE 和 FORCE fallback，不作为可增量 SQL 的默认实现。

### 同步 DML hook

在每个源事务中更新 MV 可以做到提交后零陈旧，但会增加源写延迟、耦合多源事务，
并需要新的 DML ownership protocol，不适合本次 firehose 目标，因此不属于 V1。

### 每个源一个独立 ISCP job

独立 job 容易读取，但不能在共同边界原子发布 UNION/JOIN，重试也可能重复应用。
因此选择一个 multi-source job。

### 所有聚合都重算受影响组

可以简化 HAVING/DISTINCT，但热点 group 会接近全量扫描。V1 对可逆聚合使用代数
delta，仅对 MIN/MAX 或明确 fallback 使用 affected-group recompute。

## 8. 性能与可观测性

热路径目标复杂度为 O(changed rows + changed distinct keys + affected MIN/MAX
groups)，而不是 O(source rows)。SQL parse/plan 和目标写按有界 batch 摊销。
指标包括：

- 插入/删除源行数；
- 增量/全量刷新时长和结果；
- watermark wall-clock lag；
- fallback 次数；
- 每个 delta chunk 的行数和字节数；
- affected groups 和 exact-distinct state cardinality；
- retry/error 分类。

Observability workload 的验收要求是：持续 append/update/delete 下结果正确，内存
不无界增长，并报告 freshness p50/p95/p99/max。V1 不设置脱离硬件的统一延迟 SLA；
benchmark 必须同时报告硬件和无 MV 的源写入 baseline。

## 9. 验证计划

### Planner 和 parser 单元测试

- HAVING、SUM(DISTINCT)、AVG(DISTINCT)、2～16 branch UNION ALL 的 FAST 正例；
- UNION、JOIN、子查询、窗口、LIMIT/Top-K、易变/未知函数、不兼容 branch 类型、
  不支持源和超过 16 个源的稳定 FAST 拒绝；
- 同一不支持 SQL 在 FORCE 下不生成增量规格；
- 规格版本 round-trip 和旧版本 decode。

### Consumer 单元测试

- filter/group 的 insert/delete/update 符号转换；
- HAVING false/true 完整迁移矩阵，并校验隐藏状态；
- DISTINCT duplicate、最后一次删除、NULL、负数和 decimal；
- 同一源出现在多个 UNION ALL branch，以及一个 iteration 包含多个源；
- duplicate delivery、rollback、retry、cancellation、超大 delta、源消失、
  watermark failure；
- target、state、watermark 原子性。

### SQL BVT

使用最少确定性数据；仅在验证异步公开边界时进行有界、可观察条件轮询。每种新
查询覆盖 snapshot 和 tail insert/delete/update，并把 MV 与原定义查询比较。使用
测试独占的干净 mo-service，同一 case 连续运行两次并验证清理。普通 BVT 不放大
数据量，也不做延迟断言；这些属于 benchmark。

### Benchmark

- TPC-H Q1 派生单表聚合配合 RF1/RF2，验证 HAVING 和 DISTINCT；
- DBToaster 风格 mixed insert/delete，比较代数增量和 affected-group 维护；
- 带 marker row 的双源 UNION ALL event stream，测 freshness；
- SQL 子集兼容后运行 RTABench pre-aggregated queries。

统一报告 snapshot 时间、有/无 MV 源写吞吐、freshness p50/p95/p99/max、drain time、
MV query latency、CPU/RSS/disk I/O 和最终精确正确性。只有 SQL 语义、硬件、时长、
writers、batch size、源持久化设置一致时，公开横向比较才有效。

## 10. 发布与决策记录

功能通过现有 MV 语法发布，FAST 失败关闭，FORCE 保持兼容默认。运维人员通过
ISCP job state 和 MV metrics 诊断 lag/error；删除 MV 是回滚路径。

待评审决策：

1. HAVING 使用独立完整 group state；不满足 HAVING 的 group 不出现在 target，
   但不能丢维护状态。
2. UNION ALL 使用隐藏 `(branch ID, group key)` 身份，保留不同 branch 之间的
   重复可见行。
3. exact DISTINCT 状态持久化，并由 live source cardinality 和账户普通存储配额
   限制；iteration 内存由 delta chunk 限制。增加 MV 专用硬配额前必须先提供
   state-cardinality metrics。
4. 新规格记录最低 executor capability；创建时检查所有可执行 CN，旧 executor
   拒绝任务，不使用 task pinning。
5. 在 catalog feature-version 支持安全自动降级前，降级需要先删除 MV。

本设计不故意遗留阻塞实现的问题。上述决策发生变化时，必须先提交并批准新的
设计 revision，才能继续实现评审。

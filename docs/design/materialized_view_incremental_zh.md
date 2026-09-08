# 物化视图：当前实现契约（D2）

本文对应本 PR 实现的聚合与 UNION ALL 维护方案。目录定义格式为 1，创建要求协议
56；本文不把尚未实现的算子图格式列为当前契约。

## SQL 与刷新策略

`CREATE MATERIALIZED VIEW ... REFRESH FAST|FORCE|COMPLETE ON CHANGE AS SELECT ...`
创建由 ISCP 维护的物理结果。INCREMENTAL 等价于 FAST，FULL 等价于 COMPLETE，
默认 FORCE ON CHANGE。COMPLETE ON DEMAND 使用 `REFRESH MATERIALIZED VIEW` 在
调用者事务中刷新；FAST、FORCE ON DEMAND 不支持。

FAST 接受规划器能够完整维护的确定性单源分组聚合，以及状态布局兼容的 UNION ALL
分支，包括 COUNT、SUM、AVG、MIN/MAX、DISTINCT 聚合及 HAVING。NULL 分组和聚合
输入遵循 SQL 语义。FAST 对不支持的表达式、集合操作和不兼容的分支状态明确报错。
FORCE 对超出增量子集的定义选择 COMPLETE；COMPLETE 支持最多 16 个直接基表来源，
包括连接。临时表、外部表和视图不能作为来源。

同一基表在 UNION ALL 中出现多次时，每个分支保留独立身份。调度来源去重，但一条
来源变更必须应用于所有对应分支。增量准入前统一检查输出别名和隐藏状态布局。

## 唯一权威定义与对象身份

`pkg/catalog/mvdefinition` 统一编解码和身份检查。目标表的目录属性
`mv_definition` 保存格式、能力、租户、目标库表 ID 和名称、generation、规范 CREATE
及刷新 SQL、刷新策略、可见列、去重后的来源库表 ID/名称/显式 schema version，以及
可选增量描述与辅助表身份。

generation 1 不可修改；DROP/CREATE 产生新的目标 ID。不按名称猜测迁移或重新绑定。
单源增量描述使用版本 2，UNION ALL 使用版本 3。未知、缺字段或超限定义在执行前拒绝。
ISCP 任务只保存目标 ID、generation、SHA256 摘要及派生调度来源，不再复制 SQL 和状态
布局。消费者在刷新事务中读取目录定义并验证目标、租户、来源和辅助表所有者。

查询规划及每次缓存计划执行都验证来源 ID 和 schema version。来源 DROP/重建、
TRUNCATE、改名或改变 schema version 的 ALTER 会使旧 generation 失效；查询与刷新
报错要求重建，DROP MV 仍可清理该对象。

禁止公共 DML 和 ALTER/改名/TRUNCATE 修改目标或辅助状态。普通 COMMENT 不代表
内部对象身份。公共 CREATE TABLE 拒绝 `mv_*` 属性及 `__mo_mv_state_` 名称。
内部刷新通过私有 context capability 精确授权租户、目标 ID、generation 和辅助表 ID；
通用 internal-executor 标志没有写权限。

## 创建、事务与清理

CREATE 在同一事务中解析并锁住来源，创建目标及可选辅助表，保存实际 ID，最后发布
ON CHANGE 任务。辅助表记录租户、所属目标 ID/generation 和自身 ID。消费者不再用
`CREATE TABLE IF NOT EXISTS` 按名称重建状态。

初始快照的目标和辅助数据原子替换。初始 watermark 可随后确认；这段间隙崩溃会重新
执行替换。增量刷新把结果、状态和 watermark CAS 放在同一事务。每条增量 DML 推进
statement boundary，使后续语句能看到此前 workspace 写入。

刷新按稳定顺序持有与 DDL 相同目录键上的共享锁，CAS 阻止过期 worker 发布。只有
增量事务成功回滚后的可恢复错误才允许 FORCE 全量替换。取消、超时、身份/格式错误、
资源超限、CAS 失效、回滚失败和提交结果不确定均禁止 fallback。

删除行按各自 tombstone 提交之前的快照重建，不能统一读取整个迭代起点。这样才能
处理同一区间内的插入和多次更新。历史回查复制变长数据、读取历史可见对象，并排除
指定快照之后的 tombstone；找不到历史数据必须报错，不能发布部分结果。

DROP 先按目标 ID 注销所有活跃任务 generation，再删除拥有的关系。来源改名不能
改写任务目标名称。内存任务 GC 使用相同时间单位比较。损坏任务逐个隔离，不阻断其他
任务的日志回放。

## 资源与取消边界

最多 16 个来源/分支；目录解码最多 1 MiB。增量数据在复制之前分块，每块最多
32,768 行和 8 MiB 计量暂存内存。多个删除快照组共享一次回查预算；单行超限在复制
前拒绝。生成 SQL 不超过 8 MiB。处理完一块后再推进借用批次，扫描检查取消，迭代器、
批次、SQL 结果和事务沿各自所有权路径清理。

异步刷新继承有限的迭代 deadline；手动刷新最长一小时。worker 和共享流沿用 ISCP 的
有界准入、引用释放、取消与 drain。持久 DISTINCT 多重度及受影响分组状态使用数据库
存储配额，删除零计数和已处理标记；不在每个增量上增加全表 COUNT 扫描或任意行数限制。

## 升级与回退

所有创建（包括 ON DEMAND）要求协议 56。除准入检查外，旧执行器也会在既有边界拒绝
新对象：任务落盘使用已有 IndexSync 枚举、空 index selector 和新增 MVReference；
旧 writer registry 在消费者构造及 SQL 执行之前拒绝空 selector。新解码器验证引用后
恢复内存中的 MV 类型。

目标和辅助表持久化为 view kind，并保存 MATERIALIZED CREATE 语法屏障。当前目录
读取器依据内部元数据投影成可扫描的物理目标/状态；旧版本保留 view 定义，查询无法
解析该语法，DML 则被 view kind 拒绝。这也覆盖没有后台任务的 ON DEMAND 对象。

不支持混合版本下的 MV 可用性。读取、刷新和清理使用具备能力的版本，二进制回退前先
删除物化视图。本未发布 PR 早期版本的无 envelope 元数据需要重建。组件兼容测试运行
真实前驱版本的任务解码/消费者构造及 SQL 解析路径，不声称支持整集群降级或自动迁移
早期分支目录。

## 验证与性能准入

定向测试覆盖格式/租户/generation、精确权限、回放隔离、历史数据所有权、删除快照、
分块容量/取消、事务路由和清理时间。BVT 通过独立来源聚合比对快照、追加、删除、更新、
刷盘、UNION ALL 和完整替换。身份回归覆盖普通 COMMENT、保留属性、prepared 查询、
来源 DDL、手动事务回滚和改名后的 DROP 清理。重启验证持久化目标和任务引用；共享
生命周期修改需要对应的 race 验证。

参考环境须先证明无 MV 对照能持续写入 6,000 行/秒；一个代数聚合 MV 在该负载下
p99 新鲜度不超过 5 秒，最大不超过 10 秒，来源吞吐下降不超过 20%，突发排空时间不
超过突发持续时间的两倍。活跃分组/值不增长时，逻辑辅助状态不得持续增长。PR 必须记录
实际测量、环境和配置，不能用单元测试通过替代这些准入条件。

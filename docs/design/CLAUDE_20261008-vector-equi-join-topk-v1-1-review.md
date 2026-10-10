# 等值 JOIN 向量 Top-K：已批准的落地成本策略

日期：2026-10-08。批准日期：2026-10-09。状态：用户批准方案 A；仅替换 v1 第 4.3 节完整成本比较要求，不扩大正确性或执行机制范围。

## 已验证的实现

保留原 INNER JOIN、复制并重绑定 eligibility SEMI 子树、候选预算 K+OFFSET、最终分页、required membership 与保守 one-CN 均可复用现有机制。公开 planner、plan/compile owning packages、IVF 生命周期、vectorscan race 和单机实际 SQL 已通过。1536 维复核返回正确的重复行，与 FORCE 一致，区别于 IN。

## 发现的 v1 差异

v1 第 4.3 节要求默认成本选择包含额外资格扫描/JOIN、B 再次读取、候选回表和重复展开。当前接口核验结果：

- `applyLogicalVectorIndexForSortContext` 通过 `LogicalSearchHooks.BuildLogicalSearch` 尝试改写。
- `applyVectorIndexForSortContext` 通过 `Plan().ApplyForSort` 尝试改写。
- IVF 的这两个入口最终都进入既有 `ApplyIndicesForSortUsingIvfflat`。
- 现有接口提供能力判定及既有 PRE/AUTO 策略，但没有对原 JOIN 与新增复合候选计划进行完整成本比较的接口；不能把 scan 的 `Stats.Cost` 当作此比较已经存在。

当前实现复用既有选择策略，不新增完整 JOIN 成本比较。用户在明确“正确性检查通过且索引可用就优先走 vector index，完整成本模型以后再做”后回复 `go ahead`，批准该取舍。成本比较不是正确性前提，也不要求专门接口；原成本门禁已由用户关闭，其它交付验证继续执行。

## 方案决定记录

### A：首版沿用既有默认选择策略（已批准）

允许普通 LIMIT 与显式 PRE 使用当前原型的索引路径。明确默认模式不是新增的 cost-based JOIN selector，额外 producer、B 双读及重复展开可能让小表/高重数查询更慢。保留 FORCE 和所有证明失败的旧路径；不承诺加速倍数。

完整成本模型作为后续独立优化，先校准已有 scan/JOIN/distance 成本单位与统计来源，避免本次用未经测量的常量或阈值伪装成本决策。用户已批准 A，仅更新已批准设计的这一项，不引入新执行机制。

### B：本次必须实现完整成本选择（未采用）

维持 v1 的要求；当前实现不能交付。先另行设计比较模型、统计缺失策略、插件入口及基准证据，经 review 后再继续生产代码修改。不能临时添加算法名分支或未经定标的宽度/行数阈值。

## 其它保守边界（无新增机制）

索引列须可证明非 NULL（NOT NULL 或已有显式非 NULL 过滤）；查询向量须可折叠为非 NULL 常量。未证明的参数/变量、NULL 向量继续走旧路径，避免将 NULL 排序误当成空 ANN 结果。参数 LIMIT、固定向量的 PREPARE 重执行已有验证；不声称所有动态向量参数都命中索引。

部署范围仍是保守 one-CN；并未增加分布式资格证明或新的 wire/catalog 字段。

## 批准后交付验证

2026-10-09 已更新至 mo/main `aee2a0b3764781a8ca908618548dfef75e336263`，planner/compile UT、vet、CI 版本 v2.14.0 增量 lint 均通过。两 CN 集群从 6101/6102 两入口执行唯一键、重复键、OFFSET 和反向 JOIN，均命中索引、保持 one-CN 计划且结果与 FORCE 一致；两轮含 metadata 的正常 BVT 比较各 65/65，零失败/忽略。验证没有扩张本设计范围，也不作为加速承诺。

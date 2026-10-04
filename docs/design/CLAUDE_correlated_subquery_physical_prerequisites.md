# 多层关联子查询：独立物理前置修复（r2）

## 范围与设计门禁

关联 #7559，但本 PR **不实现或关闭该 issue**。用户授权按既定 r3 方向推进 PR 系列，本次仅修复调查中发现的三个既有合法 SQL 问题。通用 decorrelation 的 owner/依赖替换、类型域、source admission、条件求值需求域和规划预算仍是后续设计门禁；本次没有启用通用 lowerer，也没有扩大物化准入。

这三个问题不依赖实验性的参数域生成器、共享 SINK 注入或内部开关：`pkg/tests/sqlintegration/multicn/join_stage_placement_test.go` 用普通 SQL 即可复现。以 `239fe81c82` 的两个生产入口制作只读 Go overlay、保留同一测试，得到以下反例；不将其称为干净工作树执行。

| 输入形状 | 未修复结果 | 正确结果契约 |
| --- | --- | --- |
| ROW_NUMBER 派生表作为 LEFT JOIN probe，另一 CN 扫描 build | 匹配行被错误扩展为 NULL，6 行变成 4 行 | 保留两个重复 outer key 对应的全部匹配，以及合法 NULL 扩展 |
| 分组 ROW_NUMBER 派生表作为 JOIN build | `20101: unexpected operator: Window` | 每组最后一行参与 JOIN |
| 数值归一化域 + COALESCE(MAX(...), 0) | `20301: string literal form requires a string literal and string type` | 结果为数值，缺失/NULL 聚合结果补 0 |

## 不变量与变更边界

### 1. 字符串来源不能超出结果类型

`possibleStringDomainsForExpr` 的 CAST 分支先检查静态结果域：结果类型不可能属于字符串域时直接返回 0。否则会把空字符串 witness 安在 BIGINT 等数值类型上，在远端表达式校验处失败。

保留字符串 CAST 的已有来源推导以及 literal 校验；不修改 protobuf、字符集/排序规则兼容范围、数值类型或 SQL 比较语义。UT 分别覆盖 BIGINT、DOUBLE、DECIMAL128 与 VARCHAR、VARBINARY 对照。

### 2. WINDOW 阶段不能被 JOIN 搬到远端

当前 `compileWin` 将 WINDOW 放在协调 CN，远端 instruction 编解码没有 WINDOW 支持。后续 JOIN 必须检查整个输入 scope/operator 树，而不仅检查根算子。

若存在 WINDOW 且后续放置可能移动它，则复用既有协调 CN Merge 边界并关闭**该 JOIN 阶段**的 shuffle。保留扫描的远端地址，不将整条 SQL 强制变成单 CN。无 WINDOW、无诊断限制的 shuffle 路径保持原逻辑。

### 3. JoinMap 发布者与消费者必须在同一进程

JoinMap 通过当前 CN 的 MessageBoard 交接，不能把远端 HashBuild 直接当作本地 HashJoin 的 build。单个 probe scope 不代表二者共址。失败路径中，本地未执行的 HashBuild 被清理时还可能发布合法的 nil map，使错误放置伪装成空 build；本次不修改这个既有空表/清理语义。

在构造广播 JoinMap 前，通过已有 Merge/Connector **传批次而不是传进程内 map**：

- 单 probe + 单 remote build、地址不同：先归并到协调 CN，再构造 JOIN/build。FULL/RIGHT/DEDUP 等可能先将多个 probe 归并到协调 CN；广播阶段必须在确定**最终** probe owner 后共址 build，不可在已安装 HashJoin 后包 Merge。
- 本地 SINK_SCAN 或 foreign 执行依赖：广播阶段保留其 owner，扫描仍可远端执行。
- 所有根 scope 已位于当前 CN 时保留原路径，包括本地并行与多 probe SINK；共址不要求单 scope/Mcpu=1。同一 remote CN、普通可搬移 local build，以及普通多 probe 广播也保持现有路径。
- force-one-CN 形状先保护 SINK_SCAN/foreign 本地依赖，再确定 FULL/RIGHT/DEDUP 最终 probe owner，随后再次共址 build；普通多 probe 不额外合并。ASOF build-left 同样使用该边界，交换后的真实 probe/build 才是判断输入。
- 原有 owner-aware shuffle Source 路径不被广播修复无条件降级。

选择协调 CN 是明确的正确性优先方案，不是最优成本模型或性能提升声明。未来若选择其他 JOIN owner，应显式建立输入批次传输边界，不得只改地址标签，也不得把缺失 JoinMap 当成空表补救。

## 所有权、终止和上界

- Scope/operator 仍由 Compile 持有；新增 Merge 接管原输入作为 PreScopes，并添加既有 Connector。没有脱离查询的新 producer、Source、reader、锁、定时器或后台 goroutine。
- `Scope.MergeRun` 启动依赖，远端扫描通过已注册接收端发送批次；HashBuild 和消费者在同一 owner 的 MessageBoard 交接。正常 EOF、query cancel、远端失败继续走既有 pipeline cleanup/reset；不改变 terminal 发布次数和空 build 表示。
- 新边界沿原输入依赖方向连接，不增加回边。接收等待仍由输入结束、错误或查询取消解除，不依赖最终消费者额外驱动未注册的 producer。
- 每个被纠正的 JOIN 最多调用两次既有输入归并构造器，内部连接开销受输入 scope 数量约束；不增加无界缓存或持久资源。现有 channel/批次/查询预算及 scope 回收负责其生命周期。
- 不新增 wire、磁盘、catalog、配置或权限字段；旧二进制回滚不需要迁移。原有 WINDOW 不可远端执行的事实不会因混合版本而被绕过。

## 验证与测试架构

- 轻量 typed-scope UT：共址/异址、local sink、本地并行/多个 probe、WINDOW 两侧及已共址并行对照；实际调用 `compileJoin` 验证该阶段 shuffle 被关闭且扫描地址保留，另保留普通安全路径对照。新增多个远端 probe + 一个远端 build 的 FULL/RIGHT、DEDUP、local sink 和普通 INNER 对照：旧代码 FULL/RIGHT 在最终本地 Join 下挂远端 HashBuild，修复后同 CN。
- 单独 `multicn` 测试进程：一个两 CN fixture，两个表共 7 行。入口 CN 在查询期间 draining，强制扫描到另一 CN；比对完整结果、LIMIT 1、空输入和 PHYPLAN 远端地址。DDL/清理前恢复入口 CN；SQL 连接、全局测试开关及 cluster 均注册清理。
- 独立子包是必要的隔离边界：共享单 CN 集成包已经持有进程内完整 cluster 准入，不能在其内部再启动第二个完整 cluster，也不应为该测试销毁共享 fixture。首次全包执行暴露这一约束，已据此调整。
- 公共 BVT：`test/distributed/cases/join/coordinator_stage.sql`，覆盖相同语义、prepare 重用和空输入；新增 FULL/RIGHT 的匹配、probe-only、build-only 结果。结果经 mo-tester 生成、人工核对、同实例普通比较复跑，并检查 schema 清理。当前两 CN SQL fixture 在旧代码也通过，说明它仅证明公开 SQL 语义不回退，**不证明该 fixture 实际产生多个 probe scope**；触发该路径的红绿证据是 typed-scope UT，不将公开测试虚报为失效前反例。
- 对 compile/plan 运行完整普通 UT；对两个集成包运行完整普通测试。另检查增量覆盖、静态分析、编译包 race 与精确两 CN 用例的按预算 race 重复。

后续通用多层关联方案仍需独立证明 demand/cardinality、空聚合重建、任意参数类型、原 outer relation 单次求值、晚到错误可见性及物化预算。本 PR 的公开 SQL 回归不充当这些能力的证据。

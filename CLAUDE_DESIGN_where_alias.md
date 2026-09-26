# WHERE 别名扩展设计 v1

关联 issue：#16244。实现 PR：待创建。用户于本轮确认默认关闭的显式开关方向；本文在实现前记录细化契约与设计审查。

## 门禁、证据及不变量
这是 SQL 兼容契约扩展，触发设计门禁。当前 main 的 SELECT 在绑定 WHERE 后才注册 SELECT 别名。MySQL 8.0 文档明确 WHERE 不支持 SELECT 别名：https://dev.mysql.com/doc/refman/8.0/en/problems-with-alias.html 。本功能是 MO 的 opt-in 扩展，不宣称 MySQL 兼容。

不变量：关闭时保持现有名称解析；开启时仅当前 SELECT 的 WHERE 可把未解析、未限定名称作为 SELECT 显式别名展开。任何原先可解析的真实列（含外层相关列）和列歧义均优先于扩展。反例：同名真实列被别名覆盖，或子查询得到外层 SELECT 别名。

## 方案与替代
1. 维持现状：兼容但不满足请求。
2. 默认开启、提前填充共享 aliasMap：会污染其它子句和相关子查询作用域，拒绝。
3. 采用 session-only `enable_where_alias` 布尔系统变量，默认 0、动态可设置、不支持 SET_VAR hint。仅为当前 WHERE binder 提供私有别名表；不改变共享 aliasMap，不移动 SELECT 的处理顺序。

## 解析契约
- 开启后，SELECT 星号展开完成时收集显式别名，WHERE 绑定结束立即移除；无 WHERE 或关闭时不收集。
- `WhereBinder.BindColRef` 只在 depth=0、未限定名称、所有可见 FROM 作用域均无同名列时尝试本层别名；歧义 FROM 列保持原错误。原生时间窗口边界 `_wstart`/`_wend` 同样优先，不被别名覆盖。
- 被引用的重复别名报歧义，未引用的重复别名不额外拒绝。
- 使用原 SELECT 表达式的独立 AST 副本；绑定展开表达式期间关闭该 binder 的别名扩展，防止自引用、循环与别名链，也避免修改原始 SELECT AST。
- 展开仍通过 WHERE binder，因此聚合、窗口、时间窗口函数保持拒绝；子查询走既有绑定及 flatten 流程，不增加跨层别名可见性。
- 表达式替换不是结果物化。SELECT 和 WHERE 分别求值，易变函数不保证同值或只求值一次；这与直接重复写表达式一致。不增加执行节点、缓存或逐行开销机制。
- JOIN ON、UPDATE/DELETE 原有 WHERE binder 没有私有表，因此不扩展；INSERT SELECT 内的 SELECT 仍按 SELECT 契约处理。

## 配置、缓存与生命周期
由前端系统变量机制验证类型与作用域。改变值后清除当前会话普通计划缓存，并将既有 prepared statement 标记 needsRebuild/compileNeedsRebuild，下一次 EXECUTE 使用当前变量重建，失败不允许执行旧计划。重复 SET 相同值不失效。会话请求按现有顺序执行；准备语句表在 session 锁内遍历，不引入 worker 或全局状态。
变量只影响当前连接，没有新目录格式、磁盘状态或协议字段。老版本不识别 SET，应用只应向已升级节点发送；关闭开关是回退方法。重连恢复默认 0，已有会话迁移使用既有系统变量重放路径（已确认迁移恢复通过 `SetSessionSysVar` 重放配置）。不增加权限或租户访问能力。

## 成本与失败路径
仅开启且有 WHERE 的 SELECT 增加 O(投影数) 的私有表；每次别名引用复制并绑定表达式，代价与直接重复表达式一致。状态由本次 WHERE binder 独占，正常/错误返回均释放引用。不添加 I/O、goroutine、重试或全局缓存。session SET 的失效成本 O(已准备语句数)，受现有会话准备语句上限约束。

## 验证及变更图
- R2 planner：普通列/表达式、真实列优先、真实列歧义、重复别名、限定名拒绝、自引用/循环拒绝、聚合/窗口拒绝、相关子查询作用域、direct/prepare。复用 planner mock 与 buildOneQuery；typed plan 检查别名引用绑定到正确列。
- R2/R3 frontend：变量默认、类型/作用域、值变化后的普通缓存清除及 prepared 失效、相同值不失效、session 隔离。复用轻量 Session fixture；无新并发运行模型。
- BVT：独立最小表、三行（含 NULL），SQL 开关、结果、失败路径、PREPARE/EXECUTE 切换、重复执行及清理。单 CN 足够：扩展在规划期完成，无分布式协议改动。
- 工具链 Go 1.26.4；UT 通过 mo-cgo-test；增量 gofmt/vet/lint；使用 mo-tester 生成并验证结果，不手工伪造结果。

## 实现前审查
决定：采用方案 3。第一 owner 为 WHERE binder 与 session 配置更新；所有下游仍消费现有计划表达式。以上名称优先级、宏展开语义、默认关闭和缓存失效闭合，未发现设计阻塞项。实施如改变这些边界，先更新设计并重新审查。执行验证缺口不得冒充通过。

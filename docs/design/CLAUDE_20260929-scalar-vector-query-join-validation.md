# SQL1 selector JOIN 边界修复与验证更正

## 范围与历史记录

本记录对应 PR #29493 的 JOIN 挂起 review 修复，代码和测试以本文件同次提交为准。红测整合基线为 `950fc3eaa5bb3d293d42321a0044a54ff0894a03`。交付前又合并 `mo/main` 的 `e5f70bedc47d84866359f903afed1da0f6562602`，得到 `4ecb07c4ae26e310d002f1f20c6c9e34b5f4ec1d`；plan protobuf 冲突通过重新生成解决。下述最终验证针对这一合并后的生产代码。

既有设计和批准记录仍绑定首次发布提交 `22a9a930f074ada65715181075892561d88e4877`；本记录不修改其历史内容/hash，不把后续测试结果或修复追认为历史用户批准。本次是恢复原设计的三个惰性分支不变量，不新增 SQL2/SQL3 优化或改变整查询协调 CN 限制。

## 已确认的启动职责错误与修复

原 `compileVectorQueryTop` 直接返回惰性 scope。上层 JOIN 复用该 scope，并将 hash-build 追加为第四个 PreScope。HashJoin 在 pull 输入之前等待 JoinMap；selector 只启动 provider 和所选结果分支，不能启动这个第四个依赖，因此查询只能靠取消/超时结束。

修复在 selector 编译输出边界增加一个普通 merge scope：

```text
普通消费者 scope（JOIN 的 build 作为并发依赖）
  ├─ 私有惰性 selector scope
  │    ├─ provider
  │    ├─ ANN
  │    └─ exact
  └─ JOIN build scope
```

selector 仍只拥有三个分支、两个 source reader；不通过同时启动 ANN/exact 或修改 reader 断言绕过问题。新边界复用已有 connector/merge、错误取消和 scope 清理协议，固定增加一个 scope/receiver/connector，不增加缓存、重试或全局状态。没有性能基准结论。

## 回归证明

- compiler 结构红测在修复前确认输出错误地仍是 LazyPreScopes。
- 公共 SQL planner 测试覆盖 IVF/HNSW：两个仅引用一次的 CTE 各自保留计算投影，使 JOIN 的两侧直接是 `VECTOR_QUERY_TOP`。显式检查只有一个 query step，排除物化 CTE 替代了待验证边界。
- 真实 HashJoin/MergeRun 单测覆盖 ANN 自连接、NULL fallback、零需求、下游错误和取消；检查结果/错误以及 scope、source、JoinMap 清理后 allocation account 和 mpool 归零。
- BVT 同时保留内联 CTE 和重复引用的物化 CTE 对照，包含空 provider fallback 与 PREPARE 参数复用；不以物化 CTE 自连接作为此次挂起的唯一 oracle。
- Go 1.26.4、本工作树 native 和仓库 CGo wrapper：完整 compile/plan/explain/vectorquery/materialized 普通测试通过，compile/vectorquery 整包 race 通过。新真实 JOIN 测试单次 race 0.02s，按 30s 预算截断到 N=100，独立重复通过。
- 本轮生产变更行 coverage-block 映射为 4/4（100%，含映射到 block 的注释行）；核心行为只有一个返回语句变更。增量 golangci-lint v2.6.2 为 0 issues。
- 真实服务使用新编译的 darwin/arm64 原生程序：单 CN 和双 CN 均为独立新数据目录的本任务 launch 实例；双 CN 为同一 launch 进程内的两个独立 CN 服务及 SQL/RPC 入口，不声称是本轮新建 Docker 分进程集群。
- mo-tester 生成结果经人工按三行 fixture 核对，再正常比较：扩展 SQL1 114/114、原 PREPARE 对照 49/49。单 CN 同实例两轮、双 CN 各入口一轮均通过，0 ignored，每次检查数据库无残留。双 CN 的 IVF/HNSW 物理执行计划和实际结果显示整个内联 CTE JOIN 查询只在入口协调 CN；未选 exact 分支 PrepareTime 为零。

## 历史 BVT 入口证据更正

在本轮隔离非默认端口验证时发现，当前 mo-tester jar 按工作目录的 `mo.yml`（否则 classpath 资源）加载配置，**不识别旧本地脚本传入的 `-Dconf.yml`**。

因此撤回首次设计发布文档验证段落及旧 PR 正文中“旧 77 条 BVT 分别直连 CN1/CN2 都已验证”的表述。此前相应脚本实际沿默认 6001 入口执行；它们可以证明该入口上的结果，不能证明标签所称的直接 CN 入口。先前通过明确 `mysql -P` 执行的物理计划/结果探针是另一组独立证据，不因这个 Java 配置问题自动失效。

本轮重新验证已在每个 mo-tester 工作目录显式放置指向隔离配置的 `mo.yml`，分别绑定 46001/46002，并将诊断端口指向本任务进程。上面的 114/114 和 49/49 结果替代旧的直接 CN BVT 声明。最初未就绪的配置尝试和误连无监听默认端口的 tester 超时不计入产品回归或通过证据。

# Issue #28998 验证与自审记录

## 版本与环境

- 独立分支：`issue-28998-redesign-main`；固定基线：`3063f44fe5527dc74dbb3887822214e6fefcc80e`。
- 参考 review：PR #29023，head `177f46d0b3db2086f68d8a83c94e8d2d8bc12748`；从头实现，未复用旧 no-op。
- R1 设计已获用户 `go ahead` 批准；批准内容 SHA-256：`8be757dff9840829a4ff76cb6acc428c8d0f8319133364cda5c7f2981c290a34`。
- 文档外最终代码、生成物、测试及结果文件的有序路径/内容 SHA-256：`a37a953e2f9267940d3e36f933f7334cb2f44321fb2e199544fa34a11baef8ae`。
- macOS arm64，Go 1.26.4；当前 worktree 构建 CGo/native；测试通过 `mo-cgo-test`；lint 2.6.2 使用 Go 1.26.4 构建。
- 自有本地集群：SQL 16998 / 16999，两独立 CN 进程，共享 TN/LOG；重启验证复用当前任务数据目录。未操作其他任务实例。
- 提交前刷新 main，较固定基线新增 4 个提交，均未触及本次修改文件；保持已批准开发基线。

## 变更闭环与自审

| 闭环 | 不变量与所有者 | 结果 |
|---|---|---|
| AST / proto / 计划 | 选项完整传递；proto 只追加字段；deepcopy 保留数据库代际 | parser 往返、plan/deepcopy UT；重新生成内容完全一致 |
| 初始化 / 迁移 | 每租户表先于 SCHEMATA；独立 4.0.9；协议 95；重复执行幂等 | 升级错误注入/混合协议 UT，真实 sys/tenant 旧 schema 升级 |
| CREATE / ALTER / DROP | 默认值行属于账户和数据库 ID；调用者事务拥有提交/回滚 | 生命周期 UT；事务 rollback、同事务 CREATE、同名重建 BVT |
| 权限与只读约束 | 按目标库检查 ALTER / DATABASE ALL / OWNERSHIP；共享写入准入 | CONNECT-only 拒绝，授权后成功，撤权后 prepared 拒绝；订阅库执行负例 UT |
| 继承 / 重试 | 列 > 表 > 目标库 > 旧库 server 回退；数据库共享锁内验证代际 | 跨库、临时表、LIKE、CTAS、ADD COLUMN BVT；完整 Compile.Run 重建 UT |
| SHOW / 变量 / 恢复 | 与持久值一致；历史默认值映射新库 ID；表恢复不改已有库 | snapshot/clone、PITR、跨账户恢复、DDL 重放、进程重启实际 SQL |
| 并发与终止 | ALTER 排他数据库锁；CREATE 共享数据库锁；错误由标准事务收尾 | 双 CN 的可观察锁等待、超时退出后成功重试；100 次聚焦 race，compile/frontend 全包 race |
| 交付 | 生成物来自生成器；结果来自 mo-tester；TODO 不提交 | 格式、diff 检查、增量 lint；相关 BVT 正常比较 |

所有变更 hunk（包括新文件、测试及生成物）均已检查并映射到以上闭环。正确性、消费者、状态/并发、活性、资源、容量、安全/兼容、操作成本、平台及测试结构均已按闭环检查。无新算子、异步 worker、索引算法、队列或后台缓存，不触发这些组件的专门审计。

### 资源与等待

- Q1：内部 SQL 返回的 result 在读取所有者内 `defer Close`；解码错误也关闭，UT 检查 mpool 回到原值。系统变量无事务读取的 BackgroundExec 由创建者 `defer Close`；AST Free 释放 charset/collation option。
- Q2：数据库 DDL 锁顺序为数据库再元数据行；沿用事务的取消/超时/回滚。过期 CREATE 计划返回标准定义变化错误，由 Compile.Run 重建完整类型；实际双 CN 等待通过 `mo_locks()` 观察，不用固定 sleep 猜测进入阶段。
- Q3：每数据库最多一行固定结构元数据；DROP 同事务回收；新库 ID 不复用旧行；无新增内存缓存、goroutine 或无界重试。
- 性能：新增工作集中于数据库 DDL、继承默认值的 CREATE 和相关元数据展示，每次按账户/数据库 ID 定点读取；普通 DML 无新增逐行工作。

## 实际验证结果

1. **修复前证据**：四个 ALTER DATABASE/SCHEMA parser 用例均语法失败；当前版本均通过。
2. **完整 owning-package UT**：mysql parser、tree、plan、compile、frontend、sysview、bootstrap、v2_0_0、v4_0_6、v4_0_9 共 10 包通过。Go test 的默认 vet 同步执行。
3. **聚焦升级集成**：`TestV409UpgradeDatabaseDefaults` 使用既有共享单 CN fixture，将 sys/普通租户恢复到无 defaults 表及旧 SCHEMATA 的状态；真实运行 4.0.9 handler 两次，验证不猜测旧库默认值，随后 ALTER/字符 MIN/MAX 生效。
4. **并发**：`TestDatabaseDefaultsCreateRebuildsAfterConcurrentAlter` 的 race 测量 case 耗时低于采样分辨率，按技能规则选 100 次；全部通过。compile/frontend 全包 race 各一次通过。
5. **新公共 BVT**：`database_defaults.sql` 由 mo-tester 生成结果，人工核对后连续两次正常比较；每次 104 条，104 成功、0 失败、0 忽略。
6. **旧公共 BVT**：18 个涉及 SHOW CREATE DATABASE 或 snapshot SHOW CREATE TABLE 的文件正常比较；合计 3962 条，3959 成功、0 失败、3 个原有忽略项、0 异常。只选取本次语义变化对应的生成结果，保留原有时间戳/ID/忽略项；部分分区用例仅同步公共 SHOW 输出，无分区语义扩展。
7. **覆盖率**：排除生成文件、测试文件和 mock，修改行与 Go cover 可执行区域交集为 **433/562 = 77.05%**；修改语句块为 354/432 = 81.94%，达到 75% 要求。保守地未计入其它包对 AST/clone 的间接覆盖和真实 SQL 覆盖。
8. **静态检查**：Go 1.26.4 gofmt；golangci-lint 2.6.2 对修改包使用固定基线增量检查，0 issues；`git diff --check` 通过。Parser 0 冲突；parser/protobuf 重新生成前后 SHA-256 一致。

### 独立服务验收

- 默认 server general-ci 时，review 六值 `a,b,c,E,C,D`：原表 MIN/MAX `a/E`，ALTER 后新表 `C/c`，COLLATION 为 bin。
- 无当前库时省略库名返回 No database selected；不存在库、未知/冲突 charset/collation、无权限均拒绝。
- 同一份数据重启后，默认值仍为 bin，新旧表 `a/B` 的 MIN/MAX 均为 `B/a`。
- CN A ALTER、CN B 二进制 prepared CREATE：EXECUTE 读取最新已提交默认值。
- CN A 未提交 ALTER 持锁，CN B CREATE 的实际锁等待可见；超时退出，A COMMIT 后 B 重试成功且继承 bin。
- SHOW 数据库/表 DDL 重放保留库 bin 与显式 general-ci 表。
- PITR 恢复修改前 bin；跨账户 snapshot 恢复映射目标 ID，目标 bin，源账户后续 general-ci 不受影响。

## 限制与交付说明

- 首批只支持 utf8mb4/general-ci 与 utf8mb4/bin，不新增 collation 算法，不转换既有表。
- 未支持的数据库选项明确拒绝，包含之前被忽略的 `CHARACTER SET utf8`；相应公共回归已更新。
- 持久默认值依赖协议 95 和 4.0.9 租户迁移；直接降级到旧实现不受支持。
- 本地验证平台为 macOS arm64；GitHub CI 状态与本地代码结论分开报告。PR 使用 draft，CI 由正常流程运行。

完整原始日志、覆盖率 profile、隔离启动配置和验收脚本保留在工作树的 `CLAUDE_*` 文件中；这些运行产物和 TODO 不进入提交。

## 2026-09-25 冲突与 CI 修复

- PR head `2a0ce98fbf` 已快进同步，并合并 `mo/main` 的新提交。主线的 prepared precision 占用 MORPC 95，因此本功能改为 MORPC 96，4.0.9 升级和所有数据库默认值门槛同步调整。
- 失败 UT 日志显示 engine/ISCP/CCPR 测试的 catalog 夹具缺少 `mo_database_defaults`；在共享的 `mock_mo_indexes` 夹具后创建该表，保留生产路径对表缺失报错的行为。
- 旧升级测试原先固定假设最新版本为 4.0.8；调整其模拟/期望至 4.0.9。重跑 `TestDatabaseDefaultsUpgrade`、`TestDatabaseDefaults`（frontend/plan/compile）、`TestSessionTenantUpgradeCancellationReleasesCNConsumer`、`TestV408LoginRepairsTenantCreatedAfterUpgradeSnapshot`，均已通过。`TestInitSql|TestISCPExecutor6` 已通过。
- 两个 Compose BVT job 在执行 SQL 前，Docker 拉取 `minio/createbuckets` 镜像返回 `unauthorized`，不是 SQL 断言失败；需在新 head 上重跑确认。旧 UT coverage 汇总随 UT 失败。扩大 engine 测试仍在执行。
- 扩大重跑 `TestISCP|TestUpdateJobSpec|TestCheckLeaseFailed|TestCancelIteration1|TestCCPR`，以及 CCPR 三个具体回归用例，均通过；对应日志为本地忽略文件 `CLAUDE_ci_repair_engine_broad_test2.log`、`CLAUDE_ci_repair_ccpr_test2.log`。合并后的测试未包含完整 Linux UT/覆盖率和 Docker BVT，须以新 head CI 判定。

# #28998：C03 验证与交付记录

## 范围与基线

- #28998 承接 #29482 的全部 C03 条件；重复 issue 的归并不视为功能完成。
- 设计：`CLAUDE_DESIGN_28998_C03.md`；延续用户批准的 r1。
- 基线：`mo/main` 的 `64b715a8234e272794185e2acc98ad58a8346c65`。保留已有合并历史，不重写分支。
- 主干已占用协议109 / 升级4.0.14，按获批顺延规则分配 C03 为协议110 / 4.0.15；最小升级4.0.14，offset=4。109 的原生 Unicode COLUMNS 和历史升级不回退。
- 工具：Go1.27.1、匹配 gofmt、仓库 `mo-cgo-test`、CI匹配 golangci-lint2.14.0；默认 vet 保持启用。本地平台为 Darwin25.6.0/arm64，CGo/native按仓库provenance校验。
- 推送前重新核实远端main仍为上述基线；现有draft PR #29374的base是main、head是`issue-28998-redesign-main`，保留其`df78c5a9d1`祖先并正常fast-forward，不另建PR、不force push。

## 验收地图

| 合同 | 实现所有者与证据 |
|---|---|
| 数据库默认持久化 | `mo_database_defaults` 按 account/database identity 存 identity/revision/version；planner/compile UT，真实 ALTER、重启、恢复 |
| database→table→column 继承 | 创建目标库而非当前库；显式覆盖、LIKE/CTAS 保留源域；真实 SQL、BVT 和 planner UT |
| session 临时表 | 既有 session DDL/COPY owner；真实 SQL 与 BVT 验证 ADD、DEFAULT不改旧列、后续ADD及MODIFY继承 |
| DEFAULT与CONVERT分离 | DEFAULT仅改变未来默认；CONVERT走既有COPY/类型赋值/索引重建；混合选项保留单独最终默认 |
| 数据与索引 | 索引/扫描等价、生成列、固定CHAR、二进制多字节容量、失败后源可用；MySQL8.4.11差分oracle及真实SQL |
| 生成列显式CAST | 严格验证最终赋值，保留用户CAST内层截断语义；`hex(v)=F09FA7AA`、`hex(g)=F09F`反例 |
| metadata一致性 | SCHEMATA/变量/SHOW CREATE；TABLES/STATUS读SchemaExtra；COLUMNS/FULL COLUMNS同域投影；非法generation不广告为合法默认 |
| 权限与准备语句 | text/binary EXECUTE重新检查当前授权；BVT PREPARE→REVOKE→拒绝；共享database-write admission处理当前库/保护库 |
| 并发新鲜度 | 两CN的text/binary prepared CREATE；连续两次提交version恰好+2；同名DROP/重建改变identity并完整replan |
| 混合升级准入 | 公共mo_ctl设协商floor109：C03请求拒绝、历史DDL仍成功；恢复110。服务清单先经其所有者强制刷新，不重试控制RPC |
| 回滚/commit失败 | 既有FJ_CNCommitAfterWorkspaceDumpFailed：数据库默认、表DEFAULT、COPY各自失败；旧域/索引/数据保留且无replacement残留 |
| 取消与清理 | 观察实际数据库锁waiter→公共KILL QUERY→错误返回→持锁者未释放时waiter清零→代际/默认不变→后续ALTER成功 |
| 恢复粒度 | table snapshot/PITR不覆盖当前database默认；database PITR恢复默认和索引语义；跨账户恢复重映射identity且不改变源 |
| 老租户升级 | 实际系统所有权升级事务、system/tenant双路径、重放幂等、不凭空生成历史默认行 |
| 持久重启 | 独立拥有集群，重开相同存储；变量、表默认新列、数据库继承与原生转换索引语义保留 |
| 支持域边界 | 保留已发布默认、兼容别名和原生UCA400身份；新latin1、禁用域及原生PRIMARY/UNIQUE保持关闭 |

分区表、存储过程、UDF及warning兼容性不在本轮范围；没有新增key format、缓存、后台worker或协议外的状态。

## 取消路径的第一性原理修复

1. 首次远端Lock请求失败，workspace仍只读且没有已确认lockTables，不代表未拥有资源：lockservice已保留indeterminate cleanup witness。
2. 删除txnOperator.unlock的上述早退；原lockservice按txn ID释放资源，不增加状态、重试或worker。解锁错误仍向调用者传播，重复终结不重复解锁。
3. 只为非空CommitTS执行RC的logtail可见性等待。空时间戳的只读/失败事务不能等待“当前logtail”：catalog replay必须先结束才能启动所等logtail，否则形成自身等待环。空时间戳仍解锁。
4. 原始失败与启动timeout堆栈保留在本地；没有延长原waiter断言、测试timeout或降低资源预算。

## Q1–Q3审查

- **Q1资源**：executor.Result由调用者Close；转换AST由本地defer Free；COPY目标/vector/index清理由既有事务所有者负责。ReplaceDef校验失败返回原错误，并在失败路径恢复charset/revision；真实commit故障验证没有半对象。
- **Q2等待**：数据库identity/generation在现有生命周期锁下核对；prepared依赖过期完整replan。锁RPC取消后的清理witness属于lockservice，不能依据workspace只读跳过。零CommitTS不建立catalog replay→自身logtail等待边。
- **Q3增长**：默认metadata按数据库identity归属，DROP删除对应行；无新worker/cache/无界扫描。COPY容量校验在最终赋值而非新增预扫描；使用已有有界预算。
- 反向消费者核对：DDL replay、LIKE/CTAS、SHOW、session变量、clone/snapshot/PITR、account恢复、升级、protobuf clone、表SchemaExtra和索引dedup证明。

## 验证终态

- 完整普通测试覆盖17个不同owning packages：bootstrap、v2_0_0/v4_0_6/v4_0_14/v4_0_15、catalog、defines、frontend、compile、mysql parser、tree、plan、function、sysview、disttae、txn/client、engine/test。最初15包成功证据仅复用相关语义未变部分；取消修复后的5个终结/消费者包重新全部通过；engine/test完整重跑161.077s通过。
- 双CN扩展测试普通r6通过，9.07s；包含混合floor、竞争写入、prepared identity及服务器锁等待取消。
- 精确race预算B=30s：双CN成功终态JSON T=14.21s，N=2，stress通过。两个新增事务终结UT终态JSON显示0.00s，实测低于输出精度上界时预算公式clamp结果为N=100；不添加人为延时、不以包/墙钟时间代替，两项stress通过。
- 随后每个完整race owner各跑一次：txn/client11.479s、frontend35.564s、compile13.925s、disttae11.598s、multicn66.192s，全部通过。
- 最终SQL/recovery/restart/upgrade重跑全部通过：恢复10.64s、继承1.20s、持久重启12.47s、实际4.0.15升级10.41s。
- 15个受影响BVT在真实statement export启用且先观察到实际statement行之后正常生成；原`ddl_accounting_ok=1`保留，genrs154.63s通过。正常比较及teardown后同实例第二轮各3442/3442，failed/ignored/abnormal均0；两轮总测试182.43s，包184.312s。
- 最后一轮完整语义审查曾发现未启用export的私有embed驱动把计费断言生成成0；该证据作废，保留失败上下文。仅按生产collector/writer所有者补齐私有驱动，不改生产计费逻辑或SQL/wait_expect/预期1；重新生成、检查原断言和两轮比较均通过。生成结果的时间/ID等动态列沿原有ignore规则，无新增忽略。
- result仅省略末尾空cell分隔符；逐列证明ResultParser按header补空后值不变。当前main相对diff检查无尾空白错误。
- 最终vet exit0；golangci-lint exit0、0 issues；所有changed/new Go由匹配gofmt检查。
- 正常parser/protobuf生成及重新生成均exit0，parser与plan.pb.go的前后SHA256完全相同；主干ViewReference/ViewStep/UTF-8 lexer合同保留。仅交付改过schema的plan.pb.go，未改schema的生成噪声恢复main。
- 最终严格AST差分非生成可执行token行覆盖率 **602/715=84.20%**；changed-block辅助统计748/831=90.01%。仅采用成功终态profile，缺映射计未覆盖，满足≥75%门槛；不以辅助口径或失败profile替代严格口径。
- 自审结论：**PASS，零未解决blocker**。当前52个非生成Go源文件、31个测试文件、grammar/proto、15组BVT语义及列metadata、生成产物和交付文件逐hunk核对；所有mandatory本地验证已终结。未宣称远程CI已通过，也未自动评论/关闭issue或监控CI。

## 审查适用性与证据复用

- 数据库状态/权限/继承/恢复/取消属于R3：已映射执行时授权、tenant/identity/generation、事务/COPY/lockservice所有者、完整replan及Q1–Q3；故障、撤权、损坏、混合floor、DROP/重建、恢复和重启均有对应反例。
- AST/protobuf/metadata属于R2：检查parse/format/replay、编号兼容、正常生成、历史迁移与协议视图，表/列/变量展示和实际比较及索引一致。没有新的data-plane协议或常驻状态。
- fixture补齐属于R1：建真实新增catalog，不删旧oracle、不放宽预算；完整owning package通过。BVT属于R2：正常生成不是通过证据，必须检查语义、metadata、非零断言并做同实例重复比较。
- index-plugin注册、hooks/build tags、算法dispatch和ISCP/CDC接口未变，不需要新增算法注册/GPU验证；转换沿既有统一重建路径，约束与索引反向消费者已核对。没有新的queue/cache/worker；普通继承是定点PK查找，DEFAULT不扫描旧数据，显式CONVERT仍由既有有界COPY预算拥有。

关键本地证据（不提交）：`CLAUDE_C03_final_owners.log`、`CLAUDE_C03_cancel_owners.log`、`CLAUDE_C03_cancel_full_owner_race.log`、`CLAUDE_C03_cancel_final_e2e.log`、`CLAUDE_C03_engine_test_owners_r2.log`、`CLAUDE_C03_delivery_diff_coverage.log`、`CLAUDE_C03_final_bvt_export_genrs_r4.log`、`CLAUDE_C03_final_bvt_export_compare_twice_r3.log`、两轮mo-tester report及生成SHA256记录。

## 证据与交付隔离

本地完整失败/成功日志、JSON、profiles、oracle、tester配置和私有缓存不提交。所有coverage仅采用成功终态profile；缺映射计未覆盖，生成代码/测试/纯声明不进入可执行差分分母。TODO、临时驱动、工具及工作目录不提交。

只交付源代码、测试/result、批准设计与本记录；正常fast-forward推送现有PR head并更新draft PR #29374。保留独立worktree供未合并PR后续维护；所有测试拥有的服务与export collector已关闭，未操作其它worktree的服务、文件或缓存。实际交付提交与PR head由推送后核验，不以本记录创建或历史覆盖率代替。

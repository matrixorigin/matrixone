# #28998：C03 验证与交付记录

## 当前状态（本地适用验收与审查已闭环）

以下原始验收记录对应历史交付 `1511bb6b9499bb354418d34d1e863bfd7a27703f`，不代表远端 CI 已通过。实际 run `38076256462` 的可复现UT/BVT失败及上游冲突已修复；维护提交为 `7a462de0f3c53ce3fcff88c1ae4f7e3198467850`。前三次pure merge冻结 `5f94c8f88a469e0e5053668bfca89770481a9595` / checkpoint `bb6d76360811a51a34566a9922a39118ffe975e8`，该范围完整owner/消费者/race/54完整public两轮/覆盖/static/正常生成均通过。最新冻结上游为 `f09d4788fd4d1aa30aae0e2f49d014589e7566c0`，独立第四次pure merge为 `a660a0fd7b353c50dbf65ce270795a4e4ec72401`：完成相关增量重验与明确资格复用，不将旧证据无条件换标签。当前严格覆盖率84.73%，没有未解决的本地审查/执行阻塞；实际提交与PR head以交付后核验为准。Darwin/embed成功不等于Linux CI green。

## 原交付范围与基线

- #28998 承接 #29482 的全部 C03 条件；重复 issue 的归并不视为功能完成。
- 设计：`CLAUDE_DESIGN_28998_C03.md`；延续用户批准的 r1。
- 基线：`mo/main` 的 `64b715a8234e272794185e2acc98ad58a8346c65`。保留已有合并历史，不重写分支。
- 主干已占用协议109 / 升级4.0.14，按获批顺延规则分配 C03 为协议110 / 4.0.15；最小升级4.0.14，offset=4。109 的原生 Unicode COLUMNS 和历史升级不回退。
- 工具：Go1.27.1、匹配 gofmt、仓库 `mo-cgo-test`、CI匹配 golangci-lint2.14.0；默认 vet 保持启用。本地平台为 Darwin25.6.0/arm64，CGo/native按仓库provenance校验。
- 原交付时核实上述main基线；本次维护采用后文冻结的最新上游输入。现有draft PR #29374的base是main、head是`issue-28998-redesign-main`，保留其`df78c5a9d1`祖先并正常fast-forward，不另建PR、不force push。

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

## 原交付验证终态（历史）

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
- 历史交付记录曾报告本地 PASS；后续实际 CI 失败已使其不再适用于当前维护。原审查工具多次截断输出，文件清单/hash不能独立证明所有未显示 hunk 都经过完整检查。当前重新建立非截断审查与验证记录；没有把历史结果或 repair 小范围覆盖率替代当前完整交付结论。未自动评论/关闭 issue 或监控 CI。

## 审查适用性与证据复用

- 数据库状态/权限/继承/恢复/取消属于R3：已映射执行时授权、tenant/identity/generation、事务/COPY/lockservice所有者、完整replan及Q1–Q3；故障、撤权、损坏、混合floor、DROP/重建、恢复和重启均有对应反例。
- AST/protobuf/metadata属于R2：检查parse/format/replay、编号兼容、正常生成、历史迁移与协议视图，表/列/变量展示和实际比较及索引一致。没有新的data-plane协议或常驻状态。
- fixture补齐属于R1：建真实新增catalog，不删旧oracle、不放宽预算；完整owning package通过。BVT属于R2：正常生成不是通过证据，必须检查语义、metadata、非零断言并做同实例重复比较。
- index-plugin注册、hooks/build tags、算法dispatch和ISCP/CDC接口未变，不需要新增算法注册/GPU验证；转换沿既有统一重建路径，约束与索引反向消费者已核对。没有新的queue/cache/worker；普通继承是定点PK查找，DEFAULT不扫描旧数据，显式CONVERT仍由既有有界COPY预算拥有。

关键本地证据（不提交）：`CLAUDE_C03_final_owners.log`、`CLAUDE_C03_cancel_owners.log`、`CLAUDE_C03_cancel_full_owner_race.log`、`CLAUDE_C03_cancel_final_e2e.log`、`CLAUDE_C03_engine_test_owners_r2.log`、`CLAUDE_C03_delivery_diff_coverage.log`、`CLAUDE_C03_final_bvt_export_genrs_r4.log`、`CLAUDE_C03_final_bvt_export_compare_twice_r3.log`、两轮mo-tester report及生成SHA256记录。

## 2026-10-11：实际 CI 修复早期快照（历史，后续状态见下一节）

- 保留上游历史，通过 merge 正常更新既有 draft PR；已完成第一纯上游 merge，第二 merge 与修复尚未提交/推送，不 force-push。
- UT 根因：table-dump/precision compiler wrappers 丢失 process-scoped 内部 SQL executor 能力，新增 defaults 查询转入缺 lockservice 的 runtime fallback。按可选能力转发，保留原 fallback；三个原 CI 测试及新增 wrapper/scope/错误/订阅/COPY 控制共八项 focused 测试通过。
- 真实产品修复：同域直接 stored-column ALTER COPY 不重新解释历史 binary payload；变化目标/计算值/generated assignment 仍严格；订阅 SHOW CREATE 使用 subscriber 数据库默认；仅将数据库查找 ExpectedEOB 翻译成 Unknown database，其他错误继续传播。
- plan/mysql/frontend/compile 四个 owning packages 已独立成功；失败的低磁盘 aggregate profile 不计成功覆盖率。plan 四项 exact race 的 T/N 为 2.27/13、2.03/14、0.01/100、0.00/100；0.00 使用 JSON 输出精度上界，不伪造正数时间。随后 plan 整包 race 一次 94.624s 通过。
- 当前小范围 repair 严格差分可执行行 **12/12=100%**、changed blocks 10/10，与历史整体 602/715=84.20% 属于不同范围。相对最新基线的全交付候选为 **601/727=82.67%**：只复用43个与1511bb6b逐字节相同的生产文件历史成功profile，10个漂移文件完全排除旧映射，并合并本轮四个成功owner profile；跨文件相关输入适用性仍需核对，不以候选数值宣布全部门槛完成。
- 22 个 result 只修改批准的 metadata cell/目录行；不替换整份 genrs 候选，不改 SQL/fixture/成功错误预期/regex/unknown/既有 ignore。mo_cloud 草稿错误地沿用基础 mo_database 的 owner='-'，已依据 exact CI、genrs、独立比较及 tenant DDL 所有者纠正新 defaults 行为 accountadmin。刷新后的22文件/46 edits/444行草稿，连同7行header-padding等价格式修正，已精确重放为当前tracked结果。
- 首轮私有驱动漏传 CI 原有 ignore/nometa，6920 条中 164 FAILED/2 ABNORAML，证据作废为接受记录但完整保留。修正模式后的 r2：6920 total、6880 success、14 FAILED、24 既有 IGNORED、2 ABNORAML，第二次未执行，仍不是 PASS。独立 accounting genrs 88.99s 再次通过，原 ddl_accounting_ok=1 与 wait_expect(1,60) 均保留。
- 原 20,000,000 行 PK 的 DROP COPY 与后续 INSERT SELECT 各触发原 120000ms socketTimeout；focused 完整脚本再次失败。自动采样显示 COPY 扫描持续前进，等待 FileService disk-cache 更新；CPU profile 的 full-object streaming/write 累计约79.5%，没有 collation 比较/锁死锁的独立归因证据。尚不能宣称它是上游问题。
- 私有 embed CN cache 为32MB，实际原 CI CN为512MB memory/3GB disk、TN为512MB/2GB；干净42d42546 main worktree的wrapper-native基线二进制已构建。构建流程exit3及后续observer即时磁盘断言均在服务启动前阻止执行；短暂17.45GiB读数未持续，没有基线child/services或产品终态。归因尚未成立；后续需对齐fixture，不放宽原SQL预算或生产execution-resource budget。
- 已从原公共脚本确认子集缺失清理上下文：show.test末尾保留test01，ddl_atomicity.sql末尾保留db1，完整CI后续公共clone/flush脚本拥有清理。尚未把所有r2计数差异归因完毕；需保留原38选择并验证原fixture闭包或符合CI的隔离方式，再做普通接受与同实例重复，不能修改黄金结果或添加临时drop掩盖失败。
- 磁盘余量持续波动，最新约9.03GiB，仍低于原16GiB reserve和本地 >17GiB 运行门槛。已回收6个本轮已关闭集群目录；五个已通过owner/race二进制在保存hash、日志/profile并证明无open handles后回收；六个下载CN/TN日志经解压SHA256逐字节校验后保留无损gzip，完整失败内容未删除。当前及基线motester二进制保留。其它维护worktree及只读原交付cache/服务完全不动，不猜测其它磁盘消耗来源。
- 本次源、测试、新UT、schema/grammar、BVT SQL、最终最小metadata修复和新增上游diff已以非截断小页检查；当前86个非生成Go的匹配gofmt检查通过，当前parser正常再生成exit0且前后hash一致。全交付其它result/generated适用性、跨文件profile复用、新上游执行及最终交付门槛仍未全部关闭。

## 冻结5f94c8f上的验收（54 脚本两轮普通比较已通过）

- 通过三次独立 pure-upstream merge 保留历史：`abd73bb9` / `ce8feef0` / `bb6d7636`，最新引入的上游为完整 `5f94c8f88a469e0e5053668bfca89770481a9595`。实际修复/golden与验证记录保持独立交付，不混进纯合并 checkpoint。
- 前述资源/归因阻塞已取得后续真实终态：干净 `42d42546` 用相同32MB私有fixture重现完整20M PK的相同两个120000ms超时（265 total/261 success/2 FAILED/2 ABNORAML）。私有driver仅通过现有fixture所有者对齐实际compose的SHARED cache：CN512MB/3GB，TN512MB/2GB，7m/0.8；原SQL、20M数据、120000ms、产品预算不变，同一完整PK脚本265/265通过。这只建立本地fixture归因，不宣称Linux CI结论。
- 原38脚本加入已有完整公共 `clone_db_table_to_new_db_table.sql` / `flush.test`，按正常排序位置构成40脚本闭包；冻结42d42546的两轮各7369 total/7345 success/24既有ignore/0 FAILED/0 ABNORAML。下一轮tester初始化前只读确认sys的test01/test02/test03/db1均已消失，未加临时DROP或污染golden。此证据不推广到最新5f94c8f。
- 最新输入下23个完整ordinary owner均count1/exit0，包含原17及collation/types/pb-plan/Parquet/sql-util/idxcron六个相关owner，逐包独立atomic profile；没有把build或空选择算作测试。
- 六组真实consumer的13个顶层精确测试与所有已选子测试均JSON PASS/exit0：单CN继承/commit失败恢复、双CN prepared freshness/服务器KILL取消、持久重启、实际4.0.15升级，以及上游identifier/quoted idxcron/历史跨目标升级/恢复启动预算/INTERSECT。另补admission取消后复用exact控制。跨目标历史升级123.64s通过；只宣称已选消费者，不冒充整个embed/集成套件。
- 最新10个exact race分别独立测量成功JSON T、B=30s、N=clamp(floor(B/T),1,100)，同一进程单独stress。T/N：事务清理两项0.00/100；table-dump 2.23/13、legacy1.99/15、CTAS0.01/100、scope wrapper0.00/100；双CN14.07/2；admission1.14/26；INTERSECT11.78/2、等待清理2.00/15。零显示值按JSON输出精度上界，不伪造正数或使用包/墙钟时间。随后txn/client、frontend、compile、disttae、plan、multicn六个完整race owner各一次，均非空PASS/exit0。
- 全交付严格差分非生成可执行token行覆盖率相对冻结5f94c8f为 **616/727=84.73%**，辅助changed-block为759/839=90.46%。只使用最新23 owner、6 consumer及补充admission的30份成功atomic profiles，缺映射计未覆盖；不再依赖跨文件适用性未确定的历史候选profile。
- 21个实际changed/new Go owning packages的vet exit0；CI匹配golangci-lint2.14.0对冻结base的增量检查exit0/0 issues；86个非生成Go匹配gofmt无差异。当前grammar正常再生成parser完全相同；`make generate-pb` exit0，实际`${GOPATH}/bin/protoc`为3.21.1（保留PATH36.2警告），plan.pb.go精确复现SHA256 `45e0e5b3538dce6e1fb2f39af674a38fd4c9da94b8383fb7f2d9752973920be8`。20份无schema改动的生成噪声恢复本轮owned预像，没有手改生成字节。
- 3031行第三批上游按source1172/test1588/public271完整分区，实际全部以无截断页可见读取；1140行protobuf代码也已实际全部可见，核对getter/register/marshal/size/unmarshal/unknown及生成临时变量变名。37份result采用完备机械分区：15份原交付SQL/result与1511bb6b逐字节相同，22份批准的46 edits+7等价格式修正已精确replay，全部包含在54普通比较中；2041语义/1136 header是只读辅助，不虚称其每页人工逐字读取。descriptor全部解压比较，仅增加DatabaseDefaults及CreateDatabase#5/AlterDatabase#3/CreateTable#16；其它字段、reserved14/RawSQL15、枚举及options相同。2221行压缩hex不宣称人工逐字审读。正常生成、完整schema/codec/consumer、批准hunk及真实比较共同拥有合同。
- 结果规范化工具只读不写oracle；历史ID/时间变化由原SQL的0/6/7等ignore列拥有，原tester RSSet保持相同行数并按既有sort/sortkey比较，原SQL/document metacmp覆盖优先于global nometa。字段值、catalog owner/accountadmin、CONVERT后bin、失败后源域、ddl_accounting_ok=1、原wait_expect(1,60)、成功/错误预期不变；原40闭包24 ignore加新增完整result_count的2个原ignore，最新54范围共26，未新增ignore。
- public范围扩大为54个完整脚本，保留40闭包、原15脚本缺口及上游3个Parquet/load/export真实路径；108份镜像SQL/result与当前源逐字节一致。最新wrapper-native二进制同实例两轮普通比较均exit0：每轮 **10679 total / 10653 success / 26原ignore / 0 FAILED / 0 ABNORAML**，54条逐脚本选择完整相同，两份errors为空，Java分别289s/328s，完整driver718.64s；原1200s外层/20min预算不变。下一轮初始化前四库残留均0，owned launcher/service/Java PID已退出、teardown通过；原20M PK两轮各265/265。原accounting1真实export证明保留。
- 服务日志检查没有panic/fatal error/data race/资源拒绝/120000ms异常签名；保留9821条ERROR及类别/样本，包含引导期、原SQL负例、关闭context取消，以及既有incrservice代际失效错误构造日志。正常测试通过不等于零ERROR日志，不将其直接称为实际无限重试。最后88静态/生成输入及108 BVT输入hash仍一致。

## f09d4788增量资格（已通过）

- 2文件168行全部可见审查：KeyFormatV1 unique precheck按backfill physical key分组，保留source诊断且符合ONLY_FULL_GROUP_BY；合法非空legacy Parts的NULL过滤与SQL逐字保持旧oracle。第一caller仍由原DDL事务、Result.Close、pre-pipeline目标锁及context恢复拥有。Build/compile/remoterun的RequireLegacy fence未变，C03仍拒绝V1及native PRIMARY/UNIQUE；支持的legacy/C03路径不新增scan/worker/lock/retry。
- 完整compile owner重新wrapper-native构建并count1/600s通过：3093 PASS事件、9.504s、独立atomic profile；precheck exact非空，8 PASS事件、0.001s、terminal0。21个变更拥有包重新vet/lint，exit0/0 issues；当前90个静态输入及88个非生成Go匹配gofmt。首次私有zsh包数组错误exit1完整保留，正确数组/CGo配置的r2结果不抹除失败。
- 对相关公共缺口重新执行9个完整脚本：三个charset、database_defaults、CREATE INDEX、ADD UNIQUE、COPY ADD INDEX、原20M PK以及原flush闭包。普通比较同实例两轮各 **1239/1239 / 0 FAILED / 0 IGNORED / 0 ABNORAML**，每轮9条完整选择/非空total，两份errors为空；Java15s/21s，完整driver73.99s。原PK两轮各265/265（10.092s/10.881s），原SQL/20M/120000ms/预算未改，下一轮初始化前四库残留0，teardown与owned进程终态已检查。
- 第一次私有include误传绝对路径换行列表而不是原tester逗号basename，得到0 TOTAL但CLI exit0；该轮明确排除、完整ZERO_SELECTION证据保留。只修正私有选择，并增加TOTAL>0断言；正确9脚本比较不是genrs或空选择通过。验资器曾将带rg行号的PASS错按裸行匹配，保留失败；正确行号-aware机械检查关闭该工具错误，不重跑已经成功的SQL。
- 两个upstream文件均等于f09d4788对象；其他生产文件、schema/parser/PB、native、模块、public SQL/result不变。仅unique SQL局部pure helper改变且合法legacy SQL等价，相关owner与新9公共路径重验完成。其余22 owners、消费者、admission、10 exact+6 core owner race可复用已证明不变的状态/并发/资源/协议闭包；54范围的证据仍标注5f94c8f，不假称全部在f09d4788重新执行。
- 相对最新f09d4788重算全交付：**616/727=84.73%**严格token行，759/839=90.46%辅助。只合并29份资格复用的成功profile+当前完整compile成功profile，排除旧compile及所有失败/候选/0 TOTAL轮。该新纯helper不在C03差分分母，其他生产映射无漂移；缺映射仍计未覆盖。生成输入/消费者未变，正常再生成及完整descriptor/codec证据明确复用，不冒充第四次再次protoc。
- 最终审查没有未解决本地finding；保留全部失败、日志级ERROR及原controls。基线80份精确证据已归档并逐member SHA256校验，重建二进制hash/无open handles证明保留；仅独立临时baseline在交付核验后经gwt-remove移除。

关键本地证据（不提交）：`CLAUDE_CI_FINAL_OWNERS_VALIDATE.exit`、`CLAUDE_CI_FINAL_E2E_VALIDATE.exit`、`CLAUDE_CI_FRESH_SUCCESS_PROFILE_MANIFEST.json`、`CLAUDE_CI_FRESH_DIFF_COVERAGE.log`、`CLAUDE_CI_FINAL_RACE_SUCCESS_MANIFEST.json`、`CLAUDE_CI_FINAL_STATIC_GENERATE.exit`、`CLAUDE_CI_LATEST_PB_REPRODUCE_CHECK.log`、`CLAUDE_CI_FINAL_PROTO_DESCRIPTOR_CHECK.log`、`CLAUDE_CI_REVIEW_LEDGER.md`及最新BVT逐轮report/error。

## 证据与交付隔离

本地完整失败/成功日志、JSON、profiles、oracle、tester配置和私有缓存不提交。所有coverage仅采用成功终态profile；缺映射计未覆盖，生成代码/测试/纯声明不进入可执行差分分母。TODO、临时驱动、工具及工作目录不提交。

只交付源代码、测试/result、批准设计与本记录；正常fast-forward推送现有PR head并更新draft PR #29374。保留独立worktree供未合并PR后续维护；所有测试拥有的服务与export collector已关闭，未操作其它worktree的服务、文件或缓存。实际交付提交与PR head由推送后核验，不以本记录创建或历史覆盖率代替。

## 2026-10-12 冲突修复增量（mo/main 5a5501b3）

本轮从已交付c02ef01f正常merge最新`mo/main`（19个上游提交），不改写历史、不替换PR。下列结果属于新的冻结输入，前述5f94c8f/f09d4788结果保留其历史范围，不自动视作对本轮上游全量回归的验收。

- **冲突合同**：14文件=protobuf schema/generated及12个完整result。保留上游已发布的`CreateTable.unresolved_fk_parents#16`/`fk_name_binding#17`；本PR尚未发布的`database_defaults`移至**#18**，reserved14/RawSQL15不变。protocol110/独立4.0.15迁移及其它编号不变。前述历史descriptor中的#16不代表本轮编号。
- **正常生成/完整descriptor**：`make generate-pb` exit0；20份无schema变化的生成noise从精确合并预image恢复。完整解压descriptor相对5a5501b3只增加DatabaseDefaults及CreateDatabase#5/AlterDatabase#3/CreateTable#18，其余字段/类型/reserved/enum/options相同。新增UT独立断言15/16/17/18、reserved14及empty/default-only/FK-only/combined的wire size、marshal/unmarshal和clone。
- **交互审查**：12个自动合并source owner相对新main的460行C03差分完整读取，配合771行相关upstream source增量及实际锁/clone owner核对。数据库默认freshness仍位于CREATE name admission锁内、发布前；proto.Clone完整携带双方字段；prepared/auth、strict最终赋值/同域stored ColRef及subscriber defaults保持。17个result冲突hunk全部机械replay保留C03 domain/NULL/width/format，只组合上游MySQL Type spelling；额外conversion结果6行同步formatter。未声称人工逐字审读全部40803行上游delta。
- **新UT/消费者**：12个完整普通owner全部PASS，34919个测试PASS事件；覆盖pb/plan、sql/plan、compile、frontend、txn/client、bootstrap、v2_0_0/v4_0_6/v4_0_15、tree、function、sysview。实际`TestDatabaseDefaultsTwoCNPreparedFreshness`串行复验PASS（包15.006s），证明新的CREATE字段与身份/代际prepared消费者边界。均count1、enabled vet、Go1.27.1/Darwin arm64、repository CGo入口。
- **新race**：txn readonly indeterminate-lock及真实CommitTS两个exact独立成功测量T=0.00/0.01s，B=30s，各N=100独立同进程stress；再完整txn/client owner一次（280个PASS事件），均exit0。未为无关上游window/vector/LLM变更扩大race；原其它race保持历史范围，不冒称重跑。
- **新公开普通比较**：26个完整脚本包含12冲突result及相关自动合并结果、C03 defaults/conversion/errors、原20M PK、会计脚本和既有cleanup owners。同实例两轮各**5427 total / 5423 success / 4原ignore / 0 failed / 0 abnormal**，Java116s/138s，完整native-linked driver321.27s PASS；每轮26个明确非空选择、errors空，下一轮初始化前四库残留0及owned进程终态。SQL/fixtures/ignore/oracle、20M、120000ms、accounting1保持。`otool`验证强制链接libmo/declared graph及绝对rpath；4282行服务ERROR原样保留，不宣称零ERROR或把error-construction日志当实际无限重试。
- **新覆盖与静态**：仅3份本轮成功atomic profile（12普通owner+双CN_r2），不使用旧或失败profile；相对5a5501b3严格非生成可执行差分**574/727=78.95%**，aux**712/838=84.96%**，缺采集cdc_testutil计未覆盖。21包vet/lint exit0/0 issues，87份非生成Go符合匹配gofmt，target-relative whitespace exit0。toolchain/native、grammar/parser、protocol/version/function allocation输入与交付HEAD相同，未机械重跑无关全仓测试或另行声称parser重复生成。
- **失败与资格记录**：首次双CN180s超时停在flock→clusteradmission→Start、尚未获得服务准入，等待本任务BVT exclusive lease；保留栈/exit1，BVT终态后用相同180s/count1串行复验，不加retry/skip或延长SQL timeout。第一版私有BVT的`-ldflags=-w`覆盖wrapper extldflags，虽两轮SQL均通过但不当最终CGo模式验收；保留结果，修正私有链接参数后重新执行相同26脚本。最终资格脚本误将YAML按JDBC URL匹配、误要求service log必须打印会计字段，已按实际YAML和普通比较原oracle修正只读谓词，未改SQL/result或为工具错误重跑成功SQL。

本轮修改只组合既有合同，没有新的worker/queue/resource owner；新native准入前均满足>17GiB，未删除历史资源或操作只读C03来源。上游FK admission新消费者通过fresh owner/公开路径/双CN闭合；durable schema、迁移、AST source及独立历史restart/recovery验收不因字段换号被冒称全部重跑。所有私有driver/TODO/log/profile/manifest留在worktree且不提交。正常推送既有draft PR#29374；本地成功仍不代表远端Linux CI已绿。

## 2026-10-12 review反例修复（mo/main 02a2823c）

此轮修复用户实际SQL发现的两项C03阻塞缺陷；不是以已有CI通过证明组合已覆盖。先正常merge两个上游commit为b0fae5c2，再补RED回归，沿用原PR/批准r1，不重写历史。

- **真实反例先红**：生产代码修改前，planner/frontend/真实SQL三条命令均exit1。真实SQL中DEFAULT前/后×MODIFY/同名CHANGE四种写法均错误提交emoji；cluster恢复后bin确实变为general_ci。强制COPY/显式目标列域控制正确拒绝。RED的另一个explicit-COLLATE控制最初错误预期INPLACE，核对既有属性规则后纠正为COPY，保留该oracle错误，不把它混作产品缺陷。
- **组合ALTER修复**：算法admission改用private effective table default，与实际COPY/INPLACE binding一致，原resolved TableDef不被修改。storage-agnostic判断包含collation revision。最终域变化走原transactional COPY，INPLACE/INSTANT/LOCK=NONE不能绕过；default-only及显式列域覆盖、兼容ASCII保持。六条真实SQL负控分别验证错误、可读原emoji、原column/table default及replacement leftovers0；成功ASCII/默认仅继承正控保留。
- **TS恢复修复**：cluster TS loader补同source tenant/TS的durable defaults读取，并在普通目标CREATE附合法charset/collation，由CREATE重新分配目标DB ID。共用preflight不再把protocol110当作元数据有效性证明，在不可逆account mutation前校验。TABLE-only原能力门/不覆盖DB默认值合同保留；subscription/system/legacy边界不变。新frontend UT覆盖source bin/native revision、legacy absence、read error、cancel及generation0在DROP/CREATE前拒绝。
- **完整owner与真实消费者**：current complete plan/compile/frontend分别PASS（11.001s/11.658s/24.383s），共23466个测试PASS事件；两条完整`TestDatabaseCharsetInheritanceContract`/`TestDatabaseDefaultsFailureAndRecovery` PASS（26个PASS事件，instrumented package20.021s），包括实际cluster restore、新表继承/变量、现有account/PITR/commit-failure消费者。plan/compile的独立成功包profile从R2混合aggregate逐包终态过滤，失败frontend计数全排除；frontend取完整R4成功profile，不把失败aggregate包装为exit0。
- **新TRUNCATE fixture与race**：此次上游加入的RC wait UT仍mock旧HasLockTable，而本PR实际admission按Snapshot的全部retained locks选择policy。仅修fixture为真实TxnSnapshot，fresh wait/cancel/retained fast-fail及wait-bookkeeping清理oracle不减少，不改production锁。exact race成功测量T=0.01s/B=30s/N=100，独立单进程stress100次全部PASS。test-only局部channel/atomic同步需要focused stress；本轮production无新增共享可写状态，原txn race源码/合同不变，不机械扩大whole-repo race。
- **公开oracle与普通比较**：两个既有完整SQL/result扩展，复用原fixtures，无新增cluster/data/service setup。mo-tester生成两份candidate后，逐hunk只回放新增SQL/oracle及恢复bin后SHOW CREATE旧general_ci表所需显式COLLATE；原旧format/timestamps/ignore/regex/unknown/warning/UDF/procedure断言全保留。最终强制libmo/declared graph/绝对rpath二次普通比较，五个exact完整case每轮**1032 total / 1032 success / 0 failed / 0 ignored / 0 abnormal**，Java14s/16s，driver112.00s。下一轮tester初始化前，sys相关DB/acc01–04/snapshots残留均0；errors为空，SQL/result十份输入与交付字节/哈希一致。823行服务ERROR保留，不宣称零ERROR。较早的五脚本普通比较也成功，但最后一轮额外验证了完整global cleanup，未增加skip或调整socket timeout。
- **覆盖/静态与复用边界**：本轮严格增量非生成可执行差分**10/12=83.33%**（aux13/15=86.67%）；PR-base相对02a2823c严格**588/738=79.67%**（aux715/826=86.56%）。后者合并fresh qualified plan/compile/frontend/SQL profiles，只复用205个byte-identical非当前core owner源文件的既有成功bootstrap/tree/function/sysview/txn/PB profile；module/native/toolchain输入不变，缺映射disttae/cdc等仍计未覆盖。不是旧78.95%结果改名。四包final vet/lint exit0/0 issues，匹配Go1.27.1 gofmt及whitespace无问题；schema/parser/native/function编号完全未改，不声称重新生成或全仓Linux验证。
- **保留失败与资源资格**：完整owner先暴露旧restore mock source metadata缺项、新truncate mock旧接口，均补真实fixture，不弱化拒绝/cleanup断言。一度尝试的TABLE preflight全跳过未交付。并行race构建使磁盘进入保留区、frontend returning spool发生cap1准入拒绝；仅取消已证明本任务拥有的Go构建，保存143/进程树和spool失败，改串行执行。cross-package instrumented SQL行为通过，但其构建期外层preflight不足以保证峰值reserve，不作为最终native资源准入证据；最终BVT分离build与execution，在执行前严格>17GiB。native补充driverbuild后第一次资格因空间不足被拒绝，保留AssertionError，回收后执行同一已验证二进制而非放宽门槛。仅精确terminal/no-open-handles的本任务private rebuildable缓存、三个本任务历史BVT临时数据目录和两个旧二进制被回收，均保留SHA256/退出及cfg/log路径映射；原成功/失败报告不删除，未操作只读C03来源、global cache或独立服务。

此轮增量source/tests/new golden hunks完成自审与真实consumer闭包，无新增永久状态/worker/row scan/生产retry。原restart/upgrade/双CN历史证据保持其限定范围；本轮新跑的是cluster恢复及上述组合，并不冒称重新验证全部历史C03或上游全部join变化。最终推送既有draft PR#29374，本地完成仍不代表远端Linux CI已绿。

## 2026-10-12 P2复审修复：合法组合INPLACE（最终base f5360574）

复审揭示上轮遗漏了INPLACE防御校验：admission按最终DEFAULT选择INPLACE，二次检查却按原future default重新绑定类型。旧column域bin、旧table default为utf8_unicode_ci，最终DEFAULT回到bin时应合法；MODIFY和同名CHANGE均被错误拒绝。上轮PASS未覆盖这个独立正向状态，不能拿其emoji负控通过关闭本次问题。

- **修复与源owner**：仅两处防御检查输入改为现有planner-owned `currentTableDef`，与final-default binding一致，不移除检查/弱化type、attrs、position、hint或LOCK门。原 `tableDef` 保留执行时原schema/index锁合同；typed UT继续逐字节断言planning不修改source。无新state/worker/cache/scan/同步/格式/协议；成本仍为已有列layout check，race/restart/upgrade/GPU不因这个单线程输入修复机械扩大。
- **先RED后GREEN**：先正常merge64b7a438为b7b910b1，相关INPLACE入口仍与8c一致。typed和真实SQL在生产修改前各exit1：四个MODIFY/同名CHANGE×DEFAULT先后顺序和显式INPLACE五个正控均`failed inplace check`，显式COPY控制成功。新真实SQL复用已有一个emoji表，每轮先DEFAULT-only重建相同source状态；固定后六种写法均成功、原emoji/列域/table default仍bin。旧utf8mb3 emoji拒绝、failure保留原数据、default-only、explicit-column override及ASCII成功控制全部保留并通过。
- **普通owner/消费者**：b7范围完整plan/compile/frontend/txn均PASS（11.929s/12.325s/27.900s/9.942s），两条完整inheritance/recovery真实SQL test PASS（32个PASS事件，包15.845s）。入口/构建使用repository CGo、Go1.27.1、count1和native linkage；预编译owner执行时保持package cwd和atomic profiles。第一次误从repo root执行导致两个relative fixture缺文件失败，完整失败JSON/profile保留且不用于coverage，纠正cwd后原fixture/assertions不改。
- **提交期正常merge**：新main f5360574的三提交（LIKE escape parity、async IVF/generated UPDATE、SQL task timeout）正常merge为f69b69ea。P2五个source/fixture文件未被incoming修改；全部83行相关bind_update/LIKE增量已读，IRREGULAR UPDATE/ISCP不是此INPLACE路径，新task timeout不改变本fixture配置。重新完整plan/function owner PASS（11.895s/10.634s，14846/10641个PASS事件）；compile/frontend/txn源文件及C3相关分支不变，明确限定复用b7成功证据，不宣称重测GPU/所有上游功能或整个历史C03。
- **公开普通验收**：现有conversion SQL/result增加两个代表性metadata-only组合及emoji/column/table default oracle，无新fixture/data/service。genrs只是candidate，完整diff只采用21行新语义结果，两个旧FULL COLUMNS trailing-empty-cell格式差异未采纳。最终新main native-linked四个完整scripts同实例两轮各**739/739，0 failed/ignored/abnormal**，errors空；初始化下一轮前所有mapped DB/账户/快照残留0。原同样四脚本在64b也通过，但最终以f536新native运行资格为准；不改变忽略规则、SQL/data量、timeout或预算。
- **覆盖与静态**：严格增量AST token **2/2=100%**（block4/4），PR-base相对f536严格 **578/740=78.11%**（aux707/830=85.18%）。采用fresh current plan/function，b7中语义有效的compile/frontend/txn成功profile，及逐文件byte-identical bootstrap/tree/sysview/PB旧profile；缺映射/未采集仍计未覆盖，无失败/RED/canceled profile。最终六包vet/lint、匹配gofmt和whitespace通过；非全仓SCA/非Linux CI。
- **资源与交付资格**：多次执行前>17GiB gate拒绝时未启动测试。精确task-owned/no-handles/terminal旧cache、历史native binary和一个f09 BVT临时data根回收，SHA256/manifest及5份cfg/log保留映射；不操作只读C03/global cache/其它任务服务。磁盘后来自然恢复36GiB，来源未归因，门槛未放宽。所有成功/失败和private driver留worktree不提交。增量source/tests/result/self-review闭包无未解决blocker；仅正常提交/推送既有draft PR，不提交GitHub review/issue comment，不声称远端Linux CI已绿。

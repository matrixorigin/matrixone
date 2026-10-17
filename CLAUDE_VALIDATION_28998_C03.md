# #28998：C03 验证与交付记录

## 当前维护轮次：main a322466757 冲突修复（2026-10-17，本地验证完成）

原head cabf62437e50d69c27e834200e58fdf1c281e4b5；正常合并冻结官方main a322466757b9b0165c59fbda565608653d923b2a。本轮旧编号599由main文件访问鉴权函数占用，保留其599，将未发布C03函数顺延600/END601；MORPC118、migration4.0.15/min4.0.14/offset4、PB18均不改。共享feature walk与正/负控制同步600，新增599即使伪造C03显示名也不误判。mock保留main无map分配switch和C03 collation_server语义；parser由合并grammar owner强制双次生成一致，无手改生成源码。

上一轮82.12%及各测试指标仅保留历史资格，不冒充此轮含新依赖/执行ID输入。以下全为本轮Go1.27.1/Darwin/arm64/native终态PASS/exit0，源码冻结不变，未完成/中断/失败profile不纳入。此轮没有C03既有SQL/oracle/数据/skip/预算修改，主干公共SQL/预期变更原样保留，无genrs、不申请复审。

|完整owner|顶层测试|PASS事件|包执行秒数|
|---|---:|---:|---|
|PB plan/pipeline|65|213|0.910 / 1.229|
|sysview|29|65|0.983|
|bootstrap|64|162|7.872|
|v4_0_15|1|7|1.042|
|CN|150|321|10.759|
|function|2118|11192|20.455|
|plan|3841|16453|24.906|
|compile|1464|3333|22.710|
|frontend|2485|6012|20.539|
|explain|141|439|5.542|
|mysql parser|220|1839|0.975|
|parser facade|35|102|0.886|
|tree|78|149|0.925|

共13组/40287 PASS事件；完整SQL integration52/359/107.070s、multicn count3为15/45/104.575s、完整isolated18/49/96.216s、原optimistic CDC1/1/6.797s、原Issue28909+29148为2/9/8.480s（顶层/PASS/包秒数）。17包vet/lint均exit0、0 issues，120个变更Go文件gofmt清洁，固定base空白检查exit0。

严格非生成差分可执行token覆盖804/980=82.04%（门槛75%）；辅助changed-block948/1077=88.02%。只合并上述13组完整成功atomic profiles，无映射/缺采集计未覆盖，SQL mode-only/失败/中断及旧输入profiles排除。

四项race独立测量B=30s：migration T=.01、derived-publication T=0、init-retry T=.20、CN-authoring T=.02，各按公式N=100压力通过。完整race frontend2484/6011/30.550s、bootstrap64/162/6.154s、CN150/321/11.715s；沿用现有!race allocation隔离及既有mock skip，没有新增skip。不称全仓race。

真实public仍为原run.sh -g -n、27脚本、同一native实例双轮；每轮3022总计/3021成功/0失败/1既有ignore/0异常，Java58s/51s，driver160.92s PASS，build/driver/BVT/FULL/FINISH均exit0。54个source及54个mirror哈希相同，比较errors空、两轮fixture及DB/account/snapshot残留均0，服务/端口关闭和admission release均ready；owned cluster28001/PID89391退出。保留2819条服务ERROR，未见panic/fatal/data-race签名，不称零ERROR。二进制、native dylib、私有driver及Go build-info哈希闭合；源码/index/commit整树和两父身份由资格脚本检查。

本轮只读变化地图为固定a322上的167个PR路径、25个新增/上游交互路径；20个非生成/非oracle交互文件1447行逐hunk审阅。主干文件访问认证身份、CHECK/组合DROP主键、explicit CAST warning、CTAS sequence及CN engine-owned drainage均保留；C03首个typed owner、严格赋值、DEFAULT/V1、恢复身份和协议118负控不退回旧逻辑。parser双次owner输出SHA一致；plan/pipeline模式、载体和generator逐SHA配对一致，仅复用生成/descriptor证明，PB执行为新运行。native源码/flags不变，Makefile变更仅UT/SCA；dragonboat更新后的执行证据已刷新。

资源低水位时只暂停自己的调度器、让已有parser子任务自然完成，再对745个Go archive作APFS透明压缩，逐SHA/mtime相同、无open handle、缓存路径/内容保留，物理blocks节省7.185741GiB；未驱逐未来normal/race缓存，未终止任何编译/测试/其它任务，两调度器恢复后全部消费者通过。源码/WIP/模块/native/数据库/binary/证据均保留。

后续只读观察到官方main38a27e67a15890df7ca6fc418c6a57ae3a0b924d（单个TAE table-tail flush修复，协议117及函数599不变）；仅作merge-tree兼容检查，未合入/不冒充后续main运行资格。上述运行范围固定a322，不代表Linux CI、GPU/全仓UT/race或旧二进制滚动升级；历史Darwin full-optools限制仍在。

## 历史维护轮次：依赖表达式评审修复及 main b3d6b21fe9 合并（2026-10-16）

本轮最终冻结输入的本地执行终态已收齐；源/index/commit和远端交付另核验。后续历史章节只保留各自输入/模式范围，不冒充本轮重跑。冻结 main=`b3d6b21fe9d260c91e1777529a48a9d93061af33`，原 PR head=`05d97d39697fc7adc0b5719f3f297490c9664376`。本轮 C03 MORPC118，保留 main117 独立 LEAST/GREATEST；599/END600、升级4.0.15与 PB18 不变。

已保留实际 SQL RED：原精度10创作、当前精度0及字符列改域下，三个部分依赖场景改变无关除法结果，五个最近控制通过。首版 typed 修复的8个实际 SQL 场景通过，但追加 bin/general/native 12轮往返暴露 compiler string-coercion 的旧 IMPLICIT rank 导致 native 非法混合；此失败不计成功，也不更改全局/session策略绕过。后续修复在首个 typed owner 内刷新编译器字符串coercion：保留物理宽度、作者/数字/assignment边界和原数字子树；只折叠相同物理类型/宽度/pad的隐式层，非字符输入的格式化cast使用 NUMERIC coercibility5，再由原字符函数binder推导实际域。普通显式 MODIFY 继续使用当前声明/会话。

- **当前普通终态**：Go1.27.1/Darwin/arm64/native wrapper/vet启用，13完整owner（PB两包/sysview/bootstrap/migration/CN/function/plan/compile/frontend/explain/mysql parser/facade/tree）共**39853 PASS事件**，全为完整atomic profiles/非空package PASS/exit0/source unchanged。完整SQL **50 top/342 PASS/105.357秒**；完整multicn count3 **15/45/105.794秒**；完整isolated **18/49/97.157秒**，包含扩展的转换表达式跨重启及未来写入。原optimistic CDC **1/1/7.301秒**，原Issue28909+29148 **2/9/7.567秒**。这些不是全仓库UT。
- **覆盖/静态**：严格非生成AST可执行token差分 **804/979=82.12%**，辅助changed-block **965/1094=88.21%**。只合并13组本轮完整成功owner；缺采集/无映射仍计未覆盖，不含失败、中断、source-changed或SQL mode-only profiles。**17适用包vet/lint exit0、0 issues；120变动Go文件gofmt干净**，相对b3 whitespace exit0。
- **生成/当前范围**：parser owner强制双次生成SHA一致，且等于自动合并parser。PB plan/pipeline schema/生成载体/gen.sh 在main76与冻结mainb3逐SHA相同；候选又等于已交付05，因此仅复用精确输入一致的完整descriptor/正常owner生成证明，PB测试仍本轮新跑。共享远端ref后续前进至3478692905（仍只占117），不改冻结BASE或把其新增binder内容冒充已经测试；交付前另核验可合并性。
- **race终态**：四个精确测试独立测量T=.04/0/.20/.02秒（migration/publication/retry/CN barrier），B=30秒，按clamp(floor(B/T),1,100)分别独立stress **100次**；T=0按100。完整frontend **2480 top/5981 PASS/27.035秒**、bootstrap **64/162/15.919秒**、CN **150/321/11.523秒**，均exit0/source unchanged。既有!race allocation排除及两模式相同mock skip不改，不称全仓race。
- **27 public同实例双轮终态**：原run.sh -g -n正常比较，每轮 **3022 total/3021 success/0 failed/1既有ignore/0 abnormal**，Java **60/74秒**，driver **186.72秒PASS/exit0**。54 source+54 mirror逐SHA相同，errors空，两轮fixture及DB/account/snapshot闭包均0，owned kernel PID88464退出；service-close/port-release ready且admission_released=true。native-linked候选binary/libmo/driver SHA及Go1.27.1 build info保留。**2831条service ERROR**原样保留，panic/fatal/data-race签名0，不称零ERROR。
- **oracle/反例边界**：本轮public仅手写追加新场景，旧oracle保持（前轮批准的一个错误文案cell本轮不再变）、不genrs；同时要求隐式native比较与作者EXPLICIT COLLATE的相反结果。两个实际SQL COLLATE控制没有失败，不称公共RED；最近typed owner/规范CAST0的EXPLICIT rank0保留断言确有RED，已修且最终owner通过。
- **失败与限制**：未运行driver、资源等待、fixture、编译、断言失败和旧平台限制独立保留。初轮owner为先闭合作者边界而停止排程，已启动测试自然结束，全部归档不参与本轮最终coverage；未终止Go编译/测试或无关进程。Darwin不代表Linux CI；不声称全仓库UT/race/GPU或旧二进制混部升级。

## 历史维护资格：CI修复及main语义协调（2026-10-16）

- **冻结范围**：原head `0f0c038d7898fb1ea93544fe49c958221f757744`，正常合并官方main `76deafc8062f9a7e33a31a4df858ebcdb9477cb5`。C03统一**117/INTERNAL_TABLE_COLLATION599/END600**；保留main原生Unicode新建普通持久表UNIQUE114、calendar115/598、division warning116，迁移4.0.15/min4.0.14/offset4及CreateTable.defaults18不变。验证期间观测main前进到b24d1b3115，仅18个window相关文件，不扩张为已验证该提交；当前证据均绑定76deaf。
- **CI根因/修复**：run38707446306的内部derived-view发布显式拥有pessimistic RC事务，不改变全局/user optimistic负控；migration在请求之间使用operation context及认证TenantInfo，保留活动用户事务compiler路径和逐次executor Close；降级71无ghost断言直接读account0原始mo_tables，不调用117 TABLES。原coverage aggregation是producer profile不完整，不冒充已算得的覆盖率失败。
- **主干语义闭环**：有效RED证明默认域展示不该走物理索引执行准入，DEFAULT不该撤销V1 native UNIQUE schema marker。formatter保留完整解码/known-format/身份与revision拒绝；DEFAULT不改原列/物理索引格式。真实SQL验证ß/ss重复1062、TABLES默认域变化而COLUMNS不变、DEFAULT后的pruned count(*)及既有索引mutation拒绝。PRIMARY/已有表索引变更/不支持的CONVERT继续由上游owner拒绝。invalid identity0/revision1不是合法RED，不放松api校验。
- **生成/来源**：pipeline按owner从合并proto双次生成一致，完整descriptor只有SourceCodeInfo差异，所有字段/编号/类型/reserved/enums/options与main相同；生成代码除descriptor数组外逐字相同。plan仅批准DatabaseDefaults及CreateDatabase5/AlterDatabase3/CreateTable18。保留main两列wide seed、普通列clone、4098 typed边界及原180秒阶段/600秒总预算，不改公共数据规模或COPY预算。
- **完整普通资格**：Go1.27.1、Darwin/arm64、仓库native/CGo及vet启用。PB两包/sysview/bootstrap/migration/CN/function/plan/compile/frontend/explain/mysql parser/facade/tree共**13组新鲜完整owner、39481 PASS事件**，均完整atomic profiles/非空package PASS/exit0；不是全仓UT。完整SQL integration **48 top/320 PASS/package108.874秒**（wrapper180.171秒）；完整multicn `-count=3` **15 top/45 PASS/package116.245秒**。最后独立刷新durable restart **1/1、8.674秒**、原optimistic CDC **1/1、6.526秒**、Issue28909+29148 **2/9、7.669秒**，均source unchanged/exit0。
- **race资格**：四个精确测试B=30秒：migration T=.01、publication T=0（低于JSON分辨率）、retry/cancel T=.20、CN barrier T=.02，按clamp(floor(B/T),1,100)分别独立stress **100次**。完整frontend **2463 top/5910 PASS/31.902秒**、bootstrap **64/162/15.797秒**、CN **156/324/11.583秒**，全部exit0。main既有live-path allocation测试受!race build tag排除，RuleCache复杂mock skip两模式相同，未新增skip；不声称全仓race/旧二进制滚动升级/GPU资格。
- **覆盖/静态**：严格非生成可执行AST token差分 **731/893=81.86%**；辅助changed-block **877/992=88.41%**。仅13组完整成功owner profiles，SQL mode-only/失败/中断/不完整profile不参与，缺采集/无映射仍计未覆盖。**13适用包vet/lint exit0、0 issues**；119个变动Go文件符合Go1.27.1 gofmt。相对固定main whitespace通过；合并含main既有golden空格，保持与main相同，不重新格式化oracle。
- **批准的单点oracle同步**：首轮27 public为2998/2996 success/1 failed/1既有ignore，唯一失败是main114已有CONVERT拒绝文案由“until collation-aware unique-index deduplication is supported”改为“without the versioned unique-key format”。对应上游helper与main逐字一致，拒绝行为/错误码不变。用户明确批准后，仅精确同步既有result第116行；**不改生产迎合旧文案、不改SQL/数据/错误码/负控/skip/预算、不调用genrs**。原失败及完整证据保留。
- **最终27 public同实例双轮**：真实native-linked kernel，原 `run.sh -g -n` 正常比较，每轮 **2998 total/2997 success/0 failed/1既有ignore/0 abnormal**，Java50秒/53秒，driver **156.36秒PASS/exit0**。54源及54mirror逐SHA一致；与原失败代际比较只有批准的一个错误文案cell变化。errors文件0bytes；两轮fixture及DB/account/snapshot闭包均0，admission释放，owned kernel PID37331退出。保留**2793条service ERROR**，panic/fatal/data-race签名0，不声称零ERROR。
- **失败/资源边界**：首轮migration旧peer消息、native114正控、开发中的不合法metadata正例、旧bootstrap flags断言及中断/资源失败均保留而不计PASS。explain构建首轮缺sonic缓存archive，未启动测试；私有回收修正为真实cwd归一化与活动Go driver/loader整缓存保护，再完整成功。只回收明确Go cache Root中受pins/mtime/SHA/open-handles保护的inactive Go archive及已证明闲置的orphan Go临时archive，保留源码/WIP/模块/数据库/native/test executable/验证证据/worktree和无关进程。16GiB为保留目标，外部并行构建曾令余量下降；持续安全回收恢复后再准入，末轮BVT开始前>23GiB，不用逻辑字节冒充物理空间。
- **交付/限制**：资格终态、生产/测试Go/proto/module冻结输入及index逐SHA核验通过；用户批准的oracle变化不使这些Go资格失效。只交付既有DESIGN/VALIDATION两份CLAUDE文档；私有TODO/driver/profile/log/cache/review不提交。正常两父merge commit/普通push原PR，不申请复审、不评论、不关闭issue、不持续监控远端CI。历史macOS完整optools失败仍保留，**不声称新head Linux CI或外部复审通过**。

## 历史维护资格：新增inline及后续parser冲突（2026-10-15）

- **范围**：原head170a5d1aa83d，正常合并main `501c85ecdd6eee0195172743c6d8aaf125cb252d`；交付前远端main核验仍相同。新增review5527576615两个inline基于旧f5a7，分别是生成表达式精度及main113错误准入，均已由170a实现修复，本轮核对实现/负控不重复改产品。当前main仍仅占用113，C03保持114、4.0.15、defaults18、函数598/END599。
- **冲突/交互**：唯一冲突是生成mysql_sql.go，合并grammar通过owner make -B两次生成、SHA一致，无手改。保留main的POINT→ST_POINT alias、表达式CHAR CHARACTER SET/二进制不填充/空charset拒绝、CREATE INDEX后缀USING及选项合并、UT批次范围；完整非生成增量和C03交互hunk已审查。没有本轮新增生产状态、goroutine、锁或持久化格式。
- **当前owner**：Go1.27.1/Darwin/native wrapper/vet启用，重新运行parser/facade/tree/plan/function/compile/frontend/explain/sysview共**9完整owner、38008 PASS事件**，各非空终态/exit0/完整atomic profile。migration和PB相关源及语义输入未变，复用170a本轮之前已核对的两个完整owner（7/186 PASS事件），明确不是11个新鲜重跑；当前覆盖使用9新鲜＋2可复用profile，不混入红/不完整profile。
- **实际SQL**：完整sqlintegration **45 top/310 PASS/177.504秒、exit0**，含原生成表达式SQL入口四组控制；另由完整plan owner覆盖9个typed表达式/域子例。当前严格非生成AST可执行token **687/852=80.63%**，辅助802/920=87.17%，缺采集/无映射仍计未覆盖。**16适用包vet/lint exit0**，96非生成C03 Go文件gofmt匹配；不是全仓静态或UT。
- **27完整public原比较双轮**：在原24脚本上增加main已有func_cast_character_set、geo_stpoint、secondary_index_create，不重新生成任何oracle。原run.sh -g -n、同一真实native-linked实例，两轮均 **2998 total/2997 success/0 failed/1原ignore/0 abnormal**，Java57秒/57秒，driver **164.83秒PASS/exit0**。54个源及54个mirror逐SHA一致、每报告28行、errors为0bytes；两轮fixture与映射DB/account/snapshot闭包均0，服务/admission/port teardown ready。保留**2803条service ERROR**，panic/fatal/data-race签名0，不声称零ERROR；上游secondary_index脚本默认test库表由tester默认fixture重建管理，不将其夸称自清理。
- **调度证据及明确限制**：新增调度合同TestEmbeddedAuditedBatchScopeAndFailure独立执行 **1 top/19 PASS、package4.615秒**。完整optools尝试没有通过：serial控制返回100（日志同时提示Darwin orphan discovery不可用），HUP mock的/bin/bash不支持read -t 0.01，随后整包180秒预算在Make配置子例到期；原始失败/stack完整保留，不改生产来掩盖、不声称该包全绿。optools和Makefile与main501逐字一致；本轮新增批次映射正负控制已覆盖。这是本地harness环境/整包资格限制，不据此新建产品故障或冒称Linux成功。
- **资格边界/交付**：170a的race/多CN/restart/upgrade保持原输入/模式范围，本轮未重跑或扩张；本次无新并发/恢复/协议格式变化，以当前parser/binder、完整SQL及public消费者证明合并交互。正常merge/push原ready PR，已修inline按权限resolve并请求复审，不擅自评论或持续监控CI。仅DESIGN/VALIDATION两个既有CLAUDE文档交付，私有TODO/driver/cache/profile/log不提交；仅经pins/mtime/SHA/open-handles保护清理再生Go archive，保留源码、模块、数据库、原生、证据、worktree及无关进程。远端Linux CI及外部复审仍待其独立终态。


## 历史状态快照（f09；当前资格见末节）

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

### 推送后的上游函数编号冲突（080365f0）

P2六文件提交214fc22a已正常推送。随后PR出现dirty，post-push fetch确认main刚新增五个hyperbolic函数591–595，碰撞本PR未发布的internal_table_collation591。正常merge后只协调两个手写function-ID contract冲突，**保留TANH591/COSH592/ASINH593/ACOSH594/ATANH595，C03 INTERNAL_TABLE_COLLATION596、END597**；588–590及旧SINH、SQL名字/返回VARCHAR(64)/classifier0、PB#18、protocol110/4.0.15不改。持久sysview保存SQL函数名，最新service按name绑定当前ID；未将草稿PR旧编号当已发布协议，也不手改生成代码。

- 重新完整plan/function owner PASS（12.214s/10.979s，14846/10682个PASS事件），含所有ID预定义/注册检查、hyperbolic encoded执行及C3元数据域正负控制。P2 source/tests/SQL/result与214fc22a字节一致，不删/弱化assertion。
- 最新native普通比较扩展原四case加完整upstream func_math_hyperbolic，五个明确脚本同实例两轮各**772/772，0 failed/ignored/abnormal**，Java3s/7s；全部mapped global DB/accounts/snapshots及hyperbolic_input表在下轮初始化前均0，errors为空。573服务ERROR原样保留，metadata/DOUBLE/溢出/nullable/prepare controls和原C03 controls不改，未重生成golden。
- 最新两个受影响package vet/lint及匹配gofmt/whitespace通过；之前六包证据限定复用。PR-base改以080365f0，fresh current plan/function配已qualified compile/frontend/txn和byte-identical非core profiles，严格仍**578/740=78.11%**（aux707/830）；纯编号/声明修改无新增执行token，registry/编码及公开consumer负责其合同。旧f536/64b结果保留各自冻结范围，不冒称重测全部C03/无关upstream功能。

此次正常merge补推既有draft PR，保留214fc22a历史，不force/rebase/新PR，不评论/关闭issue或持续监控CI；远端Linux CI仍未确认。

## 2026-10-13：上一轮冲突与交互维护（冻结main 21f19ae063，历史资格）

当前PR已经ready，保留其状态与既有正常历史。独立正常合并cd8d5abf、175a645a、21f19ae0；前述110/596/END597仅为历史未发布分配，当前为**C03能力112 / INTERNAL_TABLE_COLLATION598 / END599 / 独立4.0.15 / CreateTable.database_defaults#18**。已发布MEDIUMINT110、IndexSearchScan111及FORMAT_BYTES596/FORMAT_PICO_TIME597均保持。

- **交互合同**：MEDIUMINT authoring/remote fence与int32/uint32 Width24赋值校验保留，binary stored ColRef的COPY保留优化不覆盖数值目标。SHOW FULL建SQL前选择上游或C03投影，补binary classifier2，不再依赖已不存在的null占位字符串；sys/tenant分支共享选择。现有真实SQL binary fixture增加正控，公开现有explicit_binary fixture只加一条SHOW及4行oracle，不引入新数据/服务，旧生成CAST截断与utf8mb3拒绝控制保留。
- **生成及编号**：parser/PB正常生成；完整descriptor相对175只允许C03消息及字段5/3/18，pipeline与其余所有layout/reserved/enums/options一致。恢复19份schema不变的精确预像噪声，不恢复上游修改的pipeline。21无schema/grammar/native/module变化，生成资格限定复用，不冒称第三次重跑生成。持久视图通过SQL名字绑定598，而非保存旧未发布ID。
- **175范围fresh资格**：8完整ordinary owners全部PASS、35382 PASS事件，首次严格覆盖**575/740=77.70%**。真实inheritance/recovery/两CN各PASS（5.16s/11.49s/7.45s），后者实际109/110/111拒绝及112恢复控制成功；11包vet/lint成功。此前磁盘准入拒绝不是整体PASS，失败终态全部保留。
- **175公开比较**：正确native-linked13完整脚本同实例两轮均**2403 total / 2402 success / 1原ignore / 0 failed / 0 abnormal**，Java50s/51s，driver145.10s。下一轮初始化前mapped databases/accounts/snapshots全部0，errors空。首轮12脚本遗漏历史完整clone_db清理owner，导致clone_sys计数+3及dangling view使cluster restore失败（1989/1958/30 failed/1原ignore）；保留报告/日志，只补回完整公共owner，未改计数/错误预期、增加ignore或临时DROP。新golden候选的两处旧trailing-empty格式噪声不采纳。
- **21当前owner与消费者**：6个完整fresh owners：mysql parser1.101s/1519 PASS、parsers0.817s/92、plan13.226s/14978、function10.947s/10743、compile18.064s/3149、frontend29.708s/5624，共36105 PASS事件。重新真实inheritance/recovery/两CN，并加入MixedIntegerComparisons与PreparedZeroLimitReuse，五个精确顶层全部JSON PASS/exit0；只宣称选择内消费者，非全sqlintegration/issues套件。
- **当前覆盖/SCA**：相对21严格仍**575/740=77.70%**，aux680/785=86.62%；仅上述6 fresh successful atomic profiles及175的pb/sysview/4.0.15三份成功profile（owning package源字节相同、wire/floor/DDL定义/native/module输入不变；相关lexer/执行消费者另有fresh SQL/公开证明）。旧4个core及旧parser、失败/取消/未执行SQL profiles不进入当前数值。缺映射仍计未覆盖。当前12-package vet/lint exit0/0 issues，89份非生成Go匹配gofmt通过。
- **当前风险审查**：977行cd8交互、218行175交互、2560行21源/测试/public增量均已完整可见读取，连同作者门控/SHOW/assignment及新oracle差量形成统一闭包。21 qualified扫描上下文defer/reset、comparison域独立于assignment、SELECT限定LIMIT=0借用计划Compile返回恢复，未覆盖C03 CREATE/ALTER锁后freshness。新准入只改未发布字面能力，不改C03锁/取消/共享可写生命周期；旧txn及其它race/restart证据保持历史合同，不冒称本轮重跑。无新state/cache/worker/row scan/生产retry。未认定零ERROR日志或远端Linux green。
- **资源与证据**：按用户“自己清理”授权，只清可再生Go/lint缓存；精确路径/hash/小时使用时间/active-importcfg/open-handle筛选及停止记录保留。UT报告经解压SHA256验证无损压缩。源码/WIP/模块缓存/native/database/证据/worktree及无关进程保留；保留16GiB及>17GiB admission，不归因host后来65GiB恢复。所有私有driver/TODO/manifest/profile/log/mirror留本worktree，不提交。

**21范围历史最终本地验收闭环**：正确native新二进制、16完整public scripts同实例两轮均**2507 total / 2506 success / 1原ignore / 0 failed / 0 abnormal**，Java66s/141s，完整driver262.61s PASS/exit0。32份SQL/result与交付输入逐字节/hash一致，errors均空，两个下一初始化前mapped databases/accounts/snapshots（含三新增公共库）全0，owned launcher/test/Java终态与teardown通过。2547条服务ERROR保留，未宣称零ERROR；未发现panic/fatal/data-race签名。只读资格parser首次误将SUMMARY当第17个script的失败保留，排除SUMMARY后完整exact selection通过，未改oracle或重跑成功SQL。所有当前适用本地blockers关闭；正常提交/推送现有ready PR，最终SHA/远端HEAD/ready/mergeability另行核验，**不代表远端Linux CI已绿**。

## 2026-10-13：上一轮主干冲突维护（冻结main 35c8dd5854，历史资格）

- **输入与正常合并**：从已交付fe2ed74f正常merge冻结35c8dd5854750ef96f52f9dd298a70acd031d5e4，保留现有ready PR及全部历史。incoming为EXPLAIN JSON与SDK Actions依赖矩阵；唯一文本冲突mysql_sql.go由组合grammar正常goyacc生成，两次SHA256相同，不手改generated/ours-theirs整文件覆盖。DEFAULT/CONVERT和EXPLAIN JSON/CURRENT_USER在同一旧三行fixture前后交替parse、typed options/Convert与format/reparse控制通过。SDK change-scope Node单测成功，不宣称整个Python SDK矩阵或Linux CI。
- **编译红证据与修复**：fresh explain owner build-fail发现上游JSON仍引用已移除VectorIndexScan API。保留JSON/stderr/exit及13-byte不完整profile，不纳入coverage。只迁移新消费端到已发布111的INDEX_SEARCH_SCAN/IndexSearchScan、QueryPayload、AlgoOptions/AlgoExprs；用既有plugin ExplainSettings hook并传播错误、opaque bytes完整hex、named-expression完整值及缺项/空名/重复/NULL拒绝。无schema/协议/plugin注册/接口/reader/GPU/ISCP/CDC改动，无新生产state/cache/worker/locks/scans/retry。当前仍C03 **112 / 4.0.15 / database_defaults#18 / function598 / END599**。
- **fresh完整owners**：repository CGo、Go1.27.1、enabled vet、count1/atomic：mysql parser1.390s/1522 PASS事件，parser facade1.042s/91，tree4.641s/130，plan59.718s/14983，explain1.682s/336，function10.673s/10742，compile12.025s/3148，frontend26.913s/5628；合计**36580 PASS事件**，每包有非空tests、package终态PASS及exit0。parser/tree/plan为本轮已终态且源未变的先前成功运行，不因资源gate拒绝把其标为重跑；后四为typed index兼容修复后的fresh终态。
- **实际执行消费者**：精确选择TestDatabaseCharsetInheritanceContract **2.01s PASS**、TestDatabaseDefaultsFailureAndRecovery **11.07s PASS**、TestDatabaseDefaultsTwoCNPreparedFreshness **6.39s PASS**，两包终态PASS/exit0，保留metadata/domain/generation/authorization/rollback以及109–111拒绝/112恢复控制。不是全sqlintegration/multicn suite；另由完整public JSON覆盖prepared EXPLAIN/DML不执行及ANALYZE/PHYPLAN/CHECK拒绝。index typed焦点包含迁移后的原payload/settings/range正控与九类非法布局/codec控制，终态PASS。
- **严格覆盖**：相对35c8，非生成可执行statement-token差分**601/768=78.26%**，辅助changed-block **709/833=85.11%**。只使用上述8成功fresh atomic profiles与限定同源PB-plan/sysview/4.0.15三份成功历史profiles：完整package与d5bf0685字节相同、定义/协议/native/module输入保持；误与raw main175比较的拒绝保留，未忽略C03分支。缺采集/映射算未覆盖（包括tree原未命中格式路径、txn/disttae等历史源），不会拿旧core/失败/取消/13-byte profile补命中。新JSON消费端changed-block29/30。
- **静态与评审**：13完整适用package vet/lint均exit0、无lint问题；91份相对35非生成Go匹配同Go1.27.1 gofmt，base/staged whitespace检查成功。完整组合grammar/自动合并frontend及plan消费者、新JSON与既有index-plugin hook/当前schema、修复source/tests及三条public regex差量可见审阅；局部临时map按算法表达式数量有界，formatter持有无跨调用状态。既有race/restart/upgrade保持历史资格，单次诊断serialization无新并发owner，不机械重跑无关GPU/全仓。
- **公开普通比较**：原16完整公共owner加完整optimizer/explain_format_json.sql，共17脚本，同一native-linked新service普通run.sh -g -n双轮；每轮**2575 total / 2574 success / 1原ignore / 0 failed / 0 abnormal**，Java53s/54s，native driver **150.75s PASS**/exit0。新JSON68/68两轮，payload两种精确bytes、NProbe16、first-round2/3、上下界/prepared/查询只读控制保持；只机械同步两个query_payload标签和一个NProbe说明标签，result数据字节不改、不genrs、不新skip/retry/sleep/ignore/宽松oracle。20M PK/120000ms/ddl_accounting_ok=1/wait(1,60)不变。
- **selection/闭包/日志**：17个精确SQL路径，34份SQL/result源与私有mirror SHA256一致；两份report与selection精确相符、正TOTAL、errors各0bytes。两轮下次初始化前mapped databases/accounts/snapshots为0（含JSON库），fixture leftovers0；service-close、admission及port-lease-release成功且owned test终态。2546条服务ERROR完整保留，panic/fatal/data-race签名0，不宣称零ERROR日志。
- **资源及失败诚实性**：16GiB reserve/>17GiB admission不变，多次不足则未启动下一owner；所有拒绝与初始错误oracle/旧index API build-fail保留。终态private GoCache经路径/hash/ps env/open-handle守卫执行go clean -cache，随后APFS只读clone标准缓存（73738条/27.51GB逻辑量，output hash/Go keys正确，0 errors）；后来>1h、无active-importcfg/response-file pin/open handles的标准cache及本任务未使用匹配clone各回收5.366GB逻辑archive，APFS共享量不相加冒称物理回收，post-private余量19.92GiB。保留源码/WIP/模块/native/database/evidence/worktrees及无关进程。driver/TODO/cache/report/profile/mirror/review等保持私有。

35c8范围适用本地交付门已关闭，正常merge commit6a3ad451/普通push现有ready PR，保留fe2ed74f/35c8祖先，无rebase/force/new PR/issue评论或关闭/持续CI监控。此历史Darwin/embed PASS不是远端Linux CI green证明。

## 2026-10-13：当前CI与共享索引冲突维护（冻结main 8586fa0871）

- **CI根因**：38377757841的Ubuntu UT/UT coverage在checkout精确6a3ad451时多次gnutls_handshake TLS失败、exit128，未开始测试或产出profile；聚合prerequisites_ready=false、缺required producer，明确未计算覆盖率verdict，不是覆盖百分比失败。原run/job日志保留；不新增产品retry/skip/CI门豁免，正常push新merge触发新的普通CI，不宣称尚未终态的Linux green。
- **正常合并与合同**：incoming f14a1fa9ee共享索引说明、47bbf28c05 legacy varlen、8586 LOAD NULLIF。只在sql_json.go及sql_json_test.go冲突，逐hunk协调，不手改生成文件/whole-file ours/theirs。保留新主干canonical labels/settings、indexSearchScanSettings(spec,true)、name=value和vector query_vector/fulltext query_payload；本分支opaque完整hex与layout/空名/duplicate/NULL拒绝保留。完整expanded JSON SQL/result和LOAD SQL/result/resource与main精确相同，无golden重生成；proto/native/modules/grammar、112/defaults18/4.0.15/598/599不变。
- **当前完整owners**：Go1.27.1、repo CGo、vet启用、count1/atomic：explain10.498s/353 PASS事件；plan15.335s/14985；function18.122s/10742；compile18.249s/3148；frontend28.433s/5628；vector1.907s/670；IVF1.340s/8；HNSW1.350s/10；classic fulltext1.922s/60；fulltext2 1.327s/38。共**35642 PASS事件**，10包各非空package终态PASS/exit0/完整profile。焦点原JSON及布局控制PASS；不是全仓UT、GPU runtime或SDK全矩阵。
- **实际SQL及红证据**：原3实际顶层及所有子例PASS：recovery9.14s、inheritance2.09s、两CN6.12s，两个包终态PASS。首轮LOG服务10060 TCP bind冲突，未进入SQL断言，确切原占用PID未取得，保留日志/exit，不把resource拒绝作产品通过。恢复driver一度误传wrapper `-- go test`造成含私有tool的错误根package编译，保留R2失败并修正为正常wrapper参数。借用现有mo-test-port-leases/10000.lock排除冲突段验证、终态释放，不终止他人、不改fixture/SQL断言/timeout/预算/skip/retry。BVT前旧R1 gate因未产出VALIDATE.exit拒绝保留，改为独立成功R3聚合；验证器子例/顶层误计的拒绝保留，未弱化断言。
- **覆盖与静态**：相对8586，strict非生成statement-token **588/753=78.09%**，aux changed-block **695/818=84.96%**。当前10成功atomic profiles加明确byte-qualified的parser/facade/tree及PB/sysview/4.0.15成功profile；同源资格检查涵盖相关直接Go及proto/定义/native/module输入，不用旧core或红/未完成profile填缺，missing/unmapped仍未覆盖。18完整适用package vet/lint exit0、91非生成Go匹配gofmt及whitespace PASS。
- **公开普通双轮**：原16完整owner，加完整expanded EXPLAIN JSON及完整LOAD user-variable，共18，正确新native-linked service普通run.sh -g -n同实例两轮，每轮**2631 total / 2630 success / 1原ignore / 0 failed / 0 abnormal**，Java55s/55s，driver**156.16s PASS**/exit0。JSON115/115、LOAD9/9各两轮，包含IVF/HNSW threads/text、classic/fulltext2 mode/include/score/prepared controls和LOAD5行NULL/空串/普通值；原完整C03 metadata/20M PK/120000ms/ddl_accounting_ok/wait controls不改，无新ignore/genrs/宽松oracle。
- **资格闭包**：18精确selection、36源/mirror SHA256，两报告19行与selection精确一致、errors各0bytes；每轮fixture leftovers0及mapped databases/accounts/snapshots0（含issue_29849/JSON库），service-close/admission/port租约teardown ready、owned test退出。2560条service ERROR完整保留，panic/fatal/data-race签名0，不声称零ERROR。
- **资源与风险**：16GiB reserve/>17GiB gate保持；标准cache无旧archive候选的拒绝保留；仅私有>1h Go archive经hash、当前importcfg/response-file pins及open-handle复核回收5,386,622,686逻辑bytes。没有清模块/native/源码/WIP/database/证据/worktree/活动缓存，不归因host其它物理空间恢复。无新增生产state/cache/worker/locks/waits/retry，所有原历史race/restart/upgrade证据仍限原合同；GPU说明接口仅审阅不冒称runtime运行。私有TODO/driver/profile/mirror/report/review不交付。

本轮本地适用门均已关闭，正常merge/push原ready PR、保留6a3祖先及冻结8586父对象；单次远端SHA/ready/mergeability另存。**本地资格与MERGEABLE不代表远端Linux CI green；不持续监控或评论/关闭issue。**

## 2026-10-14：当前CI与生成/协议冲突维护（冻结main fcad62a8ff）

- **CI定位**：38421272142 coverage job115379184105的完整诊断证明TestUnicodeCollationConsumerContract在unicode_collation_test.go:218失败：SHOW FULL COLUMNS返回binary而非NULL。原测试不变，修复SHOW专用投影；information_schema binary保留。普通UT job115379110169取消，完整JSON无failed/pending事件，最后pkg/tests/issues在16:56:22 PASS；取消原因未确认，不冒称其已修复或远端CI绿色。
- **协议/生成**：保留main calendar112，C03迁113；upgrade成功CN113/113、负控113/112及sysview/plan/frontend/双CN109–112拒绝矩阵通过。迁移4.0.15/min4.0.14/offset4、PB18/FK16–17/reserved14/RawSQL15、函数598/END599不变。正常双次PB/parser生成，最终parser用make -B纠正时间戳noop；完整descriptor仅增加批准defaults消息/5/3/18，其余新main（包括nprobe2）相同。
- **当前11完整owner**：Go1.27.1、repo CGo/vet启用、count1/atomic，逐包非空terminal PASS/exit0/profile哈希资格核对。sysview1.616s/63 PASS事件；migration1.982s/7；PB0.923s/186；mysql parser9.405s/1585；facade2.290s/91；tree7.006s/134；plan38.782s/15122；function15.736s/10771；compile18.573s/3153；frontend31.391s/5666；explain2.949s/378。合计**37156 PASS事件**，不是全仓UT。
- **实际SQL**：原Unicode消费者0.81s；inheritance3.99s、recovery5.85s、双CN12.75s；calendar5.89s、geometry0.20s、legacy partition列宽COPY10.03s；external pruning0.65s、shared CTE0.15s、filtered set-operation0.40s。共10 top-level、全部子例和两个package terminal PASS/exit0；复用现有端口租约，未改fixture/资源/断言。
- **覆盖/静态**：只合并当前11成功atomic profiles，相对精确父对象fcad：严格非生成可执行token **609/756=80.56%**，辅助changed-block **718/821=87.45%**；缺采集/缺映射计未覆盖，未使用红/不完整/历史profile填补。14适用包vet/lint exit0，91个非生成Go gofmt及base/staged whitespace通过；历史race/restart/upgrade保留原输入范围。
- **公开普通双轮**：完整原18脚本＋rank options、外表NULLIF重绑、4096列宽、subscription标识符、procedure index，共23，普通run.sh -g -n/native-linked/原compose预算/同实例两轮。各**2809 total /2808 success /1原ignore /0 failed /0 abnormal**，Java54s/58s，TestCIMoTester **164.07s PASS**/exit0。46 SQL/result源文件及46 mirror逐SHA核对、每报告24行和精确selection一致、errors均0bytes；fixture及全部mapped DB/account/snapshot每轮0。owned PID82733已退出、service/admission/port teardown ready。**2782条service ERROR保留**，panic/fatal/data-race签名0，不宣称零ERROR。20M PK/120000ms/accounting/wait原契约不改，不将旧20M资格冒称本轮重跑。
- **失败保留与修复界限**：parser noop冲突编译、upgrade旧112夹具失败profile（仅夹具升级至113）、lint首轮包加载10m超时退出4完整保存，不计成功；只终止该失败lint在本worktree的精确PPID1孤儿compiler61033，未碰其它任务。初次BVT被17GiB准入拒绝，未运行断言。公开首轮**2809/2807/1失败/1原ignore**、driver91.50s exit1完整保存：第三处native_defaults.raw SHOW oracle仍binary；仅纠正该NULL单元格，保留两处information_schema binary/binary及所有字符/generated控制。另两处explicit_binary SHOW单元格同合同纠正；无golden重生成/SQL或数据改动。修复后完整双轮闭包成功，不隐藏首轮。
- **资源/交付**：16GiB reserve/>17GiB准入不变。仅经hash/mtime/完整活动root importcfg及response pins/open-handle复核回收再生Go archive；所有逻辑字节与实测物理余量分开记录，空候选安全拒绝，不调用bare lsof。保留source/WIP/database/native/module/evidence/worktree及其它进程。原PR ready历史不改，正常merge commit/普通push；私有TODO/driver/report/profile/cache/mirror/self-review不提交。远端Linux绿与BLOCKED门尚未确认，单次交付SHA/mergeability核验不替代CI终态。
- **冻结输入纠正**：最终父对象校验发现初始记录2ce4不是实际MERGE_HEAD；实际fcad62a8ff是其直接子，只有已在本轮全部source/owner输入中的ADD COLUMN列表grammar/parser/32行测试。完整阅读该差分，原TestAlterAddParenthesizedColumns已在完整parser包PASS；源码/测试未再变。按精确父对象重跑descriptor、AST paths/coverage（AST列表完全相同、仍609/756）及14包静态均PASS，修正文档；不把错误父对象记录当验收。


## CI 后续修复：跨 CN 普通读取因果边界（95f9 起点）

- 远端精确输入：`95f9d05305b2bb4369d03914a66b8973bb68a9b2`，run `38512534886` / coverage job `115685024687`。诊断 artifact `11871971761` 确认 `TestDatabaseDefaultsTwoCNPreparedFreshness` 原第137行期望2、实际0；其余 coverage 失败事件是同一包的终态。379秒准入等待不等于 SQL 超时失败。
- 修复仅涉及 `pkg/tests/sqlintegration/multicn/database_defaults_test.go`：CN1 INSERT→CN0 SELECT，以及两CN提交→CN1代际读取，使用实际 `GetLatestCommitTS()` 与现有 `WaitLogTailAppliedAt()` 建立因果边界，并断言返回 snapshot 严格晚于提交。每次等待有10秒子预算，继承原120秒上下文；失败仍直接终止，不重试、不 sleep。
- **保留产品新鲜度的原测试强度**：ALTER DATABASE→prepared EXECUTE 之间没有插入屏障；text/binary协议、metadata、比较结果2、version恰好+2、取消清理和同名identity替换全部保持原断言。没有生产代码、协议、SQL/data/result修改。
- 本地 Go1.27.1/native CGo：完整 multicn 包 `-count=3 -covermode=atomic` PASS / exit0，15个top-level及45个含子用例PASS事件，包终态115.626秒；原失败测试分别6.10、6.16、7.31秒。vet、增量lint及diff whitespace检查通过。端口排除租约已释放，资格driver终态完成。
- 该包仅含测试代码，coverprofile只有 `mode: atomic` 头，**明确排除为覆盖证据**；首次要求非空profile的资格校验拒绝保留，不将其冒充生产覆盖率。本次新增生产可执行token为0；既有生产输入完全不变，因此不扩张上一轮严格609/756=80.56%或race/restart/upgrade/public BVT的资格范围。未重新跑完整Linux coverage。
- 被取消的普通UT/PROXY不算成功或产品失败；PROXY诊断日志未见panic/fatal/data-race签名，但不是完整测试通过证明。CI Required失败是门禁汇总结果。修复交付后远端Linux green仍须终态证据，不能从本地PASS推断。
- 本轮未合并新上游：fetch所得 `mo/main=2e139d84bf8ad297fd3c08782c5b9c113213416d` 留作上下文；当前请求限CI修复，不扩大为11个新上游提交的维护重验。


## CI 后续修复：旧分区宽表 COPY 阶段预算（065d71 起点）

- 精确远端输入：`065d71ecf71468db31513804bb5c500f5832ead9`，run `38534034119` / UT job `115755398553`。只有 `TestLegacyPartitionColumnLimitCopy` 及其包失败；coverage及上一轮multicn修复已通过。`CI Required` 是门禁汇总，不是第二个测试故障。
- 首次 `gh run view` 输出中途结束；diagnostic artifact预算也删除失败输出。随后使用 `gh api --allow-escape-sequences` 得到完整2795385字节原始日志，确认原第186行 `alter table wide add replacement int, drop c4096` 在11:35:20报 `context deadline exceeded`。整个case共享180秒上下文，前面的准备及多次4098列COPY已消耗其预算；193.16秒包含后续独立cleanup/quarantine，不能当作单条SQL的执行耗时。
- 最小fixture：seed只保留相同的c0主键/hash(c0)/2分区模板；现有typed engine仍生成真正4098列的父表及两个分区子表。全部原require断言逐行比对一致；4098→4097→4096、两行95/195、rollback、replacement、1117/HY000、2分区、无copy残留、会话存活和最终数据库清理均不变。正常CREATE4096边界仍有原planner UT及公共table_column_limit SQL，不删其oracle。
- 预算修复：按已有seeding/rollback/committed shrink/replacement/final reduction/cleanup状态边界分配上下文，**每阶段仍限180秒，总测试上限明确由180秒改为600秒**。上一context即时cancel，最后context及外层context均defer释放；不增加单阶段预算，不添加重试/sleep/skip，不改生产等待或公共20000000行PK COPY的120000ms socket预算。
- 本地Go1.27.1/native，原源码focused race PASS79.70秒。仅缩小seed候选race PASS92.79秒且有并行static编译，不能据此声称提速或CI已修复。最终 `-race -tags=matrixone_test` 测量PASS75.11秒；B=30秒，N=clamp(floor(B/T),1,100)=1，独立同模式replay PASS67.47秒。所有完整package终态与exit均为0，不把首轮候选冒充最终输入资格。
- 最终普通完整 `pkg/tests/sqlintegration` 包PASS185.833秒 / exit0，42 top-level与301含子用例PASS事件，目标用例10.55秒；最终gofmt、vet、增量lint及whitespace检查通过。源码SHA256 `9eaf2a7d5b65884936f33666b50fe3820ad099b1fccc36d93a8aea29b652878a` 与资格记录一致。
- 没有生产可执行token差异；既有严格609/756=80.56%的生产输入未改，但不把旧profile扩张为新一轮完整coverage/race/restart/upgrade资格。未改公共BVT SQL/result或共享fixture所有者，因此不新增重复BVT/全包race。
- 构建期间按既有16GiB保留/>17GiB准入边界回收确认闲置的Go archive缓存：记录哈希、mtime、open handles、当前response/importcfg及活动build-root pins；保留源码/WIP/模块/原生产物/诊断/worktree和无关进程。逻辑回收字节不等于APFS物理回收量。
- 仍保留原始Linux失败、截断输出、初始API转义拒绝、baseline与候选测量。最终本地PASS不能证明新提交远端Linux绿色；以推送后确切head终态为准。

## 上一轮维护资格：生成表达式评审与main冲突（2026-10-15，170a）

- **冻结输入及分配**：从f5a7继续正常合并main `17a6200f59e764729e5544908fffd5480ae88279`，交付前远端main核验仍相同。保留main DECIMAL variance/stddev DOUBLE协议113；未发布C03迁114，113成为旧peer负控。迁移4.0.15/min4.0.14/offset4、CreateTable.defaults18、函数598/END599不变。两个冲突逐项解决，pipeline与parser按owner正常双次生成一致；完整pipeline descriptor等于main，plan仅批准defaults增量，所有其余字段/reserved/options/enum保留。
- **评审反例及修复**：review5523469825要求修复CONVERT在会话div_precision_increment从10改0后把已有生成值0.333333333333重算为0.33。有效RED_R2用旧三个owner的overlay在合并内核复现同域/换域失败，而未改会话及普通COPY控制通过。转换专用typed replay只保留输入域未变的持久化计算；适配目的赋值并保留作者CAST，原始/可见位置只重映射一次，多次换域不增长。普通MODIFY及真正受影响的输入仍按原绑定路径执行；不改COPY事务/索引/约束/清理所有权。
- **当前11完整owner**：Go1.27.1、Darwin/arm64、仓库native/CGo及vet启用，sysview/migration/PB/mysql parser/facade/tree/plan/function/compile/frontend/explain均非空package PASS/exit0、完整atomic profiles，共**38174 PASS事件**。只采用有界CAST修正后的完整plan_final profile；最新typed补充为3 top/38 PASS/5.245秒，9个算术/作者CAST×bin/general/native域子例，并覆盖DEFAULT、ON UPDATE、源schema不变、COPY及8次往返深度。不是全仓UT。
- **真实SQL完整owner**：普通sqlintegration包**45 top/310 PASS/187.182秒**，含原inheritance/recovery/Unicode/legacy COPY等全部消费者以及新SQL入口4组控制，检查既有值、目的域、后续插入及恰好两行。multicn完整包`-count=3`为**15 top/45 PASS/118.649秒**，保留跨CN普通读取提交屏障，但ALTER→prepared EXECUTE仍无新增屏障。
- **多CN首轮失败保留**：三次均在expected113/actual114的原拒绝消息失败；纯RPC/字符串字面量文件未被最初defines符号迁移覆盖。恢复版本改114、原109–112负控保留并新增113，两处消息改114；未改原120秒上下文、数据或断言强度。该失败profile不参与覆盖，不伪装成产品/logtail失败或PASS。
- **当前race**：只针对新真实SQL入口，原`-race -tags=matrixone_test`，目标首次测得**T=9.16秒，B=30秒，N=3**；完整package17.016秒/exit0。独立同模式3次replay完整package19.084秒/exit0，所有目标及子例PASS，无data-race签名。不是全包race，不扩张历史事务/restart/upgrade资格。
- **严格覆盖/静态**：精确main差分、非生成可执行AST token行，**687/852=80.63%**；辅助changed-block **802/920=87.17%**。仅本轮11成功owner及最新typed补充profiles，缺采集/无映射计未覆盖，无历史/红/不完整profile填补。最终多CN修正后重新执行**15包vet/lint exit0**，包含pipeline包；96非生成Go文件符合Go1.27.1 gofmt，C03相对main的whitespace及index/工作区一致性通过。合并引入main既有view_metadata_on_demand.result三处行尾空格，逐项与main一致，不改其oracle；不声称HEAD全合并差分无行尾空格。
- **公开SQL独立oracle**：真实native-linked tester通过原run.sh -g -n只生成新增conversion_expressions.result。独立核对10个数据行均g=0.333333333333、作者CAST=0.3333、默认值=0.333333333333，且目的三列general_ci。原23脚本/oracle不重生成；47个生成前输入与mirror哈希均不变，最终24对SQL/result各48个源及48个mirror核对一致。
- **24完整public同实例双轮**：每轮**2871 total/2870 success/0 failed/1原ignore/0 abnormal**，Java54秒/49秒，driver **155.41秒PASS/exit0**。新脚本22/22两次通过；每报告25行、errors文件0bytes，fixture以及mapped DB/account/snapshot每轮残留0；service/admission/port teardown ready、owned PID94683退出。保留**2762条service ERROR**，panic/fatal/data-race签名0，不声称零ERROR。原20M行PK COPY、120000ms socket、accounting/wait/ignore及CN/TN预算不改。
- **证据界限**：首RED边编译边修改造成vet未定义符号、最初prototype GREEN、早于有界修正的plan profile、ON UPDATE非法语法候选均单列保留，不算最终资格。当前生产源码在最终plan/SQL/race/public验证期间未变；后补typed测试与多CN字面量修正由各自新鲜资格覆盖。完整review正文及全部main交互hunk完成自审，无未解决本地阻塞；新head远端Linux CI及外部复审仍须独立终态。
- **资源/交付**：只回收确认可再生且不在活动importcfg/response/build-root/open-handle中的Go archive，保留manifest/SHA/mtime；标准旧cache耗尽后仅转本任务私有Go cache（标准最小age3600秒，私有600秒），流式hash避免大archive瞬时内存放大。16GiB为保留目标，新重型步骤>17.5GiB准入；外部并行构建曾令即时余量低于目标，持续自动回收恢复，不用逻辑字节冒充物理回收。源码/WIP/模块/数据库/原生/证据/worktree及无关进程均保留。原ready PR正常merge/普通push，私有TODO/driver/log/cache/profile/review不提交。

## 历史维护资格：函数598表达式门禁及启动闭包（2026-10-15）

- **输入/范围**：从f53bbf18cad9正常合并任务启动时最新main **bb2b725d0af4601d02ebf9cc68b1816a2b300022**。新inline4280094988的113发布漏洞由planner与真实SQL overlay分别RED复现；执行ID598→共享feature→author/load/placement/send/receive统一114，保留596/597与legacy view控制。无生成代码、wire字段、执行ID或迁移版本变化。验证期间main前进到461bc8e349，未合入其无关24文件；当前证据/PR merge-base仍为bb2，已核验与前进main无文本合并冲突，不称验证了461的新代码。
- **启动闭包**：首GREEN真实启动因author floor0而失败，保留原始红记录。prerequisite先用legacy TABLES，admission114后在既有derived事务原子替换TABLES并创建四个trace completion views；旧完整cluster skip不变。新增schema选择、DDL顺序及两个错误停止、113/pending114/ready114门槛；实际SQL同时证明先成功DROP后的CREATE拒绝会回滚原rel_id，之后114重试成功。直接/prepared发布和reader113拒绝均覆盖，恢复协议、deallocate与清库使用独立cleanup context。
- **完整owner**：Go1.27.1、Darwin/arm64、仓库native/CGo、vet启用。PB/plan/function/compile/frontend/explain/sysview/bootstrap/cnservice **9个新鲜完整owner，36646 PASS事件**，全部exit0及完整atomic profile。明确复用未改parser/facade/tree三份和migration一份profile；不采用失败、截断或取消profile。另main shardservice完整normal **75 top/86 PASS/106.964秒**。
- **实际SQL选取与终态**：显式pattern=.的完整sqlintegration为 **46 top/311 PASS/180.090秒/exit0**，包含新直接/预编译/系统视图回滚控制；新case独立G3 **7.776秒**。最初误称全包的命令实际被旧runner默认过滤为列上限1用例，已向用户纠正；该日志只保留单用例资格，不计完整SQL。完整multicn `-count=3` **15 top/45 PASS/122.186秒**，无新增ALTER→prepared屏障。
- **race**：精确测试分别测量后按B=30秒独立stress：CN completion T=0（低于JSON分辨率）/N100、CN barrier T=.02/N100、bootstrap retry/cancel T=.20/N100；各自命中100次顶层PASS。完整cnservice **155 top/320 PASS/11.699秒**、bootstrap **64 top/162 PASS/15.861秒**。main并发cache精确stress100次，完整shardservice race **75 top/86 PASS/48.834秒**；全部exit0，无data-race签名。不是全仓race，也不扩张历史真实旧二进制restart/upgrade/GPU资格。
- **覆盖/静态**：相对固定bb2、非生成可执行AST差分token，严格 **719/896=80.25%**（缺采集计未覆盖），辅助基本块 **864/996=86.75%**；仅9 fresh+4明确复用有效profile。**11包vet/lint exit0、0 issues**，**110个非生成Go文件gofmt干净**；相对bb2及最终staged whitespace通过，全部资格期间生产源hash未变。
- **27完整public同实例双轮**：原run.sh -g -n正常比较，**未生成任何oracle**。每轮 **2998 total/2997 success/0 failed/1原ignore/0 abnormal**，Java49秒/51秒，driver **153.33秒PASS/exit0**。每报告28行、errors0bytes，54源/54mirror hash一致；每轮fixture与mapped DB/account/snapshot残留0；service/admission/port teardown ready，owned PID4487退出。保留 **2791条service ERROR**，panic/fatal/data-race签名0，不称零ERROR。
- **失败诚实与资源**：旧CN正控98卡在新114门槛的初跑被本任务SIGQUIT取栈中断；保留失败，改正控114及精确5 CREATE后完整CN重跑，不加超时/sleep/重试或削弱gate。16GiB reserve/>17.5GiB重型准入；仅受保护非活动Go archives回收，保留pins/mtime/header/open handles/SHA manifests及所有源码/WIP/模块/数据库/native/证据/worktree，不停止无关进程。
- **交付边界**：正常merge/push原ready PR，仅两份既有CLAUDE设计/验证文档交付，私有TODO/驱动/日志/profile不提交。用户本轮明确要求**不申请复审**，只resolve已修inline。前轮完整macOS optools失败仍是限制，本轮未重标绿；不声称新head Linux CI或外部复审成功。

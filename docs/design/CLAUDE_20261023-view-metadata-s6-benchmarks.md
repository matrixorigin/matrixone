# S6：View 按需元数据基准方法与证据边界

任务：[#29441](https://github.com/matrixorigin/matrixone/issues/29441)，系列 [#29433](https://github.com/matrixorigin/matrixone/issues/29433)。

依据：[S1 批准合同 §11.2](CLAUDE_20260922-view-metadata-on-demand.md#112-固定基准合同s6)、[S2 v2.1](CLAUDE_20261005-view-s2-scope-revision.md)、[S3](CLAUDE_20261018-view-s3-consumers.md)、[S4](CLAUDE_20261019-view-metadata-s4-consumers.md)。本文不修改既有数值预算，不批准默认激活或跨请求缓存。

## 1. 当前决定

**S7 决策未定；S6 正式性能门禁尚未完成。本机已获准作为本次 S6 验收环境，仍缺三轮及完整矩阵。** 已完成同口径历史 B0 的资格确认和部分采样，但不能宣布“不需要缓存”，也不能因单次本机波动就宣布“必须加缓存”。S8 仍须等待 S5 与 S6。

本稿交付可复现的 tagged 基准/计量、历史 B0 测试适配，以及 §6.7 的现有 regexp 缓存容器惰性分配。后者影响默认函数的分配时机，但不改变 SQL 语义、缓存容量/所有权或缓存范围；**不是默认激活 View v2，也不是 S7 实现**。本稿为部分交付，不关闭 #29441。

### 1.1 环境变更批准（2026-10-23，重新采样前）

用户明确确认：**“确认采用本机作为 S6 验收环境”**。本次 S6 将 S1 的独立 8 CPU/16 GiB 环境条件替换为下列本机原生配置；不是声称两环境完全等价，也不外推为原固定配额/Linux 环境达标。**原 latency/分配/内存/并发/样本量预算不变。**

- Apple M1 Pro，Darwin/arm64，10 个逻辑 CPU、16 GiB 物理内存、本地 SSD；单 CN+TN，独立测试数据/端口。不是 OrbStack 容器环境。
- 固定 `GOMAXPROCS=8`、`GOGC=100`、`GOMEMLIMIT=off`；这些是 Go 运行参数，不是 CPU/整实例内存硬隔离。每个源码 revision 使用自身 go.mod 工具链及匹配 native。
- 先编译再测量；正常延迟关闭新增计量，不带 race/coverage/profile。无其它 MO 测试/编译重负载时启动，并持续记录进程负载和内存压力；不终止其它任务。
- 比较至少三轮，预先轮换运行顺序，保留每轮全部样本和波动。外来重负载有独立记录时标记整轮受干扰，不根据某轮更慢而剔除，不删除慢样本或失败。当前兼容路径仍不能替代 B0。
- §6.1/§6.2 和 phase2 的既有性能数据保持 **diagnostic-only**，不因批准而追认。按新配置重新采样，数值预算失败继续阻塞验收，需优化或另行审批。

本机环境不再是等待外部机器的阻塞项；尚未完成的同口径驱动、矩阵及资源归因不会随环境批准自动完成。

## 2. 比较对象和执行边界

| 名称 | 实际执行内容 | 不能据此声称 |
| --- | --- | --- |
| `current_compat` | **相同当前 head** 的默认兼容按需路径 | 不是历史 B0，不是完全运转的 durable refresh，也不代表所有路径都读 stale 列 |
| `cache_off` | S4 `NewViewMetadataRequest` 或 S3 `ViewSchemaRequest.BuildPlan`，禁用请求 memo | 不禁用引擎/目录缓存；不是整个数据库的 cache-off |
| `statement_memo` | 同一消费者、同一 head，正常 statement-local memo | 不跨语句共享结果；单对象跨请求暖读不应出现该 memo 命中 |
| 历史 B0（已验证，正式三轮未齐） | S1 基线 `d99187d7b3bfda8744088b3ae8cf16739b690aa0` | 不以当前兼容路径静默替代；该版本 go.mod 为 Go 1.26.4，须披露与当前工具链的差异 |

每个 op 都新建事务、process、compiler/request，并完整消费结果、释放句柄、关闭 request、关闭借用者自身资源及 rollback 测试事务。计时包含这些成本，不包含建库/建表/View 的 setup。SHOW 通过真实 frontend back-exec；I_S 通过真实 SQLExecutor 规划及 origin-CN 执行并流式消费；S3 明确是规划基准，CTAS 不创建目标表，PREPARE 不冒充 binary EXECUTE/cursor。

为在同一请求上配置和读取计量，S3 基准展开 `BuildPlanWithViewSchema` 的三行 owner 包装：同一个 `NewViewSchemaRequest → BuildPlan → Close`，只插入 tagged 配置/统计。真实封装入口由既有 native SELECT/CTAS/PREPARE、权限/订阅测试回归，不引入全局注入开关。

I_S 查询投影为 table_name、column_name、column_type，并按表名/列序排序、逐行验证。集合不是 `COUNT(*)` 的替代测量。接收通道容量 1，失败后取消、排空 batch 并 join，不让 harness 全量保留结果。排序本身仍是 SQL 合同的一部分。普通表/无关 View 是否进入推导由根描述计数独立验证。

SQLExecutor 本来每次解析/优化。基准不使用 `WithForceRebuildPlan`，因为它会额外构造第二份计划；最初带此选项的诊断运行已作废，不能用来选择缓存。

## 3. 构建隔离与计量口径

- 只有显式 `viewmetadatabench` tag 才提供 `ConfigureBenchmark` 和计量状态；正常构建使用空状态/空方法，没有生产开关、后台线程或全局计数器。
- 配置只能在首次工作前设置；统计读取与已有 binder 使用同一 gate，忙时返回 Busy，不与活跃 binder 无锁读写。Close 后仅保留数字，不保留 compiler、事务或结果。
- 每次请求计量可独立关闭。开关组合验证相同结果/错误、依赖/provenance、结构工作量与释放；真实权限/历史/订阅另外复用既有 native 组，不将这些最小证明冒称 S5 的完整矩阵。
- `view-binds/op`：opt-in 请求 scope 内的根推导及未被 memo 替代的嵌套 View 绑定尝试；不包含外围 I_S 视图本身或其他普通 SQL 规划的 bindView。不是原 `CompletedRootBinds`，后者只计算完成的根推导。
- `relation-resolves/op`：请求 scoped resolver 的按名/ID/index/subscription 关系解析调用。数据库存在性检查另计；**不包含候选/授权 SQL 的所有目录扫描，也不等于存储 RPC/I/O 次数**。
- `current_compat` 不经过新的 request 计量，其计量零值是 **N/A 占位**，不是“零绑定/零内存”的证明。关闭新增计量时也不能把新计数器的零值解释为未发生操作。
- `memo-hits/op`：实际根/嵌套 memo 命中；逻辑展开预算仍按无 memo 子树收费，不能拿较小 bind 数解释为放宽预算。
- `charged-peak-B`：请求所借执行代已计费资源峰值；**不是整个 parser/binder 的峰值 heap 或 RSS**。S2 v2.1 撤销的逐分配 128 MiB 硬证明没有恢复。
- `B/op`、`allocs/op` 来自 Go benchmark 的进程级计数，包含嵌入服务并发后台分配和 harness 校验；不能直接当作精确的单请求 allocator 账户。正式内存归因须结合 profile 和背景控制。
- p50/p95 用 nearest-rank；不足 100 个样本不输出 p99。1 次 smoke 的分位数不构成 S1 样本量证明。吞吐来自所有 worker 完成的总时间，不用单请求延迟简单乘并发数。

## 4. 可选择的 workload

native workload 默认每个对象 8 列、零数据；内存微基准投影 1 列。合成 SQL 固化列名，不使用读取时重新展开的历史 star。

| 基准/子场景 | workload |
| --- | --- |
| `BenchmarkViewSchemaStatements` | 内存目录微基准：单根、同语句重复 8 次、32 层链、128 根共享 16 层链、1,000/10,000 个浅 View；每 op 新 request |
| `BenchmarkNativeViewMetadata/single` | 单对象 I_S，串行/并发 8 |
| `.../table_single` | 普通表定向 I_S；与 1,000 表目录分开比较，opt-in View 根/绑定为 0 |
| `.../selective10000` | 1 个目标 View + 10,000 个无关 View，验证后者不推导 |
| `.../tables1000` | 1,000 个普通表全库列枚举，View 根计数必须为 0 |
| `.../chain32`、`.../shared128_chain16` | 32 层根链；128 根共享同一条 16 层中间链，选择根对象集合 |
| `.../shallow1000`、`.../shallow10000` | 全库列枚举（含基表），浅 View 均直接投影同一源表 |
| `.../wide4096` | 单 View 输出 4,096 列；从一个基列显式重复投影，避免把引擎隐藏列误当用户输出边界 |
| `BenchmarkNativeViewMetadataShow` | 单 View SHOW COLUMNS，三种模式 |
| `BenchmarkNativeViewMetadataDDLFirstRead` | 源 DDL，扇出 0/1,000/10,000，交替 INT/BIGINT；独立报告 DDL 延迟与首读延迟，首读校验新类型 |
| `BenchmarkNativeViewPlanning` | 深度 1/32 的 SELECT LIMIT 0、CTAS、文本 PREPARE 的 S3 规划；不执行数据/DDL |

S3 不消费描述 memo 的合成扫描；因此不能期待所有复杂图、或执行计划构造都降为每个唯一依赖只绑定一次。微基准、native 规划和完整元数据执行分层报告。

## 5. 可复现运行方法

在独立测试工作区运行，先确认端口、磁盘、原生产物及资源配额；不停止其他人的服务、不删除其他工作区数据。使用该 revision 的 go.mod 工具链和仓库 wrapper。以下以当前 Go 1.27.1 为例：

```zsh
export GOTOOLCHAIN=go1.27.1 GOWORK=off GOMAXPROCS=8 GOGC=100 GOMEMLIMIT=off
mkdir -p CLAUDE_validation/tmp
export TMPDIR="$PWD/CLAUDE_validation/tmp"
export MO_VIEW_BENCH_OUTPUT="$PWD/CLAUDE_validation/CLAUDE_samples.jsonl"

# 最小合同测试，不做大规模 setup。
.agents/skills/mo-dev/scripts/mo-cgo-test -tags=viewmetadatabench \
  -count=1 -run '^TestViewSchemaBenchmark' ./pkg/sql/plan
.agents/skills/mo-dev/scripts/mo-cgo-test -tags=integration,viewmetadatabench \
  -count=1 -timeout=300s \
  -run '^(TestNativeViewMetadataBenchmarkModes|TestViewMetadataBenchStreamDrainsAfterConsumerError)$' ./pkg/frontend

# 请求内微基准；不是 rollout 的端到端证据。
.agents/skills/mo-dev/scripts/mo-cgo-test -tags=viewmetadatabench \
  -run '^$' -bench '^BenchmarkViewSchemaStatements$' -benchtime=30x -benchmem ./pkg/sql/plan

# 单对象 warm：至少 1000 样本；并发 1 与 8 分开选择。
MO_VIEW_BENCH_COUNTS=0 .agents/skills/mo-dev/scripts/mo-cgo-test \
  -tags=integration,viewmetadatabench -timeout=30m -run '^$' \
  -bench '^BenchmarkNativeViewMetadata$/^single$/.*/^concurrency=1$/' \
  -benchtime=1000x -benchmem ./pkg/frontend

# 批量/宽列至少 30 样本。先以 1x 检查 fixture 和完整结果，不能把 smoke 当验收。
.agents/skills/mo-dev/scripts/mo-cgo-test -tags=integration,viewmetadatabench \
  -timeout=60m -run '^$' \
  -bench '^BenchmarkNativeViewMetadata$/(^shallow1000$|^shallow10000$|^wide4096$)/.*/^concurrency=1$/' \
  -benchtime=30x -benchmem ./pkg/frontend
```

`MO_VIEW_BENCH_COUNTS=0` 关闭新增计量，用于正常延迟；默认开启用于绑定/解析归因，应分开运行并保留环境设置。JSONL 在计时外保存每次实际 benchmark invocation 的原始纳秒序列、名字、Go/OS/arch/GOMAXPROCS 与计数总和；Go 的校准 invocation 也保留，不把不同 N 混成一个分布。每个 invocation 最多 100,000 样本，显式拒绝超限，不做无界 reservoir。

fixture 清理按逆依赖顺序逐对象提交，再删除空库并验证 catalog 中库已消失。这样不把上万个 catalog 删除堆在单一事务中；不提高读请求 deadline，不重试失败。MO 不支持单条 `DROP VIEW` 删除多个 View。大规模 setup/teardown 仍可能耗时数分钟，`go test -timeout` 覆盖整个过程，不能与查询预算混为一谈。

同时保存：`git rev-parse HEAD`、全部本地差异及源码摘要、完整命令/exit code、CPU 型号与配额、内存限制、OS/SSD/拓扑/配置、native provenance、日志级别和背景负载。干净提交前的 HEAD 不足以唯一标识本地修改。S1 固定基准环境不可在看到结果之后放宽。

CPU/alloc profile 必须单独运行。设置 `MO_VIEW_BENCH_PROFILE_DIR="$PWD/CLAUDE_validation/profiles"` 时，harness 在 setup/warm-up 之后启动 CPU profile，在测量循环结束后停止，文件名以 `CLAUDE` 开头并带随机后缀，日志记录实际位置。不要同时传 `-cpuprofile`。同时导出的 heap 是进程快照，仍含服务背景及 setup，不是请求峰值。普通 Go `-cpuprofile`/`-memprofile` 则连 CPU 也包含 fixture；未经区分不得把全文件热点归为查询。正式峰值、候选目录扫描和 source-DDL 成本需要独立归因。不要用 `-race`/coverage 构建做性能验收。

### 5.1 历史 B0 复现

随稿提供 [B0 测试适配 patch](CLAUDE_20261024-view-metadata-s6-b0.patch)，内容与已经验证的历史工作区五个测试文件逐字节一致；不修改历史生产代码。先用 `gwt-add` 创建独立历史工作区，固定到 `d99187d7b3bfda8744088b3ae8cf16739b690aa0`，在该工作区执行：

```zsh
# S6_WORKTREE 指向包含本稿的工作区。
git apply "$S6_WORKTREE/docs/design/CLAUDE_20261024-view-metadata-s6-b0.patch"
export GOTOOLCHAIN=go1.26.4 GOWORK=off GOMAXPROCS=8 GOGC=100 GOMEMLIMIT=off
mkdir -p CLAUDE_validation/tmp
export TMPDIR="$PWD/CLAUDE_validation/tmp"
make -j2 cgo
.agents/skills/mo-dev/scripts/mo-cgo-test -tags=integration,viewmetadatabench \
  -count=1 -timeout=10m -run '^TestViewMetadataS6B0Adapter$' ./pkg/frontend
.agents/skills/mo-dev/scripts/mo-cgo-test -tags=integration,viewmetadatabench \
  -c -o CLAUDE_validation/CLAUDE_b0.test ./pkg/frontend
# 单独运行已编译产物，避免编译污染计时。
(cd pkg/frontend; MO_VIEW_BENCH_COUNTS=0 ../../CLAUDE_validation/CLAUDE_b0.test \
  -test.run '^$' -test.bench '^BenchmarkNativeViewMetadata$/^single$/^historical_b0$/^concurrency=1$/' \
  -test.benchtime=1000x -test.benchmem -test.timeout=30m)
```

合同测试验证实际部署的 COLUMNS 定义含 `mo_view_columns`、完整历史类型表示、普通表、并发独立语句、预取消、缺行反例、DDL 首读和清理。B0 只支持历史 I_S 消费者；SHOW/非历史模式明确拒绝，未回移 v2 计量 API，计数占位是 N/A。历史 `int(32)`/`bigint(64)`、Go 1.26.4 与当前 Go 1.27.1 均须披露。patch 的反向 `git apply --check` 和各文件 SHA256 与冻结版本一致；这些源码身份仅支持历史证据复用，不补齐缺失的性能矩阵。

## 6. 验证与剩余门禁

### 6.1 本地功能及诊断证据（2026-10-23）

冻结生产基线/head：`e19f698de0fc52a60f60f2ff689d6d16a5100334`，另有本次未提交 diff；使用 Go 1.27.1、本工作区重建且通过 provenance 检查的 native 产物。原始日志、JSONL、profile、源码/native SHA256 清单与逐命令 exit code 保存在本地 `CLAUDE_validation/`，不提交大量运行产物。

已取得终态 PASS 的证据：

| 范围 | 证据/结果 |
| --- | --- |
| 默认构建的 owning packages | `owning-normal.log`：`pkg/sql/plan`、`pkg/frontend` 全包通过 |
| tagged schema 功能/边界 | `plan-coverage.log`：全部 `TestViewSchema*` 通过；计量开关/禁用 memo 的列、依赖、provenance、错误分类、逻辑预算与释放等价 |
| 修改生产行的覆盖率 | 与新增行相交的 statement blocks 为 **36/48 = 75%**；新计量文件 21/24。不是整个 plan 包覆盖率（后者 16.3%）；空状态/字段声明没有可执行 statement |
| native 新旧消费者 | 模式 × 新计量开关 × SHOW/I_S、DDL 后类型、S3 三种规划、并发独立请求、预取消通过；`native-existing.log` 的真实历史/订阅/权限组也通过 |
| focused/owning race | 活跃 binder 与流式接收错误各单进程 `-race -count=100`；native 模式组首次 9.24 秒，按 30 秒预算跑 3 次；两个 owning 包均通过，最终 frontend 再跑全包 race 通过 |
| 释放反例 / 静态检查 | 流式 fixture 使用 off-heap payload；overlay 去掉 Close 时确定性发现 24 字节遗留，正式代码归零；最终增量 lint 0 issues、normal lint/vet/gofmt/diff-check 通过 |
| native 形状 smoke | single、普通表 1,000、链 32、共享链、浅 View 1,000/10,000、无关 View 10,000、宽 4,096，三种模式通过；10000 规模的成功终态见 `native-large-v2.log` |
| 其它 benchmark 入口 | 并发 8、SHOW、fanout=0 的交替 DDL/首读、深度 1/32 的 S3 SELECT/CTAS/PREPARE，以及全部内存形状通过；不是完整 fanout/样本矩阵 |
| profile 生命周期 | 关闭新增计量、单对象 100 次独立 profile 运行通过；CPU 只覆盖测量循环，heap 是进程快照 |

可用于核查结构的 smoke 事实（不是延迟预算结论）：

- 128 个根共享 16 层链：实际绑定 **2176 → 144**，scoped relation resolve **2304 → 272**，memo 命中 127；小 DAG UT 同时证明 memo 不改变逻辑预算。
- 10,000 个无关 View 旁读取一个目标：cache-off/memo 都只绑定目标 1 次；普通表枚举的 opt-in View 根计数为 0。
- 10,000 个浅 View 的全枚举：两种 opt-in 模式都是 10,000 次绑定、20,000 次 scoped resolve、零 memo 命中，逐行验证了全部 80,008 行。该形状没有共享的 View 子图，不能期待请求 memo 消除这些根绑定。
- 100 次单对象短 CPU profile（进程样本 2.59 秒，测量窗口 1.90 秒）中，`BaseOptimizer.Optimize` 累计约 40.15%，带 `viewSchemaDerivation.describe` 的栈约 1.16%。这只提示下一步区分整体 I_S 规划与描述成本；背景服务、短采样和非隔离环境使它不能成为缓存收益上限或正式 S7 结论。

最初带 `WithForceRebuildPlan` 的单对象采样不可复用；最初大规模运行因单事务 `DROP DATABASE` 清理超时而失败，也不算整轮 PASS。修正 fixture 后的大规模运行及清理后置条件均已通过。后续仅涉及计时外 profile、清理/失败主因保留的调整，不把此前 smoke 冒充最终版本正式性能采样。

未实施生产优化；没有默认激活、跨请求缓存、refresh worker 或持久化写入的功能改动。源码存在、编译成功、未退出的进程都不是 PASS。

### 6.2 第二阶段增量证据（仍为诊断）

`CLAUDE_validation/phase2/` 保留原始 JSONL、命令/退出码、profile 和源码摘要。当前代码仍冻结在 e19 + 本次 diff，没有因本轮数据修改生产热路径。

- 环境核查：宿主 Darwin/arm64、10 CPU/16 GiB；OrbStack daemon 实际内存 8,393,289,728 bytes（约 7.82 GiB），且有其它任务容器。两者都不是已批准的独立 8 CPU/16 GiB 环境；没有改共享 VM 配置或终止其它任务。
- 历史 B0：独立 worktree 冻结 `d99187d7b3bfda8744088b3ae8cf16739b690aa0`，Go 1.26.4、本地 `make -j2 cgo` 和该版本 `mo-cgo-test` 均成功。历史生产源码没有改动，仅增加本地 tagged 测试适配。既有 frontend 隔离上下文测试通过；`b0-runtime-v3.log` 验证已部署 COLUMNS 定义包含 `mo_view_columns`，普通表及 View 的完整 8 列在源 DDL 前后均正确，清理后 catalog 无该数据库。**这是 B0 按需能力/运行资格证明，不是 B0 性能测量。**
- B0 的物理宽度格式为 `int(32)` / `bigint(64)`，与当前样本的 `int` / `bigint` 不同。历史 `Test_BuiltIn_MoShowVisibleBinIntegerMetadata` 明确规定该格式。首次沿用当前版本 oracle 的失败保留于 `b0-runtime.log`；随后采用严格的历史 oracle，并增加真实 DDL 首读检查，没有改生产逻辑或放宽为任意字符串。最终比较需披露版本/格式/工具链差异，不能宣称字节级等价或把全部差异归因于 memo。
- 当前三种模式 × 并发 1/8，各 **1,000** 次单对象读取通过完整结果与清理校验；`current-warm-v2.jsonl` 保留全部时延。该 `single` 形状不包含 10,000 个无关 View，不能替代 `selective10000` 门禁。新增计量关闭，兼容路径零计数仍为 N/A。运行中有其它工作区构建，因此不从模式间比值推导因果收益。
- 链 32、共享 128/16、浅 View 1,000、宽 4,096，三种模式各 **30** 次、并发 1，通过完整结果及资源清理校验；见 `current-bulk.jsonl`。不以 30 个样本报告可靠 p99。

下表仅记录本轮原始 p95（毫秒），**不是预算通过/失败表**：

| 形状 / 样本数 | current_compat | cache_off | statement_memo |
| --- | ---: | ---: | ---: |
| single，并发 1 / 1000 | 125.106 | 24.217 | 20.987 |
| single，并发 8 / 1000 | 53.252 | 52.947 | 49.466 |
| chain32 / 30 | 35.222 | 41.385 | 36.186 |
| shared128_chain16 / 30 | 261.459 | 1031.580 | 137.172 |
| shallow1000 / 30 | 262.219 | 579.792 | 3587.567 |
| wide4096 / 30 | 100.960 | 189.767 | 319.252 |

浅 View 1,000 的 memo 一轮均值约 1.50 秒，不能直接定性为 memo 回归。独立的计量关闭 profile 复测各 10 次，cache-off/memo 均值分别约 **253.109/253.231 毫秒**，未复现原波动；该 profile 运行也不替代正常时延验收。CPU 样本集中出现 Darwin 调度唤醒栈（`pthread_cond_signal` 约 48%/53%），不能据此定义跨平台瓶颈或证明最初波动的根因。没有据此做生产优化或 S7 决策。

第一次直接启动 benchmark 二进制漏设本地 `TMPDIR`，系统临时目录转相对路径后过长，Unix socket bind 报 `invalid argument`，集群未就绪；仅对该自有进程发送 SIGQUIT，保存失败/堆栈。按已记录命令设置工作区临时目录后，`current-warm-v2.exit=0`。这是启动环境修正，不删除失败记录、不把失败轮算作样本。

磁盘一度只余约 4 GiB；确认无活动构建后执行 `go clean -cache`，恢复到约 46 GiB，再继续双工具链构建。保留模块缓存、native、源码、数据库验证证据及 worktree。

### 6.3 正式门禁仍待完成

尚未完成的正式范围：

1. 按 §1.1 已批准本机配置重新采样，以及历史 B0 同口径性能对照；不再要求另找独立 8 CPU 机器。B0 已通过构建及按需读取资格验证，但同 head 的 `current_compat` 仍不能替代 B0。
2. 完整 S1 样本量、独立服务重启后的 cold 首读（至少 5 次）、稳定/持续并发 DDL、授权稀疏与订阅规模 workload。
3. 全路径 CPU/分配及峰值 heap/mpool、候选目录扫描与 DDL refresh-only 锁/写的完整归因；S8 后才适用的 DDL 零 fan-out 要求不能提前通过。
4. 解析、目录 RPC、输出三个阶段的取消时延与长期引用证据；已有最小取消/cleanup 功能测试不代替这项性能证明。
5. 在上述证据基础上，先消除无关扫描、逐列重复绑定、重复规划与语句内重复，再发布 S7 三选一决定；缓存若必需，另审完整依赖验证/失效成本、内存界限与生命周期设计。

### 6.4 本机验收重启记录

- 已快进同步 `mo/main` 到 `be663676872edf6d42695bba41757af0a3251758`，保留本地 harness diff；历史 e19 证据不混入本次性能采样。上游与本地只有 query_builder.go 路径重叠，修改分别在 DDL temporal context 和 bind 计量入口，两者均保留。
- 冻结清单 `CLAUDE_validation/local-acceptance/CLAUDE_frozen_source.json`，SHA256 `ed7ecef913894b278ec00c1169f1e17e01f847b97d05ff420804fa116fd1e75e`。记录两版本源码/native、二进制、参数、runner、初始环境及完整 patch；后续仅文档变化不改变已冻性能输入。
- 新增普通表单对象形状和既有小 fixture 的零 View 绑定 oracle。同步后的 current 模式/规划/DDL/并发/取消及 stream 资源测试通过。
- B0 适配由共同 setup、私有 session/事务、stream、结果校验、worker/report 代码生成；只移除不存在的 v2 API、替换模式名和历史严格类型格式，B0 tracked production diff 为空。小规模控制覆盖已部署按需定义、普通表/View 全行、两个并发语句、预取消、缺行拒绝及源 DDL 首读，`CLAUDE_b0_contract_v2.exit=0`。初次额外定义查询漏加 deadline 的测试接线错误保留，已补有界 context，未改生产。
- 所有基准二进制预先通过各自 `mo-cgo-test` 编译。第一组预设三轮顺序为 B0→B1→B2、B1→B2→B0、B2→B0→B1；single/table_single 每个并发 1/8 单元各 1000 次，当前 compat 不混为 B0。
- 外来编译/MO 测试出现，或其它单进程 CPU≥100% 连续三次 2 秒观测，标记整个 invocation 受干扰；不据单次慢样本剔除。持续保留进程 CPU/RSS、vm_stat、电源/thermal/swap 信息。首次 B0 的 4000 次读取全部通过，但尾部捕获外来 Go/link 构建，整轮不进入正式比较；第二次预检捕获外来 clang/make，未启动。后续启动前最多等待 10 分钟，须观测到至少 10 秒无上述重负载，不自动重试失败请求，不终止外部任务。
- 主机有正常后台程序及其它独立服务，不冒称系统完全空闲或硬隔离。初始核验为电池供电，第一次采样前已为 AC 供电；每轮保留实际电源状态，不混淆配置记录与测量时状态。
- `warm-small-attempt3` 已取得完整第一轮：B0/B1/B2 × single/table_single × 并发 1/8 × 1000，共 12000 次完整结果/清理通过，监测未触发预设干扰阈值。第二轮 B1/B2 又完成 8000 次，但 B0 的 4000 次运行中捕获其它任务 make/MO 服务启动，整个 invocation 标为受干扰并暂停后续；不把未配齐的第二轮或受干扰 B0 拼入三轮比较。
- 三轮完整结果、最终资源归因和预算评估尚未完成；没有据此决定 S7 或声明 S6 完成。部分原始分布汇总在 `warm-small-attempt3/CLAUDE_partial_summary.json`，包含受干扰数据而非只留下最快结果。
- 最终增量 lint 使用 Go 1.27.1 构建的 golangci-lint 2.14.0，正确 CGo include/library 环境下 `CLAUDE_incremental_lint_v2.exit=0`、0 issues。首次调用遗漏 include 路径引发 usearch.h/typecheck 错误，失败记录保留，未修改生产代码掩盖；格式及 diff-check 通过。

### 6.5 2026-10-24 续跑：背景监测与分配归因

本节不追认旧诊断，也不更改 §1.1 的平台范围或数值预算。

- Go 源码、B0 适配、两份冻结二进制及 native 产物 SHA256 复核一致；B0 tracked diff 仍为空。未修改生产/Go 测试源文件，未 commit/push。
- 旧预检因常驻 MO 服务存在而永久拒绝启动，因此在新样本产生前预声明独立批次口径：允许事先冻结、存活至少 600 秒的低负载服务；30 秒预检中禁止编译/测试、新服务、单进程 CPU≥100%、冻结服务合计 CPU≥100% 或 RSS 变化≥256 MiB。运行中禁止新/替换/退出服务及 RSS 增长≥256 MiB；CPU 条件连续三个 2 秒观测标记整个 invocation。记录 PID、启动身份、命令、CPU/RSS、vm_stat、电源、thermal、swap；不是硬隔离，也不按延迟剔除样本。新背景配置重新三轮，不拼接旧第一轮。
- `local-20261024-stable1/2` 各等待 600 秒，均未形成预声明窗口，**正式测量请求均为 0**。291/293 次预检观测确实捕获其它任务的编译/测试、服务负载变化；第二次包括 256 MiB RSS 增长以及 MO/浏览器/BTLEServer 高 CPU。保留完整记录及失败退出，不降低阈值反复重试，不停止其它任务。
- 本地监测边界 6 项测试、汇总边界/污染传播 2 项测试及 Python 语法检查通过。汇总保留受干扰样本，不用受干扰 B0 计算相对预算，也不把 30 样本的尾部称作可靠 p99。

正式计时停止后，另用当前冻结二进制做 `single/c1`、counts=false、cache-off/memo 各 100 次独立 profile；200 次完整结果和资源清理通过。CPU 仅在测量循环采集，累计 `alloc_space` 用同进程“校准后 → 测量后”的 heap 快照作差以减小 fixture 影响：

| 当前模式 | 采样分配差值 | `BaseOptimizer.Optimize` 累积占比 | `viewSchemaDerivation.describe` 累积占比 | fixture 编译上下文包装器累积占比 |
|---|---:|---:|---:|---:|
| cache-off | 929.58 MiB | 66.86% | 1.18% | 0.27% |
| statement memo | 917.15 MiB | 68.26% | 1.25% | 0.44% |

- 累积调用栈占比可能重叠，不能相加。差值仍含进程背景、测量间隙和 GC/分配采样误差，不等于单请求精确分配，更不是峰值。对应 Go `B/op` 约 11.81/11.58 MB；不能扣掉某部分来宣称 2 MiB 预算通过。
- 原 e19 profile 的相同诊断中通用优化路径约 66.15%；在当前 head 两种模式上重现了相近方向。它反驳“约 11 MiB/op 主要来自 fixture 私有 Session”的猜测，但不证明某条生产优化可安全落地，也不证明 S7 必要/不必要。
- 下一步归因优先级：检查元数据 SQL 的通用计划构造、表达式复制和常量求值/表达式执行器分配，定位可复现的重复工作；保持权限、创建环境、事务/快照、错误分类和实际消费者边界。不得以绕开 SQL 消费者或增加跨请求缓存换取数字。
- 原始 profile、CPU/RSS 观测、命令、输入 SHA256、退出和解析结果位于 `CLAUDE_validation/local-acceptance/diagnostic-20261024-profile/`。这是**诊断证据**，不填补三轮、selective10000、冷重启、DDL、安全规模及峰值资源的验收空缺；S6 未完成、S7 未决定。

### 6.6 具体候选与第三次稳定背景尝试（方案批准前）

- `local-20261024-stable3` 保持同一规则：初始预检约 218 秒后通过，但首个 B0 invocation 又等待 601 秒仍未形成窗口，退出 1、正式请求 0。后一段 291 次观测中，BTLEServer 高 CPU 85 次，另有 MO/浏览器/Phone 负载。没有启动测量，不能算作性能通过或预算失败；失败、背景身份及最终源码核验均保留。
- 现有 profile 的离线逐行解析定位到 `newOpBuiltInRegexp` 同时提前分配两个容量 100 的 map；cache-off/memo 构造成本占各自采样差值 4.05%/4.33%，其中两张 map 的行级权重合计约 37.17/39.18 MiB。调用栈覆盖执行器 Prepare 和 Reset，并非全部位于优化器。
- 最小候选仅将已有容器改为按后端需要分配，保持同一缓存作用域/容量/键所有权、错误及 reset/free 语义。ICU 已有 nil 初始化；RE2 必须在首次成功发布前补 nil guard。历史 B0 也有同样的提前分配，不能把它归为 S6/v2 新回归。
- 本地待审文档：`CLAUDE_S6_REGEXP_ALLOCATION_PROPOSAL_2026-10-24.md`；原始解析与命令位于 `diagnostic-20261024-detail/`，10 项退出 0。生产代码和 Go 测试尚未改变，方案等待用户确认。
- 此处只证明可调查的局部成本，实际节省须同版本前后验证；即使移除全部相关构造成本，也不足以弥合当前约 11 MiB/op 与 2 MiB 的差距。不启用全局向量池、不移除表达式所有权复制/诊断屏障、不增加跨请求缓存，不提前形成 S7 结论。

### 6.7 2026-10-24 获准实施后的同版本分配对照

用户随后授权持续实施至 draft PR。同步 `mo/main` 到 `3878a13ef00fc85b1bced8a58a4f7d17df848806` 后，在**同一基线**分别编译 before/after；前述 be663/e19 数据仅作发现证据，不用跨 upstream revision 的差异冒称优化收益。工具链均为 Go 1.27.1、匹配本工作区 CGo 产物；Sonic 在两边均提示回退到 encoding/json。数值预算、正式背景门禁和 S7/S8 状态不变。

实现仅有两处生产改动：

1. `newOpBuiltInRegexp` 返回空操作器，不提前分配 RE2/ICU 容器。
2. RE2 成功 compile/parse 后、首次发布缓存项前初始化 nil map；ICU 复用已有的惰性初始化。

缓存仍由单个表达式操作器拥有，容量/混合后端淘汰/ICU 字节上限/自有 pattern 键均保持；Reset 仍重建执行方法，Free 的既有 reuse 回调清空 executor。没有新增锁、goroutine、全局共享、跨请求缓存或生命周期延长。

`TestRegexpCacheLazyAllocation` 在未优化版本按预期失败；正式实现验证未使用/LIKE 快路不分配后端、RE2/ICU 独立分配、复用、真假/NULL 结果和非法 RE2 不发布状态。原键所有权反例只改为经生产入口建立缓存，保留全部碰撞探针与输出断言；colexec 新测试经实际函数绑定、Eval、Reset、不同输入重执行与 fresh 对照，并验证资源释放。

`BenchmarkRegexpOperatorCold` 每轮新建操作器，避免现有稳态 LIKE 基准排除构造成本；这里的 Cold **不是 S6 的服务冷重启**。每格 1000 次、重复三次，分配结果稳定：

| 场景 | before B/op | after B/op | before → after allocs/op |
|---|---:|---:|---:|
| LIKE 前缀快路 | 9,976 | 168 | 9 → 3 |
| 首次 RE2 | 17,610 | 12,753 | 50 → 48 |
| 首次 ICU | 26,072 | 21,216 | 173 → 171 |

另在真实 I_S 消费者中作独立 profile 对照：single/table_single × cache-off/memo × c1，每格前后各 100 次，共 **800 次完整结果和清理通过**。以下列出全部四格，而非只留下较快值：

| workload / 模式 | before B/op | after B/op | profile p95 before → after（ms） |
|---|---:|---:|---:|
| single / cache-off | 12,181,500 | 11,738,448 | 36.688 → 39.548 |
| single / memo | 11,957,034 | 11,526,128 | 21.218 → 35.279 |
| table_single / cache-off | 11,823,188 | 11,425,335 | 27.698 → 44.012 |
| table_single / memo | 11,824,691 | 11,368,336 | 29.812 → 26.609 |

进程级 B/op 观察到约 3.4–3.9% 的下降，但含嵌入服务背景，不当作精确请求账本。before 的四个 `alloc_space` 差值中，构造器分别占约 41.69/52.24/35.16/48.22 MiB；after 未再采样到该构造器来源，实际使用 RE2 时仍有首次 map 分配，不能声称操作器完全零分配。profile 的延迟有升有降，不用于延迟预算，也不声称“没有回退”；普通不带 profile 的独立三轮另行保留。

单个小目录 View 仍不替代 selective10000；总分配仍远高于 2 MiB 目标。本次仅消除已证实的局部分配，不用它宣布 S6 通过、S7 不必要或 S8 可激活。

### 6.8 普通三轮诊断与交付校验

在所有本任务构建/测试/race/lint 完成后，按预声明的 before→after、after→before、before→after 顺序，使用上述两份同版本二进制关闭 profile/race/coverage，固定 cache-off/c1、single/table_single 每格 1000 次，**12,000 次完整结果与资源清理通过**。监控和校准记录全部保留；没有正式稳定背景预检，因此不是历史 B0/cache-off/memo 的三轮验收。

| 轮次 / workload | before B/op | after B/op | p95 before → after（ms） |
|---|---:|---:|---:|
| 1 / single | 12,014,734 | 11,531,227 | 38.191 → 36.684 |
| 1 / table_single | 11,899,716 | 11,334,413 | 43.804 → 25.235 |
| 2 / single | 12,007,596 | 11,480,184 | 24.793 → 23.959 |
| 2 / table_single | 11,818,494 | 11,304,699 | 24.995 → 23.898 |
| 3 / single | 12,126,975 | 11,583,317 | 49.165 → 36.148 |
| 3 / table_single | 11,872,677 | 11,406,378 | 41.668 → 48.046 |

分配在六对中均下降，但第 3 轮普通表 p95 上升，且跨轮有明显波动；不汇池分位数、不择快轮、不宣称延迟无回退。该结果支持局部分配减少，不能替代正式延迟预算或证明达标。

交付前再次同步 `mo/main` 到 `d93d5d66c83d2927c233de17f21fe7f513bc7d32`，仅增加无关的窗口文档/BVT；Go/native/go.mod 和本任务 Go diff 未变，因此复用 3878 的二进制/验证身份，不伪造新 revision 测量。

- function、colexec、plan、frontend 普通 owning package 全部通过；tagged `TestViewSchema*` 通过。修改生产语句块覆盖率 **39/51 = 76.47%**，新增 regexp 三个 statement 全覆盖。
- 当前 native 模式/流式/历史/事务内 DDL/订阅/角色拒绝组通过。owning race：plan **153.206s**、frontend **36.540s**；binder/stream 按 30s 预算各单进程 100 次，native 首次 **23.10s** 后再 count=1，通过。
- normal/tagged vet、golangci-lint 2.14.0 增量检查均通过；gofmt/diff-check 通过。去掉 `strings.Clone` 的 overlay 使原有四个键所有权子例全部得到预期错误输出；没有削弱旧反例。
- [机器可读验证摘要](CLAUDE_20261024-view-metadata-s6-validation.json) 包含全部十二格的 p50/p95/p99/max、完整 Go metrics 行、源码/二进制/证据散列和终态。完整原始样本、失败、profile 与监控留在本地，未将含其他任务命令的进程日志发布。机器摘要不冒称上传了完整原始证据。

复现局部优化时，可用相同 harness 配合 `-overlay` 将 `func_builtin_regexp.go` 替换成上述 upstream 基线的原文件，分别通过 CGo wrapper 构建 before/after 二进制，再执行 `BenchmarkRegexpOperatorCold -benchtime=1000x -count=3 -benchmem`。原生普通对照使用 §5 的预编译运行方式、`single|table_single`、cache-off/c1、1000x，并固定上述三轮顺序；CPU/profile 分析另跑，不混入普通计时。

**未解决门禁仍为 §1/§6 所述完整三轮/矩阵、冷重启、DDL/安全规模、取消阶段、CPU/目录/heap/mpool 归因及逐预算判定。此稿以 draft 部分交付，#29441 保持开放。**

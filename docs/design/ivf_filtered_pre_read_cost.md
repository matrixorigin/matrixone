# #29531 — main filtered PRE 查询优化设计 v2

日期：2026-09-30。基线：`c2abd6a54b7cd3e13c1b1494388cd7a81b8369d4`。
工作树：`/mnt/nvme/issue29531-opt`；分支：`perf/29531-filtered-pre-read-cost`。
设计角色：按任务指定 `gpt-6-astra / xhigh`；实际调用身份以主代理的 spawn 记录为准。
状态：提交 GPT-6.1-sol / xhigh 独立评审。A 沿用已经获审的查询读取闭包；Q2 是新提案，
须有本版本的明确批准和下述判别实验证据才能实施/交付。

## 0. 用户硬约束与设计替代关系

只优化 **main 查询路径**。不改生产 write、flush、merge、writer policy、chunk encoding、
chunk target、对象格式或持久化内容。不重写数据，不要求重建索引。
v1 的 B/C 写入方案撤出本任务，不是本 PR 的依赖、后续阶段或验收条件。
不降低 candidate budget、probe、K、召回约束；不删除 `MustApply`、`ForceOneCN` 或 exact membership。
不添加 cache、worker、跨请求持久状态机或新的调度层；不进行不安全的局部 LZ4 解压或绕过只读 buffer 契约。

设计者只进行了源码/历史/已有证据读取；未启动服务、跑性能测试、修改生产代码或写 GitHub。
遵循 issue-to-pr、mo-dev 的 design-first/index-plugin/testing-contract/validation-evidence/
race-validation/counterexample-testing，生命周期使用 Q1–Q3 审计。

## 1. 可交付结论

建议本 PR 形成以下查询侧闭包：

* **Q1/A：filtered entries 按需读取策略贯穿整个 reader。** 修正数据预取早于过滤、
  context policy 丢失以及早期标量读取硬编码 policy=0 的断点。主要消除冷对象读取与填充放大；
  所有 fused / fallback 消费同一读取意图。
* **Q2：禁止 memory cache 写入时，保留既有 scoped 解码复用，并避免该选中列的无效缓存预留。**
  仅在已经显式 opt-in 的单列转换闭包中使用已有 uncached allocator，保留所有共享上界，
  禁止等待不可能发布的 memory fill。它降低特定 no-write 并发 miss 的重复解压/驱逐，
  不减小 legacy 单次解压字节。

Q1 可独立交付；Q2 需严格的真实 TopN、no-write、并发重叠试验。若 Q2 不能证明收益，
从实现移除并记录反例，不能用扩大缓存/并发度/改变数据布局凑出结果。

**没有证据证明这两项足以恢复 issue 的约 7 倍吞吐差距。** 31-block 原诊断不会触发
typed 单 reader 的 1024-block cache-write 阈值；因此不能把 Q2 当成该诊断的已证根因。
查询仅访问一个 legacy LZ4 extent 中的一行，缓存 miss 时也可能必须完整解压该 extent；
本设计不承诺突破这一物理限制。PR 使用非 closing issue 引用，完整 nightly 验收缺口写清。

## 2. 已证明事实、待验证假设

| 结论 | 证据与限制 |
|---|---|
| 原性能差距 | Issue #29531 的 main PRE filter 约 9 QPS，4.2 约 66；threshold 约 17 对 69。10M×768、100 并发、两 AP、每 CN 16 GiB cache。环境/路由/数据布局仍存在混杂。 |
| 主要成本归属 | 已有 warm profile 的 relationScanner/LoadColumnDataByTopN/DiskCache/LZ4 主导，distance kernel 很小；不是本次 c2 的同步 profile。 |
| 过滤有效但读取昂贵 | 诊断 31 selected blocks/30 read、245760 行到 275 行，logical extent 约 760 MB。extent 和 cache-hit 不能替代物理 I/O 或真实 decode 计数。 |
| 小数据复现 | 20k×128、20 命中、逻辑向量约 10 KiB、计入约 10.22 MiB extents；重启后约 163 ms，对 decoded memory 热态约 0.34 ms。无 4.2 对照，只有 3 blocks，不足以触发 local prefetch。 |
| Q1 源码缺口 | Local/RemoteDataSource 可先 PrefetchFile；readutil policy 从 0 开始，阈值直接赋 SkipMemoryCacheWrites；ReadDataByFilter 的两分支硬编码 0。 |
| Q2 源码缺口 | shared_decode.go:490 的 Any(SkipMemoryCache) 同时拒绝 SkipReads 与 SkipWrites；MemCache.Update 禁写时直接返回，但选中列 converter 仍可能经 MemCache.reserveCacheData 驱逐热缓存。 |
| Q2 适用范围 | typed BuildReaders 使用 readerNum=1，GetThresholdForReader(1)=1024；policy 在 readBlockCnt>threshold 后禁写。31-block 案例没有这一触发条件；上游显式 no-write policy 也可能触发。 |
| 已有机制可复用 | chunk-native TopN、fused exact/filter/INCLUDE、scoped read-only binding、bounded shared decode、local DOP 均已存在。新做一套不是合理闭包。 |
| 待测 | 实际 legacy/chunk 分布、真实 ToCacheData 次数和字节、cache admission/eviction、对象预取、CN 路由、主线与 4.2 相同逻辑数据下的热态差距。 |

## 3. main / 4.2 实际查询路径对照

4.2 SHA `1a44f3db3056e32975fde86ca0ab69a094996a3f` 使用内部 entries SQL，经 planner、compile、
scope 和 engine reader；main typed `relationScanner` 直接构造 ranges/readers，并在已封存的
generation/txn/snapshot/domain 下执行。两者均已有 ObjectIO vector TopN，不能从入口差异
直接推断 4.2 没有整列解压。

两版的小 exact PK 集合均有跳过 centroid 限定、覆盖完整 exact domain 的行为。
main 为避免 underfill 使用 allCentroids；不能删除它或把 main 内部 budget 改为 4.2 所见
外部 limit。测试分别记录 candidate budget、probe、selected blocks、scores 和返回结果。

typed 路径当前没有 SetOrderBy。**不能把 distance Func 机械传入该接口以关闭 prefetch**：
LocalDataSource.getBlockZMs 只支持 ColRef，会报 invalid ORDER BY column。独立 request policy
才是合法 owner 边界。不要恢复内部 SQL 或放开 required membership 的跨 CN 路由。

主线 storageTopK、filteredStorageTopK、PostFilterTopOnly 三种资格已经有语义保护。
lower distance bound、不安全的 affine QuantMul upper-bound 转换、DESC、非 plain-column
函数/cast、类型不匹配等 fallback 不能通过放宽资格换性能。BatchTransform 在没有 storage
distance 时才计算距离，并清空宽 entry vector；不要加入二次计算或保留 embedding。

`compactRelationTop` 是稳定排序加 owned scalar copy，超过 2K 时压缩到 K，最终再压缩。
K=50、约 30 blocks 的这部分没有 profile 证据支配成本；当前不换 heap/合并算法或 tie 规则。
若同步 profile 显示其占比达到实质水平，再单独修订设计，而不把所有查询层重写混入本 PR。

4.2 不支持 main 的 chunk 编码，跨版本测试用相同确定性逻辑数据、独立数据目录、顺序启动。
不把 main 数据目录交给 4.2，且将物理布局差异如实记录为对照限制。

## 4. Q1/A：读取策略的最小闭包

### 4.1 owner 与接口

`scanEntriesInDomain` 是过滤语义 owner，既有 `RelationScanRequest` 增加值字段
`ReadPolicy fileservice.Policy`。当 `sqlproc.IvfHasMembershipFilter || len(filters)>0` 时
设置 SkipFullFilePreloads；metadata/centroids/unfiltered entries 保持零。
不能从 PostFilterTopOnly 反推有过滤，它也用于 unfiltered 的 unsupported/DESC fallback。

`relationScanner.ScanRelation` 在本次调用的局部 ctx 上 OR
`GetFileServicePolicy(ctx) | req.ReadPolicy`；不改 proc.Ctx，不将 policy 留在 reader/session。
沿现有函数参数传递 context，不新增 engine setter/interface。

### 4.2 消费端闭包

1. readutil.reader.read 从当前 ctx policy 初始化，原阈值只 OR SkipMemoryCacheWrites。
   上游 SkipCacheReads/SkipDiskCacheWrites 等必须保留，不改变阈值计数/边界。
2. blockio.ReadDataByFilter 增加明确 policy 参数；唯一生产调用点传同一 policy。
   LoadColumnDataBySearch 与 readBlockData 两个早期 scalar 分支都取消 hardcode 0。
3. Local/RemoteDataSource.Next 依据当前 ctx flag 跳过 data object PrefetchFile；
   tombstone 预取及可见性路径继续原行为。局部判断，不新增持久开关。
4. mergeReader 的 Read/ReadWithFilter/ReadWithFilterAndTopK 已逐级传同一 ctx；
   不改生产结构，用两 child 的测试覆盖切换和 capability fallback。
5. 单列 TopN、fused exact、fused INCLUDE、late materialization、legacy fallback 都接收
   调用者策略；不把通用 ObjectIO 默认策略改成 range-only。保留 legacy-default 测试。

Object metadata 已有自己的策略。range disk cache 能消费旧 full-object 文件，不清空 cache。
memory key 维持 path/offset/size；共享 key 的 policy/codec/size 不删减。Q1 不降低
legacy 必须解压的 extent 大小，也不保证减少 warm-memory 时间。

## 5. Q2：no-write scoped decode 的查询内闭包

### 5.1 为什么现有机制尚不够

SkipMemoryCacheWrites 的目标是阻止持久准入；现有共享 registry 是 IO-scoped immutable
结果复用，release 后即退出，二者不必绑定。当前 Any(SkipMemoryCache) 把这两种情况合并，
导致重叠读取反复转换。另一方面，禁止写入仍通过 MemCache reserve/evict，再由 Update
拒绝准入，选中宽列可能无收益地驱逐其他热列。

### 5.2 最小实现，禁止扩大为通用 allocator 改造

只改 `S3FS.prepareSharedDecodeInternal` 已存在的 opt-in 单列闭包：

1. 拒绝条件从 Any(SkipMemoryCache) 收窄为 SkipMemoryCacheReads。所有原资格检查保留：
   一个 descriptor、合法已知 size、无 custom cache、无 stream/writer-for-read、无既有结果等。
2. no-write 请求明确不调用 acquireFill，不等待 memory publication；
   `coordinateFill && !SkipMemoryCacheWrites && !SkipDiskCacheReads && ...` 才协调 fill。
   普通 decode registry 仍复用，不增加 wait budget、容量或 participant 上限。
3. 在 selected entry 的 ToCacheData 闭包内，若本次 IOVector 有 SkipMemoryCacheWrites，
   将传入 allocator 换成 **只读调用持有的轻量 allocator view**。该 view 实现现有
   CacheDataAllocator 的四方法（BackingSize、Allocate、带 hint 的 Allocate、Copy），
   BackingSize 转发到实际 uncached allocator；复用 `MemCache.uncachedAllocator`，并与现有 reserve-failure
   分支一致给 Bytes 标记 `cacheAdmissionOwner = m.allocator.owner`。不创建 allocator pool。
   普通有准入请求继续原 allocator。no-write decoded bytes 既不 reserve 也不 evict。
   allocator 的局部选择先于 len(data) 与 key.size 不符的原转换 fallback，避免该分支重新预留。
4. view 只由上述读取闭包构造；不改 MemCache.AllocateCacheData 默认行为，不依据全局/ambient
   ctx 改 allocator，不改 S3FS/LocalFS Write 或 writer 任何调用点。可用一个包私有 helper
   复用已有 transient 分配几行逻辑，不能把 marker 清掉以帮助准入。
5. 计算共享 charge 仍为既有两种 allocator 的最大 backing size；返回数据由完整 IOVector
   持有，复用现有 decodeLease、Retain/Release、beginRead/endRead、Close drain。
   registry 拒绝 admission/超时/超容量时走原转换，只是 no-write selected column 的
   原转换使用上述 transient view；错误前后不新增重试。
6. 不处理尚未 opt-in 的 scalar read、RemoteCache response copy 或 custom cache；
   它们不是本闭包的承诺。单独给所有 FS allocator 加 policy 是范围扩张且可能影响写入，禁止。

Q2 同样支持已有 Read/DecodeFromBytes 的 opted-in selected column，无需改 ObjectIO decoder。
将已有 eligibility 测试的 no-write negative case 改为有内容的 positive contract 测试，
SkipMemoryCacheReads/SkipAllCache 继续拒绝共享。

### 5.3 所有权与资源

复用现有 registry 上限：min(effective memory cache,64 MiB) bytes、64 entries、128 participants，
原 follower timeout 和 close/cancel 协议不动。对象大于共享预算时不共享；不能放大预算掩盖效果。
8 MiB cache 配 25 MiB legacy column 时 Q2 不会把该列变成可共享，必须报告这一反例。
用于证明机制的 fixture decoded 列需小于该预算，且有真实重叠和 no-write policy。

leader 将 transient decoded data交给既有 registry/IOVector 引用；follower 拿各自 lease；
最后一个 owner 释放 backing。no-write path 不进入 cache FIFO，不持有 cache reservation。
取消只结束自己的等待，不取消仍有用户的 leader；Close 沿已有 active-read/lease drain；
转换错误不发布半成品。Q1 不增加 goroutine，Q2 不增加队列或 registry key 维度。

## 6. 最小生产文件与风险

| 文件 | 所属职责与改动 |
|---|---|
| pkg/vectorindex/ivfflat/relation_search.go | 过滤语义选择 request policy。 |
| pkg/vectorindex/sqlexec/relation_scan.go | request 新增只读 value 字段。 |
| pkg/vectorindex/ivfflat/plan_reader.go | 单 scan 局部 ctx 合并。 |
| pkg/vm/engine/readutil/reader.go | 保留 ctx flags 并 OR no-write threshold。 |
| pkg/vm/engine/readutil/datasource.go | remote data prefetch 服从请求策略。 |
| pkg/vm/engine/disttae/local_disttae_datasource.go | local data prefetch 服从策略。 |
| pkg/vm/engine/tae/blockio/read.go | early-filter 两分支接收 policy。 |
| pkg/fileservice/shared_decode.go | Q2 eligibility、禁 fill wait、读闭包 transient allocator 选择。 |
| pkg/fileservice/mem_cache.go | Q2 包私有 transient allocator view/helper；默认分配方法不变。 |

对应现有 *_test.go、必要 benchmark、设计文档和已有 vector BVT 增补。不需要改 writer、
compress、ObjectIO 编码文件、planner 路由或 engine 接口。R3：跨查询读取 owner、热路径、
Q2 共享生命周期契约扩大，必须新版本独立批准。mergeReader 只加证据，不为凑文件改生产。

## 7. 判别实验与验收

所有小规模试验在独立服务/数据目录进行，版本顺序跑；不碰机器既有 MO 服务。
CPU 限制 2，query concurrency 1/2/4，按需上到 8；不默认 100 并发，不跑 10M。
40k×128 raw 约 20 MiB，memory cache 8 MiB 与足够容纳的 64 MiB 两档；
>=4 ranges，至少两个 object，必须以实际元数据确认，不能仅按行数假设。

| 实验 | 设置 / 观测 | 判别与门禁 |
|---|---|---|
| E1：Q1冷读 | >=4 ranges；零/单/少量分散/密集命中；cold object+empty disk/memory | 计实际 backing read bytes/requests、PrefetchFile 次数和向量 converter bytes。Q1应去掉 filtered data whole prefetch；零命中不得读向量。不要用 logical extents 替代。 |
| E2：三种热态 | cold；warm compressed disk but empty decoded memory；warm decoded memory | 分别记 p50/p95、CPU、disk bytes、actual decode calls/bytes、RSS/allocations。Q1不应被包装成减少必要 warm legacy decode。 |
| E3：Q2原机制反例 | <=8 MiB decoded预算内的真实legacy TopN列，显式SkipWrites，2/4重叠请求；用barrier留住IOVector，不靠sleep | 改前每请求convert；改后每个同key重叠组1次，memory准入0、hot sentinel未被驱逐、reservedBytes无增长、release后registry归零。普通独立不重叠组应重新convert。 |
| E4：Q2资源边界 | decoder结果>预算；独立keys；SkipReads；cache pinned；取消/Close/错误 | 保守fallback、无新增等待、无死锁/泄漏；不能声称这些情形解压次数下降。 |
| E5：相同逻辑数据4.2/main | 顺序独立目录，固定 seed/query/probe/K/cache、相同FP vector；cold/disk/memory三态 | 必须记录实际路由、budget、block/domain、格式；归因只到已测机制。主线 patched/unpatched 同目录同对象另作严格A/B。 |
| E6：fallback成本 | 下界range、QuantMul≠1、DESC、cast/function/type不合资格、INCLUDE residual | 与精确基线结果一致；捕获真正fallback并记录距离转换次数，不通过放开storage资格提速。 |

Q1验收：在可触发fixture上不必要full preload消失；selected domain、budget、score、结果完全
相同；默认非过滤预取未改变；dense fixture记录请求碎片化可能增加的成本。
Q2验收：E3有真实转换和缓存保留的机制收益，E4全通过；普通 policy=0 无行为/性能实质退化。
性能采样先预热、固定重复次数、报告方差；短bench做至少前后各5组，不用一个延迟样本下结论。
最终独立 QA 记录通过、失败、未覆盖三类；完整 10M nightly 留给有资源环境，不能假定通过。

## 8. UT / race / BVT / 独立QA矩阵

* Q1请求：membership、scalar residual、二者并存、unfiltered、unfiltered DESC；metadata/centroids
  不带策略；先filtered后ordinary同session、ctx原flags OR、readBlockCnt阈值两侧。
* Q1消费者：local >=4ranges / remote data与tombstone分别计数；两个merge children切换；
  fused exact、fused INCLUDE、ReadDataByFilter search/general分支、late-materialized、appendable/inmem。
* Q2必须用真实S3FS+disk cache+ObjectIO legacy TopN测试，不仅mock或converter单测；
  另覆盖DecodeFromBytes的chunk opt-in、policy key隔离、whole-cache hit、同range不同codec、
  mismatched size、skipreads/customcache/multimarked拒绝、converter error、cancel、Close和重复release边界。
* 精确语义：required integer/noninteger/composite membership，false-positive陷阱，空domain，
  domain<K，重复key，跨block ties，null，f32/f64/narrow/quantized，cast/function/query type mismatch，
  residual INCLUDE，distance lower/upper/both/empty bounds，ASC/DESC，snapshot更新/删除可见性。
* read-only buffer：持有完整IOVector期间borrow；外逸结果仅owned PK/距离/include；
  不把validatedVectorCacheData.Bytes()改成借用；unsorted selection与legacy原fallback保留。
* race聚焦新增共享路径的barrier并发、不同key、cancel/close，低GOMAXPROCS和-p1；
  UT先涉及包定向，只有具体失败才扩大。遵守本仓库native CGo构建要求。
* BVT使用已有IVF PRE/exact membership/INCLUDE场景，结果和执行形状都核实。
  独立QA由6.1角色执行，设计者不得把自己推断写成QA通过。

## 9. 回滚与交付

Q1与Q2分别提交便于revert；Q2失败可完整退回原eligibility/allocator路径。
无on-disk或catalog迁移，正常重启或代码回退即可；range/full disk cache互操作沿用既有实现。
交付附设计批准版本、准确git SHA、changed-file map、命令/日志/实测表、机制边界和未跑nightly项。
不宣称改变写入性能，不宣称少量本地试验已解决全量7倍差距，不使用closing引用。

## 独立评审决定

GPT-6.1-sol / xhigh 批准上述 v2 原文 SHA256 `c6c95bca15aacc79baeef7a87deaf1a5dba5b44a4095013a447acbf7f5d43d56` 的 Q1。Q2 不获生产实施批准：尚无真实 TopN E3 收益证据，且独立 fallback decoded allocations 的总体峰值约束不充分。本次仅实施 Q1；Q2 留作未获批准的设计分析，不属于交付承诺。写入路径不修改。

# 4.1.6-hotfix：有界 NULL-aware membership 索引

关联 issue：#32263。版本：v5 草案，2026-11-06。实施 PR：[32315](https://github.com/matrixorigin/matrixone/pull/32315)。

**状态：DRAFT / 待技术评审与批准。** 当前实现冻结于 `264f8842007f570c641f0122d77a2c33f635db85`，仍是 v4 行为；本文 v5 的渐进成本准入、类型范围收缩和统计补齐尚未实现。不得把已有实现/测试或工作计划授权当作对本版本的设计批准。

v2 明确独立 pipeline 的取消隔离：builder 取消只禁用共享优化，不取消仍有效的兄弟消费者。
v3 闭合原生物理分配失败：MPool 在 calloc 返回错误时回滚已准入的 pool/global 配额并返回原错，确保可选索引仍发布终态。
v4 将索引实现及测试放到实际所有者 `pkg/vm/message`，不再给底层 `common/hashmap` 添加 batch 依赖；CI 必须验证完整测试依赖图及 batch 自包测试。
v5 根据 [成本准入意见](https://github.com/matrixorigin/matrixone/pull/32315#discussion_r4490654617) 和 [统计意见](https://github.com/matrixorigin/matrixone/pull/32315#discussion_r4490654627)，提出先 evaluator、按真实工作量触发共享索引，并把实际发布的 native 容量统计一次。

## 评审门禁与版本记录

- 整体范围包含 JoinMap 共享状态/等待/生命周期、执行热点容量，以及 MPool 的 native 分配失败返回与配额补偿；存在明确复杂度触发，不以 bug fix 名称豁免。
- 首个提交 `936c5ce664` 同时引入设计与实现。当前没有可引用的、针对确切版本的独立技术批准记录。早先对工作计划的聊天授权不应被重新描述成完整设计技术批准。
- 本次先提交仅含设计/状态说明的独立修订；批准前不继续实现 v5，不把两条行内线程标为已解决。
- 批准记录必须包含：被评审的 commit permalink、reviewer、技术结论/已接受取舍及可引用记录。当前均为待补，不补造历史。
- 技术评审应覆盖本文完整方案，而不仅是两个局部修复；批准后实现必须对照该确切版本，实质偏离需再评审。
- 现有正常、race、BVT 和性能证据只证明其对应旧实现，不能作为 v5 的通过证明。

## 设计范围与评审结论

本设计落实共享索引、成本/容量门禁和统计，风险为 R3：共享生命周期、取消、热点成本、全局 native allocator 的失败边界。维护边界为发布分支的执行层优化，不扩展 planner 非空推导。

拟选方案：保留 LoopJoin、HashBuild 和 wire；先执行 evaluator，只有已发生的有效比较工作达到成本阈值、且仍有未处理 probe 行时，才在已有 JoinMap 上创建一次只读有界索引。v5 第一阶段仅加速直接列引用、同型的整数/Boolean 单列等值 MARK；变长字符串继续 evaluator，不以未经证明的类型 Width 估算其 hashing 成本。不扩展一般表达式、复合键、shuffle 或 spill。

主要取舍：按 RHS 行数而非 distinct 数预留槽容量，大量重复键仍可能提前预算回退；v5 不保留缺少成本上界证明的变长键加速。所有未优化 SQL 保留 evaluator 语义。固定宽度域使用保守的工作量门槛，而不是维护键频率采样、时间预测器、额外缓存或 per-worker 索引。

## 问题与可观察契约

本分支 MARK 使用 LoopJoin，逐 probe 行执行完整 RHS predicate，最坏 O(P×B)。nullable UNION 元数据修正后，NOT IN 正确选择该路径，却产生明显性能损失。#32085 的 planner 修正仍是独立依赖，不能通过恢复错误非空标注来避开 MARK。

单键 SQL 三值逻辑：空 RHS → FALSE；精确命中 → TRUE；未命中且 probe 为 NULL 或 RHS 含 NULL → UNKNOWN；否则 FALSE。NOT IN 由既有上层 NOT 处理。每个 probe 行恰好一条输出，重复 RHS 不增加输出。

公开结果由真实 SQL 验证；内部路径和共享次数由 typed UT 验证。正确但慢的旧实现不应被描述为结果错误。

reviewer 在 v4 上报告：2M 条相同 RHS、1 个早命中 probe 的 NOT IN 中位数 2.083→74.808 ms；256 个早命中 probe 的 IN 为 7.655→74.195 ms，且一个 distinct key 占用32 MiB 索引。这些是 reviewer 提供的对照，本文不冒充作者本轮已复现。源码可直接证明 v4 在第一行 probe 之前全扫 RHS；O(B+P) 不是普遍有利的成本证明。

另一个明确问题：HashBuild 在发布 JoinMap 前已执行 Alloc，而 index 在 LoopJoin 后建；只有 JoinMap.Size 不能使新增内存进入算子统计。

## 候选比较

1. **最小回移 #25822**：参考其 marker 语义和测试，但初始实现携带 TrackNullKeys wire / spill 及通用 key evaluator；不包含本 issue 要求的完整 scratch 准入。裁剪和重新证明之后，维护成本仍高于本地受限路径。不导入主线后续大范围 allocation-account 重构。
2. **渐进准入的共享借用键 hash（拟选）**：先支付可观察的 evaluator 成本，充分摊销后才构建一个原生槽数组；现有 JoinMap 保持唯一索引所有者。不要继续采用 v4 的无条件全量建表。
3. **排序行引用+二分**：容量同样可证明，但需要可取消排序，O(B log B+P log B)，额外排序 scratch 或手写排序状态。未选择。
4. **仅按 probe 批大小/总基数决策**：不能区分早命中与 miss，也不能预知后续批；单靠大 probe 仍可能误判，拒绝。
5. **按墙钟计时预测或维护额外 distinct/前缀采样索引**：会引入时序依赖、额外状态、碰撞/复制/计费面；当前没有必要。采用已有 evaluator 工作观测的 rent-before-buy 思路，不声称硬件无关的绝对时延竞争比。
6. 每 worker 全量索引、无界 Go map、只读 headroom 的估算准入均不接受。

## 安全域与求值顺序

- 顶层 predicate 必须为内建 `=`；两个参数均为直接 `ColRef`，分别来自 relation 0 / 1；交换参数次序等价。
- 编译类型 OID、scale、width 必须相同；实际向量类型也必须匹配。
- v5 优化域仅为全部有符号/无符号整数和 bool，单键最多8字节。浮点、decimal、JSON、数组、特殊内部类型，以及 char/varchar/text/blob/binary/varbinary/datalink 均留 evaluator。
- 收缩变长域是明确取舍：按“比较行数”触发不足以摊销很长字符串的整表 hashing，而预先精确扫描其长度又会重现小 probe 全扫问题。仅看类型 Width、area capacity 或 determinism 不能提供实际 hashing 成本证明。未来恢复该优化须有独立的键字节成本/采样预算设计及性能证据，不把死代码和仅由测试使用的变长索引能力留在 v5。
- 原有字符串 SQL 结果/BVT 仍保留，作为 evaluator 兼容控制；不宣称该域有性能提升。
- 不处理函数、cast、常量 predicate、复合条件、残余条件、mixed-side expression；既不依据 NotNullable 推断安全，也不执行新的表达式。
- 检查实际向量上的 GROUPING metadata，有 GROUPING 时回退。
- 原始字节仅用于已证明等价的同型整数/Boolean 域；整数的字节顺序只影响 hash，不影响同型相等。
- 空 probe 不建立索引。空 build 保留既有 emptyProbe 行为。

## 渐进成本准入与中途切换（v5 待实现）

### 成本定义与边界

- `B` 复用 HashBuild 在 SendJoinMap 前发布的 `GetRowCount()`：它来自实际 `InputBatchRowCount`，不是 planner 基数估计。正常 MARK build 不去重、不 spill，发布后数据/计数不可变；无需在第一个 probe 前扫描 batch 元数据或键值。若 B 缺失、非正或超出既有容量界限，直接保留 evaluator，不为了优化重扫数据推导基数。
- 真正达到成本门槛后，构造器仍校验实际 batches 的类型、row count 与已发布 B 一致，并执行原生容量准入；不一致时禁用优化而不是按不可信的较小 B 建表。手工构造 JoinMap 的内部测试要明确提供实际行数，不能靠调小统计值诱发快路径。
- `E` 为该 JoinMap、该精确 key descriptor 上已完成的 evaluator 行工作。每个成功的、非 const predicate result vector 记入其实际覆盖的 RHS 行数；TRUE 早停后不计未访问的 batch。const result 不按逻辑 Length 虚记全批工作，避免 O(1) 的常量/NULL 路径诱发 O(B) 建表。
- 固定宽度键的 hash/clear/insert 都按 B 有界；拟定保守阈值 `T = 32 × B`。K=32 是待评审的明确初值，不是 issue-specific 数据形状判断，也不声称适用于任意变长数据或所有硬件。
- E 只在短锁内按同 key descriptor 聚合，饱和于 T；不创建无界计数、全局缓存或按键统计。乘加先做溢出检查。每消费者最多持有一个未提交的 O(1) 工作增量。
- 只有 E≥T 且调用者确有尚未处理的 probe 行、当前输出批仍有空间时才选举 builder。第一个 probe 无历史成本，必走 evaluator；查询最后一行之后不为“可能出现的未来输入”建表。
- 已发布 index 可被任何合格消费者直接使用，不要求每个 worker 重复积累 T。

### 选择、续传与接口

- 拟将内部 API 扩为 `GetMembershipIndex(ctx, col, typ, completedWork) (index, newlyPublishedBytes, error)`。completedWork 仅来自已完成的安全 evaluator，不接受 planner 的非空/基数估计替代。
- API 在 observing 状态返回 nil/0/nil 并保留观测资格，不把“成本尚不足”当作本代永久禁用。不同 descriptor 不提交工作，直接回退；构建中的同 descriptor 消费者仍走 ready/context 可取消等待。
- LoopJoin 在处理下一条 probe 之前交入已完成工作的增量。一次 Call 内可以先 evaluator 后 index；必须保留已追加的前缀，索引只处理当前未消费行，不 reset 结果、不重放已输出行。
- 切换后的新增行数上限为 `DefaultBatchSize - prefixRows`，结果行数为 prefixRows+新增行数；probeIdx 始终是下一条未消费行。批满时先交付现有结果，下一次 Call 才继续；不因为到达成本阈值而突破批大小。
- 尚未提交的工作可跨 Call 保存，Reset/Free 必须清零，不跨 JoinMap 或执行代际。最后一行的工作不必为了全局计数而触发一次无收益的构建。
- evaluator 错误/取消立即返回，不将其改写成成本观测成功；OOM/预算失败后的永久 evaluator 回退仍不重试 index。

### 可证明的选择控制与性能证据

- reviewer 的1/256早命中 + 2M RHS，在正常 bounded RHS batches 下只访问前面少量批，E不足32B，因此不建32 MiB表；即使内部 fixture 只有一条巨大 RHS batch，单 probe 也不会在其最后一行结束后再建表。
- 大 probe/miss 在真实比较达到阈值后复用共享索引，保留最终期望 O(P+B) 的收益；承认先期会支付最多约 T 的行工作，并用原3k/6k以及多批 probe 对照量化。
- 同机交替运行 exact base / v4 / v5，固定资源、预热和测量次数，报告中位数、离散程度、建表次数、native bytes 与 Go allocations。低收益场景的确定性 oracle 是“没有建表/没有整表预扫描”，不是 UT/BVT 的毫秒阈值。
- 本轮尚未测量 v5。若32B使高 probe 收益丢失或仍出现明显退化，应更新设计/取舍并重新评审，不能把阈值调参藏在实现或弱化测试里。

## 数据布局与容量

索引置于 `pkg/vm/message/membership_index.go`，由持有 RHS 的 JoinMap 管理，只供此 membership 路径使用，不新增通用缓存框架。不能将 batch-aware 的实现放入 `common/hashmap`：batch 自包测试依赖 aggexec，而 aggexec 依赖 hashmap，会形成测试 import cycle。

- 一个 `[]uint64` 槽数组，每槽编码 `(batchIndex << 32) | (rowIndex + 1)`，0 表示空槽；无 Go 指针存入原生内存。
- 单个 `maphash.Seed`，直接对受支持固定宽度值的借用字节求 hash；碰撞时取 RHS 原值做完整 bytes.Equal，不以 hash 值当相等证明。
- 槽数为大于等于 `2 × RHS row count` 的最小 2 的幂，load factor 不超过 1/2。重复键共享槽；NULL 只记录布尔事实。
- 原生槽数组由 `MPool` 的 `offHeap=true` 分配；成功前已有原子 pool/global cap 检查。明确清零，不依赖原生分配器返回零内存。
- 单索引额外容量上限为 `min(64 MiB, MPool.Cap()/8)`；大小/行号溢出检查在分配前。超限返回“不适用”，不分配。
- **build/probe 没有随键长或行数增长的编码 scratch**：键是 RHS/probe 向量中的借用切片；不使用现有 StrHashMap iterator 的 Go key buffer，不预留 per-worker 完整表。变长输入不进入 v5 index。
- 索引的主体容量恰为 `8 × slots`；构建无第二张表和增长峰值。固定大小索引头/同步对象为每个现有 JoinMap 的 O(1) 开销，不伪称整个查询的 Go heap 已由 MPool 控制。
- 对已有 RHS storage 不重复计费。新增 native 表与其它 native 分配共同受既有 cap 约束；测试同时检查新增长期 Go key buffer 为零和 native 容量。
- 探测额外 scratch 为 O(1)；结果 batch 仍由原 LoopJoin 输出逻辑持有，其失败必须传播，不能误判为可选索引失败。

## 所有权、并发和状态

```text
HashBuild 收集 batches → GetJoinMap 转交 batches → 已有 refCnt 个消费者
                                               ├→ 消费者先 evaluator，聚合有效工作
                                               ├→ 达到阈值且仍有 probe 的消费者构建索引
                                               ├→ 其它消费者等待发布或自身取消
                                               └→ 最后消费者 Free → 索引 Free → batches Clean
MessageBoard 在所有 scopes 清理后 Destroy → 已清空对象上的无操作清理
```

JoinMap 内只保留一个 descriptor（build 列+完整类型）和一个发布结果：

- `未尝试`：首个合格 probe 在短锁内登记 descriptor，读取已发布实际 B 并计算 T，仅初始化成本状态，不扫描 RHS 或分配索引。
- `观测中`：同 descriptor 的已完成 evaluator 工作饱和累计到 E；E<T 返回 evaluator，不建立等待关系。E≥T 且存在待处理 probe 时，短锁内创建 ready channel，选出唯一同步 builder。容量/规模根本不支持时直接进入禁用态，不持续重算。
- `构建中`：不持有发布锁扫描或分配；其它同 descriptor 消费者 `select(ready, ctx.Done())`，不同 descriptor 直接回退，不能创建第二张表。
- `可用`：发布不可变索引，然后关闭 ready。消费者不再修改索引。
- `禁用`：资格、容量或可恢复分配失败；发布 nil 索引，永久保留本代 evaluator 路径。
- `失败`：无关错误发布 error 并关闭 ready；builder 取消则释放部分索引并发布禁用状态，只有该 builder 返回取消，未取消的等待者使用 evaluator。
- `清理`：只发生在调用方持有的 JoinMap 引用释放后；索引先于借用的 RHS 清理。JoinMap 不跨查询复用；Reset 新建 JoinMap。

builder 不另起 goroutine；其消费者已有引用保护 borrowed RHS。等待者取消不释放 builder 的资源、不修改构建状态。各 pipeline 的 context 可独立取消，因此 builder 取消不能污染仍有效的其它消费者：builder 返回取消错误，本代索引禁用且不重试，其它消费者依据自身 context 取消或回退。只持短锁修改状态，绝不在锁下运行构建、I/O、channel 等待。

当前分支 `Compile.Reset/clear` 先清理 scopes，后重置 message board；multi-CN 的 board Reset 返回新 board，不立即销毁其它 fragment 仍可访问的旧 board。遵循此已有生命周期，不支持在活跃消费者持有引用时强行调用 FreeMemory。

## 算子内存统计（v5 待实现）

- MPool 准入和算子观测是两件事。保持现有 cap/AllocatedBytes/物理 Free 契约；补齐 analyzer 不通过二次物理预留来“统计”。
- 唯一成功 builder 在发布不可变 index 后，从内部 API 得到 `newlyPublishedBytes = index.Size()`；等待者、已有 index 重用、成本不足、预算拒绝、OOM、取消或其它失败均得到0。没有独立的全局统计表或每消费者计费位。
- builder 所在 LoopJoin 在自身执行 goroutine 上，返回 probe/错误之前，立即调用自己的 `OpAnalyzer.Alloc(newlyPublishedBytes)`。不把非线程安全的 Analyzer 存入共享 JoinMap，也不从别的消费者线程写 builder 的统计。
- 这条统计只发生一次。`analyze_module.go` 已对同 plan node 的各 operator MemorySize 求和，因此多消费者统计之和只增加一份 index native bytes；RHS 原 storage 仍由 HashBuild 统计，不能再次计入新增 index bytes。
- 成功发布后紧接着取消或输出分配失败，索引仍是已发生的有效分配，必须保留统计；失败且未发布的临时 allocation 不记成保留 index。原生 OOM 的配额补偿规则不变。
- 本分支 Alloc 的 MemorySize 是本代累计/历史观测，不是 live bytes。Reset/Free 释放 MPool 内存并清理本地工作计数，但不调用负 Alloc 或提前清空 analyzer 抹去待收集的本代成本；下一代 Prepare 按现有协议调用 analyzer.Reset 清零统计，新 JoinMap 可重新构建并计费一次。
- 不扩项修复所有既有 LoopJoin 输出/表达式统计；此次必须确保新增共享 index 不低报、不重复。

## 错误、回退和取消

- 支持域不匹配、索引大小超限：返回 nil / 无错误；保留 RHS。
- 仅在 native 索引分配调用的边界识别 ErrOOM 和本分支明确的 mpool cap 错误；不能按全部 ErrInternal 降级，不能吞其它调用的错误。
- MPool 在配额准入成功后若物理 calloc 失败，必须补偿 pool/global reservation 并返回原始 error，不得 panic 或登记不存在的 buffer。OOM 经既有 nil-index 分支发布禁用终态并关闭 ready；非 OOM 经错误分支发布原错并关闭 ready。测试在 allocator 的 calloc 边界注入 NULL，区别于 cap 拒绝，不添加 recover 吞任意 panic。
- 确定性测试点仅在显式启用的 fault 测试域生效，并按 allocation size 过滤；默认禁用的成功路径只增加一次 fault 开关读取，不改变分配大小/所有权。测试必须撤销测试域，不能带入跨测试状态。
- 发生上下文取消时，builder 自身返回取消；释放未发布数组，向其它消费者发布禁用状态。等待者自身的取消仍优先于回退。
- 构建每个 bounded 行窗口、碰撞链检查 ctx；probe 每个 bounded 窗口及碰撞链检查 ctx；回退 evaluator 补上行 / RHS batch 间取消检查。
- 成功发布后查询只读；取消不会销毁别的消费者仍使用的索引。
- 在每条未消费 probe 行之前选择 evaluator/index。中途选择 index 必须保留已有结果前缀。该索引的 probe 无内存分配，因此没有“输出一半后 probe scratch OOM”这一分支；输出向量分配失败直接传播。
- 结果续传保持既有 probeIdx / DefaultBatchSize 契约；Reset 和直接 Free 都清空索引借用及 JoinMap 引用，避免沿用前代 NULL 事实。

## 兼容性与运维

不修改 plan、proto、持久格式、catalog 或外部协议；remote LoopJoin 根据现有 NonEqCond 在本 CN 选择路径。每 CN / 每现有 JoinMap 至多一张表，不宣称跨 CN 单表。

无需新开关或配置；不适用与内存压力自动回退。删除快路径即可恢复原 evaluator，不需要数据迁移。无新权限、后台任务、日志维度或租户共享全局索引。随机 hash seed 和精确碰撞比较保证正确性，容量上限限制额外内存；碰撞导致的长循环受取消约束。

## 验证与验收

- pure helper：空/全 NULL/命中+NULL/NULL probe/重复、常量向量、跨 batch、固定宽度边界、反例类型及 GROUPING。变长字符串/内嵌 NUL 保留公共 SQL/evaluator 控制，不以未使用的字符串 index 能力扩大实现。
- 容量：实际并发构建共用小 cap，barrier 保持 live 索引；验证容量限制与部分成功/降级、无编码缓冲；检查清理后恢复基线。
- shared owner：同 JoinMap 多消费者只构建一次，等待者取消、builder 取消、不同 descriptor 回退、Reset/Free。
- allocator failure：在 cap 足够时注入 calloc NULL；反复失败后 pool/global 计费与原有 live buffer 不变、可重新使用全部剩余额度。用 barrier 确认等待者到达 ready/context select，再使 builder 分配失败，验证 OOM 回退和非 OOM 错误发布；真实 HashBuild→LoopJoin 消费链验证 evaluator 结果及错误传播。
- operator：真实 HashBuild → LoopJoin → 输出链；DefaultBatchSize+1、evaluator 差分、错误顺序与非 OOM 传播。
- SQL：mo-tester 正常比对、NULL 真值表、天然 nullable 输入；另做 #32085 元数据修正后的隔离组合验证。
- 成本状态 UT：以2–3个 RHS 行、显式已完成工作覆盖 T-1/T/T+1；有未消费 probe 才构建，空/单 probe 结束不构建，const/NULL 结果不虚记行工作。多消费者只共享一个饱和预算与一次构建。复用现有 fixture，不新增集群或 sleep。
- 续传 UT：同一 Call 先 evaluator 再 index，前缀+尾部完整；批满、下一批、Reset/Free 后工作量/NULL事实/统计等价于新实例。保留 `DefaultBatchSize+1` 的最小边界数据。
- 统计 UT：真实 HashBuild→多个 LoopJoin，成功发布前/后检查各 OpAnalyzer.MemorySize，合计恰好一份 index.Size；重复 Call 不增加，fallback为0，发布后取消/输出错误仍保留历史统计，Reset/Free后native归零但不抹历史，下一代Prepare重置。不能只检查 JoinMap.Size。
- 公开统计：用真正触发 v5 建表的高工作量 SQL 做 EXPLAIN ANALYZE，与无 index 控制比较 join MemorySize；1/256早命中不建表时则不能虚报32 MiB。
- 性能矩阵：probe=1/256/3k/6k，RHS少量/2M，重复/唯一，早命中/miss，跨probe批；包括旧3k/6k高 probe控制。2M规模只在benchmark/独立SQL性能验证，不进入普通UT/BVT。报告IN和NOT IN结果及成本，不在UT/BVT断言耗时。
- 静态/包级：Go 1.26.4、匹配 native artifacts；变更包测试及消费者、vet/lint；共享状态定向/所属包 race；变更覆盖率 ≥75%。
- 依赖方向：`go list -deps -test ./...` 必须成功，batch 自包测试及 aggexec 消费者必须通过；不能只测新增 helper 所在包，以免漏掉反向测试依赖。

未完成证据不得标为通过。v5 批准后才实施本方案；本草案不抹掉旧实现两条未解决 review。既有 native allocator 错误返回/配额补偿纳入本次整体设计评审，但不继续扩展 allocator 框架；若需 wire、其它类型范围或新的全局资源机制，先更新范围并重新评审。

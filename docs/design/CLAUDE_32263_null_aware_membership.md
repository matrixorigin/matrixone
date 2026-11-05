# 4.1.6-hotfix：有界 NULL-aware membership 索引

关联 issue：#32263。版本：v2，2026-11-06。实施 PR：尚未创建。

v2 明确独立 pipeline 的取消隔离：builder 取消只禁用共享优化，不取消仍有效的兄弟消费者；其余设计保持 v1。

## 设计范围与评审结论

本设计落实共享索引和容量门禁，风险为 R3：共享生命周期、取消和热点内存。维护边界为发布分支的执行层优化，不扩展 planner 非空推导。

选定方案：保留 LoopJoin、HashBuild 和 wire；在已有 JoinMap 上惰性创建一个只读的、借用 RHS 键的有界哈希索引。只优化直接列引用的同型单列等值 MARK。下述所有权、错误和容量边界作为实施约束；不扩展到一般表达式、复合键、shuffle 或 spill。

主要取舍：按 RHS 行数而非 distinct 数预留表容量，大量重复键可能提前回退；放弃通用字符串 iterator 和主线的宽类型覆盖，以换取无需编码 scratch 的容量证明。

## 问题与可观察契约

本分支 MARK 使用 LoopJoin，逐 probe 行执行完整 RHS predicate，最坏 O(P×B)。nullable UNION 元数据修正后，NOT IN 正确选择该路径，却产生明显性能损失。#32085 的 planner 修正仍是独立依赖，不能通过恢复错误非空标注来避开 MARK。

单键 SQL 三值逻辑：空 RHS → FALSE；精确命中 → TRUE；未命中且 probe 为 NULL 或 RHS 含 NULL → UNKNOWN；否则 FALSE。NOT IN 由既有上层 NOT 处理。每个 probe 行恰好一条输出，重复 RHS 不增加输出。

公开结果由真实 SQL 验证；内部路径和共享次数由 typed UT 验证。正确但慢的旧实现不应被描述为结果错误。

## 候选比较

1. **最小回移 #25822**：参考其 marker 语义和测试，但初始实现携带 TrackNullKeys wire / spill 及通用 key evaluator；不包含本 issue 要求的完整 scratch 准入。裁剪和重新证明之后，维护成本仍高于本地受限路径。不导入主线后续大范围 allocation-account 重构。
2. **共享借用键 hash（选定）**：O(B+P) 期望工作；一个原生槽数组；字符串不编码、不复制；现有 JoinMap 已持有 RHS，适合承载借用关系。
3. **排序行引用+二分**：容量同样可证明，但需要可取消排序，O(B log B+P log B)，额外排序 scratch 或手写排序状态。未选择。
4. 每 worker 全量索引、无界 Go map、只读 headroom 的估算准入均不接受。

## 安全域与求值顺序

- 顶层 predicate 必须为内建 `=`；两个参数均为直接 `ColRef`，分别来自 relation 0 / 1；交换参数次序等价。
- 编译类型 OID、scale、width 必须相同；实际向量类型也必须匹配。
- 首版覆盖全部有符号/无符号整数、bool，以及本分支按完整字节相等比较的 char/varchar/text/blob/binary/varbinary/datalink。其它类型，包括浮点、decimal、JSON、数组和特殊内部类型，留 evaluator。
- 不处理函数、cast、常量 predicate、复合条件、残余条件、mixed-side expression；既不依据 NotNullable 推断安全，也不执行新的表达式。
- 检查实际向量上的 GROUPING metadata，有 GROUPING 时回退。
- 原始字节仅用于已证明等价的同型域；整数的字节顺序只影响 hash，不影响同型相等。字符串长度参与完整字节相等判定。
- 空 probe 不需要建立索引。空 build 保留既有 emptyProbe 行为。

## 数据布局与容量

索引置于 `pkg/common/hashmap`，只供此 membership 路径使用，不新增通用缓存框架。

- 一个 `[]uint64` 槽数组，每槽编码 `(batchIndex << 32) | (rowIndex + 1)`，0 表示空槽；无 Go 指针存入原生内存。
- 单个 `maphash.Seed`，直接对向量借用的 `[]byte` 求 hash；碰撞时取 RHS 原值做完整 bytes.Equal，不以 hash 值当相等证明。
- 槽数为大于等于 `2 × RHS row count` 的最小 2 的幂，load factor 不超过 1/2。重复键共享槽；NULL 只记录布尔事实。
- 原生槽数组由 `MPool` 的 `offHeap=true` 分配；成功前已有原子 pool/global cap 检查。明确清零，不依赖原生分配器返回零内存。
- 单索引额外容量上限为 `min(64 MiB, MPool.Cap()/8)`；大小/行号溢出检查在分配前。超限返回“不适用”，不分配。
- **build/probe 没有随键长或行数增长的编码 scratch**：键是 RHS/probe 向量中的借用切片；不使用现有 StrHashMap iterator 的 Go key buffer。不预留 per-worker 完整表，也不临时复制长字符串。
- 索引的主体容量恰为 `8 × slots`；构建无第二张表和增长峰值。固定大小索引头/同步对象为每个现有 JoinMap 的 O(1) 开销，不伪称整个查询的 Go heap 已由 MPool 控制。
- 对已有 RHS storage 不重复计费。新增 native 表与其它 native 分配共同受既有 cap 约束；测试同时检查新增长期 Go key buffer 为零和 native 容量。
- 探测额外 scratch 为 O(1)；结果 batch 仍由原 LoopJoin 输出逻辑持有，其失败必须传播，不能误判为可选索引失败。

## 所有权、并发和状态

```text
HashBuild 收集 batches → GetJoinMap 转交 batches → 已有 refCnt 个消费者
                                               ├→ 首个合格消费者构建索引
                                               ├→ 其它消费者等待发布或自身取消
                                               └→ 最后消费者 Free → 索引 Free → batches Clean
MessageBoard 在所有 scopes 清理后 Destroy → 已清空对象上的无操作清理
```

JoinMap 内只保留一个 descriptor（build 列+完整类型）和一个发布结果：

- `未尝试`：短锁内登记 descriptor、创建 ready channel，选出同步 builder。
- `构建中`：不持有发布锁扫描或分配；其它同 descriptor 消费者 `select(ready, ctx.Done())`，不同 descriptor 直接回退，不能创建第二张表。
- `可用`：发布不可变索引，然后关闭 ready。消费者不再修改索引。
- `禁用`：资格、容量或可恢复分配失败；发布 nil 索引，永久保留本代 evaluator 路径。
- `失败`：无关错误发布 error 并关闭 ready；builder 取消则释放部分索引并发布禁用状态，只有该 builder 返回取消，未取消的等待者使用 evaluator。
- `清理`：只发生在调用方持有的 JoinMap 引用释放后；索引先于借用的 RHS 清理。JoinMap 不跨查询复用；Reset 新建 JoinMap。

builder 不另起 goroutine；其消费者已有引用保护 borrowed RHS。等待者取消不释放 builder 的资源、不修改构建状态。各 pipeline 的 context 可独立取消，因此 builder 取消不能污染仍有效的其它消费者：builder 返回取消错误，本代索引禁用且不重试，其它消费者依据自身 context 取消或回退。只持短锁修改状态，绝不在锁下运行构建、I/O、channel 等待。

当前分支 `Compile.Reset/clear` 先清理 scopes，后重置 message board；multi-CN 的 board Reset 返回新 board，不立即销毁其它 fragment 仍可访问的旧 board。遵循此已有生命周期，不支持在活跃消费者持有引用时强行调用 FreeMemory。

## 错误、回退和取消

- 支持域不匹配、索引大小超限：返回 nil / 无错误；保留 RHS。
- 仅在 native 索引分配调用的边界识别 ErrOOM 和本分支明确的 mpool cap 错误；不能按全部 ErrInternal 降级，不能吞其它调用的错误。
- 发生上下文取消时，builder 自身返回取消；释放未发布数组，向其它消费者发布禁用状态。等待者自身的取消仍优先于回退。
- 构建每个 bounded 行窗口、碰撞链检查 ctx；probe 每个 bounded 窗口及碰撞链检查 ctx；回退 evaluator 补上行 / RHS batch 间取消检查。
- 成功发布后查询只读；取消不会销毁别的消费者仍使用的索引。
- 在输出前选择本次 probe batch 的 index/evaluator 路径。该索引的 probe 无内存分配，因此没有“输出一半后 probe scratch OOM”这一分支；输出向量分配失败直接传播。
- 结果续传保持既有 probeIdx / DefaultBatchSize 契约；Reset 和直接 Free 都清空索引借用及 JoinMap 引用，避免沿用前代 NULL 事实。

## 兼容性与运维

不修改 plan、proto、持久格式、catalog 或外部协议；remote LoopJoin 根据现有 NonEqCond 在本 CN 选择路径。每 CN / 每现有 JoinMap 至多一张表，不宣称跨 CN 单表。

无需新开关或配置；不适用与内存压力自动回退。删除快路径即可恢复原 evaluator，不需要数据迁移。无新权限、后台任务、日志维度或租户共享全局索引。随机 hash seed 和精确碰撞比较保证正确性，容量上限限制额外内存；碰撞导致的长循环受取消约束。

## 验证与验收

- pure helper：空/全 NULL/命中+NULL/NULL probe/重复、常量向量、跨 batch、长字符串/内嵌 NUL、反例类型及 GROUPING。
- 容量：实际并发构建共用小 cap，barrier 保持 live 索引；验证容量限制与部分成功/降级、无编码缓冲；检查清理后恢复基线。
- shared owner：同 JoinMap 多消费者只构建一次，等待者取消、builder 取消、不同 descriptor 回退、Reset/Free。
- operator：真实 HashBuild → LoopJoin → 输出链；DefaultBatchSize+1、evaluator 差分、错误顺序与非 OOM 传播。
- SQL：mo-tester 正常比对、NULL 真值表、天然 nullable 输入；另做 #32085 元数据修正后的隔离组合验证。
- 性能：3,000/6,000 行等值 IN/NOT IN 对照，不在 UT/BVT 中断言耗时。
- 静态/包级：Go 1.26.4、匹配 native artifacts；变更包测试及消费者、vet/lint；共享状态定向/所属包 race；变更覆盖率 ≥75%。

未完成证据不得标为通过。若实施发现需要 wire/global allocator 变更，停止扩项并重新评审设计。

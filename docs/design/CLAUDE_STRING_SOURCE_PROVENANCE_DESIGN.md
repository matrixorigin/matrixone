# StringSource 独立行级来源语义设计

- 状态：Proposed，等待独立设计审批
- Owning issue：[matrixorigin/matrixone#27215](https://github.com/matrixorigin/matrixone/issues/27215)
- Implementation PR：[matrixorigin/matrixone#27467](https://github.com/matrixorigin/matrixone/pull/27467)
- 设计 owner：SQL execution / container vector maintainers
- 实现核对表：[CLAUDE_STRING_SOURCE_BOUNDARY_INVENTORY.md](../../CLAUDE_STRING_SOURCE_BOUNDARY_INVENTORY.md)
- 设计审批记录：等待 maintainer 对精确 commit revision 给出独立 approval；实现审批在此之前保持阻塞

## 1. 背景、问题证据与成功标准

MatrixOne 的字符串值可能来自普通表达式、SQL literal、user variable、SQL PREPARE 参数或 MySQL binary protocol COM_STMT 参数。这些来源与 runtime string domain、prepare parameter conversion kind 是相互独立的语义轴。历史实现把部分来源隐含在 prepare/runtime metadata 中，导致透明边界、NULL、planner rewrite、group/aggregate、spill、remote transport 和 reset/reuse 发生来源丢失或错误合并。

问题由 issue #27215 的边界审计及 PR #27467 的反例测试确认：

- 透明 projection、VALUE_SCAN、LAG/LEAD、time-window 等边界可能把 NULL 或 selected value 的来源重置为 Expression；
- UNION、DISTINCT、GROUP BY 与 aggregate 既不能把来源作为 SQL equality key，也不能任意丢弃多个贡献者；
- sidecar 分配若晚于 vector length 或 group hash commit，会产生不可安全重试的半发布状态；
- MORPC 老版本不能解释新 metadata；
- 无 metadata 的 UnionBatch 若逐行扫描来源，会破坏基础向量复制热路径。

### 1.1 核心不变量

1. 合法来源集合固定为 `Expression`、`Literal`、`UserVariable`、`SQLPrepare`、`COMStmt`；未知值必须在状态发布前失败。
2. 透明边界逐行保持来源；选择边界采用实际 selected value 的来源，NULL 不例外。
3. 多个值共同生成一个代表值时：全部来源相同则保持，否则合并为 `Expression`；显式产生新语义值的普通函数、算术和 explicit CAST 结果属于 `Expression`。
4. 来源不参与 SQL value equality、hash key 或 filter-domain equality。
5. payload、length、hash/state 与来源 metadata 属于同一个 publication unit；所有可失败分配必须发生在不可逆发布之前。
6. 无来源 metadata 的路径不分配 sidecar、不写 trailer、不逐行扫描来源；uniform 来源保留标量表示。

### 1.2 可度量成功标准

- 五种合法来源在 batch、grouping、aggregate/group state 与 remote-capable codec 中按协议版本 round-trip；非法值确定性拒绝。
- issue #27215 清单中的每个透明、选择、合并、排序、重排、materialization、spill、transport 与 reuse 边界都有 owner、规则及 typed oracle。
- allocation rejection 只能发生在 vector/group/aggregate commit 之前；commit 后来源发布不得分配。
- source-free `BenchmarkUnionBatchNoMetadata` 为 `0 B/op`、`0 allocs/op`，metadata 工作为 O(1)，同机同模式中位数不得超过不含本功能基线的 2 倍；uniform scalar 同样为 O(1) 且不得超过 source-free 当前实现的 2 倍。
- mixed sidecar 路径允许 O(n) 扫描，但不得重复扫描同一 selection；每个 flat physical row 的来源容量上界为 1 byte。
- fresh execution 与 reset/reuse execution 结果及来源一致。

## 2. 范围与非目标

### 2.1 范围

- `pkg/container/vector` 的标量/逐行来源表示、append/union/clone/window/sort/shuffle/compact/grow/reset；
- expression executor、planner literal/fold/deepcopy/hash/rewrite 与 VALUE_SCAN；
- UNION、DISTINCT、GROUP BY、aggregate、window、spill/materialization；
- batch、group state、process parameter、dispatch/remote result 与 MORPC capability gate；
- protobuf 中 planner literal 的来源编码；
- typed UT、兼容性测试、allocation failure test、benchmark 与 SQL BVT。

### 2.2 非目标

- 不用来源推断 runtime binary/text domain 或 numeric conversion kind；
- 不改变 SQL equality、排序、去重行数或普通函数的字节语义；
- 不把来源暴露为用户可查询 SQL metadata；
- 不为该 metadata 增加独立持久化 catalog、后台 worker、配置项或动态 feature flag；
- 不保证旧 MORPC peer 保留其无法解释的新来源，只保证安全降级和既有 metadata 兼容规则不被放宽。

## 3. 现状与替代方案

### 3.1 方案 A：继续复用 prepare/runtime metadata（拒绝）

优点是新增字段较少。缺点是三个语义轴并不等价：Literal、UserVariable 与 COMStmt 的 conversion/domain 可能相同但 owner 不同；NULL 也可能没有 prepare kind 却仍有来源。继续复用会让 consumer 通过来源猜类型，无法给出 total、稳定的 merge rule。

### 3.2 方案 B：来源进入 value/hash identity（拒绝）

将来源和 value 共同参与 hash/equality 可以自然保留不同 owner，但会改变 SQL UNION、DISTINCT、GROUP BY、IN/filter 的结果行数和匹配语义。来源是执行 provenance，不是 SQL value domain，因此不能成为 key。

### 3.3 方案 C：始终分配逐行 sidecar（拒绝）

实现简单且所有 row operation 都可直接复制一个 byte，但会让普通向量固定增加 1 byte/row，并使 source-free append/union、marshal 和 reset 付出持续成本；这违反主路径预算。

### 3.4 选定方案：标量 fast path + 按需逐行 sidecar

Vector 保存一个 uniform scalar source；仅 mixed rows 分配 `[]StringSource` sidecar。透明 row movers 与值使用同一 permutation/selection；合并 owner 显式应用规则。wire metadata 独立 capability-gated。该方案在 correctness、兼容性、容量和 source-free 性能之间具有最小总复杂度。

## 4. 数据模型、owner 与边界规则

### 4.1 表示

- `stringSource`：整个 vector 的 uniform owner；默认 `Expression`。
- `stringSources`：仅 mixed flat vector 使用，长度跟随 physical metadata row count，元素宽度 1 byte。
- constant vector 只能有一个来源；mixed constant decode 必须失败。
- `StringSource` 与 `PrepareParamKind`、`RuntimeStringDomain`、binary/text row bitmap 独立存储、独立验证。

Vector 是 row-level metadata 的第一 owner；Batch 只协调多列原子 admission；operator/aggregate 拥有其 commit 前的预检与 publication lifetime；transport owner 负责 capability gate。

### 4.2 规则表

| 边界类型 | 规则 |
|---|---|
| Literal / variable / prepared parameter 创建 | 写入对应来源，NULL 相同 |
| projection、implicit CAST overload 0、clone/window/union | 按实际 row 透明复制 |
| COALESCE、LAG/LEAD default、winner selection | 采用 selected row 来源 |
| IF/CASE/IFNULL common-domain、普通函数、算术、explicit CAST | `Expression` |
| UNION/DISTINCT/GROUP BY duplicate representative | SQL key 忽略来源；同源保持，异源合并 `Expression` |
| MIN/MAX/MAX_BY representative | winner replacement 复制 winner；equal contributors 同源保持、异源合并 `Expression` |
| sort/shuffle/compact | 与 value/NULL/grouping 使用同一 permutation；compact 仅在全部逐行语义相等时物理合并 |
| reset/reuse | 释放 sidecar，恢复默认 scalar；下一 generation 不继承旧来源 |

详细 owner 与测试映射由 implementation inventory 维护；若 inventory 与本文冲突，以经批准的本文为准并先更新设计。

## 5. Publication、失败与生命周期

### 5.1 Vector publication

状态顺序为：

1. 根据 source scalar/sidecar 与 selection 计算 summary；
2. 预留 payload、area、NULL/grouping bitmap、prepare/runtime metadata 与 StringSource sidecar；
3. retain source reservation，禁止中间 uniform 状态触发 normalize/release；
4. 发布 payload 与 length；
5. 写入所有逐行 metadata；
6. finalize 并将最终 uniform sidecar 归一化回 scalar。

任一步骤 1–2 失败时 logical length 和 payload 不变。步骤 3 后的错误出口必须统一 finalize；已预留容量可以保留复用，但不得留下 publishable proof。步骤 4 后不得发生来源分配。

Batch 在发布第一列前完成全部列的来源 admission，并在所有成功/错误出口 finalize。已有 mixed destination 可直接证明 sidecar capacity，不重复扫描 source selection。

### 5.2 Group 与 aggregate publication

Group/aggregate outer preflight 必须覆盖：new row、existing row、loser、tie、transient winner、same-source 后 mixed-source 的顺序。source reservation 从 preflight 成功保留到完整 `BatchFill`/`BatchMerge` 结束。hash preview commit 后，group key 与 aggregate source mutation 均不得分配；预检失败进入既有 spill/retry，而不是发布半成品。

### 5.3 Reset、reuse、cancel 与 corrupt input

- reset/free 是 sidecar 的唯一 terminal release owner；MPool owner 与 vector backing 一致。
- cancel/error 只结束 reservation proof，不回滚已经合法扩容但尚未发布的 capacity。
- decoder 在任一列 metadata apply 失败时清理全部列的 prepare kind、runtime domain 与 StringSource，避免前序列残留半状态。
- unknown enum、mixed const、row-count/version mismatch 在返回前保持 decoder 可安全复用。

本设计不新增 goroutine、锁、wait dependency、重试队列或无界容器。

## 6. 性能与容量模型

| 情形 | 时间 | 额外容量 | 分配预算 |
|---|---:|---:|---:|
| source-free / scalar Expression | metadata O(1) | 0 | 0 |
| uniform non-Expression | summary O(1)，标量 adoption O(1)；已有 mixed destination 使用 bulk range fill | 0 或既有 sidecar capacity | publication 后 0 |
| mixed source | O(selected rows) | 1 byte / physical row | commit 前至多一次必要 grow |
| wire | O(rows) 仅在存在 metadata 时 | bounded trailer | 老版本 0 source trailer |

明确接受标准：1024-row INT64 `BenchmarkUnionBatchNoMetadata` 与无本功能基线同机同模式中位数比值不高于 2.0，且 0 allocation；uniform scalar 相对 source-free 当前实现不高于 2.0。任何超过阈值的变化阻塞合并。mixed benchmark 用于监控线性斜率和重复扫描，不与 source-free 阈值比较。

## 7. MORPC v36/v37 兼容、降级与回滚

### 7.1 Capability ownership

- MORPC v35 归属 scaled variance state with exact numeric origins。
- MORPC v36 归属 prepared JSON comparison execution and exact parameter types，不代表独立 StringSource capability。
- MORPC v37 是第一个声明独立 StringSource batch/process/group-state transport 的版本。
- sender 和 remote-result receiver 必须使用同一个 v37 gate；未知 service/version fail closed。

### 7.2 混合版本矩阵

| Sender / receiver capability | 行为 |
|---|---|
| 双方 >= v37 | 编码并恢复 StringSource；非法值拒绝 |
| 任一方仅 v36 | 丢弃 source-only metadata，不写 v37 trailer；JSON comparison/exact parameter type v36 行为保持不变 |
| 老 peer 且存在旧 prepared metadata incompatibility | 继续按旧 gate 拒绝，不能因 source downgrade 放宽 |
| local/spill codec | 使用显式 local framing；reader 对版本、row count 和 enum 严格验证 |

### 7.3 Rollback

实现 rollback 不需要 catalog migration：停用/回退到 v36 peer 时 sender 不发送 v37 source metadata，远端安全退化为 `Expression`，SQL value bytes、NULL 和 row count 不改变。回滚损失的是不可见 provenance 精度，不产生无法读取的持久化数据。恢复 v37 后仅新传输值重新具有精确来源；不尝试反推历史来源。

Planner protobuf 新字段使用 backward-compatible default/offset encoding：旧 payload 的零值保持原有 Expression 语义；旧 reader 忽略未知字段。生成文件必须由 protobuf tooling 产生。

## 8. Rollout、fallback 与可观测性

1. 先以 typed UT 和 local codec 验证五类来源、NULL、mixed、invalid、reuse；
2. 再启用 v37 sender/receiver capability，混合版本矩阵作为 merge gate；
3. BVT 只证明 SQL 路径可达、bytes/NULL/row count 不回退；来源精度由 typed oracle 证明；
4. benchmark 阈值阻止 source-free 热路径回退。

无需用户配置。安全 fallback 是协商至 v36 并丢弃 source-only metadata。诊断依赖 deterministic decode error、MORPC negotiated version、现有 allocation-account telemetry 以及对应 typed test；不在 per-row 热路径增加日志或高基数指标。

若上线后发现来源 metadata 引发 correctness 或性能问题，可先将 remote capability gate 回退到 v36，保留本地来源语义；若本地也需回滚，则回退该实现 commit。由于无 catalog/on-disk migration，rollback 不需要数据修复。

## 9. 安全与隔离

StringSource 是内部枚举，不包含用户文本、secret、地址或 tenant identifier，不对 SQL 用户暴露。remote decoder 在 trust boundary 验证 enum、长度和版本，防止 malformed trailer 造成越界或无界分配；sidecar capacity 受 vector row count、MPool 与 allocation account 约束。来源不参与权限判断，也不能替代 authentication/authorization context。

## 10. 验证计划与 acceptance evidence

| 合同 | 最小证明 |
|---|---|
| enum / scalar / mixed / NULL | vector 与 expression typed UT |
| transparent/select/merge rules | owning operator UT，逐行 `GetStringSourceAt` oracle |
| sort/shuffle/compact/reset | row-transform 与 lifecycle UT |
| publication atomicity | reject-next-allocation、future row、existing row、reverse-order UT |
| group/aggregate commit | Group/MergeGroup spill/retry + aggregate fill/merge UT |
| codec compatibility | v36 negative、v37 positive、invalid/reuse、buffered/streaming UT |
| public SQL reachability | StringSource provenance BVT；bytes/NULL/row-count oracle |
| hot path | no-metadata/uniform/mixed benchmarks，0 alloc 与阈值检查 |
| generated protobuf | generator output、build/vet/round-trip UT |

BVT 不作为内部 source 的直接 oracle，因为现有 SQL 函数不读取该 metadata；增加公开 introspection API 不在范围内。并发/race 专项不适用：该设计不新增共享 mutable state 或 goroutine，Vector 仍由既有 pipeline ownership 串行访问。

## 11. 风险、权衡与决策记录

1. **v36 peer 会丢失来源精度**：接受；peer 不具备 StringSource capability，保留 SQL value 与 JSON comparison/exact parameter type v36 行为优先。旧 prepared incompatibility 仍拒绝。
2. **mixed row 增加 1 byte/row**：接受；仅 mixed vector 按需付费，受 row capacity 和 allocation account 约束。
3. **Expression 同时表示真实 Expression owner 与 conservative mixed merge**：接受；该枚举是 owner category，不提供贡献者集合审计。
4. **BVT 不能直接观察内部来源**：接受；typed owner oracle 与 public bytes/NULL reachability 分工，避免新增用户 API。
5. **不增加 feature flag**：接受；wire 已 capability-gated，本地表示 backward compatible，额外配置会引入双语义状态和长期维护成本。

无阻塞开放问题。若审批者改变 v36 downgrade、merge rule、sidecar bound 或性能阈值，必须先更新本文并重新进行受影响设计审批，再调整实现。

## 12. 设计审批与实现符合性

### 12.1 待审批内容

审批者需要明确确认：

- 独立来源轴及 selected/merge rule；
- scalar + on-demand sidecar owner/bound；
- commit 前 admission 与 retain/finalize 生命周期；
- MORPC v36/v37 mixed-version、downgrade 与 rollback；
- source-free/uniform 性能 acceptance threshold；
- rollout、fallback、observability 和验证矩阵。

### 12.2 Approval record

- 设计 revision：等待本文提交后的精确 commit SHA
- 审批者：待 maintainer
- 决定：Pending
- 日期：待填写
- 实现偏差：当前未发现；PR #27467 必须以获批 revision 为基准再次核对

本文进入独立 design approval 前保持 `Proposed`。只有 traceable maintainer approval 覆盖精确 revision 后，才能把状态改为 `Approved`，并解除 implementation approval gate。

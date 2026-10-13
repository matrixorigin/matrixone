# 普通等值 INNER JOIN 的向量 Top-K 索引支持：设计 v1

- 所属 issue：#29702。
- 状态：v1 已获用户批准；2026-10-09 用户批准 v1.1 方案 A，完整成本比较不再作为首版门禁。批准记录见第 9 节及配套 v1.1 文档。
- 日期：2026-10-08。
- 基线：`mo/main`，`ffcce7ecbab771d78edf8415db32880b133cce71`。
- 实现 PR：尚未创建。
- 范围：此前确认的阶段一，不扩展所有 JOIN。

## 1. 问题、证据和设计门禁

目标查询：

```sql
SELECT a.chunk_id
FROM chunks a
INNER JOIN documents b ON a.document_id = b.document_id
WHERE l2_distance(a.embedding, '<查询向量>') <= 0.5
ORDER BY l2_distance(a.embedding, '<同一个查询向量>') ASC
LIMIT 10;
```

历史 `d2a3391` 的单机实测：plain LIMIT 和显式 PRE 都是普通扫描 + INNER JOIN + Sort；相同表去掉 JOIN 后 PRE 命中 IVF。历史测试不是当前基线的红例，批准后必须在当前基线重新建立公开 SQL 证据。

当前源码事实：

- `apply_indices_vector.go:buildVectorSortContextThroughJoin` 识别 SEMI membership，或 `INNER + trivial ON` 的单行查询向量 provider；不识别一般等值 INNER JOIN。
- `apply_indices.go:applyVectorIndicesEarly` 在 join ordering/distribution 前通过 `LogicalSearchHooks` 产生逻辑索引扫描；另有晚期 `applyVectorIndexForSortContext`。
- `apply_indices_ivfflat.go` 已有精确 required membership domain、SEMI 屏障、PK 回表、距离阈值处理及 PRE placement 约束。
- `force` 明确跳过索引；不能解释为强制使用索引。

这是新增查询优化能力，虽以 fix 请求开始，仍按 feature 处理。复杂度门禁：改变 JOIN/Top-K 热路径候选资格与重复语义，并穿过 planner、runtime-filter 和 compile placement 边界。必须先批准设计；不以预估 diff 小为理由免除。

## 2. 范围和非目标

首版支持：

- 一个普通等值 INNER JOIN，向量排序只依赖其中一侧 A 和原有规则支持的固定/执行绑定查询向量。
- A、B 为可安全重复读取的普通表扫描；两侧确定性本地过滤保留。
- ON 为跨两侧列的等值条件合取；匹配键可唯一，也可重复。
- 单个受支持向量距离排序 + LIMIT；OFFSET 的预算和最终分页明确分离。
- 向量表写在 JOIN 左侧或右侧均可识别，通过绑定 tag 定位，不靠 SQL 文本顺序。
- 普通 LIMIT 和显式 PRE；其它显式策略若不能证明满足此候选资格契约则回退，不静默改变 FORCE/POST/AUTO 配置语义。
- 最終可投影 A/B 的确定性列或表达式，保留原 JOIN 供 B 列解析；不把 B 列塞入索引结果。

首版索引能力：复用已有支持 required membership 的 IVF-FLAT 路径。继续通过插件 registry/capability 派发，不新增算法名分支。HNSW/GPU 索引没有等价 membership 能力时按现有能力判定回退；不声称首版覆盖所有向量算法。

非目标：OUTER/ANTI 的新增规则、非等值 JOIN、跨多级 JOIN 的泛化、双侧向量全局最近对、排序含额外 tie-break 键、聚合/DISTINCT/window 后的 JOIN Top-K、相关查询向量、任意不可安全复制的子查询/CTE、分区表/charset/UDF 等默认排除范围。

这些形态继续正确使用旧计划，不报新增“不支持查询”错误。已有 SEMI/provider/scalar-vector 能力不得回归。

## 3. 核心不变量与反例

采用 SQL bag 语义，不是集合语义。

1. 若 A 行 a 在 B 中匹配 m(a) 行，输出仍有 m(a) 份，且 B 输出列来自正确匹配行。
2. 所有影响 a 是否有合法匹配的谓词必须在候选预算之前参与资格判断。
3. 最终 LIMIT/OFFSET 位于保留的 INNER JOIN 之后；候选侧不提前消费最终 OFFSET。
4. 原距离过滤及不被索引编码可靠表示的谓词不得丢失或只用近似分数判定。
5. mode=force 保持精确旧路径；ANN 分支不新增全量精确召回承诺。
6. 插件/证明拒绝时原计划及引用、用户分页、scan filter、统计状态不被部分修改。
7. membership 域完全构建、发布后，向量候选 reader 才能读取；空域是拒绝全部，不是 PASS。

最小反例：全局最近 A 行无匹配；稍远 A 行匹配两条 B。先全局 Top-K 再 JOIN 会缺结果，直接 SEMI 替换 INNER 会少重复，提前 OFFSET 会跳过错误的行。

## 4. 推荐架构：资格与重复展开分离

将目标形态理解为：

```text
原始：
  FinalTopK(K,O)
    INNER JOIN(A, B)

候选改写：
  FinalTopK(K,O)                  ← 原排序、最终 LIMIT/OFFSET
    原 INNER JOIN                 ← 保留 ON、重复次数及两侧输出
      候选 A（按距离取最多 K+O 个合格 A，OFFSET=0）
        IVF-FLAT + 必需的精确 membership 域
          membership producer：复制的 A SEMI JOIN 复制的 B
      原 B
```

membership producer 保留与原 JOIN 等价的资格条件，只发布 A 的主键，不输出重复。原 INNER JOIN 仍执行匹配展开；不新建 count-map 或“重复行展开”执行算子。

唯一键情形同样使用这条通用路径。首版不要求额外实现基于唯一键证明的 JOIN 消除；这样不为重复语义引入第二套算法，后续再按证据优化唯一键成本。

### 4.1 候选预算证明

令 E 是所有通过完整资格判断的 A 行，距离只依赖 A，且每个 a∈E 的 m(a)≥1。令 P=K+O。

对 E 按距离取前 P 个不同 A：任意被排除 A 之前至少有 P 个合格 A，而这 P 个 A 在原 JOIN 中至少贡献 P 行。因此被排除 A 不可能影响 JOIN 后前 P 行的窗口，最终可以在展开结果上应用 OFFSET O、LIMIT K。

距离同分且 SQL 没有其它排序键时不规定固定 tie 顺序；不能新增全序要求。此证明是候选截断的关系语义证明，不是 ANN 全量召回证明；单列表/完全探测用于结果 oracle，生产近似策略沿用现有契约。

如果资格不完整、m(a)可能在后续谓词过滤后变为 0、距离依赖 B、存在额外排序键或聚合，此证明不成立，必须拒绝改写。

### 4.2 规划及引用闭包

- 在 early logical 与 late fallback 中共享同一识别/应用入口；不修改原有 provider JOIN 的 eligibility。
- 插件入口前构造隔离候选子树：内部 PROJECT 只投影所需 A 列/主键和距离，不复制原上层混合输出表达式。
- 候选子树使用现有 SEMI `vectorSortContext` 和 registry 的 membership 能力，避免扩展 wire/protobuf 或为此添加插件接口。
- 复制 producer 两侧并重新绑定：特别不能让 membership producer 与仍保留的原 INNER JOIN 共用一个可变 B 节点，否则 remap/compile 多父使用会污染引用。
- 从原 JOIN 的 ON、两侧 filter、上层 projection/order 引用计算 A 所需列；防止 covering 普通索引/IVF index-only 提前剪掉 document_id。
- 候选成功后才把原 JOIN 的 A 输入替换为候选输出，发布局部 A tag→输出表达式映射；B 及外层输出保持原绑定语义。
- 同步 guard：在递归普通索引改写前保护待识别扫描，成功或拒绝后正常释放保护，不永久禁用普通索引。
- 有不可推入 producer 的 post-JOIN FILTER、子计划分页、volatile/副作用表达式、不一致 snapshot 或不稳定来源时保守回退。
- 查询快照、对象权限与 prepared dependency 由原机制传播，不以生成 SQL/新事务执行内部子查询。

### 4.3 模式、距离与成本选择

- 使用既有插件类型/距离函数/方向 eligibility，不自己认所有距离函数。
- 原 SEMI 的 required membership 可复用，但距离范围和 residual filter 必须分别保留；有损索引不能用编码距离代替精确 WHERE。
- 显式 FORCE 无条件保持旧路径。显式 PRE 可用当前支持的 membership 策略。
- 默认路径在正确性检查和既有插件能力门禁通过、索引可用时优先使用 IVF，不要求新增 JOIN 成本比较接口。显式 AUTO/POST 若无法保持原语义，首版保留原关系计划并在测试中明确覆盖；FORCE 始终保持旧路径。
- 额外 eligibility producer、B 再次读取、候选回表和重复展开可能使部分查询更慢；这是用户批准的首版取舍，不编造固定“更快”结论或加速倍数。
- 完整成本模型作为后续独立优化，不在本次增加未经定标的阈值、算法名派发或展开运行时框架。该变更于 2026-10-09 通过用户 `go ahead` 批准，替代 v1 原成本比较要求。

## 5. 方案比较

| 方案 | 正确性 | 成本/复杂度 | 决定 |
|---|---|---|---|
| 原 full scan + JOIN + Top-K | 保持全部语义 | 向量宽列与距离计算成本高 | 保留为回退/独立 oracle |
| 直接 INNER 改 SEMI | 重复键和 B 投影错误 | 最简单 | 拒绝；仅额外唯一性证明时可能安全 |
| 先全局 ANN K，再 JOIN | 最近不匹配行占预算 | 简单但少结果 | 拒绝 |
| 新建 join-aware 迭代探测/权重展开执行算子 | 可覆盖更一般场景 | 增加状态、协议、补搜、取消与跨 CN 成本 | 本期不采用 |
| 精确 membership + 保留 INNER JOIN | 可保留重复和两侧投影 | 增加一次资格生产和 B 读取，复用现有算子 | 推荐 |

参考：SQL INNER JOIN 的 bag/multiplicity 与 LIMIT 窗口是强制契约；pgvector 的 distance operator + ORDER BY + LIMIT 仅是规划先例，不保证所有 JOIN 必然使用索引。设计优先复用 MO 自己已有 SEMI membership、索引插件和 placement，而非仿造 PG 算子。

## 6. 所有权、资源、执行和兼容

- 新增的是 statement-local planner 子树和映射；owner 为 QueryBuilder，无新增后台 goroutine、持久缓存、catalog 状态或跨语句复用数据。
- 运行时 membership filter 仍由现有 HashBuild/消息/reader 生命周期拥有；原 hash join 和 sort 仍拥有重复展开及最终 Top-K 的内存/溢写。
- 设 A/B 大小 N/M，合格 A 数 E，最大匹配重数 D，P=K+O：producer 规模至多原 eligibility JOIN，精确域含至多 E 个 A PK；结果 JOIN 的 A 侧候选有 P 个，输出可达 P·D。P 大和 D 大时仍存在原 JOIN 资源风险，不能提前分配 P·D 缓冲或承诺复杂度与 K 完全线性。
- 不引入新的无限补搜；沿用已有 probe/candidate 限制。成本与 domain size 由原 memory/spill/error 机制约束，不吞内存不足或取消错误。
- 发布顺序：producer seal→required domain publication→reader opens→候选输出→原 JOIN→FinalTopK。失败、空输入、取消、consumer LIMIT 提前结束及 PREPARE 重执行不得遗留 domain/reader。
- 新复合计划首版保守使用 one-CN membership 路径；不能仅把某个 scan 标记为 local 就声称整个依赖图已安全。必须以最终 physical placement 验证生产者、消费者在同一个域可见范围；现有分布式资格证明不得误接受新 composite shape。
- 不修改 wire/磁盘/catalog，旧 binary 不会生成新改写；普通算子和原协议门禁继续有效。mixed-version 若 placement 能力未满足则使用现有 one-CN/旧计划回退，不新增协议号。
- 两次 B 读取必须使用同一事务快照；普通授权、租户过滤与 snapshot 传播分别覆盖 producer、候选回表及原 JOIN，不开独立内部 SQL 通道。
- 可通过既有 force/优化器回退禁用新路径；EXPLAIN 能看到 Vector Index Scan、资格 producer、保留的 INNER JOIN 和最终 LIMIT，便于诊断。

## 7. 变更/风险地图

| 闭包 | owner/consumer | 风险 | 拟修改/证明 |
|---|---|---|---|
| 等值 JOIN 识别、隔离候选、拼接和 remap | pkg/sql/plan；plugin BuildLogicalSearch/ApplyForSort | R2，结果及绑定契约 | early/late 同一规则、typed plan、reject 不改原树 |
| required membership + final JOIN placement | 既有 planner→compile→HashBuild→vector reader | R3，domain 先行、取消与热路径 | 无新增执行状态；实际 placement/result，相关 lifecycle consumer 测试 |
| 测试与公开 SQL | planner fixture、现有 vector membership BVT | R1/R2 | 有索引/无索引独立 oracle、最小数据 |
| 稳定设计/使用范围说明 | docs/design | R0 | reviewed revision 与批准证据，TODO 不提交 |

不预先扩大其它 owning package 变更；实现出现新协议/算子或 materially 不同资源模型时停止并重新设计。

## 8. 验证矩阵与验收

优先复用 `apply_indices_vector_join_test.go` fixture 和 `vector_ivf_membership.sql` 的小数据资格场景；现有后者含持久化等无关内容，是否增加独立 coherent BVT 由相同 fixture 与成本决定，不把新语义埋进无关大测试。

| 语义维度 | 独立 oracle | 核心检查 |
|---|---|---|
| 唯一键/重复键 | 无索引或 FORCE 的 JOIN 输出；不是 IN 输出 | 重复次数、B 输出列、最终 K |
| 最近 A 无匹配 | 手写小样本预期 | eligible ANN 在候选预算前过滤 |
| NULL、无匹配、空表、少于 K | 返回行集合/数量 | 空域不是 PASS，无错误补满 |
| 阈值边界/有损 payload | 源距离谓词结果 | 不丢距离过滤、不假定量化 score 精确 |
| A 位于左右两侧、多个等值键 | 等价 SQL 结果 + typed references | 不靠位置识别；类型/coercion 安全 |
| 距离别名/中间 PROJECT/右侧输出 | 公开 planner + SELECT | tag/列裁剪/regular covering index 闭包 |
| LIMIT 0、OFFSET、参数 LIMIT 与向量 | 两次 PREPARE EXECUTE 不同参数/数据 | P=K+O，OFFSET 仅最终应用，不缓存旧 domain |
| FORCE、no LIMIT、其它 JOIN/排序、volatile | unchanged plan/result | 保守拒绝，现有 SEMI/provider/scalar 不回归 |
| 复制失败/插件拒绝 | typed 原计划比较 | 失败无部分改写、guard 不泄漏 |
| one-CN 与来自两 CN 入口 | physical placement + 独立结果 | domain 完整、无重复多 CN multiplicity 放大 |
| cancel、producer error、提前 consumer stop | 既有生命周期 seam 的错误/资源 oracle | reader/filter/mpool 释放、无等待环 |

确定性 UT/BVT 通常只需 2 维、5–8 条 A、少量 B，单列表完全探测；不沿用 120×1536 作为常规 UT fixture。历史业务维度仅做一次手动复核。无 wall-clock 性能断言或 sleep 同步。

执行顺序：当前基线 red→focused typed UT/公开 planner→完整 owning plan package→直接 consumer/placement→mo-tester genrs 人工 review→比较两次并检查 teardown→增量静态检查及需要的 race/benchmark。工具链严格 Go 1.27.1，CGo 使用 repository wrapper；native 来源检查通过后再启动单机。

改动生产行覆盖率 ≥75%，不把 EXPLAIN 成功当作结果正确；多 CN/性能只在契约依赖处加证据。默认不承诺全库 CI/生产召回或大规模加速倍数。

## 9. review 决策记录

- 已确定：保留 INNER JOIN 处理重复；复用 exact membership；不新建执行算子；不扩展其它 JOIN；优先 IVF-FLAT 原能力。
- 设计批准前需确认：首版上述 eligibility/模式范围、保守 one-CN 执行边界以及双读 B 的成本取舍。
- 设计技术核验项：当前基线公开 red、candidate 子树的列映射与 cost/placement proof 将在获准实现前/实现过程中用定向测试验证；一旦发现不能复用现有机制，停止代码扩张并更新设计，而非假定可行。
- 批准记录：用户在 review 本设计后回复 `go ahead`，批准实现 v1。批准前完整文档 SHA256：`062cda7b7acc5e56763bd4588912d78e4624a5cb8d65de41bfaf8040786cb2ba`。
- 2026-10-08 当前基线公开 planner 红例已确认：唯一键/重复键两组都未出现 Vector Index Scan；相同 TestEquiVectorTopKPublicPlan 将用于绿例。Go 1.27.1，CGo wrapper，退出码 1，仅测试代码新增，未改变生产代码。
- 2026-10-08 实现核验：核心原型、owning packages、生命周期/race、两轮正常 BVT 及 1536 维实际 SQL 通过；当时因第 4.3 节的完整成本承诺未满足而暂停。
- 2026-10-09 用户在明确“正确性检查通过且索引可用就优先走 vector index，成本模型以后再做”后回复 `go ahead`，批准 `CLAUDE_20261008-vector-equi-join-topk-v1-1-review.md` 方案 A。已关闭成本门禁；合并最新 mo/main 并继续最终验证，未授权本次自动 push/PR。

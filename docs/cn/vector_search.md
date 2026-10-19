# 向量检索模式与 IVF 参数

本文说明当前主干的检索行为。排查已发布版本时，请同时记录镜像标签、
镜像摘要和 `SELECT VERSION()`；仅凭 `latest` 标签无法判断包含哪些修复。

## IVFFLAT：返回行数与召回率

```sql
SELECT id FROM t WHERE category = 1
ORDER BY l2_distance(v, '[0,0]') LIMIT 5
BY RANK WITH OPTION 'mode=auto';
```

这里的向量维度应与 `v` 一致。IVFFLAT 的模式含义如下：

| 模式 | 行为 | 使用边界 |
| --- | --- | --- |
| `post` | 在有限的索引候选中搜索，再应用剩余过滤条件 | 即使表中有足够的匹配行，也可能返回不足 `LIMIT` 的结果；INCLUDE 条件下的空页回退见下文 |
| `pre` | 将可提前执行的过滤条件用于索引搜索或主键成员过滤 | 通常改善过滤检索，但有限探测仍可能遗漏近邻 |
| `include` | 将索引覆盖的过滤条件下推到索引读取；无剩余过滤条件时可逐轮扩大探测范围 | 有未覆盖的过滤条件时转为 PRE 成员过滤；仍不保证精确召回 |
| `auto` | 根据统计信息选择精确扫描，或在结果不足时依次尝试 POST、PRE、FORCE | 补足结果行数；满页的近似结果不保证精确召回 |
| `force` | 对符合条件的原表数据计算距离并排序 | 用于需要精确结果的查询，也可作为召回率比较基准 |

`include` 的过滤覆盖列包括索引的 INCLUDE 列和单列主键，复合主键不自动覆盖。
只有查询所需列均被覆盖、没有剩余过滤条件且规划边界可确定等条件满足时，才可能
只读索引而不回原表。读取实现背景见 [INCLUDE Top-K RFC](../rfcs/20260904_ivfflat_filtered_include_topk.md)，
多轮探测与未覆盖条件示例见 [`vector_ivfflat_include_rounds.sql`](../../test/distributed/cases/vector/vector_ivfflat_include_rounds.sql)。

显式 `mode=post` 若含可由 INCLUDE 列覆盖的过滤条件，且查询形状支持安全重放，
首轮结果为空时会在该 Top-K 子树内回退到精确扫描；非空但不足 `LIMIT` 的页保留 POST 结果。

AUTO 的回退在对应的 Top-K 子查询内进行，也处理非空但不足 `LIMIT` 的页。
不适合安全重放的查询形状会直接使用精确路径。没有 `OFFSET` 时，若只有
`m` 行符合条件，最终返回 `min(LIMIT, m)` 行；不足一页本身并不表示错误。
使用 `OFFSET` 时，还需扣除被跳过的行。

AUTO 可以接受已满页的近似候选，因此“返回 5 行”和“找到了真正最近的 5 行”
是不同的保证。需要精确召回时使用 `mode=force`。回退到精确扫描可能增加耗时；
AUTO 不是固定成本的检索策略，也不会把执行错误当作结果不足而继续回退。

以上 AUTO 说明针对 IVFFLAT，不能据此推断其他索引算法支持相同模式。
省略 `mode` 时还会受到 `enable_vector_auto_mode_by_default` 和
`enable_vector_prefilter_by_default` 的影响；比较模式时应显式指定。

## `probe_limit` 与查询级 `nprobe`

IVF 的 `lists` 是建索引时的聚类数，探测数决定一次搜索访问多少个聚类。
更多探测通常增加搜索工作量并改善召回；固定探测数下增大 `lists` 会降低
访问的聚类比例。没有适用于所有数据分布和维度的精确召回参数组合。

`probe_limit` 是动态系统变量，默认值为 5，合法范围为 1–80000。
普通 `SET` 或 `SET SESSION` 修改当前连接的值。请在**同一个连接**中执行并读回：

```sql
SELECT @@session.probe_limit;
SET SESSION probe_limit = 4;
SELECT @@session.probe_limit;  -- 4
```

连接池中的另一条连接不会继承这次会话修改。`SET GLOBAL` 设置全局默认值，
已有会话仍保留自己的值；不要把全局修改当作当前会话的调参方式。

查询级 `nprobe` 优先于会话值，范围同样为 1–80000，且不会修改会话状态：

```sql
SELECT id FROM t ORDER BY l2_distance(v, '[0,0]') LIMIT 5
BY RANK WITH OPTION 'mode=post', 'nprobe=10';
SELECT @@session.probe_limit;  -- 仍为 4
```

`nprobe` 也可单独使用，不要求同时指定 `mode`。AUTO 可以在这个基础探测数上
自适应扩大搜索范围，因此它不是 AUTO 的硬上限；实际访问的聚类数不超过 `lists`。
FORCE 不使用索引探测数。
可通过 `EXPLAIN VERBOSE` 的 `NProbe` 字段检查索引计划使用的探测参数。
比较召回率时，应使用相同过滤条件下的 FORCE 结果作为独立基准。

## IVFPQ 的构建能力要求

普通 CPU 构建提供 IVFFLAT 和 HNSW。IVFPQ 与 CAGRA 需要启用 GPU 支持的
MatrixOne 构建及相应 CUDA/cuVS 运行环境。IVFPQ 已有实现，并非只有语法支持。

```sql
SET experimental_ivfpq_index = 1;
CREATE INDEX ip USING ivfpq ON t(v) OP_TYPE 'vector_l2_ops' lists=10;
```

实验开关控制功能启用，不能为 CPU 二进制添加 GPU 能力。CPU 构建接受这段语法，
但在规划 DDL 时返回 `unsupported index type: ivfpq`；错误发生在创建索引隐藏表之前。
语法能被解析不代表当前二进制支持执行该索引。遇到此错误时应先确认构建是否启用
GPU 支持，而不是反复设置实验开关。

GPU 环境的功能验证可参考
[`vector_ivfpq.sql`](../../test/distributed/gpu_cases/vector/vector_ivfpq.sql)。
CPU 环境的 IVF 探测参数示例见
[`vector_rank_options.sql`](../../test/distributed/cases/vector/vector_rank_options.sql)。

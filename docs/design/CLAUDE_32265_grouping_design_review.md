# 分组所有权设计：独立审查记录

## 1. 当前决定

**v2 独立 AI 设计审查：PASS。维护者设计批准：待定。实现批准：BLOCKED。**

- 所属 [issue #32265](https://github.com/matrixorigin/matrixone/issues/32265)，实现 [PR #32520](https://github.com/matrixorigin/matrixone/pull/32520)。回应 [2026-11-13 设计门禁 review](https://github.com/matrixorigin/matrixone/pull/32520#pullrequestreview-5884437499)。本记录不自行将该 review 标记为已解决，不发起新的 review request。
- 精确审阅的设计：[v2，commit `65eed3a1342fe170523c5c434891eb3374398311`](https://github.com/matrixorigin/matrixone/blob/65eed3a1342fe170523c5c434891eb3374398311/docs/design/CLAUDE_32265_grouping_ownership.md)。Git blob `33b0e59ad26dfd414a061b16970495d1b657c882`；SHA-256 `1c883c22c2efffb85db8a0216402300f2775277bd98c36b9414c811233c434e8`。
- 实现检查锚点 `16082594b63b24b0ecf4aba43c92e15edc4f8a0b`，已合并 target `9937f1b74a7a7a01f8e0dcb6e7d3ff447e0ce29e`。v1/v2 的提交只改变设计文档，实际生产代码不变。
- 审阅方式：两次独立启动的 Codex CLI 0.167.0 只读会话，调用配置 `gpt-6-astra` / reasoning `high`；这说明调用配置，不冒充服务端模型身份证明，也不是人类维护者意见。审查者均在开始与结束验证文档 hash。
- 范围：设计的语义、替代方案、owner、证明发布点、lineage、成本/兼容取舍及验收；只读源码用于确认关键边界。未调用 `mo-self-review`，未在这两次审查中执行新 SQL/UT、运行服务或操作 GitHub。

## 2. 版本与审查顺序

| 轮次 | 精确对象 | 结论 | 审阅时HEAD与出处 |
| --- | --- | --- | --- |
| 1，2026-11-13 | [v1：`a6c4c9e30d8609a270c5c83b90b45db4359b9c8d`](https://github.com/matrixorigin/matrixone/blob/a6c4c9e30d8609a270c5c83b90b45db4359b9c8d/docs/design/CLAUDE_32265_grouping_ownership.md)；blob `27adc1a05f256141899c161c87b79d1fc1655ae6`；SHA-256 `fc7d4447ee70b6be9b2477e96adf0e9f7f129db16eba8a97d90ccc5cbb6dd66d` | REQUEST_CHANGES：DR1、DR2 | 在`16082594b6`工作区冻结内容上审阅，结束后以相同blob提交为a6c4c9e30d |
| 2，2026-11-13 | 上述v2精确revision/hash | PASS：两个设计决定已闭合，无其他设计阻塞 | 在`a6c4c9e30d`工作区冻结v2上审阅，结束后相同blob提交为65eed3a134 |

这是一段可单独识别的**后补设计阶段**，不是伪称“实现前已批准”。首轮失败被保留，没有用最终PASS覆盖历史。原始提示词、JSONL和完整终态输出保留在任务本地证据中，不把包含工作区路径的执行日志提交到仓库；这里版本化其必要审阅对象、发现、决定和出处。

原始终态输出校验：v1 SHA-256 `63bebb53b005a0d5d6580c6d1a33fe46afa2a7c1b42a079cf92353507389a2b1`；v2 SHA-256 `50784d35f8de0f15b4ac5047a96f9aaf8c304e1840a9c273fb0db55a7721e39a`。两次CLI均正常退出0；exit0本身不是设计通过，以上结论取自实际评审正文。

## 3. 发现、决定与未完成的实现

### DR1：DISTINCT 等价类不能凭隐藏 bitmap 选择来源

首轮发现：当输入含实际NULL时，用户只选g，真实NULL组与subtotal的g相等，但GROUPING(g)不同。DISTINCT必须按用户tuple折叠它们；再排序未选出的GROUPING没有唯一来源。保留bitmap不是答案，把marker偷偷加入DISTINCT键则改变行重数。

v2 §4.7 决定：AGG/SAMPLE统一保守准入。先复用已选槽位或可安全匹配的完整选出表达式，递归处理剩余依赖；未被覆盖的GROUPING调用一律1149。不依据PK/NOT NULL/当前数据放宽，不用IS NULL猜mask。显式选出flag或完整`GROUPING(g)+1`表达式的排序仍可合法。

复审结论：**设计关闭，实现未关闭。** 正反验收包含实际NULL/subtotal、重复集合、alias、LIMIT、SAMPLE、无NULL对照，拒绝路径须验证CTAS无残表；合法对照和用户tuple行重数可排除“一律拒绝”或“偷加隐藏key”。当前 `groupingDistinctOrder` 的 `allowGrouping=true` 仍是实现不符合项。

兼容代价已接受为设计取舍、尚待维护者批准：部分过去接受的AGG DISTINCT排序会变为1149，旧视图重绑也可能受此影响。旧格式可读不等于所有旧查询行为不变；显式选出marker会改变DISTINCT tuple，不能自动代应用改写。

### DR2：NULL 证明必须先于最早的分支消费者

首轮发现：v1把刷新放在UNION后，但分支HAVING和SAMPLE物化可先调用flatten。`HAVING NOT(g+0 IN(...))` 中subtotal操作数是NULL，不能让旧非空证明提前触发错误的MARK优化，最后修metadata也救不回来。

v2 §4.6 决定：建立分支输出证明B与合并输出证明U。B先于任何branch输出consumer的flatten/简化，保持owner tag、仅修改null-extended输出证明；AGG输入编码类型和原始sampleTag值保持独立。U在UNION后合并分支证明，供全局window/project/subquery使用；每次换孩子仍须自底向上刷新。

复审结论：**设计关闭，实现未关闭。** 验收包括分支HAVING和可达的SAMPLE早期消费、空/含NULL RHS、公共键/COUNT/COALESCE及原始采样值控制；同时检查flatten前类型、MARK前提和公有结果。不可达的synthetic内部测试不得冒充公有SQL复现。当前实现仍需补齐分支证明时序并用失败证据验证。

这两项是设计与当前源码之间的静态差距；本轮没有执行新反例，不宣称已经复现某个具体错误结果，也没有修改生产代码。

## 4. 接受的替代方案与风险

- A（局部修复并收缩支持）是有效备选，但不满足本次保留公开能力并纠正组合语义的目标；B（外层AST规范化）仍需解决scope/隐藏依赖/来源身份，不能自动消除边界问题；C（typed边界）以tag/ordinal保持身份，承担binder复杂度；D（原生多group AGG）改变runtime/分布式契约，超出本次hotfix。
- 选择C不是以既有实现为证据。接受分支扫描数及CUBE指数展开不变、规划复制/匹配可能超线性、没有新硬上限；全局WINDOW/DISTINCT面对完整行集的内存代价也必须明示。
- 不新增后台资源、执行算子或错误结果fallback。SAMPLE+OVER保持原NYI。
- 遵循停机升级；不要求混版本、downgrade或per-feature MORPC gate。历史CTAS数据/约束不自动修复。
- 精确生产Go计数是651增35删=686，grammar另5行；v1和先前说明中的786是统计错误，v2已更正，不改变门禁分类。

## 5. 继续生效的门禁

复审的原始决定为：

> Design decision: PASS，仅限本次审阅的 v2 设计。DR1、DR2 的设计决定均已闭合，未发现其他设计阻塞。§8 的实现缺口仍然有效，当前不能称为 PR ready。

合并/发布前仍需：

1. 维护者对以上精确设计revision的决定；本记录不代替GitHub设计批准。
2. 作者对DR1/DR2取得最小失败证据，按设计修复、完成正反UT/BVT与实现符合性审查。任何设计偏离重新评审，不能只更新“已通过”的文字。
3. 新基线compiler/frontend/reader消费链验证；实际remote贡献的多CN grouping/window及SAMPLE/DISTINCT/order证据、两个coordinator的结果与错误/取消清理。旧head单CN通过数字不自动覆盖新head。
4. 发布owner固定支持的已发布源tag/commit，验收持久视图重绑、停机升级/备份恢复，并告知新增1149影响。按目标负载需要补性能SLA证据；历史宽键耗时不是通用承诺。

此次文档交付只完成版本化设计、替代方案比较及独立设计审查，不宣称上述实现/发布工作已完成。未请求GitHub re-review、未监控CI、未改变PR的非draft状态。

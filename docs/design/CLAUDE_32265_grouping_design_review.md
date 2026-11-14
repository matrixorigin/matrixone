# 分组所有权设计：独立审查记录

## 1. 当前决定

**历史 v2 独立 AI 设计审查：PASS。维护者设计批准：待定。** 当前[设计 v3](CLAUDE_32265_grouping_ownership.md)整理公开说明、保留技术设计契约，未新增独立审查；历史结论不代表当前实现已经验收。

- 所属 [issue #32265](https://github.com/matrixorigin/matrixone/issues/32265)，实现 [PR #32520](https://github.com/matrixorigin/matrixone/pull/32520)。回应 [2026-11-13 设计门禁 review](https://github.com/matrixorigin/matrixone/pull/32520#pullrequestreview-5884437499)。本记录不自行将该 review 标记为已解决，不发起新的 review request。
- 历史独立审阅的精确设计：[v2，commit `65eed3a1342fe170523c5c434891eb3374398311`](https://github.com/matrixorigin/matrixone/blob/65eed3a1342fe170523c5c434891eb3374398311/docs/design/CLAUDE_32265_grouping_ownership.md)。Git blob `33b0e59ad26dfd414a061b16970495d1b657c882`；SHA-256 `1c883c22c2efffb85db8a0216402300f2775277bd98c36b9414c811233c434e8`。
- 实现检查锚点 `16082594b63b24b0ecf4aba43c92e15edc4f8a0b`，已合并 target `9937f1b74a7a7a01f8e0dcb6e7d3ff447e0ce29e`。v1/v2 的提交只改变设计文档，实际生产代码不变。
- 审阅方式：两次独立启动的 Codex CLI 0.167.0 只读会话，调用配置 `gpt-6-astra` / reasoning `high`；这说明调用配置，不冒充服务端模型身份证明，也不是人类维护者意见。审查者均在开始与结束验证文档 hash。
- 范围：设计的语义、替代方案、owner、证明发布点、lineage、成本/兼容取舍及验收；只读源码用于确认关键边界。未调用 `mo-self-review`，未在这两次审查中执行新 SQL/UT、运行服务或操作 GitHub。

## 2. 版本与审查顺序

| 轮次 | 精确对象 | 结论 | 审阅时HEAD与出处 |
| --- | --- | --- | --- |
| 1，2026-11-13 | [v1：`a6c4c9e30d8609a270c5c83b90b45db4359b9c8d`](https://github.com/matrixorigin/matrixone/blob/a6c4c9e30d8609a270c5c83b90b45db4359b9c8d/docs/design/CLAUDE_32265_grouping_ownership.md)；blob `27adc1a05f256141899c161c87b79d1fc1655ae6`；SHA-256 `fc7d4447ee70b6be9b2477e96adf0e9f7f129db16eba8a97d90ccc5cbb6dd66d` | REQUEST_CHANGES | 在`16082594b6`工作区冻结内容上审阅，结束后以相同blob提交为a6c4c9e30d |
| 2，2026-11-13 | 上述v2精确revision/hash | PASS（仅设计） | 在`a6c4c9e30d`工作区冻结v2上审阅，结束后相同blob提交为65eed3a134 |

这是一段可单独识别的**后补设计阶段**，不是伪称“实现前已批准”。首轮失败被保留，没有用最终PASS覆盖历史。原始提示词、JSONL和完整终态输出保留在任务本地证据中，不把包含工作区路径的执行日志提交到仓库；这里记录其审阅对象、设计决定和出处，不作为当前实现符合性的结论。

原始终态输出校验：v1 SHA-256 `63bebb53b005a0d5d6580c6d1a33fe46afa2a7c1b42a079cf92353507389a2b1`；v2 SHA-256 `50784d35f8de0f15b4ac5047a96f9aaf8c304e1840a9c273fb0db55a7721e39a`。两次CLI均正常退出0；exit0本身不是设计通过，以上结论取自实际评审正文。

## 3. 接受的替代方案与风险

- A（局部修复并收缩支持）是有效备选，但不满足本次保留公开能力并纠正组合语义的目标；B（外层AST规范化）仍需解决scope/隐藏依赖/来源身份，不能自动消除边界问题；C（typed边界）以tag/ordinal保持身份，承担binder复杂度；D（原生多group AGG）改变runtime/分布式契约，超出本次hotfix。
- 选择C不是以既有实现为证据。接受分支扫描数及CUBE指数展开不变、规划复制/匹配可能超线性、没有新硬上限；全局WINDOW/DISTINCT面对完整行集的内存代价也必须明示。
- 不新增后台资源、执行算子或错误结果fallback。SAMPLE+OVER保持原NYI。
- 遵循停机升级；不要求混版本、downgrade或per-feature MORPC gate。历史CTAS数据/约束不自动修复。
- 精确生产Go计数是651增35删=686，grammar另5行；v1和先前说明中的786是统计错误，v2已更正，不改变门禁分类。

## 4. 审查与验证边界

历史v2的PASS仅针对设计，不是维护者批准或实现验收。当前文档整理不新增审查结论，也不改变以下验证要求：

1. 维护者对精确设计revision作出决定；本记录不代替GitHub设计批准。
2. 新基线compiler/frontend/reader消费链验证；实际remote贡献的多CN grouping/window及SAMPLE/DISTINCT/order证据、两个coordinator的结果与错误/取消清理。旧head单CN通过数字不自动覆盖新head。
3. 发布owner固定支持的已发布源tag/commit，验收持久视图重绑、停机升级/备份恢复，并明确设计中的兼容性影响。按目标负载需要补性能SLA证据；历史宽键耗时不是通用承诺。

此次文档交付只完成版本化设计、替代方案比较及独立设计审查，不宣称上述实现/发布工作已完成。未请求GitHub re-review、未监控CI、未改变PR的非draft状态。

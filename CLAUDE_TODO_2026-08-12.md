# PR #26923 review / CI 修复计划（2026-08-12）

## 问题与不变量

1. 多目标 UPDATE 的 STORED generated column guard 当前引用 assignment PROJECT 自身输出；优化器只能从该 PROJECT 的子节点重映射列，因此合法计划在 `neededProj` remap 阶段失败。
2. CHECK loser-candidate BVT 使用两个正值，没有真正证明 `active AND row_number = 1` 能隔离未选中的违规候选。

必须保持以下不变量：

- PROJECT 内每个表达式只能引用其输入侧可解析的列，不能循环引用同一 PROJECT 的输出。
- generated/default/cast/CHECK 等 target-local 行为统一由最终 selected candidate（active 且 row_number=1）控制。
- 黑盒回归必须让 loser 实际违反 CHECK，并同时断言 winner、sibling 数据和 affected rows，避免只验证“语句未报错”。

## 实施步骤

1. 核对 PR 精确 head、工作树和中断进程；保留当前 review-fix worktree。
2. generated-column guard 改用 PROJECT 下方的 Rowid/row_number selector 表达式，与普通 assignment guard 共用同一构造路径。
3. 增强 planner typed UT，验证 generated guard 不引用当前 PROJECT 输出，并覆盖 generated target 的合法计划构造。
4. 将 CHECK loser BVT 的第二个候选改为负值，生成并验证精确 golden；同时运行已有 generated-column multitable BVT。
5. 运行 focused/full planner tests、覆盖率、build/vet、SCA、`make build` 和相关 BVT。
6. 对完整分支相对 verified PR base 执行 `mo-self-review`，检查 selector 从 PROJECT 输入到 generated/CHECK/PRE_INSERT/writer 的闭环，以及 Q1 资源、Q2 wait、Q3 增长边界。
7. push 前 fetch 并 merge 最新 `mo/main`；如有变化，重跑受影响验证。正常 push 到 origin，不 force push、不评论 PR。

## 回归矩阵

| 场景 | Oracle |
|---|---|
| generated target 位于多目标 UPDATE | 优化成功，基础列与 stored generated 值正确 |
| generated target 位于非首个 target | 两个 target 都写入且 generated 值正确 |
| loser 候选为负值且 winner 为正值 | UPDATE 成功，target=5、两个 sibling 更新、affected rows=3 |
| selected 候选违反 CHECK | 返回 CHECK 错误且事务不产生部分写入 |

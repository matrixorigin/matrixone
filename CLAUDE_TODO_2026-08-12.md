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

## 上游 merge 适配

最新 `mo/main` 开始让所有可用 regular index 参与 optimizer rewrite，暴露出 stage-1 multi-target UPDATE 的 target selector/dedup 计划尚不能安全穿过 index-scan 替换：`UPDATE IGNORE` 会绕过 target-local dedup 并提交唯一键错误。当前阶段对 multi-target UPDATE 禁用 source index rewrite，保持关系扫描语义；单目标 DML 和 SELECT 的索引选择不受影响。后续若支持该优化，必须先证明 selector、Rowid、dedup 与 index-only/index-join 重写的完整列映射闭环。

## Review 修复：irregular index 行镜像与索引 rewrite 边界

### 不变量

- 每个 irregular maintenance 描述符必须拥有该物理目标从列 0 开始的独立最终行镜像；不得把多个目标拼接后的全局 projection 直接交给假定局部布局的 insert/delete planner。
- irregular delete 使用旧主键，insert 使用最终新主键；主键变化不能留下旧 MASTER/FULLTEXT 隐表记录。
- 多目标 UPDATE 只能保护 writable target 的 Rowid/selector scan，不能全局关闭 `applyIndices`；只读 FULLTEXT/MATCH source 与显式 FORCE INDEX 仍需按正常 optimizer 路径处理。

### 实施与验证

1. 追踪 final projection、materialized sink、`irregularUpdateMaintenance` 到 delete/insert sub-plan，记录每个目标的局部列切片、old PK 和 new PK。
2. 调整 descriptor/consumer，使非首目标从自身偏移构造局部 projection，并将旧 PK 位置显式传给 delete plan。
3. 将全局 skip 改为只阻止 writable target 上会破坏 selector 的 regular scan rewrite；保留只读 FULLTEXT/MATCH 和 FORCE INDEX rewrite。
4. 增加 typed UT 与 BVT：普通+FULLTEXT、MASTER+FULLTEXT、MASTER PK 连续更新、两个普通 writable target + 只读 MATCH source，并断言基础表、MATCH、隐表和 affected rows。
5. 跑 planner/compile/fulltext owning/dependent tests、相关 BVT、build/vet/SCA、覆盖率与完整 self-review；merge 最新 `mo/main` 后正常 push。

## 回归矩阵

| 场景 | Oracle |
|---|---|
| generated target 位于多目标 UPDATE | 优化成功，基础列与 stored generated 值正确 |
| generated target 位于非首个 target | 两个 target 都写入且 generated 值正确 |
| loser 候选为负值且 winner 为正值 | UPDATE 成功，target=5、两个 sibling 更新、affected rows=3 |
| selected 候选违反 CHECK | 返回 CHECK 错误且事务不产生部分写入 |

# issue #27088 volatile 分支专项回归与 Draft PR

## 当前基线

- worktree：`issue-27088-volatile-main`，工作区干净。
- 当前 HEAD：`dd943ec5063da0c14300068ed135a4e7dc5bb93e`，包含 scalar 与 tuple volatile IN 两笔修复。
- 最新 `mo/main`：`344d852ac3288e391a3ae6be3e3c7108caf2b75d`；merge-base 为
  `853fb9bf9b1d0ea1aa09b3cc7443ad3828ffab4a`。当前分支领先 2 笔、落后 1 笔。
- GitHub issue #27088 仍为 OPEN；当前 head 尚无同源 PR，目标 base 为 `mo/main`。

## 回归契约

- 不变量：一个 SQL 左操作数中的 volatile 表达式，在一次根表达式求值中只能执行一次；tuple 中不同
  volatile 元素必须拥有独立 identity。binder 将 mixed-type `IN`/`NOT IN` 展开为多个比较时不能改变该契约。
- 最小反例：`nextval` 左值被每个展开分支重新执行，使 sequence 推进多次，并使第二个候选观察到新值，最终翻转
  `IN`/`NOT IN` 的布尔结果。
- 公共路径：MySQL binary COM_STMT（`interpolateParams=false`）在单行表的 `WHERE` 中触发运行时参数类型绑定、
  mixed-type IN 展开、scan/filter AuxId 处理、colexec memo executor 与结果返回。
- 独立 oracle：同时检查查询布尔值与 `currval`。前者证明 SQL 语义，后者精确证明不可逆副作用只发生一次。

## 精简测试矩阵

在 `pkg/tests/issues/issue_27088_volatile_test.go` 增加一个表驱动的 embedded-CN 专项测试，只保留四行：

| 左值形态 | 谓词 | 正确结果 | 副作用 oracle |
|---|---|---:|---|
| scalar `nextval` | `IN (?, ?)` | false | `currval = 1` |
| scalar `nextval` | `NOT IN (?, ?)` | true | `currval = 1` |
| 两个独立 `nextval` 的 tuple | `IN ((?, ?), (?, ?))` | false | 两条 sequence 各推进一次 |
| 两个独立 `nextval` 的 tuple | `NOT IN ((?, ?), (?, ?))` | true | 两条 sequence 各推进一次 |

候选值会让未修复实现的第二次求值恰好命中第二个候选，因此不仅 `currval` 失败，布尔结果也会反转。每行在独立
sequence 上运行，不依赖随机数、sleep、执行顺序或概率重试。

现有 `TestMemoExpressionExecutorCachesOncePerRootEvaluation` 继续作为白盒机制 oracle，专项测试不重复其缓存重置断言。
本 PR 不纳入 BETWEEN、IF/CASE 选择行、prepared metadata、DECIMAL 精度边界或完整 #27088 矩阵；这些属于其他拆分分支。

## 实施与验证步骤

1. merge 最新 `mo/main`（按仓库规范不 merge `origin/main`），解决冲突后再开始测试修改。
2. 新增上述专项测试并 `gofmt`；只 stage 明确归属的测试与本 TODO，不改动现有两笔生产修复的语义。
3. 使用 `gwt-add` 建立临时 unfixed baseline worktree，仅移植测试提交，证明同一测试在无修复的最新 main 上因
   布尔结果/`currval` 失败；记录终态后用 `gwt-remove` 删除临时 worktree/branch。
4. 在修复分支运行非空测试选择与聚焦回归：
   - `TestMemoExpressionExecutorCachesOncePerRootEvaluation`
   - 新增的 issue #27088 volatile binary-COM_STMT 专项测试
5. 从完整 diff 推导 owning packages，使用 `mo-cgo-test` 完成 `pkg/sql/colexec`、`pkg/sql/plan`、
   `pkg/tests/issues` 的必要测试，并完成 build、vet、`git diff --check`。
6. 按 `mo-self-review` 对完整 diff 检查 memo state 的创建/共享/Reset/Free 闭环：引用计数恰好归零一次、错误构建释放、
   每次 root Eval 清缓存、跨 query Reset 下推、memo map 生命周期有界。该状态只在单线程 expression executor 树内使用，
   无并发共享证据时不额外制造无意义的 race 压测；若审计发现可信并发路径，再执行自适应 race 门禁。
7. 所有验证证据必须晚于最后一次 merge/语义修改并取得真实退出码。随后检查 status、完整 diff 与 staged diff，提交并普通
   push 到 `origin/issue-27088-volatile-main`，禁止 force push。

## 自审发现与补充方案

- 将专项回归从无表投影加强为单行表 `WHERE` 后，当前修复的 `scalar NOT IN` 仍失败：结果由 true 翻转为 false，且
  `currval` 为 2。其余 scalar `IN`、tuple `IN`、tuple `NOT IN` 均通过。
- 进一步用计划内 memo identity 取证后确认：`NOT IN` 展开的两个比较都正确保留 `AuxId=-1`，但 optimizer 将外层
  `AND` 拆成两个独立 `FilterList` 根表达式。每个根分别创建 memo build context，因此同一 identity 仍各执行一次。
  先前怀疑的存储侧重复求值已被反证，相关实验修改不保留。
- 最小修复：`splitPlanConjunction` 在 `AND` 两侧共享同一负 AuxId 时不跨该边界拆分，保证这些 occurrence 留在同一
  expression root；不共享 identity 的普通 `AND` 仍按原逻辑拆分。嵌套场景只保留必要的最小子树，不阻止无关 conjunct
  继续拆分/下推。
- 增加 plan 层白盒回归，覆盖 shared identity 不拆分、不同 identity 正常拆分、外层无关 conjunct 仍可拆分；专项黑盒
  四例继续作为最终语义 oracle。修改后重新执行全部 owning tests、build、vet、diff-check 与 self-review，旧绿灯不复用。

## Draft PR

- 关联 issue：#27088（提交 PR 前由用户确认）。
- base/head：`matrixorigin/matrixone:main` ← `ck89119:issue-27088-volatile-main`。
- 使用仓库 PR 模板，类型勾选 `BUG`，英文标题与正文；明确说明本 PR 只拆出 volatile IN 单次求值闭环及精简专项回归。
- 创建 Draft PR 后只回读 URL、head/base、Draft 状态与 CI 初始状态；不擅自评论 issue/PR。

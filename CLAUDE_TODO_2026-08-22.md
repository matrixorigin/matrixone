# PR #27448 第三轮 P1：volatile 谓词不得跨越 JOIN 基数边界

## 不变量与根因

- 不变量：包含 volatile 函数的谓词必须在 SQL 语义规定的 JOIN 输入基数上求值；JOIN pushdown 不得通过提前求值、复制或
  谓词推导改变其求值次数。
- 反例：左侧一行、右侧两行的 INNER JOIN 上层过滤 `l.a + nextval('s') in ('0','2')`，正确结果为
  `count=1,currval=2`；当前按 `JoinSideLeft` 下推后得到 `count=0,currval=1`。
- 根因：现有保护只覆盖 `JoinSideNone` 的双边复制，遗漏 `JoinSideLeft`、`JoinSideRight`、INNER JOIN 从 `OnList` 转入
  filters、LEFT JOIN 的右侧 `OnList` 下推，以及 function-scan 特殊下推路径。

## 最小闭环方案

1. 在 JOIN filters 按 side 分流之前统一保留 volatile predicate，使 None/Left/Right/Both 都不能进入 child pushdown；INNER
   JOIN 原始 `OnList` 可留在 JOIN `OnList` 或 JOIN 上层单一 filter root，但不得进入 child。
2. LEFT JOIN 两处 `OnList` 右侧下推仅接受非 volatile 条件，volatile 条件保留在 `OnList`，维持 outer-join 匹配语义。
3. function-scan 特殊路径不得绕过统一保护再次把 volatile filter 加入 child。
4. 增加白盒矩阵覆盖 Left/Right/None、INNER `OnList`、LEFT `OnList`；在现有 embedded SQL 专项增加用户给出的单侧列、多行
   fan-out 黑盒反例，同时断言结果与 `currval`。

## 验证与发布

按 review-comment 例外直接实施。先证明新增黑盒在当前 head 失败，再修复并运行 focused tests、完整专项、由 diff 推导的
owning package 测试/build/vet、diff-check 和完整 self-review。若 `mo/main` 更新则 merge 后重跑最终门禁；正常 push 到现有
分支并更新 Draft PR，不 force push、不擅自回复或 resolve thread。

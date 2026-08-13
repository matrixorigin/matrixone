# PR #26907：收敛 REGEXP binary/nonbinary 合法性契约

## 背景

MySQL 8.4.8 并非拒绝所有 binary REGEXP，而是在隐式类型转换前拒绝静态、非 NULL、非参数标记的 binary/nonbinary 不兼容组合；合法的 binary/binary 与 prepared 参数场景继续按字节执行。

当前 PR #26907 已提供 binary metadata 与字节执行基础，但若缺少绑定期校验，会错误接受静态 binary + text。PR #26724 中只有预转换合法性校验与 3995 错误契约应收敛到本 PR，不合入其完整且重叠的执行实现。

## 不变量与边界

- 绑定/类型解析：REGEXP 的静态字符串语义参数中，一侧为 binary、另一侧为 nonbinary 时返回 MySQL 3995。
- 参数标记不参与静态不兼容判断；其实际 binary 属性由运行时 metadata 决定。
- 裸 NULL 不参与静态不兼容判断；带显式 binary 类型的表达式（包括 `CAST(NULL AS BINARY)`）仍参与判断。
- binary/binary 合法，进入 #26907 的字节执行路径；text/text 进入字符语义路径。
- `regexp_replace` 除 subject/pattern 外，还检查 replacement 与 subject 的兼容性。
- 校验发生在隐式 cast 改写前，避免 binary provenance 被 varchar cast 隐藏。

## 修改步骤

1. 在 `moerr` 增加只对应 MySQL ER_CHARACTER_SET_MISMATCH(3995) 的内部错误码、构造函数与映射测试。
2. 在通用函数绑定路径增加窄范围 REGEXP 参数兼容性校验，不改动其他字符串函数。
3. 增加 binder UT，覆盖：binary+text、text+binary、binary+binary、裸 NULL+text、显式 binary NULL+text、prepared binary 参数+text、数值+binary，以及 `regexp_replace` replacement。
4. 运行 owning package 的 list/build/vet/test；若触发 CGo 依赖，改用 `mo-cgo-test`。
5. 检查完整 diff，执行 self-review，确认未带入 #26724 的重叠执行代码；提交并推送到 `origin/issue-25295-binary-planner`。

## 当前分支说明

开始本轮工作时，专用 worktree 已有未推送但已提交的 `346995432e`（LIKE/REPEAT metadata 修复）以及两次 `mo/main` merge；本轮保留这些既有提交，在其上追加 REGEXP 合法性修复，不改写历史、不 force-push。

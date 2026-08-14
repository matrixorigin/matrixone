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

## PR #26907 后续冲突修复

- 刷新并 merge 最新 `mo/main`（`263567cf1a`），不 rebase、不 force-push。
- 冲突预检显示双方重叠在 MySQL parser 生成文件；以 `mysql_sql.y` 为源，保留本 PR 的 binary introducer 语义与 main 的最新语法，再用仓库生成目标重建 `mysql_sql.go`，不手工维护生成文件。
- 对其他冲突逐一比较 base/ours/theirs，保留双方独立契约，禁止用整文件选边覆盖。
- merge 后重新检查冲突标记、生成文件一致性，并运行 parser、`moerr`、planner 的 list/build/vet/test 以及 REGEXP focused tests。
- 自检完整 merge diff 后提交 merge commit并推送到 `origin/issue-25295-binary-planner`。

## PR review comments：binary-string 完整闭环

### 统一不变量

binary-string provenance 必须由所有合法来源产生，经参数/变量/表达式/物化/格式化重解析传播，并由每个字符串消费者按 MySQL 的函数域规则解释；不能只依赖静态 OID，也不能用批级 true 覆盖 selected-row false。返回类型宽度必须覆盖所有可产生的字节，CTAS 格式化结果必须能在任意 SQL mode 下等价重解析。

### 修复矩阵

1. 补齐 X/0x/B/0b literal 的来源识别与字符串消费者覆盖，包括长度、切片、反转、填充、替换、定位、ORD、选择函数、LIKE/REGEXP；REGEXP 静态合法性同时识别 `Literal.IsBin`。
2. frontend 将 COM_STMT BLOB 与 SQL PREPARE 用户变量的 binary-string provenance 写入 `ParamValue.BinaryString`，并贯通 mock/生产 `TxnCompilerContext`。
3. 为 COALESCE/IFNULL/CASE 等 selected-row 结果提供可表达 false 的行级 provenance，避免静态 VARBINARY/BLOB 覆盖动态选中结果。
4. LEAST/GREATEST 采用所有字符串实参的 binary common domain，而不是最终选中值的类型。
5. 修正 CTAS 宽度：INSERT 计入插入串增量，CONCAT_WS 按实际 separator 次数累计，CHAR 的上界保持可用 VARBINARY 宽度。
6. CTAS 内部 SQL 使用 mode-independent binary literal 格式，并修正 `_binary` 与 `CHAR(... USING ...)` 的 formatter/parser 往返。
7. 为 unary `+` 增加 binary-string identity 语义，同时保持普通文本/数值的一元加号规则。
8. 每项增加 focused UT；对外语义增加真实 SQL/CTAS、SQL PREPARE 和 COM_STMT 回归，覆盖评论中的失败例及相邻 text/binary/NULL 控制组。

# issue-27088 review blocker 修复

## 当前状态

- 工作分支：`issue-27088-main`
- 起始提交：`45509e54045a6172922fd9526e29b68337637d29`
- 目标分支：`mo/main`
- 修复性质：review blocker；记录方案后直接实施、验证、提交并 push。

## 设计与不变量

1. 完整、无小数部分的 36–76 位十进制文本应按其真实整数位宽参与 DECIMAL 公共类型推导，不能虚构 `scale=9`。若实际值与对端 DECIMAL 的公共域可由 Decimal256 精确表达，则结果不得退化为 DOUBLE。
2. prepared plan 的缓存键描述稳定语义类别，而不是二进制协议参数的物理有符号类型。相同的 binary integer 参数连续执行时，首次收敛后必须复用同一计划；signedness 仍只保留在参数值和执行元数据中。
3. 修复保持窄边界：只调整 frontend prepared 参数绑定分类与缓存键生成，不引入新缓存、全局状态或每行开销。

## 实施步骤

1. fetch 最新远端并 merge `mo/main`，检查并解决冲突，保留本分支 prepared DECIMAL 语义与上游变更。
2. 调整完整文本数字分类：对 36–76 位完整整数及等价的全零小数文本保留真实 DECIMAL 宽度/scale；保留指数、前缀数字、超 Decimal256 输入的既有近似/回退契约。
3. 删除 binary integer 初始绑定类型对 `preparedNumericProtocolExact` 的 `T_int64/T_uint64` 覆盖，使初始比较、重建收敛和缓存写入使用同一稳定类别。
4. 增加白盒边界测试：35/36/67/68/76 位整数、`.000...` 等价形式、真实 fractional 控制，以及 signed/unsigned integer 连续执行的计划指针复用。
5. 增加或扩展公开路径回归，覆盖真实 COM_STMT 精确算术/CTAS 可观察类型与结果；确保反例在未修复逻辑上因目标原因失败。
6. 对 owning package 执行 `go list`、build、vet、focused/full test；若触发 CGo 问题，按 `mo-dev` wrapper 分层处理。最后检查 diff、commit 并 push 到 `origin/issue-27088-main`，不 force-push。

## 测试矩阵

| 不变量 | 见证 | 变化维度 | Oracle |
|---|---|---|---|
| 可精确表达的完整大整数不退化为 DOUBLE | 36 位整数与 `DECIMAL(46,10)` 运算 | 无小数、`.0000000000`、真实小数 | 绑定类型宽度/scale；公开 SQL 结果为精确 1 |
| 边界分类不越过 Decimal256 能力 | 35/36/67/68/76/77 位文本 | 位宽边界、指数形式 | 分类 mode 与实际 domain |
| 稳定语义类别可复用 prepared plan | 同一 binary int64/uint64 连续执行两次 | signedness | 第二次 `PreparePlan` 指针不变，结果一致 |

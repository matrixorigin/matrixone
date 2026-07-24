# PR #25335 第九轮 review 修复

## 有效问题

grouping-set DISTINCT 分支用同一 projection 表达式同时构造
`GROUPING(project)` 和 `IF` 的 else 分支，导致 `nextval()` 等易变或有副作用的
表达式被重复执行，改变序列状态乃至 DISTINCT 结果行数。

## 修复方案

1. 在 grouping-set 分支内先用独立 PROJECT 计算一次原始可见 projection。
2. 后续 NULL 物化只引用该 PROJECT 的输出列及其 grouping provenance，不再复制原始
   表达式树；然后维持 UNION ALL 上方的一次全局 DISTINCT。
3. 增加 planner 计划形状断言，确认每个分支只有一个 `nextval()` 求值点；增加
   ROLLUP、CUBE、GROUPING SETS 的真实执行回归，校验输出和 `currval()`。

## 验证

- 运行 planner 定向及全量 CGo UT、真实 sequence BVT、build、覆盖率、静态检查。
- 完成全差异 self-review 后提交并正常推送，不 force push，不修改 GitHub thread。

## PR #25335 第十轮 review 修复

### 有效问题

最新 `mo/main` 已新增 `VECBF16`、`VECF16`、`VECINT8`、`VECUINT8`，但 IFF
只为 `VECF32`、`VECF64` 提供执行分派和公共类型规则。单一窄向量会在 `iffFn`
panic，不同窄向量混用还会按参数顺序选择有损结果类型。

### 修复方案

1. 合并最新 `mo/main`，以精确合并态修复和验证。
2. 为六种向量 OID 补齐 IFF 执行分派，并集中定义向量公共类型规则。
3. 同 OID 要求维度一致；`VECF32` 与 `VECF64` 无损提升为 `VECF64`；其他不同
   OID 若不存在明确无损公共类型则在绑定期拒绝，确保结果与参数顺序无关。
4. 扩展 function/planner 单测覆盖六种同类型、双向混合、维度和边界值；增加
   六种向量 DISTINCT ROLLUP 真实执行回归。

### 验证

- 运行 function/planner 定向及全量 CGo UT、真实 rollup BVT、build、覆盖率、
  静态检查和 `git diff --check`。
- 完成全差异 self-review 后提交并正常推送，不 force push，不修改 GitHub thread。

## PR #25335 第十一轮 review 修复

### 有效问题

新增的 `nextval()` grouping-set BVT 会在并行执行分支中触发 sequence 的既有共享
状态竞态，结果不确定，并可能因 `concurrent map writes` 终止整个服务。即使竞态
不是本 PR 引入，该不稳定用例本身仍会阻塞 PROXY/PESSIMISTIC BVT。

### 修复方案

1. 从 rollup BVT 及 golden 中完整移除 ROLLUP、CUBE、GROUPING SETS 三组
   sequence 创建、`nextval()`、`currval()` 和清理语句。
2. 保留 planner UT，通过静态计划断言验证每个 grouping-set 分支只包含一个
   `nextval()` 表达式节点，不执行 sequence，也不接触共享运行时状态。

### 验证

- 确认 rollup SQL/result 不再包含新增 sequence 名称。
- 串行运行完整 rollup BVT、planner 定向及全量 CGo UT、静态检查和
  `git diff --check`。
- 完成全差异 self-review 后提交并正常推送，不 force push，不修改 GitHub thread。

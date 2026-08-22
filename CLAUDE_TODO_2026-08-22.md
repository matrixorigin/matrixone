# issue #27088 common-type planner 实施方案

## 当前基线与依赖

- 工作分支：`issue-27088-common-type-main`。
- 初始基线：`mo/main@dae2725bef5c81dcb9b5d2fab9a48afe37e2c760`。
- 实施期间 `mo/main` 前进；最终已 fast-forward 并三方恢复本分支改动到
  `mo/main@6329ed97345dd6888fdc43186f1a6d756d72543b`。
- 已合入依赖：PR #27440，merge commit
  `37c75ed209b348c26397c63bf9e0b4f633d3f0a9`。
- CAST 基础层已提供 `function.GetNumericStringPrefix`，以及通过 numeric target
  `Charset=255` 执行 MySQL numeric-prefix DECIMAL 转换的能力。
- 原大 PR #27093 的最终 head
  `1b7e481e8e97629312c852a8bbfe6116a5962b6d` 仅作为反例、测试矩阵和函数级 patch
  来源，不整体 cherry-pick，也不恢复旧 frontend 状态机。
- 本文档完成前未修改生产代码。

## 拆分边界

本分支承担此前拆分方案中的 **prepared comparison/common-type planner**：

- DECIMAL/REAL 公共域推导；
- `COALESCE`、`GREATEST`、`LEAST` 的 prepared 参数公共类型；
- DECIMAL comparison、`BETWEEN`、`IN/NOT IN` 的同域绑定；
- 执行期 plan copy 的参数类型 specialization；
- planner UT、comparison/common-type BVT 和必要的公开 COM_STMT 值语义回归。

本分支明确不承担：

- volatile 表达式 memo/单次求值；
- `SELECT ?`、IF/CASE/UNION/CTAS 的稳定结果列 metadata；
- 跨执行结果类型高水位、`ColDefData` 生命周期和 prepared cache dependency lineage；
- 为 metadata 目的新增 proto 字段或把历史结果类型回灌到本次参数执行域；
- 普通字符串算术、聚合、赋值或用户显式 CAST 的全局语义改写。

如果公开回归暴露必须修改上述后续层才能保证“值正确”，应先停止并重新划分依赖；不能以本
分支名义把 metadata/lifecycle 大块重新带回。

## 根因与不变量

### 根因

prepared 参数在缓存计划中以 TEXT 作为传输表示。当前执行期 specialization 只识别完整数值
文本；`12.5tail`、日期前缀、空串和 `abc` 等 MySQL numeric-prefix 输入仍保留 TEXT，随后让
`COALESCE/GREATEST/LEAST` 或比较器选择字符串域。反向把所有 TEXT 参数都当 DECIMAL 又会破坏
全字符串函数和普通字符串上下文。

### 必须保持的不变量

1. TEXT 是传输表示，不是 numeric common-type 中的语义域；只有被 DECIMAL-aware consumer
   直接消费的参数才按 MySQL numeric-prefix 参与推导。
2. planner 与 executor 必须消费同一个 numeric prefix。planner 只调用
   `function.GetNumericStringPrefix`，不得复制另一套前缀词法；执行 cast 使用 #27440 已合入的
   numeric-prefix 分支。
3. 公共类型必须形成执行闭包：函数返回类型、每个输入 cast、外层比较和物理 vector ABI 必须
   一致，不能只改 resolver 的输入类型。
4. 可由 Decimal256 精确表达的组合不得退化为 DOUBLE；需要的 precision 按
   `max(integral width) + max(scale)` 计算并在 Decimal64/128/256 间提升。超过 76 位物理域时
   统一进入显式 DOUBLE，不能回退词法 TEXT 或饱和为无关 DECIMAL 最大值。
5. FLOAT/DOUBLE 是近似域边界；BOOL、BIT、YEAR、signed/unsigned integer 以各自完整十进制容量
   参与 exact domain，不能由较窄整数 overload 截断 prepared 小数。
6. 全字符串参数保持字符串域。例如 `greatest('10','2')` 仍按 VARCHAR 比较，
   `coalesce('abc','def')` 仍返回 VARCHAR `abc`；不能因为函数名属于 common-type 集合就强制
   DECIMAL。
7. `IN/NOT IN` 的列表按一个公共比较域处理；其中任一真实 FLOAT 可令列表进入 REAL。显式
   `a=x OR a=y` 仍是两个独立比较，不能与 IN 列表机械等同。
8. `BETWEEN` 只有在两个 bound 都是标量常量时才可走原生三参数向量函数；存在逐行 bound 时
   必须保持与 `left >= low AND left <= high` 相同的逐行语义。
9. cached prepared plan 不被本次值污染。运行时类型只作用于 #27316 已有的 isolated execution
   plan copy；NULL、INTEGER、DECIMAL、FLOAT、TEXT 连续执行不能修改缓存模板。
10. 新增扫描只允许在 bind/execute-time specialization 发生，复杂度为输入长度 O(n)，不得引入
    逐行解析、指数值级循环、正则表达式或无界大整数构造。

## 设计方案

### 1. 建立 planner-owned 的上下文类型分类

- 保留 `PreparedRuntimeTypeFromString` 的通用“完整数值文本”职责；新增窄的
  common-type 上下文入口，先调用 `function.GetNumericStringPrefix` 取得 CAST 层认可的 token，再
  对该 token 推导运行时数值域。
- 域推导只分析已返回的 token，不再扫描原始字符串词法；覆盖前导空白、符号、小数点、大小写
  指数、尾随垃圾和空/非数字输入。
- 无 numeric prefix 的非空/空字符串仅在 DECIMAL-aware 上下文表示数值零；离开该上下文仍是
  TEXT。
- 完整值与带 suffix 的等价 prefix 必须得到相同域；36–76 位精确值保留真实
  width/scale，77 位或 Decimal256 无法容纳的非零域使用 DOUBLE。极小但非零且超出可保留 scale
  的值不能提前归零，应进入 DOUBLE。

### 2. 在 common-type binding 边界注入域，而非全局改写 ParamRef

- prepare-time direct ParamRef 只有在 `COALESCE/GREATEST/LEAST` 或目标 comparison 已确认存在
  DECIMAL numeric peer 时，才以稳定的 DECIMAL 参数 envelope 参与 resolver；原 ParamRef 继续
  保持 TEXT，便于后续插入真实 cast。
- execution-time `ResetParamRefRule` 在重绑上述 consumer 时读取本轮实际参数；TEXT 参数通过步骤
  1 得到 contextual runtime domain，native INT/UINT/BOOL/DECIMAL/FLOAT 使用 wire/runtime type。
- 不修改普通 `ABS`、算术、SUM、赋值和显式 CAST 的参数分类；它们继续使用现有 specialization
  规则。
- 所有决定只落在 execution plan copy，cached plan、cached compile 和后续 metadata 状态保持
  原样。

### 3. 统一公共数值域和 cast 物化

- 为 common-type consumer 构造独立的 resolution types；源表达式类型不提前改写。
- resolution 规则：
  - 任一真实 FLOAT/DOUBLE：DOUBLE；
  - 否则任一 DECIMAL：计算所有 DECIMAL/整数/BOOL/BIT/YEAR 的最大整数位和最大 scale，并选择
    Decimal64/128/256；
  - 超出 Decimal256：所有数值 operand 显式 cast 为 DOUBLE；
  - 存在真实 string/ENUM 等字符串边界：沿用各函数现有 string-domain 规则。
- resolver 选定结果后，再为每个参数和 peer 物化与结果类型完全一致的 cast，避免 fixed-width
  executor 读取错误 vector ABI。
- TEXT -> DECIMAL 的 planner-injected cast 在 target type 上设置 numeric-prefix 标志
  `Charset=255`；用户显式 CAST 和普通隐式转换不设置该标志。

### 4. comparison、IN 和 BETWEEN 使用同一分类器

- 二元 DECIMAL/string comparison、`<=>` 和 direct/foldable prepared 参数复用步骤 1 的域分类，
  保留 2^53 以上精度。
- `IN/NOT IN` 先按整个列表确定公共域，再统一物化；FLOAT 控制组进入 REAL，精确 DECIMAL 控制组
  留在 exact domain。
- row-dependent `BETWEEN` 不进入 const-bound executor；必要时在 planner 层展开为两次比较并
  平衡组合，三个 operand 仍复用同一 numeric-prefix 分类规则。

### 5. 滚动升级门禁随首次可达 consumer 落地

#27440 只提供解析入口，当前 `main` 没有生产代码生成 numeric DECIMAL `Charset=255`。本分支会
首次让该标志进入可分发 plan，因此兼容门禁不能推迟到 metadata PR：

- 当前 `MORPCVersion23/24` 已分别被 explicit-text provenance 和 affected-row selector 占用；若
  需要新增能力版本，使用新的 `MORPCVersion25`，不复用旧版本号。
- negotiated protocol < v25 时不得生成新 numeric-prefix plan 标志，维持 legacy planner 行为；
  >= v25 时才启用新语义。
- 增加 version 24/25 边界和远端执行防线测试，证明旧 CN 不会误执行新计划。
- 本分支不新增缓存状态机；现有通用 protocol-version mismatch 机制会在 v24/v25 切换时重建
  prepared plan，本分支只补该既有机制的边界测试。

## 反例驱动测试矩阵

| 不变量 | 公开/内部见证 | 变化维度 | Oracle |
|---|---|---|---|
| 全字符串参数不进入 DECIMAL | `greatest(?,?)`、`least(?,?)`、`coalesce(?,?)` | `10/2`、`abc/def`、NULL | VARCHAR 值与类型；无 numeric-prefix cast |
| DECIMAL peer 启用 contextual prefix | `coalesce(?, DECIMAL)` | `abc`、空白、日期、`12.5tail` | 值分别按 0/前缀转换；普通字符串上下文不变 |
| 2^53 以上保持 exact | DECIMAL128 列与参数 | `9007199254740992.0001` 相邻值 | 精确行集，不出现 DOUBLE 假相等 |
| 等价 spelling 同域 | common-type 函数 | full、exponent、suffix prefix | 相同结果类型和值 |
| Decimal256 边界正确 | 36/38/39/65/76/77 位及 scale 30/31/38 | 前导零、整数/小数、正负号 | <=76 精确；超域统一 DOUBLE；不饱和 |
| 极小非零不被归零 | `1e-100` 与等价小数 | 零、可表示小数 | 非零进入 DOUBLE；零仍为零 |
| FLOAT 是近似域边界 | DECIMAL + FLOAT + 参数 | COALESCE 与 GREATEST/LEAST | 遵循各函数 MySQL domain；vector ABI 一致 |
| exact 非 DECIMAL peer 完整计宽 | BOOL/BIT/YEAR/int/uint + DECIMAL | 最大 unsigned、fractional 参数 | 不截断、不溢出到窄整数 |
| IN 使用列表公共域 | DECIMAL `IN(?,?)` | exact+FLOAT、顺序、NOT IN | 与 MySQL 行集一致；与显式 OR 的差异保留 |
| BETWEEN 保持逐行 bound | `? BETWEEN d AND ?`、`? BETWEEN ? AND d` | SELECT/UPDATE、NOT BETWEEN | 与二元比较展开的行集/影响行数一致 |
| execution specialization 不污染模板 | 同一 statement 连续执行 | TEXT/INT/DECIMAL/FLOAT/NULL | cached plan 指针/内容不变，本轮 execution plan 正确 |
| rolling upgrade 安全 | 同一 plan 在 v24/v25 | 单 CN/远端 CN | v24 不生成/拒绝新标志；v25 正确执行 |

测试只保留能增加独立语义维度或独立 oracle 的行；不从旧 #27093 机械复制数百行重复矩阵。

## 预计文件归属

最终以实际最小闭包为准，优先限制在：

- `pkg/sql/plan/base_binder.go` 及 planner 单测；
- `pkg/sql/plan/utils.go` / `visit_plan_rule.go` 及 execute-time specialization 单测；
- `pkg/sql/plan/function/operator_between.go` 及单测（若 row-bound 修复仍需要）；
- comparison/common-type 的精简 BVT/issue UT；
- `pkg/defines/const.go` 和最小远端兼容检查（仅当 v25 门禁确需新增）。

不直接修改生成 protobuf；若最终设计证明必须新增 proto 源字段，应停止并先向用户说明它已越过本
拆分边界。

## 实施步骤

1. 先以当前 `main` 写出失败的 planner/execute-copy 白盒反例，证明旧逻辑因 TEXT 传输域或错误
   common domain 失败；同时锁定最近邻 string/FLOAT 控制组。
2. 实现 contextual numeric-prefix runtime domain helper，复用
   `function.GetNumericStringPrefix`，补齐 O(n) 和 36–77 位/指数/空值边界单测。
3. 在 common-type binder 和 execute-time visitor 中接入该 helper，统一 resolution type 与最终 cast
   物化；确认 cached plan 未被修改。
4. 收敛 comparison、IN/NOT IN、BETWEEN 到同一域规则；只修复由本不变量导出的路径，不恢复旧
   PR 的大块通用框架。
5. 实现并验证 v25 激活边界；若现有协议设施不能在不引入 lifecycle 状态机的情况下安全门控，
   暂停并报告 blocker。
6. 增加精简公开回归：真实 COM_STMT 与 BVT 检查值/行集/物理结果类型；稳定 ColDef、高水位和
   direct projection 留给 metadata 分支。
7. 最终语义编辑后，从 diff 推导 owning packages，执行 gofmt、go list/build/vet、focused/full
   CGo test、必要 race、干净实例 mo-tester、changed-code coverage >=75%、`git diff --check`。
8. 使用 `mo-self-review` 检查完整 diff、错误/NULL/reuse/remote 路径和热路径成本；通过后再向用户
   汇报。未经新的明确授权不 commit/push/创建 PR，也不评论原 issue/PR。

## Review checkpoint

用户已明确 `go ahead`，本方案 review checkpoint 已通过并进入实施。

## 实施与验证结果

- 已在 `mo/main@6329ed97345dd6888fdc43186f1a6d756d72543b` 完成 common-type planner、
  execute-time isolated plan specialization、逐行 BETWEEN bound、MORPC v25 门禁及精简黑盒回归；
  2026-08-22 收尾时再次 fetch，`mo/main` 仍为该提交。
- 最终 diff 推导出的 7 个 owning packages 均在 `GOWORK=off`、`-mod=readonly` 和隔离
  `GOCACHE/GOTMPDIR` 下完成 `go list`；有生产 Go 文件的 6 个 package 通过 `go build`，全部 7 个
  package 通过 `go vet`。
- 通过仓库 `mo-cgo-test` 门禁运行 `pkg/defines`、`pkg/pb/plan`、`pkg/sql/plan/function`、
  `pkg/sql/plan`、`pkg/frontend` owning package 全量测试；`pkg/sql/compile` 的 v25 远端门禁定向测试
  通过。focused test 先经 `-list` 确认非空；上述测试 profile 覆盖新增/修改可执行语句
  406/495（82.0%）。
- 嵌入式集群黑盒测试通过真实 go-sql-driver COM_STMT 与 SQL PREPARE，覆盖 2^53 以上精确比较、
  IN 列表、逐行 BETWEEN bound、DECIMAL common-value 函数和全字符串控制组。
- owning packages 的 `go vet` 与 `git diff --check` 通过。改动不引入 goroutine、锁、channel 或共享
  可变状态，因此不增加 race 专项门禁。
- `pkg/sql/compile` 全量测试仍有 3 个 parquet 空资源 fanout 用例失败；在不包含本分支改动的
  `dae2725bef` 基线上逐项复现同一失败，且相关 `scope.go/scope_test.go` 未被本分支修改，判定为
  现存基线问题。
- 已完成 `mo-self-review` 和 unhappy-path Q1-Q3 审计：execution plan copy 与 cached plan/compile
  所有权分离；无新增等待关系；扫描和公共域推导均为输入规模 O(n)，无跨执行累积。
- 用户已用 `pr` 明确授权提交、正常 push，并以 issue #27088 创建 Draft PR。

# Issue #27218 工作计划

## 目标

修复二进制字符串表达式在返回类型、宽度、字符集、协议元数据和 CTAS 物化之间不一致的问题，确保合法结果超过 `VARBINARY` 上限时提升为 `BLOB`，不截断数据，也不低报类型域。

## 当前基线

- Base：`mo/main`
- 基线提交：`4661c00ce0`
- Worktree：`.worktrees/issue-27218-main`
- Branch：`issue-27218-main`
- Issue：<https://github.com/matrixorigin/matrixone/issues/27218>

## 工作步骤

1. **建立现状与变更地图**
   - 阅读 #26907 及相关 binary-string 改动，定位类型推导、函数注册、运行时求值、协议输出和 CTAS 建表各自的 owner。
   - 盘点 `CONVERT(... USING binary)`、`CHAR`、`CONCAT[_WS]`、`REPLACE`、`LPAD/RPAD`、`INSERT`、`REPEAT`、`REGEXP_*` 的现有宽度规则和测试。
   - 用最小 SQL/typed test 复现固定宽度截断、默认 `CHAR` 域不一致、扩张函数低报宽度以及 CTAS 类型不一致等 witness。

2. **形成设计并确认边界**
   - 定义固定宽度、有界宽度、literal-dependent、未知宽度四类输入的 checked width arithmetic。
   - 明确 `VARBINARY` 到 `BLOB` 的唯一 promotion owner，以及 binary charset/collation 的传播规则。
   - 明确直接表达式、prepared execution、formatter/reparse、协议 metadata 和 CTAS 共同消费的语义契约。
   - 检查是否需要仓库正式 design 文档；若达到 feature/major-refactor gate，先提交设计文档供 review，再进入实现。

3. **最小完整实现**
   - 优先修复公共类型推导/宽度提升边界，避免逐函数打补丁。
   - 补齐确实无法复用公共逻辑的函数规则和 consumer。
   - 保证大结果路径使用受控分配和内存记账，不引入无界中间副本。

4. **测试闭环**
   - Typed UT：覆盖 0/1、上限减一、上限、上限加一、70,000 bytes、NULL、未知宽度、溢出及分配失败。
   - SQL/BVT：覆盖直接执行、prepared、derived/view（支持范围内）、CTAS、`DESC`、`information_schema.columns`。
   - 协议测试：核对 text/binary protocol 的类型、长度、charset 与运行时值。
   - 对每类扩张函数保留最小代表性控制组，避免重复的大数据 fixture。

5. **验证与交付**
   - 先跑定向 UT，再跑 owning package；涉及 BVT 时生成并验证 result。
   - 执行完整 change-map/self-review，核对无意外文件和未关闭 unhappy path。
   - push 前汇报验证证据；创建 PR 前按约定使用 issue #27218 和仓库模板，PR 为 draft。

## 风险与待确认点

- Issue 范围跨 planner、function runtime、frontend protocol 和 CTAS，可能触发正式设计门禁；实现前先根据代码现状拆分 closure，避免一次性大改。
- `REGEXP_*` 与 #25299 的兼容语义有边界重叠，本任务只处理返回域/宽度，不扩展到 regexp source compatibility。
- 若发现 #27218 依赖尚未进入 `main` 的前置 PR，将先列出依赖和影响，不擅自搬入依赖代码。

## 完成记录

- 设计审批：`ba4592e694a35b20af9a211d98db95a545c8585d`，用户明确批准。
- 实现：checked Known/Unknown width、VARBINARY→BLOB promotion、CONVERT pre-cast width、默认/显式 CHAR domain、扩张函数 metadata、CTAS 与 information_schema binary charset。
- UT：`pkg/sql/plan/function`、`pkg/sql/plan`、MySQL parser、`pkg/util/sysview` owning package 均通过。
- BVT：`dtype/binary_string_result_domain.test` 在 clean ready instance 生成 result 后 normal comparison 连续运行两次，均为 12/12 passed；覆盖 direct protocol metadata、70,000-byte runtime/CTAS、DESC 和 information_schema。
- 最终 self-review：R2 width/runtime/parser closure 与 R3 catalog-view consumer closure 已逐项核对；generated parser 可重复生成；无 concurrency、wait、background state 或新持久化/wire schema；无未解决 blocker。

## Review findings 修复计划

1. 为 `CONVERT ... USING` 增加保留原字符串 OID/width/charset 的 overload resolution，避免 BLOB/VARBINARY 在执行前转成 VARCHAR；非字符串仍按现有 conversion cast。
2. 为 REPEAT/REPLACE/INSERT/REGEXP/LPAD/RPAD 增加 binary string overload/checker，使真实 BINARY/VARBINARY/BLOB 与 binary-charset VARCHAR 使用同一 binary return-domain callback，且不经 text cast。
3. 将 MAKE_SET、EXPORT_SET、QUOTE 从 selected/source-preserving width helper 分离：分别计算 contributor+separator 上界、64-slot 上界和 checked quote escaping 上界。
4. LPAD/RPAD 以 byte budget 预检最终编码大小，并避免未记账的大 Go-heap intermediate；补 allocation/boundary 测试。
5. 在 v4_0_6 tenant upgrade list 末尾追加 information_schema.COLUMNS refresh entry和 offset/upgrade 测试，覆盖已升级租户。
6. 跑 owning package、upgrade tests、BVT、最终 self-review，直接 push review fix 并更新 PR。

## Review findings 完成记录

- CONVERT 与扩张函数使用 string-domain-aware checker，真实 BLOB/VARBINARY/BINARY 不再经过 VARCHAR；typed probe 与 70,000-byte BVT 均验证无截断。
- MAKE_SET、EXPORT_SET、QUOTE 使用独立 checked width helper；BVT metadata 分别为 VARCHAR(5)、VARCHAR(127)、VARCHAR(2)。
- LPAD/RPAD 在构造 Go result 前计算 UTF-8 byte size并预留 MPool area；emoji 上限和低-cap allocation rejection UT 通过。
- v4_0_6 tenant upgrade list 末尾追加 COLUMNS binary charset refresh entry，offset/entry UT 通过。
- 修复 CONVERT USING formatter/reparse，CTAS from BLOB 成功保留 70,000 bytes并物化为 BLOB。
- 更新后的 BVT 在 clean-ready instance normal mode 连续两次 21/21 passed；owning packages 全量通过。

## 第二轮 review findings 修复计划

1. 为 QUOTE 增加 binary-aware overload matching 与逐 byte quoting，实现无效 UTF-8 不替换，并补真实 BINARY/VARBINARY/BLOB 探针及 width 边界。
2. 为 REPEAT、REPLACE、INSERT、REGEXP_REPLACE/REGEXP_SUBSTR 及 LPAD/RPAD 建立 allocation-free checked result-size admission；删除结果长度相关的 Go-heap 中间 string/[]rune，直接写入已由 MPool admission 的 result area，覆盖大输入与扩张拒绝。
3. 在 binder 读取已证明的 count/target literal，按批准设计细化 REPEAT、LPAD、RPAD 的 binary 返回宽度；动态值维持 Unknown/BLOB，并更正 BVT metadata 期望。
4. 跑 typed probes、allocation/accounting UT、owning packages、BVT 与最终 self-review；按 review-fix 例外直接 commit/push 并更新 PR。

## 第二轮完成记录

- QUOTE 真实 VARBINARY/BLOB 不再 cast，并按 byte 保留无效 UTF-8；静态上界为 `2*n+2`。
- REPEAT/REPLACE/INSERT/REGEXP 与 PAD 在构造扩张结果前执行 checked MPool admission；PAD preflight 改为 allocation-free UTF-8 scan；补 low-cap rejection UT。
- binder 对 REPEAT count literal 与 LPAD/RPAD target literal 细化 VARBINARY width；动态表达式维持 BLOB。
- BVT 更新为 23/23 passed；`repeat(X'61',2)` 和 `lpad(...)` 均报告 VARBINARY(2)，动态 count 报告 BLOB。

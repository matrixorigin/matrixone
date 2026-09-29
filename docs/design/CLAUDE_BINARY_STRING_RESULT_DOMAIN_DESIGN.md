# Binary-string 返回域与 CTAS 宽度契约设计

- 状态：Approved
- Owning issue：[matrixorigin/matrixone#27218](https://github.com/matrixorigin/matrixone/issues/27218)
- 依赖契约：`CLAUDE_BINARY_STRING_SEMANTIC_MODEL.md`
- 设计 owner：SQL planner/function registry；CTAS、frontend protocol 为 consumer
- 设计门禁：跨 planner、runtime function、catalog materialization 与 client metadata，命中两个以上 ownership boundary 和 public compatibility contract

## 1. 问题与不变量

当前返回类型 callback 存在三类互相独立的问题：

1. `CONVERT(... USING binary)` 主要从目标 charset 构造普通 `VARCHAR`，未按源类型的最大可打印字节数描述结果；固定类型可能被低报并在后续 materialization 中截断。
2. `CONCAT` 等函数当前只要发现 binary 输入就直接返回默认 `BLOB`，而 text 路径通常返回默认最大 `VARCHAR`；两者都没有表达有界输入、literal-dependent 扩张和未知宽度的差异。
3. expression、wire metadata 与 CTAS 各自解释 OID/width/charset，导致 direct execution 的值域与 CTAS 声明列可能不同。

核心不变量：**声明的静态结果类型必须包含表达式能够合法产生的每个值。expression、runtime value、text/binary protocol metadata 和 CTAS column 必须描述同一个静态结果域。** 若 binary 结果的证明上界超过 `MaxVarBinaryLen`，结果必须晋升 `BLOB`；不得将 width 饱和到 65535 后仍声明 `VARBINARY`，也不得为适配声明类型而截断 runtime value。

非目标：不改变函数的 byte-position compatibility 语义；不改变 #27214 定义的 runtime domain/source/literal-form；不新增持久化格式或 wire 字段。REGEXP result-domain 与 runtime admission 不在本 PR 的 closure 中，由后续 linked fix 统一处理。

## 2. 统一宽度模型

planner/function registry 是静态返回域的唯一 owner。所有受影响函数调用同一组纯 helper，结果为：

```text
Bound = Known(bytes) | Unknown
Domain = Text | Binary
```

### 2.1 输入分类

- **固定类型**：使用格式化实现可产生的最大字节数，而不是 storage size。包括 bool、signed/unsigned integer、float、decimal、date/time/datetime/timestamp/year、UUID。
- **有界字符串**：`CHAR/VARCHAR` 的 width 是字符数；转为 binary bytes 时按 charset 最大 bytes/character 做 checked multiplication。`BINARY/VARBINARY` width 直接是 bytes。
- **literal-dependent**：只有 binder 已证明为常量且函数语义依赖该值时，才使用 literal 值，例如 `REPEAT` count、`LPAD/RPAD` target length。非负性和乘加必须 checked。
- **未知宽度**：无声明上界的 `TEXT/BLOB`、动态扩张参数、无法证明的 legacy/zero width。未知不是 0，也不是 65535。

所有加法、乘法和 charset 扩张使用 checked `uint64` 算术；overflow 转为 `Unknown`，不得 wrap 或 cap。

### 2.2 OID 与 width 选择

| Domain / bound | 返回类型 |
| --- | --- |
| Binary，Known `<= MaxVarBinaryLen` | `VARBINARY(bound)`，binary charset |
| Binary，Known `> MaxVarBinaryLen` | `BLOB`，保留已知 width 仅在现有 Type/metadata contract 可无歧义表达时，否则 width 0 |
| Binary，Unknown | `BLOB(0)`，binary charset |
| Text，Known `<= MaxVarcharLen`（字符上界可证明） | `VARCHAR(bound)`，合并后的 text charset/collation |
| Text，超过上限或 Unknown | `TEXT(0)`，合并后的 text charset/collation |

BLOB/TEXT 的 `width == 0` 统一表示 unbounded/unknown；不得把默认 `ToType()` 的 65535 当作“未知”。已有 persisted TINY/MEDIUM/LONG text marker 只在 owner 明确识别时视为有界。

## 3. 函数规则

### 3.1 `CONVERT(expr USING binary)`

- 结果 domain 为 Binary。
- 字符串输入按其 declared payload bound；非字符串输入按统一 maximum formatted-byte table。
- `T_any`、无界文本/二进制及无法证明输入为 Unknown，返回 BLOB。
- 其他 `USING` charset 返回 Text；bound 按目标 charset 的字符/字节规则换算，不从 synthetic charset 参数继承错误的 source width。

### 3.2 `CHAR(...)`

- 默认 `CHAR(...)` 与显式 `USING binary` 返回 Binary；`USING utf8mb4` 返回 Text。
- 每个整数参数最多贡献其实现允许产生的字节数，按参数个数 checked sum；大 arity 超过上限时晋升 BLOB/TEXT。
- charset、OID、protocol flags 与 runtime domain 必须一致，不能以 runtime provenance 补救错误静态 OID。

### 3.3 扩张/变换函数

- `CONCAT`：所有实际 value 参数 bound 之和；任一 Unknown 即 Unknown。任一有效 binary contributor 使静态 domain 为 Binary。
- `CONCAT_WS`：value 参数之和，加 `separator * max(non-NULL value count-1)`；separator 是 contributor。
- `REPLACE`：最坏替换次数不超过 source bound；使用 `source * max(1, replacement)` 作为安全上界。
- `LPAD/RPAD`：常量非负 target length 给出 bound；动态 target 为 Unknown。binary target 以 bytes，text target 以 characters 并做 charset byte bound。
- `INSERT`：可证明 source/replacement bounds 时使用 checked `source + replacement` 作为安全上界；否则 Unknown。
- `REPEAT`：常量非负 count 使用 checked `source * count`；动态 count 为 Unknown。

控制参数（position、occurrence、match type、length count）不参与 domain 合并，只参与可证明 width 的计算。

## 4. CTAS 与 metadata 消费规则

1. binder 生成的 expression `plan.Type` 是 authoritative contract。
2. CTAS 直接复制该 type 的 OID、width、charset/collation 和 nullability；只允许 catalog 对同一 domain 做 lossless canonicalization，禁止重新根据 observed row value 缩窄或把 BLOB 降为 VARBINARY。
3. `DESC` 与 `information_schema.columns` 从同一 catalog type 映射；binary charset/collation 不经过 text fallback。
4. frontend text/binary protocol 从 expression output type 生成 field metadata；runtime row provenance 只处理逐行语义，不改写声明 OID/width。
5. formatter/reparse 和 prepared execution 必须保留同一 callback 输入类型与常量证明；若执行时参数值变化，只能使用 prepare-time 的安全 Unknown/上界，不按首轮值缩窄缓存计划。

本设计不改变 catalog/wire schema，因此 mixed-version 行为沿用现有 type encoding；旧 client 仍可读取 BLOB/TEXT 标准类型。回滚仅恢复旧推导逻辑，不需要数据迁移；已经 CTAS 为 BLOB 的列仍是合法超集。

## 5. 资源、失败和性能

- 宽度推导为 bind-time O(number of arguments)，不扫描 runtime rows。
- helper 不分配与结果长度成比例的 buffer；仅读取 type 和已存在的 constant literal。
- runtime 实现保留现有 MPool/result append owner；本任务不增加中间完整副本。
- checked arithmetic overflow 产生 Unknown/BLOB/TEXT，而不是错误或 panic；非法负 literal 依函数现有 runtime contract处理。
- allocation failure 必须在 result append 前返回，已有 FunctionResult/MPool 回滚与 accounting contract 不得弱化。

## 6. 替代方案

1. **维持逐函数 callback（拒绝）**：容易让同一扩张规则在 CONCAT/REPEAT/CTAS 漂移，无法形成唯一 promotion owner。
2. **一律返回 BLOB/TEXT（拒绝）**：正确但过度放宽 metadata，破坏 bounded type、client compatibility、索引/DDL 能力和优化信息。
3. **把 width 饱和到 65535（拒绝）**：声明域不能容纳合法值，直接违反不变量。
4. **统一 Bound helper + lossless promotion（选定）**：bind-time 成本低，保留可证明的最窄安全域，并让所有 consumer 使用同一 type。

## 7. 验证地图

| 契约 | 最小证明 |
| --- | --- |
| checked add/multiply、Known/Unknown、promotion | helper table UT：0/1、65534/65535/65536、70000、overflow |
| fixed source conversion | overload/binder UT：bool、year、temporal、UUID、integer、decimal、unknown |
| CHAR domain/arity | parser+binder/function UT，默认/binary/utf8mb4 与大 arity |
| expanding functions | return callback table UT；每类一个 runtime value control，避免重复大 fixture |
| formatter/prepared | planner round-trip 与 prepared rebind UT，参数值变化不得缩窄 |
| CTAS/catalog | planner CTAS UT + 最小 BVT，核对 value length、DESC、information_schema |
| protocol metadata | frontend field metadata typed UT；必要时真实 client BVT |
| 70000-byte value | 最小 public SQL BVT，核对 `length` 和 CTAS round-trip，不输出完整 payload |
| allocation/accounting | focused reject-next-allocation UT，确认无泄漏/无额外完整副本 |

BVT 数据规模固定为单行、单表达式；边界由 `repeat`/literal 构造，不创建大表。无并发、restart、upgrade 专项：本设计不增加共享状态、持久化格式或分布式协议。

## 8. 审批记录

- 设计 revision：`ba4592e694a35b20af9a211d98db95a545c8585d`
- 审批者：user（会话内明确回复 “go ahead”）
- 决定：Approved，2026-08-29
- superseded 实现偏差：曾为保持 text-only compatibility，让动态 text 扩张函数继续返回 VARCHAR；但 runtime 仍可合法产生超过 65535 的值，违反静态结果域必须包含合法 runtime value 的核心不变量。
- 修订决定：按 user 2026-08-30 review correction，未知或可超限的动态 text 扩张结果晋升 TEXT；该决定覆盖上述 VARCHAR 偏差，并要求 protocol、CTAS/catalog 与 runtime 使用同一 lossless 静态结果域。

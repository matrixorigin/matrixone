# 普通 binary-string 函数与 charset/collation 语义设计

- 状态：Approved
- Owning issue：[matrixorigin/matrixone#27216](https://github.com/matrixorigin/matrixone/issues/27216)
- 依赖契约：`CLAUDE_BINARY_STRING_SEMANTIC_MODEL.md`
- 已合并前置：#27215 / PR #27467（provenance/transport）、#27218 / PR #27841（result-domain/CTAS）
- 设计 owner：function registry（静态参数与返回域）、scalar executor（逐行算法与输出 provenance）、frontend/catalog metadata consumer
- 能力边界：只使用现有 `binary`、`utf8`、`utf8mb4` 及已支持 collation/兼容别名；不增加名称、ID、编码转换、排序权重表或 UCA 实现

## 1. 问题与不变量

当前普通字符串函数存在四类彼此相关但 owner 不同的缺口：

1. `CHAR_LENGTH`、切片、反转、位置、`ORD`、padding、`INSERT`、case conversion 和 `LIKE` 的 executor 主要固定使用 rune/UTF-8 语义，无法消费 `RuntimeStringDomain`，导致 binary literal、BINARY/VARBINARY/BLOB、user variable、prepared parameter 和 mixed-row vector 得到错误结果；无效 UTF-8 还会被 `RuneError` 重编码。
2. 部分 resolver 仍把 binary family 隐式 cast 为 `VARCHAR`，在 executor 之前擦除静态 binary domain；另一些返回 callback 把 replacement/pad 等辅助参数错误地当成结果 domain owner。
3. 普通变换函数尚未把 subject/contributor 的逐行 domain 传给结果，下一层 `CHAR_LENGTH` 等 consumer 会把同一 binary value 静默解释成 text。
4. `CHARSET()`/`COLLATION()` 忽略参数而返回 session 设置；`information_schema.columns` 仍从旧字段解释 charset，可能与 expression type、protocol metadata 和 CTAS catalog type 不一致。

核心不变量：

- **静态不变量**：bind-time `types.Type` 是结果 OID、width、charset/collation 和 client/materialization metadata 的唯一 owner；runtime sidecar 不改写静态类型。
- **逐行不变量**：每个非 NULL row 的 effective domain 由 `Vector.GetIsBinaryStringAt(row)` 决定。Binary row 只按原始 bytes 执行；text row 按现有 UTF-8 character 语义执行。不得以第 0 行或任意辅助参数决定整批算法。
- **角色不变量**：只有矩阵中标为 domain selector 的参数决定 subject 的 byte/text 模式；needle、delimiter、replacement、pad、position/count 等 auxiliary/control 参数不能独立切换 subject。
- **provenance 不变量**：subject-preserving 结果按 subject effective domain 归一化为相对静态结果类型的 `RuntimeStringDomain`；真正拼接多个值的函数才使用 contributing-values 规则。
- **byte safety**：binary path 不 decode、repair 或 re-encode UTF-8。`0xff` 等任意 bytes 必须逐字节计数、定位、切片、反转和匹配。

## 2. MySQL 8.4.8 oracle 与兼容边界

本设计以本机 MySQL 8.4.8 的最小 differential matrix 为公开兼容 oracle。已确认：

- `LENGTH`/`OCTET_LENGTH` 总是 byte count；`BIT_LENGTH` 为 byte count × 8；`CHAR_LENGTH` 对 text 数字符、对 binary 数 bytes。
- `SUBSTRING`、`LEFT/RIGHT`、`REVERSE`、`LOCATE/INSTR/POSITION`、`INSERT`、`LPAD/RPAD` 的 source/haystack 是 domain selector。Binary needle、replacement 或 pad 本身不改变 text subject 的结果域。
- `LOWER/UPPER` 保留 source domain；binary payload 原样返回。
- `ORD('你') = 0xE4BDA0 = 14990752`，而 `ORD(_binary 0xE4BDA0) = 228`。Text 多字节值按首字符编码的 big-endian byte sequence 组装。
- `REPLACE` 与 `SUBSTRING_INDEX` 的匹配本身是 exact sequence；其结果域来自 source。
- text `utf8mb4_general_ci` 的 `LOCATE/INSTR` 保留当前 case-insensitive 路径，`utf8mb4_bin` 与 binary domain 使用 exact comparison；本任务不补充新的 accent/UCA weight 语义。
- `CHARSET`/`COLLATION` 报告表达式静态 metadata。MySQL 的 `COALESCE(text,binary)` 可逐行用 text/binary 算法，但 `CHARSET` 仍对所有行报告静态 common domain；这验证了静态 metadata 与逐行算法必须分离。

MySQL 会在部分 text-subject + invalid binary auxiliary coercion 上执行 charset conversion 并报错。本任务不新增 encoding conversion；只保留当前合法 UTF-8 auxiliary compatibility，并以“binary subject 的 invalid UTF-8 绝不修复/截断”为 closure 条件。完整 REGEXP legality、byte engine 与 match-memory 仍属于 #27217。

## 3. 函数与参数角色矩阵

“输出域”同时指静态 domain owner 和需要传播的逐行 provenance owner。别名共享同一行规则。

| 函数 | subject | auxiliary / delimiter | control | domain selector | text 算法 | binary 算法 | 输出域 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `LENGTH` / `OCTET_LENGTH` | arg0 | — | — | 无（恒定 bytes） | `len(bytes)` | `len(bytes)` | numeric |
| `BIT_LENGTH` | arg0 | — | — | 无（恒定 bytes） | `len(bytes)*8` | 同左 | numeric |
| `CHAR_LENGTH` / `CHARACTER_LENGTH` | arg0 | — | — | arg0 | UTF-8 code-point count | byte count | numeric |
| `ORD` | arg0 | — | — | arg0 | 首字符编码 bytes 按 big-endian 组装 | 首 byte | numeric |
| `LOCATE(needle,haystack[,start])` / `POSITION` | haystack | needle | start | haystack | character start/result；按已有 text collation identity 选择 CI/exact | byte start/result，exact | numeric |
| `INSTR(haystack,needle)` | haystack | needle | — | haystack | character result；同上 | byte result，exact | numeric |
| `SUBSTRING` / `SUBSTR` / `MID` | arg0 | — | start, length | arg0 | character slice | byte slice | arg0 |
| `LEFT` / `RIGHT` | arg0 | — | length | arg0 | character slice | byte slice | arg0 |
| `REVERSE` | arg0 | — | — | arg0 | rune reverse | byte reverse | arg0 |
| `LOWER` / `UPPER` | arg0 | — | — | arg0 | 现有 Unicode case map | payload unchanged | arg0 |
| `LTRIM` / `RTRIM` | arg0 | implicit ASCII space | — | arg0 | trim ASCII space | trim byte `0x20` | arg0 |
| `TRIM` | source（registry arg2） | remstr（arg1） | BOTH/LEADING/TRAILING（arg0） | source | exact boundary sequence | exact boundary bytes | source |
| `REPLACE(source,from,to)` | source | from=delimiter，to=payload auxiliary | — | source | exact sequence replace | 同左，禁止 decode | source；to 不切换 domain |
| `INSERT(source,pos,len,newstr)` | source | newstr | pos, len | source | character position/range | byte position/range | source；newstr 不切换 domain |
| `LPAD/RPAD(source,target,pad)` | source | pad | target | source | source/pad 均按 characters 截断/补齐 | source/pad 均按 bytes 截断/补齐 | source；pad 不切换 domain |
| `SUBSTRING_INDEX(source,delimiter,count)` | source | delimiter | count | source | exact delimiter sequence | exact delimiter bytes | source |
| `SPLIT_PART(source,delimiter,index)` | source | delimiter | index | source | MO 扩展：exact delimiter sequence | exact delimiter bytes | source |
| `REPEAT(source,count)` | source | — | count | source | byte-preserving copy | byte-preserving copy | source |
| `LIKE(value,pattern[,escape])` | value | pattern；escape 为 control delimiter | — | value | `_` 匹配一个字符，`%` 匹配字符序列 | `_` 匹配一个 byte，`%` 匹配 byte 序列 | boolean |
| `CONCAT` | 所有非 NULL value | — | — | contributing values | 原样拼接 | 原样拼接 | 静态任一 binary contributor → binary；runtime binary contributor → binary override |
| `CONCAT_WS` | separator 与实际非 NULL value | — | NULL skipping | contributing values | 原样拼接 | 原样拼接 | 同上 |
| `CHARSET` / `COLLATION` | arg0 type | — | — | **静态 type，不读 row override** | 返回 canonical existing name | `binary` / `binary` | text name |

本次不改变以下固定-domain encoder/constructor 的语义：`HEX/UNHEX`、`TO_BASE64/FROM_BASE64`、`QUOTE`、crypto/compression、JSON/geometry。它们的结果 domain 由函数定义而非输入 subject 决定。`ELT/MAKE_SET/EXPORT_SET/LEAST/GREATEST` 的静态 common/selected result-domain 已由 #27218 覆盖；本次不重新定义其选择/比较算法。REGEXP 全部排除。

## 4. Resolver、返回类型与 collation 规则

1. 所有矩阵中的 generic varlena consumer 使用 `stringDomainFixedTypeMatch`，保留 CHAR/VARCHAR/TEXT/BINARY/VARBINARY/BLOB 的原 OID、width 和 charset；control 参数仍走普通 fixed cast。
2. subject-derived 返回 callback 只从 subject 推导静态 domain：
   - `REPLACE` 的 `from/to`、`INSERT` 的 `newstr`、`LPAD/RPAD` 的 `pad` 不得使 text source 变成 binary。
   - bound 计算继续复用 #27218 checked arithmetic 和 BLOB/TEXT promotion。改变 selector 不允许恢复 cap/truncation，也不增加完整中间副本。
   - binary `INSERT` 不再经过 RuneError 重编码，因此移除旧 bound 中的三倍扩张补偿；安全上界仍为 source + replacement。
3. `caseConversionReturnType` 对 binary subject 返回同域 `VARBINARY/BLOB`；text 保留 resolved charset。
4. `CONCAT/CONCAT_WS` 保持 #27218 的 contributing-values 静态 domain 与 width 规则，只补 runtime provenance。
5. `LOCATE/INSTR` 的 text comparison identity：`CharsetUTF8` 复用当前 general-ci case-fold path；`CharsetUTF8MB4Bin`、`CharsetLegacy` 使用 exact text matching；binary effective row 一律 exact bytes。这里不增加 weight table。
6. `CHARSET/COLLATION` canonical mapping：

| `types.Type` identity | `CHARSET()` | `COLLATION()` |
| --- | --- | --- |
| binary OID 或 `CharsetBinary` | `binary` | `binary` |
| `CharsetUTF8` | `utf8mb4` | `utf8mb4_general_ci` |
| `CharsetUTF8MB4Bin` | `utf8mb4` | `utf8mb4_bin` |
| `CharsetLegacy` text | `utf8` | `utf8_general_ci`（保持当前 protocol compatibility fallback） |

`utf8mb4_0900_ai_ci` 继续按当前实现归一到 general-ci identity；不声称提供 UCA 9.0。未知/旧 text identity 继续使用现有 protocol fallback，不发明新名称。

## 5. Runtime 设计

新增 function-package 私有 helper，职责严格分离：

- `stringDomainMode`：在无 row sidecar 时一次选定 uniform text/binary fast path；存在 row sidecar 时才逐行调用 `GetIsBinaryStringAt`。
- `setSelectedStringResultDomain`：仅对 subject-derived string result，把 subject effective domain 归一化到 result static domain；无动态 metadata 时直接返回，不分配 sidecar。
- `setContributingStringResultDomain`：只供 `CONCAT/CONCAT_WS`，逐行合并实际非 NULL contributors，binary 胜出；结果 NULL row 不写 metadata。
- domain-aware unary helper：uniform path委托现有 optimized template；只有 mixed-row 才进入逐行分支。

所有输出 metadata 使用 `SetRuntimeStringDomain*WithMP`，不得继续调用无法表达“static binary + selected text”的 legacy bool setter。sidecar allocation error 原样返回；无 provenance 的 hot path 不申请 metadata MPool。

逐行 executor 的顺序是：读取同一 logical row（const wrapper 自行映射 row 0）→ NULL/mask 判断 → 选择 subject domain → checked sizing/direct writer → append value → 批量安装 runtime domain。任何错误返回时仍由现有 FunctionResult owner 释放；不增加 unaccounted full-result buffer。

`LIKE` 的 normal text fast path和 regexp cache保留；只有可能出现 binary/mixed subject 时使用无分配的 byte wildcard matcher。Binary matcher按 byte解析 `%`、`_` 和现有 escape contract，不能把 value/pattern转成 rune。terminal suffix、纯 literal run，以及 `%` 后 `_` run + literal segment 使用线性 fast path；其余 greedy fallback具有固定的 backtracking step budget，超限返回明确错误，不能形成不受限的 value × pattern CPU 工作量。

## 6. Metadata 与 materialization closure

- `CHARSET/COLLATION` 从输入 vector 的静态 `Type` 返回名称，不消费 session default，也不让 per-row runtime override改变结果。
- frontend protocol继续从 expression/result `Type` 生成 ColumnDefinition；本次通过 resolver修正后，binary function output自然得到 collation 63 和 binary flag，text `_bin` 仍得到现有 utf8mb4_bin ID。COM_STMT direct parameter result按 direct-result position与 packet-derived runtime type specialization，即使值为 typed NULL也保留 BLOB metadata；NULL-first、复用上一包类型和缓存命中必须等价。
- `internal_column_character_set` 改为读取序列化 `Type.Charset`（binary OID仍权威），并让 `information_schema.columns` 对现有四种 identity 映射到上表名称。它不再把 `Scale` 当 charset。
- CTAS继续复制 planner result type；不按 observed row 或 runtime sidecar缩窄/改域。`DESC`、information_schema 与 direct `CHARSET/COLLATION` 必须对同一静态 expression一致。
- SQL `EXECUTE ... USING` 物化同时保留 assignment-time `SourceType` 与独立的 `RuntimeStringDomain`；typed non-NULL、typed NULL、重复执行和 prepared-plan cache复用均不得把三态 provenance压回静态域。
- remote pipeline sender与receiver对所有已改变的 Function ID执行 MORPC v45 fail-closed barrier，包括 `POSITION`、`INTERNAL_CHAR_SIZE` 和 `INTERNAL_COLUMN_CHARACTER_SET`；catalog upgrade barrier只控制 view物化，不能代替 executor barrier。

## 7. 边界、失败与性能

- NULL：typed NULL 仍有静态 domain，故 `CHARSET(CAST(NULL AS BINARY))='binary'`；值函数保持现有 strict NULL result。NULL row 不携带 selected-value runtime provenance。
- empty：长度/位置/切片沿用 MySQL 边界；binary empty 不触发 UTF-8 decode。
- int boundary：position/length 使用先比较后转换，避免 `MinInt64` 取负或 `int` overflow。
- result limit：继续调用 #27218 的 `maxStringFunctionResultLength`、checked bound 和 `AppendBytesWithWriter`；超限行为不改变。
- const/mask：uniform const保留 template broadcast fast path；mixed flat row严格按 logical row；masked row写 NULL且不传播 metadata。
- 性能：metadata-free uniform text/binary不新增 O(rows) scratch或 sidecar allocation；mixed/dynamic provenance允许一个 O(rows) domain scratch和逐行常数分支。Binary slicing/insert/pad/reverse直接写 result，禁止额外完整 payload副本。

## 8. 被拒绝方案

1. **仅按 OID 选 executor overload**：无法处理 text-shaped动态 binary、static binary + runtime text mixed row，且 prepared/user variable仍错误。
2. **任意 binary 参数切换整个函数**：会让 binary needle/pad/replacement错误改变 text subject 的位置单位和结果 metadata，违反 MySQL oracle。
3. **以 `Literal.IsBin` 或 vector 第 0 行为开关**：literal form不是 domain，且会污染 mixed row、NULL-first batch和复用执行。
4. **让 runtime sidecar改写 protocol/CTAS type**：客户端 metadata在行执行前已固定，也会使同一 result set逐行改变 schema。
5. **整体摘取 #26907**：旧分支混入 REGEXP、width、transport和 legacy bool provenance，且已发现 `ORD` endian、selector和 text-override缺口；只把它当反例来源。

## 9. 验证地图

| 契约 | 最小证据 |
| --- | --- |
| resolver不擦除 binary family；auxiliary不切域 | return/type-checker table UT：source text/binary × needle/replacement/pad text/binary |
| length/ORD/position | focused executor UT：text/binary最近控制、empty/NULL、0/1/-1/越界、4-byte UTF-8、invalid bytes |
| slice/reverse/case/trim | table UT：static text + runtime binary、static binary + runtime text、mixed rows、const与mask |
| insert/pad/replace | direct-writer UT：byte/rune unit、invalid bytes、result limit与已有 #27218 width controls |
| LIKE | matcher UT + executor mixed-row UT：`_` 的 character/byte差异、`%`、escape、invalid bytes、reviewer adversarial线性 benchmark和 fallback budget rejection；REGEXP测试不改 |
| result provenance | nested consumer UT：变换结果再进 `CHAR_LENGTH`；无 metadata fast path断言不分配 sidecar |
| CHARSET/COLLATION | static type table UT；runtime mixed override不改变名称；legacy fallback控制 |
| information_schema/CTAS | internal metadata UT + public BVT，核对 binary/general-ci/utf8mb4-bin |
| protocol | frontend field metadata UT；binary source-derived output为 collation 63，text `_bin` 非63；每个变更 Function ID 的真实 sender encode / receiver decode v44 reject与v45 accept |
| reachable sources | public SQL覆盖 raw/_binary、BINARY/VARBINARY/BLOB、CAST/CONVERT、column、bare variable、SQL PREPARE；COM_STMT用现有 planner/frontend typed parameter fixture |
| stability | owning package tests、normal BVT同实例两轮、`git diff --check`、mo-self-review change map |

新增/修改代码覆盖率目标至少 75%。测试保持单表、少量 rows和短 byte sequence；不增加 sleep、大表或新 cluster topology。

## 10. 审批记录

- 设计 revision：当前 worktree 版本
- 审批者：user
- 决定：用户在本轮明确要求 “Proceed with the approved implementation for issue 27216”；据此记录为 Approved 并进入实现。

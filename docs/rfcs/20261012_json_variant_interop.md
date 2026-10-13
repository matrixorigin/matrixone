# MO JSON 精确解析与 VARIANT 互操作

- Status: implemented for review
- English implementation design: [Exact JSON parsing and Parquet VARIANT interoperability](../design/20261013-exact-json-parquet-variant.md)
- Feature issues: [#30057](https://github.com/matrixorigin/matrixone/issues/30057), [#30058](https://github.com/matrixorigin/matrixone/issues/30058)
- Publication integration base: `3e50e1b32d8da61bbbf70c89989fb08b671809de`
- Start Date: 2026-10-12
- Implementation: 本地工作区已实现精确解析、未 shredded VARIANT 读写与受限类型范围内的 shredded VARIANT 导入；自动存储子列仍属设计。
- MatrixOne baseline: `dd38098853ea197699ed3fac3e69dc53975ac061`
- DuckDB 2.0 reference: `v2.0-cyanoptera`，提交 `c65ca930b9fe39b2e8d8ed8821018dca650965f4`。

MO 的 JSON 已经具备类型化标量、路径查询、JSON_TABLE、生成列和索引。与 Doris、DuckDB 的差距主要集中在自动存储子列、SQL 类型身份的覆盖范围以及文件互操作。本地改动补齐精确文本解析和一部分 Parquet VARIANT 互操作；这些改动不等于实现了 Doris 的动态子列存储或 DuckDB 的 shredded 字段下推。

## 源码对应关系

| 能力 | MO 基线源码 | Doris 与 DuckDB 的实现差异 | 本地改动 |
| --- | --- | --- | --- |
| JSON 数字解析 | `byteJsonWriter.parseNumber` 将小数、指数和超出 uint64 的整数转换为 float64 | DuckDB VARIANT 枚举保留 INT8/16/32/64/128、UINT、FLOAT/DOUBLE、DECIMAL 等类型身份 | 增加 `JSON_PARSE_EXACT`，小数、指数和超大整数使用已有 DECIMAL 文本载荷 |
| SQL 标量转 JSON | `jsonvalue.FromVector` 已保留 DECIMAL、DATE、TIME、DATETIME、二进制等 | DuckDB 还区分 UUID、纳秒时间戳、时区时间戳等；MO JSON 没有这些独立标签 | 保持现有类型边界，文件转换遇到无等价类型时明确报错 |
| 类型化路径投影 | `jsonvalue.ConvertScalarWithContext`、JSON_TABLE 已有范围、截断、NULL、缺失等状态 | Doris 4.0 的同一路径会合并为公共子列类型；DuckDB 在 shredded 状态和统计信息允许时下推提取 | 精确解析的结果直接复用已有转换层，并增加精度回归 |
| 热路径与索引 | 已支持 STORED 生成列及索引；FULLTEXT2 JSON 索引使用叶子键和值并保留原谓词复查 | Doris 4.0 使用动态子列与 sparse 列；DuckDB 有独立的 shredded 存储状态 | 验证热点列建表方案；未实现自动路径物化 |
| Parquet JSON | 普通嵌套组可重建为 JSON；JSON 导出默认为字符串 | 标准 VARIANT 是带逻辑类型注解的 metadata/value 组，不能按普通对象理解 | 接入 VARIANT v1 读写及 shredded 对象/数组导入；导出仍为未 shredded，由显式格式开启 |

MO 对应源码：

- [JSON 编码与解析](../../pkg/container/bytejson/bytejson.go)
- [SQL 标量构造](../../pkg/sql/jsonvalue/value.go)
- [类型转换](../../pkg/sql/jsonvalue/conversion.go)
- [JSON_TABLE](../../pkg/sql/jsonvalue/table.go)
- [JSON 索引优化](../../pkg/sql/plan/apply_indices_fulltext_json.go)
- [JSON 索引数值编码](../../pkg/fulltext2/jsonterm.go)
- [生成列 SQL 回归](../../test/distributed/cases/ddl/generated_column.sql)
- [Iceberg 类型解析](../../pkg/iceberg/metadata/parser.go)

其他数据库的源码范围：

- [Doris 4.0 ColumnVariant](https://github.com/apache/doris/blob/branch-4.0/be/src/vec/columns/column_variant.h)，文件 blob `d5e54e10e75d344f7b578e1d87a903fda7de682f`。`Subcolumn::LeastCommonType` 管理公共类型；超出子列数量限制时使用 `Map(String,String)` 存放路径和序列化值。
- [Doris 4.0 子列选择](https://github.com/apache/doris/blob/branch-4.0/be/src/vec/columns/column_variant.cpp)，文件 blob `009c8df65458807f2a1b6de1864b89392fe5fd73`。`pick_subcolumns_to_sparse_column` 按非 NULL 值数量排序，受预算约束保留子列，其他路径进入 sparse。这里的排序依据是非 NULL 值数量，并不是实际查询热度。
- [Doris 开发分支 ColumnVariantV2](https://github.com/apache/doris/blob/2ad5417a9d0e74a1986f288a6ec781e0cedcb519/be/src/core/column/variant_v2/column_variant_v2.h) 区分 encoded、typed scalar、format-owned shredded tree，并定义延迟物化边界。[存储写入](https://github.com/apache/doris/blob/2ad5417a9d0e74a1986f288a6ec781e0cedcb519/be/src/storage/segment/variant/v2/variant_column_writer.cpp) 同时处理物化路径列和 doc/sparse 存储。该提交代表开发分支，不能视作所有 Doris 4.x 发布版的实现。
- [DuckDB 1.5 类型枚举](https://github.com/duckdb/duckdb/blob/v1.5.0/src/include/duckdb/common/types/variant.hpp)，文件 blob `afeccc8fd63d46845c0e573a7257df02a3aaa053`。
- [DuckDB 1.5 VARIANT 存储](https://github.com/duckdb/duckdb/blob/v1.5.0/src/storage/table/variant_column_data.cpp)，文件 blob `d349239c6aa911e46dd40525138136a45c4e4de2`。`PushdownShreddedFieldExtract` 检查路径、是否 fully shredded、统计信息和叶子类型，再决定是否下推。不能将支持 VARIANT 等同于任何路径都能裁剪物理读。

## DuckDB 2.0 发布分支复核

本节以 2026-10-12 的 `v2.0-cyanoptera` 提交为准。[官方发布日历](https://duckdb.org/release_calendar)当时列出 2.0.0 的计划日期为 2026-10-21，[2.0 开发版本说明](https://duckdb.org/2026/09/02/try-duckdb-20-alpha)介绍了该发布分支。它是可参考的 2.0 开发版本；正式发布后应重新固定 release tag。VARIANT 在源码中仍标记为 1.5 引入，以下能力不能全部称为 2.0 新增。

### 类型与数值语义

[2.0 类型枚举](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/common/types/variant.hpp)新增了相对 1.5 枚举的 `TIMESTAMP_NANOS_TZ`，继续保留有符号与无符号整数位宽、DECIMAL、FLOAT、DOUBLE、UUID、时间精度、时区、BIGNUM、BITSTRING、GEOMETRY 等身份。MO JSON 的数字宽度与时间标签较少，现有文件转换也不能凭借字符串保留这些 SQL 类型身份。

VARIANT 能保存 DECIMAL，并不意味着文本 JSON 自动成为精确 DECIMAL。[JSON 转换](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/function/cast/variant/json_to_variant.hpp)将 JSON unsigned/signed 整数转换成 UINT64/INT64，将 REAL 以及由 yyjson 以 RAW 保留的超范围数字转换成 DOUBLE。金额的精度合同需要从类型化 DECIMAL 进入 VARIANT，或在文本解析阶段明确处理。MO 的 `JSON_PARSE_EXACT` 因此仍有独立价值。

[2.0 比较类别](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/common/types/variant/variant_comparison.cpp)将整数、DECIMAL、BIGNUM 统一为 NUMBER，将 FLOAT/DOUBLE 统一为另一个 REAL 类别。不同类别按类型顺序比较；同一个 NUMBER 类别按精确十进制规范键比较。[上游回归](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/test/sql/variant/variant_comparator.test)明确要求整数 1 与 DOUBLE 1.0 的 comparator key 不相等，整数 5 与 DECIMAL 5.00 的 key 相等。

`variant_comparator` 由 [collation binding](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/planner/collation_binding.cpp)接入比较、排序和分组。MO JSON 使用统一数值域，整数 1、DECIMAL 1.00 与 DOUBLE 1.0 可以相等，并生成相同的数值规范键。迁移时需要核对 DISTINCT、分组、连接和 MIN/MAX，不能把 DuckDB 的类型顺序直接替换进 MySQL JSON。DATE 与非时区 TIMESTAMP 在 DuckDB 的比较中也能归入相同时间类别；MO JSON 仍保留自己的类型优先级。

### NULL 与路径语义

[HandleVariantNull](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/function/cast/variant/to_variant_fwd.hpp)将顶层 null 设置为向量级 SQL NULL；嵌套 null 写为 `VARIANT_NULL`。因此，JSON 文本 `null` 转成顶层 DuckDB VARIANT 时，不能继续假设它与 SQL NULL 可区分。MO JSON 和本地 Parquet 读写仍区分顶层 JSON null 与 SQL NULL。

对象内的 missing 与显式 null 则必须保留。[variant_exists 的 shredded 回归](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/test/sql/function/variant/variant_exists_shredded.test)检查了残余字段、缺失字段、显式 null 和 SQL NULL。Shredding 不能将一个不存在的字段变成存在且为 null 的字段。

[variant_extract](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/function/scalar/variant/variant_extract.cpp)的整数数组索引从 1 开始，索引 0 报错。MO JSONPath 的数组下标从 0 开始。两边迁移表达式时，不能直接复制数组位置。

### 自动 shredding 与延迟物化

[Checkpoint](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/storage/table/variant_column_data.cpp)在 row group 有变化、达到最小行数并能推断 shredding 类型时，重写成 unshredded 残余和 typed 列。当前 [设置默认值](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/common/settings.json)为 `variant_minimum_shredding_size = 30000`，-1 关闭。每个 row group 只有 10,000 行时，默认不会触发这条原生自动 shredding 路径；功能 case 数量不能替代对存储行数和 checkpoint 的验证。

[类型推断](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/storage/table/variant/variant_shredding.cpp)按类型出现次数选择物化类型，其余保留在残余中；DECIMAL 的 width/scale 一致性会影响是否物化。这与 Doris 公共子列类型的合并策略不同。VARIANT 键区分大小写，但用于承载 typed 字段的 STRUCT 有大小写约束；检测到 `a`/`A` 冲突时，相关对象不能自动转成该 STRUCT。空字段名不纳入自动 typed 字段。[冲突回归](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/test/issues/general/test_24298.test)要求保留原始键，并允许其他无冲突路径继续 shredding。

2.0 相对 1.5 已读源码的一个明确变化是 `CreateScanStates` 不再要求下推结果必须是非 VARIANT 的 CAST 类型。它可以先提取 typed 叶子，再返回 VARIANT。[SHREDDED_VECTOR](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/common/vector/shredded_vector.cpp)让扫描和切片继续保留两种表示，到确实需要完整值时再 unshred；该向量的序列化要求 storage version 2.0.0，兼容旧版本时先展开。

[优化器](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/optimizer/remove_unused_columns.cpp)当前只下推按键提取，不下推 VARIANT 数组索引提取；`TRY_CAST` 不按普通 CAST 方式整体推入扫描。目标为 JSON 的 CAST 也保留在字段提取之上，因为 VARIANT→JSON 是序列化，而 VARCHAR→JSON 是解析，替换会改变语义。fully-shredded 状态、统计信息和标量叶子仍是原生字段提取下推的条件。

### Parquet 互操作边界

[Parquet reader](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/extension/parquet/reader/variant_column_reader.cpp)及 [ParquetVariantIterator](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/common/types/variant/parquet_variant_iterator.cpp)共同读取 typed_value 与 value。部分对象需要合并 typed 字段和残余字段，并保留 missing/null。MO 本地导入已增加 shredded 对象、数组与残余字段合并，导出仍为未 shredded。支持范围受 MO JSON 类型身份约束；尚未运行 DuckDB 2.0 实例互读，不能将其任意 shredded 文件都标为已兼容。

[Parquet 自动分析](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/extension/parquet/writer/variant/analyze_variant.cpp)同样依据类型频次与 decimal 一致性选择 typed schema，但它是文件编码分析，不能与数据库 checkpoint 算法混为一谈。该实现中还保留了针对低频路径是否值得物化的 TODO，因此不能认定它已有 Doris 那样的完整长尾路径预算策略。

[未 shredded 的写入转换](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/common/types/variant/parquet_variant_encoding.cpp)把 UINT64、INT128、UINT128 尝试转换成 INT64，超出范围时失败；BIGNUM、INTERVAL、BITSTRING 等也不是所有内部类型都能直接输出为该格式。MO 编码器对 uint64 上半区使用 decimal16 是保值策略，不能由 DuckDB 内部支持 UINT64 就推断其所有导出路径也支持。

### 对 Agent 场景的满足度

| 场景 | DuckDB 2.0 源码支持 | MO 当前满足度与改进方向 |
| --- | --- | --- |
| 完整工具输入输出和 trace 回放 | 对象、数组及 typed/residual 重建；顶层 null 有转换边界 | JSON 可满足文档保留与回放，保留顶层 NULL 分类 |
| 成本与 token 数保真 | typed DECIMAL、整数类别丰富；文本 JSON 转换仍可能经过 DOUBLE | 精确入口加 DECIMAL/整数投影可满足；聚合需指定类型 |
| 稳定标量属性的大规模筛选 | checkpoint shredding、typed 字段提取、统计信息与延迟物化 | 手工热点列和索引可用；自动路径存储与扫描下推仍是差距 |
| 工具调用数组的任意元素查询 | 可存取数组；当前按数组下标提取没有同等下推路径 | JSON_TABLE 或拆分 span/事件表更可控，须独立测扫描成本 |
| 类型漂移与长尾标签 | 按选定类型物化，其他保留残余；存在 case/decimal 一致性约束 | JSON 可保留混合值；列预算、类型冲突与回填合同需补 |
| 纳秒、时区、UUID 的原始类型身份 | 有对应 VARIANT 类型，文件支持依具体写入路径而定 | 普通 JSON 标签不足；保留到独立 SQL 列，或设计新版本类型编码 |
| 多类型值的去重与分组 | NUMBER/REAL 类别和逻辑 collation | MO 保留 MySQL JSON 语义；不能将两者视作无差异替换 |

### 本地实现的补充

参考 2.0 使用可变宽度字典与字段偏移的方向，本地 Parquet VARIANT 编码改为按实际范围选择 1/2/3/4 字节字典、字段 ID、偏移，以及 1/4 字节容器计数。整数选择可容纳的有符号物理宽度；decimal 根据系数与 scale 选择 decimal4/8/16。这些都是文件物理编码选择，MO SQL 比较和 JSON 存储标签保持原合同。

独立 golden vector `{"a":1}` 的 metadata＋value 从上一版 40 字节变为 12 字节；这只是该载荷大小，不代表 Parquet 文件压缩率或跨数据库吞吐差异。新增 10,000 条数值比较与规范键一致性用例，以及大小写键、空键、missing/null、整数边界、256 字段边界、字典与偏移宽度、16 MiB 子值和文件读写回归。

2.0 的源码快照与文件 blob 清单位于工作区 `variant-source-review/duckdb-2.0`。本轮验证运行的是 MO 本地测试，未运行 DuckDB 2.0 二进制、checkpoint 或跨引擎文件互读；源码合同与已执行结果应分别使用。

## 精确解析的语义

```sql
SELECT JSON_PARSE_EXACT('{"cost":9007199254740993.001}');
SELECT CAST(JSON_EXTRACT(
    JSON_PARSE_EXACT('{"cost":9007199254740993.001}'), '$.cost'
) AS DECIMAL(38,3));
```

`JSON_PARSE_EXACT` 接受一个字符串参数并返回 JSON。SQL NULL 传播为 SQL NULL；文本 `null` 是非 SQL NULL 的 JSON null。无效 JSON 报错。可容纳的整数继续使用 int64/uint64；其他数字复用已有 DECIMAL 编码，保留原始数值文本，不经过 float64。容器嵌套上限复用现有的 100 层限制。

普通 JSON 解析、文本到 JSON 的隐式转换和已有 SQL JSON 类型不改变。已经存成 float64 的值无法通过新函数恢复精度，必须在原始文本进入 JSON 时使用精确入口。SQL 函数 ID 追加为 591，已有 ID 不移动。

精确存储与精确计算是两个边界。现有 `SUM(JSON)`、`AVG(JSON)` 等聚合仍进入 DOUBLE 域；金额、成本等需要先 CAST 到合适的 DECIMAL，或直接聚合物化的 DECIMAL 列。`JSON_EXTRACT_FLOAT64` 和 FULLTEXT2 的数值探测也继续使用 float64 域，不能用于宣称精确 decimal 路径索引。

程序化构造 `json.Number` 现在复用文本解析的数字转换逻辑，修正 `strings.Contains("Ee.")` 的分类检查，并统一拒绝 NaN、Inf 等非有限值。

## Parquet VARIANT 读写

编码按 [Apache Parquet VARIANT v1](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) 实现。库 `parquet-go v0.25.1` 只提供 VARIANT 注解和组结构，本地 `pkg/util/parquetvariant` 补充了实际载荷编码与解码。

```sql
SELECT JSON_PARSE_EXACT('{"cost":9007199254740993.001}') AS payload
INTO OUTFILE 'agent.parquet' FORMAT 'parquet_variant';
```

新导出格式将 JSON 列写成 optional VARIANT 组，其中 metadata 和 value 为 required BINARY。其他 SQL 列沿用现有 Parquet 导出映射。普通 `FORMAT 'parquet'` 继续将 JSON 写成字符串；`.parquet` 后缀也不会隐式开启 VARIANT。

现有 Parquet 外部扫描和 LOAD 路径识别 VARIANT 注解，将其载荷重建为 MO JSON，而非生成包含 metadata/value 的 JSON 对象。JSON null 编码为有效载荷；SQL NULL 使用外层组的 NULL 状态。嵌套重建中的 VARIANT 也使用同一解码器。

| 标准 VARIANT 值 | MO JSON 映射 |
| --- | --- |
| null、boolean、string、object、array | 保留逻辑类别及结构 |
| int8/16/32/64 | int64；不保留原始整数位宽 |
| float、double | float64；float32 精确扩宽，但不保留独立 FLOAT 身份；非有限值拒绝 |
| decimal4/8/16 | DECIMAL 文本，精确保存系数和 scale |
| date | DATE；当前转换支持年份 1 至 9999 |
| timestamp without timezone MICROS | DATETIME，微秒精度，当前转换支持年份 1 至 9999 |
| time without timezone MICROS | TIME，限一个日历日 |
| binary | 二进制 BLOB 语义，存储时复用现有兼容编码 |
| UTC timestamp、纳秒 timestamp、UUID | 拒绝，避免丢失类型身份或精度 |
| shredded VARIANT | 导入支持对象、数组、typed/residual 合并及类型漂移；不支持的标量类型或畸形数据报错；导出仍为未 shredded |

导出 uint64 超出 int64 范围时使用 decimal16，保留数学值，但返回时可能成为 DECIMAL。Parquet DECIMAL 的精度、scale 上限为 38；无法精确表示的值报错，必要时可移除无意义的尾随零。JSON 数字的原始指数写法、整数位宽和 decimal 文本拼写不属于文件往返保证。

解码检查字典版本、UTF-8、长度、字段 ID、字段名唯一性与顺序、容器偏移、标量精确边界及嵌套深度。对象载荷允许非顺序偏移；别名、重叠、空洞及多余字节拒绝。输入和保守的单行展开配额限制为 64 MiB，并检查上下文取消。配额同时计入字典、容器表、typed 字段、残余片段和嵌套装配的子文档复制；所有片段共享同一配额，不能逐片重置。因此不保证接受所有 64 MiB 文档，该配额也不是进程 RSS 的硬上限。

## Shredded 导入的严格合同

- 所有字段按名字识别；metadata 是共享的必填字典，typed 对象 schema 的字段名必须在字典中。
- 两种表示均为空的对象字段保持 missing；显式 VARIANT null 保持 JSON null；可选外层组为空保持 SQL NULL。根部两种表示均为空按规范恢复为 JSON null；数组元素缺失报错。
- 部分对象的残余必须是对象，且不得含 typed schema 中的任何字段，包括当前行 missing 的字段。标量和数组同时含 value/typed_value 则报错，不静默选择其中一个。
- 数组要求标准三层 LIST，element 是 required 字段组；逐叶校验定义层级、初始重复层级、外层元素边界及空数组占位，拒绝遗漏和重复占位。
- typed 与 residual 使用同一标量解码规则，保留 DECIMAL、DATE、微秒 NTZ 时间和二进制身份；DECIMAL 检查声明精度、scale、大端补码字节宽度及固定宽度可承载的最大精度，INT8/16 检查物理 INT32 的实际范围。
- 逻辑容器深度上限 100，typed 与 residual 合并计算，不能各自重置。parquet-go v0.25.1 另有 127 层物理 schema 限制，深度较大的 fully typed schema 会更早被文件读取层拒绝。
- 导入不等于存储自动 shredding，不提供按 JSON 路径的物理页裁剪。

扩大真实文件测试发现 parquet-go v0.25.1 的 GenericWriter map/any 批量路径会嵌套调用计数，导致文件行数重复累计和 row group 边界崩溃。MO 的导出已改用物理 WriteRows 单一路径，并加入文件声明行数、每组行数和读回值的回归断言；默认字符串 JSON 导出也覆盖该修复。

## Agent 可观测性的热点列方案

下面方案复用已有 STORED 生成列与普通索引。JSON 保留完整文档；trace 查询与成本聚合可以显式读取类型化列。

```sql
CREATE TABLE agent_exact (
    id BIGINT PRIMARY KEY,
    doc JSON,
    trace_id VARCHAR(128) AS (
        JSON_EXTRACT_STRING(doc, '$.trace_id')
    ) STORED,
    cost DECIMAL(38,3) AS (
        CASE WHEN JSON_TYPE(JSON_EXTRACT(doc, '$.cost'))
                       IN ('INTEGER','DECIMAL')
             THEN CAST(JSON_EXTRACT(doc, '$.cost') AS DECIMAL(38,3))
             ELSE NULL END
    ) STORED,
    INDEX trace_idx(trace_id),
    INDEX cost_idx(cost)
);

INSERT INTO agent_exact(id, doc)
VALUES (1, JSON_PARSE_EXACT(
    '{"trace_id":"trace-1","cost":9007199254740993.001}'
));

SELECT trace_id, SUM(cost) FROM agent_exact GROUP BY trace_id;
```

这里的 scale 3 是示例业务约束；低于 0.001 的成本需要选择更大的 scale。类型漂移为字符串或复合值时，示例投影为 NULL，不隐式将字符串成本当作数值。要统计缺失、JSON null 与类型错误，需要另外使用 `JSON_CONTAINS_PATH`、`JSON_TYPE` 保留分类信息。

完整文档回放、工具参数及输出、动态标签适合保留 JSON。稳定的时间、租户、trace_id、状态、token 数和成本适合类型化列。对象数组、事件关系与多级工具调用适合已有 JSON_TABLE，或拆成按 trace/span 关联的子表。新函数提升数值保真，不自动解决任意动态属性的大范围扫描。

## 自动子列的剩余设计

自动子列需要改变存储与扫描协议，不能通过 JSON 别名或运行时解析缓存完成。后续实现至少需要以下合同：

1. 按完整路径建立身份，区别对象键、数组位置和 wildcard；保留 missing、JSON null、SQL NULL 与类型冲突。
2. 控制每列路径预算，用查询频率、非 NULL 比例、读取收益和写入成本选择物化路径；长尾仍保留在原始 JSON 或 sparse 载荷。
3. 子列类型须有版本与冲突策略。默认回退到原文复查，不允许失败转换悄悄变成可参与错误索引裁剪的 NULL。
4. 原文、子列和索引需要相同提交版本，覆盖 INSERT、UPDATE、DELETE、回滚、后台回填、compaction、恢复及复制。
5. 优化器只有在路径、类型及版本合同满足时才能替换表达式；不满足时继续读取原文。不同 block 的路径集合不能被全表 schema 假设替代。
6. 文件 shredded 导入的 value/typed_value 合并与缺失重建已实现；自动路径投影、统计信息与物理子列 I/O 裁剪仍需改变扫描协议。
7. Iceberg 的 variant 类型当前仍被 metadata parser 拒绝。Parquet VARIANT 导入并不会自动使 Iceberg VARIANT 可用。

这些是待实现的存储功能，本地改动没有将其标记完成。

## 验证范围

- `TestParseExactTenThousandDocuments`：10,000 条固定种子的嵌套 agent 文档，覆盖符号、大整数部分、小数及指数；检查路径提取与文本往返。
- `TestDecodeVariantTenThousandCases`：10,000 条 decimal 对象，检查独立十进制期望值及 VARIANT 编解码往返。
- `TestJSONCommonNumericDomainTenThousandCases`：10,000 条整数、DOUBLE 与精确 DECIMAL 的比较和规范键一致性检查，验证文件编码改动不会改变 MO 数值域合同。
- `TestParquetVariantShreddedTenThousandFileRows`：10,000 条 typed/residual 混合行，覆盖类型漂移、missing、JSON null、SQL NULL，在 79 个 row group 和小 page 上导入 JSON/text，逐行验证值与总行数。
- `TestParquetVariantShreddedTenThousandInvalidRows`：10,000 条畸形行，要求全部报错，覆盖元数据遗漏/重复、类型宽度溢出、物理类型冲突、非法定义/重复层级及双表示冲突。
- `TestParquetVariantExportTenThousandRowsAcrossGroups`：10,000 条输出，普通/VARIANT 格式分别验证 79 个 row group、声明行数、SQL NULL 和精确成本值。
- 1,000 条对象数组文件行覆盖大小写字段、Unicode、空数组、null 元素、空对象及部分对象；另验证嵌套数组、组合深度、取消和小载荷的嵌套复制放大拒绝。
- VARIANT golden vectors 独立按标准编码指定，覆盖标量、二进制、非顺序对象偏移、嵌套和畸形数据；不是只用生产编码器生成生产解码器的测试输入。
- 完整 Parquet 文件测试覆盖多个 row group、JSON/text 目标、普通列与 VARIANT 混合扫描、NULL 区别、导出 schema 与载荷、默认格式兼容、常量 NULL、拒绝不支持的 schema。
- SQL 函数测试覆盖注册、参数类型、常量、跳过行、NULL 和错误；规划测试覆盖 prepared 参数、DECIMAL CAST、热点生成列及新导出格式。
- Fuzz 分别针对线格式解码与 shredded 行重建，检查不崩溃、输出合法 JSON、二进制表示有效以及可支持类型的编码往返；race 覆盖 codec、导入与导出。

以上属于源码、规划器和本地 Parquet 文件测试。Doris/DuckDB 生产实例互读、分布式 MO 的 LOAD/OUTFILE 端到端测试、存储恢复和性能压测仍需专门验证，不据此宣称性能优于其他数据库。一个已有 JSON Schema 测试会启动本地 HTTP 服务，在当前禁止创建 socket 的沙箱中被排除；相关无网络 JSON 回归仍执行。

2026-10-13 本地执行记录：

| 检查 | 结果 |
| --- | --- |
| bytejson、VARIANT codec 全量单元测试 | 通过；codec 语句覆盖率 90.6% |
| 全部 `TestParquet*` 回归、导出与格式校验、SQL 规划与类型投影 | 通过 |
| JSON 函数扩大回归 | 通过；排除上文说明的单个 HTTP 测试 |
| bytejson、codec、VARIANT 导入/导出的 race 回归 | 通过 |
| 线格式 fuzz，30 秒，1 worker | 161,197 次执行，未发现失败 |
| shredded 行 fuzz，30 秒，1 worker | 15,613 次执行，未发现失败；含对象、数组、层级突变和合法输入必须被接受的断言 |
| DuckDB/Doris 独立实例互读 | 未执行；依赖安装的网络权限请求被中止 |

三组新增 10,000 条测试的计数指逐行输入及期望结果，不是 10,000 种不同查询，也不是吞吐量测试。语句覆盖率与有限时长 fuzz 不代表所有输入或生产故障模式已验证。

已构建 cgo 与 thirdparties 原生库的 Linux 环境，可在仓库根目录复跑：

```bash
export CGO_CFLAGS="-I$PWD/cgo -I$PWD/thirdparties/install/include"
export CGO_LDFLAGS="-L$PWD/cgo -L$PWD/thirdparties/install/lib"
export LD_LIBRARY_PATH="$PWD/cgo:$PWD/thirdparties/install/lib:${LD_LIBRARY_PATH:-}"

go test ./pkg/container/bytejson ./pkg/util/parquetvariant -count=1
go test ./pkg/sql/plan/function \
  -run 'Test.*[Jj][Ss][Oo][Nn]|Test_funids|Test.*Function' \
  -skip 'TestJsonSchemaExternalReferenceDoesNotPerformNetworkIO' -count=1
go test ./pkg/sql/colexec/external -run '^TestParquet' -count=1
go test ./pkg/sql/jsonvalue ./pkg/sql/plan ./pkg/frontend \
  -run 'TestExactIngestionTypedProjection|TestJsonParseExactSQLPlanning|TestParquetVariant|Test_getExportFormat|Test_validateExportFormat|Test_vectorValueToParquet|Test_buildParquetNode' \
  -count=1
go test -race ./pkg/util/parquetvariant ./pkg/sql/colexec/external ./pkg/frontend \
  -run 'TestDocument|^TestParquetVariant' -count=1
go test ./pkg/util/parquetvariant -run '^$' \
  -fuzz '^FuzzDecodeVariant$' -fuzztime=30s -parallel=1
go test ./pkg/sql/colexec/external -run '^$' \
  -fuzz '^FuzzParquetVariantShreddedRows$' -fuzztime=30s -parallel=1
```

# MatrixOne 二进制字符串语义模型

本文定义 issue #27214 的基础契约。后续传播、函数行为、REGEXP 和返回类型任务必须分别消费这些维度，不能重新把它们折叠成一个布尔值或来源枚举。

## 不变量

一个 SQL 值的下列状态彼此正交：

| 维度 | 类型 | owner | 含义 |
| --- | --- | --- | --- |
| 静态 SQL 类型 | `types.Type` / `plan.Type` | binder | OID、width、scale、charset、collation；一经解析即为静态域的唯一依据 |
| 行级字符串域 | `types.RuntimeStringDomain` | vector | `inherit`、显式 `text`、显式 `binary`；只覆盖当前选中值 |
| 动态来源 | `types.StringSource` | 值的创建入口 | expression、literal、裸 user variable、SQL PREPARE、COM_STMT |
| 字面量形式 | `types.StringLiteralForm` / `plan.Literal.literal_form` | parser/binder | 普通 text、`_binary` introducer、raw hex、raw bit |
| 转换类别 | `types.StringConversionKind` | frontend binding | string、integer、float、decimal、boolean；不是来源 |
| NULL 类别 | `types.StringNullKind` | 静态类型 + NULL 状态 | 非 NULL、`T_any` 的 untyped NULL、有确定类型的 typed NULL |

`Literal.IsBin` 继续兼容 raw hex/bit 的数值解释，但不得作为通用 binary-string provenance。`PrepareParamKind` 是 `StringConversionKind` 的兼容别名，不得增加 user-variable、SQL-PREPARE 或 COM-STMT 来源值。

## 合法状态表

| 值的形态 | 静态类型 | runtime | source | literal form | conversion | NULL |
| --- | --- | --- | --- | --- | --- | --- |
| 普通字符串 literal | text | inherit | literal | text | string | not-null |
| `_binary` literal | binary | inherit | literal | binary-introducer | string | not-null |
| raw hex / raw bit | unresolved 或上下文类型 | inherit | literal | hex / bit | string | not-null |
| 普通 column/expression | binder 结果 | inherit | expression | none | string | not-null/typed-null |
| 选中 text 值进入 binary 公共类型 | binary | text | 原值来源 | none 或原 literal form | 原 conversion | 原 NULL 类别 |
| 选中 binary 值进入 text 公共类型 | text | binary | 原值来源 | none 或原 literal form | 原 conversion | 原 NULL 类别 |
| 裸 user variable | resolver 结果 | 本次值决定 | user-variable | none | 本次绑定转换类别 | 来源在 NULL 时仍保留 |
| SQL PREPARE marker | binder 结果 | 本次值决定 | sql-prepare | none | 本次绑定转换类别 | marker 在 NULL 时仍保留 |
| COM_STMT text/BLOB marker | 协议结果 | text/binary | com-stmt | none | 协议值的转换类别 | marker 在 NULL 时仍保留 |

以下组合必须确定性拒绝：未知枚举；非 literal 来源携带 literal form；非 NULL literal 缺少 form；具体非字符串类型携带 runtime text/binary；untyped NULL 携带具体类型；typed NULL 携带 `T_any`；普通 expression/literal 携带动态转换类别。

## 合并策略

- `selected-value`：输入必须恰好有一个已选中状态。公共静态类型替换输入静态类型，其余来源、literal form、conversion 和 runtime domain 属于被选中值。
- `common-domain`：候选值只参与 binder 的公共静态类型解析。结果来源为 expression，runtime 为 inherit，conversion 为 string。
- `contributing-values`：只传入真正贡献结果字节的参数。忽略 NULL；任一非 NULL 贡献值有效域为 binary 时，结果为 binary，否则为 text。delimiter、position、length、match type 等控制参数不得加入输入集合。

三种策略都拒绝空输入。全部 NULL 的结果依据 binder 给出的结果类型形成 typed/untyped NULL。

## Vector 表示与复杂度

Vector 复用现有 scalar + bitmap 存储，不增加普通行的结构体大小：

| 存储状态 | runtime 含义 |
| --- | --- |
| bitmap inactive，`binaryString=false` | inherit |
| bitmap inactive，`binaryString=true` | uniform explicit binary |
| bitmap active，bit=0 | explicit text |
| bitmap active，bit=1 | explicit binary |

静态 binary 类型在 bitmap inactive 时仍由 `types.Type` 提供 binary 有效域。selected-value API 在静态 binary 公共类型中保留 bitmap，使 bit=0 能覆盖为 text；全 binary 的普通路径折叠为 inherit，不分配 sidecar。

bitmap 通过 Vector 现有 MPool/allocation-account 生命周期管理。容量预留失败不得发布 active 状态；`CleanOnlyData`、reset 和 free 清除逻辑状态，已有容量可以安全复用。getter 为 O(1)，普通 uniform 路径不扫描整列。

## Wire 兼容

`plan.Literal.literal_form` 使用新的可选 enum 字段。旧 plan 缺少该字段时得到 `NONE`；对于可执行 identity，`NONE` 与普通 `TEXT` 等价，其他 forms 必须保持可区分。deepcopy、hash/equality 和 protobuf round-trip 都必须保留非默认 form。

## 后续任务边界

- #27215：把 source/runtime/conversion 的 typed 状态接入所有表达式、关系算子和 transport owner。
- #27216：按本契约修改普通字符串函数的参数角色和 byte/text 算法。
- #27217：实现 REGEXP 的 binary rejection、3995 以及各参数来源矩阵。
- #27218：实现返回宽度、BLOB 晋升和 CTAS/metadata 行为。

这些任务只能通过显式 merge policy 或 typed getter 消费状态；不得根据 `Literal.IsBin`、`PrepareParamKind` 或静态 OID 猜测其他维度。

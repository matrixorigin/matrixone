# general-ci 跨入口描述与创建请求 v1 草案

状态：**r3-review.1 的送审附录，不是现有协议**。隶属[主设计](CLAUDE_issue-25096-general-ci-design.md)，须整体取得设计批准，不单独授权实现。分析基线`mo/main@3b7752551b8a1cc60daddecde9a5438d5357cdf4`。只给DUMP、GETDDL和CDC复用既有plan/schema序列化、声明验证和CREATE owner；不是通用SQL执行封装或第二套collation registry。

## 1. 三种东西不能混成一个hash

1. **完整传输值**：包含完整Type、声明文本、已绑定Expr及创建上下文；用于验证与恢复，不能为了算hash改写权威源对象。
2. **逻辑语义像S1**：源/目标重命名不影响的列、默认域、逻辑键及表达式语义。只在副本上规范化已证明非语义的lineage/统计字段。
3. **物理布局像L1**：主表和各索引角色的key format、物理列类型/seqnum、隐藏key。跨OID传输不复制租户/表OID、owner或本地schema epoch。

CDC按逻辑行写入，不要求源/目标seqnum相同；分别验证其物理布局符合各自逻辑定义即可。hash用于漂移检测，不替代原schema verifier：既有可证明无损的非collation类型别名/历史显示宽度兼容仍归原owner判定，不一律要求S1字节完全相等；完整Type必须保留供判断，新域/revision差异不能借此放宽。DUMP/CCPR安装物理对象，必须在原安装owner中保持源布局，或完成该owner实际支持且已验证的映射，不能把CDC的逻辑相等当物理文件相容。若要向空目标应用源seqnum，必须通过原schema/ReplaceDef发布，检查唯一性、保留序号、next-seqnum及完整索引集合，不能直接写系统表；失败仍回滚原目标。

## 2. 基本编码

S1/L1是**用于明确传输和摘要的受限schema像**，不替代现有plan/vector/对象文件格式。`schema_image/layout_image`按下述布局传输原值像；计算摘要时才在副本上规范化Type.table等明确列出的非语义字段。不能直接对未经规范化的传输字节做语义hash；原始传输完整性另由GETDDL行摘要/原表达式payload校验承担。读写helper归现有`pkg/pb/plan`元数据序列化owner；需要binder/目录的验证留在`pkg/sql/plan`与各入口，避免反向依赖或再建通用框架。

| 原语 | 确切字节规则 |
| --- | --- |
| B | 一个字节0或1，其他值非法 |
| U8/U32/U64 | 无符号，大端，分别1/4/8字节 |
| I32 | 4字节二补码大端；保留负scale，不经无符号转换解释 |
| Bytes | U32字节数随后原始字节；先检查长度/累计预算，再分配 |
| Text | Bytes形式的合法UTF-8，不自动NFC、不trim、不转换大小写 |
| List(T) | U32项数，随后依次T；计数和乘法溢出先拒绝 |
| Optional(T) | B存在标志，1后跟T，0后没有内容；空消息不等于缺失 |
| Digest | 固定32字节SHA-256；JSON外层用64位小写hex |

schema名字使用catalog已解析的规范名；原始大小写是另外的显示信息，不由编码器重新推断大小写政策。无map，不接受尾随字节、未知字段或重复角色。排序按规范名的UTF-8字节序，不能用正在设计的SQL collation来排序元数据。

外层JSON的二进制字段采用标准RFC4648有padding的base64，只允许规范形式；内层结构值不经过浮点数。未知格式失败，不取低位、不按旧版猜解码。所有SHA-256只用于一致性/截断检测，不能替代授权或可信源证明。

## 3. 完整Type与声明

### 3.1 TypeImage

按当前[plan.Type](../../proto/plan.proto)的以下顺序逐项编码，字段不能因值为0而省略：

```text
I32(id), B(notNullable), B(auto_incr), I32(width), I32(scale),
Text(table), Bytes(enumvalues原始字节), U32(charset), B(pad_space),
U32(collation_coercibility), B(collation_coercibility_set),
B(collation_merge_conflict), U32(collation_version)
```

完整传输保留全部13项；enumvalues按原字节保存，不能由JSON/UTF-8替换无效字节，各类型合法性仍交原Type owner。S1/L1计算副本的`table`固定为空串：已有`sameTableDumpBoundExpression`把它识别为catalog lineage；实际目标lineage由目标binder拥有。**其余12项不做别名归一化**，特别不能抹去3/0与3/1、coercibility-set、merge-conflict、PAD、宽度或NULL约束。protobuf中的未知Type字段不得忽略后生成S1；须提升格式或明确不支持。

### 3.2 BindingImage

`Optional(U32(rules), U32(connection_id), U32(connection_revision))`。缺失表示主设计的历史绑定规则；存在只接受rules=1和已支持domain。不能把存在且rules=0当成缺失。它是来源记录，Expr中每个完整Type仍是执行权威。

拟在原Default/OnUpdate/GeneratedCol/CheckDef加入同义可选message：message内fields 1/2/3依次对应上述三个uint32；四个wrapper的候选字段号分别为4/3/4/4，实现前再次核对未被上游占用，使用proto生成器。View的JSON记录使用同样三个字段，不借用表物理标记。

### 3.3 已绑定表达式

传输继续使用现有受限Cols/Checks protobuf payload及其**原始字节SHA-256**，旧v2/v3算法不变。新增字段在v4/typed模式才写入；无表达式时省略payload，不让旧`tableDumpRestoredExpressions`的“必须有表达式”限制变成拒绝普通表。

S1内的`ExprImage`为规范化后`plan.Expr` protobuf的Bytes：

- 先经原wire预算/形状检查和普通DDL重绑定见证验证；未知field、重复非repeated field、多重oneof、非法wire type及不可用builtin/Expr种类拒绝。不得直接信任客户端func ID或ColPos。
- ColRef只允许当前声明列作用域，按照验证过的源列映射转为**可见列序号**；Name用该列规范名，TblName/DbName清空，RelPos规范为0。不能把不在本声明范围的引用清零伪装成本地列。
- 所有嵌套Type.table清空；只将Expr.ndv/selectivity统计字段在摘要副本清零。**AuxId完整保留**：负值由`evalExpression.go`用作单次求值memo身份，不是可随意丢弃的统计字段。其余计算字段也保留，包含Literal.Src、literal/string-source、prepared-numeric中合法的持久语义、函数重载、cast来源、精度、NULL及native domain。不重编号memo，不额外规范化时间宽度，不删除不认识的字段来凑相等。
- Function.ObjectRef须由原builtin binder见证验证稳定的函数名/重载ID；Server/数据库/Schema等本地对象身份不得通过此payload注入。UDF等原来不支持的持久外部调用不因封套获得准入。
- protobuf规范字节：字段号升序，重复项保持原顺序；varint最短形式，proto3非oneof默认标量省略，oneof的显式零和已存在空message保留，fixed32/fixed64按protobuf自身小端规则，字符串原字节。此规则只约束已有消息的规范写法，不重新发明Expr消息。decoder须在预算内遍历全部嵌套边，包括Src和prepared metadata。

声明文本仍在payload与准确CREATE的匹配中校验，但不加入S1的计算语义像。DEFAULT/OnUpdate/GeneratedCol/Check的S1内容分别是：

```text
DefaultImage = B(null_ability), BindingImage, Optional(ExprImage)
OnUpdateImage = BindingImage, Optional(ExprImage)
GeneratedImage = B(is_stored), BindingImage, ExprImage
CheckImage = Text(name), BindingImage, ExprImage
```

wrapper缺失由外层Optional表示。CHECK按规范名排序。这样一个表达式换排版不会冒充域改变；修改实际绑定或创建规则必定改变像。原声明文本改变仍由正常schema generation/DDL核对处理，不依赖这个hash单独判定所有schema变化。

## 4. S1、L1及guard摘要

### 4.1 S1固定顺序

```text
ASCII("MO-COLL-S1\0")
U32(default_identity), U32(default_revision)
List(
  Text(column_name), TypeImage,
  Optional(DefaultImage), Optional(OnUpdateImage), Optional(GeneratedImage)
)                                            // 仅用户可见列，按逻辑列顺序
Optional(List(Text(primary_key_column)))      // 逻辑PK，顺序保留
List(
  Text(index_name), Text(index_algo), Text(index_algo_table_type),
  B(unique), B(effective_visible), Text(index_algo_params),
  Optional(B(create_extra_table), Text(parser_name), I32(ngram_token_size)),
  List(Text(part)), List(Text(included_column))
)                                            // 按name/algo_table_type排序
List(CheckImage)
```

IndexOption的visibility先由既有effective-visibility owner解析为B，不能把旧proto3 false误认作INVISIBLE；其他option字段按上述Optional保留。`part`采用现有IndexDef.parts的已验证表示，前缀/表达式项必须由原index owner解析并核对，不能截掉alias只留列名。params保留原规范化owner产出的原串，不由本编码器另造JSON默认值。物理索引表随机名、idx_id和本地OID不进S1；同名多物理子表由algo_table_type区分。FK/partition等被原CDC投影明确排除的内容不伪装成已复制；本像也不替代一般schema匹配。

`schema_hash = SHA256(规范化后的S1完整字节)`。表默认必须用主设计4.1解码后的域，绝不直接拿表CollationVersion拼进来。

### 4.2 L1固定顺序

```text
ASCII("MO-COLL-L1\0")
List(
  Text(role), Text(index_name), Text(index_algo_table_type),
  U32(table_key_format), U32(index_key_format), U32(legacy_physical_marker),
  Text(physical_pk_column), U32(next_column_seqnum),
  List(Text(physical_column_name), U32(seqnum), B(hidden), TypeImage)
)                                            // 角色排序；每关系按seqnum排序
```

role只允许main/index，main的index_name/algo_table_type为空；key format取真实schema值，不从domain名称推导。主表、隐藏PK与完整索引关系都在原owner校验。物理表名/OID、文件路径、block ID不在L1，归原对象清单与ID映射。`layout_hash=SHA256(规范化后的L1完整字节)`。

CDC的`schema_hash`响应明确改为guard摘要：`SHA256(ASCII("MO-COLL-G1\0") || schema_hash原32字节 || layout_hash原32字节)`，表ID另列返回。完整模式同时给出S1/L1和两个独立hash；hash模式给同一guard摘要，不能两端各自hash JSON。DUMP v4保留旧SchemaHash，新增S1/L1及其hash；GETDDL每表复用同一值。

数据库记录仅携带解码默认domain，不复制源端本地epoch。GETDDL行摘要固定为每行`Text(dbname), Text(tablename), U64(tableid), Bytes(SQL), Bytes(metadata原字节)`依次连接后SHA-256；按现有BIGINT约定检查ID可无损传送，header/database/end的ID=0。end不把自身纳入摘要。

GETDDL第五列JSON字段固定如下；除明确可省略的payload对之外均必需，拒绝额外字段和重复key：

| kind | 除kind与format=1外的字段 |
| --- | --- |
| header | `publication`、`subscription_account`字符串；`scope={level,database,table}`，level为account/database/table，非适用名字为空；`snapshot={name,physical,logical}` |
| database | `default_identity`、`default_revision`，均U32范围整数；库名由第一列给出 |
| table | `sql_sha256`、`schema_image`、`schema_sha256`、`layout_image`、`layout_sha256`；可选的`bound_expressions`与`expression_sha256`必须同时出现 |
| end | `database_count`、`table_count`为U32范围整数，`rows_sha256`为前述行摘要 |

snapshot.physical为源时间戳原有非负I64纳秒值，logical为U32，不转成浮点/日期字符串；名字与请求匹配，初始化的`-`须携带这次完整读取实际使用的固定事务时间戳。库/表名取前两列，与已授权scope及SQL匹配；源表ID只来自第三列并进入原ID映射，不进入S1/L1。完整guard模式的metadata为`format=1`加table行的四个schema/layout image/hash字段；不返回SQL/payload或其他租户数据。

### 4.3 限额与验证向量

单metadata最大64MiB、表达式payload32MiB、单表列/CHECK各16384、wire fields100000、嵌套64、声明文本合计1MiB、AST节点16384、candidate visits1000000；新模式整清单256MiB。规范化会复制表达式，副本同样计入请求预算；protobuf/JSON堆对象归原请求/process，在结束时释放引用，不能声称它们已被mpool统计。已有vector/scratch仍沿原mpool owner清理。不能只计压缩后/base64前的网络字节却无视展开后的节点内存。

固定向量：VARCHAR(20)，notNullable/auto_incr/pad/conflict均false，table/enumvalues为空，charset=3、coercibility=2且set=true。TypeImage分别为：

```text
3/0: 0000003d000000000014000000000000000000000000000000030000000002010000000000
3/1: 0000003d000000000014000000000000000000000000000000030000000002010000000001
```

单列S1使用默认域3/0、可见列c和上述Type、Default存在且null_ability=true/无binding/无Expr、OnUpdate/Generated/逻辑PK缺失、无索引/CHECK。每个像80字节；仅列revision不同。

| 向量 | 字节数 | SHA-256 |
| --- | ---: | --- |
| Type 3/0 | 37 | `44ffc6ad92d28e8df304f3857c1a0e32f6af6a8c80182dc3976bb2acf9708a78` |
| Type 3/1 | 37 | `3b3e068e3eb9983ca46873fbd6cc902d6f7881e5c3a0dcd6ae3b73ef31ab0f15` |
| S1 legacy列 | 80 | `e67f172daa8aa621e3b0fcf9dcc1e10bc6cc866ef350bec22177e8486c37f3b0` |
| S1 native列 | 80 | `c592e59e16f45ce4792f5ee62cf413ad9206400da283e180d2c1900615a5557a` |

本地按字段手工拼接与struct两路核对，并检查I32(-1)=ffffffff及Text分段不碰撞。它们是格式草案向量，非产品encoder、数据库或升级测试；代码实现后还须固定为真实owner的回归向量。

## 5. MO目标：一个CREATE请求，不创建后补丁

推荐在现有frontend命令分派处增加窄的CDC输入适配：

```text
__++__internal_cdc_create_v1 <base64url-no-padding的单个JSON封套>
```

外层单token仅为传输，内部二进制字段仍用第2节base64。JSON固定字段为`format=1, database, table, create_sql, sql_sha256, schema_image, schema_sha256, layout_image, layout_sha256, bound_expressions, expression_sha256`；无表达式的最后两项必须成对缺失。只允许一个普通、非临时CREATE TABLE，不支持任意SQL列表、ALTER/REPLACE/AS SELECT/LIKE、文件路径或调用者指定catalog owner。原CDC已排除的外部/cluster/FK/partition边界不因封套放开。

流程必须是：

1. 在解码前检查包/编码长度；严格JSON、格式、hash、对象名、SQL单语句和预算验证。当前请求收齐后才能继续，不提供分片会话状态。
2. 把真正的目标CREATE AST送入**原普通CREATE权限检查**；不是按内部命令名字授予系统权限，不用guard的INSERT权限代替CREATE TABLE/数据库权限。仅INSERT用户仍不能自动建表。源账号/OID/属性不构成授权。
3. 使用原`AnalyzeTableDumpBindings`/声明匹配验证source Expr，再由原`WithPersistedDDLReplay`补齐的受限输入边界传入准确目标库/名/SQL。普通binder仍生成列、逻辑/物理PK及索引；source表达式和目标列按已校验的可见名称/序号映射，不能只靠当前helper中的tableName匹配。
4. 源表映射到另一个目标名字时，使用已有DDL声明重命名/colID映射owner处理限定引用，不做字符串替换。每条声明用自己的BindingImage，普通用户会话配置不改变；目标本地OID和权限归目标owner。
5. 完整校验后才进入**同一次正常CREATE事务**，不是先CREATE再ReplaceDef，也不新增恢复状态机。原engine的排他名字锁/提交点处理并发同名创建；失败无半建表。结果仍为普通CREATE的OK/error，随后原guard完成live schema验证与ack。

将适配生成的AST当作普通CREATE进入既有frontend pipeline，不能另写一份直接调用engine.Create的特权分支。源SQL/表达式中的builtin准入也沿原binder，不能用校验和绕过。payload不完整写入错误日志。

原capability入口另接受`'create_bindings_v1'`，只返回整数1表示这个输入格式可用，不读取目标、不授予CREATE权限；发送前在原目标连接/owner fence下检查。旧目标、权限拒绝、未知格式都不回退到普通CREATE。

目标已存在时不调用创建适配，走原身份+schema校验；不让IF NOT EXISTS吞掉不同定义。首次准入在观察到absent时使用非幂等CREATE。发送后断连导致结果不确定，沿现有TargetPreIdentity/TargetIdentityAck的恢复判定，禁止盲重试、DROP、自动“接管同名表”；没有既有所有权证据时暂停，由原显式恢复流程处理。不存在端到端exactly-once成功承诺。

## 6. MySQL目标：值语义导出，不发送MO protobuf

不要求MySQL理解MO的绑定封套。选择在现有`ConstructCreateTableSQL`/持久表达式重建owner中补齐**受限的MySQL值表达式投影**，输出一个普通CREATE；不增加session模式，不用CREATE+多条ALTER，也不新增函数registry或SQL执行器。仅相关声明进入该投影，普通legacy CDC的既有输出不全局改写。

关键不变量：输出表达式在每个collation敏感运算处使用源端**已解析的操作数域**；不是把每个原始字符串都加COLLATE。后者把coercibility 4改为0，可能抢过原本应胜出的列域，甚至制造illegal mix。使用已有`expressionCollationIndexes`/共同域选择及实际函数消费者，不按SQL字符串猜测。

| 节点/边界 | 投影规则 |
| --- | --- |
| 已折叠常量且无动态Src | 输出准确常量值；数值/NULL与字符串/bytes分开，复用现有类型/字面量formatter，不重新计算OriginString |
| 原合法实时表达式 | 保留其运行时计算，沿既有Src/函数owner；不得把CURRENT_TIMESTAMP冻结成导出时值 |
| 列/普通函数 | 使用已验证列映射和原tree formatter；保留参数顺序、显式cast及数值精度，不自行改写函数含义 |
| native GeneralCI字符串比较、IN、BETWEEN、NULLIF及字符串选择边界 | 给该边界的相关操作数使用`CONVERT(value USING utf8mb4) COLLATE utf8mb4_general_ci`，所有参与项遵守同一个已解析域；NULL安全运算符保持原样 |
| 历史字节比较边界 | 用无长度的`CAST(value AS BINARY)`，不能用PAD SPACE的utf8mb4_bin冒充NO PAD字节比较；CHAR裁剪按源已绑定Type/消费者规则显式保持 |
| IF/CASE/COALESCE及选择函数结果 | 保留原分支结构和字符值；上层边界重新按其已解析域处理。不得把比较用binary/权重作为实际返回文本；必要时用UTF8转换恢复值类型。新3/1的LEAST首项/GREATEST末项按主文4.4直接映射，不倒置实参 |

投影在原声明重绑定验证时，临时保留SQL节点到已选域/转换的映射，按原AST形状输出；映射仅属于当前请求，受相同AST节点/字节预算限制，不持久化、不成为另一个authoring框架。不能将lowered后的`IF(a=b,NULL,a)`简单复制为NULLIF的导出形式；保留经验证的原NULLIF/IN/BETWEEN语法及每个操作数原有出现次数。已折叠且无动态Src的子树用权威常量，运行时子树保留；已有隐式转换则按已验证类型显式输出。

native GeneralCI准入的全部普通DDL表达式形状必须有对应投影与最近负对照，不允许将“renderer尚未支持”变成普遍拒绝带DEFAULT/CHECK/生成列的native表。旧表达式仍按自己记录的域与规则验证，不因为外层列/另一个CHECK启用3/1就改写成新域。MO独有函数/类型及原有UCA CDC等跨产品不兼容仍属原边界；须明确列出具体不兼容，不把整个general-ci表归入unsupported，也不声称实现任意MO表达式到MySQL的通用等价编译。

此投影只声称MySQL可表达部分的值/比较关系等价，不要求保存MO的Expr protobuf、coercibility元数据或物理布局。必须逐操作验证而非仅检查输出包含COLLATE。特别覆盖旧默认+新CHECK、连接collation与列不同、尾空格、NULL、多级IF/CASE和ß/ss；不能用一次SET NAMES代替。

目标核对也要检查声明：沿原事务MDL，以information_schema.columns/CHECK_CONSTRAINTS/TABLE_CONSTRAINTS或SHOW CREATE取得实际DEFAULT、ON UPDATE、generated和CHECK，解析为AST后仅规范化括号/关键字及不改变已验证类型的字面量拼写，再与**投影后的期望**比较。二进制/文本区别、COLLATE、CAST、实参/分支顺序和出现次数都不能规范化掉；不能拿MO的S1与MySQL内部格式直接比hash，也不能忽略缺失CHECK/生成列。权限/元数据不可见时，在任何数据DML之前失败。一般schema不兼容不会靠删除约束“修复”。

新MySQL只读oracle已经证明：一个coercibility=2的bin操作数，与普通abc字面量比较为0，强加general-ci后变为1；bin PAD与CAST AS BINARY对尾空格也不同。这仅支持上述设计反例，不是CREATE/CDC运行验收。

### 6.1 选择函数缺口的决策

MySQL 8.4.11在general-ci下的GREATEST同权值选末项，MO当前collated owner选首项。**不在导出器仿真旧策略，而是在3/1首次准入时按主设计4.4采用MySQL规则**；LEAST保持首项。旧3/0/UCA仍走原owner的原策略，不做全局替换。新规则属于新域首次发布的完整合同，不更改权重或再加持久函数标志。

因此新3/1的`GREATEST(e1,e2,...)`直接输出同顺序、同次数的调用；纯转换仅包住原节点，不重复子树。DEFAULT中的RAND/实时函数不倒序、不复制、不预先求值，不需要跨连接传memo。CHECK/generated仍沿原`checkExprForVolatileFunc`准入；DefaultBinder能否使用某种memo/rewrite仍按原实际规则，不借本功能放开以前不合法的表达式。

这也划清证据范围：保证此投影不新增重排/重复，不把各引擎已有NULL短路差异、随机序列、UUID算法或时钟同步纳入新的互操作承诺。CDC对普通列写入源端已物化值；真正需要目标计算的generated表达式按其原确定性约束处理。新域选择值/原文、普通DEFAULT与未来写入的验收仍须完成，不能用这些边界豁免general-ci错误。

### 6.2 可复核的最小MySQL oracle

独立MySQL 8.4.11/arm64结果：

| 输入（连接general-ci） | LEAST的HEX | GREATEST的HEX |
| --- | --- | --- |
| a, A | 61 | 41 |
| A, a | 41 | 61 |
| á, A, a | C3A1 | 61 |
| a空格, a | 6120 | 61 |
| 空串, 空格 | 空串 | 20 |
| ß, S | C39F | 53 |
| 任一NULL | NULL | NULL |

```sql
SET NAMES utf8mb4 COLLATE utf8mb4_general_ci;
SELECT HEX(LEAST('a','A')), HEX(GREATEST('a','A'));
SELECT HEX(LEAST('a ','a')), HEX(GREATEST('a ','a'));
SET @calls=0;
SELECT HEX(GREATEST(IF((@calls:=@calls+1)=1,'a','x'),
                    IF((@calls:=@calls+1)=2,'A','y'))), @calls;
```

最后一行得到41和2，用于SELECT求值观察；**用户变量不是DEFAULT设计或实现机制**。实际DEFAULT另外使用独立连接中的TEMPORARY TABLE验证，无持久表/函数或GLOBAL改动：

```sql
-- 在已存在的隔离测试库中执行，关闭连接也会销毁临时表。
CREATE TEMPORARY TABLE gc_selection_probe (
  id INT PRIMARY KEY,
  a VARCHAR(8) COLLATE utf8mb4_general_ci,
  b VARCHAR(8) COLLATE utf8mb4_general_ci,
  g VARCHAR(8) DEFAULT (GREATEST(a,b)),
  v VARCHAR(8) DEFAULT (GREATEST(IF(RAND()>=0,'a','x'),'A')),
  stamp DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
  generated_value VARCHAR(8) GENERATED ALWAYS AS (GREATEST(a,b)) STORED
);
SET SESSION rand_seed1=123, rand_seed2=456;
SELECT RAND(), RAND(), RAND(), RAND(), RAND();
SET SESSION rand_seed1=123, rand_seed2=456;
INSERT INTO gc_selection_probe(id,a,b) VALUES (1,'a','A'),(2,'A','a'),(3,'a ','a');
SELECT RAND();
SELECT id,HEX(g),HEX(v),HEX(generated_value) FROM gc_selection_probe ORDER BY id;
DROP TEMPORARY TABLE gc_selection_probe;
```

参考RAND第4项与3次默认值之后的下一项同为`0.00006629247224544405`，观察到每行一次调用。g/generated分别为41、61、61；v均41。另以会话`timestamp`设定不同的建表/写入日期，确认CURRENT_TIMESTAMP按每次写入时刻计算，UPDATE后generated重算；无sleep或概率判断。该oracle支持规则选择，**不代表MO实现、跨引擎CDC、旧版本或失败原子性已经验证**；这些仍在主设计第7节交付门内。

## 7. 源端写删两条反向链也要保持一致

当前CDC builder排除了hidden列，却仍列出可见generated列；stream又假定逻辑单列PK必无隐藏复合key，DELETE把复合key交给普通Unpack。这些假设不能原封不动套在新的隐藏native PK上。

- INSERT/UPDATE SQL只写普通可见列；generated列由目标相同语义的表达式计算。RowIterator仍读取完整源行，builder保存源序号→写入序号映射，不能直接缩短row slice造成位置错读。UPSERT同样不赋值generated/hidden列。generated作为逻辑PK时，DELETE仍使用它的逻辑值域。
- AtomicBatch净效应去重使用真实**物理PK列**的key，不按`len(Pkey.Names)==1`选择原文单列。这样同权值大小写更新的insert/delete属于同一key；SQL写入部分仍取原文列。
- 对native GeneralCI物理PK的DELETE，不能把权重传给普通serial_extract/Unpack当原文。推荐由原tuple/keycodec owner验证schema、外层格式和C1 framing，再生成**仅供等值谓词的代表值w**，满足`K(w)=k`。这不是恢复原文，不能用于INSERT、回表、用户SELECT或任何持久列。
- 这个受限代表值只适用已冻结GeneralCI：当前ValidateKey要求每个非空格权重本身是合法且不动点的BMP字符。解析C1的空格标记和终止符即可给出同权值字符序列；再次编码必须逐字节等于原key，否则失败。不猜domain，不支持UCA逆推，不新增映射registry。复合key逐分量按既有物理格式处理，非native分量保持原值。
- 因为目标已验证为同一native GeneralCI关系，普通`WHERE logical_pk=w`能使用其索引，且不会冒充用户原始文本。对ß，代表值S可匹配，而ss不匹配；INSERT依然保留原始C39F。接收端是3/0时不得应用此规则。

这是一项新的、须明确审批的keycodec能力建议；交付前必须有全冻结权重不动点检查、非法帧/截断/空串/NUL/内部空格/补充字符/复合键、同权值UPDATE净效应和真实CDC删除证明。它优于另建旧行preimage查询/GC保留状态，也不允许“反推原文”作为优化捷径。若原keycodec无法满足此严格右逆合同，须重新评审，不静默退成全表扫描或拒绝native PK。

# 分组扩展的表达式所有权与 CTAS 一致性设计

## 0. 版本、范围与审批状态

- 文档版本：**v2**，2026-11-13；v1 的独立审查为 REQUEST_CHANGES，§4.6—4.7 关闭两项设计决定缺口，并在§8区分尚未完成的实现修改。所属 [issue #32265](https://github.com/matrixorigin/matrixone/issues/32265)，实现 [PR #32520](https://github.com/matrixorigin/matrixone/pull/32520)，目标 `4.1.6-hotfix`。
- 实现审阅锚点：`16082594b63b24b0ecf4aba43c92e15edc4f8a0b`，已合并目标 `9937f1b74a7a7a01f8e0dcb6e7d3ff447e0ce29e`。本 PR 自有实现与 `0a88c1d8cd412b106901b95dfcb7ac4ad949be6e` 相同；新增基线包含 #32609、#32596、#32591，不归为本设计的实现成果。
- **审批状态：独立设计审查结论以相邻版本化审查记录为准；维护者批准待定，实现批准路径暂停。** 本文的设计决定不代表代码已符合；§8列出的实现阻塞必须另行关闭。PR 正文必须链接被审阅的精确 Git revision，而非只链接可移动分支。
- 这是针对已经实现的改动补做设计门禁。此前局部修复演变为架构变更时没有及时重新分类；不把历史实现、测试通过或用户授权写文档当成设计批准。本轮先冻结设计，再单独评审并记录决定；任何实现修改须在设计决定后另行验证。
- 触发门禁：排除测试、生成 parser 和上游合并后，手写生产 Go 为新增651/删除35，共686行增删（另有grammar 5行）；更正早先786行的误计；更重要的是改变分支输出与全局消费者的求值、身份及类型证明边界，并影响热路径。
- `groupingStage` 是 query-local binder 状态及 lowering 辅助逻辑，**不是新执行算子、协议字段或跨查询缓存**。这种区别不豁免设计审查。

## 1. 问题、目标与不变量

### 1.1 原始问题及扩大的问题

CTAS 会绑定 SELECT 推导表结构，再格式化源查询生成 INSERT SELECT。原来 ROLLUP/CUBE 展开修改源 AST，格式化还丢失显式空/重复 grouping sets，且补 NULL 的键保留输入 NOT NULL 类型；可导致重绑改变行重数、CTAS 解析失败或 3819 非空约束错误。

仅修格式化会让原本不能执行的 CTAS 开始执行旧 planner 的错误结果。后续反例证明，旧实现把完整 SELECT 放进每个分组分支，分别运行 DISTINCT、窗口和投影后再 UNION ALL，不能表达整个 SELECT 的正确求值顺序。新边界自身又暴露 bitmap、别名身份、SAMPLE、复合 nullable 等问题，不能将这些都称为原有缺陷。

目标是使已支持的 grouping SQL 在 SELECT、视图、CTAS、显式 INSERT SELECT、重复绑定中保持一致的值、行重数、输出类型和错误。CTAS 不另设一套 grouping 语义。

### 1.2 必须成立的契约

| 编号 | 不变量 | 最小反例/否定 |
| --- | --- | --- |
| I1 | 每次绑定独立展开；格式化保留空/重复集合与原别名语义 | 同一 AST 绑定三次，多出小计或 CTAS 字符串改变 |
| I2 | 每个集合贡献自己的多重集；只有全局 DISTINCT 消除最终选出值的重复 | `GROUPING SETS ((g),(g))` 的 DISTINCT 仍输出四行而非两行 |
| I3 | 缺失键在分支输出补 NULL，输入编码类型不被污染；行来源不等于值是否 NULL | 实际 NULL 与 subtotal NULL 的 GROUPING 相同，或 CTAS 声明缺失键 NOT NULL |
| I4 | 每个引用指向已产生它的 owner/tag/ordinal；独立表达式不因结构相同而合并 | 隐藏排序引用同层 PROJECT 的未来输出；`ORDER BY concat(b,'')` 使用 a |
| I5 | 全局窗口/投影消费者看到完整分组结果；排序和 LIMIT 不改变窗口输入域 | `COUNT(*) OVER()` 分支内计数，或 LIMIT 先截掉小计 |
| I6 | NULL 证明随孩子变更自底向上重算，再供 flatten/MARK 优化及 CTAS 使用 | `g+0 IN (...)` 的 subtotal 左值还被认为非空，`NOT flag` 错留 NULL |
| I7 | DISTINCT 之后的隐藏排序只能依赖仍可获得的选出结果；递归检查列表/包装 | 未选出的 h 藏在 `(g,h)` 中越过 DISTINCT，执行时越界 |
| I8 | 运输集合由消费者决定，不能把未用宽键全部带过 UNION | count-only 查询运输两个 2 KiB 键；所选 volatile 因排序被重新执行 |
| I9 | 失败不发布不可执行的表达式/部分目标表；重试/重绑不继承上次 binder 状态 | DISTINCT PROJECT 残留 Sub；失败 CTAS 遗留目标表 |

### 1.3 明确边界

- 非目标：原生一次扫描多 grouping-set 算子、共享各分支 scan、优化 SAMPLE 算法、通用函数/MARK nullability 重设计、pipeline 生命周期改造。
- 保留普通 GROUP BY、公开 SQL UNION 的 common-type 规则及已有错误类；不以“内部 UNION”模式改变用户 UNION。
- 继续排除分区表、存储过程、UDF、warning 兼容性。SQL TIME WINDOW/FILL 的现有能力不因本改动而扩展；普通分析窗口 `OVER` 属于本设计。
- #32549 的普通 grouped SAMPLE passthrough panic、#32595 的 SAMPLE 后置投影绑定错误不在这里修复，不作为通过证据。
- 保留该分支空输入 ROLLUP/CUBE 的零行行为；**显式纯空集合** `GROUPING SETS ((),())` 则在空输入上贡献两行全局聚合。不能用“所有空集合等价普通全局 AGG”覆盖这一既有差异。
- 不承诺未指定 ORDER BY 的行序、随机 SAMPLE 的逐值相等、每个标量表达式相对于无关错误的固定执行先后。

## 2. 现有边界与标准参照

目标分支的 `bindSelect` 展开 ROLLUP 为 K+1 个集合、CUBE 为 2^K 个集合，为每个集合构造完整 SelectClause 后调用 `buildUnion`。每个普通分支沿用：

```text
FROM/WHERE → AGG 或 SAMPLE → HAVING → WINDOW → PROJECT → DISTINCT
                       各分支最终输出 → UNION ALL → ORDER BY/LIMIT
```

`buildUnion` 默认按公开 SQL set-operation 协调类型（例如 BINARY/VARBINARY 转 BLOB）；`remapAllColRefs` 剪枝和重编号，CTAS 从最终 ProjectList 推导列定义。分组键的值类型、grouping bitmap、用户裸 NULL 来源、别名表达式身份是不同的信息，不能只拿最后一层值或 SQL 文本恢复。

参考语义（采用原则，不宣称完全实现这些产品的语法/优化器）：

- [PostgreSQL 18：GROUPING SETS/CUBE/ROLLUP](https://www.postgresql.org/docs/18/queries-table-expressions.html#QUERIES-GROUPING-SETS)：多集合组合及缺失列 NULL 的关系语义。
- [PostgreSQL 18：窗口处理](https://www.postgresql.org/docs/18/queries-table-expressions.html#QUERIES-WINDOW)：窗口处理发生在 grouping/HAVING 之后。
- [MySQL 8.4：GROUP BY modifiers](https://dev.mysql.com/doc/refman/8.4/en/group-by-modifiers.html)：GROUPING 区分 super-aggregate NULL 和实际 NULL；本分支 ROLLUP 空输入行为已有 MySQL 8.4.11 对照。
- SQL 的 bag 语义、三值逻辑、输出别名身份是正确性要求；SAMPLE 是 MO 扩展，以现有普通 grouped/global SAMPLE 为参照，不借用其他数据库的 TABLESAMPLE 契约。同一 SELECT 的 `SAMPLE + OVER` 仍按现有 validSample 返回 NYI；下面的统一流程图不新增该组合支持。

## 3. 替代方案与选择

| 方案 | 正确性/复杂度 | 性能、兼容与运维 | 可测性/交付代价 | 决定 |
| --- | --- | --- | --- | --- |
| A. 只修格式化/AST/nullability，完整 SELECT 仍在各分支 | 原始 CTAS 面较小；不能解决全局窗口与 DISTINCT，不可静默放行会写错的新 CTAS | 最小 planner 扰动；必须收缩功能或对未闭合组合明确拒绝，可能收紧原 SELECT 能力 | 局部 UT/BVT 较小，但必须重新界定支持范围、撤回已有修复并获产品同意 | 作为可行的范围收缩方案保留；**不满足本次保持现有公开 grouping/DISTINCT/分析窗口/SAMPLE 能力并纠正其组合语义的目标**，不是技术上绝对不可行 |
| B. AST 规范化为外层 SELECT + 内层分支 UNION | 复用普通 binder；仍需提取 aggregate/SAMPLE/key 依赖、生成隐藏列、保存别名/子查询 scope/GROUPING 来源 | 可以用既有节点；公开 UNION coercion 和 derived-table 边界仍可能丢 bitmap/provenance；额外 AST 复制与重绑定 | 可通过同一反例矩阵；CTAS 序列化前后、别名 shadowing/correlation 需额外证明；不天然比 typed lowering 小 | 可信替代方案；当前不选，避免把已绑定身份退回 SQL 名称再解析 |
| C. 显式 binder 内部运输边界，复用已有执行节点 | 在已绑定的 tag/ordinal 上确定 owner，只把分支产物送过 UNION；集中维护一份表达式遍历/重绑定契约 | 仍保留原分支扫描数；只运输用到的列；新增 query-local 状态与复制，不新增长期资源/协议 | typed UT 可直接证明引用、类型与阶段；SQL 可独立证明结果。代价是必须审查新边界，不能当局部修补 | **选择**；接受新增约 403 行 stage 模块及配套 binder 修改的维护代价 |
| D. 原生 grouping-set AGG，单次输入产生多个集合 | 最直接表达多 grouping 输出，但要求分布式 partial/final aggregate、SAMPLE、bitmap、剪枝一起设计 | 可能减少重复 scan，但增加执行器/资源/分布式契约，不能未经测量承诺收益 | 新算子/plan/runtime及取消/多CN验证显著超出 hotfix | 长期可研究，本次不选 |

选择 C 不是因为代码已经写好：**已绑定身份保持、内部同构类型、SAMPLE 的分支物化**是主要理由。A 是有损收缩，B 仍必须实现相同语义边界，D 扩大 runtime 风险。若维护者认为 C 的 hotfix 风险不能接受，正确行动是暂缓发布或批准重新限定范围，不是重命名/内联 `groupingStage` 绕过门禁。

## 4. 所选所有权模型

### 4.1 顺序与发布点

```text
源 AST（逻辑语义保留；每次绑定创建局部展开）
 ├─ 分支0 FROM/WHERE → AGG/SAMPLE 输出证明B → 分支 HAVING → 私有运输 PROJECT
 ├─ 分支1 FROM/WHERE → AGG/SAMPLE 输出证明B → 分支 HAVING → 私有运输 PROJECT
 └─ ...（SAMPLE 的所选物化/相关 flatten 也必须先消费证明B）
        ↓ 同构 slots，保留 bitmap/来源，不走用户 UNION 的强制转换
      UNION ALL
        ↓ 重新定位 tag/ordinal，发布合并输出证明U，刷新派生 NULL 证明
      WINDOW → window-dependent HAVING
        ↓ flatten 子查询后产生最终选出值
      selected PROJECT → [全局 DISTINCT]
        ↓ 仅基于可用输出重建隐藏排序
      PROJECT → ORDER BY → LIMIT/OFFSET → 仅公开用户结果列
```

这不是“所有表达式都移到 UNION 后”：AGG 输入、分支 HAVING、SAMPLE 所选产物必须在其 owner 处求值。最终执行计划只包含既有节点。binder 失败只返回错误；成功经已有 createQuery/remap 校验后才交给 compiler。CTAS 的建表/插入事务与失败清理由既有 frontend/DDL 路径拥有，本设计不另建提交协议。

### 4.2 Owner 与允许跨越的数据

| 状态/表达式 | 首个 owner | 跨边界形式与禁止事项 |
| --- | --- | --- |
| 源 grouping AST/别名 | parser/调用者；展开属于本次 binder | 局部复制展开列表/HAVING 标记；不向原查询写回展开结果或用输出表达式替换 ORDER BY 别名。不是宣称整个现有 AST 永远不做任何 qualification |
| group/aggregate 输入 | 各分支 BindContext + AGG | groupTag/aggregateTag 的列引用及分支输出类型；禁止把输入编码类型改为缺失键的 nullable |
| group membership | GroupBinder 的完整根键索引与 groupingFlag | 必须成功查找根键；子表达式不能默认命中 ordinal 0 |
| AGG 分支 lineage | 既有 aggregate/grouping bitmap | 经过直接运输列及 UNION 保留；不能用 `IS NULL` 推导；不能插入 bitmap 丢失的二进制 cast |
| SAMPLE 值/lineage | 各 SAMPLE 分支 | 缺失键作为 typed NULL 分组输入；GROUPING 在此按 groupingFlag 算 mask；所选 post-sample 值先物化 |
| 消费依赖索引 | groupingStage.inputs | `(owner tag, ordinal) → 私有 slot`；只记录需要的 group/aggregate 列，按统一键/聚合顺序发布 |
| SAMPLE 预折叠身份 | groupingStage.sampleProjects | GROUPING 折叠前的副本；保留 marker 与用户字面量 0 的区别，不作为运行时二次采样计划 |
| 用户 projection/裸 NULL 来源 | 首分支 stage，合并后输出 context 接管 | user slot 的 bareNulls 与 transport slot 独立；不能拿同一个下标套用到不同 schema |
| 窗口、非 SAMPLE 最终表达式 | 合并后输出 context | 复制首分支已绑定消费者并重绑定；不在所有分支执行窗口 |
| ORDER BY 所选别名 | selected-output tag/ordinal | 精确引用 a/b 槽位；不靠结构相等复制/复用 volatile 或独立子查询 |
| 子查询内部 plan | 子查询自身 context | 不递归重写其内部 tags；量化 SubqueryRef.Child 是外层操作数，必须遍历 |

各 branch 根据同一 select/group 结构绑定，只允许 membership 与由此导出的 nullable 不同。slot 含义、数据类型族/宽度/精度、slot 数必须同构；因此允许跳过 SQL common-type coercion。这个证明是内部模式前提，不适用于任意用户 UNION，分支 schema 产生差异时不能默默取首分支。

### 4.3 身份不是表达式结构相等

普通 alias 消费的是已选出的值；`GROUPING(alias)` 例外，描述分组键来源。`AliasAsOutput` 在 grouping DISTINCT/SAMPLE 的 ORDER BY 绑定期间保留别名语法，局部 `orderByProjects` 只存精确输出引用，函数退出时 defer 清空。GROUPING 绑定实参时恢复相应的 bound grouping origin；别名遮蔽源列时仍取该别名来源。

结构相等仅作为确定性表达式的备用匹配；不能合并 `hex(random_bytes(8)) a` 和独立的同形 b，也不能忽略 Subquery NodeId 用 SQL 文本去重。对 selected subquery alias，必须在 flatten 把身份改成 join 输出之前解析排序依赖。

SAMPLE 隐藏排序可能同时需要 selected chosen 和未选出的 marker：`chosen-100*GROUPING(g)`。处理方式是单独运输 marker、复用 chosen，再在合并后组合；禁止把整棵表达式塞回尚未生成 chosen 的分支 PROJECT，禁止通过重新执行 chosen/volatile 消除自引用。

### 4.4 值类型、NULL 证明与 GROUPING

- 缺失键只弱化输出 `NotNullable`；公共键、COUNT、有效非空 COALESCE fallback 保留对应证明。
- UNION 输出列类型由同构输入合并 nullability；对函数、窗口按孩子先于父的顺序使用既有 `DeduceNotNullable` / `DeduceWindowNotNullable`。未知证明不得升级成 NOT NULL。
- 每次证明更新必须先于该层的 quantified-subquery flatten/MARK 优化；不只是在 UNION 之后刷新。分支消费者先取证明B、全局消费者再取证明U，详见§4.6。本设计不改 MARK→ANTI 的算法，只保证其前提真实。
- 逻辑窗口的 partition/order/function 依赖都要采集；最终 remap 后 PARTITION-only key 属于上游 PARTITION 输入坐标，可能已从 WINDOW child 剪掉，不能再按 WINDOW 的坐标访问它或用越界 guard 掩盖 owner 错误。
- 对 AGG，GROUPING 读取保留下来的键 bitmap；对 SAMPLE，它是分支 origin 的标量 mask（按参数顺序左移，缺失为 1），不可能从采样后 NULL 值重新计算。
- 对 **AGG 与 SAMPLE 的 DISTINCT 都一样**：选择键 g 不授予对其隐藏 GROUPING marker 的访问。bitmap 能运输不等于 DISTINCT 等价类上来源唯一。具体保守准入规则和现有实现差距见§4.7；不得把来源加入去重键来规避歧义。

### 4.5 行重数、优化与错误

- 多集合先 UNION ALL，再以**用户结果列**做 DISTINCT；隐藏排序值不能被加入去重键改变行重数。
- 显式无键分组又没有 aggregate/SAMPLE 时，隐藏 COUNT 只维持全局单行 cardinality；不能对 SAMPLE 注入 COUNT。
- omitted-key AGG 不可因原始键为 PK 删除；对补 NULL 键的输出谓词以及无键全局 AGG 的输出谓词不可下推成输入 WHERE。公共键仍可安全下推。
- hidden order 的函数/tuple/list 必须逐层证明来源；不可用依赖按既有 1149 syntax error 拒绝。多行标量子查询与原有不支持的嵌套左子查询继续返回原错误，不转成 NULL/空集。
- flatten 完成的公开 PROJECT 不得残留可执行期不能消费的 Sub；CTAS 失败后目录中无目标表、服务仍能执行后续查询是独立公有断言。

### 4.6 设计决定：两层 NULL 证明，覆盖最早消费者

v1 把刷新描述为 UNION 后操作，漏掉分支 HAVING 及 SAMPLE 物化之前的 flatten；仅补最终列 metadata 不足以修复已经做出的优化决定。

**证明B（分支逻辑输出发布点）**：groupingFlag 与完整根键映射确定后、任何消费 AGG/SAMPLE 输出的表达式进入 flatten/布尔简化/NULL 敏感优化之前，创建该分支输出类型视图。缺失 group key 的输出 NotNullable=false；聚合输出仍由其函数规则推导，采样原始值仍用 SAMPLE 自身语义。保持 owner tag/ordinal，不在这一层改为 UNION tag。

- 作用域包含分支 HAVING、SAMPLE 所选 post-sample 表达式与分支隐藏依赖，以及它们的复合孩子和量化 `SubqueryRef.Child`。逐节点孩子先于父地刷新；若函数仍将子查询转换为 MARK，转换输入及其比较表达式必须已读取证明B。
- AGG 的输入表达式/编码类型、采样算子的原始 `sample(v)` 输入不是缺失键输出，不可一并改为 nullable。`groupTag` 引用描述的 null-extended key 与 `sampleTag` 描述的采样值必须区分。
- 证明B是**逻辑类型发布时序**，不要求物理 AGG node 必须先 append；任何更早消费此输出类型的 binder/flatten 调用都须先获得证明B。不能寄希望于最终 remap 再修复。
- 子查询内部 context 的 tags 不做无差别替换；其已有外层相关依赖必须由既有 correlation/flatten 所有者正确转接。若出现不能转接的相关形态，保留原有拒绝而非输出悬空引用；不借此新增相关子查询支持。

**证明U（合并输出发布点）**：同一 slot 的 branch proof 合并后、全局 window/project/subquery 消费之前，绑定到 UNION 输出；再次自底向上重算派生证明。输出强非空必须是所有可能生产分支均能保证的结论。任何后续替换孩子的 rewrite 仍须保持既有证明刷新契约，不能把证明U当永久属性。

独立验收输入：`t(g NOT NULL)` 中为1，`u(x NOT NULL)` 中为2，`... GROUP BY g WITH ROLLUP HAVING NOT(g+0 IN(SELECT x FROM u))` 只能保留明细1，subtotal为UNKNOWN必须删除。SAMPLE 早期消费者增加同一 key-output/原始sample-value对照；使用 SAMPLE 最后、100% 的合法形态避开 #32595。对每种场景覆盖空 RHS、含NULL RHS、`COALESCE(g,0)` 非空fallback，分别检查 flatten 前类型、MARK转换前提、SELECT值及CTAS残表。若合法公有SAMPLE表达式无法到达某内部节点，就用可达的分支HAVING证明公有语义，并把synthetic类型测试明确标为内部契约，不伪造SQL复现。

### 4.7 设计决定：DISTINCT 后只允许来源明确的 GROUPING 排序

令用户选出tuple为 V，DISTINCT 按 V 的值等价关系折叠，**不包含隐藏lineage**。实际NULL组与subtotal可能具有相同V、不同mask；任意挑选一个bitmap继续排序没有定义，额外把mask加入key又改变 DISTINCT 的行重数。

本版本选择保守准入，不增加数据/约束驱动的唯一来源证明：

1. ORDER BY 子树若指向已选出槽位，或与可安全复用的完整选出表达式匹配，使用那个结果，不重新执行。`GROUPING(g) flag` 被选出，`ORDER BY flag` 或 `flag+chosen` 合法；选出 `GROUPING(g)+1 x` 后 `ORDER BY x` 同样合法。
2. 对其余可组合的函数/list递归处理；遇到**尚未被完整选出表达式覆盖的 GROUPING 调用即1149**，无论AGG还是SAMPLE，也不因g非空/PK/当前数据无NULL而豁免。普通可由选出值重建的函数仍合法。
3. `GROUPING(alias)` 先按§4.3解析键origin，再进行相同的选出依赖匹配；既不能丢失alias来源，也不能绕过此门禁。
4. 禁止将隐藏marker运输到DISTINCT key，禁止用`IS NULL`推导mask，禁止采用首个/最后一个来源的隐式选择。相同V含不同flag时，显式选出flag就改变了用户tuple，这种保留两行是正确而非重复。

最小反例：`t(g)` 有NULL、1，`SELECT DISTINCT g FROM t GROUP BY g WITH ROLLUP ORDER BY GROUPING(g) DESC,g IS NULL,g LIMIT 1` 必须在绑定/规划时1149，CTAS无残表。选出`g,GROUPING(g) flag`的合法对照结果按flag排序应明确区分实际NULL和subtotal；再加重复集合、无NULL输入、SAMPLE、alias和LIMIT控制，证明此决定不是数据依赖特判。

这是对当前实现的**待实施设计约束**：`groupingDistinctOrder` 当前允许递归重建未选出的AGG GROUPING，不能因v1已有局部测试通过就宣称符合v2。后续修复须保留合法 selected-marker 和普通 `chosen+1` 路径，并以统一AGG/SAMPLE门禁覆盖该边界。

## 5. 状态生命周期、资源与性能

### 5.1 生命周期

状态转移：`新BindContext → 分支绑定 → 私有slot发布 → 合并后重绑定/flatten → 最终plan发布`；任何错误转入 `本次构建丢弃`。stage/map/expr copies 属于 QueryBuilder/BindContext 的 Go heap，随构建对象失去引用释放；没有跨查询 registry、持久化 stage、goroutine、channel 或后台清理任务。prepared 重复执行/CTAS 再绑定必须创建新状态，不能把一次生成的 tags 留在源 AST。

没有新重试/取消状态机；绑定中已有调用返回取消/错误即终止这次构建，不声称新加了 CPU-bound 展开的抢占取消。执行阶段继续由原 PROJECT/UNION/AGG/SAMPLE/WINDOW 的现有 operator、mpool、spill 与 pipeline error/close 所有者负责。输入错误、排序错误、LIMIT early-stop 不授权跳过既有 cleanup；不以成功路径测试替代 compiler/runtime 生命周期证据。

### 5.2 成本模型与明确预算

设 K 为 grouping keys，S 为展开集合数，E 为绑定表达式节点总数，P 为选出列数，H 为隐藏排序依赖节点数，R 为所有分支合计输出行数，W 为实际运输列每行宽度。

- 展开数量不变：ROLLUP K+1、CUBE 2^K、显式 sets 按书写重数。仍可能各分支重复扫描；本设计不承诺 scan sharing 或修复既有 CUBE 指数膨胀。
- stage 元数据/表达式副本按分支与查询大小增长，粗界 O(S×(E+K+P+H))，不是按输入行数增长的 binder cache。结构匹配/递归复制会有超线性规划成本；保守预算按 O(S×H×P×E²) 检查最坏树形，不能宣称全流程线性。
- 运行时运输 O(R×W)，私有 slot 的预算是**已消费 group/aggregate 列**，无依赖时只加一列固定宽度 row carrier；不是所有 K 个键。SAMPLE 运输完整选出值及不可重建的 branch-only 依赖；不为排序重复采样/重新执行选出 volatile。
- 不新增无界持久资源，但没有额外硬性 S/E 上限；沿用现有查询资源治理并保留输入复杂度风险，不能把 O(表达式大小) 当常量内存保证。此 hotfix 不新增配置/feature flag。
- 窗口/全局 DISTINCT/sort 现在确实面对完整 R，所需工作内存可能比错误的各分支结果大，这是语义要求；继续使用已有执行器限额/溢写能力，不宣称每种算子都有统一 spill 上限。

验收预算：count-only transport 不携带任何未消费宽键；纯 formatter/普通 GROUP BY 路径不创建 stage；每个选出 alias 只能引用自己的结果；不增加分支数量或新 scan。性能证据使用 typed output schema、scan/plan 数、必要的单独 benchmark，而非 UT/BVT 墙钟断言。

历史宽键对照（20,000 行、两个约 2 KiB 键，五次交替 warm run）在 pruning 修复后 base/candidate 中位数 50.2/51.2ms，结果均 60,000；只证明该本机样例未再出现先前明显运输膨胀，不是当前最新基线性能验收或生产延迟承诺。若新 workload 需要发布性能 SLA，由发布 owner 在投产前做真实数据与多CN评估；不得从本数据推导通用速度提升。

## 6. 兼容、安全与发布

### 6.1 兼容范围

本 PR 不修改 plan/proto 字段、operator opcode、catalog schema、TAE/WAL/checkpoint 格式或 MySQL wire 编码；新的 binder 状态不会被序列化。生成计划仍使用已有算子，但计划形状改变能影响已有 compiler/远程执行路径，不能由“无协议改动”推出“无分布式风险”。

本项目只支持**停机升级**：停止全体服务、替换一致版本、再启动。不支持集群内新旧 CN/TN 混跑，也不支持新版本写盘后降级；本设计不加 MORPCVersion、capability gate 或 legacy 编码。review 提到 mixed-version 的应答是明确这条支持边界，而不是制造未支持的滚动升级方案。

持久化兼容的实际输入：旧版存储的视图 SQL、既有表数据/列约束、备份中的这些对象。新 binary 应能读取旧格式；重解析视图时应用修正后的分组语义。不自动重算历史 CTAS 数据，不自动修正旧表 NOT NULL，已丢失语义的历史序列化 SQL 也不能被推断恢复。现有 prepared plans 在进程退出后重建，不设计跨版本复用 binder state。

没有 schema migration，不能因此声称已做 release-tag 升级测试。发布 owner 仍须在实际支持的**已发布源版本**上生成普通表与 grouping view，停机切至目标 release 后确认读取/view重绑、新 CTAS 和恢复；具体源 tag/commit 由发布矩阵决定。`af531771cc` 等开发 SHA 是回归对照，不是受支持升级源的证明。

### 6.2 权限与错误面

所有源对象仍经既有 FROM/subquery binder 解析、权限与 snapshot 检查；不增加后台特权 session，不按 SQL 文本跨用户共享结果。隐藏运输列不得出现在结果 headings/客户端 schema 中。新的 NULL 证明只描述值，不改变授权。复杂 grouping 查询已有的指数 planner/执行成本属于容量/拒绝服务风险，上一节不新增通用 admission 机制；对外开放复杂 SQL 的资源限额仍由既有会话/查询管理承担。

### 6.3 发布、观测及回退

1. 设计文档冻结、独立审查完成；维护者在精确 revision 上给出设计决定前，**不得宣称 PR ready/approved**。
2. 按设计矩阵审核现有证据及新增基线影响；必要补齐再进行实现批准/发布。以 bug fix 发布，说明 CTAS 可执行性、结果重数和 nullable 元数据的纠正可能改变依赖错误行为的应用。§4.7的保守准入会将此前接受的部分AGG DISTINCT隐藏GROUPING排序改为1149，这是明确的release-branch兼容取舍；不能在release note中只写“CTAS修复”。应用可显式选出marker后排序，但这会使marker参与DISTINCT，须由应用确认所需结果语义，不能自动代改SQL。
3. 使用既有 EXPLAIN 检查 UNION 输出 schema 与 WINDOW/DISTINCT 所在位置，用 SQL 日志、错误码、查询内存/延迟监控诊断；不增加 per-query 高基数指标或后台任务。
4. 不提供“关掉 stage 继续返回旧错误结果”的运行开关。发布前可在源码中 revert 本 PR 自有变更并重新构建测试，不能直接丢弃上游合并；发布后应前向修复或按经批准的完整备份恢复方案处理，**不能保证旧 binary 直接启动新数据可降级**。不授权自动修改/重建客户历史 CTAS 表。

## 7. 反例 → 验证映射

以下是可执行验证计划，也是历史证据的索引；不要求设计文档重复运行已有效验证，也不以仅 bind 成功代替真实执行。测试文件链接位于同一版本的仓库；具体 `Test...` 名称可直接选择。

| 反例/风险 | 不变量与最便宜 typed UT | 公有/独立 oracle与失败路径 |
| --- | --- | --- |
| 原始格式、CTAS3819、空/重复集合、重复展开 | I1/I3；[tree tests](../../pkg/sql/parsers/tree/group_by_test.go) `TestGroupByFormatRoundTrip` / `TestGroupingQualifiedColumns`；[grouping_test](../../pkg/sql/plan/grouping_test.go) `TestGroupingRepeatedPlanning` / `TestGroupingOutputPreservesInputType` | 普通 GROUP BY + typed NULL UNION 的手算多重集；SELECT/INSERT/CTAS/view 元数据；NULL 插入约束；同 AST 三绑、prepared 三执行 |
| review1：全局 DISTINCT、复合根键、PK subtotal、纯空集合 | I2/I3；[review tests](../../pkg/sql/plan/grouping_review_test.go) `TestGroupingDistinctAfterUnion`、`TestGroupingExpressionFlags`、`TestGroupingPrimaryKeySubtotal`、`TestGroupingEmptySetNoAggregate` | 两个重复集合有/无 DISTINCT、8行表达式 CUBE、PK三行、空输入双常量；普通 PK/常量scan控制 |
| review2：filter boundary、global window、裸 NULL 来源 | I3/I5；[boundary tests](../../pkg/sql/plan/grouping_boundary_test.go) `TestGroupingOutputFilterBoundary`、`TestGlobalGroupingOutputFilterBoundary`、`TestGroupingCommonKeyFilterStillPushesDown`、`TestGroupingWindowAfterUnion`、`TestGroupingProjectionProvenance` | outer `g=1` 不保留 NULL；无键false过滤无行；窗口三行全计数；decimal与公开NULL UNION类型 |
| review3：quantified.Child、DISTINCT Sub、SAMPLE与格式 | I4/I6/I9；[consumer tests](../../pkg/sql/plan/grouping_consumer_test.go) `TestGroupingQuantifiedSubqueryInputs`、`TestGroupingDistinctSubqueryProjection`、`TestGroupingDistinctSubqueryError`、`TestGroupingSampleTransport`；[sample formatter](../../pkg/sql/parsers/tree/sample_format_test.go) | IN/NOT IN/ANY/ALL 的 NULL/空 RHS 与三值逻辑；多行scalar错误/CTAS清理；100%或超过行数预算的确定性SAMPLE |
| review4：binary bitmap、SAMPLE omitted、window NULL、list order、宽键 | I3/I5/I7/I8；[transport tests](../../pkg/sql/plan/grouping_transport_test.go) `TestGroupingBinaryTransport`、`TestGroupingSampleOmittedKeys`、`TestGroupingWindowConsumerNullability`、`TestGroupingWindowPartitionOnlyKey`、`TestGroupingDistinctCompositeOrder`、`TestGroupingTransportOnlyConsumers` | binary/varbinary实际与合成NULL；FIRST/LAST_VALUE view/CTAS；1149与无残表；单独宽键性能对照，普通 SQL UNION仍BLOB |
| review5：marker折叠成0、字面量0混淆 | I4/I8；transport `TestGroupingSampleHiddenGroupingTransport` / `TestGroupingSampleLiteralSortOrigin` | subtotal-first LIMIT 应5,7,9而非5,5,7；actual NULL、literal 0、选/未选marker |
| review6：复合nullable、子查询alias在CTAS丢身份 | I1/I4/I6；[subquery tests](../../pkg/sql/plan/grouping_subquery_contract_test.go) `TestGroupingCompositeOperandProof`、`TestGroupingDistinctSubqueryAliasOrder`、`TestGroupingAliasSubqueryIdentity`、`TestGroupingSubqueryAliasRepeatedCTAS` | 外层NOT flag不能写NULL；fallback对照；scalar alias `k+1` SELECT/CTAS；独立Sub NodeId不合并 |
| review7：独立volatile a/b被结构匹配偷换 | I4/I8；[identity tests](../../pkg/sql/plan/grouping_order_identity_test.go) `TestGroupingVolatileAliasSlot` / `TestGroupingOrderReferenceAndStructuralFallback` | 比较实际返回 b 的排序，不固定random golden，不retry到成功；CTAS用row_number记录输入ordinal再验证 |
| review8：mixed SAMPLE ORDER BY未来输出自引用 | I4/I9；identity `TestGroupingSampleMixedOrderScope` | arithmetic/CASE/list/quantified/scalar混合；分支payload无projectTag自引用；DISTINCT非法marker1149及零残表 |
| review9：GROUPING(alias)取未来输出而非根键 | I3/I4；identity `TestGroupingArgumentAliasOrigin` | alias shadow/quoted/CUBE多参数顺序；actual NULL；selected-marker与未选SAMPLE-marker控制 |
| 设计审查DR1：AGG DISTINCT等价类丢失未选lineage | I2/I3/I7；**待新增**全局DISTINCT前后可用依赖断言；AGG/SAMPLE同一准入矩阵 | §4.7的NULL/subtotal/duplicate-set/alias/LIMIT；未选marker1149和CTAS清理、selected完整表达式合法对照；本轮未执行 |
| 设计审查DR2：分支先于UNION消费旧NULL证明 | I3/I6；**待新增**证明B先于HAVING/SAMPLE flatten的typed断言与证明U对照 | §4.6的分支HAVING/早期SAMPLE，空/NULL RHS、fallback、原始采样值；本轮未执行 |

公有回归汇总：[grouping_ctas.sql](../../test/distributed/cases/function/grouping_ctas.sql) / [生成并比较的 result](../../test/distributed/cases/function/grouping_ctas.result)。三行 fixture `(g,h,v)=(1,10,5),(1,20,7),(2,10,9)` 加独立 nullable、PK、empty、小型binary对象；不加入宽键大数据或延迟断言。相邻 SAMPLE、CTAS、ROLLUP、hint CTE 用现有 suite，不为文档新增集群 fixture。

### 7.1 已有证据的身份与限制

- 历史复现锚点依次为 `5918e55dcb`、`512f10f324`、`a2103870aa`、`bcec0af15d`、`7674e4e2c4`、`f0b27a96ed`；review9 修复为 `f14f314d58`。它们证明对应旧问题，不是当前 head 的新失败声明。
- `0a88c1d8cd` / target `af531771cc`：Go1.26.4、Darwin/arm64、匹配 CGo/native，planner整包 2125 pass events/3既有skip；五个相关consumer package复用源码有效的review6证据，合计4753 pass events。分组BVT 300/300 两次，metadata开启/no ignore；review1—9 与最初独立SQL oracle全部重新通过。PR生产statement coverage 373/384（97.14%）。这些是本地验证，不是维护者验收或远程CI绿色。
- 三条compile Parquet fanout fixture失败在精确target同样复现，未宣称compile全包绿；旧CI runner失联、checkpoint/bootstrap/ISCP故障另见 #32640/#32641/#32636/#32639，不通过删断言/加timeout掩盖。
- 本轮 `16082594b6` 合入的 #32591 改变远程receiver/全局merge所有权，#32596 改变frontend/background/runtime filter，#32609改NULL pruning。因此旧单CN SQL证据**不自动升级为此新head执行证据**；parser/typed owner断言在相关源码不变时可复用，涉及compiler/frontend/reader的结果需按相应依赖重新确认。
- 当前证据不覆盖生产规模、release-tag停机升级、真实多CN grouping/window执行。最新基线已有远程receiver修复说明 topology 风险真实存在；实现交付前应验证至少一个实际remote贡献的 grouping+window 和 SAMPLE/DISTINCT/order组合，并记录两个coordinator结果、错误/取消清理。这里是明确待补项，不冒充已跑。
- 多CN补验证复用既有compiler/operator故障与清理契约，不另造terminal协议。若发现需要修改operator，则停止局部补丁并更新该设计的所有权分析。

## 8. 决策记录与阶段验收

| 决策 | 原因/接受的代价 | 重开条件 |
| --- | --- | --- |
| D1 选typed内部边界，不选AST文本重写 | 保留tag/ordinal与subquery/volatile身份；接受binder复杂度 | 同构slot无法证明、跨scope重绑不可闭合 |
| D2 AGG bitmap与SAMPLE标量mask分开 | 来源机制确实不同，不能从NULL统一猜测 | 新operator要求第三种lineage机制时重新设计 |
| D3 原扫描/展开数量不优化 | hotfix不引入native多group AGG或共享状态 | 大K或scan成本成为阻塞需求时另立性能设计 |
| D4 新stage无开关、错误不fallback到旧错误结果 | 避免两套结果语义并行与silent corruption | 维护者决定收缩支持范围时回到方案A重新审批 |
| D5 只支持停机升级，不加mixed-version gate | 遵循项目升级契约；仍需持久视图与release来源检查 | 项目公开支持混跑或引入持久格式变化 |
| D6 后补设计与维护者批准分开记 | 历史实现/本地独立评审不能冒充批准 | 文档语义或实现ownership有实质变化时重新审查 |
| D7 DISTINCT按用户值去重，隐藏GROUPING保守拒绝 | 不定义任意来源合并，不增加数据相关准入证明；接受此前部分合法绑定形态收紧为1149 | 若要放宽，必须证明每个等价类中来源唯一并重新审查 |
| D8 分支证明B与合并证明U双发布 | 关闭早期HAVING/SAMPLE consumer与flatten时序，不修改输入编码类型 | 新消费阶段/表达式替换绕过相应证明发布点 |

**设计阶段验收**：替代方案、I1—I9、owner/slot/NULL/lineage与成本模型一致；所有设计阻塞有决定；独立审查指明版本与PASS/REQUEST_CHANGES。设计通过允许继续实施验证，不等于所有发布测试已完成。

**当前实现阻塞（不得靠文档宣称已修复）**：DR1，AGG DISTINCT的隐藏GROUPING准入不符合§4.7；DR2，当前流程缺少§4.6要求的分支早期证明闭合，需要针对相关调用验证并修复。依据是设计与源码时序/准入不一致，本轮没有执行新的反例，不宣称已运行得到某个错误结果。作者在设计获准后补最小失败证据、实现与上述回归，再审查实现符合性；文档发布不能解除这两项实现阻塞。

**实现/发布阶段待办（不是可默默豁免的证据）**：维护者对精确设计revision的决定；新基线的受影响消费链验证；真实多CN边界证据；支持的release源与持久视图升级检查；若目标工作负载要求性能SLA，则在发布前由发布owner确认。作者负责补证据，维护者负责合并门禁，release owner负责支持矩阵/投产验收；没有这些证据不宣称可发布。

独立审查结果和后续文档版本由相邻版本化审查记录链接；只记录真实评审内容、身份与结论，不把自动审查当 GitHub maintainer approval。

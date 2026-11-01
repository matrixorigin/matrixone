# 显式 general-ci：保留默认行为的扩展设计

状态：**r3-review.2，提交独立设计评审，尚未批准实现**。关联 [#25096](https://github.com/matrixorigin/matrixone/issues/25096)；本设计通过独立的design-only Draft PR评审，后续完整实现仍使用 [#31408](https://github.com/matrixorigin/matrixone/pull/31408)。源码基线：`mo/main@3b7752551b8a1cc60daddecde9a5438d5357cdf4`。本PR不包含生产、proto或测试改动，也不关闭issue。

本草案扩展 [r2设计](CLAUDE_issue-25096-ci-database-design.md) 的D1，不继承其对native general-ci的实施批准；r2事实上没有批准此项。继续遵守D2的默认不变方向、已有UCA400能力、事务修复独立和复用公共owner的要求。不重开或整支cherry-pick #30684。

本版给出完整的推荐方案：保持默认与旧对象语义，显式选择才启用native GeneralCI；用[附录](CLAUDE_issue-25096-schema-envelope-v1.md)的MO受限创建请求与S1/L1共用描述闭合外部边界。此前的GREATEST缺口在4.4解决：**给尚未开放的3/1域定义MySQL一致的原文选择规则，不重排或复制volatile实参**。native隐藏PK的CDC写删反向链也属于同一闭包。所有新增能力仍待维护者批准；独立MySQL探针、规格向量和冻结表检查不是MO实现验收，#31408已有的UCA400测试仍只代表原范围。

## 1. 问题与产品范围

#25096有两个不同诉求：

1. MO默认行为与MySQL 8不同；这是产品默认兼容策略。
2. 明确写出`utf8mb4_general_ci`后，比较仍按历史字节域执行；这是本轮要修复的功能缺口。

沿用[fengttt的方向](https://github.com/matrixorigin/matrixone/pull/30684#pullrequestreview-5646922664)：**保持当前默认，允许显式创建真正CI的数据库，后续定义继承其语义**。不把关闭issue与切换到0900默认绑定。默认差异应作为明确的产品取舍记录；只有维护者认可此验收定义，并且功能/兼容门完成后，才可以讨论关闭issue，不能声称原文所有默认比较都变成1。

目标包括：显式general-ci表达式、库/表/列声明及继承；原始查询、IF派生排序、去重与适用物理key；新旧对象共存时的持久、复制和恢复安全。已有UCA400仍须正确。

非目标：切换server/connection/握手默认到0900；激活其他native bin/0900/GBK域；新增C/CI名称；修改事务提交/关闭体系；分区、存储过程、UDF、warning兼容。不得用这些非目标删除现有能力或掩盖本次新增域的错误。

## 2. 算法与独立验收

GeneralCI不是UCA400，也不是lowercasing。复用`pkg/common/collation`已有的冻结GeneralCI权重后端，并通过独立MySQL命名域oracle核对。

| 输入比较 | utf8mb4_general_ci | utf8mb4_unicode_ci | utf8mb4_0900_ai_ci |
| --- | ---: | ---: | ---: |
| abc / ABC | 相等 | 相等 | 相等 |
| café / cafe | 相等 | 相等 | 相等 |
| a空格 / a | 相等 | 相等 | 不等 |
| ß / ss | 不等 | 相等 | 相等 |
| ß / s | 相等 | 不等 | 不等 |
| é / e+组合重音 | 不等 | 相等 | 相等 |
| NUL / 空串 | 不等 | 相等 | 相等 |

参考[MySQL collation差异](https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-sets.html)及[coercibility](https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html)。0900为NO PAD；原issue将其尾空格预期写为1不应成为实现oracle。本设计不激活0900。

必须保留原始复现，而非替换成unicode_ci：

```sql
SELECT 'abc' = 'ABC' COLLATE utf8mb4_general_ci;
-- 1
CREATE TABLE t(c VARCHAR(20) COLLATE utf8mb4_general_ci);
INSERT INTO t VALUES ('abc');
SELECT COUNT(*) FROM t WHERE c='ABC';
-- 1
```

在CI库内省略列COLLATE也要得到同一比较域。binary对照仍区分大小写。对同权值排序不固定原字节先后；IF派生的a/A/aa/AA/aaa/BBB验证权重组次序，必要时用唯一id消除平局。

## 3. 为什么不能只改名称解析

当前[metadata.go](../../pkg/common/collation/metadata.go)把general-ci准入为legacy `identity=3, revision=0`；[表达式binder](../../pkg/sql/plan/collation_expression.go)、[DDL类型应用](../../pkg/sql/plan/build_util.go)只对UCA400选择原生修订。

[字符串公共语义](../../pkg/container/types/collation_semantics.go)及hash/keycodec、sort/shuffle、index_util、readutil等消费者多处只看charset。把identity=3直接加入原生分支会同时改写旧对象；仅允许`3/1`而不迁移这些消费者则仍会丢语义。

还有一个现存字段陷阱：[build_ddl.go](../../pkg/sql/plan/build_ddl.go)在建立native UCA UNIQUE时，将**表级**`CollationVersion`置为1作为物理schema准入标记；[ALTER DEFAULT](../../pkg/sql/plan/alter_table_charset.go)即使把默认改成legacy bin/general，也保留该标记。现有`TestAlterTableDefaultPreservesNativeUniqueSchemaMarker`明确保护这种行为。因此不能将任意`TableDef(DefaultCharset=3, CollationVersion=1)`直接解读为新的GeneralCI默认；否则会在旧表后续ADD COLUMN时偷偷改继承语义。列Type的revision与该表级双重用途必须分清。

旧general-ci也不能简单统一重命名为bin：历史[MIN/MAX](../../pkg/sql/colexec/aggexec/minmax2.go)有独立general-ci分派，不等价于所有路径上的纯字节域。

本扩展跨frontend/planner、执行、物理key及持久边界，触发R3及设计先行门。已有权重代码、现有revision字段和通过的UCA测试，都不等于native general-ci已经可安全开放。

## 4. 解析、继承与执行方案

### 4.1 三类来源必须分开

建议复用现有identity/revision，不分配第二个general-ci身份，不新增开关或可变registry：

| 来源 | 目标行为 |
| --- | --- |
| 新SQL明确写`COLLATE utf8mb4_general_ci` | 选择`3/1`；先完成全链路后统一开放 |
| 无显式COLLATE的当前默认、既有compatibility alias | 保持原解析结果；不能因名字也叫general_ci而升级 |
| 已保存/继承的Type、数据库/表默认 | 原样传递identity/revision，不重新解析名字，不根据当前默认猜测 |

库默认已有`CollationId/CollationRevision/version`，列已有完整Type，可沿用。表默认采用**独立的、有“历史缺省”状态的revision字段**，而不是重新解释`CollationVersion`。

拟在`plan.TableDef`及`api.SchemaExtra`增加同一含义的`default_collation_revision`枚举，值为`UNSPECIFIED=0`、`LEGACY=1`、`V1=2`；后两者分别解码为语义revision 0/1，不能将枚举整数直接传给权重API。身份继续使用`DefaultCharset`，不复制名称或增加第二个identity。

| 保存状态 | 默认域解释 |
| --- | --- |
| 新字段缺失/0，DefaultCharset=3，即使旧CollationVersion=1 | legacy `3/0` |
| 新字段缺失/0，DefaultCharset=`collation.UTF8UnicodeCIIdentity`（9） | 沿用已有UCA400 `9/1`（`utf8_unicode_ci`） |
| 新字段缺失/0，DefaultCharset=`collation.UTF8MB4UnicodeCIIdentity`（10） | 沿用已有UCA400 `10/1`（`utf8mb4_unicode_ci`） |
| 新字段缺失/0，DefaultCharset=`collation.GBKChineseCIIdentity`（12） | 该原生身份仍未开放，保持既有准入拒绝；不得将`12/1`解释为UCA400或回退到legacy |
| 新字段缺失/0，其他历史身份 | 沿用该身份已有默认规则，不根据会话猜测 |
| 新字段V1，DefaultCharset=3 | native GeneralCI `3/1` |
| 新字段LEGACY | 明确revision 0；仍须检查身份/修订是否合法 |
| 未知枚举或非法身份组合 | 发布/解码前报错，禁止退回字节域 |

新的native general-ci表默认才必须写V1；普通legacy建表及无关DDL不补写新字段、不批量迁移目录。旧`CollationVersion`及原有物理准入标记继续读写兼容，不再作为新默认继承的权威。仅改变默认不修改旧列Type或key；显式CONVERT才进入重建。

真实保存链为：[PlanDefsToExeDefs](../../pkg/vm/engine/types.go)→`api.SchemaExtra`→已有`schema_extra`目录/TAE保存→[disttae读取](../../pkg/vm/engine/disttae/txn_table.go)。同时覆盖`api.CloneExtra`、catalog cache、`DeepCopyTableDef`、ReplaceDef错误回滚和snapshot/clone，而不是只加proto字段。所有默认消费者经过一个只读解码helper；物理准入仍独立检查完整/剪枝后的schema形状。同名DROP/recreate及prepared CREATE继续使用现有identity/代际校验。

`applyTextCharsetToPlanType`等仅凭identity补revision的入口须改为接收已解析域。只在定义/绑定阶段解析，执行时不查询catalog。冲突继续按coercibility处理；显式COLLATE生成表达式转换，不原地修改共享列。

### 4.2 session/握手不能误当成普通SQL显式选择

初始化、握手和`SET NAMES`最终可以写入同一个名为general_ci的会话字符串。仅按字符串重新解析无法辨别是否应开启新修订。尤其客户端可能自动发送协议ID45；把该ID统一改成native可能改变没有写COLLATE的既有客户端。

推荐纳入**显式SESSION赋值**：`SET NAMES utf8mb4 COLLATE utf8mb4_general_ci`及`SET SESSION collation_connection='utf8mb4_general_ci'`选择3/1；初始化、握手、`SET NAMES utf8mb4`省略COLLATE、DEFAULT重置仍走原规则。GLOBAL配置继续作为原有默认来源，不在本次引入全局native默认切换。这样既不把客户端自动发送45当作用户新选择，也不让显式SESSION选择继续无效。

在现有session变量owner内保存固定大小的已解析域，与显示字符串一起在原有同步边界发布；不是另建cache。`handleSetVar`在完整求值/校验后区分显式、隐式和恢复来源，低层`SetSessionSysVar`不能把握手调用误认成新SQL。失败恢复字符串和域两个部分；session clone、背景会话、RESET/CHANGE USER及关闭均沿用原owner。

binder只在绑定时读取一次域；普通表达式缓存把完整域作为依赖，不能因字符串仍叫general_ci就认为没有变化。`PrepareStmt`捕获准备时域，执行、重绑定和迁移不得改用当前会话域；参数的NULL/binary/类型协商仍归现有参数owner。现有`query.PrepareStmt`及`MigrateConnFromResponse/MigrateConnToRequest`需携带对应值，proxy原样转发并在目标发布前校验。不给同集群协议增加MORPC/能力门；已有迁移状态机与取消清理不变。

这新增每会话/每prepared handle一个定长值，不增加每行查询、worker或全局注册表。prepared的创建时语义须用独立MySQL会话oracle验证，不能只靠实现自证。

### 4.3 消费者与资源

以完整Type选择比较/key域；相等、IN、hash join/group、DISTINCT、shuffle、order/peer、MIN/MAX共享同一GeneralCI关系。返回值及原始存储值不替换为权重。

plan/vector、opaque remote batch、aggregate及spill保留完整Type；空批次、NULL-only、常量也验证合法域。非法UTF8、未知revision及错误后复用不允许静默退为原字节成功。

沿用既有scratch/mpool owner，生命周期为输入借用→临时key→消费/复制→当前执行释放。GeneralCI key上界使用现有`4*n+1`校验，不按声明VARCHAR最大长度逐行填充。legacy路径目标为零额外分配；不新增worker、锁、queue、timer、重试或跨查询cache。热路径改动须实测分配与成本。

### 4.4 LEAST/GREATEST：比较关系与返回哪个原文分开

GeneralCI相等并不意味着原文字节相同。MySQL 8.4.11独立oracle确认：`LEAST('a','A')`在general-ci下取首项a，`GREATEST('a','A')`取末项A；交换实参时对应返回A/a。尾空格和空串的同权值也遵守这个规则。MO当前`leastGreatestCollatedFn`两者都用严格比较，因而选首项，不能不加区别地复用其选择策略。

**推荐只对新native GeneralCI `3/1`采用：LEAST取最小权值的首项，GREATEST取最大权值的末项。** 在原LEAST/GREATEST执行owner按已经解析的完整参数域选择策略，等值时仅决定是否替换候选：LEAST仍用`cmp < 0`，GREATEST用`cmp >= 0`。返回被选输入的原始值，不输出权重，不用原字节二次比较打破CI平局。有效空串与未初始化候选分开；NULL结果和select-list行为继续由原owner处理。

此规则不改变GeneralCI权重/相等/hash/索引，也不改变legacy `3/0`、binary、现有UCA400或数值/日期重载。选择策略按**运算的已绑定域**判定，不按当前session、目标列赋值类型或表默认猜测。例如legacy赋值列的DEFAULT内部显式使用3/1，内部GREATEST仍遵守新规则；反之不能因外层列为3/1而改变旧表达式内部的UCA/legacy规则。普通ORDER BY平局及aggregate MIN/MAX的代表值合同不借此扩大。

安全边界是3/1尚被`RequireLegacy`的生产准入关闭，没有需要保留“3/1首项GREATEST”的已支持持久对象。新规则在**首次开放3/1之前**完成；不是给已发布revision偷偷改义，不增加一个函数级持久开关、第二套函数注册或新MORPC门。若发现受支持旧发行版实际写过3/1，必须返回设计评审，不能沿用此前提。

原参数向量、`FunctionExpressionExecutor`、select-list和memo owner不变，只改变已求值候选的等值选择。常量折叠也调用同一函数owner，prepared重绑定/迁移、DEFAULT/CHECK/generated及远端执行必须使用相同完整Type和规则。来源捕获仍发生在折叠前；旧typed payload和旧View保持原域，不能按新SQL名字改写。

MySQL导出保留原实参列表和顺序，每个子表达式只出现原有次数，给相关运算边界补显式域即可；不倒序、不展开重复CASE、不借助用户变量或辅助持久列。因此无需跨连接传递MO memo，原来准入的volatile DEFAULT不因这个问题被拒绝。各引擎原有的随机序列、时钟、NULL短路及非collation函数兼容边界不在这里重新定义；本方案不承诺两个数据库产生相同UUID/随机数，也不通过投影增加调用。CDC普通列仍发送源端已物化的值，只有generated列由目标按相同声明计算。

独立MySQL探针还在连接私有TEMPORARY TABLE中确认：3次INSERT默认值只消耗3次RAND（重置会话种子后与参考序列第4项对比）；GREATEST返回41，CURRENT_TIMESTAMP在后续写入而非建表时求值，generated列UPDATE后重算。该证据支持设计选型，不是MO实现/CDC通过。实现验收必须另有MO计数式执行器证明未重排/重复求值，以及3/0/UCA不变、常量折叠/动态参数一致的负对照。

## 5. 物理key和原子性

写入、唯一性、probe、锁、范围扫描与剪枝必须同域。显式COLLATE作用于legacy列时，不能错误使用旧字节索引作native裁剪；只保守退出不安全的优化，不全局禁用索引。

推荐复用`KeyFormatV1`和现有`physical_serial/physical_serial_full`，不增加key格式编号。原文列永远保留，索引只保存派生key。普通持久表的单列/复合PK、UNIQUE和普通二级索引是交付范围，不以扩大拒绝代替实现。

| 形状 | 推荐路径及必须补齐的owner |
| --- | --- |
| native单列字符串PK | 复用`__mo_cpkey_col`隐藏存储列，逻辑`Pkey.Names`仍是一个用户列；原文列不当作物理排序key |
| 含native项的复合PK | 同一隐藏列，按各项完整Type生成physical_serial；非native表保持原serial与字节 |
| UNIQUE/二级索引 | 复用preinsertunique/preinsertsecondaryindex、backfill和index lookup；都从schema选择physical serializer，保留NULL、前缀及回表原值规则 |
| 原物理格式表增加native索引/转换列 | 沿用COPY原子重建受影响表和整个相关索引集合，不能在仍为旧格式的表上孤立安装V1索引 |

[makeCompPkeyExpr](../../pkg/sql/plan/build_dml_util.go)目前固定调用serial；[preinsert](../../pkg/sql/colexec/preinsert/preinsert.go)已经按CompPkeyExpr生成隐藏列。复用的是这条生产链，不是宣称现有链已支持上述形状。[primary_key_semantics.go](../../pkg/sql/plan/primary_key_semantics.go)目前拒绝逻辑单列名与物理列名不同的唯一性证明，必须用完整native物理形状证明替代该假设，不能删除防伪检查。

反向消费者包括UPDATE/DELETE旧key、锁、重复检查、FK parent/child probe、读端点查/range/zone map、index-only回表和优化器唯一性推导。权重不可反解原文；不允许serial_extract权重作为用户值。必须按真实物理隐藏列裁剪；不能证明与源列同域的优化退回有残余谓词的路径，不拒绝合法SQL。

最近反例：general-ci PK允许同时存`ß`和`ss`，而`GROUP BY c COLLATE utf8mb4_unicode_ci`应合成一组；不得拿物理PK唯一性删除这个分组。abc/ABC必须发生native PK/UNIQUE冲突，多NULL的UNIQUE和所有未来写入需正确。FK域兼容验证不能只比较SQL类型名。

旧域中可并存的abc/ABC转换到CI时可能冲突。沿用既有CONVERT/COPY事务和发布点：冲突、取消或提交前失败均保留完整旧表与旧索引；不启动时自动去重、不偷偷改原值。失败临时key/索引由原COPY清理owner释放；不新增恢复状态机。只支持[shutdown upgrade](../../.agents/skills/mo-dev/references/upgrade-compatibility.md)，不新增MORPC数字、peer门或旧端fallback，不承诺downgrade。

CDC也是真实反向消费者，不能只补目标建表：当前stream按逻辑单列PK选择原文列，tombstone却携带物理PK，builder还对复合删除做普通Unpack；native隐藏PK会打破这些假设。[附录7](CLAUDE_issue-25096-schema-envelope-v1.md#7-源端写删两条反向链也要保持一致)推荐按物理PK统一净效应去重，并仅为DELETE等值谓词生成同权值代表值w，严格满足K(w)=k。w不是原文，不进入INSERT/存储/用户结果；这项受限keycodec能力须明确审批。新增上游#31321禁止普通SQL写hidden列，仍须遵守；不能因CDC修复把该权限围栏重新打开。

## 6. DDL、旧备份及SQL-only持久对象

### 6.1 MO普通表/列已有无新增语法的回放办法

当前`CHARACTER SET utf8mb4`而不带COLLATE会选择legacy `3/0`，即使目标库为CI也覆盖目标库默认。只要第4节保持这一既有charset默认规则，就可以在共享formatter中：

- 对语义已确定为legacy `3/0`的表/列输出显式`CHARACTER SET utf8mb4`，不输出会被新binder解释为`3/1`的COLLATE；表级物理标记须先按4.1解歧义；
- 对native `3/1`输出显式`COLLATE utf8mb4_general_ci`；
- 对其他已有域保持原有语义与输出规则。

这保留同一MO表/列Type，不靠新增legacy collation名字。必须通过在不同MO目标默认下重新绑定、完整Type及行为对比验证；不能只验证生成SQL包含某个字符串。**这不是MySQL方言规则**：MySQL的charset默认并不表示MO历史3/0。CDC MySQL继续原legacy列映射，对新3/1明确输出general-ci，由原formatter的目标方言分支区分；不能把charset-only规则无条件套给MySQL。

### 6.2 这不能自动解决旧表达式和无版本SQL

legacy `expr COLLATE utf8mb4_general_ci`具有显式coercibility。把它改成普通CAST/CHARACTER SET可能改变优先级；裸文本不能区分旧`3/0`和新`3/1`的作者意图。

内部有完整TableDef/表达式时，复用[既有DDL replay](../../pkg/sql/plan/ddl_expression_replay.go)，按原colID/Type恢复已有绑定；不得在snapshot、clone等每个调用点重写一套解释器。普通用户MODIFY/CHANGE重新声明仍是新定义，不自动当作历史回放。

**无版本SQL的推荐导入契约：** 普通连接收到的SQL始终是当前规则下的新声明，不从文件名、注释、导出工具名、错误信息或同名collation猜历史来源。不新增长期SESSION兼容开关或通用SQL导入解释器。已知旧MO脚本若要求保留原有general-ci语义，先在隔离的、与其来源兼容的旧版本实例按原创建环境恢复，再按既有停机升级路线打开这些持久对象；随后使用结构化备份/恢复转移，不把裸SQL再次交给新binder。仅普通表/列且来源已确认时，也可采用6.1的charset-only声明。

这是明确的操作成本：复杂旧脚本不承诺直接在新版执行仍保持旧含义；必须保留可用的旧版本恢复环境及所需会话设置。来源不明的脚本没有可推导的历史语义，不能声称无损恢复。该路线不影响已有结构化v1/v2/v3备份的直接LOAD，也不拒绝所有旧备份。审批必须接受这项取舍，并以真正旧二进制生成/恢复的对象做停机升级验证，而不是新代码手造revision=0。

### 6.3 View采用原有创建上下文，不加通用回放框架

给现有`ViewData`增加可选`collation_binding`，内容限定为绑定规则版本及创建时connection identity/revision。缺失表示历史GeneralCI解析规则；显式使用新general-ci或新SESSION域的View才需要写入新信息，普通旧View不补写。使用记录在折叠前捕获，不能靠最终BOOL输出Type判断是否用过新域。

[viewRegenerationContext](../../pkg/sql/plan/view_regeneration.go)在局部绑定范围提供这一不可变值，嵌套View分别使用自己的上下文，不修改调用session。重启、依赖刷新、恢复及SHOW输出类型均读取同一来源；普通ALTER VIEW是新声明。[WithRestoredViewDefinition](../../pkg/sql/plan/view_restore_definition.go)已按目标库/名/准确SQL限定恢复范围，继续复用。未知版本在执行/发布前失败，原权限与租户检查保留。

### 6.4 持久表达式同样需要折叠前的来源记录

反例：显式设置native general-ci的会话中，整型列的`DEFAULT ('abc'='ABC')`可能折叠为BOOL/整数常量1；CHECK也可能只剩TRUE。最终表达式没有字符串Type，但旧规则重放同一SQL会得到0/FALSE。仅检查列Type或最后的BOOL Type，既无法选择正确DUMP格式，也无法安全导出给旧接收端。

推荐在现有[Default、OnUpdate、GeneratedCol、CheckDef](../../proto/plan.proto)四个持久声明包装内增加可选`collation_binding`，采用与View相同的定长值语义：`rules=1`及创建时connection identity/revision。它只说明这条声明用过新绑定规则，不代替表达式自身完整Type，更不是执行时的配置开关。绑定期间按声明作用域、在折叠前捕获native GeneralCI使用；仅相关新声明写入，历史缺失保持旧规则，未知rules/domain组合拒绝。新字段号须在实现时核对并由生成器生成。

同表可混合旧DEFAULT、新CHECK及以后新增的表达式，因而不能用一个表级布尔位代替。无关ALTER保留各声明的原记录，显式重新声明才重新捕获；删除声明一并删除记录。常量折叠仅改变Expr，不清除包装内的来源；不复用有实时重绑定含义的`Literal.Src`冒充中性标记。按原声明wrapper进入ColDef/SchemaExtra、deep copy、ReplaceDef回滚、clone/snapshot和既有表达式payload；CHECK恢复不能只复制`.Check`而遗漏其来源。

现有[AnalyzeTableDumpBindings](../../pkg/sql/plan/table_dump_binding.go)仍负责把payload与真实声明、完整绑定对上；新增的已记录规则/connection域限定collation重绑定上下文，其他历史环境检查不放松。没有记录的旧表达式按历史GeneralCI规则验证，不能在一个表达式树里逐节点试出互相矛盾的时代。这样不会为了一个新常量CHECK而误改同表的旧DEFAULT，也不依赖遍历整个catalog反推历史意图。

每个受影响持久声明增加一个小的可选值，普通旧声明不写入；不在每个运行时Expr、每行数据上增加状态。查询执行仍只用绑定结果，不查询这份来源记录。这是显式新增的持久格式，须与表默认、View一起获得明确审批。

### 6.5 DUMP增加语义校验，不改旧hash含义

[表级DUMP](../../pkg/frontend/table_dump.go)的普通schema hash主要来自重建的SQL，不是完整Type序列。因此不能假定旧3/0和新3/1必定得到不同hash。已有`BoundExpressions`也只允许TableDef的Cols/Checks，且恢复时要求存在表达式；不能直接塞入一整个schema来绕过其检查。

推荐新general-ci相关dump选择显式的新manifest版本4；相关性包括native域以及6.4记录，即使只剩折叠常量也必须进入v4。普通legacy/UCA dump继续原v1/v2/v3。v4在原CreateSQL/原schema hash之外保存[附录](CLAUDE_issue-25096-schema-envelope-v1.md)规定的S1逻辑语义像、L1物理布局像及各自hash：已解码的表默认域、完整列Type、逐声明绑定、逻辑键与物理格式。已有表达式payload继续单独使用原校验和、解码预算及声明匹配，不扩大成任意catalog protobuf。资源上限沿用manifest限制，列数至多16384，表达式递归至多64层；新描述也在分配前检查大小/数量。

LOAD仍要求合适的空目标，不偷偷修改用户指定的新native列为legacy列：先做原schema/topology校验，再精确比较语义描述/绑定，最后才允许对象安装及原有ReplaceDef事务发布。语义hash只用于v4，不能替换旧hash算法让旧备份失效。

旧v1/v2/v3不能因为同名SQL的hash一致就接受新general-ci列/表默认。普通列可用6.1的charset-only声明创建旧域目标。表达式必须分两类处理，不能一看到目标表达式含新域就一律拒绝：

- **v2/v3有typed payload：** 沿用`tableDumpRestoredExpressions`。在列值类型、原始声明、CHECK名字/文本严格匹配后，允许将临时目标中重新绑定的表达式替换为备份内已有绑定，再校验最终schema。当前实现本来就支持“声明相同、绑定不同”，可保留旧general-ci的CHECK/default；此时禁止重新按当前会话解释payload。
- **v1没有typed payload：** 对已匹配目标的同一组声明，用明确的历史GeneralCI解析规则重绑定；仍复用`AnalyzeTableDumpBindings`排除其他未知会话环境依赖。仅在非collation环境已经可证明无关时，用这个旧域绑定进入已有LOAD的ReplaceDef/发布流程。不能把hash相同当成绑定相同，也不能直接信任manifest内未经匹配的另一段SQL。若历史信息确实不足，必须沿用已有明确的限制，而不能新增“所有旧备份都不支持”的拒绝。

验收需包含：目标旧域列+新绑定CHECK→恢复旧CHECK后才安装旧数据；恢复失败目标不变；native列/默认不被LOAD偷偷降级；折叠BOOL仍与备份的权威绑定一致。这个方案不新增LOAD语法或自动建表，不更改现有目标所有权。无版本外部SQL没有这些来源证明，采用6.2的显式操作路线，不能伪造旧manifest给任意SQL套用历史规则。

### 6.6 GETDDL：同一owner的显式格式，不把JSON塞进SQL

当前[parseCmdGetDdl](../../pkg/frontend/util.go)严格接受六个参数，GETDDL返回`dbname, tablename, tableid, tablesql`四列。推荐在原命令末尾增加第七个固定token `bindings_v1`；六参数模式不变，第四列仍是SQL，第五列新增`binding_metadata`二进制UTF-8 JSON。新模式不是普通SHOW输出，也不是新的集群内MORPC门。

新模式的完整响应是一次授权、一个固定源快照下的以下记录；没有表的数据库也必须表示出来：

| 记录 | 前四列与metadata合同 |
| --- | --- |
| 第一条header | 前四列为空串/0；metadata含`kind=header`、`format=1`、准确的publication/作用域、源snapshot名称及实际physical/logical时间戳 |
| database | dbname非空，其余为空/0；metadata含`kind=database`、已解析默认identity/revision，不能仅给默认名字 |
| table | 保留原四列；metadata含`kind=table`、`format=1`、SQL的SHA-256、6.5语义描述及hash、已有受限表达式payload及校验和（包括6.4的声明来源）；源表ID与第三列一致 |
| 最后一条end | 前四列为空串/0；metadata含`kind=end`、库/表计数、对前面所有原始行字段计算的顺序摘要 |

行摘要采用附录4.2的明确Text/U64/Bytes编码后SHA-256，不依赖JSON对象键排序或Go map遍历；表/库按名称排序。该摘要检测截断/混配，不替代连接认证。空表集仍有header/end；缺end、重复对象、错误计数、未知kind/format、重复JSON key、未知必需字段或多条CREATE/错对象SQL一律失败。64位值使用整数解码，不经过float64。

语义描述采用附录S1/L1受限结构，不接收任意catalog protobuf。物理列seqnum/隐藏key、逻辑PK和索引形状必须与实际源对象及目标规划结果/原安装owner的已验证映射一致；重用原index/table ID映射，不把源租户、目标OID、权限或任意properties直接写入目录。仅源定义中允许的publication属性沿原owner产生。payload校验必须先按源码声明重绑定/核对再进入原DDL replay，不是绕过binder安装任意表达式。

源端沿用[snapshot.go](../../pkg/frontend/snapshot.go)的`ensurePublicationInternalCmdAccess`、`GetAccountIDFromPublication`授权owner，并对请求/快照作用域显式复核publication覆盖，不能仅取得account ID就任意读取该账户其他库；库默认和所有表定义必须读取同一事务快照。新接收端以这一个响应获得库与表清单，避免初始化原先两次`-`查询之间发生DDL而混用状态；迭代使用其已有命名snapshot。缺失的表代表删除只能在完整end及作用域验证后判定，不能把权限错误/半包当空清单。

预算明确为：单表metadata不超过64 MiB（表达式payload仍不超过原32 MiB），单表所有声明合计文本/AST节点和总candidate visits沿用现有1 MiB/16384/1000000预算，表达式深度64；整份新模式响应（含SQL）最多256 MiB。源端建batch、接收端暂存清单都在累计分配前计数并遵守原请求deadline；超限不回退、不创建目标，不输出可应用的半份清单。暂存由当前请求/iteration释放，不新增worker、全局cache或磁盘spool。这是新模式的明确容量取舍：超大publication须缩小既有发布作用域，审批需接受并以边界测试和内存证据验证；不把这个上限套到未改的v0协议或普通建表入口。

#### 跨集群版本合同

- 新接收端先请求七参数模式。旧源仅限已支持当前六参数GETDDL、且尚未开放native GeneralCI的版本；只有SQLSTATE/错误号和完整的既有“expected: getddl 六参数格式”诊断精确匹配，才允许同一请求改走六参数模式一次。普通syntax、权限、网络、取消、预算、未知revision等错误均不作为旧源证据；新源不得用该旧诊断表示新格式拒绝。新模式返回四列也算协议错误，不能猜降级。
- 明确识别出的旧源响应按历史GeneralCI规则绑定，仅限定本次准确DDL对象/SQL，不能改变接收session或以后用户DDL；UCA规则不降级。旧源数据库清单协议没有默认域，沿用其原有能力边界，不声称可以凭空恢复其数据库默认。旧源→新目标的判定须用实际旧二进制协议测试，不仅mock错误字符串。
- 新源的六参数GETDDL先检查完整范围；只要含native GeneralCI域或6.4/6.3的新来源记录，就整体返回“不支持该表示”的错误，不返回部分DDL、不把第五列偷偷送给旧端。六参数GETDATABASES也检查其即将返回的范围，避免旧初始化先建库后才知道不兼容；普通legacy/UCA对象协议不变。
- 新源→旧接收端不支持新域复制。运行含新域的复制前先升级接收端；不声称旧接收端具有新版的全批预检。尤其旧stale恢复已经可能先发出清理DDL，源端不能撤回旧端已执行的命令；其原事务回滚必须实证，不能把源端拒绝当作“旧端从未DROP”的证明。没有该证据，不得宣布旧端失败安全。

#### 检查必须早于三个真实写入入口

1. [订阅初始化](../../pkg/frontend/publication_subscription.go)目前边读库清单边CREATE DATABASE，再逐表CREATE；改为先收完整typed响应并完成授权、作用域、对象名/SQL、域、表达式、物理形状、目标碰撞及映射预检。
2. [ExecuteIteration](../../pkg/publication/iteration.go)目前stale分支在`RequestUpstreamSnapshot`前调用`CleanPrevData`；新顺序必须先拿到固定快照与完整清单并预检，再允许stale清理。仅把校验塞在`ProcessDDLChanges`开头仍太迟。
3. [ProcessDDLChanges](../../pkg/publication/ddl.go)目前先建库，ALTER还有DROP→CREATE和inplace分支；都只消费已验证清单。schema比较加入完整域/声明来源，不能因SQL名字相同或表ID未变跳过语义变化。

预检不是commit：目标身份/权限/定义变化仍须由原事务及catalog锁在执行/发布时复核，失败不提交本轮对象映射、数据或watermark。新增库默认仅在对应account/database级publication所拥有的库上复制；table级复制到既有库不擅改整个库默认。默认值通过原数据库默认owner发布，本地失效代际由本地owner产生，不复制源端epoch充当本地代际。

这里不新增恢复状态机。需区分旧注释与事实：虽然`ddl.go`有非原子DROP/CREATE的TODO，当前`InternalSQLExecutor`的DDL实际使用`WithTxn`，`IterationContext.Close(err==nil)`决定提交/回滚；不能仅凭该TODO断言目前已丢表。应对初始化和迭代分别注入失败验证真实回滚，不能靠预检承诺解决所有后续I/O故障。

### 6.7 CDC：扩展现有事务内目标guard

实际可复用owner是[cdc_target_guard.go](../../pkg/frontend/cdc_target_guard.go)、[target_identity.go](../../pkg/cdc/target_identity.go)和[sinker_v2.go](../../pkg/cdc/sinker_v2.go)，不是新增公开catalog查询。现有MO guard锁住库/表catalog名字键、推进RC快照再解析live表，锁保持到目标DML事务结束；frontend已有针对目标表的INSERT权限及protected database检查。

推荐在`CALL mo_cdc_target_identity(db, table)`上增加可选第三个字面量：

| 调用 | 返回及使用 |
| --- | --- |
| 原两参数 | 原单列`target_table_id`不变，原legacy路径不额外分配/查询 |
| 第三参数`schema_v1` | 一行`target_table_id, schema_hash, schema_metadata`；metadata含附录的完整S1/L1及独立hash，不输出任意catalog或其他租户信息 |
| 第三参数`schema_hash_v1` | 一行`target_table_id, schema_hash`；hash严格为附录G1摘要，与完整模式相同；覆盖逻辑语义和目标自身物理布局，不误要求源/目标seqnum相等 |

三个模式共享原事务/权限/名字锁owner。新增模式同样要求显式的悲观RC事务；解析模式不能绕过`parseCDCTargetGuardCall`对应的frontend权限分支。无INSERT权限的用户不得读取定义，即使能看information_schema；受限账号只需既有目标写权限，不要求直接SELECT系统catalog或额外管理员权限。保持背景执行、普通CALL及prepared CALL的权限一致性。**完整模式会向获准INSERT该表的调用者披露受限定义中的DEFAULT/CHECK常量等内容**，这是须权限维护者明确批准的新API披露范围，不等同于旧接口仅返回表ID；不披露其他对象、owner、任意catalog属性或不相关SQL。未知模式明确拒绝，不能退回名字验证。

native相关CDC首次准入/恢复时，在原`targetIdentityAck`之前取得完整描述，用既有值类型/列序/逻辑PK/唯一键校验再加完整domain及合法物理形状校验。仅名字同为general_ci但目标3/0的MO表不能接受；不能为了通过校验自动转换用户目标。对于受此功能影响的创建SQL，同样使用原声明绑定恢复owner，不能只处理列而漏掉CHECK/default。新目标不支持此模式时，必须在目标CREATE或ack之前失败。推荐原`mo_cdc_target_guard_capability`增加可选参数`'schema_v1'`，返回一行整数`schema_format=1`；仍要求原事务条件，无目标名称、不读取任何表。原无参数调用不变，未知模式或旧端拒绝不得fallback。该检查在CREATE DATABASE/TABLE之前，不以创建后再验证代替准入；读侧支持不等于创建支持，发送附录的MO受限创建请求前还须检查`create_bindings_v1`格式，且正常CREATE授权独立执行。

每个native相关目标DML事务在现有BEGIN guard处比对ID和schema hash，不只比表ID：原地ALTER可以保留ID。hash不同则在**同一个仍持锁的事务内**读取完整描述、重做原准入校验；通过后才刷新当前sinker的定长摘要，否则原事务ROLLBACK，不发DML、不推进watermark、不自动DROP/rebuild。新连接/恢复/新源generation必须重新完整验证；摘要只是当前sinker的派生缓存，不是持久权威或新状态机。目标DDL在guard后等待原名字锁，当前DML提交/回滚后下一事务看到新定义。

MySQL目标沿用InnoDB身份/MDL owner：native源要求实际列的`collation_name=utf8mb4_general_ci`，并在每个native相关DML事务的guard锁内重验列/键及附录规定的实际声明，不能只在启动时验一次。`3/0`历史源保持原有映射，不顺带重新定义其跨产品语义；UCA拒绝边界不放开。配置的MO/MySQL类型不能通过任意错误自动互换，两类各自的身份/guard必须成立。数据仍发送原文值。

成本是native相关事务一次固定摘要响应及服务端O(schema)取定义/摘要，首次或变化时O(schema)完整传输；MySQL为每事务列/键查询及必要的声明读取。沿请求上限约束payload，逐行不做元数据查询，不增加锁类别、后台worker或独立重试。必须测试宽表及小事务开销；不声称已有零开销证据。所有rows/cancel/事务资源由原sinker和executor关闭路径负责，权限撤销、取消、断连、类型不符及提交失败沿原终态处理。

### 6.8 本版的具体选型与实施门

[附录](CLAUDE_issue-25096-schema-envelope-v1.md)与4.4给出以下推荐决策，供维护者按同一revision整体评审：

1. **MO创建传输：** 一个限定CREATE的封套，正常CREATE权限、声明重绑定见证与精确作用域replay，完整规划后才进入原CREATE事务。无先建表后修补，无SESSION隐式恢复开关，无新恢复状态机。已有表与不确定提交沿现有身份/ack流程，不能盲重试或自动接管。
2. **MySQL创建：** 不传MO protobuf、不依赖目标会话；按已绑定操作数域投影到普通表达式SQL，保留原值、数值/NULL及分支语义。反例已证明简单给字面量加COLLATE会改变优先级，bin也不能代替字节NO PAD。原formatter/表达式owner必须完成新native准入形状的投影，不扩大成通用跨引擎编译框架，也不以拒绝全部表达式表结束。
3. **统一编码：** 明确13个Type字段、声明绑定、逻辑S1/物理L1、proto规范字节、G1/行摘要、缺省/未知字段和预算；提供37字节Type及80字节S1固定向量。逻辑CDC与物理DUMP/CCPR的相容要求分开。
4. **反向链：** 同一设计补齐generated列DML投影、真实物理PK净效应及严格K(w)=k的DELETE谓词代表值。不得把代表值当用户原文；对冻结表的性质检查不代替产品keycodec/CDC测试。

此前D4b的同权值/volatile缺口已选定4.4的解法：新3/1域在MO原owner中采用与MySQL相同的LEAST首项/GREATEST末项规则，导出无需重排、复制或提前执行实参。它与完整Type及声明来源共同持久化，不改变旧3/0/UCA对象；不存在留待实现临时选择的tie策略。

这些是**已形成推荐而未批准的技术决策**。审批必须包括新的CDC输入适配、完整MySQL值投影、有限key代表值能力、D4c持久字段、旧脚本额外恢复环境与容量取舍。若实现/独立oracle证明某个原已支持的native形状不能满足上述合同，必须返回设计评审，不以静默降域、删除约束或扩大表拒绝绕过。当前仍禁止开放实现准入，文档检查不等于设计PASS。

## 7. 最小验证闭包

| 契约 | 复用入口与新增证明 |
| --- | --- |
| 原始general-ci表达式/列复现 | 现有collation_expression与database_defaults表项；红例、binary/legacy最近控制及第2节判别对 |
| 默认/CI库/覆盖/参数 | defaults_conversion及expressions BVT；普通库不变、显式CI库、跨库目标、charset-only legacy、prepared值/NULL及默认代际 |
| IF-derived排序 | 现有expression BVT；scalar LIMIT/OFFSET和peer关系，不能让tester结果排序掩盖实际顺序 |
| 表默认的独立字段 | 原marker UT加旧General默认+UCA UNIQUE→新增列仍legacy、新General默认→新增列native；新字段缺失时9→9/1、10→10/1，12仍拒绝；非法组合/克隆/ReplaceDef回滚 |
| 会话与prepared | 相同显示名的旧域/新域、显式/隐式SET、失败后全状态、RESET、prepare后SET再重绑定/迁移；复用既有session和proxy fixture |
| 全Type及资源 | 现有plan/vector/remote/aggregate/spill表项加入3/1，对照3/0；成功/非法/空/NULL/常量和mpool释放 |
| 选择函数原文/求值 | 4.4的LEAST首项/GREATEST末项；a/A、变音、PAD、空串、NULL和不等控制；HEX核对原文，常量折叠/动态/跨节点一致；计数式volatile参数不重排/重复、实时Src不冻结；3/0/UCA/数值重载保持旧行为 |
| hash、索引与剪枝 | 普通单/复合PK、UNIQUE和二级索引的写/改/删/锁/扫描/回表/前缀/NULL/FK；PK ß/ss改用UCA分组的负向优化控制 |
| 持久与失败 | 现有恢复/restart/两CN fixture；实际旧二进制写入/旧脚本恢复后停机升级、旧备份、schema/表达式保真、失败原子性和租户隔离 |
| 折叠后的来源 | 原DEFAULT/CHECK/GeneratedCol/OnUpdate fixture；native比较折叠为1/TRUE，仍选择v4/阻止v0误放；同表旧DEFAULT+新CHECK经COPY/恢复仍各守原域 |
| GETDDL与订阅 | 两个接收入口及stale清理前预检；最后一行坏payload/缺end/重复对象/空库/默认变化/权限撤销/预算边界均无目标DDL；坏格式不fallback；实际旧源兼容与旧接收端错误回滚分别验证 |
| CDC目标 | 同名3/0拒绝、3/1接受；相同表ID的原地ALTER、guard后DDL等待、断连重验、权限限制、NULL/原文值；MySQL在事务锁内重验，旧UCA拒绝不变 |
| CDC创建/写删 | MO坏封套/仅INSERT权限/已有表/提交不确定；MySQL实际CREATE及未来写入、coercibility/PAD/NULL/同权值选择原字节；generated列映射、单列隐藏PK和复合PK净效应、谓词代表值与原文隔离 |
| 共用格式 | 附录固定向量、字段存在/缺失、负scale、未知/重复field、长度溢出/预算、Src深度、S1语义与L1布局分离；实际产品编码与旧二进制互通另测 |
| 热路径 | 同模式ASCII、多字节、PAD、长前缀以及group/join/order基准；报告权重临时内存和旧域分配差异 |

不新增同义重型集群；通常两值反例加一排序控制。先focused、拥有包/static、消费者，再按实际风险增加race/拓扑/升级。生产变更语句覆盖率至少75%；BVT工具生成结果独立审查后正常比较、同实例两轮及清理后置检查。

旧UCA通过证据只对未变输入有效；一旦修改默认/解析/比较/key或相关fixture即失效，不把历史计数当新general-ci证据。

## 8. 替代方案、决策及实施门

### 8.1 关键替代方案

| 选择 | 正确性/成本判断 |
| --- | --- |
| 直接把表级CollationVersion=1当native默认 | 拒绝：会误读旧UCA索引表，不能靠新测试或名字纠正已保存状态 |
| 用FeatureFlag的一位表示native默认 | 不选：现有scalarReplacementShape用FeatureFlag!=0排除特殊表，会耦合无关DDL准入；其余扩展也会把规则修订散落为feature位 |
| 独立默认revision枚举 | 推荐：一个定长字段，沿原SchemaExtra保存；身份不重复，缺字段旧读规则明确，无全局迁移 |
| 将CI原文列原地改成权重、或把CI PK变成普通UNIQUE | 拒绝：分别丢原文、改变PK及FK契约；不能把CI比较问题转成数据模型变化 |
| 复用隐藏物理PK及physical serializer | 推荐：保留逻辑PK/原文，复用现有锁/写入owner；代价是审查单列/复合的旧假设及全读写消费者 |
| DUMP只改SQL名字或沿用原hash表示新语义 | 拒绝：同名不同revision可能同hash；不能证明旧数据与目标约束兼容 |
| native相关dump选择v4，旧格式维持原hash | 推荐：新格式随显式native能力opt-in，不全局翻转；增加一次schema级语义校验而非逐行状态 |
| 给无版本SQL新增长期SESSION旧语义开关 | 不选：扩大新声明/恢复混用范围及连接泄漏风险；推荐已知来源脚本先旧版隔离恢复再停机升级，代价是额外恢复环境 |
| 仅扫最终Type识别native表达式 | 拒绝：折叠TRUE/1不再有字符串Type；仅给相关持久声明记录创建绑定上下文，不给每个执行Expr挂新状态 |
| GETDDL在第四列放JSON或遇到任何错误就fallback | 拒绝：旧端可能先DROP，错误也不证明旧源；显式第七参数/第五列及整份响应，旧源只按受限诊断识别 |
| CDC直接SELECT mo_columns或新增目标校验服务 | 不选：增加授权/竞态边界；扩展已有guard，在原事务持锁读取真实Type；代价是native事务的schema摘要成本 |
| MO先CREATE后补表达式、MySQL用多条ALTER恢复每个会话域 | 不选：增加半建表状态/权限与失败恢复；MO一个受限创建请求，MySQL一个普通CREATE的值表达式投影 |
| 无条件给字面量COLLATE，或用utf8mb4_bin代表所有旧字节比较 | 拒绝：独立MySQL反例分别证明优先级和PAD关系改变；在已绑定的运算边界投影 |
| 新3/1继续沿用旧GREATEST首项，导出倒序/CASE仿真 | 不选：把本可在首次准入时定义清楚的函数合同转移到外部重写，增加volatile重排/重复风险；采用4.4的新域末项规则，仅修改既有选择owner |
| 全局将GREATEST改成末项，或增加通用memo传输/辅助列 | 拒绝：前者改变已保存UCA/legacy行为，后者增加状态/持久格式与跨产品框架；新域策略不需要这些机制 |
| CDC删除为找原文增加旧行preimage查询/GC保留体系 | 不选：扩展状态/保留与资源契约；GeneralCI仅使用已校验的等值谓词代表值，不能用于数据回填，需完整右逆证明 |

### 8.2 决策状态

| 决策 | 推荐/当前状态 | 决策责任与时间 |
| --- | --- | --- |
| D1：默认与issue验收 | 保留默认，不激活0900；用户方向已确认，关闭issue的产品口径待维护者认可 | 产品/SQL维护者，设计批准前 |
| D2：显式SESSION选择 | 4.2已给出推荐来源规则、owner、失败/重置/缓存/prepared迁移契约；GLOBAL与握手不变 | frontend/planner维护者，**方案未批准** |
| D3：表默认与物理能力 | 独立默认revision、原值+隐藏物理key；包括CDC物理PK净效应及仅供DELETE的GeneralCI同权值代表值，不用新增拒绝缩小范围 | planner/执行/存储/CDC维护者，**方案未批准** |
| D4a：View与结构化dump | 原ViewData创建上下文、v4语义描述、旧hash不变；旧v2/v3复用已有绑定移植，v1用已知旧collation上下文并保留其他环境敏感性检查 | schema/frontend维护者，**方案未批准** |
| D4b：SQL-only外部边界 | MO单请求CREATE、S1/L1编码及MySQL原顺序值投影；4.4给出新3/1原文tie规则，旧域不变 | schema/frontend/publication/CDC维护者，**方案未批准** |
| D4c：折叠表达式来源 | 6.4新增每持久声明的可选创建上下文，补齐BOOL折叠后的格式选择/旧端准入；不扩大为通用authoring框架 | schema/planner维护者，**新增建议未批准** |

本文与附录已对已发现的机制缺口给出明确推荐，现提交维护者设计评审，**不是已经获批的实现许可**。须整体批准新3/1的选择函数原文规则、受限创建输入、值投影、DELETE代表值、D4c持久字段、旧脚本恢复步骤、v1容量和旧接收端失败边界。若真实旧二进制或原事务owner不能满足上述合同，必须修改设计再评审，不得以静默降域、扩大表/索引拒绝或新增恢复状态机绕过。

整个变更按R3设计评审，不以“只是bug”或拆提交豁免。当前为**设计文档送审，实施仍阻塞于维护者批准**；没有修改任何proto或运行时准入。批准需关联本设计的确切commit/revision。之后按第7节完成MO红例、实现、完整验证，再交付#31408的功能扩展；本设计PR不作为功能完成，不自动关闭issue。

### 8.3 落地顺序与操作诊断

1. 设计批准后先冻结旧版本/旧域控制和原issue的MO红例；在保持准入关闭时完成共享语义、Type、选择函数及物理key消费者。
2. 完成session/prepared与持久来源、恢复/外部传输的完整闭包，再通过同一次交付开放显式3/1准入；不先开放SQL后靠拒绝普通索引/表达式补洞。
3. 全集群停机升级；升级接收端后才建立带native域的外部复制。普通库、已有表和握手默认不变；用户仅通过显式选择opt-in，无后台批量改写。
4. 诊断使用原请求/任务错误与watermark：分别报告unknown domain/rules、metadata budget、schema/identity mismatch、权限拒绝及结果不确定。日志不打印完整SQL封套/payload，不添加按表名/表达式内容增长的metric label。
5. 出现上述错误时停止当前owner的发布/水位推进并按原事务清理；不降级为字节比较、不自动重建已有目标。原告警/任务状态承担可见性，无新增监控worker或自动重试层。旧源识别范围、预算阈值和实际旧接收端回滚均是交付门，未经验证不扩大兼容承诺。

# 4.2 ENUM/SET 索引重建

旧版回填 ENUM/SET 二级索引时可能保存显示字符串，而普通写入保存 ENUM ordinal 或
SET bitmap。修复版使用存储值维护索引，但不会自动改写旧索引。升级时必须在恢复业务
前从基础表重建所有受影响的二级索引，包括复合索引。
旧版 UPDATE 还可能已错误改写基础表中的 SET bitmap；索引重建不能恢复这些值，
需要结合备份或业务记录核对基础数据。

这里的离线流程是 4.2 发布兼容例外，适用于旧版生成的受影响索引键和历史前缀定义。
它明确接受三项行为边界：旧键在升级后不会自动转换，受影响索引必须在开放读写前离线重建；
修复版不会原样重放含 ENUM/SET 前缀的旧表定义，相关快照/PITR 必须使用经演练的离线恢复路径；
旧版独立 CREATE INDEX 若只存在于分区叶、父表目录不可见，父表 DELETE 不会由本 guard 检查该索引。
发布前由 4.2 发布策略负责人确认例外，并在 PR 记录批准人、受影响的发布版本和恢复范围、
包含叶分区布局的 DML 影响、历史恢复范围、批准依据及仍需履行的恢复义务。批准尚未取得，当前例外状态为 pending。每个部署仍须完成
自己的盘点和恢复演练。完整盘点确认无受影响项时，本变更无需额外迁移；盘点不完整或结果
无法判定时，不按无命中处理。

| 存量情况 | 修复版行为 | 升级验收要求 |
| --- | --- | --- |
| 全量盘点未发现受影响键或定义 | 无需离线迁移 | 保存覆盖所有租户、分区叶和恢复点的盘点证据 |
| 旧版回填的 ENUM/SET 索引键；定义本身可重建 | 不自动改写；查询和唯一性仍可能受旧键影响 | 停止业务，在修复版从基础行重建所有受影响索引并核验后才开放读写 |
| 活跃索引或历史恢复点含 ENUM/SET 正前缀 | 新建受限定义会被拒绝；依赖旧定义的原样 RESTORE/CLONE 可能失败 | 按下文导出基础行、批准替代约束、重建并验证；没有覆盖承诺恢复点的演练路径时不得验收 |
| 独立索引只存在于分区叶，父表目录不可见 | 父表 DELETE 不会检查目录中不存在的叶索引 | 按分区影子表流程逐叶迁移和核验，完成前保持停流 |
| 盘点、可见性或类型判定不完整 | 无法证明无命中 | 继续隔离和调查，不按“无影响”放行 |

修复版的 MySQL 1712 是 DELETE 的 fail-closed 保护，不是迁移工具，也不能证明 SELECT、
唯一性检查或恢复结果正确。目标行应有的普通或唯一 ENUM/SET 索引项无法匹配时，DELETE
会报 1712 并回滚事务；复合唯一键任一列为 NULL 时无需索引项，复合非唯一键仍可能保存 NULL。
出现 1712 时保持该表停流，检查并重建索引。排查应在升级前进行，不能等业务收到 1712 才开始处理。
此保护只覆盖规划器能从当前表定义看到的索引。旧版独立 CREATE INDEX 若只写在叶分区、
父表元数据没有该索引，父表 DELETE 可能绕过该索引检查；不能用“DELETE 未报 1712”作为
叶索引完整或迁移完成的证据。必须逐个核对叶表并按分区影子表流程重建。

**先检查整个集群的受影响表，再安排升级。** 如果任一表不能按下面的条件安全重建，
先不要升级集群。维护期间停止应用和后台读写、连接池重试及相关 DDL，等待所有 CN 上的
在途事务结束，并按发布流程停止全部服务（包括 CN、TN 和 LogService）后整体替换版本。
所有表完成重建和验证前保持停流，不要让新旧版本混跑。保留升级前备份。

升级前还要检查每张表实际索引的 `algo_params` 中 `prefix_lengths` 或 `prefix_lengths_v2`：
若前缀直接作用于 ENUM 或 SET 列，该索引在旧版不同写入路径中可能使用不同的键。本版本
拒绝新建可识别的此类索引，不能安全地直接重建它们；有此类存量索引时先不要升级，也不要
自动去掉前缀。普通字符串列的前缀不受此限制。某些历史 SET 定义可能没有保留可区分的类型
信息，若无法确认列的有效类型或定义，应停止升级，不能仅依赖自动类型判断。

分区表还要核对保存的 `rel_createsql`：即使对应索引已删除，历史建表语句中的前缀仍可能
使 `ADD PARTITION` 失败；需要先按下述条件验证并处理旧定义。基于旧定义的 `CREATE TABLE
LIKE`、克隆、部分恢复或复制式 ALTER 也可能被拒绝。

## 升级前检查保留的快照和 PITR

删除当前表的索引不会修改旧快照里的表和索引定义。含旧 ENUM/SET 前缀索引的快照
仍可能支持读取基础行。新版的 `RESTORE TABLE` 和 `CLONE` 遇到可识别的此类前缀时
会拒绝重建原表定义。
在升级前，按租户盘点仍在恢复承诺内的备份、快照和 PITR。核对系统及各租户的权限和
可见范围；`SHOW SNAPSHOTS` 只用于盘点当前身份可见的命名快照，不能代替 PITR 或其他
保留点的盘点。每个仍需可恢复的受影响保留点都要有经过演练的数据恢复路径。

对命名快照，查询**同一快照**里的表、列和索引目录。以下例子查询单个表；同一份查询
去掉三个快照提示后可用于当前表。数据库或账户范围需要覆盖其中的所有表，包括实际
保存索引的每个分区叶。目录不可访问、范围不完整或索引与列无法匹配时，需要离线
恢复演练来确认，不能把这种结果当成没有受影响的索引。

```sql
SHOW SNAPSHOTS;

SELECT t.rel_id, i.name, i.type AS index_type, i.column_name, i.algo_params,
       CASE WHEN LENGTH(c.attr_enum) > 0 THEN mo_show_visible_bin_enum(c.atttyp, c.attr_enum)
            ELSE mo_show_visible_bin(c.atttyp, 3) END AS column_type,
       t.rel_createsql
FROM mo_catalog.mo_tables {snapshot='snapshot_name'} AS t
JOIN mo_catalog.mo_indexes {snapshot='snapshot_name'} AS i ON i.table_id = t.rel_id
LEFT JOIN mo_catalog.mo_columns {snapshot='snapshot_name'} AS c
       ON c.att_relname_id = t.rel_id AND c.attname = i.column_name
WHERE t.reldatabase = 'db_name' AND t.relname = 't'
ORDER BY i.name, i.ordinal_position;
```

先记录所有包含 ENUM/SET 键列的索引，包括没有前缀的复合索引，并用 `index_type`
确认快照里的有效唯一性。即使 `RESTORE TABLE` 或 `CLONE` 成功，旧隐藏索引对象
仍可能包含混用的键编码。对来自受影响版本的这些索引，恢复后要保持停流，
按下文适用的普通表或分区流程从基础行重建，并核验实际索引键和主键归属，
完成后再提供服务。

在 `prefix_lengths` 或 `prefix_lengths_v2` 中再找出具有正前缀长度的实际键列，与
`column_type` 对照。这些定义不能按上述方式原样创建，需先确认离线替代方案。自动附加的 `__mo_alias_` 主键列不属于用户声明的前缀。
普通字符串前缀不属于此问题。旧 `SET('')` 可能在目录里显示为 `BIGINT UNSIGNED`；
对有前缀而有效列类型不明的条目，核对建表和变更记录或在源版本演练。原始
`rel_createsql` 可帮助定位，经过 ALTER 后它可能过时。

对仍需恢复的受影响保留点，在维护窗口前逐个完成离线演练，并保留原始备份和与之
匹配的源版本二进制，直到对应恢复义务到期或已获批准的替代义务生效：

1. 在隔离环境读取该保留点的表、列、索引和分区元数据，并导出基础行。若修复版支持
   直接查询该命名快照，可用 USE INDEX() 读取基础表；若新版本拒绝原样 RESTORE/CLONE，
   使用匹配的源版本和该备份在隔离环境恢复后导出。保留点是 PITR 时，按承诺的时间点和
   多表一致性范围逐个演练；单个快照的成功不能替代 PITR 或集群级恢复证明。
2. 在修复版创建不含被拒绝前缀定义的新表，显式重建列、主键、分区和依赖。由表负责人
   批准每个前缀索引的替代定义。唯一索引必须确认替代约束符合业务规则并检查基础行冲突。
   将唯一前缀换成全列唯一索引会改变约束，例如旧规则下冲突的 'abc' 和 'abd' 在新规则下
   可以并存。由约束负责人批准未来写入规则，并按旧前缀和候选定义分别核对非 NULL 键元组，
   包含其他键列及实际排序规则。遇到冲突先处理数据和约束，不能自动改索引或丢行。
3. 将基础行写入新表；逐行对照主键、ENUM 序号、SET 位图、NULL 和其他列，再核对新索引的
   物理键及完整 owner。执行提交后的读写检查、重启检查，并从新表创建新快照后实际恢复一次。
   记录旧保留点到新表/新快照的映射，以及原始备份的保留位置。
4. 证明所有承诺的快照、PITR 时间点、表间一致性和分区叶都有恢复路径。未演练、不可见、
   不可读取或定义有歧义的恢复点保持为未解决项，停止升级验收。

已验证的例子是单个普通表，显式整数主键和 `ENUM('abc','abd')` 列。在隔离的新版
实例可以用如下方式从仍可读的命名快照提取基础行，再为修复后的表创建新快照：

```sql
CREATE TABLE db_name.t_recovered (
    id INT PRIMARY KEY, e ENUM('abc','abd'), KEY ix(e)
);

INSERT INTO db_name.t_recovered (id, e)
SELECT id, e FROM db_name.t {snapshot='snapshot_name'} USE INDEX();
```

这个示例选择全列非唯一索引作为替代。实际表要显式补齐可写列及其类型，
按下文的规则无损复制 SET 并逐行对照。表有依赖或有原先的唯一前缀约束时，
需要核对应有的权限和约束后再确定替换方式。这里的新快照保存修复后表的数据和表
定义，旧快照仍保留原定义。多表快照的一致性及 PITR 能力需要按原恢复范围验证。

## 非分区表

保存 `SHOW CREATE TABLE` 和索引目录，记录每个索引的列顺序、前缀、唯一性、选项及
实际可见性。对可以按原定义重新创建的普通和 UNIQUE 索引，使用修复版逐个重建，例如：

```sql
ALTER TABLE db_name.t DROP INDEX ix_status;
CREATE INDEX ix_status ON db_name.t(status);
```

DROP 和 CREATE 分别执行，不能在索引缺失时恢复业务。UNIQUE 重建若报重复键，检查并
处理基础表数据后再试，不会自动删除行；报错中的重复值可能显示为序列化键的十六进制。
遇到无法保留原定义的索引或依赖时，不要继续升级。

恢复流量前，用基表扫描与同定义的全新验证索引逐项核对每个索引的物理键和完整主键归属。
应按多重集合双向比较 (key, owner)，包括缺失、多余和重复项；分区索引逐叶核对。
验证索引应从基表行完整写入，并保留列顺序、类型、支持的前缀和唯一性。
不要用公开 SQL 的 serial(e, ...) 计算预期物理键，因为公开函数仍可按显示值绑定。
验收还需核对基表的主键、ENUM 序号、SET 位图和 NULL。用隔离副本验证重启后 DELETE
删除正确的基表行及其索引项，且重复非 NULL 唯一键仍被拒绝。核对失败、重建中断或结果
不明确时保持停流，先检查目录和实际索引状态再决定是否重建。


## 分区表：离线影子表迁移

旧版本的独立 `CREATE INDEX` 可能只在叶分区创建索引，父表 `SHOW CREATE TABLE` 会漏掉它。
不要对分区父表照搬上面的 DROP/CREATE：已有叶可能遗留旧键，后续新分区也可能缺索引或
写错索引表。本流程为已验证的有限场景提供迁移：同库普通表，显式稳定整数主键，RANGE
分区，以及能在初始 CREATE 中无损表达的可见普通二级索引。已使用 `v4.2.8-hotfix-20261119`
和 `v4.2.9` 的持久化数据验证 ENUM/SET 复合索引、普通兄弟索引、NULL、空成员和新增分区。
对 `v4.2.9` 的两叶 HASH 表，还验证了父表无索引、两个叶表均有同一个普通 ENUM 索引的迁移：
迁移前有一行缺少叶索引键；迁移后逐叶比较键和主键归属，并在切换、删除和重启后复核。
其他 HASH 定义、源版本、索引类型、分区类型或复杂表结构需要另行验证。后续 ALTER 索引可能使保存的
建表定义与当前定义不一致；
本流程不保证这种变更后再 ADD PARTITION 的行为，需在执行前另行评估。

影子表是**新表 ID**。迁移前必须确认表 ownership、对象级授权、入向和出向外键、表级
PITR、发布订阅、CDC、视图及其他依赖不会因 ID 改变而丢失，或已有经过验证的处理方案。
还要核对 AUTO_INCREMENT、生成列、插件索引、列和索引选项，以及所有 ENUM/SET 的有效成员
及顺序。旧目录中的 ENUM 定义可能有歧义；无法确认有效域或依赖时，不要执行切换。

### 1. 盘点父表和所有叶

下面以 `db_name.t` 为例。保存父表原始 CREATE、`SHOW CREATE TABLE db_name.t`、父表 ID、
每个叶及其索引；将 SQL 中的库名和表名改为实际名称：

```sql
SELECT rel_id, relname, rel_createsql
FROM mo_catalog.mo_tables
WHERE reldatabase = 'db_name' AND relname = 't';

SELECT t.relname, t.rel_id, i.name, i.type, i.algo, i.algo_table_type,
       i.algo_params, i.is_visible, i.hidden, i.comment, i.column_name,
       i.ordinal_position, i.options, i.index_table_name
FROM mo_catalog.mo_tables AS t
LEFT JOIN mo_catalog.mo_indexes AS i ON i.table_id = t.rel_id
WHERE t.reldatabase = 'db_name'
  AND (t.relname = 't' OR t.rel_id IN (
      SELECT p.partition_id FROM mo_catalog.mo_partition_tables AS p
      WHERE p.primary_table_id = (
          SELECT rel_id FROM mo_catalog.mo_tables
          WHERE reldatabase = 'db_name' AND relname = 't')))
ORDER BY t.relname, i.name, i.ordinal_position;
```

对照各叶确认完整索引集合和定义；父表没有、只有叶表有的索引也必须计入。将 `algo_params`
中的前缀列与实际列类型逐一对照，并检查原始 `rel_createsql`。若前缀只存在于历史 CREATE，
确认当前父表和每个叶都已无该索引后，可在满足影子表全部条件时按实际有效定义重建影子表；
若实际索引仍含 ENUM/SET 列前缀则不要升级。`ordinal_position` 是索引内的列顺序，不能用来
推断不同索引的物理顺序。索引定义或叶之间有无法解释的差异时停止。
检查影子表名、备份表名以及派生的叶表名均未占用，并预留复制数据和索引所需空间。

### 2. 创建、复制并核对

在修复版中，手工创建同库影子表，把盘点确认的**所有**索引写在初始 `CREATE TABLE` 内，
保留列、主键、分区边界和索引的实际定义。不要仅依赖可能缺索引的父表 `SHOW CREATE`。
下面只是已经验证的 `id/e/s/n` 示例，不能代替其他表的完整定义：

```sql
CREATE TABLE db_name.t_rebuild (
    id INT PRIMARY KEY,
    e ENUM('', 'red', 'green'),
    s SET('', 'a', 'b'),
    n INT,
    KEY sn(n),
    KEY ie(e, s)
) PARTITION BY RANGE(id) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20)
);

INSERT INTO db_name.t_rebuild (id, e, s, n)
SELECT id, e, s + 0, n FROM db_name.t USE INDEX();
```

例如，上述 HASH 已验证场景的源表是 `id INT PRIMARY KEY, e ENUM('z','a')`，
父表没有索引，两个叶表各有 `KEY idx(e)`。影子表的定义和复制语句为：

```sql
CREATE TABLE db_name.t_rebuild (
    id INT PRIMARY KEY, e ENUM('z','a'), KEY idx(e)
) PARTITION BY HASH(id) PARTITIONS 2;
INSERT INTO db_name.t_rebuild (id, e) SELECT id, e FROM db_name.t USE INDEX();
```

为每个 SET 列使用数值投影。直接复制 SET 的显示字符串可能将 bitmap 1 变成 0，而两者都
显示为空。ENUM 保持直接列投影；将 ENUM 改成数值投影时，ordinal 0 可能导致复制失败。
实际表有其他列时必须补齐可写列，并确认所有值都被无损复制。

按主键双向核对**全部行**，比较 NULL、每个 ENUM 的 ordinal、每个 SET 的 bitmap 和其他列
的完整值；仅比较行数或显示字符串不够。对于上例，以下两个结果都必须为 0：

```sql
SELECT COUNT(*)
FROM db_name.t AS o USE INDEX()
LEFT JOIN db_name.t_rebuild AS n USE INDEX() ON o.id = n.id
WHERE n.id IS NULL OR NOT (o.e + 0 <=> n.e + 0)
   OR NOT (o.s + 0 <=> n.s + 0) OR NOT (o.n <=> n.n);

SELECT COUNT(*)
FROM db_name.t_rebuild AS n USE INDEX()
LEFT JOIN db_name.t AS o USE INDEX() ON o.id = n.id
WHERE o.id IS NULL;
```

对于其他列，按实际类型扩展核对，不能依赖可能忽略字节差异的字符串比较。
逐叶核验重建表各索引的 `(key, owner)`，并对照等价普通表的插入结果；例如通过上面的目录
查询找到隐藏索引表后，查询 `SELECT __mo_index_pri_col, HEX(__mo_index_idx_col) FROM
\`索引表名\` ORDER BY __mo_index_pri_col`。NULL、重复值及各叶都要覆盖。任一核对失败都保持
停流，不执行切换。

### 3. 切换并验收

记录原表和影子表的 `rel_id`。在无活动事务的维护连接执行一条语句：

```sql
RENAME TABLE db_name.t TO db_name.t_backup, db_name.t_rebuild TO db_name.t;
```

使用 `RENAME TABLE`；不要改成两条 `ALTER TABLE ... RENAME`。提交成功不代表其他 CN 已
看到新表。保持停流和 CN 拓扑稳定，在执行切换的 CN 上运行以下命令，并确认返回的同步
CN 数量与全部拟接流量的 CN 一致、水位非零；该数量不能证明节点身份。命令失败、节点
不可达或无法核对路由时不要恢复业务，未覆盖或新加入的 CN 不得接流：

```sql
SELECT mo_ctl('cn', 'syncCommit', '');
```

随后逐个直连每个业务 CN，使用新的 autocommit 连接执行下列查询。结果必须恰好两行：`t`
指向记录的影子表 ID，`t_backup` 指向原表 ID，`t_rebuild` 不存在。任一节点仍看到旧值、
缺失或重复结果时保持停流；同步不会结束已有事务。

```sql
SELECT relname, rel_id FROM mo_catalog.mo_tables
WHERE reldatabase = 'db_name' AND relname IN ('t', 't_backup', 't_rebuild');
```

在每个 CN 重新检查分区、索引、数据、应用角色及其他依赖，保留备份表和升级前备份。若连接
在提交时中断，继续保持停流，先确认事务结束并同步，再由新连接按表名和已记录的 ID 判断是否
已切换，不要盲目重试。恢复业务时使用新连接；恢复写入后备份表不含新写入，不能直接切回而
不对账。

完成所有受影响表并验证后，再恢复应用及后台流量。

# 4.2 ENUM/SET 索引重建

旧版回填 ENUM/SET 二级索引时可能保存显示字符串，而普通写入保存 ENUM ordinal 或
SET bitmap。修复版使用存储值维护索引，但不会自动改写旧索引。升级时必须在恢复业务
前从基础表重建所有受影响的二级索引，包括复合索引。
旧版 UPDATE 还可能已错误改写基础表中的 SET bitmap；索引重建不能恢复这些值，
需要结合备份或业务记录核对基础数据。

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

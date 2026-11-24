# 4.2 ENUM/SET 索引重建

旧版对 ENUM/SET 索引的回填可能把显示字符串写入隐藏索引，而普通写入保存的是
ENUM ordinal 或 SET bitmap。修复版按存储值维护索引，但不会自动改写已有索引数据。
升级前已存在的 ENUM/SET 二级索引必须从基础表重建后再恢复业务流量。

## 维护步骤

1. 阻止受影响表的应用查询、写入和后台写入，等待在途事务结束。关闭旧 CN；在
   所有受影响表的索引重建和验证完成前，不恢复业务流量，也不让旧版 CN 重新加入。
2. 保存每张受影响表的 `SHOW CREATE TABLE`，列出索引键包含 ENUM 或 SET 列的索引，
   包括复合索引和 UNIQUE 索引。保留原索引的列顺序、前缀、唯一性和可见性定义。
3. 使用修复版逐个从基础表重建索引。DROP 和 CREATE 分别执行，维护期间不要向缺少
   索引的表写入。例如原定义为 `KEY ix_status(status)`：

   ```sql
   ALTER TABLE db_name.table_name DROP INDEX ix_status;
   CREATE INDEX ix_status ON db_name.table_name(status);
   ```

   UNIQUE 索引按原定义重新创建。分区表也要分开执行 DROP 和 CREATE，不要把两条 DDL
   放在同一事务中。重建失败时保持该表停用；UNIQUE 重建遇到重复键时，由用户检查并
   处理基础表数据后再重试，不会自动删除行。
4. 对每个索引验证定义和查询结果，再恢复应用流量。所有受影响表都完成后，才重新开放
   应用及后台写入。

该流程是离线维护，不支持新旧 CN 混跑，也不会在启动时自动重建索引。

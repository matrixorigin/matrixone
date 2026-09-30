# DECIMAL256 索引升级

DECIMAL 精度大于 38 使用 DECIMAL256。此次修复沿用已有 tuple 编码，但旧版
legacy 索引写入器可能省略 DECIMAL256 组件，或在过滤 NULL 时丢失索引行及主键
对齐。旧键不能通过新 decoder 补回缺失信息，必须从基础表重建受影响索引。

已复现的入口是带外键表的 `LOAD DATA`：开启 `foreign_key_checks` 时回退到
legacy planner，Parquet 可以提供 DECIMAL256 输入。普通无外键复合索引的导入
通常先被旧 `serial`/`serial_full` 拒绝，不能据此排除 legacy 写入的历史数据。
单列 UNIQUE、复合 UNIQUE、普通索引和含 DECIMAL256 主键的索引都需要检查；
普通索引的隐藏主键尾部也属于范围。

## 必需维护窗口

含这类历史数据的集群必须按下面步骤升级。不能让新旧 writer 混写，也不能在
重建完成前恢复用户查询。此次修复不提供在线滚动重建协议。

1. 保存可恢复的升级前备份，并阻止所有账户的用户读写，以及导入、CDC、任务和
   其他后台写入。确认在途事务结束；维护连接只允许管理员执行下面的只读清单。
2. 在每个账户用管理员身份执行
   [`etc/decimal256_index_inventory.sql`](../../etc/decimal256_index_inventory.sql)。
   该只读清单保守列出含 DECIMAL256 表的全部普通、UNIQUE 和 RTREE 索引，可能多报。
   不只按用户索引列筛选，避免遗漏隐藏主键尾部。保存每张表的 `SHOW CREATE TABLE`
   和 `SHOW INDEX`，包括索引名称、列顺序、前缀、可见性、注释及其他原定义。
3. 关闭全部旧 CN，使用修复版二进制启动维护 CN，保持用户读写和后台 writer 关闭。
   从基础表逐个重建清单中的索引，使用保存的原定义，不能从旧隐藏索引表回填。
   例如原索引为 `KEY ab(a,b)` 时：

   ```sql
   ALTER TABLE db_name.table_name DROP INDEX ab;
   CREATE INDEX ab ON db_name.table_name(a,b);
   ```

   UNIQUE 使用原来的 `CREATE UNIQUE INDEX` 定义。示例不能替代实际保存的 DDL。
   DROP 和 CREATE 是独立 DDL；CREATE 失败时保持维护窗口，不开放缺失索引的表。
   UNIQUE 重建若发现重复，从基础表查找非 NULL 键的重复项（仅对仍存在的索引使用 `IGNORE INDEX`），
   保留数据并人工处理，禁止自动删行。失败或回滚必须恢复整份升级前备份及相应
   旧二进制；不能把新键或已部分重建的数据目录交回旧 writer。
4. 检查重建后定义与原定义一致。对每个索引用 `EXPLAIN` 确认索引路径，再比较
   `FORCE INDEX(index_name)` 和 `IGNORE INDEX(index_name)` 的完整、有序结果：
   等值、可支持的范围、超过 DECIMAL128 的正负值和 NULL。NULL 或范围查询若
   planner 不使用该索引，不把这个查询当作索引路径证据。
   在可回滚事务或独立验证表中验证 UPDATE、DELETE、UNIQUE 重复拒绝和 NULL
   行的主键对齐；回滚后再次检查原数据及索引结果一致。
5. 只有每个账户的清单、重建和验证全部完成后，才恢复读写和后台任务。

## 回归证据与复现

`optimizer/composite_range_decimal.test` 及两个 Parquet 夹具覆盖新 legacy writer
的普通/复合 UNIQUE/单列 UNIQUE 索引、NULL 压缩、DECIMAL256 主键对齐、强制索引
与基础表对照，以及既有 DROP/CREATE 重建后的更新和删除。夹具用 PyArrow
`decimal256(65,0)`、`int32` 列和 `use_dictionary=False` 写出；CSV 的类型拒绝
不能代替这个可达路径。

升级挑战还使用 PR 原始 head `183d9f17f130e2b7a40142e3b3f7d68bbf0fd4b8`
二进制写入带外键的普通索引表，再停旧进程、用修复二进制打开同一数据目录。
重建前强制索引查询漏行，`IGNORE INDEX` 返回正确基础表行；按上面现有 DDL
重建后结果一致，UPDATE、DELETE 和 NULL 验证通过。这是升级流程要求，不能
用“编码格式未变”替代历史索引重建。

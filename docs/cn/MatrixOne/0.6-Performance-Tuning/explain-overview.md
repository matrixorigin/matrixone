# MatrixOne 执行计划概述

使用 EXPLAIN 可查看 MatrixOne 执行某条 SQL 语句时的执行计划。

!!! note
    使用 MySQL 客户端连接到 MatrixOne 时，为避免输出结果在终端中换行，可先执行 `pager less -S` 命令。执行命令后，新的 `EXPLAIN` 的输出结果不再换行，可按右箭头 **→** 键水平滚动阅读输出结果。

## EXPLAIN 示例

**数据准备**：

```sql
CREATE TABLE t (id INT NOT NULL PRIMARY KEY auto_increment, a INT NOT NULL, pad1 VARCHAR(255), INDEX(a));
INSERT INTO t VALUES (1, 1, 'aaa'),(2,2, 'bbb');
EXPLAIN SELECT * FROM t WHERE a = 1;
```

**返回结果**：

```sql
+------------------------------------------------+
| QUERY PLAN                                     |
+------------------------------------------------+
| Project                                        |
|   ->  Table Scan on aab.t                      |
|         Filter Cond: (CAST(t.a AS BIGINT) = 1) |
+------------------------------------------------+
```

`EXPLAIN` 实际不会执行查询。`EXPLAIN ANALYZE` 可用于实际执行查询并显示执行计划。如果 MatrixOne 所选的执行计划非最优，可用 `EXPLAIN` 或 `EXPLAIN ANALYZE` 来进行诊断。

**EXPLAIN 分析**

- QUERY PLAN，即本次执行的主题，查询计划

   + Filter Cond：过滤条件
   + Table Scan：对某个全表进行扫描

- Project 为这次查询过程中的执行顺序的父节点，Project 的结构是树状的，子节点计算完成后“流入”父节点。父节点、子节点和同级节点可能并行执行查询的一部分。

**范围查询**

在 `WHERE/HAVING/ON` 条件中，MatrixOne 优化器会分析主键或索引键的查询返回。如数字、日期类型的比较符，如大于、小于、等于以及大于等于、小于等于，字符类型的 `LIKE` 符号等。

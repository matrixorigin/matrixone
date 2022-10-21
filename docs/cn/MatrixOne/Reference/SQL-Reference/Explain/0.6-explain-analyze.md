# 通过 `EXPLAIN ANALYZE` 获取信息

`EXPLAIN ANALYZE`，它运行 SQL 语句产生 `EXPLAIN` 输出，此外，还产生其他信息，例如时间和基于迭代器的附加信息，以及关于优化器的预期与实际执行的匹配情况。对于每个迭代器，提供以下信息:

- 预计执行成本

   成本模型没有考虑一些迭代器，因此不包括在估算中。

- 估计的返回的行数

- 返回第一行的时间

- 执行此迭代器（仅包括子迭代器，但不包括父迭代器）所花费的时间，以毫秒为单位。

- 迭代器返回的行数

- 循环数

查询执行信息使用 `TREE` 输出格式显示，其中节点代表迭代器。`EXPLAIN ANALYZE` 始终使用 `TREE` 输出格式，也可以选择使用 `FORMAT=TREE;` 显式指定。其他格式 `TREE` 暂不支持。

`EXPLAIN ANALYZE` 可以与 `SELECT` 语句一起使用，也可以与多表 `UPDATE` 和 `DELETE` 语句一起使用。

你可以使用 `KILL QUERY` 或 `CTRL-C` 终止此语句。

`EXPLAIN ANALYZE` 不能与 `FOR CONNECTION` 一起使用。

## 示例

**建表**

```sql
CREATE TABLE t1 (
    c1 INTEGER DEFAULT NULL,
    c2 INTEGER DEFAULT NULL
);

CREATE TABLE t2 (
    c1 INTEGER DEFAULT NULL,
    c2 INTEGER DEFAULT NULL
);

CREATE TABLE t3 (
    pk INTEGER NOT NULL PRIMARY KEY,
    i INTEGER DEFAULT NULL
);
```

**表输出结果**:

```sql
> EXPLAIN ANALYZE SELECT * FROM t1 JOIN t2 ON (t1.c1 = t2.c2)\G
*************************** 1. row ***************************
QUERY PLAN: Project
*************************** 2. row ***************************
QUERY PLAN:   Analyze: timeConsumed=0us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 3. row ***************************
QUERY PLAN:   ->  Join
*************************** 4. row ***************************
QUERY PLAN:         Analyze: timeConsumed=5053us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 5. row ***************************
QUERY PLAN:         Join Type: INNER
*************************** 6. row ***************************
QUERY PLAN:         Join Cond: (t1.c1 = t2.c2)
*************************** 7. row ***************************
QUERY PLAN:         ->  Table Scan on aaa.t1
*************************** 8. row ***************************
QUERY PLAN:               Analyze: timeConsumed=2176us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 9. row ***************************
QUERY PLAN:         ->  Table Scan on aaa.t2
*************************** 10. row ***************************
QUERY PLAN:               Analyze: timeConsumed=0us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
10 rows in set (0.00 sec)

> EXPLAIN ANALYZE SELECT * FROM t3 WHERE i > 8\G
*************************** 1. row ***************************
QUERY PLAN: Project
*************************** 2. row ***************************
QUERY PLAN:   Analyze: timeConsumed=0us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 3. row ***************************
QUERY PLAN:   ->  Table Scan on aaa.t3
*************************** 4. row ***************************
QUERY PLAN:         Analyze: timeConsumed=154us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 5. row ***************************
QUERY PLAN:         Filter Cond: (CAST(t3.i AS BIGINT) > 8)
5 rows in set (0.00 sec)

> EXPLAIN ANALYZE SELECT * FROM t3 WHERE pk > 17\G
*************************** 1. row ***************************
QUERY PLAN: Project
*************************** 2. row ***************************
QUERY PLAN:   Analyze: timeConsumed=0us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 3. row ***************************
QUERY PLAN:   ->  Table Scan on aaa.t3
*************************** 4. row ***************************
QUERY PLAN:         Analyze: timeConsumed=309us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 5. row ***************************
QUERY PLAN:         Filter Cond: (CAST(t3.pk AS BIGINT) > 17)
5 rows in set (0.00 sec)
```

该语句输出中显示的实际时间值以毫秒为单位。

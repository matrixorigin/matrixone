# EXPLAIN

EXPLAIN — 展示一个语句的执行计划。

## 语法结构

```
EXPLAIN [ ( option [, ...] ) ] statement

where option can be one of:
    ANALYZE [ boolean ]
    VERBOSE [ boolean ]
    FORMAT { TEXT | JSON }
```

0.5.1 版本仅支持 `TEXT` 格式。

## 语法描述

此命令主要作用是显示出 MatrixOne 计划程序为提供的语句生成的执行计划。执行计划显示了如何通过普通顺序扫描、索引扫描等方式扫描语句引用的表，如果引用了多个表，将使用什么连接算法将每个输入表中所需的行聚集在一起。

显示的最关键部分是估计语句执行成本，即计划程序将估计运行语句所需时间(以任意一种成本单位衡量，但通常是听过磁盘页获取)。实际上这里显示了两个数字：返回第一行之前的启动成本，以及返回所有行的总成本。对于大多数查询来说，总成本是最重要的，但在 `EXISTS` 中的子查询中，计划程序会选择最小的启动成本，而不是最小的总成本(因为执行者在获得一行之后就会停止)。此外，如果您使用 `LIMIT` 从句限制返回的行数，计划程序将在端点成本之间进行适当的插值，以便估计哪个计划真正是最便宜的。

`ANALYZE` 子句语法选项为语句实际执行，而不仅仅是计划执行，然后将实际运行时统计信息添加到显示中，包括每个计划节点中花费的总运行时间(以毫秒为单位)和实际返回的行总数。这有助于了解规划者的期望是否接近实际。

## 参数释义

* ANALYZE:

执行该命令并显示实际运行时和其他统计数据。该参数默认为 `FALSE`。

* VERBOSE:

`VERBOSE` 用作显示有关计划的其他信息。具体来说，包括计划树中每个节点的输出列列表、模式限定表和函数名称，始终使用范围表别名标记表达式中的变量，并且始终打印显示统计信息的每个触发器的名称。该参数默认为 `FALSE`。

* FORMAT:

`FORMAT` 用作指定输出格式，可以是 *TEXT*、*JSON*。非文本输出包含与文本输出格式相同的信息，且容易被程序解析。该参数默认为 `TEXT`。

* BOOLEAN:

`BOOLEAN` 指定所选选项是打开还是关闭。你可以写 `TRUE` 来启用该选项， 或者写 `FALSE` 来禁用它。

`boolean` 值也可以省略，省略 `boolean` 值的情况下默认为 `TRUE`。

* STETEMENT

MatrixOne 支持任何`SELECT`，`UPDATE`，`DELETE` 语句执行计划。在 MatrixOne 0.5.1 版本中仅支持 `INSERT`语句类型中的 `INSERT INTO..SELECT` 语句，暂不支持 `INSERT INTO...VALUES` 语句。

## 示例

### Node_TABLE_SCAN

```sql
mysql> explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 OR N_NATIONKEY < 10;
+------------------------------------------------------------------------------------+
| QUERY PLAN                                                                         |
+------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                             |
|   Output: #[0,0], #[0,1]                                                           |
|   ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)      |
|         Output: #[0,1], #[0,2]                                                     |
|         Table: 'nation' (0:'n_nationkey', 1:'n_name', 2:'n_regionkey')             |
|         Filter Cond: ((CAST(#[0,0] AS INT64) > 0) or (CAST(#[0,0] AS INT64) < 10)) |
+------------------------------------------------------------------------------------+
```

### Node_VALUE_SCAN

```sql
mysql> explain verbose select abs(-1);
+-----------------------------------------------------------------------------+
| QUERY PLAN                                                                  |
+-----------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=1.00 ndv=0.00 rowsize=0)                       |
|   Output: 1                                                                 |
|   ->  Values Scan "*VALUES*" (cost=0.00..0.00 card=1.00 ndv=0.00 rowsize=0) |
|         Output: 0                                                           |
+-----------------------------------------------------------------------------+
```

### Node_SORT

```sql
mysql> explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10 ORDER BY N_NAME, N_REGIONKEY DESC;
+--------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                 |
+--------------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                     |
|   Output: #[0,0], #[0,1]                                                                   |
|   ->  Sort(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                  |
|         Output: #[0,0], #[0,1]                                                             |
|         Sort Key: #[0,0] INTERNAL,  #[0,1] DESC                                            |
|         ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                         |
|               Output: #[0,0], #[0,1]                                                       |
|               ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)  |
|                     Output: #[0,1], #[0,2]                                                 |
|                     Table: 'nation' (0:'n_nationkey', 1:'n_name', 2:'n_regionkey')         |
|                     Filter Cond: (CAST(#[0,0] AS INT64) > 0), (CAST(#[0,0] AS INT64) < 10) |
+--------------------------------------------------------------------------------------------+
```

带有限制和偏移量：

```sql
mysql> explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY limit 10;
+-------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                |
+-------------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                    |
|   ->  Sort(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                 |
|         Sort Key: #[0,0] DESC,  #[0,1] INTERNAL                                           |
|         Limit: 10                                                                         |
|         ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                        |
|               ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|                     Filter Cond: (abs(CAST(#[0,1] AS INT64)) > 0), (#[0,0] like '%AA')    |
+-------------------------------------------------------------------------------------------+

```

```sql
mysql> explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10 offset 20;
+-------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                |
+-------------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                    |
|   ->  Sort(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                 |
|         Sort Key: #[0,0] DESC,  #[0,1] INTERNAL                                           |
|         Limit: 10, Offset: 20                                                             |
|         ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                        |
|               ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|                     Filter Cond: (abs(CAST(#[0,1] AS INT64)) > 0), (#[0,0] like '%AA')    |
+-------------------------------------------------------------------------------------------+
```

### Node_AGG

```sql
mysql> explain verbose SELECT count(*) FROM NATION group by N_NAME;
+-------------------------------------------------------------------------------------+
| QUERY PLAN                                                                          |
+-------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                              |
|   Output: #[0,0]                                                                    |
|   ->  Aggregate(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                      |
|         Output: #[-2,0]                                                             |
|         Group Key:#[0,1]                                                            |
|         Aggregate Functions: starcount(#[0,0])                                      |
|         ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|               Output: #[0,0], #[0,1]                                                |
|               Table: 'nation' (0:'n_nationkey', 1:'n_name')                         |
+-------------------------------------------------------------------------------------+
```

### Node_JOIN

```sql
mysql> explain verbose SELECT NATION.N_NAME, REGION.R_NAME FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND LENGTH(NATION.N_NAME) > LENGTH(REGION.R_NAME);
+--------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                 |
+--------------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=125.00 ndv=0.00 rowsize=0)                                    |
|   Output: #[0,1], #[0,0]                                                                   |
|   ->  Filter(cost=0.00..0.00 card=125.00 ndv=0.00 rowsize=0)                               |
|         Output: #[0,0], #[0,1]                                                             |
|         Filter Cond: (length(CAST(#[0,1] AS CHAR)) > length(CAST(#[0,0] AS CHAR)))         |
|         ->  Join(cost=0.00..0.00 card=125.00 ndv=0.00 rowsize=0)                           |
|               Output: #[0,1], #[1,0]                                                       |
|               Join Type: INNER                                                             |
|               Join Cond: (#[1,1] = #[0,0])                                                 |
|               ->  Table Scan on tpch.region(cost=0.00..0.00 card=5.00 ndv=0.00 rowsize=0)  |
|                     Output: #[0,0], #[0,1]                                                 |
|                     Table: 'region' (0:'r_regionkey', 1:'r_name')                          |
|               ->  Table Scan on tpch.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|                     Output: #[0,0], #[0,1]                                                 |
|                     Table: 'nation' (0:'n_name', 1:'n_regionkey')                          |
|                     Filter Cond: (CAST(#[0,1] AS INT64) > 10)                              |
+--------------------------------------------------------------------------------------------+

```

### Node_INSERT

```sql
mysql> explain verbose INSERT NATION select * from nation;
+---------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                  |
+---------------------------------------------------------------------------------------------+
| Insert on db1.nation (cost=0.0..0.0 rows=0 ndv=0 rowsize=0)                                 |
|   Output: #[0,0], #[0,1], #[0,2], #[0,3]                                                    |
|   ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                |
|         Output: #[0,0], #[0,1], #[0,2], #[0,3]                                              |
|         ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)         |
|               Output: #[0,0], #[0,1], #[0,2], #[0,3]                                        |
|               Table: 'nation' (0:'n_nationkey', 1:'n_name', 2:'n_regionkey', 3:'n_comment') |
+---------------------------------------------------------------------------------------------+
7 rows in set (0.00 sec)
```

### Node_Update

```sql
mysql> explain verbose UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2 WHERE N_NATIONKEY > 10 LIMIT 20;
+-------------------------------------------------------------------------------------+
| QUERY PLAN                                                                          |
+-------------------------------------------------------------------------------------+
| Update on db1.nation (cost=0.0..0.0 rows=0 ndv=0 rowsize=0)                         |
|   ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                        |
|         Output: #[0,0], 'U1', CAST(2 AS INT32)                                      |
|         Limit: 20                                                                   |
|         ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|               Output: #[0,1]                                                        |
|               Table: 'nation' (0:'n_nationkey', 1:'PADDR')                          |
|               Filter Cond: (CAST(#[0,0] AS INT64) > 10)                             |
+-------------------------------------------------------------------------------------+
```

### Node_Delete

```sql
mysql> explain verbose DELETE FROM NATION WHERE N_NATIONKEY > 10;
+-------------------------------------------------------------------------------------+
| QUERY PLAN                                                                          |
+-------------------------------------------------------------------------------------+
| Delete on db1.nation (cost=0.0..0.0 rows=0 ndv=0 rowsize=0)                         |
|   ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                        |
|         Output: #[0,0]                                                              |
|         ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|               Output: #[0,1]                                                        |
|               Table: 'nation' (0:'n_nationkey', 1:'PADDR')                          |
|               Filter Cond: (CAST(#[0,0] AS INT64) > 10)                             |
+-------------------------------------------------------------------------------------+
```

带有限制：

```sql
mysql>  explain verbose DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20;
+-------------------------------------------------------------------------------------+
| QUERY PLAN                                                                          |
+-------------------------------------------------------------------------------------+
| Delete on db1.nation (cost=0.0..0.0 rows=0 ndv=0 rowsize=0)                         |
|   ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                        |
|         Output: #[0,0]                                                              |
|         Limit: 20                                                                   |
|         ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|               Output: #[0,1]                                                        |
|               Table: 'nation' (0:'n_nationkey', 1:'PADDR')                          |
|               Filter Cond: (CAST(#[0,0] AS INT64) > 10)                             |
+-------------------------------------------------------------------------------------+
```

# 使用 EXPLAIN 解读执行计划

SQL 是一种声明性语言，因此无法通过 SQL 语句直接判断一条查询的执行是否有效率，但是可以使用 `EXPLAIN` 语句查看当前的执行计划。

## 示例

我们这里准备一个简单的示例，帮助你理解使用 EXPLAIN 解读执行计划。

```sql
> drop table if exists a;
> create table a(a int);
> insert into a values(1),(2),(3),(4),(5),(6),(7),(8);
> select count(*) from a where a>=2 and a<=8;
+----------+
| count(*) |
+----------+
|        7 |
+----------+
1 row in set (0.00 sec)

> explain select count(*) from a where a>=2 and a<=8;
+-----------------------------------------------------------------------------------+
| QUERY PLAN                                                                        |
+-----------------------------------------------------------------------------------+
| Project                                                                           |
|   ->  Aggregate                                                                   |
|         Aggregate Functions: starcount(1)                                         |
|         ->  Table Scan on aab.a                                                   |
|               Filter Cond: (CAST(a.a AS BIGINT) >= 2), (CAST(a.a AS BIGINT) <= 8) |
+-----------------------------------------------------------------------------------+
5 rows in set (0.00 sec)
```

以上是该查询的执行计划结果。从 `Filter Cond` 算子开始向上看，查询的执行过程如下：

1. 先执行过滤条件 `Filter Cond`：即过滤出数据类型为 `BIGINT` 且大于等于 2，小于等于 8 的整数，按照计算推理，应该为 `(2),(3),(4),(5),(6),(7),(8)`。
2. 扫描数据库 aab 中的表 a;
3. 聚合计算满足条件整数的个数，为 7 个。

最终，得到查询结果为 7，即 `count(*)` = 7。

### 评估当前的性能

EXPLAIN 语句只返回查询的执行计划，并不执行该查询。若要获取实际的执行时间，可执行该查询，或使用 EXPLAIN ANALYZE 语句：

```sql
> explain analyze select count(*) from a where a>=2 and a<=8;
+-------------------------------------------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                                                    |
+-------------------------------------------------------------------------------------------------------------------------------+
| Project                                                                                                                       |
|   Analyze: timeConsumed=0us inputRows=1 outputRows=1 inputSize=8bytes outputSize=8bytes memorySize=8bytes                     |
|   ->  Aggregate                                                                                                               |
|         Analyze: timeConsumed=3317us inputRows=2 outputRows=2 inputSize=8bytes outputSize=16bytes memorySize=16bytes          |
|         Aggregate Functions: starcount(1)                                                                                     |
|         ->  Table Scan on aab.a                                                                                               |
|               Analyze: timeConsumed=6643us inputRows=31 outputRows=24 inputSize=96bytes outputSize=64bytes memorySize=64bytes |
|               Filter Cond: (CAST(a.a AS BIGINT) >= 2), (CAST(a.a AS BIGINT) <= 8)                                             |
+-------------------------------------------------------------------------------------------------------------------------------+
8 rows in set (0.00 sec)
```

执行以上示例查询耗时 0.00 秒，说明执行性能较为理想。也由于我们这次示例中执行的查询简单，满足较高的执行性能。

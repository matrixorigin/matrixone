# Using EXPLAIN to learn the execution plan

Because SQL is a declarative language, you cannot automatically tell whether a query is executed efficiently. You must first use the `EXPLAIN` statement to learn the current execution plan.

## Example

We have prepared a simple example to help you understand how to interpret an execution plan using `EXPLAIN`.

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

Above are the execution plan results for this query. Starting from the `Filter Cond` operator, the execution process of the query is as follows:

1. Execute the Filter condition `Filter Cond` first: the integer whose data type is `BIGINT` and is greater than or equal to 2 and less than or equal to 8 is filtered out. According to the calculation reasoning, it should be `(2)`,`(3)`,`(4)`,`(5)`,`(6)`,`(7)`,`(8)`.

2. Scan Table in database *aab*.

3. The number of integers is 7.

In the end, the query result is 7, which means `count(*)` = 7.

### Assess the current performance

`EXPLAIN` only returns the query execution plan but does not execute the query. To get the actual execution time, you can either execute the query or use `EXPLAIN ANALYZE`:

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

The example query above takes 0.00 seconds to execute, which is an ideal performance. Also, because the query we executed in this example is simple, it meets the high execution performance.

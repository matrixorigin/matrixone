# **SELECT**

## **Description**

Retrieves data from a table.

## **Syntax**

``` sql
SELECT
    [ALL | DISTINCT ]
    select_expr [, select_expr] [[AS] alias] ...
    [INTO variable [, ...]]
    [FROM table_references
    [WHERE where_condition]
    [GROUP BY {col_name | expr | position}
      [ASC | DESC]]
    [HAVING where_condition]
    [ORDER BY {col_name | expr | position}
      [ASC | DESC]]
    [LIMIT {[offset,] row_count | row_count OFFSET offset}]
```

## **Examples**

```sql
> SELECT number FROM numbers(3);
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
+--------+

> SELECT * FROM t1 WHERE spID>2 AND userID <2 || userID >=2 OR userID < 2 LIMIT 3;

> SELECT userID,MAX(score) max_score FROM t1 WHERE userID <2 || userID > 3 GROUP BY userID ORDER BY max_score;

> create table t1 (spID int,userID int,score smallint);
> insert into t1 values (1,1,1);
> insert into t1 values (2,2,2);
> insert into t1 values (2,1,4);
> insert into t1 values (3,3,3);
> insert into t1 values (1,1,5);
> insert into t1 values (4,6,10);
> insert into t1 values (5,11,99);
> select userID,count(score) from t1 group by userID having count(score)>1 order by userID;
+--------+--------------+
| userid | count(score) |
+--------+--------------+
|      1 |            3 |
+--------+--------------+
> select userID,count(score) from t1 where userID>2 group by userID having count(score)>1 order by userID;
Empty set (0.01 sec)s
```

## **Constraints**

1. Table alias is not supported in GROUP BY.
2. SELECT...FOR UPDATE clause is not supported now.
3. INTO OUTFILE is limitedly support.
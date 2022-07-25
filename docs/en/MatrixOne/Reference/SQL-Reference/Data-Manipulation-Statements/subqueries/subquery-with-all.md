# **Subqueries with ALL**

## **Description**

The word `ALL`, which must follow a comparison operator, means “return `TRUE` if the comparison is `TRUE` for `ALL` of the values in the column that the subquery returns.”:

```
operand comparison_operator ALL (subquery)
```

For example:

```
SELECT s1 FROM t1 WHERE s1 > ALL (SELECT s1 FROM t2);
```

Suppose that there is a row in table t1 containing (10). The expression is `TRUE` if table t2 contains (-5,0,+5) because 10 is greater than all three values in t2. The expression is `FALSE` if table t2 contains (12,6,NULL,-100) because there is a single value 12 in table t2 that is greater than 10, and the result returns `Empty set`. The expression is unknown (that is, `NULL`) if table t2 contains (0,NULL,1).

Finally, the expression is `TRUE` if table t2 is empty. So, the following expression is `TRUE` when table t2 is empty:

```
SELECT * FROM t1 WHERE 1 > ALL (SELECT s1 FROM t2);
```

But this expression is `NULL` when table t2 is empty:

```
SELECT * FROM t1 WHERE 1 > (SELECT s1 FROM t2);
```

In addition, the following expression is NULL when table t2 is empty:

```
SELECT * FROM t1 WHERE 1 > ALL (SELECT MAX(s1) FROM t2);
```

In general, tables containing `NULL` values and empty tables are “edge cases.” When writing subqueries, always consider whether you have taken those two possibilities into account.

## **Syntax**

```
> SELECT column_name(s) FROM table_name {WHERE | HAVING} [not] expression comparison_operator ALL (subquery)
```

## **Examples**

```sql
> create table t1 (a int);
> create table t2 (a int, b int);
> create table t3 (a int);
> create table t4 (a int not null, b int not null);
> create table t5 (a int);
> create table t6 (a int, b int);
> insert into t1 values (2);
> insert into t2 values (1,7),(2,7);
> insert into t4 values (4,8),(3,8),(5,9);
> insert into t5 values (null);
> insert into t3 values (6),(7),(3);
> insert into t6 values (10,7),(null,7);
> select * from t3 where a <> all (select b from t2);
+------+
| a    |
+------+
|    6 |
|    3 |
+------+
2 rows in set (0.00 sec)

> select * from t4 where 5 > all (select a from t5);
+------+------+
| a    | b    |
+------+------+
|    4 |    8 |
|    3 |    8 |
|    5 |    9 |
+------+------+
3 rows in set (0.01 sec)

> select * from t3 where 10 > all (select b from t2);
+------+
| a    |
+------+
|    6 |
|    7 |
|    3 |
+------+
3 rows in set (0.00 sec)
```

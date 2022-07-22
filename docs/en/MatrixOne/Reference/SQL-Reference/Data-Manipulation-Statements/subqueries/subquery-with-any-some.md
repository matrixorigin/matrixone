# **Subqueries with ANY or SOME**

## **Description**

Comparison operators (=, >, < , etc.) are used only on subqueries that return one row. SQL Subqueries with `ANY`, you can make comparisons on subqueries that return multiple rows. `ANY` evaluate whether any or all of the values returned by a subquery match the left-hand expression.

Subqueries that use the `ANY` keyword return true when any value retrieved in the subquery matches the value of the left-hand expression.

!!! note  "<font size=4>note</font>"
    <font size=3>The word `SOME` is an alias for `ANY`.</font>

## **Syntax**

```
> SELECT column_name(s) FROM table_name WHERE column_name ANY (subquery);
```

## **Examples**

```sql
> create table t1 (a int);
> create table t2 (a int, b int);
> create table t3 (a int);
> create table t4 (a int not null, b int not null);
> insert into t1 values (2);
> insert into t2 values (1,7),(2,7);
> insert into t4 values (4,8),(3,8),(5,9);
> insert into t3 values (6),(7),(3);
> select * from t3 where a <> any (select b from t2);
+------+
| a    |
+------+
|    6 |
|    3 |
+------+
2 rows in set (0.00 sec)

> select * from t3 where a <> some (select b from t2);
+------+
| a    |
+------+
|    6 |
|    3 |
+------+
2 rows in set (0.00 sec)

> select * from t3 where a = some (select b from t2);
+------+
| a    |
+------+
|    7 |
+------+
1 row in set (0.00 sec)

> select * from t3 where a = any (select b from t2);
+------+
| a    |
+------+
|    7 |
+------+
1 row in set (0.00 sec)
```

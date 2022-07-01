# **Subqueries with EXISTS or NOT EXISTS**

## **Description**

The `EXISTS` operator is used to test for the existence of any record in a subquery.

If a subquery returns any rows at all, `EXISTS` subquery is `TRUE`, and `NOT EXISTS` subquery is `FALSE`.

## **Syntax**

```
> SELECT column_name(s)
FROM table_name
WHERE EXISTS
(SELECT column_name FROM table_name WHERE condition);
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
> select * from t3 where exists (select * from t2 where t2.b=t3.a);
+------+
| a    |
+------+
|    7 |
+------+
1 row in set (0.00 sec)
> select * from t3 where not exists (select * from t2 where t2.b=t3.a);
+------+
| a    |
+------+
|    6 |
|    3 |
+------+
2 rows in set (0.00 sec)
```

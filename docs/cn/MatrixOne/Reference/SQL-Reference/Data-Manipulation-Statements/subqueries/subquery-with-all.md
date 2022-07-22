# **Subqueries with ALL**

## **语法描述**

The word `ALL`, which must follow a comparison operator, means “return `TRUE` if the comparison is `TRUE` for `ALL` of the values in the column that the subquery returns.”:

关键词 `ALL` 必须跟在比较操作符后面，指如果子查询返回的列中值的 `ALL` 的比较是 `TRUE`，则返回 `TRUE`。

```
operand comparison_operator ALL (subquery)
```

示例如下：

```
SELECT s1 FROM t1 WHERE s1 > ALL (SELECT s1 FROM t2);
```

如上述示例中，假设表 t1 中有一行包含(10)，表 t2 包含(-5,0，+5)，则表达式为 `TRUE`，因为10大于 t2 中的所有三个值。如果表 t2 包含(12,6,NULL，-100)，则表达式为 `FALSE`，因为在表 t2 中有一个大于10的值12。如果表 t2 包含(0,NULL,1)，则表达式为 `NULL`。

- 如果表 t2 为空，则表达式为 `TRUE`。例如，当下表 t2 为空时，表达式是 `TRUE`：

```
SELECT * FROM t1 WHERE 1 > ALL (SELECT s1 FROM t2);
```

- 下面示例中，当表 t2 为空时，这个表达式是 `NULL`：

```
SELECT * FROM t1 WHERE 1 > (SELECT s1 FROM t2);
```

或：

```
SELECT * FROM t1 WHERE 1 > ALL (SELECT MAX(s1) FROM t2);
```

**说明**：在书写子查询语法时，要注意考虑到含有 `NULL` 值的表和空表的情况。

## **语法结构**

```
> SELECT column_name(s) FROM table_name {WHERE | HAVING} [not] expression comparison_operator ALL (subquery)
```

## **示例**

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

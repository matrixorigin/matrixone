# **DIV**

## **运算符说明**

`DIV` 运算符用于整数除法。

如果两个运算数值都是非整数类型，则会将运算数值转换为 `DECIMAL`，并在将结果转换为 `BIGINT` 之前使用 `DECIMAL` 算法进行除法。如果结果超出 `BIGINT` 范围，则会发生错误。

## **语法结构**

```
> SELECT value1 DIV value2;
```

```
> SELECT column1 DIV column2... FROM table_name;
```

## **示例**

```sql
> SELECT 5 DIV 2, -5 DIV 2, 5 DIV -2, -5 DIV -2;
+---------+----------+----------+-----------+
| 5 div 2 | -5 div 2 | 5 div -2 | -5 div -2 |
+---------+----------+----------+-----------+
|       2 |       -2 |       -2 |         2 |
+---------+----------+----------+-----------+
1 row in set (0.00 sec)
```

```sql
> create table t2(c1 int, c2 int);
> insert into t2 values (-3, 2);
> insert into t2 values (1, 2);
> select c1 DIV 3 from t2;
+----------+
| c1 div 3 |
+----------+
|       -1 |
|        0 |
+----------+
2 rows in set (0.00 sec)
> select c1 DIV c2 from t2;
+-----------+
| c1 div c2 |
+-----------+
|        -1 |
|         0 |
+-----------+
2 rows in set (0.00 sec)
```

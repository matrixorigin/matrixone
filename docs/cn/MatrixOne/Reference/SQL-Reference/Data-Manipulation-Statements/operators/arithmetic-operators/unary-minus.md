# **-**

## **运算符说明**

`-` 为负号运算符。负号运算符将表达式的符号从正数反转为负数，反之亦然。

## **语法结构**

```
> SELECT -column1, -column2, ...
FROM table_name;
```

## **示例**

```sql
select -2;
+------+
| -2   |
+------+
|   -2 |
+------+
```

```sql
> create table t2(c1 int, c2 int);
> insert into t2 values (-3, 2);
> insert into t2 values (1, 2);
> select -c1 from t2;
+------+
| -c1  |
+------+
|    3 |
|   -1 |
+------+
2 rows in set (0.00 sec)
```

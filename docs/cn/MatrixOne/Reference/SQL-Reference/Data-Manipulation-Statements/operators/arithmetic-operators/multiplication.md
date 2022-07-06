# *****

## **运算符说明**

`*` 运算符用于乘法运算。

## **语法结构**

```
> SELECT value1*value2;
```

```
> SELECT column1*column2... FROM table_name;
```

## **示例**

```sql
> select 1123.2333*1233.3331;
+-----------------------+
| 1123.2333 * 1233.3331 |
+-----------------------+
|    1385320.8079122303 |
+-----------------------+
1 row in set (0.01 sec)
```

```sql
> create table t2(c1 int, c2 int);
> insert into t2 values (-3, 2);
> insert into t2 values (1, 2);
> select c1*2 from t2;
+--------+
| c1 * 2 |
+--------+
|     -6 |
|      2 |
+--------+
2 rows in set (0.00 sec)
> select c1*c2 from t2;
+---------+
| c1 * c2 |
+---------+
|      -6 |
|       2 |
+---------+
2 rows in set (0.01 sec)
```

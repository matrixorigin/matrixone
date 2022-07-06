# **+**

## **Description**

The `+` operator is used for addition.

## **Syntax**

```
> SELECT value1+value2;
```

```
> SELECT column1+column2... FROM table_name;
```

## **Examples**

```sql
> select 1123.2333+1233.3331;
+-----------------------+
| 1123.2333 + 1233.3331 |
+-----------------------+
|             2356.5664 |
+-----------------------+
1 row in set (0.01 sec)
```

```sql
> create table t2(c1 int, c2 int);
> insert into t2 values (-3, 2);
> insert into t2 values (1, 2);
> select c1+5 from t2;
+--------+
| c1 + 5 |
+--------+
|      2 |
|      6 |
+--------+
2 rows in set (0.00 sec)
> select c1+c2 from t2;
+---------+
| c1 + c2 |
+---------+
|      -1 |
|       3 |
+---------+
2 rows in set (0.00 sec)
```

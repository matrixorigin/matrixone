# **-**

## **Description**

Unary minus. This operator changes the sign of the operand.

## **Syntax**

```
> SELECT -column1, -column2, ...
FROM table_name;
```

## **Examples**

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

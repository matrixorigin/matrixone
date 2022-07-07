# **%,MOD**

## **Description**

The `%,MOD` operator is used for modulo operation. Returns the remainder of N divided by M. For more information.

## **Syntax**

```
> SELECT N % M, N MOD M;
```

## **Examples**

```sql
> select 12 mod 5;
+--------+
| 12 % 5 |
+--------+
|      2 |
+--------+
1 row in set (0.00 sec)
```

```sql
> create table t2(c1 int, c2 int);
> insert into t2 values (-3, 2);
> insert into t2 values (1, 2);
> select c1 mod 2 from t2;
+--------+
| c1 % 2 |
+--------+
|     -1 |
|      1 |
+--------+
2 rows in set (0.01 sec)
> select c1 mod c2 from t2;
+---------+
| c1 % c2 |
+---------+
|      -1 |
|       1 |
+---------+
2 rows in set (0.01 sec)
```

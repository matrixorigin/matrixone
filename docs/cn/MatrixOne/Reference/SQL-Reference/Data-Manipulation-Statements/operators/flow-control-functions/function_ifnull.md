# **IFNULL**

## **Description**

If expr1 is not `NULL`, `IFNULL()` returns expr1; otherwise it returns expr2.
The default return type of `IFNULL(expr1,expr2)` is the more “general” of the two expressions, in the order `STRING`, `REAL`, or `INTEGER`.

## **Syntax**

```
> IFNULL(expr1,expr2)
```

## **Examples**

```sql
> SELECT IFNULL(NULL,10);
+------------------+
| ifnull(null, 10) |
+------------------+
|               10 |
+------------------+
1 row in set (0.00 sec)
```

```sql
SELECT CAST(IFNULL(NULL, NULL) AS DECIMAL);
+-----------------------------------------+
| cast(ifnull(null, null) as decimal(10)) |
+-----------------------------------------+
|                                    NULL |
+-----------------------------------------+
1 row in set (0.01 sec)
```

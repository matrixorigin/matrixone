# **POWER()**

## **Description**

POWER(X, Y) returns the value of X raised to the power of Y.

## **Syntax**

```
> POWER(X, Y)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| X | Required. Any numeric data type supported now. |
| Y | Required. Any numeric data type supported now. |

## **Examples**

```sql
> drop table if exists t1;
> create table t1(a int,b int);
> insert into t1 values(5,-2),(10,3),(100,0),(4,3),(6,-3);
> select power(a,b) from t1;
+-------------+
| power(a, b) |
+-------------+
|      0.0400 |
|   1000.0000 |
|      1.0000 |
|     64.0000 |
|      0.0046 |
+-------------+
> select power(a,2) as a1, power(b,2) as b1 from t1 where power(a,2) > power(b,2) order by a1 asc;
+------------+--------+
| a1         | b1     |
+------------+--------+
|    16.0000 | 9.0000 |
|    25.0000 | 4.0000 |
|    36.0000 | 9.0000 |
|   100.0000 | 9.0000 |
| 10000.0000 | 0.0000 |
+------------+--------+
```

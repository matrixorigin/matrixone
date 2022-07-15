# **ABS()**

## **Description**

ABS(X) Returns the absolute value of X, or NULL if X is NULL.

## **Syntax**

```
> ABS(number)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| number | Required. Any numeric data type supported now. |

The result type is derived from the argument type.

## **Examples**

```sql
> drop table if exists t1;
> create table t1(a int,b float);
> insert into t1 values(1,-3.1416);
> insert into t1 values(-1,1.57);
> select abs(a),abs(b) from t1;
+--------+--------+
| abs(a) | abs(b) |
+--------+--------+
|      1 | 3.1416 |
|      1 | 1.5700 |
+--------+--------+
```

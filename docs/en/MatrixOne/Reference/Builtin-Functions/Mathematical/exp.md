# **EXP()**

## **Description**

The EXP() function returns the value of e (the base of natural logarithms) raised to the power of X. 

## **Syntax**

```
> EXP(number)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| number | Required. Any numeric data type supported now. |




## **Examples**

```sql
> drop table if exists t1;
> create table t1(a int ,b float);
> insert into t1 values(-4, 2.45);
> insert into t1 values(6, -3.62);
> select exp(a), exp(b) from t1;
+----------+---------+
| exp(a)   | exp(b)  |
+----------+---------+
|   0.0183 | 11.5883 |
| 403.4288 |  0.0268 |
+----------+---------+

```

## Constraints
Currently, MatrixOne doesn't support select function() without from tables.

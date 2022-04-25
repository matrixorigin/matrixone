# **COS()**

## **Description**

The COS() function returns the cosine of input number(given in radians).


## **Syntax**

```
> COS(number)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| number | Required. Any numeric data type supported now. |


## **Examples**

```sql
> drop table if exists t1;
> create table t1(a int,b float);
> insert into t1 values(1,3.14159);
> insert into t1 values(-1,1.57);
> select cos(a),cos(b) from t1;
+--------+---------+
| cos(a) | cos(b)  |
+--------+---------+
| 0.5403 | -1.0000 |
| 0.5403 |  0.0008 |
+--------+---------+
```

## Constraints
Currently, MatrixOne doesn't support select function() without from tables.

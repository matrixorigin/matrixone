# **COT()**

## **Description**

The COT() function returns the cotangent of input number(given in radians).


## **Syntax**

```
> COT(number)
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
> insert into t1 values(-1,0);
> select cot(a),cot(b) from t1;
+---------+--------+
| cot(a)  | cot(b) |
+---------+--------+
| -0.5574 | 1.0000 |
|  2.5574 | 1.0000 |
+---------+--------+
```

## Constraints
Currently, MatrixOne doesn't support select function() without from tables.

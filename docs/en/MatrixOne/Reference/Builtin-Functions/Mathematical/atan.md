# **ATAN()**

## **Description**

The ATAN() function returns the arctangent(given in radians) of the input number.

## **Syntax**

```
> ATAN(number)
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
> insert into t1 values(0,1);
> select atan(a),atan(tan(b)) from t1;
+---------+--------------+
| atan(a) | atan(tan(b)) |
+---------+--------------+
|  0.7854 |      -0.0000 |
|  0.0000 |       1.0000 |
+---------+--------------+

```

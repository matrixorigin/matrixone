# **ACOS()**

## **Description**

The ACOS() function returns the arccosine(given in radians) of the input number.

## **Syntax**

```
> ACOS(number)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| number | Required. Any numeric data type supported now. |

## **Examples**

```sql
> drop table if exists t1;
> create table t1(a float,b int);
> insert into t1 values(0.5,1);
> insert into t1 values(-0.5,-1);
> select acos(a),acos(b) from t1;
+---------+---------+
| acos(a) | acos(b) |
+---------+---------+
|  1.0472 |  0.0000 |
|  2.0944 |  3.1416 |
+---------+---------+

```

# **SINH()**

## **Description**

The SINH() function returns the hyperbolic sine of the input number(given in radians).

## **Syntax**

```
> SINH(number)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| number | Required. Any numeric data type supported now. |

## **Examples**

```sql
> drop table if exists t1;
> create table t1(a int,b float);
> insert into t1 values(1,3.14159),
                       (-1,-3.14159);
> select sinh(a), sinh(b) from t1;


```

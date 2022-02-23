# **ROUND()**

## **Description**

The ROUND() function rounds a number to a specified number of decimal places.  
The function returns the nearest number of the specified order. In case when given number has equal distance to surrounding numbers, the function uses banker’s rounding for float number types and rounds away from zero for the other number types (Decimal).


## **Syntax**

```
> ROUND(number, decimals)
> ROUND(number)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| number | Required.  The number to round, including any numeric data type supported now. |
| decimals| Optional. An integer that represents the number of decimal places you want to round to. Default value is 0. <br> **decimals>0** then the function rounds the value to the right of the decimal point. <br> **decimals<0** then the function rounds the value to the left of the decimal point. <br> **decimals=0** then the function rounds the value to integer.|



## **Examples**


```
> drop table if exists t1;
> create table t1(a int ,b float);
> insert into t1 values(1,0.5);
> insert into t1 values(2,0.499);
> insert into t1 values(3,0.501);
> insert into t1 values(4,20.5);
> insert into t1 values(5,20.499);
> insert into t1 values(6,13.500);
> insert into t1 values(7,-0.500);
> insert into t1 values(8,-0.499);
> insert into t1 values(9,-0.501);
> insert into t1 values(10,-20.499);
> insert into t1 values(11,-20.500);
> insert into t1 values(12,-13.500);
> select a,round(b) from t1;

a	round(b)
1	0.0000
2	0.0000
3	1.0000
4	20.0000
5	20.0000
6	14.0000
7	-0.0000
8	-0.0000
9	-1.0000
10	-20.0000
11	-20.0000
12	-14.0000
```


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
round(1.5) ----> 2
round(2.5) ----> 2
round(3.5) ----> 4 
round(12, -1) ----> 10
round(12) ----> 12
round(-12, -1) ----> -20
round(-12, 1) ----> -12
round(12.345) ----> 12
round(12.345, 1) ----> 12.3
round(-12.345, 1) ----> -12.3
round(-12.345, -1) ----> -10
round(-12.345) ----> -12
```

## Constraints
Currently, MatrixOne doesn't support select function() without from tables.

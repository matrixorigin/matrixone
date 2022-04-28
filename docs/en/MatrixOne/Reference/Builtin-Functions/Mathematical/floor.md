# **FLOOR()**

## **Description**

The FLOOR() function returns the largest round number that is less than or equal to the number.


## **Syntax**

```
> FLOOR(number, decimals)
> FLOOR(number)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| number | Required. Any numeric data type supported now. |
| decimals| Optional. An integer that represents the number of decimal places. By default it is zero, which means to round to an integer.<br>**decimals** may also be negative.|



## **Examples**

```
floor(12, -1) ----> 10
floor(12) ----> 12
floor(-12, -1) ----> -20
floor(-12, 1) ----> -12
floor(12.345) ----> 12
floor(12.345, 1) ----> 12.3
floor(-12.345, 1) ----> -12.4
floor(-12.345, -1) ----> -20
floor(-12.345) ----> -13
```

## Constraints
Currently, MatrixOne doesn't support select function() without from tables.

# **MIN**

## **Description**

Aggregate function.

The MAX() function calculates the maximum value across a group of values.

## **Syntax**

```
> MIN(expr)
```

## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| expr  | Any expression |

## **Returned Value**
Returns the minimum value of expr. MIN() may take a string argument, in such cases, it returns the minimum string value. 

## **Examples**

Note: numbers(N) – A table for test with the single number column (UInt64) that contains integers from 0 to N-1.

```
> SELECT MIN(*) FROM numbers(3);
+--------+
| min(*) |
+--------+
|      0 |
+--------+

> SELECT MIN(number) FROM numbers(3);
+-------------+
| min(number) |
+-------------+
|           0 |
+-------------+

> SELECT MIN(number) AS min FROM numbers(3);
+------+
| min  |
+------+
|    0 |
+------+
```
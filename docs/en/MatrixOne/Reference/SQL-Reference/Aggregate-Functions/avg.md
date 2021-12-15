# **AVG**

## **Description**

Aggregate function.

The AVG() function calculates the average value of the argument.


## **Syntax**

```
> AVG(expr)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| expr  | Any numerical expression |

## **Returned Value**
The arithmetic mean, always as Double.

NaN if the input parameter is empty.

## **Examples**

Note: numbers(N) – A table for test with the single number column (UInt64) that contains integers from 0 to N-1.

```
> SELECT AVG(*) FROM numbers(3);
+--------+
| avg(*) |
+--------+
|      1 |
+--------+

> SELECT AVG(number) FROM numbers(3);
+-------------+
| avg(number) |
+-------------+
|           1 |
+-------------+

> SELECT AVG(number+1) FROM numbers(3);
+----------------------+
| avg(plus(number, 1)) |
+----------------------+
|                    2 |
+----------------------+

> SELECT AVG(number+1) AS a FROM numbers(3);
+------+
| a    |
+------+
|    2 |
+------+
```
***
